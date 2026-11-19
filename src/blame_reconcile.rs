//! Durable current-tip warming. There is no receive hook or per-tip queue.
use super::{Request, ResourceProfile, batch::WriterProof, derive};
use crate::{
    Result,
    blob::Store,
    cache::Cache,
    derived_cache::{CacheConfig, DerivedCache},
    invalid,
    metadata::warm,
    process::Operation,
    repository::blame_source::Source,
};
use git_rs_core::{CapacityBudget, Control, blame::ProducerVersion};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "blame_initial_warm.rs"]
mod initial;

const SQL_SECONDS: u64 = 5;

fn setting(name: &str, default: u64, max: u64) -> Result<u64> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if !(1..=max).contains(&value) {
        return Err(invalid(&format!("{name} must be in 1..={max}")));
    }
    Ok(value)
}

/// One worker with no target queue and its own per-job budget and process.
/// Only slot IDs survive a scan; every job acquires the live branch and ACL.
pub fn run(once: bool) -> Result<()> {
    let seconds = setting("GIT_RS_WARM_JOB_SECONDS", 120, 300)?;
    let interval = setting("GIT_RS_WARM_INTERVAL_SECONDS", 60, 3600)?;
    let selected = std::env::var_os("GIT_RS_BLAME_RESOURCE_PROFILE");
    let resources = ResourceProfile::from_os_value(selected.as_deref())?;
    resources.validate_capacity(1, 0)?;
    let initial = if std::env::var_os("GIT_RS_WARM_INITIAL_SECONDS").is_some() {
        if !once || resources != ResourceProfile::LargeV1 {
            return Err(invalid(
                "initial warm requires large-v1 warm-reconcile --once",
            ));
        }
        Some(initial::Limits::load(seconds)?)
    } else {
        None
    };
    ProducerVersion::verified()?;
    let config = CacheConfig::from_env()?;
    let key_file = std::env::var_os("GIT_RS_BLAME_CHECKPOINT_KEY_FILE")
        .ok_or_else(|| invalid("warm reconciliation requires a protected blame signing file"))?;
    let writer = WriterProof::load(&config, Path::new(&key_file))?;
    let env = derive::Environment {
        store: Store::from_env()?,
        files: Arc::new(Cache::new(
            initial.map_or(32 * 1024 * 1024, |limits| limits.range_bytes),
            8 * 1024 * 1024,
        )),
        derived: Some(DerivedCache::open(config)?),
        writer: Some(writer),
        observer: None,
        options: Default::default(),
        resources,
        reuse: true,
        body_lookup: true,
    };
    loop {
        match scan(&env, seconds, initial) {
            result if once => return result,
            Err(_) => eprintln!("warm reconciliation pass had errors; retrying on the next pass"),
            Ok(()) => {}
        }
        std::thread::sleep(Duration::from_secs(interval));
    }
}

fn scan(env: &derive::Environment, seconds: u64, initial: Option<initial::Limits>) -> Result<()> {
    let deadline = initial.map(|limits| Instant::now() + limits.total);
    let mut failed = false;
    for slot in Operation::new(SQL_SECONDS).enter(warm::slots)? {
        if let (Some(limits), Some(deadline)) = (initial, deadline) {
            if Instant::now() >= deadline {
                return Err(invalid("initial warm aggregate deadline exceeded"));
            }
            if let Err(error) = initial::run(env, slot, limits, deadline) {
                if std::env::var_os("GIT_RS_DEBUG_TIMINGS").as_deref()
                    == Some(std::ffi::OsStr::new("1"))
                {
                    eprintln!("initial_warm_failed: {error}");
                }
                failed = true;
            }
            continue;
        }
        let operation = Operation::new(seconds);
        if operation
            .enter(|| target(env.clone(), slot, operation.clone()))
            .is_err()
        {
            failed = true;
        }
    }
    if failed {
        Err(invalid("warm reconciliation pass had target errors"))
    } else {
        Ok(())
    }
}

fn target(env: derive::Environment, slot: u8, operation: Arc<Operation>) -> Result<()> {
    let budget = CapacityBudget::new(env.resources.job_bytes());
    let control = Control::new(operation.deadline());
    let Some(crate::object_source::WarmSource {
        source,
        principal,
        path,
    }) = Source::acquire_warm(slot, operation, &budget)?
    else {
        return Ok(());
    };
    let tip = *source
        .snapshot
        .refs
        .get(&source.snapshot.default_ref)
        .ok_or_else(|| invalid("missing warm branch tip"))?;
    let query = Request::from_raw(tip, path)?;
    // This process never constructs a serving Engine or uses foreground permits.
    // Drop the response and its cache/source holds before visiting the next slot.
    drop(derive::derive_source(
        env,
        source,
        principal,
        query,
        derive::Lane::Reconcile,
        &control,
        &budget,
    )?);
    Ok(())
}
