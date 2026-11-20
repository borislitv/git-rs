//! Test-only IPC over public artifact APIs. This is a libtest fixture, not a
//! service endpoint, compute worker ABI, or additional Cargo example target.
use super::support::{self, Source};
use git_rs::{
    Result,
    derived_cache::{
        ArtifactKind, BlameArtifacts, BlameBuilds, BuildLease, BuildLimits, BuildLookup,
        CacheConfig, CacheMaintenance, CheckpointVersion, CompleteArtifact, CompleteCheckpoint,
        DerivedCache, StagedChunk,
    },
    invalid,
    process::{self, Admission, Operation},
};
use std::{
    collections::HashMap,
    io::{BufRead, Read, Write},
    sync::Arc,
};

fn response(value: &str) {
    println!("ARTIFACT {value}");
    std::io::stdout().flush().unwrap();
}
fn limits() -> Result<BuildLimits> {
    let limits = BuildLimits {
        hard_seconds: std::env::var("GIT_RS_TEST_BUILD_HARD_SECONDS")
            .unwrap_or_else(|_| "30".into())
            .parse()?,
        lease_seconds: std::env::var("GIT_RS_TEST_BUILD_LEASE_SECONDS")
            .unwrap_or_else(|_| "15".into())
            .parse()?,
    };
    limits.validate()?;
    Ok(limits)
}
fn report(body: &CompleteArtifact) -> Result<String> {
    body.with_chunks(|chunks| {
        let mut digest = openssl::sha::Sha256::new();
        for chunk in chunks {
            for block in chunk.chunks(65536) {
                process::check_operation()?;
                digest.update(block);
            }
        }
        Ok(format!(
            "complete {} {}",
            body.len(),
            git_rs::protocol::hex(&digest.finish())
        ))
    })
}
fn derive(
    cache: &DerivedCache,
    artifacts: &BlameArtifacts,
    admission: &Arc<Admission>,
    name: &str,
    length: usize,
    byte: u8,
) -> Result<String> {
    let _actual_work = admission
        .try_admit()
        .ok_or_else(|| invalid("fixture admission exhausted"))?;
    let key = support::key(name.as_bytes());
    let source = Source::acquire(&key);
    let repo = source.repo.clone();
    let lease = match BlameBuilds::new(cache).claim(&key, source, limits()?)? {
        BuildLookup::Claimed(lease) => lease,
        BuildLookup::Pending => return Err(invalid("fixture build pending")),
    };
    let local = vec![byte; length];
    lease.run_with_renewal(|build| {
        response(&format!("working {repo}"));
        // This caller owns a complete admitted local result. Optional cache
        // failure may lose warming, never that source-authorized result.
        if let Ok(Some(chunk)) = artifacts.stage_chunk(build, &local) {
            let _ = artifacts.publish(build, ArtifactKind::CompleteBody, &[chunk]);
        }
        build.check_local()?;
        Ok(format!(
            "local {} {}",
            local.len(),
            git_rs::protocol::hex(&openssl::sha::sha256(&local))
        ))
    })
}

fn report_checkpoint(body: &CompleteCheckpoint) -> Result<String> {
    body.with_chunks(|chunks| {
        let mut digest = openssl::sha::Sha256::new();
        for chunk in chunks {
            for block in chunk.chunks(65536) {
                process::check_operation()?;
                digest.update(block);
            }
        }
        Ok(format!(
            "complete {} {}",
            body.len(),
            git_rs::protocol::hex(&digest.finish())
        ))
    })
}

pub fn run() -> Result<()> {
    let cache = DerivedCache::open(CacheConfig::from_env()?)?;
    let artifacts = BlameArtifacts::new(&cache);
    let maintenance = CacheMaintenance::new(&cache);
    let admission = Admission::new(1);
    let mut builds: HashMap<String, Arc<BuildLease>> = HashMap::new();
    let mut chunks: HashMap<String, Vec<StagedChunk>> = HashMap::new();
    let mut held: HashMap<String, CompleteArtifact> = HashMap::new();
    let mut versions: HashMap<String, CheckpointVersion> = HashMap::new();
    let mut checkpoints: HashMap<String, CompleteCheckpoint> = HashMap::new();
    let mut checkpoint_attempt = None;
    let mut maintenance_lease = None;
    let mut consumers = 0usize;
    response("ready");
    let mut input = std::io::stdin().lock();
    loop {
        let mut line = String::new();
        if input.by_ref().take(8193).read_line(&mut line)? == 0 {
            break;
        }
        if line.len() > 8192 {
            return Err(invalid("fixture command too long"));
        }
        let args: Vec<_> = line.split_whitespace().collect();
        if args.as_slice() == ["quit"] {
            break;
        }
        let result = Operation::new(30).enter(|| -> Result<String> {
            match args.as_slice() {
                ["build", name] if builds.len() < 16 => {
                    let key = support::key(name.as_bytes());
                    match BlameBuilds::new(&cache).claim(&key, support::authority(), limits()?)? {
                        BuildLookup::Claimed(lease) => {
                            builds.insert(name.to_string(), Arc::new(lease));
                            Ok("claimed".into())
                        }
                        BuildLookup::Pending => Ok("pending".into()),
                    }
                }
                [action @ ("stage" | "stage-short"), name, length, byte] => {
                    let length: usize = length.parse()?;
                    if length == 0 || length > 4 * 1024 * 1024 {
                        return Err(invalid("fixture chunk length"));
                    }
                    let existing = chunks.entry(name.to_string()).or_default();
                    if existing.len() >= 65 {
                        return Err(invalid("fixture chunk count"));
                    }
                    let lease = builds
                        .get(*name)
                        .ok_or_else(|| invalid("fixture build absent"))?;
                    let bytes = vec![byte.parse::<u8>()?; length];
                    let staged = if *action == "stage-short" {
                        Operation::new(1).enter(|| artifacts.stage_chunk(lease, &bytes))
                    } else {
                        artifacts.stage_chunk(lease, &bytes)
                    };
                    match staged? {
                        Some(chunk) => {
                            existing.push(chunk);
                            Ok("staged".into())
                        }
                        None => Ok("pending".into()),
                    }
                }
                ["publish", name] => {
                    let lease = builds
                        .get(*name)
                        .ok_or_else(|| invalid("fixture build absent"))?;
                    Ok(artifacts
                        .publish(
                            lease,
                            ArtifactKind::CompleteBody,
                            chunks.get(*name).map_or(&[], Vec::as_slice),
                        )?
                        .to_string())
                }
                ["checkpoint-start", name] if checkpoint_attempt.is_none() => {
                    let build = builds
                        .get(*name)
                        .ok_or_else(|| invalid("fixture build absent"))?
                        .clone();
                    let expected = versions.get(*name).cloned();
                    let batch = chunks.remove(*name).unwrap_or_default();
                    let artifacts = BlameArtifacts::new(&cache);
                    checkpoint_attempt = Some(std::thread::spawn(move || {
                        Operation::new(30).enter(|| {
                            artifacts.replace_checkpoint(&build, expected.as_ref(), &batch)
                        })
                    }));
                    Ok("started".into())
                }
                ["checkpoint-await"] => {
                    let result = checkpoint_attempt
                        .take()
                        .ok_or_else(|| invalid("fixture attempt absent"))?
                        .join()
                        .map_err(|_| invalid("fixture checkpoint thread panicked"))??;
                    Ok(match result {
                        Some(version) => format!(
                            "checkpoint {} {} {}",
                            version.payload_id(),
                            version.generation(),
                            git_rs::protocol::hex(version.digest())
                        ),
                        None => "pending".into(),
                    })
                }
                ["checkpoint", name] => {
                    let build = builds
                        .get(*name)
                        .ok_or_else(|| invalid("fixture build absent"))?;
                    let version = artifacts.replace_checkpoint(
                        build,
                        versions.get(*name),
                        chunks.get(*name).map_or(&[], Vec::as_slice),
                    )?;
                    let Some(version) = version else {
                        return Ok("pending".into());
                    };
                    let reply = format!(
                        "checkpoint {} {} {}",
                        version.payload_id(),
                        version.generation(),
                        git_rs::protocol::hex(version.digest())
                    );
                    versions.insert(name.to_string(), version);
                    chunks.remove(*name);
                    Ok(reply)
                }
                [action @ ("checkpoint-get" | "checkpoint-hold"), name] => {
                    if checkpoints.len() >= 4 || versions.len() >= 16 {
                        return Err(invalid("fixture checkpoint owner bound"));
                    }
                    let Some(body) = artifacts.get_checkpoint_verified(
                        &support::key(name.as_bytes()),
                        support::authority(),
                    )?
                    else {
                        return Ok("miss".into());
                    };
                    let reply = report_checkpoint(&body)?;
                    versions.insert(name.to_string(), body.version().clone());
                    consumers += 1;
                    if *action == "checkpoint-hold" {
                        checkpoints.insert(name.to_string(), body);
                    }
                    Ok(reply)
                }
                ["checkpoint-consume", name] => {
                    let reply = report_checkpoint(
                        checkpoints
                            .get(*name)
                            .ok_or_else(|| invalid("fixture checkpoint absent"))?,
                    )?;
                    consumers += 1;
                    Ok(reply)
                }
                ["checkpoint-release", name] => {
                    checkpoints.remove(*name);
                    Ok("ok".into())
                }
                [action @ ("get" | "hold"), name] => {
                    if held.len() >= 4 {
                        return Err(invalid("fixture held-body bound"));
                    }
                    let Some(body) = artifacts.get_verified(
                        &support::key(name.as_bytes()),
                        ArtifactKind::CompleteBody,
                        support::authority(),
                    )?
                    else {
                        return Ok("miss".into());
                    };
                    let value = report(&body)?;
                    consumers += 1;
                    if *action == "hold" {
                        held.insert(name.to_string(), body);
                    }
                    Ok(value)
                }
                ["consume", name] => {
                    let value = report(
                        held.get(*name)
                            .ok_or_else(|| invalid("fixture body absent"))?,
                    )?;
                    consumers += 1;
                    Ok(value)
                }
                ["release", name] => {
                    held.remove(*name);
                    Ok("ok".into())
                }
                ["consumers"] => Ok(consumers.to_string()),
                ["drop-builds"] => {
                    builds.clear();
                    chunks.clear();
                    Ok("ok".into())
                }
                ["maintenance"] => {
                    maintenance_lease = maintenance.claim()?;
                    Ok(if maintenance_lease.is_some() {
                        "owned"
                    } else {
                        "pending"
                    }
                    .into())
                }
                [action @ ("expire" | "retire" | "delete")] => {
                    let lease = maintenance_lease
                        .as_ref()
                        .ok_or_else(|| invalid("fixture maintenance absent"))?;
                    Ok(match *action {
                        "expire" => maintenance.expire_batch(lease)?.to_string(),
                        "retire" => maintenance.retire_payloads(lease)?.to_string(),
                        _ => maintenance.delete_retired(lease)?.to_string(),
                    })
                }
                ["derive", name, length, byte] => {
                    let length: usize = length.parse()?;
                    if length == 0 || length > 4 * 1024 * 1024 {
                        return Err(invalid("fixture derive length"));
                    }
                    derive(&cache, &artifacts, &admission, name, length, byte.parse()?)
                }
                ["active"] => Ok(admission.active().to_string()),
                ["source"] => {
                    let snapshot =
                        git_rs::metadata::snapshot("synthetic/cache-source", "alice", false)?;
                    Ok(format!(
                        "source {} {}",
                        snapshot.generation,
                        snapshot.refs.len()
                    ))
                }
                _ => Err(invalid("unknown fixture command")),
            }
        });
        match result {
            Ok(value) => response(&value),
            Err(error) => response(&format!("error {error}")),
        }
    }
    Ok(())
}
