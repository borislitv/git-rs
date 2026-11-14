//! Test-only prepared-core entrypoint. Catalog paths are supplied by the external
//! synthetic producer; no Git process, checkout/config discovery or server fallback.
use git_rs::{Result, invalid};
use git_rs_core::{
    CapacityBudget, Control, SnapshotIdentity, SnapshotSession,
    blame::BlameRequest,
    pack::{DecoderOptions, PackProvider, PackSet},
    porcelain::Porcelain,
};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

pub fn run(args: &[String]) -> Result<()> {
    let options = super::fixture_options(args)?;
    let commit = gix::ObjectId::from_hex(super::option(args, "--commit")?.as_bytes())?;
    let path = super::option(args, "--path")?;
    let indexes: Vec<PathBuf> = args
        .windows(2)
        .filter(|pair| pair[0] == "--index")
        .map(|pair| PathBuf::from(&pair[1]))
        .collect();
    if indexes.is_empty() || indexes.len() > 32 {
        return Err(invalid(
            "prepared fixture needs 1..=32 explicit catalog indexes",
        ));
    }
    // One shared 1 GiB job budget, historical 256 MiB decoder quota, no added
    // ODB/object memo or mapping checkpoint. All knobs are fixed before timing.
    let budget = CapacityBudget::new(1024 * 1024 * 1024);
    let control = Control::new(Instant::now() + Duration::from_secs(20));
    let limits = options.read_limits();
    let identity = SnapshotIdentity {
        source: b"synthetic-prepared-2000".to_vec(),
        generation: commit.as_slice().to_vec(),
        object_hash: gix::hash::Kind::Sha1,
    };
    let packs = PackSet::open_with_options(
        indexes,
        (),
        identity.object_hash,
        &control,
        limits,
        DecoderOptions {
            cache_slots: 4096,
            cache_bytes: 256 * 1024 * 1024,
            max_cache_entry_bytes: 1024 * 1024,
            header_slots: 4096,
            retained_workspace_bytes: 16 * 1024 * 1024,
        },
        budget.clone(),
    )?;
    let provider = PackProvider::new(identity.clone(), packs, budget.clone(), limits);
    let session =
        SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)?;
    let started = Instant::now();
    let blame = session.blame_complete(
        BlameRequest {
            commit: &commit,
            path: path.as_bytes(),
        },
        &options,
    )?;
    let stats = blame.statistics();
    let body = Porcelain::encode_complete(&blame, &control, &budget, 256 * 1024 * 1024)?;
    let mut output = std::io::stdout().lock();
    for chunk in body.chunks() {
        control.check()?;
        output.write_all(chunk)?;
    }
    output.flush()?;
    control.check()?;
    eprintln!(
        "PREPARED {{\"commits\":{},\"seconds\":{},\"option_bits\":{},\"mapping_hits\":{},\"body_bytes\":{}}}",
        stats.commits_traversed,
        started.elapsed().as_secs_f64(),
        options.canonical_bytes()[16],
        stats.mapping.hits,
        body.len()
    );
    Ok(())
}
