//! Native full push to an empty held source. One admitted compressed backing
//! moves from framing through validation into the authoritative upload.
use super::*;
use crate::metrics::BootstrapStage;
use git_rs_core::receive::packed::{InventoryFailure, Packed, Progress, ReuseStatistics};
use std::io::Read;

pub(super) fn run_http(
    session: ReceiveSession<'_>,
    request: &http::Request,
    input: &mut impl BufRead,
    encoding: transport::Encoding,
    profile: ResourceProfile,
) -> Result<Option<Completed<Prepared>>> {
    let stage = BootstrapStage::Body.start(true);
    if encoding != transport::Encoding::Identity {
        return Err(invalid(
            "large bootstrap HTTP receive requires identity encoding",
        ));
    }
    if !session.source.snapshot.refs.is_empty() || !session.source.snapshot.packs.is_empty() {
        return Err(invalid(
            "large bootstrap requires a source with no refs or packs",
        ));
    }
    let io = session.admitted.io_control();
    let control = io.control();
    let capacity = session.admitted.budget();
    let reader_capacity = capacity.reservation(32 * 1024)?;
    let mut body = BufReader::with_capacity(
        32 * 1024,
        request.body_with_limit(input, profile.body_bytes()),
    );
    let section = CommandSection::read(
        &mut body,
        control,
        capacity.clone(),
        CommandLimits::default(),
    )?;
    let mut bytes = capacity.buffer(0)?;
    if !body.fill_buf()?.is_empty() {
        if matches!(&section, CommandSection::Noop(_)) {
            return Err(invalid("unexpected input after HTTP no-op"));
        }
        const HEADER: usize = gix::odb::pack::data::header::SIZE;
        let mut header = [0; HEADER];
        body.read_exact(&mut header)?;
        let (_, count) = gix::odb::pack::data::header::decode(&header)?;
        if count as usize > profile.limits().1.max_incoming_objects {
            return Err(invalid("large receive pack count exceeds profile"));
        }
        let limit = usize::try_from(profile.body_bytes())?;
        bytes = capacity.buffer(32 * 1024)?;
        bytes.extend_from_slice(&header)?;
        transport::copy_bounded(
            &mut body,
            &mut transport::MemoryOutput {
                bytes: &mut bytes,
                control,
                limit,
            },
            profile.body_bytes() - HEADER as u64,
            &|| {
                io.operation().check()?;
                control.check()?;
                Ok(())
            },
        )?;
    }
    // At this point Body has checked terminal HTTP framing; neither a partial
    // body nor buffered bytes beyond the terminal boundary can enter the job.
    drop(body);
    drop(reader_capacity);
    stage.complete();
    session.admitted.run(Job {
        source: session.source,
        section,
        bytes,
        profile,
    })
}

struct Job {
    source: Source,
    section: CommandSection,
    bytes: CapacityBuffer,
    profile: ResourceProfile,
}

impl CpuJob for Job {
    type Output = Prepared;

    fn run(self, control: &Control, capacity: &CapacityBudget) -> Result<Prepared> {
        let commands = match self.section {
            CommandSection::Commands(commands) => commands,
            CommandSection::Noop(token) => return noop(self.source, token, control, capacity),
        };
        let capabilities = commands.capabilities();
        let projection = CommandProjection::from_batch(&commands, capacity, control)?;
        let validation = validate(
            &self.source,
            commands,
            &projection,
            self.bytes,
            self.profile,
            control,
            capacity,
        )
        .map(Preparation::Changes);
        Ok(Prepared {
            validation,
            commands: projection,
            capabilities,
            control: control.clone(),
            capacity: capacity.clone(),
            source: self.source,
        })
    }
}

fn validate(
    source: &Source,
    commands: CommandBatch,
    projection: &CommandProjection,
    bytes: CapacityBuffer,
    profile: ResourceProfile,
    control: &Control,
    capacity: &CapacityBudget,
) -> Result<ValidatedPack> {
    let inventory = BootstrapStage::Inventory.start(true);
    source.check()?;
    if bytes.as_slice().is_empty() {
        return Err(invalid("bootstrap updates require an incoming pack"));
    }
    let identity = source.identity();
    let (read_limits, receive_limits) = profile.limits();
    let decoder_budget = budget::Budget::new(capacity.clone())?;
    let mut substage: Option<metrics::BootstrapStageGuard> = None;
    let mut reuse = ReuseStatistics::default();
    let packed = metrics::measure(Metric::Phase(Phase::ReceivePackPrepare), || {
        Packed::open_forest(
            bytes.as_slice(),
            &identity,
            control,
            capacity,
            &decoder_budget,
            read_limits,
            receive_limits,
            |progress| {
                let next = match progress {
                    Progress::Checksum => BootstrapStage::InventoryChecksum,
                    Progress::Entries => BootstrapStage::InventoryEntries,
                    Progress::Sort => BootstrapStage::InventorySort,
                    Progress::Resolving { .. } => return,
                    Progress::Reuse(delta) => {
                        reuse.hits += delta.hits;
                        reuse.avoided_ancestor_bytes += delta.avoided_ancestor_bytes;
                        metrics::bootstrap_reuse(None, delta);
                        return;
                    }
                    Progress::EntryBatch {
                        objects,
                        logical_bytes,
                        checkpoints,
                    } => {
                        metrics::bootstrap_inventory(objects, logical_bytes, checkpoints);
                        return;
                    }
                };
                if let Some(previous) = substage.replace(next.start(true)) {
                    previous.complete();
                }
            },
        )
    });
    metrics::bootstrap_reuse(Some(packed.is_ok()), reuse);
    if let Err(error) = &packed
        && let Some(reason) = error.downcast_ref::<InventoryFailure>()
    {
        metrics::bootstrap_inventory_failure(*reason);
    }
    let packed = packed?;
    if let Some(last) = substage {
        last.complete();
    }
    inventory.complete();
    let (objects, logical, checkpoints) = packed.statistics();
    let graph = BootstrapStage::Graph.start(true);
    let snapshot = SnapshotSession::open(
        &packed,
        &identity,
        control.clone(),
        capacity.clone(),
        read_limits,
    )?;
    let validated = metrics::measure(Metric::Phase(Phase::Graph), || {
        git_rs_core::receive::ReceiveSession::new(snapshot, receive_limits)?
            .run_packed(commands, &source.snapshot)
    });
    metrics::bootstrap_work(
        bytes.as_slice().len() as u64,
        objects as u64,
        logical,
        checkpoints.max(packed.statistics().2),
    );
    let validated = validated?;
    validated.check()?;
    let accepted = validated.commands().commands();
    if accepted.len() != projection.updates().len() {
        return Err(invalid("bootstrap command projection length changed"));
    }
    for (core, sql) in accepted.iter().zip(projection.updates()) {
        control.check()?;
        if oid(core.old())? != sql.old
            || oid(core.new())? != sql.new
            || core.name() != sql.name.as_slice()
        {
            return Err(invalid("bootstrap command projection changed"));
        }
    }
    let count = validated.incoming_objects();
    if count == 0 {
        return Err(invalid("bootstrap updates require incoming objects"));
    }
    let mut ff_capacity = capacity.reservation(validated.fast_forward().len())?;
    let mut fast_forward = Vec::new();
    fast_forward.try_reserve_exact(validated.fast_forward().len())?;
    ff_capacity.resize(fast_forward.capacity())?;
    fast_forward.extend_from_slice(validated.fast_forward());
    drop(validated);
    graph.complete();

    // v2: fanout and headers, 20-byte IDs, CRCs, offsets (at most 8 extra
    // bytes each), and two SHA-1 digests. Reserve the full worst case once.
    let max_index = 8 + 4 * 256 + count * (20 + 4 + 4 + 8) + 40;
    let index_stage = BootstrapStage::Index.start(true);
    let mut index = capacity.buffer(max_index)?;
    packed.write_index(
        &mut transport::MemoryOutput {
            bytes: &mut index,
            control,
            limit: max_index,
        },
        control,
    )?;
    index_stage.complete();
    let hash = packed.hash().to_string();
    drop(packed);
    source.check()?;
    control.check()?;
    Ok(ValidatedPack {
        pack: Some(PackBytes {
            hash,
            pack: PackBacking::Packed(bytes),
            index: PackBacking::Packed(index),
        }),
        fast_forward,
        _capacity: ff_capacity,
    })
}
