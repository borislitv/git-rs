//! Direct receive composition for the normal HTTP and SSH transport entrypoints.
//!
//! Only `publish` can turn a private preparation into a receipt. Parsing, pack
//! preparation and core semantic validation separately grant no publication right.
//! HTTP framing/gzip completion and SSH command/single-pack boundaries are
//! distinct authorities. Neither accepts a caller-supplied EOF boolean.

use crate::{
    Result, auth,
    blob::Store,
    cpu_jobs::{Completed, CpuAdmission, CpuIoJob, CpuJob},
    engine::Engine,
    http, invalid,
    metadata::{self, Barrier, Snapshot},
    metrics::{self, Metric, Phase},
    object_source::{RangeStore, Source as HeldSource},
    protocol::{Oid, Update},
    receive_io::{IoControl, ReceiveInput},
};
use git_rs_core::{
    CapacityBudget, CapacityBuffer, CapacityReservation, Control, ReadLimits, SnapshotSession,
    receive::{CommandBatch, CommandLimits, CommandSection, ReceiveLimits, RefSnapshot},
};
use std::{
    io::{BufRead, BufReader, Cursor, Write},
    os::fd::OwnedFd,
    sync::Arc,
};

mod budget;
mod pack;
pub(crate) mod transport;
pub mod wire;

/// Held authorized source and immutable memory/range storage. No cache-generation equality is
/// imposed: SQL retains its existing old-OID/ACL/epoch/live-lease CAS semantics.
/// Error/queue-discard cleanup may run Barrier's existing separate five-second
/// metadata-only Drop under the actual work permit; acquisition never runs there.
struct Source {
    held: Arc<HeldSource>,
    store: Store,
    cache: Arc<crate::cache::Cache>,
    principal: String,
    operation_id: String,
}

impl std::ops::Deref for Source {
    type Target = HeldSource;
    fn deref(&self) -> &HeldSource {
        &self.held
    }
}

/// The actual runtime slot precedes source/scratch/body acquisition. Field order
/// is significant: preparation failures destroy source/scratch BEFORE refunding
/// admission. Jobs/results/errors subsequently own that same slot, not a second
/// admission or a waiter-local last source Arc without a corresponding permit.
pub struct ReceiveSession<'cpu> {
    source: Source,
    admitted: CpuAdmission<'cpu>,
}

impl<'cpu> ReceiveSession<'cpu> {
    /// Engine must obtain `self.blame.cpu().try_admit(original_operation)` first.
    /// This factory performs real authorized acquisition only under that token;
    /// callers cannot inject pre-acquired scratch or an unrelated request budget.
    pub fn acquire(
        admitted: CpuAdmission<'cpu>,
        engine: &Engine,
        slug: &str,
        principal: &str,
    ) -> Result<Self> {
        let io = admitted.io_control();
        let operation = io.operation().clone();
        let source = operation.enter(|| -> Result<Source> {
            auth::repository(slug)?;
            auth::principal(principal)?;
            operation.check()?;
            io.control().check()?;
            let held =
                HeldSource::acquire(slug, principal, true, operation.clone(), admitted.budget())?;
            let operation_id = crate::scratch::identifier()?;
            operation.check()?;
            io.control().check()?;
            Ok(Source {
                held,
                store: engine.store.clone(),
                cache: engine.cache.clone(),
                principal: principal.to_owned(),
                operation_id,
            })
        })?;
        Ok(Self { source, admitted })
    }

    /// Use this exact context if constructing an owned reader after admission.
    /// Existing HTTP BufReader prefetch must not be discarded or read twice.
    pub fn io_control(&self) -> IoControl {
        self.admitted.io_control()
    }

    /// SSH discovery shares the held snapshot and admission with the following
    /// command/pack phase. Output must have the original absolute interrupt; this
    /// generic writer does not make arbitrary blocking stdout/socket writes safe.
    pub fn advertise(&self, smart_http: bool, output: &mut impl Write) -> Result<()> {
        let _wire_capacity = self.admitted.budget().reservation(4096)?;
        self.source.operation.enter(|| {
            self.source.operation.check()?;
            self.admitted.io_control().control().check()?;
            wire::advertise(&self.source.snapshot, smart_http, output)
        })
    }

    /// Consume the *actual* HTTP framing reader through its terminal boundary.
    /// Short Content-Length/chunk syntax errors cannot create a CPU job. No
    /// caller-supplied boolean or arbitrary Read EOF is accepted as that witness.
    /// The caller must retain its absolute socket interrupt while this runs.
    /// Preparation is admitted already, including finite disk spooling. Decoding
    /// and semantic work subsequently use the SAME admission and capacity budget.
    pub fn run_http(
        self,
        request: &http::Request,
        input: &mut impl BufRead,
    ) -> Result<Option<Completed<Prepared>>> {
        let http::Action::Git(cgi) = &request.action else {
            return Err(invalid("receive requires a Git HTTP action"));
        };
        if request.service != auth::Service::Receive
            || cgi.method != "POST"
            || cgi.path != "/git-receive-pack"
            || !cgi.query.is_empty()
            || !["", "version=0", "version=1", "version=2"].contains(&request.protocol.as_str())
        {
            return Err(invalid("unsupported direct receive HTTP request"));
        }
        let encoding = transport::Encoding::parse(&cgi.encoding)?;
        let io = self.admitted.io_control();
        let mut body = self.admitted.budget().buffer(32 * 1024)?;
        let mut output = transport::MemoryOutput {
            bytes: &mut body,
            control: io.control(),
            limit: http::MAX_BODY as usize,
        };
        transport::copy_bounded(
            &mut request.body(input),
            &mut output,
            http::MAX_BODY,
            &|| {
                self.source.operation.check()?;
                self.admitted.io_control().control().check()?;
                Ok(())
            },
        )?;
        self.source.operation.check()?;
        self.admitted.run(HttpJob {
            source: self.source,
            body,
            encoding,
        })
    }

    /// Typed SSH backend boundary: move an owned UnixStream/TcpStream clone into
    /// OwnedFd and retain the original for framed output. There must be no second
    /// reader/pump on the same input. No socket drain or client stdin EOF is used.
    pub fn run_ssh(self, fd: OwnedFd) -> Result<Option<Completed<Prepared>>> {
        let input = ReceiveInput::new(fd, self.admitted.io_control())?;
        self.admitted.run_io(
            input,
            SshJob {
                source: self.source,
            },
        )
    }
}

/// HTTP discovery acquires no scratch or packs. The parent may write HTTP success
/// headers only AFTER this authorized owner exists. Lease cleanup and any output
/// stay under admission, including errors and a slow/disconnected client.
pub struct ReceiveDiscovery<'cpu> {
    snapshot: Snapshot,
    _lease: Barrier,
    admitted: CpuAdmission<'cpu>,
}

impl<'cpu> ReceiveDiscovery<'cpu> {
    pub fn acquire_http(
        admitted: CpuAdmission<'cpu>,
        request: &http::Request,
        principal: &str,
    ) -> Result<Self> {
        let http::Action::Git(cgi) = &request.action else {
            return Err(invalid("receive discovery requires a Git HTTP action"));
        };
        if request.service != auth::Service::Receive
            || cgi.method != "GET"
            || cgi.path != "/info/refs"
            || cgi.query != "service=git-receive-pack"
            || !["", "version=0", "version=1", "version=2"].contains(&request.protocol.as_str())
        {
            return Err(invalid("unsupported direct receive discovery"));
        }
        let io = admitted.io_control();
        let operation = io.operation().clone();
        let (snapshot, lease) = operation.enter(|| {
            operation.check()?;
            io.control().check()?;
            metadata::leased_snapshot(&request.slug, principal, true)
        })?;
        Ok(Self {
            snapshot,
            _lease: lease,
            admitted,
        })
    }

    pub fn write(&self, output: &mut impl Write) -> Result<()> {
        let _wire_capacity = self.admitted.budget().reservation(4096)?;
        let io = self.admitted.io_control();
        io.operation().enter(|| {
            io.control().check()?;
            io.operation().check()?;
            wire::advertise(&self.snapshot, true, output)
        })
    }
}

/// Private fields bind the completed HTTP body to its original session. Its
/// construction is possible only after `http::Body` acknowledged completion.
struct HttpJob {
    source: Source,
    body: CapacityBuffer,
    encoding: transport::Encoding,
}

/// This is a sealed application join, not an independently constructible copy of
/// the core witness. The borrowed structural/semantic owners coexist inside the
/// CPU callback until both succeed and their exact publication fields are sealed.
/// Normalized files are written only from those validated bytes. Their private
/// scratch and old-source pins survive output, PUT, COMMIT and client output.
pub struct Prepared {
    validation: Result<Preparation>,
    commands: CommandProjection,
    capabilities: git_rs_core::receive::Capabilities,
    control: Control,
    capacity: CapacityBudget,
    source: Source,
}

enum Preparation {
    Noop(git_rs_core::receive::Noop),
    Changes(ValidatedPack),
}

fn noop(
    source: Source,
    token: git_rs_core::receive::Noop,
    control: &Control,
    capacity: &CapacityBudget,
) -> Result<Prepared> {
    control.check()?;
    Ok(Prepared {
        validation: Ok(Preparation::Noop(token)),
        commands: CommandProjection {
            updates: Vec::new(),
            _capacity: capacity.reservation(0)?,
        },
        capabilities: Default::default(),
        control: control.clone(),
        capacity: capacity.clone(),
        source,
    })
}

struct ValidatedPack {
    pack: Option<PackBytes>,
    fast_forward: Vec<bool>,
    _capacity: CapacityReservation,
}

struct PackBytes {
    hash: String,
    pack: gix::odb::pack::memory::OwnedBuffer<u8>,
    index: gix::odb::pack::memory::OwnedBuffer<u8>,
}

impl CpuJob for HttpJob {
    type Output = Prepared;

    fn run(self, control: &Control, capacity: &CapacityBudget) -> Result<Prepared> {
        control.check()?;
        // Gzip works on finite private input on the *same* CPU runtime. HTTP
        // framing has completed already; decoder EOF must additionally validate
        // CRC/ISIZE, every concatenated member, and reject any non-member tail.
        let body = if self.encoding == transport::Encoding::Gzip {
            let mut decoded = capacity.buffer(32 * 1024)?;
            let mut output = transport::MemoryOutput {
                bytes: &mut decoded,
                control,
                limit: http::MAX_BODY as usize,
            };
            transport::decode_gzip(&mut Cursor::new(self.body), &mut output, control, capacity)?;
            decoded
        } else {
            self.body
        };
        let mut input = Cursor::new(body);
        let section = CommandSection::read(
            &mut input,
            control,
            capacity.clone(),
            CommandLimits::default(),
        )?;
        let commands = match section {
            CommandSection::Commands(commands) => commands,
            CommandSection::Noop(token) => {
                if !input.fill_buf()?.is_empty() {
                    return Err(invalid("unexpected input after HTTP no-op"));
                }
                return noop(self.source, token, control, capacity);
            }
        };
        let capabilities = commands.capabilities();
        let updates = CommandProjection::from_batch(&commands, capacity, control)?;
        let validation = validate(
            &self.source,
            commands,
            &mut input,
            pack::Boundary::Http,
            control,
            capacity,
        )
        .map(Preparation::Changes);
        Ok(Prepared {
            validation,
            commands: updates,
            capabilities,
            control: control.clone(),
            capacity: capacity.clone(),
            source: self.source,
        })
    }
}

struct SshJob {
    source: Source,
}

impl CpuIoJob for SshJob {
    type Output = Prepared;

    fn run(
        self,
        input: &mut ReceiveInput,
        control: &Control,
        capacity: &CapacityBudget,
    ) -> Result<Prepared> {
        control.check()?;
        let _reader_capacity = capacity.reservation(32 * 1024)?;
        let mut input = BufReader::with_capacity(32 * 1024, input);
        let section = CommandSection::read(
            &mut input,
            control,
            capacity.clone(),
            CommandLimits::default(),
        )?;
        let commands = match section {
            CommandSection::Commands(commands) => commands,
            CommandSection::Noop(token) => return noop(self.source, token, control, capacity),
        };
        let capabilities = commands.capabilities();
        let updates = CommandProjection::from_batch(&commands, capacity, control)?;
        let validation = validate(
            &self.source,
            commands,
            &mut input,
            pack::Boundary::Ssh,
            control,
            capacity,
        )
        .map(Preparation::Changes);
        Ok(Prepared {
            validation,
            commands: updates,
            capabilities,
            control: control.clone(),
            capacity: capacity.clone(),
            source: self.source,
        })
    }
}

/// Immutable application/SQL projection of the entire parsed batch. This is NOT
/// semantic, transport, or authorization evidence. No mutable vector, subset
/// constructor, or detached allocation is exposed. Prepared seals it only after
/// joining the independent witnesses; rejected batches use it for ordered status.
pub struct CommandProjection {
    updates: Vec<Update>,
    _capacity: CapacityReservation,
}

impl CommandProjection {
    pub fn from_batch(
        commands: &CommandBatch,
        budget: &CapacityBudget,
        control: &Control,
    ) -> Result<Self> {
        let count = commands.commands().len();
        let names: usize = commands
            .commands()
            .iter()
            .map(|command| command.name().len())
            .sum();
        // There are at most 10,000 refs, each <=1,024 bytes. Admission precedes both
        // vector and name allocation; reconcile actual capacities before retaining.
        let mut reservation = budget.reservation(count * std::mem::size_of::<Update>() + names)?;
        let mut updates = Vec::new();
        updates.try_reserve_exact(count)?;
        let mut actual_names = 0;
        for command in commands.commands() {
            control.check()?;
            let mut name = Vec::new();
            name.try_reserve_exact(command.name().len())?;
            name.extend_from_slice(command.name());
            actual_names += name.capacity();
            updates.push(Update {
                old: oid(command.old())?,
                new: oid(command.new())?,
                name,
            });
        }
        reservation.resize(updates.capacity() * std::mem::size_of::<Update>() + actual_names)?;
        Ok(Self {
            updates,
            _capacity: reservation,
        })
    }

    pub fn updates(&self) -> &[Update] {
        &self.updates
    }
}

fn oid(id: gix::ObjectId) -> Result<Oid> {
    Ok(Oid(id
        .as_slice()
        .try_into()
        .map_err(|_| invalid("receive requires SHA-1 IDs"))?))
}

impl RefSnapshot for Snapshot {
    fn ref_oid(&self, name: &[u8]) -> Option<gix::ObjectId> {
        self.refs.get(name).map(|oid| gix::ObjectId::from(oid.0))
    }
}

fn validate(
    session: &Source,
    commands: CommandBatch,
    input: &mut impl BufRead,
    boundary: pack::Boundary,
    control: &Control,
    capacity: &CapacityBudget,
) -> Result<ValidatedPack> {
    let limits = ReadLimits::default();
    let identity = session.identity();
    let provider = RangeStore::open(
        session.held.clone(),
        session.store.clone(),
        session.cache.clone(),
        capacity.clone(),
        limits,
        control,
    )?;
    let old = SnapshotSession::open(
        provider,
        &identity,
        control.clone(),
        capacity.clone(),
        limits,
    )?;
    let budget = Arc::new(budget::Budget::new(capacity.clone())?);
    let resolve = |id: &gix::hash::oid, out: &mut [u8]| {
        let Some(object) = old
            .verified_object(id)
            .map_err(gix::error::Error::from_boxed)?
        else {
            return Ok(None);
        };
        if object.as_slice().len() > out.len() {
            return Err(gix::error::allocation_limit(
                "external receive base exceeds decoder capacity",
            )
            .into());
        }
        for (destination, source) in out
            .chunks_mut(32 * 1024)
            .zip(object.as_slice().chunks(32 * 1024))
        {
            vendor_check(control)?;
            destination[..source.len()].copy_from_slice(source);
        }
        Ok(Some((object.header().kind, object.as_slice().len())))
    };
    let outcome = pack::prepare(
        input,
        &commands,
        boundary,
        limits,
        budget.as_ref(),
        &resolve,
        &|| vendor_check(control),
    )?;
    let normalized = outcome.as_ref().map(pack::Normalized::open).transpose()?;
    let mut entries = pack::Entries::new(normalized.as_ref());
    let combined = pack::Combined {
        old: &old,
        incoming: normalized.as_ref(),
        identity: &identity,
    };
    let snapshot = SnapshotSession::open(
        combined,
        &identity,
        control.clone(),
        capacity.clone(),
        limits,
    )?;
    let validated = metrics::measure(Metric::Phase(Phase::Graph), || {
        git_rs_core::receive::ReceiveSession::new(snapshot, ReceiveLimits::default())?.run(
            commands,
            &session.snapshot,
            &mut entries,
        )
    })?;
    validated.check()?;
    let mut ff_capacity = capacity.reservation(validated.fast_forward().len())?;
    let mut fast_forward = Vec::new();
    fast_forward.try_reserve_exact(validated.fast_forward().len())?;
    ff_capacity.resize(fast_forward.capacity())?;
    fast_forward.extend_from_slice(validated.fast_forward());
    let incoming = validated.incoming_objects();
    validated.check()?;
    drop(validated);
    drop(normalized);
    let pack = if incoming == 0 {
        None
    } else {
        let outcome =
            outcome.ok_or_else(|| invalid("validated incoming objects have no structural pack"))?;
        Some(PackBytes {
            hash: outcome.outcome.data_hash.to_string(),
            pack: outcome.pack.into_owned(budget.clone())?,
            index: outcome.index.into_owned(budget.clone())?,
        })
    };
    control.check()?;
    Ok(ValidatedPack {
        pack,
        fast_forward,
        _capacity: ff_capacity,
    })
}

fn vendor_check(control: &Control) -> gix::error::Result {
    control
        .check()
        .map_err(|error| gix::error::Error::from_boxed(Box::new(error)))
}

/// A confirmed success and a rejected/unknown publication are disjoint. A later
/// socket write failure cannot change the stored publication result to rollback.
pub struct Receipt {
    prepared: Completed<Prepared>,
    publication: Result<Publication>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publication {
    Noop,
    Published { generation: u64 },
}

/// The only PUT/SQL path. Consuming the shared runtime's output keeps its actual
/// admission, scratch, source pins and lease until the receipt is consumed/dropped.
/// Store owns staged-before-PUT and metadata owns exact lost-COMMIT confirmation.
pub fn publish(prepared: Completed<Prepared>, store: &Store) -> Receipt {
    let publication = prepared.source.operation.enter(|| {
        let preparation = prepared
            .validation
            .as_ref()
            .map_err(|_| invalid("receive validation rejected"))?;
        let Preparation::Changes(files) = preparation else {
            prepared.control.check()?;
            prepared.source.operation.check()?;
            return Ok(Publication::Noop);
        };
        let session = &prepared.source;
        prepared.control.check()?;
        session.operation.check()?;
        session.lease().check()?;
        let mut packs = Vec::new();
        if let Some(pack) = &files.pack {
            packs.push(metrics::measure(Metric::Phase(Phase::Upload), || {
                store.upload_bytes(
                    &session.snapshot.id,
                    &session.operation_id,
                    &pack.hash,
                    &pack.pack,
                    &pack.index,
                )
            })?);
        }
        session.operation.check()?;
        prepared.control.check()?;
        metrics::measure(Metric::Phase(Phase::Publish), || {
            metadata::publish(
                &session.snapshot,
                session.lease(),
                &session.operation_id,
                &session.principal,
                prepared.commands.updates(),
                &files.fast_forward,
                &packs,
            )
        })
        .map(|generation| Publication::Published { generation })
    });
    Receipt {
        prepared,
        publication,
    }
}

impl Receipt {
    /// Extract only a bounded owned post-confirmation hint, then destroy all
    /// receive resources/holds before a caller can submit independent CPU work.
    pub(crate) fn after_cleanup<T>(
        self,
        hint: impl FnOnce(&Snapshot, u64, &str, &[Update]) -> T,
    ) -> Option<T> {
        let pending = match self.publication() {
            Ok(Publication::Published { generation }) => Some(hint(
                &self.prepared.source.snapshot,
                generation,
                &self.prepared.source.operation_id,
                self.prepared.commands.updates(),
            )),
            _ => None,
        };
        drop(self);
        pending
    }

    pub fn publication(
        &self,
    ) -> std::result::Result<Publication, &(dyn std::error::Error + Send + Sync)> {
        self.publication
            .as_ref()
            .copied()
            .map_err(|error| error.as_ref())
    }

    pub fn write_status(&self, output: &mut impl Write) -> Result<()> {
        self.prepared.control.check()?;
        self.prepared.source.operation.check()?;
        let _wire_capacity = self.prepared.capacity.reservation(4096)?;
        wire::report_status(
            self.prepared.commands.updates(),
            self.prepared.capabilities,
            self.publication.is_ok(),
            output,
        )
    }
}
