//! Direct receive composition. Dispatch is deliberately not enabled here: the
//! parent owns the Stage 8 fixture selection and subsequent qualified cutover.
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
    process,
    protocol::{Oid, Update},
    receive_io::{IoControl, ReceiveInput},
    repository,
    scratch::Scratch,
};
use git_rs_core::{
    CapacityBudget, CapacityReservation, Control, ReadLimits, SnapshotIdentity, SnapshotSession,
    receive::{CommandBatch, CommandLimits, ReceiveLimits, RefSnapshot},
};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Seek, Write},
    os::fd::OwnedFd,
    sync::Arc,
};

mod budget;
mod pack;
mod transport;
pub mod wire;

/// Held authorized source and private scratch. No cache-generation equality is
/// imposed: SQL retains its existing old-OID/ACL/epoch/live-lease CAS semantics.
/// Error/queue-discard cleanup may run Barrier's existing separate five-second
/// metadata-only Drop under the actual work permit; acquisition never runs there.
struct Source {
    snapshot: Snapshot,
    scratch: Scratch,
    lease: Barrier,
    principal: String,
    operation_id: String,
    operation: Arc<process::Operation>,
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
            let (snapshot, lease) = metadata::leased_snapshot(slug, principal, true)?;
            let scratch = engine.materialize(&snapshot)?;
            let operation_id = crate::scratch::identifier()?;
            operation.check()?;
            io.control().check()?;
            Ok(Source {
                snapshot,
                scratch,
                lease,
                principal: principal.to_owned(),
                operation_id,
                operation: operation.clone(),
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
        let _spool_capacity = self.admitted.budget().reservation(32 * 1024)?;
        let mut file = transport::private_file(&self.source.scratch.path.join("receive-body"))?;
        transport::copy_bounded(&mut request.body(input), &mut file, http::MAX_BODY, &|| {
            self.source.operation.check()?;
            self.admitted.io_control().control().check()?;
            Ok(())
        })?;
        self.source.operation.check()?;
        file.rewind()?;
        drop(_spool_capacity);
        self.admitted.run(HttpJob {
            source: self.source,
            file,
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
    file: File,
    encoding: transport::Encoding,
}

/// This is a sealed application join, not an independently constructible copy of
/// the core witness. The borrowed structural/semantic owners coexist inside the
/// CPU callback until both succeed and their exact publication fields are sealed.
/// Normalized files are written only from those validated bytes. Their private
/// scratch and old-source pins survive output, PUT, COMMIT and client output.
pub struct Prepared {
    validation: Result<ValidatedFiles>,
    commands: CommandProjection,
    capabilities: git_rs_core::receive::Capabilities,
    control: Control,
    capacity: CapacityBudget,
    source: Source,
}

struct ValidatedFiles {
    hash: Option<String>,
    fast_forward: Vec<bool>,
    _capacity: CapacityReservation,
}

impl CpuJob for HttpJob {
    type Output = Prepared;

    fn run(mut self, control: &Control, capacity: &CapacityBudget) -> Result<Prepared> {
        control.check()?;
        // Gzip works on finite private input on the *same* CPU runtime. HTTP
        // framing has completed already; decoder EOF must additionally validate
        // CRC/ISIZE, every concatenated member, and reject any non-member tail.
        if self.encoding == transport::Encoding::Gzip {
            let mut decoded =
                transport::private_file(&self.source.scratch.path.join("receive-decoded"))?;
            transport::decode_gzip(&mut self.file, &mut decoded, control, capacity)?;
            decoded.rewind()?;
            self.file = decoded;
        }
        let _reader_capacity = capacity.reservation(32 * 1024)?;
        let mut input = BufReader::with_capacity(32 * 1024, self.file);
        let commands = CommandBatch::read(
            &mut input,
            control,
            capacity.clone(),
            CommandLimits::default(),
        )?;
        let capabilities = commands.capabilities();
        let updates = CommandProjection::from_batch(&commands, capacity, control)?;
        let validation = validate(
            &self.source,
            commands,
            &mut input,
            pack::Boundary::Http,
            control,
            capacity,
        );
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
        let commands = CommandBatch::read(
            &mut input,
            control,
            capacity.clone(),
            CommandLimits::default(),
        )?;
        let capabilities = commands.capabilities();
        let updates = CommandProjection::from_batch(&commands, capacity, control)?;
        let validation = validate(
            &self.source,
            commands,
            &mut input,
            pack::Boundary::Ssh,
            control,
            capacity,
        );
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
) -> Result<ValidatedFiles> {
    let limits = ReadLimits::default();
    let identity = SnapshotIdentity {
        source: session.snapshot.id.as_bytes().to_vec(),
        generation: session.snapshot.generation.to_be_bytes().to_vec(),
        object_hash: gix::hash::Kind::Sha1,
    };
    let old = repository::open_snapshot(
        &session.scratch,
        identity.clone(),
        &session.lease,
        control.clone(),
        capacity.clone(),
        limits,
    )?;
    let budget = budget::Budget::new(capacity.clone())?;
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
        &budget,
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
    let validated = git_rs_core::receive::ReceiveSession::new(snapshot, ReceiveLimits::default())?
        .run(commands, &session.snapshot, &mut entries)?;
    validated.check()?;
    let mut ff_capacity = capacity.reservation(validated.fast_forward().len())?;
    let mut fast_forward = Vec::new();
    fast_forward.try_reserve_exact(validated.fast_forward().len())?;
    ff_capacity.resize(fast_forward.capacity())?;
    fast_forward.extend_from_slice(validated.fast_forward());
    let hash = if let Some(outcome) = &outcome {
        if validated.incoming_objects() == 0 {
            None
        } else {
            let hash = outcome.outcome.data_hash.to_string();
            let directory = session.scratch.path.join("receive-output");
            fs::create_dir(&directory)?;
            // Private regular-file writes/flushes are byte/count bounded, with
            // checks between 32 KiB chunks, not instantaneously preemptible OS
            // I/O. A blocked filesystem or cleanup is not a stop acknowledgment.
            for (extension, bytes) in [("pack", &outcome.pack[..]), ("idx", &outcome.index[..])] {
                let mut file =
                    transport::private_file(&directory.join(format!("pack-{hash}.{extension}")))?;
                for chunk in bytes.chunks(32 * 1024) {
                    control.check()?;
                    file.write_all(chunk)?;
                }
                file.flush()?;
            }
            Some(hash)
        }
    } else {
        None
    };
    validated.check()?;
    Ok(ValidatedFiles {
        hash,
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
    publication: Result<u64>,
}

/// The only PUT/SQL path. Consuming the shared runtime's output keeps its actual
/// admission, scratch, source pins and lease until the receipt is consumed/dropped.
/// Store owns staged-before-PUT and metadata owns exact lost-COMMIT confirmation.
pub fn publish(prepared: Completed<Prepared>, store: &Store) -> Receipt {
    let publication = prepared.source.operation.enter(|| {
        let files = prepared
            .validation
            .as_ref()
            .map_err(|_| invalid("receive validation rejected"))?;
        let session = &prepared.source;
        prepared.control.check()?;
        session.operation.check()?;
        session.lease.check()?;
        let mut packs = Vec::new();
        if let Some(hash) = &files.hash {
            packs.push(store.upload(
                &session.snapshot.id,
                &session.operation_id,
                &session.scratch.path.join("receive-output"),
                hash,
            )?);
        }
        session.operation.check()?;
        prepared.control.check()?;
        metadata::publish(
            &session.snapshot,
            &session.lease,
            &session.operation_id,
            &session.principal,
            prepared.commands.updates(),
            &files.fast_forward,
            &packs,
        )
    });
    Receipt {
        prepared,
        publication,
    }
}

impl Receipt {
    pub fn publication(&self) -> std::result::Result<u64, &(dyn std::error::Error + Send + Sync)> {
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
