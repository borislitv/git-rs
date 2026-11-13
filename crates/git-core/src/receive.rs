//! Receive command and object-semantic validation. This module does not certify
//! pack framing, HTTP/gzip completion, authorization or publication. The structural
//! ingest adapter must enumerate every physical entry (including duplicate IDs),
//! retain its own validated pack outcome, and join both results before publication.

use crate::objects::{Result, invalid};
use crate::{CapacityBudget, Control, Object, ReadLimits, SnapshotSession, VerifiedObjectProvider};
use gix_hash::{ObjectId, oid};

mod commands;
mod semantics;
mod storage;
mod walk;

pub use commands::{Capabilities, Command, CommandBatch, CommandLimits};
pub use semantics::{Warning, WarningKind};
use storage::ChargedVec;

/// Additional aggregate limits; all work, including repeated ancestry reads, is
/// charged to the same invocation. ReadLimits additionally bounds each object.
#[derive(Clone, Copy, Debug)]
pub struct ReceiveLimits {
    pub max_incoming_objects: usize,
    pub max_decoded_bytes: u64,
    pub max_object_reads: usize,
    pub max_warnings: usize,
}

impl Default for ReceiveLimits {
    fn default() -> Self {
        Self {
            max_incoming_objects: 100_000,
            max_decoded_bytes: 8 * 1024 * 1024 * 1024,
            max_object_reads: 1_000_000,
            max_warnings: 1024,
        }
    }
}

/// Trusted structural adapter, not a client-controlled proof. `object_count`
/// comes from the validated pack header/manifest. `next_object` returns each
/// physical incoming entry, not distinct index lookup winners. Errors must remain
/// errors, never be translated into end-of-input. The receiver rehashes all bytes.
/// No adapter method below establishes transport EOF or structural validation.
pub trait IncomingObjects {
    fn object_count(&self) -> usize;
    fn next_object(
        &mut self,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<(ObjectId, Object)>>;
}

/// Immutable authorized snapshot lookup. SQL must independently recheck old IDs
/// and ACL under its publication lock; this lookup does not grant write authority.
pub trait RefSnapshot {
    fn ref_oid(&self, name: &[u8]) -> Option<ObjectId>;
}

/// The provider owns/borrows the source hold and immutable old+incoming objects.
/// Its identity, budget and request control are checked by SnapshotSession.
pub struct ReceiveSession<P> {
    snapshot: SnapshotSession<P>,
    limits: ReceiveLimits,
}

/// Private-constructor **semantic** result. Retains the exact ordered command
/// batch and source owner. It is deliberately not a pack/transport/ACL witness.
/// There is no public constructor, EOF boolean, or publication method. Root wiring
/// must join this with its independently completed structural/transport outcome.
pub struct ValidatedReceive<P> {
    commands: CommandBatch,
    fast_forward: ChargedVec<bool>,
    warnings: ChargedVec<Warning>,
    incoming_objects: usize,
    _snapshot: SnapshotSession<P>,
}

impl<P> ValidatedReceive<P> {
    pub fn commands(&self) -> &CommandBatch {
        &self.commands
    }
    pub fn fast_forward(&self) -> &[bool] {
        self.fast_forward.as_slice()
    }
    pub fn warnings(&self) -> &[Warning] {
        self.warnings.as_slice()
    }
    pub fn incoming_objects(&self) -> usize {
        self.incoming_objects
    }
    /// Request-lifetime check for root integration before its independent source,
    /// transport and SQL fences. This does not replace those fences.
    pub fn check(&self) -> Result<()> {
        self._snapshot.control.check()?;
        Ok(())
    }
}

impl<P: VerifiedObjectProvider> ReceiveSession<P> {
    pub fn new(snapshot: SnapshotSession<P>, limits: ReceiveLimits) -> Result<Self> {
        snapshot.control.check()?;
        if snapshot.identity().object_hash != gix_hash::Kind::Sha1 {
            return Err(invalid("receive supports SHA-1 repositories only"));
        }
        if limits.max_incoming_objects == 0
            || limits.max_decoded_bytes == 0
            || limits.max_object_reads == 0
            || limits.max_warnings == 0
        {
            return Err(invalid("receive limits must be nonzero"));
        }
        Ok(Self { snapshot, limits })
    }

    /// Validate old IDs, every physical incoming entry, complete typed closure
    /// and branch ancestry. Structural ingest and HTTP completion are separate
    /// mandatory owners; success here alone must never authorize PUT or SQL.
    pub fn run(
        self,
        commands: CommandBatch,
        refs: &impl RefSnapshot,
        incoming: &mut impl IncomingObjects,
    ) -> Result<ValidatedReceive<P>> {
        let snapshot = &self.snapshot;
        snapshot.control.check()?;
        if !commands.budget.shares_with(&snapshot.capacity) {
            return Err(invalid("receive commands require session capacity budget"));
        }
        for command in commands.commands() {
            snapshot.control.check()?;
            let old = refs
                .ref_oid(command.name())
                .unwrap_or_else(|| ObjectId::null(gix_hash::Kind::Sha1));
            if old != command.old() {
                return Err(invalid("stale expected ref"));
            }
        }
        let count = incoming.object_count();
        if count > self.limits.max_incoming_objects {
            return Err(invalid("incoming object count limit exceeded"));
        }
        let mut work = walk::Work::new(snapshot, self.limits)?;
        let mut roots = ChargedVec::new(snapshot.capacity.clone(), count)?;
        for _ in 0..count {
            snapshot.control.check()?;
            let (id, object) = incoming
                .next_object(&snapshot.control, &snapshot.capacity, snapshot.limits)?
                .ok_or_else(|| invalid("incoming object enumeration truncated"))?;
            work.charge(object.data.as_slice().len())?;
            if id.kind() != snapshot.identity().object_hash || id.is_null() {
                return Err(invalid("invalid incoming object identity"));
            }
            let object = crate::objects::VerifiedObject::verify(
                &id,
                object,
                &snapshot.control,
                &snapshot.capacity,
                snapshot.limits,
            )?;
            // Never skip this call on a duplicate ID. Ingest membership must also
            // be established in the combined immutable provider, below.
            work.inspect(&id, object.header().kind, object.as_slice(), |_| Ok(()))?;
            let present = work.read(&id)?;
            if present.header() != object.header() {
                return Err(invalid("incoming object absent from combined snapshot"));
            }
            roots.push(id, &snapshot.control)?;
        }
        if incoming
            .next_object(&snapshot.control, &snapshot.capacity, snapshot.limits)?
            .is_some()
        {
            return Err(invalid("incoming enumeration exceeds declared count"));
        }
        for id in roots.as_slice() {
            work.closure(*id, None)?;
        }
        for command in commands.commands() {
            if !command.new().is_null() {
                let kind = command
                    .name()
                    .starts_with(b"refs/heads/")
                    .then_some(gix_object::Kind::Commit);
                work.closure(command.new(), kind)?;
            }
        }
        let mut fast_forward =
            ChargedVec::new(snapshot.capacity.clone(), commands.commands().len())?;
        for command in commands.commands() {
            let ff = if command.old().is_null() {
                true
            } else if command.new().is_null() {
                false
            } else if command.name().starts_with(b"refs/heads/") {
                work.is_ancestor(command.old(), command.new())?
            } else {
                command.old() == command.new()
            };
            fast_forward.push(ff, &snapshot.control)?;
        }
        snapshot.control.check()?;
        Ok(ValidatedReceive {
            commands,
            fast_forward,
            warnings: work.finish(),
            incoming_objects: count,
            _snapshot: self.snapshot,
        })
    }
}

fn nonnull(id: &oid) -> Result<()> {
    if id.is_null() {
        return Err(invalid("null object graph edge"));
    }
    Ok(())
}

#[cfg(test)]
mod path_control_tests {
    use super::*;
    use crate::{SnapshotIdentity, StopReason};
    use gix_object::{Header, Kind, bstr::ByteSlice};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{Duration, Instant};

    struct Source {
        identity: SnapshotIdentity,
        tree: ObjectId,
        bytes: Vec<u8>,
        blob: ObjectId,
        dropped: Arc<AtomicBool>,
    }

    impl Source {
        fn find(&self, id: &oid) -> Option<(Kind, &[u8])> {
            if id == self.tree {
                Some((Kind::Tree, &self.bytes))
            } else if id == self.blob {
                Some((Kind::Blob, b""))
            } else {
                None
            }
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    impl VerifiedObjectProvider for Source {
        fn identity(&self) -> &SnapshotIdentity {
            &self.identity
        }
        fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
            control.check()?;
            Ok(self.find(id).map(|(kind, bytes)| Header {
                kind,
                size: bytes.len() as u64,
            }))
        }
        fn object(
            &self,
            id: &oid,
            control: &Control,
            budget: &CapacityBudget,
            _: ReadLimits,
        ) -> Result<Option<Object>> {
            control.check()?;
            self.find(id)
                .map(|(kind, bytes)| {
                    let mut data = budget.buffer(bytes.len())?;
                    data.resize(bytes.len(), 0)?;
                    data.as_mut_slice().copy_from_slice(bytes);
                    Ok(Object { kind, data })
                })
                .transpose()
        }
    }

    struct Empty;
    impl RefSnapshot for Empty {
        fn ref_oid(&self, _: &[u8]) -> Option<ObjectId> {
            None
        }
    }
    impl IncomingObjects for Empty {
        fn object_count(&self) -> usize {
            0
        }
        fn next_object(
            &mut self,
            _: &Control,
            _: &CapacityBudget,
            _: ReadLimits,
        ) -> Result<Option<(ObjectId, Object)>> {
            Ok(None)
        }
    }

    fn assert_stop_inside_large_path(reason: StopReason) {
        const NAME_BYTES: usize = 1024 * 1024;
        let mut hfs = [0xe2, 0x80, 0x8c].repeat(NAME_BYTES / 3);
        hfs.extend_from_slice(b"ordinary");
        let mut ntfs = b".git".to_vec();
        ntfs.resize(NAME_BYTES, b' ');
        let options = gix_validate::path::component::Options {
            protect_windows: false,
            protect_hfs: true,
            protect_ntfs: true,
        };
        for (label, name, baseline) in [
            ("separator", vec![b'x'; NAME_BYTES], None),
            (
                "hfs",
                hfs,
                Some(gix_validate::path::component::Options {
                    protect_hfs: false,
                    ..options
                }),
            ),
            (
                "ntfs",
                ntfs,
                Some(gix_validate::path::component::Options {
                    protect_ntfs: false,
                    ..options
                }),
            ),
        ] {
            let stop_at = if let Some(baseline) = baseline {
                // Public maintained baseline disables only the target policy.
                // Its entire separator scan plus fixed decision work cannot
                // reach this stop; the extra cushion excludes outer polls.
                let polls = Cell::new(0usize);
                assert!(
                    gix_validate::path::component_controlled(
                        name.as_bstr(),
                        None,
                        baseline,
                        &|| {
                            polls.set(polls.get() + 1);
                            Ok(())
                        },
                    )
                    .is_ok()
                );
                polls.get() + 8
            } else {
                // An opaque pre/post wrapper has only two checks.
                3
            };
            let blob = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, b"").unwrap();
            let mut bytes = b"100644 ".to_vec();
            bytes.extend_from_slice(&name);
            bytes.push(0);
            bytes.extend_from_slice(blob.as_slice());
            let tree = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Tree, &bytes).unwrap();
            let identity = SnapshotIdentity {
                source: b"controlled-path".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            };
            let dropped = Arc::new(AtomicBool::new(false));
            let source = Source {
                identity: identity.clone(),
                tree,
                bytes,
                blob,
                dropped: dropped.clone(),
            };
            let budget = CapacityBudget::new(32 * 1024 * 1024);
            let control = Control::new(Instant::now() + Duration::from_secs(10));
            let snapshot = SnapshotSession::open(
                source,
                &identity,
                control.clone(),
                budget.clone(),
                ReadLimits::default(),
            )
            .unwrap();
            let session = ReceiveSession::new(snapshot, ReceiveLimits::default()).unwrap();
            let mut wire = Vec::new();
            gix_packetline::blocking_io::encode::data_to_write(
                format!(
                    "{} {tree} refs/tags/large\0",
                    ObjectId::null(gix_hash::Kind::Sha1)
                )
                .as_bytes(),
                &mut wire,
            )
            .unwrap();
            gix_packetline::blocking_io::encode::flush_to_write(&mut wire).unwrap();
            let batch = CommandBatch::read(
                &mut std::io::Cursor::new(wire),
                &control,
                budget.clone(),
                CommandLimits::default(),
            )
            .unwrap();
            let polls = Rc::new(Cell::new(0usize));
            let observed = polls.clone();
            let request = control.clone();
            assert!(!dropped.load(Ordering::Acquire));
            let result = semantics::with_path_checkpoint(
                move || {
                    observed.set(observed.get() + 1);
                    if observed.get() == stop_at {
                        match reason {
                            StopReason::Cancelled => request.cancel(),
                            StopReason::Deadline => {
                                // Wait for this actual request's absolute deadline at
                                // the inner checkpoint, not via a racing timer thread.
                                while !request.remaining().is_zero() {
                                    std::thread::park_timeout(request.remaining());
                                }
                            }
                        }
                    }
                },
                || session.run(batch, &Empty, &mut Empty),
            );
            let error = result
                .err()
                .expect("an inner stop must not yield ValidatedReceive");
            assert_eq!(
                polls.get(),
                stop_at,
                "{label}: stop must occur inside the maintained scan"
            );
            let context = error
                .downcast_ref::<gix_error::Error>()
                .expect("maintained path context must survive the core error boundary");
            assert_eq!(
                context.downcast_any_ref::<StopReason>(),
                Some(&reason),
                "{label}: original request stop cause"
            );
            assert!(
                context
                    .downcast_any_ref::<gix_validate::path::component::Error>()
                    .is_none(),
                "{label}: a stop is not a fabricated filename validation error"
            );
            assert!(
                dropped.load(Ordering::Acquire),
                "{label}: release source owner"
            );
            assert_eq!(
                budget.used(),
                0,
                "{label}: release batch, graph and object capacity"
            );
        }
    }

    #[test]
    fn cancellation_inside_oversized_names_returns_no_receive_and_releases_owners() {
        assert_stop_inside_large_path(StopReason::Cancelled);
    }

    #[test]
    fn deadline_inside_oversized_names_preserves_typed_cause_and_releases_owners() {
        assert_stop_inside_large_path(StopReason::Deadline);
    }
}
