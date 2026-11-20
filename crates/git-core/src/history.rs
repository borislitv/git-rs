//! Physical, maintained topological traversal and byte-preserving commit rewrite.

use crate::{
    CapacityBudget, CapacityBuffer, CapacityReservation, Control, MAX_REF_BYTES, MAX_REFS, Object,
    SnapshotIdentity, SnapshotSession, VerifiedObjectProvider,
    objects::{MAX_METADATA_HEADER_BYTES, Result, invalid, metadata_header_end},
    storage::{ChargedVec, Map, Set},
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind, commit::ref_iter::Token};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};

pub struct History {
    ids: ChargedVec<ObjectId>,
}
impl History {
    pub fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.ids.as_slice().iter().copied()
    }
    pub fn len(&self) -> usize {
        self.ids.as_slice().len()
    }
    pub fn is_empty(&self) -> bool {
        self.ids.as_slice().is_empty()
    }
}

struct TopoBudget {
    control: Control,
    capacity: CapacityBudget,
    reservation: Mutex<Option<CapacityReservation>>,
}
impl gix_traverse::commit::topo::Budget for TopoBudget {
    fn check(&self) -> gix_error::Result {
        check(&self.control)
    }
    fn reserve(&self, bytes: usize) -> gix_error::Result {
        let mut reserved = self
            .reservation
            .lock()
            .map_err(|_| gix_error::Error::from_boxed(invalid("topological admission poisoned")))?;
        if reserved.is_some() {
            return Err(gix_error::Error::from_boxed(invalid(
                "duplicate topological admission",
            )));
        }
        *reserved = Some(
            self.capacity
                .reservation(bytes)
                .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))?,
        );
        Ok(())
    }
    fn release(&self, _: usize) {
        self.reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

struct HeaderFind<'a> {
    control: &'a Control,
    known: &'a Map<ObjectId, usize>,
    headers: &'a [CapacityBuffer],
}

struct CommitHeaders<'a> {
    values: ChargedVec<CapacityBuffer>,
    control: &'a Control,
}

impl Drop for CommitHeaders<'_> {
    fn drop(&mut self) {
        // Headers own separate allocations. Finish their cleanup even after a
        // stopped traversal, polling between bounded batches of destructions.
        while !self.values.as_slice().is_empty() {
            for _ in 0..32 {
                drop(self.values.pop());
            }
            #[cfg(test)]
            self.control
                .observe(crate::control::TestCheckpoint::HistoryHeaderCleanup {
                    remaining: self.values.as_slice().len(),
                });
            let _ = self.control.check();
        }
    }
}

impl gix_object::Find for HeaderFind<'_> {
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        buffer.clear();
        let result = (|| -> Result<()> {
            self.control.check()?;
            let position = self
                .known
                .get(&id.to_owned())
                .ok_or_else(|| invalid("topological read escaped the complete inventory"))?;
            let header = self.headers[*position].as_slice();
            if buffer.capacity() < header.len() {
                return Err(invalid("topological Find buffer was not admitted"));
            }
            // Reuse the exact header copied from this invocation's hash-verified
            // physical commit. Keep its admitted owner until the walker drops;
            // repeated topology lookups must not decode the whole object again.
            for chunk in header.chunks(32 * 1024) {
                self.control.check()?;
                buffer.extend_from_slice(chunk);
            }
            Ok(())
        })();
        result.map_err(gix_error::Error::from_boxed)?;
        Ok(Some(gix_object::Data::new(buffer, Kind::Commit, id.kind())))
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Native-style topological order, not breadth-first or timestamp-pruned
    /// ancestry. Preflight exhaustively counts all physical nodes and edges.
    pub fn history(&self, tip: ObjectId, max: usize) -> Result<History> {
        if max == 0 || max > self.limits.max_graph_objects {
            return Err(invalid("history count limit exceeded"));
        }
        let mut known = Map::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut headers = CommitHeaders {
            values: ChargedVec::new(self.capacity.clone(), self.limits.max_graph_objects)?,
            control: &self.control,
        };
        let mut edges = 0usize;
        let reachable = self.visit_commits(tip, |id, commit| {
            let end = metadata_header_end(
                commit.data.as_slice(),
                self.limits.max_metadata_bytes,
                &self.control,
            )?;
            let mut header = self.capacity.buffer(end)?;
            header.extend_controlled(&commit.data.as_slice()[..end], end, &self.control)?;
            known.insert(id, headers.values.as_slice().len(), &self.control)?;
            headers.values.push(header, &self.control)?;
            for token in
                gix_object::CommitRefIter::from_bytes(&commit.data.as_slice()[..end], id.kind())
            {
                self.control.check()?;
                match token? {
                    Token::Parent { .. } => {
                        edges = edges
                            .checked_add(1)
                            .filter(|edges| *edges <= self.limits.max_graph_edges)
                            .ok_or_else(|| invalid("history edge limit exceeded"))?
                    }
                    Token::Author { .. } => break,
                    _ => {}
                }
            }
            Ok(())
        })?;
        let _setup = self.capacity.reservation(4096)?;
        let admission = Arc::new(TopoBudget {
            control: self.control.clone(),
            capacity: self.capacity.clone(),
            reservation: Mutex::new(None),
        });
        let mut walk = gix_traverse::commit::topo::Builder::new(HeaderFind {
            control: &self.control,
            known: &known,
            headers: headers.values.as_slice(),
        })
        .with_tips([tip])
        .sorting(gix_traverse::commit::topo::Sorting::TopoOrder)
        .with_admission(
            admission,
            gix_traverse::commit::topo::Limits {
                commits: reachable.len(),
                edges,
                buffer_bytes: MAX_METADATA_HEADER_BYTES,
            },
        )?
        .build_admitted()?;
        let mut ids = ChargedVec::new(self.capacity.clone(), max)?;
        for _ in 0..max {
            self.control.check()?;
            let Some(info) = walk.next() else {
                break;
            };
            ids.push(info?.id, &self.control)?;
        }
        drop(walk);
        self.control.check()?;
        Ok(History { ids })
    }

    /// Rewrite each branch independently with the existing topological keep
    /// policy. Raw non-parent headers and messages survive byte-for-byte; invalid
    /// signatures/mergetags and their continuation lines are removed.
    pub fn rewrite_history<'a>(
        self,
        branches: impl IntoIterator<Item = (&'a [u8], ObjectId)>,
        keep: usize,
        nonce: &[u8],
    ) -> Result<RewrittenHistory<P>> {
        if !(1..=10_000).contains(&keep) || nonce.is_empty() || nonce.len() > 128 {
            return Err(invalid("invalid history rewrite bounds or identity"));
        }
        let mut objects = ChargedVec::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut lookup = Map::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut refs = ChargedVec::new(self.capacity.clone(), MAX_REFS)?;
        for (name, tip) in branches {
            self.control.check()?;
            if !name.starts_with(b"refs/heads/") || name.len() > MAX_REF_BYTES {
                return Err(invalid("history rewrite requires branch refs"));
            }
            gix_validate::reference::name(name.into())?;
            let history = self.history(tip, keep)?;
            let mut retained = Set::new(self.capacity.clone(), keep)?;
            for id in history.iter() {
                retained.insert(id, &self.control)?;
            }
            let mut rewritten = Map::new(self.capacity.clone(), keep)?;
            let mut metadata_bytes = 0usize;
            for id in history.ids.as_slice().iter().rev() {
                self.control.check()?;
                let old = self.required(id, Kind::Commit)?;
                metadata_bytes = metadata_bytes
                    .checked_add(old.data.as_slice().len())
                    .filter(|bytes| *bytes <= 64 * 1024 * 1024)
                    .ok_or_else(|| invalid("retained commit metadata exceeds 64 MiB budget"))?;
                let bytes = rewrite_commit(&self, &old, &retained, &rewritten)?;
                let mut hash = gix_hash::hasher(id.kind());
                hash.update(&gix_object::encode::loose_header(
                    Kind::Commit,
                    bytes.as_slice().len() as u64,
                ));
                for chunk in bytes.as_slice().chunks(32 * 1024) {
                    self.control.check()?;
                    hash.update(chunk);
                }
                let new = hash.try_finalize()?;
                rewritten.insert(*id, new, &self.control)?;
                if lookup.get(&new).is_none() {
                    let position = objects.as_slice().len();
                    objects.push(NewCommit { id: new, bytes }, &self.control)?;
                    lookup.insert(new, position, &self.control)?;
                }
            }
            let new = *rewritten
                .get(&tip)
                .ok_or_else(|| invalid("rewritten tip is missing"))?;
            let mut bytes = self.capacity.buffer(name.len())?;
            bytes.extend_controlled(name, name.len(), &self.control)?;
            refs.push((bytes, new), &self.control)?;
        }
        if refs.as_slice().is_empty() {
            return Err(invalid("truncate requires a branch"));
        }
        let mut identity = self.identity().clone();
        identity.generation.extend_from_slice(b"/rewrite/");
        identity.generation.extend_from_slice(nonce);
        let provider = RewrittenProvider {
            objects,
            lookup,
            identity: identity.clone(),
            base: self.provider,
        };
        let snapshot = SnapshotSession::open(
            provider,
            &identity,
            self.control,
            self.capacity,
            self.limits,
        )?;
        Ok(RewrittenHistory { refs, snapshot })
    }
}

fn rewrite_commit<P: VerifiedObjectProvider>(
    snapshot: &SnapshotSession<P>,
    old: &Object,
    retained: &Set<ObjectId>,
    rewritten: &Map<ObjectId, ObjectId>,
) -> Result<CapacityBuffer> {
    let bytes = old.data.as_slice();
    let end = metadata_header_end(bytes, snapshot.limits.max_metadata_bytes, &snapshot.control)?;
    if end < 2 || &bytes[end - 2..end] != b"\n\n" {
        return Err(invalid(
            "commit rewrite requires a complete header separator",
        ));
    }
    let mut parents = ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_edges)?;
    for token in
        gix_object::CommitRefIter::from_bytes(&bytes[..end], snapshot.identity().object_hash)
    {
        snapshot.control.check()?;
        match token? {
            Token::Parent { id } if retained.contains(&id) => parents.push(
                *rewritten.get(&id).ok_or_else(|| {
                    invalid("topological rewrite encountered an unwritten retained parent")
                })?,
                &snapshot.control,
            )?,
            Token::Author { .. } => break,
            _ => {}
        }
    }
    let mut output = snapshot.capacity.buffer(0)?;
    let limit = snapshot.limits.max_object_bytes;
    let mut skip = false;
    for line in bytes[..end - 1].split_inclusive(|byte| *byte == b'\n') {
        snapshot.control.check()?;
        if !line.starts_with(b" ") {
            skip = [
                b"parent ".as_slice(),
                b"gpgsig ",
                b"gpgsig-sha256 ",
                b"mergetag ",
            ]
            .iter()
            .any(|prefix| line.starts_with(prefix));
        }
        if skip {
            continue;
        }
        output.extend_controlled(line, limit, &snapshot.control)?;
        if line.starts_with(b"tree ") {
            for id in parents.as_slice() {
                let mut bytes = [0u8; 80];
                let mut line = std::io::Cursor::new(bytes.as_mut_slice());
                writeln!(line, "parent {id}")?;
                let len = line.position() as usize;
                output.extend_controlled(&bytes[..len], limit, &snapshot.control)?;
            }
        }
    }
    output.extend_controlled(b"\n", limit, &snapshot.control)?;
    output.extend_controlled(&bytes[end..], limit, &snapshot.control)?;
    Ok(output)
}

struct NewCommit {
    id: ObjectId,
    bytes: CapacityBuffer,
}

pub struct RewrittenHistory<P> {
    refs: ChargedVec<(CapacityBuffer, ObjectId)>,
    snapshot: SnapshotSession<RewrittenProvider<P>>,
}
impl<P: VerifiedObjectProvider> RewrittenHistory<P> {
    pub fn refs(&self) -> impl Iterator<Item = (&[u8], ObjectId)> {
        self.refs
            .as_slice()
            .iter()
            .map(|(name, id)| (name.as_slice(), *id))
    }
    pub fn snapshot(&self) -> &SnapshotSession<RewrittenProvider<P>> {
        &self.snapshot
    }
}

/// Private generated commit bytes overlay the pinned physical source. This is
/// available only through rewrite_history, with a distinct derivation identity.
pub struct RewrittenProvider<P> {
    objects: ChargedVec<NewCommit>,
    lookup: Map<ObjectId, usize>,
    identity: SnapshotIdentity,
    base: P,
}

impl<P: VerifiedObjectProvider> VerifiedObjectProvider for RewrittenProvider<P> {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
        if let Some(position) = self.lookup.get(&id.to_owned()) {
            return Ok(Some(Header {
                kind: Kind::Commit,
                size: self.objects.as_slice()[*position].bytes.as_slice().len() as u64,
            }));
        }
        self.base.header(id, control)
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: crate::ReadLimits,
    ) -> Result<Option<Object>> {
        if let Some(position) = self.lookup.get(&id.to_owned()) {
            let object = &self.objects.as_slice()[*position];
            if object.id.as_ref() != id
                || object.bytes.as_slice().len() > limits.max_object_bytes
                || !object.bytes.shares_budget(budget)
            {
                return Err(invalid(
                    "rewritten commit identity, limit or budget mismatch",
                ));
            }
            let mut bytes = budget.buffer(object.bytes.as_slice().len())?;
            bytes.extend_controlled(object.bytes.as_slice(), limits.max_object_bytes, control)?;
            return Ok(Some(Object {
                kind: Kind::Commit,
                data: bytes,
            }));
        }
        self.base.object(id, control, budget, limits)
    }
    fn packed_object(
        &self,
        id: &oid,
        control: &Control,
    ) -> Result<Option<crate::pack::ranges::PackedObject>> {
        if self.lookup.get(&id.to_owned()).is_some() {
            return Ok(None);
        }
        self.base.packed_object(id, control)
    }
}

fn check(control: &Control) -> gix_error::Result {
    control
        .check()
        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ReadLimits, StopReason,
        control::{TestCheckpoint, TestObserver},
    };
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };

    struct Source {
        identity: SnapshotIdentity,
        commits: BTreeMap<ObjectId, Vec<u8>>,
    }

    impl VerifiedObjectProvider for Source {
        fn identity(&self) -> &SnapshotIdentity {
            &self.identity
        }

        fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
            control.check()?;
            Ok(self.commits.get(id).map(|bytes| Header {
                kind: Kind::Commit,
                size: bytes.len() as u64,
            }))
        }

        fn object(
            &self,
            id: &oid,
            control: &Control,
            budget: &CapacityBudget,
            limits: ReadLimits,
        ) -> Result<Option<Object>> {
            let Some(bytes) = self.commits.get(id) else {
                return Ok(None);
            };
            let mut data = budget.buffer(bytes.len())?;
            data.extend_controlled(bytes, limits.max_object_bytes, control)?;
            Ok(Some(Object {
                kind: Kind::Commit,
                data,
            }))
        }
    }

    #[derive(Debug, Default)]
    struct Observations {
        cancelled: bool,
        moved_after_cancellation: usize,
        pending_cleanup: Option<usize>,
        cleanup_checks: Vec<(usize, usize)>,
    }

    #[derive(Debug)]
    struct CancelRelocation {
        stop: Control,
        budget: CapacityBudget,
        observations: Mutex<Observations>,
    }

    impl TestObserver for CancelRelocation {
        fn observe(&self, checkpoint: TestCheckpoint) {
            let mut state = self
                .observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match checkpoint {
                TestCheckpoint::VectorRelocation { moved, total: 64 } => {
                    // The first owner has moved; the next real Control::check
                    // must stop inside this populated relocation, not in I/O.
                    if moved == 1 {
                        state.cancelled = true;
                        self.stop.cancel();
                    }
                    state.moved_after_cancellation = moved;
                }
                TestCheckpoint::HistoryHeaderCleanup { remaining } => {
                    state.pending_cleanup = Some(remaining);
                }
                TestCheckpoint::Check => {
                    if let Some(remaining) = state.pending_cleanup.take() {
                        state.cleanup_checks.push((remaining, self.budget.used()));
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn history_cancellation_during_header_relocation_preserves_polled_cleanup() -> Result<()> {
        let mut source = Source {
            identity: SnapshotIdentity {
                source: b"history-relocation-cleanup".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            },
            commits: BTreeMap::new(),
        };
        let mut parent = None;
        // The 65th retained header grows the already populated 64-owner vector.
        for _ in 0..65 {
            let mut bytes = b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n".to_vec();
            if let Some(id) = parent {
                bytes.extend_from_slice(format!("parent {id}\n").as_bytes());
            }
            bytes.extend_from_slice(b"author A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\nsynthetic\n");
            let id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Commit, &bytes)?;
            source.commits.insert(id, bytes);
            parent = Some(id);
        }
        let tip = parent.expect("65 physical commits");
        let budget = CapacityBudget::new(4 * 1024 * 1024);
        let control = Control::new(Instant::now() + Duration::from_secs(30));
        let observer = Arc::new(CancelRelocation {
            stop: control.clone(), // This clone has no observer: no callback cycle.
            budget: budget.clone(),
            observations: Mutex::new(Observations::default()),
        });
        let identity = source.identity.clone();
        let session = SnapshotSession::open(
            source,
            &identity,
            control.with_observer(observer.clone()),
            budget.clone(),
            ReadLimits::default(),
        )?;
        let error = match session.history(tip, 1) {
            Ok(_) => panic!("relocation cancellation must reject even a one-commit prefix"),
            Err(error) => error,
        };
        assert_eq!(
            error.downcast_ref::<StopReason>(),
            Some(&StopReason::Cancelled)
        );
        let state = observer.observations.lock().expect("observation lock");
        assert!(
            state.cancelled,
            "cancellation reached the populated relocation"
        );
        assert_eq!(
            state
                .cleanup_checks
                .iter()
                .map(|(remaining, _)| *remaining)
                .collect::<Vec<_>>(),
            vec![32, 0],
            "the 64 retained headers must reach two real cleanup checks, not disappear with an interrupted drain"
        );
        assert_eq!(
            state.moved_after_cancellation, 64,
            "finish moving owners before propagating the stop"
        );
        assert!(
            state.cleanup_checks[0].1 > state.cleanup_checks[1].1,
            "admitted header backing is released between cleanup checks"
        );
        assert!(
            state.cleanup_checks[1].1 > 0,
            "outer header storage stays admitted through the last cleanup check"
        );
        drop(state);
        drop(session);
        assert_eq!(
            budget.used(),
            0,
            "all request backing is released after cleanup"
        );
        Ok(())
    }
}
