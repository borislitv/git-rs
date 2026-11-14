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

struct HeaderFind<'a, P> {
    snapshot: &'a SnapshotSession<P>,
    known: &'a Set<ObjectId>,
}
impl<P: VerifiedObjectProvider> gix_object::Find for HeaderFind<'_, P> {
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        buffer.clear();
        let result = (|| -> Result<()> {
            self.snapshot.control.check()?;
            if !self.known.contains(&id.to_owned()) {
                return Err(invalid("topological read escaped the complete inventory"));
            }
            let object = self.snapshot.required(id, Kind::Commit)?;
            let end = metadata_header_end(
                object.data.as_slice(),
                self.snapshot.limits.max_metadata_bytes,
                &self.snapshot.control,
            )?;
            if buffer.capacity() < end {
                return Err(invalid("topological Find buffer was not admitted"));
            }
            // The maintained walker needs only headers. Physical bytes were hash
            // verified first; messages/signatures are not reparsed as graph data.
            for chunk in object.data.as_slice()[..end].chunks(32 * 1024) {
                self.snapshot.control.check()?;
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
        let reachable = self.reachable_from_refs([(b"tip".as_slice(), tip)])?;
        if reachable.is_empty() {
            return Err(invalid("history tip must name a physical commit"));
        }
        let mut known = Set::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut edges = 0usize;
        for id in reachable.iter() {
            known.insert(id, &self.control)?;
            let commit = self.required(&id, Kind::Commit)?;
            let end = metadata_header_end(
                commit.data.as_slice(),
                self.limits.max_metadata_bytes,
                &self.control,
            )?;
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
        }
        let _setup = self.capacity.reservation(4096)?;
        let admission = Arc::new(TopoBudget {
            control: self.control.clone(),
            capacity: self.capacity.clone(),
            reservation: Mutex::new(None),
        });
        let mut walk = gix_traverse::commit::topo::Builder::new(HeaderFind {
            snapshot: self,
            known: &known,
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
