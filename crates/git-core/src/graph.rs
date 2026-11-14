//! Exhaustive parent traversal, deliberately independent of timestamps.
//! Worklists and results own charged byte buffers, not logical cache weights.

use crate::executor::CapacityReservation;
use gix_hash::{ObjectId, oid};
use gix_object::{Kind, commit::ref_iter::Token};
use hashbrown::HashTable;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use crate::CapacityBuffer;
use crate::objects::{Result, SnapshotSession, VerifiedObjectProvider, invalid};

/// Complete reachable commit IDs in deterministic traversal order (for a fixed
/// input ref order). This is neither timestamp nor topological sorting. Keeping
/// this result alive retains its full backing-capacity reservation.
pub struct Reachable {
    ids: IdList,
}

impl Reachable {
    pub fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.ids
            .bytes
            .as_slice()
            .chunks_exact(self.ids.width)
            .map(ObjectId::from_bytes_or_panic)
    }
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Worklist/output storage is flat; membership uses a separately admitted
// maintained hash table, not an O(history^2) scan of this list.
struct IdList {
    bytes: CapacityBuffer,
    width: usize,
    max: usize,
}

struct Seen {
    table: HashTable<ObjectId>,
    hash: RandomState,
    _capacity: CapacityReservation,
    admitted_entries: usize,
}

impl Seen {
    fn new<P: VerifiedObjectProvider>(session: &SnapshotSession<P>) -> Result<Self> {
        Self::allocate(session, session.limits.max_graph_objects.min(16))
    }
    fn allocate<P: VerifiedObjectProvider>(
        session: &SnapshotSession<P>,
        max: usize,
    ) -> Result<Self> {
        session.control.check()?;
        // Hashbrown uses a power-of-two bucket count with <= 7/8 occupancy.
        // Twice the requested capacity plus the control-group tail is a
        // conservative pre-admission bound; reconcile with allocation_size().
        let buckets = max
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| invalid("graph hash allocation overflow"))?;
        let upper = buckets
            .checked_mul(std::mem::size_of::<ObjectId>() + 1)
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| invalid("graph hash allocation overflow"))?;
        let mut reservation = session.capacity.reservation(upper)?;
        let hash = RandomState::new();
        let mut table = HashTable::new();
        table
            .try_reserve(max, |id| hash.hash_one(id))
            .map_err(|_| invalid("graph hash allocation failed"))?;
        reservation.resize(table.allocation_size())?;
        session.control.check()?;
        Ok(Self {
            table,
            hash,
            _capacity: reservation,
            admitted_entries: max,
        })
    }
    fn contains(&self, id: &ObjectId) -> bool {
        self.table
            .find(self.hash.hash_one(id), |other| other == id)
            .is_some()
    }
    fn insert<P: VerifiedObjectProvider>(
        &mut self,
        id: ObjectId,
        session: &SnapshotSession<P>,
    ) -> Result<()> {
        if self.table.len() >= self.admitted_entries {
            let next = self
                .admitted_entries
                .saturating_mul(2)
                .min(session.limits.max_graph_objects);
            if next <= self.table.len() {
                return Err(invalid("graph object limit exceeded"));
            }
            let mut replacement = Self::allocate(session, next)?;
            // Rebuild explicitly instead of allowing HashTable to rehash the
            // complete history inside an uninterruptible reserve operation.
            for old in self.table.iter() {
                session.control.check()?;
                replacement
                    .table
                    .insert_unique(replacement.hash.hash_one(old), *old, |id| {
                        replacement.hash.hash_one(id)
                    });
            }
            std::mem::swap(self, &mut replacement);
            // Old table is freed before its capacity reservation is released.
        }
        session.control.check()?;
        self.table
            .insert_unique(self.hash.hash_one(id), id, |id| self.hash.hash_one(id));
        Ok(())
    }
}

impl IdList {
    fn new<P: VerifiedObjectProvider>(session: &SnapshotSession<P>, max: usize) -> Result<Self> {
        Ok(Self {
            bytes: session.capacity.buffer(0)?,
            width: session.identity().object_hash.len_in_bytes(),
            max,
        })
    }
    fn len(&self) -> usize {
        self.bytes.as_slice().len() / self.width
    }
    fn push<P: VerifiedObjectProvider>(
        &mut self,
        id: &oid,
        session: &SnapshotSession<P>,
    ) -> Result<()> {
        session.control.check()?;
        if self.len() >= self.max {
            return Err(invalid("graph object/edge limit exceeded"));
        }
        let length = self.bytes.as_slice().len();
        let needed = length
            .checked_add(self.width)
            .ok_or_else(|| invalid("graph capacity overflow"))?;
        if needed > self.bytes.capacity() {
            let entries = self.len().saturating_mul(2).max(16).min(self.max);
            let capacity = entries
                .checked_mul(self.width)
                .ok_or_else(|| invalid("graph capacity overflow"))?;
            let mut replacement = session.capacity.buffer(capacity)?;
            for from in self.bytes.as_slice().chunks(64 * 1024) {
                session.control.check()?;
                let start = replacement.as_slice().len();
                replacement.resize(start + from.len(), 0)?;
                replacement.as_mut_slice()[start..].copy_from_slice(from);
            }
            self.bytes = replacement;
        }
        self.bytes.resize(needed, 0)?;
        self.bytes.as_mut_slice()[length..needed].copy_from_slice(id.as_bytes());
        Ok(())
    }
    fn pop(&mut self) -> Option<ObjectId> {
        let length = self.bytes.as_slice().len();
        if length == 0 {
            return None;
        }
        let next = length - self.width;
        let id = ObjectId::from_bytes_or_panic(&self.bytes.as_slice()[next..]);
        self.bytes
            .resize(next, 0)
            .expect("shrinking a capacity buffer");
        Some(id)
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Inclusive ancestry. Even equal IDs must name a physical commit. A positive
    /// match does not hide missing/corrupt other parents: the walk completes.
    pub fn is_ancestor(&self, ancestor: &oid, descendant: &oid) -> Result<bool> {
        self.required(ancestor, Kind::Commit)?;
        let mut tips = IdList::new(self, self.limits.max_graph_edges)?;
        tips.push(descendant, self)?;
        self.walk_commits(tips, Some(ancestor))
            .map(|(_, found)| found)
    }

    /// All commits reachable from byte-named ref tips, peeling annotated tags.
    /// Only commits are returned, not a full tag/tree/blob or pack-object closure.
    /// Direct tree/blob refs are valid but add no commits. Each ref's tag chain
    /// is independently bounded by `max_graph_objects`, not the pack delta limit.
    /// Ref visibility and authorization belong to the caller's pinned snapshot.
    pub fn reachable_from_refs<'a>(
        &self,
        refs: impl IntoIterator<Item = (&'a [u8], ObjectId)>,
    ) -> Result<Reachable> {
        let mut tips = IdList::new(self, self.limits.max_graph_objects)?;
        let mut refs_seen = 0usize;
        for (_, mut id) in refs {
            self.control.check()?;
            refs_seen = refs_seen
                .checked_add(1)
                .ok_or_else(|| invalid("ref count limit exceeded"))?;
            if refs_seen > self.limits.max_graph_objects {
                return Err(invalid("ref count limit exceeded"));
            }
            let mut tags = Seen::new(self)?;
            loop {
                self.control.check()?;
                let header = self
                    .header(&id)?
                    .ok_or_else(|| invalid("missing ref target"))?;
                if header.kind == Kind::Commit {
                    tips.push(&id, self)?;
                    break;
                }
                // Terminal trees/blobs add no commits. Their physical header
                // establishes existence/kind without applying a body-read limit
                // intended for objects that actually participate in this walk.
                if header.kind != Kind::Tag {
                    break;
                }
                if tags.contains(&id) {
                    return Err(invalid("tag cycle"));
                }
                tags.insert(id, self)?;
                let object = self.required(&id, Kind::Tag)?;
                let header_end = crate::objects::metadata_header_end(
                    object.data.as_slice(),
                    self.limits.max_metadata_bytes,
                    &self.control,
                )?;
                // Peeling uses the validated header only. Parsing the opaque
                // message/signature would scan a large body without control.
                let tag = gix_object::TagRef::from_bytes(
                    &object.data.as_slice()[..header_end],
                    id.kind(),
                )?;
                let target = tag.target();
                let header = self
                    .header(&target)?
                    .ok_or_else(|| invalid("missing tag target"))?;
                if header.kind != tag.target_kind {
                    return Err(invalid("tag target type mismatch"));
                }
                id = target;
            }
        }
        self.walk_commits(tips, None).map(|(ids, _)| ids)
    }

    fn walk_commits(&self, mut pending: IdList, target: Option<&oid>) -> Result<(Reachable, bool)> {
        let mut seen = IdList::new(self, self.limits.max_graph_objects)?;
        let mut membership = Seen::new(self)?;
        let mut edges = 0usize;
        let mut found = false;
        while let Some(id) = pending.pop() {
            self.control.check()?;
            if membership.contains(&id) {
                continue;
            }
            let object = self.required(&id, Kind::Commit)?;
            seen.push(&id, self)?;
            membership.insert(id, self)?;
            found |= target.is_some_and(|target| id.as_ref() == target);
            // parent_ids() deliberately suppresses parse errors, so use tokens.
            for token in gix_object::CommitRefIter::from_bytes(object.data.as_slice(), id.kind()) {
                self.control.check()?;
                match token? {
                    Token::Parent { id } => {
                        edges = edges
                            .checked_add(1)
                            .ok_or_else(|| invalid("graph edge limit exceeded"))?;
                        if edges > self.limits.max_graph_edges {
                            return Err(invalid("graph edge limit exceeded"));
                        }
                        pending.push(&id, self)?;
                    }
                    // Parents form the contiguous header segment before author.
                    // Full strict commit validation belongs to receive, not graph
                    // walking. Avoid allocating unfolded signature/extra headers.
                    Token::Author { .. } => break,
                    _ => {}
                }
            }
        }
        self.control.check()?;
        Ok((Reachable { ids: seen }, found))
    }
}
