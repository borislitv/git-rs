//! Complete typed object selection for fetch, backup and catalog compaction.
//! Commit ordering for history truncation is a separate topological contract.

use crate::{
    SnapshotSession, VerifiedObjectProvider,
    objects::{Result, invalid, metadata_header_end},
    storage::{ChargedVec, Set},
};
use gix_hash::ObjectId;
use gix_object::{Kind, commit::ref_iter::Token};

pub struct ObjectSelection {
    ids: ChargedVec<ObjectId>,
    membership: Set<ObjectId>,
    identity: crate::SnapshotIdentity,
}

impl ObjectSelection {
    pub fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.ids.as_slice().iter().copied()
    }
    pub fn len(&self) -> usize {
        self.ids.as_slice().len()
    }
    pub fn is_empty(&self) -> bool {
        self.ids.as_slice().is_empty()
    }
    pub fn contains(&self, id: &ObjectId) -> bool {
        self.membership.contains(id)
    }
    pub fn identity(&self) -> &crate::SnapshotIdentity {
        &self.identity
    }

    /// An explicit physical catalog inventory. Unlike a ref closure this must
    /// preserve unreachable objects; it cannot be used as want authorization.
    pub fn catalog<P: VerifiedObjectProvider>(
        session: &SnapshotSession<P>,
        ids: impl IntoIterator<Item = ObjectId>,
    ) -> Result<Self> {
        let mut selection = Self::empty(session)?;
        for id in ids {
            selection.insert(session, id)?;
        }
        Ok(selection)
    }

    fn empty<P: VerifiedObjectProvider>(session: &SnapshotSession<P>) -> Result<Self> {
        Ok(Self {
            ids: ChargedVec::new(session.capacity.clone(), session.limits.max_graph_objects)?,
            membership: Set::new(session.capacity.clone(), session.limits.max_graph_objects)?,
            identity: session.identity().clone(),
        })
    }
    fn insert<P: VerifiedObjectProvider>(
        &mut self,
        session: &SnapshotSession<P>,
        id: ObjectId,
    ) -> Result<()> {
        session.control.check()?;
        if id.kind() != session.identity().object_hash || id.is_null() {
            return Err(invalid("selection object identity is invalid"));
        }
        if self.membership.insert(id, &session.control)? {
            self.ids.push(id, &session.control)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Frame {
    Enter {
        id: ObjectId,
        expected: Option<Kind>,
    },
    Leave(ObjectId),
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Select roots, all typed tag/tree/blob edges and all commit parents except
    /// explicit shallow boundaries. Excluded objects must come from a separately
    /// established common-history closure. No timestamps prune ancestry.
    pub fn select_objects(
        &self,
        roots: impl IntoIterator<Item = ObjectId>,
        excluded: Option<&ObjectSelection>,
        shallow: &[ObjectId],
    ) -> Result<ObjectSelection> {
        self.control.check()?;
        if excluded.is_some_and(|selection| selection.identity() != self.identity()) {
            return Err(invalid("excluded closure belongs to a different snapshot"));
        }
        let mut selection = ObjectSelection::empty(self)?;
        let mut complete = Set::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut active = Set::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let mut boundaries = Set::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        for id in shallow {
            boundaries.insert(*id, &self.control)?;
        }
        let mut pending = ChargedVec::new(
            self.capacity.clone(),
            self.limits
                .max_graph_edges
                .checked_add(self.limits.max_graph_objects)
                .ok_or_else(|| invalid("selection worklist size overflow"))?,
        )?;
        for id in roots {
            pending.push(Frame::Enter { id, expected: None }, &self.control)?;
        }
        let mut edges = 0usize;
        while let Some(frame) = pending.pop() {
            self.control.check()?;
            let (id, expected) = match frame {
                Frame::Leave(id) => {
                    active.remove(&id);
                    complete.insert(id, &self.control)?;
                    continue;
                }
                Frame::Enter { id, expected } => (id, expected),
            };
            let header = self
                .header(&id)?
                .ok_or_else(|| invalid("missing selected physical object"))?;
            if expected.is_some_and(|expected| header.kind != expected) {
                return Err(invalid("selected graph edge type mismatch"));
            }
            if complete.contains(&id) {
                continue;
            }
            let omitted = excluded.is_some_and(|selection| selection.contains(&id));
            // A client's common closure can end at a shallow commit. Omitting
            // that commit must not prune newly requested parents during deepen.
            // Trees/blobs have no such shallow edge and are complete subgraphs.
            if omitted && matches!(header.kind, Kind::Tree | Kind::Blob) {
                complete.insert(id, &self.control)?;
                continue;
            }
            if active.contains(&id) {
                return Err(invalid("selected object graph cycle"));
            }
            active.insert(id, &self.control)?;
            if !omitted {
                selection.insert(self, id)?;
            }
            pending.push(Frame::Leave(id), &self.control)?;
            let mut child = |target: ObjectId, kind: Kind| -> Result<()> {
                edges = edges
                    .checked_add(1)
                    .filter(|edges| *edges <= self.limits.max_graph_edges)
                    .ok_or_else(|| invalid("selected graph edge limit exceeded"))?;
                pending.push(
                    Frame::Enter {
                        id: target,
                        expected: Some(kind),
                    },
                    &self.control,
                )
            };
            match header.kind {
                Kind::Blob => {}
                Kind::Tree => {
                    let tree = self.required(&id, Kind::Tree)?;
                    let mut entries =
                        gix_object::TreeRefIter::from_bytes(tree.data.as_slice(), id.kind());
                    while let Some(entry) = entries.next_controlled(&|| check(&self.control))? {
                        if entry.mode.is_commit() {
                            continue;
                        }
                        child(
                            entry.oid.to_owned(),
                            if entry.mode.is_tree() {
                                Kind::Tree
                            } else {
                                Kind::Blob
                            },
                        )?;
                    }
                }
                Kind::Commit => {
                    let commit = self.required(&id, Kind::Commit)?;
                    let end = metadata_header_end(
                        commit.data.as_slice(),
                        self.limits.max_metadata_bytes,
                        &self.control,
                    )?;
                    for token in gix_object::CommitRefIter::from_bytes(
                        &commit.data.as_slice()[..end],
                        id.kind(),
                    ) {
                        self.control.check()?;
                        match token? {
                            Token::Tree { id } => child(id, Kind::Tree)?,
                            Token::Parent { id: parent } if !boundaries.contains(&id) => {
                                child(parent, Kind::Commit)?
                            }
                            Token::Author { .. } => break,
                            _ => {}
                        }
                    }
                }
                Kind::Tag => {
                    let tag = self.required(&id, Kind::Tag)?;
                    let end = metadata_header_end(
                        tag.data.as_slice(),
                        self.limits.max_metadata_bytes,
                        &self.control,
                    )?;
                    let tag =
                        gix_object::TagRef::from_bytes(&tag.data.as_slice()[..end], id.kind())?;
                    child(tag.target(), tag.target_kind)?;
                }
            }
        }
        self.control.check()?;
        Ok(selection)
    }
}

impl<S: crate::pack::ranges::PackSource> SnapshotSession<crate::pack::ranges::RangePackSet<S>> {
    /// Enumerate every physical catalog object, independently of ref reachability.
    /// Only compaction/backup administration may use this as a storage inventory;
    /// it never authorizes an upload-pack want.
    pub fn catalog_objects(&self) -> Result<ObjectSelection> {
        let mut selection = ObjectSelection::empty(self)?;
        self.provider
            .for_each_object(&self.control, |id| selection.insert(self, id))?;
        self.control.check()?;
        Ok(selection)
    }
}

fn check(control: &crate::Control) -> gix_error::Result {
    control
        .check()
        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
}
