use super::semantics::Edge;
use super::storage::{ChargedVec, Set};
use super::{ReceiveLimits, Warning, nonnull, semantics};
use crate::objects::{Result, VerifiedObject, invalid};
use crate::{SnapshotSession, VerifiedObjectProvider};
use gix_hash::ObjectId;
use gix_object::Kind;

#[derive(Clone, Copy)]
enum Frame {
    Enter(Edge),
    Leave(ObjectId),
}

pub(super) struct Work<'a, P> {
    snapshot: &'a SnapshotSession<P>,
    limits: ReceiveLimits,
    decoded: u64,
    reads: usize,
    edges: usize,
    complete: Set<ObjectId>,
    gitmodules_done: Set<ObjectId>,
    warnings_seen: Set<(ObjectId, semantics::WarningKind)>,
    warnings: ChargedVec<Warning>,
}

impl<'a, P: VerifiedObjectProvider> Work<'a, P> {
    pub fn new(snapshot: &'a SnapshotSession<P>, limits: ReceiveLimits) -> Result<Self> {
        Ok(Self {
            snapshot,
            limits,
            decoded: 0,
            reads: 0,
            edges: 0,
            complete: Set::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?,
            gitmodules_done: Set::new(
                snapshot.capacity.clone(),
                snapshot.limits.max_graph_objects,
            )?,
            warnings_seen: Set::new(snapshot.capacity.clone(), limits.max_warnings)?,
            warnings: ChargedVec::new(snapshot.capacity.clone(), limits.max_warnings)?,
        })
    }

    pub fn charge(&mut self, size: usize) -> Result<()> {
        self.snapshot.control.check()?;
        self.reads = self
            .reads
            .checked_add(1)
            .filter(|reads| *reads <= self.limits.max_object_reads)
            .ok_or_else(|| invalid("receive object-read budget exceeded"))?;
        self.decoded = self
            .decoded
            .checked_add(u64::try_from(size)?)
            .filter(|bytes| *bytes <= self.limits.max_decoded_bytes)
            .ok_or_else(|| invalid("receive aggregate decoded-byte budget exceeded"))?;
        Ok(())
    }

    pub fn read(&mut self, id: &ObjectId) -> Result<VerifiedObject> {
        nonnull(id)?;
        // Admission is established before allocation by the provider. Charge
        // aggregate CPU work after the exact verified physical size is known.
        let object = self
            .snapshot
            .verified_object(id)?
            .ok_or_else(|| invalid("missing receive graph object"))?;
        self.charge(object.as_slice().len())?;
        Ok(object)
    }

    pub fn inspect(
        &mut self,
        id: &ObjectId,
        kind: Kind,
        bytes: &[u8],
        mut edge: impl FnMut(Edge) -> Result<()>,
    ) -> Result<()> {
        let snapshot = self.snapshot;
        let control = &snapshot.control;
        let edges = &mut self.edges;
        let warnings = &mut self.warnings;
        let warnings_seen = &mut self.warnings_seen;
        semantics::inspect(
            *id,
            kind,
            bytes,
            control,
            &snapshot.capacity,
            snapshot.limits.max_metadata_bytes,
            snapshot.limits.max_graph_edges,
            |value| {
                control.check()?;
                *edges = edges
                    .checked_add(1)
                    .filter(|edges| *edges <= snapshot.limits.max_graph_edges)
                    .ok_or_else(|| invalid("receive aggregate edge budget exceeded"))?;
                edge(value)
            },
            |warning| {
                if warnings_seen.insert((warning.object, warning.kind), control)? {
                    warnings.push(warning, control)?;
                }
                Ok(())
            },
        )
    }

    pub fn closure(&mut self, root: ObjectId, expected: Option<Kind>) -> Result<()> {
        let root_object = self.read(&root)?;
        let kind = root_object.header().kind;
        if expected.is_some_and(|expected| expected != kind) {
            return Err(invalid("unexpected receive root object type"));
        }
        drop(root_object);
        let snapshot = self.snapshot;
        let mut stack = ChargedVec::new(
            snapshot.capacity.clone(),
            snapshot
                .limits
                .max_graph_edges
                .checked_add(snapshot.limits.max_graph_objects)
                .ok_or_else(|| invalid("receive graph worklist overflow"))?,
        )?;
        let mut active = Set::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?;
        stack.push(
            Frame::Enter(Edge {
                id: root,
                kind,
                parent: false,
                gitmodules: false,
            }),
            &snapshot.control,
        )?;
        while let Some(frame) = stack.pop() {
            snapshot.control.check()?;
            match frame {
                Frame::Leave(id) => {
                    active.remove(&id);
                    self.complete.insert(id, &snapshot.control)?;
                }
                Frame::Enter(edge) => {
                    // Check type even if a different typed edge visited this ID.
                    // An OID-only visited fast path before this check is unsound.
                    let object = self.read(&edge.id)?;
                    if object.header().kind != edge.kind {
                        return Err(invalid("receive graph edge type mismatch"));
                    }
                    // Filename-derived context must be checked before an OID-only
                    // completion shortcut, including a previously ordinary blob.
                    if edge.gitmodules
                        && self.gitmodules_done.insert(edge.id, &snapshot.control)?
                        && super::gitmodules::validate(
                            object.as_slice(),
                            &snapshot.control,
                            &snapshot.capacity,
                            snapshot.limits.max_metadata_bytes,
                        )?
                    {
                        let warning = Warning {
                            object: edge.id,
                            kind: semantics::WarningKind::GitmodulesParse,
                        };
                        if self
                            .warnings_seen
                            .insert((edge.id, warning.kind), &snapshot.control)?
                        {
                            self.warnings.push(warning, &snapshot.control)?;
                        }
                    }
                    if active.contains(&edge.id) {
                        return Err(invalid("receive object graph cycle"));
                    }
                    if self.complete.contains(&edge.id) {
                        continue;
                    }
                    active.insert(edge.id, &snapshot.control)?;
                    stack.push(Frame::Leave(edge.id), &snapshot.control)?;
                    self.inspect(&edge.id, edge.kind, object.as_slice(), |child| {
                        stack.push(Frame::Enter(child), &snapshot.control)
                    })?;
                }
            }
        }
        Ok(())
    }

    pub fn is_ancestor(&mut self, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
        if self.read(&ancestor)?.header().kind != Kind::Commit {
            return Err(invalid("old branch target is not a physical commit"));
        }
        let snapshot = self.snapshot;
        let mut pending =
            ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_edges)?;
        let mut seen = Set::new(snapshot.capacity.clone(), snapshot.limits.max_graph_objects)?;
        pending.push(descendant, &snapshot.control)?;
        let mut found = false;
        while let Some(id) = pending.pop() {
            snapshot.control.check()?;
            if !seen.insert(id, &snapshot.control)? {
                continue;
            }
            let object = self.read(&id)?;
            if object.header().kind != Kind::Commit {
                return Err(invalid("branch parent is not a commit"));
            }
            found |= id == ancestor;
            // Complete all parents even after finding a match; never prune using
            // timestamps or a presumed linear first-parent history.
            self.inspect(&id, Kind::Commit, object.as_slice(), |edge| {
                if edge.parent {
                    pending.push(edge.id, &snapshot.control)?;
                }
                Ok(())
            })?;
        }
        Ok(found)
    }

    pub fn finish(self) -> ChargedVec<Warning> {
        self.warnings
    }
}
