use super::{Request, UploadSession, encode, peel};
use crate::{
    SnapshotSession, VerifiedObjectProvider,
    objects::{Result, invalid, metadata_header_end},
    storage::{ChargedVec, Set},
};
use gix_hash::ObjectId;
use gix_object::{Kind, commit::ref_iter::Token};
use std::io::Write;

pub(super) struct Boundaries {
    desired: ChargedVec<ObjectId>,
    added: ChargedVec<ObjectId>,
    removed: ChargedVec<ObjectId>,
}

impl Boundaries {
    pub fn boundaries(&self) -> &[ObjectId] {
        self.desired.as_slice()
    }
    pub fn changed(&self) -> bool {
        !self.added.as_slice().is_empty() || !self.removed.as_slice().is_empty()
    }
    pub fn write<P: VerifiedObjectProvider>(
        &self,
        session: &UploadSession<P>,
        output: &mut impl Write,
    ) -> Result<()> {
        for (verb, ids) in [("shallow", &self.added), ("unshallow", &self.removed)] {
            for id in ids.as_slice() {
                session.snapshot.control.check()?;
                let mut bytes = [0u8; 64];
                let mut line = std::io::Cursor::new(bytes.as_mut_slice());
                writeln!(line, "{verb} {id}")?;
                let len = line.position() as usize;
                encode::data_to_write(&bytes[..len], &mut *output)?;
            }
        }
        Ok(())
    }
}

pub(super) fn compute<P: VerifiedObjectProvider>(
    session: &mut UploadSession<P>,
    request: &Request,
) -> Result<Boundaries> {
    for id in request.shallow.as_slice() {
        if !session.is_common(id)? {
            return Err(invalid(
                "client shallow boundary is not an authorized reachable commit",
            ));
        }
    }
    if request.relative && request.depth.is_none() {
        return Err(invalid("deepen-relative requires a depth"));
    }
    let snapshot = &session.snapshot;
    let max = snapshot.limits.max_graph_objects;
    let mut desired = ChargedVec::new(snapshot.capacity.clone(), max)?;
    let mut added = ChargedVec::new(snapshot.capacity.clone(), max)?;
    let mut removed = ChargedVec::new(snapshot.capacity.clone(), max)?;
    let mut client = Set::new(snapshot.capacity.clone(), max)?;
    for id in request.shallow.as_slice() {
        client.insert(*id, &snapshot.control)?;
    }
    if !request.deepening() {
        for id in client.iter() {
            desired.push(*id, &snapshot.control)?;
        }
        return Ok(Boundaries {
            desired,
            added,
            removed,
        });
    }

    let mut forbidden = Set::new(snapshot.capacity.clone(), max)?;
    for id in request.deepen_not.as_slice() {
        if snapshot
            .header(id)?
            .is_none_or(|header| header.kind != Kind::Commit)
        {
            return Err(invalid("deepen-not ref must peel to a commit"));
        }
        let history = snapshot.reachable_from_refs([(b"excluded".as_slice(), *id)])?;
        for id in history.iter() {
            forbidden.insert(id, &snapshot.control)?;
        }
    }
    let mut roots = ChargedVec::new(snapshot.capacity.clone(), max)?;
    for id in request.wants.as_slice() {
        let id = peel(snapshot, *id)?.unwrap_or(*id);
        if snapshot
            .header(&id)?
            .is_some_and(|header| header.kind == Kind::Commit)
        {
            roots.push(id, &snapshot.control)?;
        }
    }
    let mut included = Set::new(snapshot.capacity.clone(), max)?;
    let mut ordered = ChargedVec::new(snapshot.capacity.clone(), max)?;
    let mut pending = ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_edges)?;
    let relative = request.relative && !request.shallow.as_slice().is_empty();
    if relative {
        // First retain the client's existing history up to its old frontier.
        // Then count only additional parent steps from that frontier. This avoids
        // making merge paths depend on an arbitrary first DFS visit or timestamp.
        let mut prefix =
            ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_edges)?;
        for id in roots.as_slice() {
            prefix.push(*id, &snapshot.control)?;
        }
        let mut seen = Set::new(snapshot.capacity.clone(), max)?;
        let mut cursor = 0;
        while let Some(id) = prefix.as_slice().get(cursor).copied() {
            cursor += 1;
            if !seen.insert(id, &snapshot.control)? {
                continue;
            }
            if included.insert(id, &snapshot.control)? {
                ordered.push(id, &snapshot.control)?;
            }
            if client.contains(&id) {
                pending.push((id, 0usize), &snapshot.control)?;
            } else {
                for parent in links(snapshot, id)?.parents.as_slice() {
                    prefix.push(*parent, &snapshot.control)?;
                }
            }
        }
    } else {
        for id in roots.as_slice() {
            pending.push((*id, 1usize), &snapshot.control)?;
        }
    }
    let mut seen = Set::new(snapshot.capacity.clone(), max)?;
    let mut cursor = 0;
    let depth = request.depth.unwrap_or(usize::MAX);
    while let Some((id, distance)) = pending.as_slice().get(cursor).copied() {
        cursor += 1;
        snapshot.control.check()?;
        // FIFO parent edges have unit cost: the first visit has minimal depth,
        // including when one commit has several paths through a merge.
        if !seen.insert(id, &snapshot.control)? || forbidden.contains(&id) {
            continue;
        }
        let commit = links(snapshot, id)?;
        if request.since.is_some_and(|since| commit.time < since) {
            continue;
        }
        if included.insert(id, &snapshot.control)? {
            ordered.push(id, &snapshot.control)?;
        }
        if distance < depth {
            for parent in commit.parents.as_slice() {
                pending.push((*parent, distance + 1), &snapshot.control)?;
            }
        }
    }
    if !roots.as_slice().is_empty() && ordered.as_slice().is_empty() {
        return Err(invalid("no commits selected by shallow request"));
    }
    let mut boundary_set = Set::new(snapshot.capacity.clone(), max)?;
    for id in ordered.as_slice() {
        snapshot.control.check()?;
        if links(snapshot, *id)?
            .parents
            .as_slice()
            .iter()
            .any(|parent| !included.contains(parent))
        {
            boundary_set.insert(*id, &snapshot.control)?;
        }
    }
    // Other client branches remain shallow even if absent from this want set.
    for id in client.iter() {
        if !included.contains(id) {
            boundary_set.insert(*id, &snapshot.control)?;
        } else if !boundary_set.contains(id) {
            removed.push(*id, &snapshot.control)?;
        }
    }
    for id in boundary_set.iter() {
        desired.push(*id, &snapshot.control)?;
        if !client.contains(id) {
            added.push(*id, &snapshot.control)?;
        }
    }
    Ok(Boundaries {
        desired,
        added,
        removed,
    })
}

struct Links {
    parents: ChargedVec<ObjectId>,
    time: i64,
}

fn links<P: VerifiedObjectProvider>(snapshot: &SnapshotSession<P>, id: ObjectId) -> Result<Links> {
    let commit = snapshot.required(&id, Kind::Commit)?;
    let end = metadata_header_end(
        commit.data.as_slice(),
        snapshot.limits.max_metadata_bytes,
        &snapshot.control,
    )?;
    let mut parents = ChargedVec::new(snapshot.capacity.clone(), snapshot.limits.max_graph_edges)?;
    let mut time = None;
    for token in gix_object::CommitRefIter::from_bytes(&commit.data.as_slice()[..end], id.kind()) {
        snapshot.control.check()?;
        match token? {
            Token::Parent { id } => parents.push(id, &snapshot.control)?,
            Token::Committer { signature } => {
                time = Some(signature.time()?.seconds);
                break;
            }
            _ => {}
        }
    }
    Ok(Links {
        parents,
        time: time.ok_or_else(|| invalid("selected commit has no committer"))?,
    })
}
