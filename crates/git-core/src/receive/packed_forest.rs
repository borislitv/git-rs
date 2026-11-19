//! Physical-index scheduling only. Gitoxide owns entry parsing, delta semantics,
//! zlib framing and hashing. Independent OFS roots share one admitted work budget.
use super::*;
use std::{
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    thread,
    time::Duration,
};

const LANES: usize = 4;
const STACK_BYTES: usize = 1024 * 1024;
const FRONTIER: usize = CACHE_BYTES / LANES;
const NONE: usize = usize::MAX;

struct Node {
    offset: usize,
    start: usize,
    end: usize,
    crc: u32,
    parent: usize,
    child: usize,
    sibling: usize,
    kind: gix_object::Kind,
    size: usize,
    inflated: usize,
    plan: Plan,
    subtree_bytes: u64,
    id: OnceLock<ObjectId>,
}

#[derive(Default)]
struct Totals {
    work: AtomicU64,
    objects: AtomicU64,
    logical: AtomicU64,
    hits: AtomicU64,
    avoided: AtomicU64,
    frontier_peak: AtomicUsize,
    stopped: AtomicBool,
}

fn tick(control: &Control, totals: &Totals) -> gix_error::Result {
    checked(control)?;
    if totals.stopped.load(Relaxed) {
        return Err(gix_error::Error::from_boxed(invalid(
            "packed receive peer lane stopped",
        )));
    }
    totals
        .work
        .try_update(Relaxed, Relaxed, |n| (n < DECODER_POLLS).then_some(n + 1))
        .map_err(|_| {
            gix_error::Error::from_boxed(Box::new(InventoryFailure::DecoderCheckpoints))
        })?;
    Ok(())
}

fn codec<T>(result: gix_error::Result<T>, _totals: &Totals) -> Result<T> {
    match result {
        Err(err)
            if err.downcast_any_ref::<InventoryFailure>()
                == Some(&InventoryFailure::DecoderCheckpoints) =>
        {
            Err(InventoryFailure::DecoderCheckpoints.into())
        }
        other => Ok(other?),
    }
}

fn buffer(
    target: &mut CapacityBuffer,
    size: usize,
    budget: &CapacityBudget,
    control: &Control,
) -> Result<()> {
    if target.capacity() < size {
        *target = budget.buffer(size)?;
    }
    target.resize_controlled(size, 0, control)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn inventory(
    packed: &mut Packed<'_>,
    bytes: &[u8],
    end: usize,
    count: usize,
    control: &Control,
    budget: &CapacityBudget,
    limits: ReadLimits,
    receive: super::super::ReceiveLimits,
    progress: &mut impl FnMut(Progress),
) -> Result<()> {
    let totals = Totals::default();
    let mut nodes = ChargedVec::<Node>::new(budget.clone(), count)?;
    let mut roots = ChargedVec::new(budget.clone(), count)?;
    let mut logical = 0u64;
    let mut offset = 12;
    {
        let mut inflate = packed.decoder.borrow_mut();
        let mut framing = budget.buffer(0)?;
        for ordinal in 0..count {
            control.check()?;
            if offset >= end {
                return Err(invalid("truncated physical packed receive inventory"));
            }
            let entry = packed.file.entry(offset as u64)?;
            let start = usize::try_from(entry.data_offset)?;
            if start >= end {
                return Err(invalid("packed receive entry header overlaps checksum"));
            }
            let inflated = usize::try_from(entry.decompressed_size)?;
            if inflated > limits.max_decode_bytes {
                return Err(InventoryFailure::ChainWorkspace.into());
            }
            let parent = match &entry.header {
                pack::data::entry::Header::OfsDelta { base_distance } => {
                    let base = usize::try_from(
                        entry
                            .checked_base_pack_offset(*base_distance)
                            .ok_or_else(|| invalid("invalid packed receive OFS base"))?,
                    )?;
                    nodes
                        .as_slice()
                        .binary_search_by_key(&base, |node| node.offset)
                        .map_err(|_| invalid("OFS base is not a prior physical entry start"))?
                }
                pack::data::entry::Header::RefDelta { .. } => {
                    return Err(invalid(
                        "large bootstrap requires full objects or OFS_DELTA",
                    ));
                }
                _ => NONE,
            };
            let kind = if parent == NONE {
                entry
                    .header
                    .as_kind()
                    .ok_or_else(|| invalid("invalid full pack object kind"))?
            } else {
                nodes.as_slice()[parent].kind
            };
            let full_plan = if parent == NONE {
                Some(Plan::full(inflated, limits)?)
            } else {
                None
            };
            if parent != NONE {
                // Check all limits knowable before allocating the instruction frame.
                nodes.as_slice()[parent].plan.child(0, inflated, limits)?;
            }
            buffer(&mut framing, inflated, budget, control)?;
            let (_, consumed, _) = codec(
                inflate.inflate.once_controlled(
                    &bytes[start..end],
                    framing.as_mut_slice(),
                    &|| tick(control, &totals),
                ),
                &totals,
            )?;
            let (size, plan, sibling) = if let Some(plan) = full_plan {
                (inflated, plan, NONE)
            } else {
                let base = &mut nodes.as_mut_slice()[parent];
                let (base_size, prefix) =
                    pack::data::delta::decode_header_size(framing.as_slice())?;
                let (size, _) =
                    pack::data::delta::decode_header_size(&framing.as_slice()[prefix..])?;
                if base_size != base.size as u64 {
                    return Err(invalid("packed receive delta base size mismatch"));
                }
                let size = usize::try_from(size)?;
                let plan = base.plan.child(size, inflated, limits)?;
                (size, plan, std::mem::replace(&mut base.child, ordinal))
            };
            logical = logical
                .checked_add(size as u64)
                .filter(|n| *n <= receive.max_decoded_bytes)
                .ok_or(InventoryFailure::LogicalBytes)?;
            let next = start
                .checked_add(consumed)
                .filter(|n| consumed != 0 && *n <= end)
                .ok_or_else(|| invalid("packed receive entry overlaps checksum"))?;
            let mut crc = crc32fast::Hasher::new();
            for chunk in bytes[offset..next].chunks(QUANTUM) {
                control.check()?;
                crc.update(chunk);
            }
            nodes.push(
                Node {
                    offset,
                    start,
                    end: next,
                    crc: crc.finalize(),
                    parent,
                    child: NONE,
                    sibling,
                    kind,
                    size,
                    inflated,
                    plan,
                    subtree_bytes: size as u64,
                    id: OnceLock::new(),
                },
                control,
            )?;
            offset = next;
        }
    }
    if offset != end {
        return Err(invalid("packed receive count or trailing bytes mismatch"));
    }

    // Oldest-first physical ordinals are topological. Schedule the largest
    // independent roots first so the late physical tail does not serialize
    // just because native Git wrote its roots last.
    let mut largest_root_bytes = 0;
    for ordinal in (0..count).rev() {
        control.check()?;
        let node = &nodes.as_slice()[ordinal];
        let (parent, weight) = (node.parent, node.subtree_bytes);
        if parent == NONE {
            roots.push((ordinal, weight), control)?;
            largest_root_bytes = largest_root_bytes.max(weight);
        } else {
            nodes.as_mut_slice()[parent].subtree_bytes += weight;
        }
    }
    crate::storage::sort_controlled(roots.as_mut_slice(), budget, control, |a, b| {
        b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
    })?;

    let lanes = LANES.min(roots.as_slice().len());
    progress(Progress::Resolving {
        roots: roots.as_slice().len(),
        lanes,
        largest_root_bytes,
    });
    control.check()?;
    let _stacks = budget.reservation(lanes * STACK_BYTES)?;
    let next = AtomicUsize::new(0);
    let error = Mutex::new(None);
    let mut reported = (0, 0, 0, 0, 0);
    let mut report = || {
        let objects = totals.objects.load(Relaxed);
        let logical = totals.logical.load(Relaxed);
        let checkpoints = totals.work.load(Relaxed);
        let hits = totals.hits.load(Relaxed);
        let avoided = totals.avoided.load(Relaxed);
        progress(Progress::EntryBatch {
            objects: objects - reported.0,
            logical_bytes: logical - reported.1,
            checkpoints: checkpoints - reported.2,
        });
        progress(Progress::Reuse(ReuseStatistics {
            hits: hits - reported.3,
            avoided_ancestor_bytes: avoided - reported.4,
        }));
        reported = (objects, logical, checkpoints, hits, avoided);
    };
    thread::scope(|scope| -> Result<()> {
        let mut handles = [const { None }; LANES];
        for slot in handles.iter_mut().take(lanes) {
            let spawn = thread::Builder::new()
                .name("receive-ofs".into())
                .stack_size(STACK_BYTES)
                .spawn_scoped(scope, || {
                    let run =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
                            let _codec = budget
                                .reservation(gix_zlib::Workspace::ZLIB_RS_0_6_8.entry_inflate)?;
                            let mut lane = Lane {
                                nodes: nodes.as_slice(),
                                bytes,
                                control,
                                budget,
                                totals: &totals,
                                inflate: gix_zlib::Inflate::default(),
                                instructions: budget.buffer(0)?,
                            };
                            while let Some(&(root, _)) =
                                roots.as_slice().get(next.fetch_add(1, Relaxed))
                            {
                                lane.walk(root, None, 0)?;
                            }
                            Ok(())
                        }))
                        .unwrap_or_else(|_| Err(invalid("packed receive inventory lane panicked")));
                    if let Err(cause) = run {
                        let mut first = error.lock().unwrap_or_else(|e| e.into_inner());
                        if first.is_none() {
                            *first = Some(cause);
                        }
                        totals.stopped.store(true, Relaxed);
                    }
                });
            match spawn {
                Ok(handle) => *slot = Some(handle),
                Err(cause) => {
                    let mut first = error.lock().unwrap_or_else(|e| e.into_inner());
                    if first.is_none() {
                        *first = Some(Box::new(cause));
                    }
                    totals.stopped.store(true, Relaxed);
                    break;
                }
            }
        }
        while handles.iter().flatten().any(|handle| !handle.is_finished()) {
            report();
            thread::sleep(Duration::from_millis(100));
        }
        for handle in handles.into_iter().flatten() {
            handle
                .join()
                .map_err(|_| invalid("packed receive lane join failed"))?;
        }
        report();
        Ok(())
    })?;
    if let Some(cause) = error.into_inner().unwrap_or_else(|e| e.into_inner()) {
        return Err(cause);
    }
    packed.polls.set(DECODER_POLLS - totals.work.load(Relaxed));
    packed.reuse = ReuseStatistics {
        hits: totals.hits.load(Relaxed),
        avoided_ancestor_bytes: totals.avoided.load(Relaxed),
    };
    packed.frontier_peak = totals.frontier_peak.load(Relaxed);
    packed.logical_bytes = logical;
    for node in nodes.as_slice() {
        let id = *node
            .id
            .get()
            .ok_or_else(|| invalid("physical packed receive entry not verified"))?;
        packed.physical.push(
            Record {
                id,
                offset: node.offset as u64,
                kind: node.kind,
                size: node.size as u64,
                plan: node.plan,
            },
            control,
        )?;
        packed
            .index
            .push((id, node.crc, node.offset as u64), control)?;
    }
    Ok(())
}

struct Lane<'a> {
    nodes: &'a [Node],
    bytes: &'a [u8],
    control: &'a Control,
    budget: &'a CapacityBudget,
    totals: &'a Totals,
    inflate: gix_zlib::Inflate,
    instructions: CapacityBuffer,
}

impl Lane<'_> {
    fn decode(&mut self, ordinal: usize, base: Option<&[u8]>) -> Result<CapacityBuffer> {
        let node = &self.nodes[ordinal];
        let input = &self.bytes[node.start..node.end];
        let mut result = self.budget.buffer(node.size)?;
        for start in (0..node.size).step_by(QUANTUM) {
            codec(tick(self.control, self.totals), self.totals)?;
            result.resize(node.size.min(start + QUANTUM), 0)?;
        }
        let destination = if base.is_some() {
            buffer(
                &mut self.instructions,
                node.inflated,
                self.budget,
                self.control,
            )?;
            self.instructions.as_mut_slice()
        } else {
            result.as_mut_slice()
        };
        let (_, consumed, _) = codec(
            self.inflate
                .once_controlled(input, destination, &|| tick(self.control, self.totals)),
            self.totals,
        )?;
        if consumed != input.len() {
            return Err(invalid("packed receive framed entry changed"));
        }
        if let Some(base) = base {
            codec(
                pack::data::delta::reconstruct_controlled(
                    base,
                    result.as_mut_slice(),
                    self.instructions.as_slice(),
                    &|| tick(self.control, self.totals),
                ),
                self.totals,
            )?;
        }
        Ok(result)
    }

    fn materialize(&mut self, ordinal: usize) -> Result<CapacityBuffer> {
        // Bounded fallback for a branched frontier; the full plan was checked
        // before any worker started. Reconstruction retains only two bodies.
        let mut path = [NONE; 65];
        let depth = self.nodes[ordinal].plan.depth;
        let mut at = ordinal;
        for slot in path[..=depth].iter_mut().rev() {
            *slot = at;
            at = self.nodes[at].parent;
        }
        let mut body = self.decode(path[0], None)?;
        for &child in &path[1..depth + 1] {
            body = self.decode(child, Some(body.as_slice()))?;
        }
        Ok(body)
    }

    fn walk(&mut self, mut ordinal: usize, mut base: Option<&[u8]>, retained: usize) -> Result<()> {
        let mut handoff: Option<CapacityBuffer> = None;
        loop {
            codec(tick(self.control, self.totals), self.totals)?;
            let node = &self.nodes[ordinal];
            if node.parent != NONE && self.nodes[node.parent].id.get().is_none() {
                return Err(invalid("packed receive parent was not verified"));
            }
            let parent_bytes = handoff
                .as_ref()
                .map(CapacityBuffer::as_slice)
                .or(base.take());
            let body = if let Some(parent) = parent_bytes {
                let child = self.decode(ordinal, Some(parent))?;
                self.totals.hits.fetch_add(1, Relaxed);
                self.totals
                    .avoided
                    .fetch_add(self.nodes[node.parent].plan.expanded, Relaxed);
                child
            } else {
                self.materialize(ordinal)?
            };
            drop(handoff.take());
            let mut hash = gix_hash::hasher(HASH);
            hash.update(&gix_object::encode::loose_header(
                node.kind,
                node.size as u64,
            ));
            for chunk in body.as_slice().chunks(QUANTUM) {
                // As in the serial inventory, hashing is charged by the
                // logical-byte cap and the same request deadline.
                self.control.check()?;
                if self.totals.stopped.load(Relaxed) {
                    return Err(invalid("packed receive peer lane stopped"));
                }
                hash.update(chunk);
            }
            node.id
                .set(hash.try_finalize()?)
                .map_err(|_| invalid("physical packed receive entry visited twice"))?;
            self.totals.logical.fetch_add(node.size as u64, Relaxed);
            self.totals.objects.fetch_add(1, Relaxed);
            let mut child = node.child;
            if child == NONE {
                return Ok(());
            }
            let retained_here = retained + body.capacity();
            if self.nodes[child].sibling != NONE && retained_here > FRONTIER {
                drop(body);
                while child != NONE {
                    self.walk(child, None, retained)?;
                    child = self.nodes[child].sibling;
                }
                return Ok(());
            }
            while self.nodes[child].sibling != NONE {
                self.totals.frontier_peak.fetch_max(retained_here, Relaxed);
                self.walk(child, Some(body.as_slice()), retained_here)?;
                child = self.nodes[child].sibling;
            }
            ordinal = child;
            handoff = Some(body);
        }
    }
}
