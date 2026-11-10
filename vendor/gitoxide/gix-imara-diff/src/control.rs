//! Request-local cooperative work checks. No process or thread-local state is used.
//!
//! Callbacks must themselves be bounded and nonblocking. An adapter may check a
//! request's atomic cancellation flag and monotonic deadline. A checkpoint denotes
//! completed inner work, allowing deterministic event-gated tests without sleeps.
//! These APIs bound CPU work between callbacks, not allocator or OS scheduling time.

/// The inner operation which has made progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    /// Searching for line boundaries, including within a single long line.
    Tokenize,
    /// Hashing a byte token or building its cached index.
    Hash,
    /// Comparing candidate byte tokens.
    Equal,
    /// Copying a newly interned byte token.
    Copy,
    /// Searching common token prefixes or suffixes.
    Common,
    /// Counting or pruning tokens before Myers search.
    Preprocess,
    /// Searching a Myers frontier or middle snake.
    Myers,
    /// Extending a common run inside a Myers middle-snake search.
    MiddleSnake,
    /// Adjusting diff hunk placement.
    Postprocess,
    /// Collecting complete output.
    Output,
    /// Traversing commits, cached parent lists or trees.
    Graph,
    /// Assigning blame hunks and coalescing runs.
    Attribution,
    /// Before a capacity increase; processed is the proposed allocation in bytes.
    Allocate,
    /// Sorting fixed-size metadata or merging ordered index runs.
    Sort,
    /// Releasing owned scratch in bounded batches. Errors cannot skip cleanup.
    Cleanup,
    /// Constructing or matching rename candidates.
    Rename,
    /// Enumerating actual rename/copy source candidates, not emitted callbacks.
    RenameCandidate,
    /// Initializing or rehashing the byte-only numeric index.
    Index,
}

/// Progress after a bounded unit of inner work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    /// Operation which completed a unit of work.
    pub stage: Stage,
    /// Units completed in this scan (bytes or tokens, depending on the stage).
    pub processed: usize,
}

/// The request was cancelled or its deadline expired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("diff operation cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// A borrowed request-local fallible check; never stored globally.
pub type Check<'a> = dyn FnMut(Checkpoint) -> Result<(), Cancelled> + 'a;

/// Move-only charge for backing capacity. Implement with an application-owned
/// per-job budget (for example an Arc-owned CapacityReservation), never ambient
/// state. A failed resize must leave the old charge intact.
pub trait Allocation: Send + Sync {
    /// Admit a new conservative backing-capacity charge before growth.
    fn resize(&mut self, bytes: usize) -> Result<(), Cancelled>;
}

/// Application capacity admission, independent of any object provider.
pub trait Allocator: Send + Sync {
    /// Return a live charge before the associated allocation is attempted.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn Allocation>, Cancelled>;
}

/// Conservative request arena: growth charges are retained until all request
/// scratch AND returned output have been freed. Reallocation is deliberately
/// overcharged rather than releasing a charge while old backing may be pinned.
/// This accounts backing capacity, not allocator metadata, RSS or scheduler time.
pub struct Allocations {
    leases: Vec<Box<dyn Allocation>>,
    metadata: Box<dyn Allocation>,
    allocator: std::sync::Arc<dyn Allocator>,
}

impl Allocations {
    /// Call only after all charged backing owners were freed. Never let a stop
    /// error skip release; capacity-reservation destructors must be bounded.
    pub fn clear_with_control(&mut self, check: &mut Check<'_>) {
        while !self.leases.is_empty() {
            for _ in 0..32 {
                drop(self.leases.pop());
            }
            let _ = poll(check, Stage::Cleanup, self.leases.len());
        }
        drop(std::mem::take(&mut self.leases));
        let _ = self.metadata.resize(0);
    }
    /// Create an empty arena. Store it after its charged output owner so Rust
    /// destroys the output before releasing the arena's charges.
    pub fn new(allocator: std::sync::Arc<dyn Allocator>) -> Result<Self, Cancelled> {
        Ok(Self {
            leases: Vec::new(),
            metadata: allocator.reserve(0)?,
            allocator,
        })
    }

    /// Admit one conservative capacity increase, retaining its charge. The
    /// caller must emit an Allocate event for every controlled backing growth.
    pub fn admit(&mut self, bytes: usize) -> Result<(), Cancelled> {
        if self.leases.len() == self.leases.capacity() {
            let target = self.leases.capacity().saturating_mul(2).max(32);
            if target > 131072 {
                return Err(Cancelled);
            }
            self.metadata
                .resize(target * std::mem::size_of::<Box<dyn Allocation>>())?;
            self.leases
                .try_reserve_exact(target - self.leases.len())
                .map_err(|_| Cancelled)?;
            self.metadata
                .resize(self.leases.capacity() * std::mem::size_of::<Box<dyn Allocation>>())?;
        }
        self.leases.push(self.allocator.reserve(bytes)?);
        Ok(())
    }
}

pub(crate) fn poll(check: &mut Check<'_>, stage: Stage, processed: usize) -> Result<(), Cancelled> {
    check(Checkpoint { stage, processed })
}

/// Maximum byte scan, comparison, hash write or copy between checks.
pub const BYTE_CHUNK: usize = 4096;

/// Maximum size of one controlled allocation. Aggregate admission belongs to
/// the caller, which can reject `Allocate` checkpoints before allocation starts.
pub const MAX_ALLOCATION_BYTES: usize = 256 * 1024 * 1024;

/// Bound recursive search metadata independently of a request deadline.
pub const MAX_SEARCH_DEPTH: usize = 256;

/// Fallibly acquire owned bytes with admitted capacity and inner copy checks.
pub fn try_copy_bytes(bytes: &[u8], check: &mut Check<'_>) -> Result<Vec<u8>, Cancelled> {
    let mut out = Vec::new();
    try_reserve(&mut out, bytes.len(), check)?;
    for chunk in bytes.chunks(BYTE_CHUNK) {
        out.extend_from_slice(chunk);
        poll(check, Stage::Copy, out.len())?;
    }
    Ok(out)
}

/// Admit a conservative backing-allocation bound, including node-based maps.
/// Callers must still account for aggregate simultaneously live allocations.
pub fn admit_bytes(bytes: usize, check: &mut Check<'_>) -> Result<(), Cancelled> {
    if bytes > MAX_ALLOCATION_BYTES {
        return Err(Cancelled);
    }
    poll(check, Stage::Allocate, bytes)
}

/// Admit geometric capacity, then fallibly reserve it before growing a vector. A
/// capacity denial is a stopped operation, just like a cancellation check.
/// Allocation and deallocation syscalls themselves are not interruptible.
pub fn try_reserve<T>(out: &mut Vec<T>, additional: usize, check: &mut Check<'_>) -> Result<(), Cancelled> {
    let len = out.len().checked_add(additional).ok_or(Cancelled)?;
    if len <= out.capacity() {
        return Ok(());
    }
    let maximum = MAX_ALLOCATION_BYTES / std::mem::size_of::<T>().max(1);
    if len > maximum {
        return Err(Cancelled);
    }
    let target = len.max(out.capacity().saturating_mul(2)).min(maximum);
    let bytes = target.checked_mul(std::mem::size_of::<T>()).ok_or(Cancelled)?;
    admit_bytes(bytes, check)?;
    out.try_reserve_exact(target - out.len()).map_err(|_| Cancelled)
}

/// Push one fixed-size element, admitting geometric capacity growth first.
pub fn try_push<T>(out: &mut Vec<T>, value: T, check: &mut Check<'_>) -> Result<(), Cancelled> {
    if out.len() == out.capacity() {
        let maximum = MAX_ALLOCATION_BYTES / std::mem::size_of::<T>().max(1);
        let target = out.capacity().saturating_mul(2).max(32).min(maximum);
        if target <= out.len() {
            return Err(Cancelled);
        }
        try_reserve(out, target - out.len(), check)?;
    }
    out.push(value);
    Ok(())
}

/// Compare bytes lexicographically with a check after at most 4 KiB of work.
pub fn try_cmp_bytes(a: &[u8], b: &[u8], check: &mut Check<'_>) -> Result<std::cmp::Ordering, Cancelled> {
    use std::cmp::Ordering;
    for (n, (a, b)) in a.chunks(BYTE_CHUNK).zip(b.chunks(BYTE_CHUNK)).enumerate() {
        let common = a.len().min(b.len());
        let order = a[..common].cmp(&b[..common]);
        poll(check, Stage::Equal, n * BYTE_CHUNK + common)?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(a.len().cmp(&b.len()))
}

/// Stable, fallible sorting through numeric index runs. No panic control flow,
/// fallible std comparator or cloning/dropping of elements is required. The
/// comparator must check its own variable-sized work (e.g. `try_cmp_bytes`).
/// The controlled service uses fixed-size metadata as `T`, never user callbacks.
pub fn try_sort_by<T>(
    items: &mut [T],
    mut compare: impl FnMut(&T, &T, &mut Check<'_>) -> Result<std::cmp::Ordering, Cancelled>,
    check: &mut Check<'_>,
) -> Result<(), Cancelled> {
    let len = items.len();
    let mut order = Vec::new();
    try_reserve(&mut order, len, check)?;
    for start in (0..len).step_by(1024) {
        order.extend(start..(start + 1024).min(len));
        poll(check, Stage::Sort, order.len())?;
    }
    let mut spare = Vec::new();
    try_reserve(&mut spare, len, check)?;
    let mut width = 1;
    while width < len {
        spare.clear();
        for start in (0..len).step_by(width * 2) {
            let mid = (start + width).min(len);
            let end = (mid + width).min(len);
            let (mut a, mut b) = (start, mid);
            while a < mid && b < end {
                if compare(&items[order[a]], &items[order[b]], check)? != std::cmp::Ordering::Greater {
                    spare.push(order[a]);
                    a += 1;
                } else {
                    spare.push(order[b]);
                    b += 1;
                }
                poll(check, Stage::Sort, spare.len())?;
            }
            for indices in [&order[a..mid], &order[b..end]] {
                for chunk in indices.chunks(1024) {
                    spare.extend_from_slice(chunk);
                    poll(check, Stage::Sort, spare.len())?;
                }
            }
        }
        std::mem::swap(&mut order, &mut spare);
        width *= 2;
    }
    // Invert destination->source to source->destination, then apply cycles by
    // swapping fixed-size elements. Equal keys retain their original order.
    spare.clear();
    for chunk in order.chunks(1024) {
        spare.extend(std::iter::repeat_n(0, chunk.len()));
        poll(check, Stage::Sort, spare.len())?;
    }
    for (dest, &src) in order.iter().enumerate() {
        spare[src] = dest;
        poll(check, Stage::Sort, dest + 1)?;
    }
    for pos in 0..len {
        while spare[pos] != pos {
            let next = spare[pos];
            items.swap(pos, next);
            spare.swap(pos, next);
            poll(check, Stage::Sort, pos + 1)?;
        }
    }
    Ok(())
}

/// Release vectors whose elements own further allocations in bounded batches.
/// Checks continue after cancellation; cleanup must finish before stop is acked.
pub fn clear_with<T>(items: &mut Vec<T>, check: &mut Check<'_>) {
    while !items.is_empty() {
        let len = items.len().saturating_sub(32);
        items.truncate(len);
        let _ = poll(check, Stage::Cleanup, items.len());
    }
}

/// Collect byte-line ranges with a check at least every `BYTE_CHUNK` bytes,
/// including when no LF is present. No partial range collection escapes errors.
pub fn try_line_ranges(bytes: &[u8], check: &mut Check<'_>) -> Result<Vec<std::ops::Range<usize>>, Cancelled> {
    let mut ranges = Vec::new();
    let mut start = 0;
    for (n, chunk) in bytes.chunks(BYTE_CHUNK).enumerate() {
        for (offset, &byte) in chunk.iter().enumerate() {
            if byte == b'\n' {
                let end = n * BYTE_CHUNK + offset + 1;
                try_push(&mut ranges, start..end, check)?;
                start = end;
            }
        }
        poll(check, Stage::Tokenize, n * BYTE_CHUNK + chunk.len())?;
    }
    if start < bytes.len() {
        try_push(&mut ranges, start..bytes.len(), check)?;
    }
    Ok(ranges)
}
