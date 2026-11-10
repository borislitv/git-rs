# Request-local controls

`file` preserves the original signature. `file_with_tuning` takes immutable
`Tuning`: `reuse_line_tokens`, `retain_current_input_tokens`,
`reuse_unchanged_runs`, `bounded_resource_cache`, and `index_changes`. These
choices never use process environment, globals, or thread-local state.

## Production integration

`file_with_control(odb, start, commitgraph, &mut platform, path, options,
tuning, &mut check)` returns a complete `Outcome` or an error. The borrowed
callback accepts `gix_diff::blob::control::Checkpoint` and returns
`Result<(), Cancelled>`. It must check the application's request-local stop flag
and absolute monotonic deadline, and must itself be bounded and nonblocking.
Checkpoints are work/admission boundaries, not a product telemetry protocol.

For aggregate backing-capacity admission, use `file_with_allocations` with the
same arguments, followed by `Arc<dyn Allocator>` and `&mut check`. Its result is
`AdmittedOutcome`, not an independently movable `Outcome`:

- `Allocator::reserve(bytes)` returns `Result<Box<dyn Allocation>, Cancelled>`.
- `Allocation::resize(bytes)` changes its live charge; failure must preserve the
  old charge. Reservation destruction must release the charge and be bounded.
- Implement these traits with the application's per-job budget/reservation.
  Keep source pins, executor permits, provider admission, and the output owner
  alive through their corresponding work and output lifetimes.
- `AdmittedOutcome::outcome()` borrows the output. `clear_with_control` frees
  output backing before releasing charges, even if every cleanup check fails.
  Drop also cleans output and charges. No owned-output escape bypasses the owner.

The conservative arena retains every growth charge through request scratch and
output lifetimes, and separately admits its reservation-vector backing. It may
reject a request earlier than an exact live-capacity implementation. It accounts
backing capacities, not allocator/opaque reservation metadata, process RSS, or
scheduler time. `Allocate` callbacks without an allocator are only admission
notifications; they do not independently establish aggregate ownership.

## Controlled work

The selected byte path uses fallible vector-backed indexes, cached numeric
hashes, chunked byte equality/copy, and a stable fallible numeric-index sorter.
There are no node-allocated byte indexes or directory-relation sets in this path.
Generic `Interner::intern`/`intern_ref` remain uncontrolled; mixing them with byte
insertion preserves token identity but may rebuild their generic hash table.

The diff-level APIs, re-exported through `gix_diff::blob`, include:

- `ByteInput::{try_new, as_input, with, clear_with_control}` and
  `Interner<Vec<u8>>::try_intern_bytes` for controlled line ownership.
- `Diff::try_compute_with` for Histogram, Myers, and MyersMinimal, including
  preprocessing, occurrence lists, frontiers, common scans, and middle snakes.
- `Diff::{try_postprocess_lines, try_postprocess_lines_with, try_hunks}` and
  `try_diff_bytes` for controlled sliders, indentation, and complete output.
- `control::{try_reserve, try_push, try_sort_by, try_cmp_bytes}` for admitted
  growth and stable ordering without panic-based cancellation.

`tree_with_rewrites::diff_with_control` and
`Tracker::{push_change_with_control, emit_with_control, clear_with_control}`
cover tree queues, paths, exact/fuzzy rename candidates, filename preference,
directory relations, and exhaustive copy sources. The controlled traversal
collects exhaustive sources by breadth-first diff against the empty tree.

Blame uses checked path lookup, graph queues, suspect/path copies, change
attribution and splits, prepared token/run ownership, coalescing, and output
sorting. Scoped owners clean nested scratch on early returns. Cleanup ignores
further stop errors until owned resources have been freed; charge release is
batched after the corresponding backing owners are destroyed. Raw cache cleanup
also frees its empty map/vector backings before request charges can be released.

`Outcome::try_entries_with_lines` checks copies within an individual long line
and returns complete output or cancellation. It is not a native porcelain
serializer. A caller retaining/materializing that additional output must supply
its own capacity admission and ownership; the borrowed `AdmittedOutcome` does
not automatically charge arbitrary downstream allocations.

## Bounds and provider contract

- Byte hash/comparison/copy scans check at most every 4 KiB; ordinary numeric
  runs use chunks of at most 1024. Nested owner/charge cleanup uses batches of 32.
- Each admitted backing is limited to 256 MiB. Objects and initial contents are
  limited to 128 MiB; metadata records and paths to 64 KiB; parent/suspect lists
  to 128; arena growth reservations to 131072. Recursive Histogram/Myers search
  depth is limited to 256. Limit/admission failures return an error, never a
  partially successful controlled outcome.
- Fixed-size/validated metadata parsing and allocator/deallocator syscalls have
  no internal callback. Work bounds are not a hard scheduler-realtime latency
  promise or an ability to interrupt a syscall.
- `Platform::validate_controlled_raw` rejects non-`ToGit` modes, worktree roots,
  diff drivers, oversized caches, and legacy nonempty conversion caches. The raw
  path bypasses attributes, filters, external commands, and conversions, and
  retains at most two immutable resources.
- The blame adapter checks headers, admits destination buffers before `Find`,
  validates returned kind/length and bounded metadata, and supports providers
  returning immutable bytes rather than filling supplied scratch.
- Arbitrary `Find`/`FindHeader` implementations are not made cancellable by
  wrapping them. Production must supply verified bounded header/read/decode
  operations with the same request control and its own capacity/source ownership.
  A provider may refuse destination growth: callers pre-admit its buffer.

The controlled path is Rust, without a native Git fallback. Native Git remains
an external compatibility oracle. Existing vendored Myers pointer internals do
not make the entire dependency closure unsafe-free.

## Qualification, 2026-11-10

Verification uses Docker Rust 1.99.1, two pinned CPUs/two build jobs, a 4 GiB
memory/no-extra-swap limit, PID limit 256, an isolated target/Cargo home, read-only
shared registry, and explicit command/service timeouts.

Strict Clippy and the affected behavioral suites passed. These include all 32
tuning combinations, request isolation, allocation denial/lifetime, provider
no-growth conformance, long lines, raw-pipeline rejection, stable ties, controlled
tree/rename/copy equivalence against original snapshots, and cleanup after stop.
Five final-source mutations removing inner Hash, Equal, MiddleSnake, Output, or
RenameCandidate checks failed their intended behavioral assertions while entry
checks remained. Sources were restored byte-exactly. Additional controls qualify
adversarial recursion rejection and explicit charge release after output cleanup.

The normal three-package command also checks the imara package provenance ledger.
`src/byte_input.rs` and `src/control.rs` are recorded there as local additions;
modified upstream sources retain their origin and notices. This gate must pass
alongside the behavioral suites. Passing vendor tests does not establish
application graph/serializer integration or deployment latency by itself.
