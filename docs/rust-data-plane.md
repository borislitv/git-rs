# Rust server data plane

## Required boundary

The target server uses maintained Rust Git libraries rather than the Git CLI for
wire protocols, object and pack handling, graph validation, history and blame.
Native Git remains an independent client and semantic oracle in the test harness.
The current implementation is partway through this migration; the runtime image
still requires Git until every remaining boundary below is replaced and qualified.

Safe Rust, byte-oriented Git names, request cancellation and bounded resources
remain requirements. The target is one shared Rust core with cooperative checks
inside long-running algorithms. The existing external blame executable is migration
debt, not a permanent second backend. Moving work into the serving process does not
by itself give it a hard cancellation boundary.

## Current replacement map

| Boundary | Current implementation | Replacement gate |
| --- | --- | --- |
| Empty request repository | `gix` bare initialization with isolated configuration | Native-readable SHA-1 layout; private cleanup; no copied hooks or request state |
| Snapshot refs and HEAD | `gix-ref` transactions and validated byte-exact HEAD; PostgreSQL remains authoritative | Exact byte-oriented refs, arbitrary default branch, empty/unborn repository, no reflogs |
| Ref-name validation | `gix` reference validation at command parsing and validation | Native-compatible valid and invalid names, including non-UTF-8 names |
| Snapshot/blame object kind and size | `gix-pack`/`gix-odb` physical header-only reads | Native-exact loose/ref-delta headers, sealed RAM links, missing-object and cancellation checks, bounded-memory regression |
| Path, metadata and ancestry reads | Native Git commands | Library reads with exact object identity, literal paths and complete graph semantics |
| Push ingestion and validation | Native receive-pack/index-pack/fsck | Streaming Rust ingestion; strict validation of every incoming object; authenticated existing bases; complete graph closure |
| Fetch and negotiation | Native upload-pack/http-backend | Ordinary clients over HTTP/SSH; protocol versions, tags, notes and shallow behavior qualified explicitly |
| Complete blame | Native default or an operator-selected worker | General graph/path behavior, exact owners/positions/metadata/content, bounded complete response and cancellation |
| Backup and history maintenance | Native Git commands plus database fencing | Verified compatible backup/restore, ownership, fencing and durable retirement |

`gix` 0.90.0 supplies useful object, pack, ref, traversal and blame components.
Its pinned `gix-protocol` 0.68.0 documents client handshake and fetch operations;
it is not a drop-in receive-pack/upload-pack server. Server negotiation, session
state, streaming and atomic publication integration require their own tested
implementation. Do not replace standard binary formats with ad hoc parsers.

The pinned `gix-ref` 0.69.1 symbolic-target writer formats names through `Display`,
which replaces non-UTF-8 bytes. Snapshot restoration therefore validates the full
target with the library, then writes the small HEAD record as raw bytes in the
unexposed, request-exclusive workspace. A native oracle checks the resulting HEAD
and all restored refs, including non-UTF-8 branch names.

The pinned `gix-odb` 0.87.0 directory discovery excludes symlinked pack indexes.
Snapshot header checks instead open the application-owned, catalog-verified
pack/index pairs through `gix-pack`. This preserves zero-copy links to sealed
RAM cache entries and their request-lifetime pins. Library header decoding checks
physical existence and branch target types without inflating complete blobs;
loose objects and delta bases remain supported. These checks do not replace the
strict ingestion and graph validation required before publication.

Blame uses the same physical header reader for its commit-kind and uncompressed
blob-size preflight; those checks no longer execute `cat-file`. A synthetic test
reads loose and sealed-RAM ref-delta blobs around the 128 MiB payload limit in a
child with a 96 MiB address-space limit, so replacing a header probe with full
body inflation fails. This test limit is not a serving-process RSS or hard
cancellation guarantee. Published reachability, literal path resolution and the
default annotation computation still use their explicit native boundaries.
Push tip checks remain part of the native ingestion/validation boundary.

Newly migrated paths must not invoke Git as a hidden fallback. Existing native
paths remain explicit migration debt; they are not evidence that the final
Rust-only boundary has been met. Compatibility must be preserved through each
replacement rather than by disabling strict validation or narrowing accepted
repository shapes.

## Shared core foundation

`crates/git-core` provides the versioned `git-rs-core` library. Its
`SnapshotSession`/`SnapshotOdb` accepts an explicitly pinned physical object provider
and verifies object identity. The root `Scratch::snapshot_odb` adapter preserves
sealed pack/index mappings and source ownership through full reads, cross-pack
delta resolution, byte-oriented paths and exhaustive commit ancestry. An absent
physical empty tree stays absent. `reachable_from_refs` enumerates commits, not the
complete tree/blob closure needed by a future pack writer or strict receiver.

`CpuExecutor` has a fixed worker count, bounded queue and request-local `Control`.
Dropping a completion `Ticket` does not cancel independently owned work; explicit
cancellation and deadline propagation stop cooperative algorithms. A timeout is
not stop acknowledgment. Captured source owners, buffers and admission survive
until the worker has finished cleanup. `CapacityBudget` accounts for live backing
capacity, including retained results; it is not an RSS or allocator-overhead metric.
Arbitrary callbacks, panic hooks and destructors still require bounded behavior.

The audited Gitoxide closure lives in `vendor/gitoxide`, with original source,
patches, licenses and a source manifest. Root path overrides and the locked package
graph prevent duplicate registry/fork type instances. Controlled pack, zlib, diff,
tokenization and blame entrypoints expose inner-loop cancellation and admission;
configuration is request-local rather than operational environment mutation.
The core currently uses an uncached decoder. General blame still needs owner-aware
scratch reuse, complete native porcelain provenance and application integration
before replacing its existing serving boundary.

Run `make check` for the source audit, workspace formatting, strict Clippy and
application/core tests. `make vendor-check` separately runs the five affected
vendor packages, strict Clippy and explicit doctests. Build output stays outside
the audited vendor inventory; only the exact generated fixture directories are
ignored. Root contract tests cover real sealed-RAM links, cross-pack bases,
wrong-object rejection, no-growth caller buffers, source lifetimes and cancellation.
These foundation gates do not establish a Git-free runtime, general native-exact
blame, hard realtime cancellation or production deployment readiness.

## Generality and scaling

The service is for arbitrary Git repositories: multiple files/directories,
branches, merges, renames, tags and nonmonotonic commit timestamps. Single-path or
linear-history optimizations need complete semantic alternatives. Test varied
history shapes and unrelated-path changes as well as large-file stress cases.

The current complete-pack-catalog materialization is a scaling limitation. A
library object-store adapter should access verified immutable objects without
fetching every live pack for a small read, while maintaining snapshot leases,
catalog digests, object closure and memory limits. Repository-level graph/index
work should not be rebuilt independently for each queried file.

## Storage and failure semantics

`derived_cache` provides independently migrated opaque artifacts before the Git
engine refactor. It is not yet connected to serving/blame. `cache-migrate` initializes
logged `git_rs_cache` tables through an independent, bounded PostgreSQL pool. The
metadata text-row limit remains 8,192 bytes; artifacts use parameterized binary
transport. Keys include repository, security scope, object format, ordered inputs,
options, representation and producer version. Only raw payload encoding is currently
supported, so there is no implicit decompression or unbounded expansion.

The caller supplies an `Access` implementation retaining an independently owned
source hold and checking authorization/fences. A cache claim cannot replace it.
Claims use fresh tokens, increasing generations, database-clock leases and a hard
job lifetime. Publication is conditional; ambiguous publication is confirmed using
its exact key/payload/digest. This is optional derivation, not a source transaction.

Inline payloads and S3 GETs are bounded and verified before immutable RAM ownership.
The S3 upload uses sealed memfd staging, never a persistent local cache. Cache S3
configuration is explicit and separate from source storage. Durable upload intents,
read holds, irreversible retirement and never-reused keys protect reclamation.
`CacheMaintenance` is a library interface using its own cache-database claim; the
elected source-maintenance Deployment is a later integration.

The opaque-artifact `Coordinator` coalesces local requests, owns actual-work permits
and source holds after waiter cancellation, and uses a finite independent operation
budget. Its trusted computation callback must poll `Derivation::check()` inside
bounded loops. This is not a general hard-stop guarantee for arbitrary callbacks;
the later Git-core executor must qualify every inner algorithm separately.

Build first with `make cache-build` (or set `CARGO_TARGET_DIR` to a fresh build
directory). The cache qualification target is `make cache-check`, using those
prebuilt executables without invoking Cargo during measurement. Run it inside a
systemd unit with `RuntimeMaxSec=900`, `TimeoutStopSec=40`, `KillMode=control-group`,
`CPUQuota=100%`, `MemoryMax=768M`, `MemorySwapMax=0`, `TasksMax=64`, and
`AllowedCPUs` set to the first two CPUs in the invoking process's affinity. Set
`ExecStopPost=/usr/bin/python3 -B <absolute-checkout>/tests/cache_gate.py cleanup`
and the unit working directory to the checkout; run as a user with Docker access.
The CI cache job provides this invocation. The controller requires systemd's
32-hex `INVOCATION_ID`, labels only its owned Docker fixtures, and retains fresh
logs in `.local/cache-gate-<INVOCATION_ID>/`; independent `ExecStopPost` cleanup
also handles a killed controller. A direct unguarded invocation is rejected.
The controller explicitly runs the
otherwise-ignored PostgreSQL contracts, independent-process and protocol fault
fixtures, and the versioned synthetic PG/S3 storage-cost gate. The latter uses an
owned streaming standby and concurrent authoritative metadata reads. Its budget
file is `tests/fixtures/cache-storage-budget.json`; startup-independent reuse,
wire accounting and copy-coverage limitations are recorded in each report.
These checks do not qualify general Git derivation, serving integration or HA.

PostgreSQL remains authoritative for refs, authorization, catalog and fencing;
immutable S3 objects hold pack/index bytes. Computation changing from Git CLI to
Rust must preserve these invariants:

- A reader uses one consistent leased refs/catalog snapshot. Replica-local data
  is disposable and cannot become a second source of truth.
- A push validates all received objects and the resulting graph, uploads immutable
  objects, then publishes the entire authorized old-OID-checked ref update and
  catalog in one confirmed transaction, including non-atomic client pushes.
- Failure before publication cannot expose dangling refs. Orphan uploads are not
  published state and remain subject to separately fenced reclamation.
- An unknown COMMIT outcome is not blindly retried. Durable operation identity
  resolves whether publication occurred.
- Cancellation, stale workers and maintenance obey durable fencing and reader
  protection. Cache hits do not bypass permissions or resurrect retired state.

Qualify replica termination during upload/publication, ambiguous commit, cold
restart, dependency failure, real PostgreSQL failover and consistent metadata/object
restore separately. Local tests and successful blame timings do not establish
production HA or storage durability after a failure-domain loss.
