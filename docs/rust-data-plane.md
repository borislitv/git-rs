# Rust server data plane

## Required boundary

The server uses maintained Rust Git libraries for wire framing, objects, packs,
deltas, graph traversal and blame. The application owns bounded server negotiation,
authorization, storage access and confirmed atomic publication. Native Git remains
an independent client and semantic oracle in a separate image. The production
image has no Git executable, helper directory, client repository or worker fallback.

This document describes the source contracts and verification definitions. A
source implementation or CI workflow does not establish that its current image,
dependency graph, failure paths or performance have passed qualification.

Safe Rust, byte-oriented Git names, request cancellation and bounded resources
remain requirements. The target is one shared Rust core with cooperative checks
inside long-running algorithms. Complete blame now runs directly in that core;
external blame-worker selection is obsolete and rejected at serving startup.
Cooperative in-process cancellation is not a hard realtime cancellation boundary.

## Production boundaries

| Boundary | Rust implementation | Required verification |
| --- | --- | --- |
| Snapshot refs and HEAD | Leased PostgreSQL snapshot, byte-oriented advertisement, maintained ref-name validation | Default/unborn branch, tags, notes, non-UTF-8 refs, no filesystem restoration |
| Physical object access | `RangePackSet`, admitted indexes and bounded immutable ranges | Exact object identities, CRC/checksum rules, cross-pack REF/OFS bases, missing/corrupt source recovery |
| Blame path, metadata and ancestry | `SnapshotSession` over the same physical provider | Literal byte paths, header-only preflight, exhaustive ancestry and native-compatible output |
| Push ingestion and validation | Maintained controlled stream ingestion, strict physical validation and typed closure | Every entry including duplicate/unreachable objects, thin bases, `.gitmodules`, current SQL authorization |
| Fetch and negotiation | Rust upload session and maintained packet-line/pack primitives | HTTP/SSH v0/v1/v2, shallow/deepen/merge behavior, complete pack/index acceptance |
| Complete blame | Pinned Rust core, NativeRootV1 whole-file Myers and complete porcelain | Native oracle equivalence, general graph/rename behavior, bounded complete response, source fencing and cooperative cancellation |
| History and backup | Maintained admitted topological traversal, Rust bundle envelope and streaming pack writer | Complete ordered history equality, independently verified bundle/restore, leased output ownership |
| Truncation and compaction | Admitted rewrite/catalog selection, multipart output and exact SQL journal | Verified pre-rewrite backup, all-catalog object preservation, stale/unknown publication and reader fencing |
| Source cleanup | Durable jobs, advisory Kubernetes candidacy and PostgreSQL fence | Least-privilege named Lease, monotonic authority, cancellation/drain, permanent retirement and late PUT reconciliation |

No request bare repository, checkout, hook/config execution or pack-file/tmpfs
bridge is required. Protocol strings such as `git-upload-pack` and
`git-receive-pack` remain ordinary Git wire identifiers, not executable dispatch.
The core supports SHA-1 only and fails explicitly at admission/format limits.
It does not substitute whole-pack allocation for filesystem materialization.

Header probes establish physical type/size without inflating a full large blob.
Strict ingestion still validates all physical incoming objects before publication;
a header probe, CRC, index lookup or connectivity check alone is not equivalent.

## Shared core foundation

`crates/git-core` provides the versioned `git-rs-core` library. Its
`SnapshotSession`/`SnapshotOdb` accepts a leased physical object provider and verifies
object identity. The root `Source`/`RangeStore` adapter preserves source authority,
budget identity and reader pins through byte paths and cross-pack delta resolution.
An absent physical empty tree stays absent. Typed object selection distinguishes
commit reachability from complete tree/blob/tag closure. Compaction enumerates the
complete catalog, including physically present unreachable objects.

`CpuExecutor` has a fixed worker count, bounded queue and request-local `Control`.
Dropping a completion `Ticket` does not cancel independently owned work; explicit
cancellation and deadline propagation stop cooperative algorithms. A timeout is
not stop acknowledgment. Captured source owners, buffers and admission survive
until the worker has finished cleanup. `CapacityBudget` accounts for live backing
capacity, including retained results; it is not an RSS or allocator-overhead metric.
Arbitrary callbacks, panic hooks and destructors still require bounded behavior.

Gitoxide is a normal dependency pinned by full revision to the owned
[`borislitv/gitoxide` fork](https://github.com/borislitv/gitoxide/pull/2).
`docs/gitoxide-source.json`, Cargo manifests and lockfile must agree on that
revision, upstream base, licenses and imara origin. Changes and upstream tests
remain in the fork, not copied into application source. The source audit rejects
mixed registry/fork or revision identities. See [dependency sources](dependency-sources.md).

Controlled pack, zlib, traversal, diff, tokenization and blame entrypoints expose
inner-loop cancellation and capacity admission. Admitted topological traversal
uses maintained Gitoxide ordering, not a timestamp-pruned or breadth-first
substitute. Optional completed mapping reuse does not change scoring or tie rules;
raw mapping edges and fresh computations are counted independently of memo hooks.

Production constructs `BlameOptions::default()`: all five qualified `BlameTuning` switches
(`reuse_line_tokens`, `retain_current_input_tokens`, `reuse_unchanged_runs`,
`bounded_resource_cache`, `index_changes`) are explicitly on in NativeRootV1.
The canonical profile remains versioned and immutable, with tuning byte 31;
the vendor tuning default remains all-off. The explicit conservative core profile
is `BlameOptions::native_root(BlameTuning::default())`; fixture `--tuning off`
also remains available. There is no production HTTP/CLI tuning or producer override.
Authenticated completed-work reuse is a separate optional integration, not an
implicit enablement of these tuning switches. The codec is also a pinned owned
Git dependency; its source identity is recorded in `docs/codec-source.json`.
Runtime verification binds the GNU libc iconv profile and actual GNU C/ASCII
thread codeset before serving/cache use. Native compatibility oracles use LC_ALL=C.

The profile has historical synthetic native-parity/performance qualification.
Those measurements bind their recorded sources, executables, producer and resource
envelope; they do not qualify subsequent range-source, traversal, maintenance or
image changes. Preserve the profile and unchanged budgets while qualifying the
joined implementation. Synthetic timing is not a full-repository or HA guarantee.

Run `make check` for the source audit, workspace formatting, strict Clippy and
application/core tests that are not ignored. `make vendor-check` separately
verifies owned origin, exact revision, clean tracked source and license notices
for both checkouts (`GITOXIDE_DIR` and `CODEC_DIR`, defaulting to `.local/gitoxide`
and `.local/iconv-native`). It runs five Gitoxide packages, strict Clippy and
explicit Gitoxide doctests, plus codec library/tests and strict Clippy. CI checks
out those immutable pins; build output remains under the application's target
directory. Cargo fetches the
same immutable source for application and Docker builds. Root contract tests cover
RAM pins, cross-pack bases, wrong-object rejection, no-growth caller buffers,
source lifetimes, invalidation races and cancellation.
These gates do not establish a Git-free runtime, hard realtime cancellation,
full-repository performance or production deployment readiness.

## Generality and scaling

The service is for arbitrary Git repositories: multiple files/directories,
branches, merges, renames, tags and nonmonotonic commit timestamps. Single-path or
linear-history optimizations need complete semantic alternatives. Test varied
history shapes and unrelated-path changes as well as large-file stress cases.

Data-plane paths use demand-driven indexes and 4 MiB immutable ranges, bounded by
4096 refs, 1 MiB ref names, 4096 packs and 128 MiB aggregate index bytes. An
individual source pack is at most 12 GiB; it is not loaded as one buffer. Encoded
pack reuse first authenticates its full catalogued byte stream in bounded ranges
and retains proof only for that job. Metadata has a separate <=128 MiB bound.
Decoder, graph, selection, output and overlap capacity remain independently charged.
These bounds do not promise every repository fits one job or shared graph work.

## Storage and failure semantics

`derived_cache` is connected to complete blame bodies and, with an explicit signing
key, authenticates both complete-body descriptors and completed-work checkpoints.
`cache-migrate` initializes/upgrades logged `git_rs_cache` tables to version 4
through an independent, bounded pool; source `migrate` is separate. Upgrades from
cache v1, v2 or v3 are forward-only, with each migration transactional. Version 3 adds
the jobs reverse lookup and planner-visible retention predicates without changing
cache authority or retirement semantics. Version 4 adds explicit raw/zstd physical
metadata with legacy raw-read compatibility. Old migrators reject v4; rollback,
transactional index-build blocking and namespace ownership policy are
documented in [operations](../OPERATIONS.md#cache-schema-upgrade-and-rollback).
The metadata text-row limit remains 8,192 bytes; artifacts use parameterized binary
transport. Keys include repository, security scope, object format, ordered inputs,
options, representation and producer version. Logical payload identity is independent
of physical raw-v1/zstd-v1 encoding. Beneficial payloads use level 3, a 1 MiB window
and a single checksummed frame; logical and stored lengths each retain the 8 MiB
limit. Frame metadata, complete consumption and both digests are checked before
logical bytes escape. Decoder/context/output reservations survive actual cleanup.
Old raw APIs exclude compressed rows before read-hold acquisition.

The caller supplies an `Access` implementation retaining an independently owned
source hold and checking authorization/fences. A cache claim cannot replace it.
Claims use fresh tokens, increasing generations, database-clock leases and a hard
job lifetime. Publication is conditional; ambiguous publication is confirmed using
its exact key/payload/digest. This is optional derivation, not a source transaction.

Inline payloads and S3 GETs are bounded and verified before immutable RAM ownership.
S3 uploads borrow bounded admitted buffers, never a persistent local cache. Cache S3
configuration is explicit and separate from source storage. Durable upload intents,
read holds, irreversible retirement and never-reused keys protect reclamation.
`CacheMaintenance` uses its independent cache-database claim. The elected
source-maintenance Deployment never borrows that claim as source authority.

The opaque-artifact `Coordinator` coalesces local requests, owns actual-work permits
and source holds after waiter cancellation, and uses a finite independent operation
budget. Its trusted computation callback must poll `Derivation::check()` inside
bounded loops. This is not a general hard-stop guarantee for arbitrary callbacks;
the Git-core executor must qualify every inner algorithm separately.

The root blame scheduler gives actual work a finite independent lifetime and owns
its source hold, buffers and admission through cleanup and complete response
consumption. Each waiter retains its own cancellation/deadline. The last queued
waiter's departure cancels queued work; a departed running waiter cannot cancel
other waiters or release the job's source early. Source authorization, generation
and storage epoch fences remain required on cached as well as computed results.

An authenticated `WorkSet` contains completed physical objects and mappings with
both complete byte-line partitions. It persists neither a suspended traversal
frontier nor vendor interner IDs. HMAC-SHA256 binds the entire frame to the exact
producer/options, repository/principal, generation/epoch, namespace and authorized
owner. Objects also verify kind/size/OID. Imports are temporary until outer cache
and fresh source fences succeed. Memory callbacks perform no SQL/network I/O.
Checkpoint cadence is 256 successful mapping insertions or five seconds at a
complete-edge boundary, plus the final complete batch. Storage failure skips
optional writes; source, cancellation and codec-runtime errors remain errors.

Opt-in one-path warming runs only after confirmed publication, on the same CPU
pool and actual-work admission, with one additional warm admission and a 30-second
deadline including queue time. It acquires fresh source authority after dequeue;
stale hints, capacity and optional-cache failures cannot roll back a confirmed push.
See [blame](blame.md) and the key/rotation policy in [operations](../OPERATIONS.md).

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

### Current CI coverage and publication gaps

The cache CI job invokes the independent `cache_gate.py` controller.
`make check` does not execute ignored contracts. The separate blame gate discovers
the exact ignored-test inventory from each compiled suite's `--ignored --list`,
including nested runtime modules, and records the selected names in its sealed
manifest. It excludes only the named subprocess helpers `artifact_process`,
`checkpoint_reuse_child`, `warm_cache_outage_child` and
`runtime_support::runtime_child`; helpers execute through their parent contracts.
There is no fixed contract-count acceptance threshold. These contracts require
explicit owned fixtures. The split job wires `tests/blame_artifacts.py` for
contracts/faults/migration; its legacy phase requires real old binaries and their
verified hashes. An ordinary green workspace test is not migration evidence.

CI now separates `blame-images` from `blame`: each job retains a 20-minute ceiling.
The former uses bounded Docker workers for four commit-bound image roles, then
saves their immutable IDs in one uncompressed layer-deduplicated archive.
The same workflow/run/attempt artifact contains its SHA256 and current commit;
the latter job verifies archive descriptor/config image IDs and checkout before
loading, then retains separate 900-second preparation and measurement units.
Image-build/Cargo downloads and existing runtime fixtures use host networking; image
inspection remains offline. Exact-HEAD qualification must retain real artifact
transport, inventory, outcomes and cleanup evidence. Historical success is not a
verdict for a changed checkout. No deadline/resource limit is raised for those costs.

The runtime gate consumes prebuilt inputs in separate attested systemd units for
filesystem inventory, serial PostgreSQL source/maintenance contracts, integration,
receive, dependency failures, candidate Lease behavior and real Kubernetes smoke.
Fresh Kubernetes 1.37 containers, controllers, backends, PG/S3 and actual CRI tasks
must fit the same aggregate envelope. External clients never share their Git
installation or repositories with a backend. See [runtime contracts](runtime-contracts.md).

Root/checkpoint functional and actual-work reuse proofs do not establish general
production performance. Qualify the complete implementation, real-image lifecycle,
native semantics and performance before publication; deployment HA remains a
separate acceptance scope. Do not present workflow wiring as executed coverage.

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
