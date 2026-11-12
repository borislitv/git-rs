# Rust server data plane

## Required boundary

The target server uses maintained Rust Git libraries rather than the Git CLI for
wire protocols, object and pack handling, graph validation, history and blame.
Native Git remains an independent client and semantic oracle in the test harness.
The current implementation is partway through this migration; the runtime image
still requires Git until every remaining boundary below is replaced and qualified.

Safe Rust, byte-oriented Git names, request cancellation and bounded resources
remain requirements. The target is one shared Rust core with cooperative checks
inside long-running algorithms. Complete blame now runs directly in that core;
external blame-worker selection is obsolete and rejected at serving startup.
Cooperative in-process cancellation is not a hard realtime cancellation boundary.

## Current replacement map

| Boundary | Current implementation | Replacement gate |
| --- | --- | --- |
| Empty request repository | `gix` bare initialization with isolated configuration | Native-readable SHA-1 layout; private cleanup; no copied hooks or request state |
| Snapshot refs and HEAD | `gix-ref` transactions and validated byte-exact HEAD; PostgreSQL remains authoritative | Exact byte-oriented refs, arbitrary default branch, empty/unborn repository, no reflogs |
| Ref-name validation | `gix` reference validation at command parsing and validation | Native-compatible valid and invalid names, including non-UTF-8 names |
| Snapshot/blame object kind and size | `gix-pack`/`gix-odb` physical header-only reads | Native-exact loose/ref-delta headers, sealed RAM links, missing-object and cancellation checks, bounded-memory regression |
| Blame path, metadata and ancestry reads | Rust `SnapshotSession` over verified physical objects | Byte-oriented literal paths, exact object identity and exhaustive ancestry; native maintenance paths remain separate debt |
| Push ingestion and validation | Native receive-pack/index-pack/fsck | Streaming Rust ingestion; strict validation of every incoming object; authenticated existing bases; complete graph closure |
| Fetch and negotiation | Native upload-pack/http-backend | Ordinary clients over HTTP/SSH; protocol versions, tags, notes and shallow behavior qualified explicitly |
| Complete blame | Pinned Rust core, NativeRootV1 whole-file Myers and complete porcelain | Native oracle equivalence, general graph/rename behavior, bounded complete response, source fencing and cooperative cancellation |
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
annotation computation now use the Rust core as well. Push tip checks remain part
of the native ingestion/validation boundary. Receive-pack, upload-pack/http-backend,
backup and history maintenance still require the native runtime.

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

Gitoxide is a normal Git dependency pinned to the owned
[`borislitv/gitoxide` fork](https://github.com/borislitv/gitoxide/pull/2), revision
`7ce87f63e03ef116cb2d3df097cd688e6d19c35b` (C7 completed MappingMemo,
bounded fixed-work polling, linear immutable trace-root maintenance and exclusive
prepared-token leases, bounded single-origin hunk reuse and request-local raw
input pair preservation and lazy controlled indentation preparation). The original
complete admitted indentation dictionary is built only on the first maintained
heuristic request and reused across both passes; no request does no dictionary
work. Scoring, tie preference, polling and public heuristic signatures are unchanged.
Raw mapping edges and fresh diff computations are counted independently of optional
memo hooks. Hook-free accounting creates no producer identity, wire slot or cache;
unchanged and empty fast paths do not manufacture mapping work from commit counts.
Successful no-rewrite traversal, including consumer
Break, can retain the existing two-resource raw pair within the request. Entry
provenance reset and rewrite/error/stop/panic cleanup preserve source isolation;
this does not add a cache or extend the platform's request lifetime.
`docs/gitoxide-source.json` records its upstream base, licenses and imara origin;
original source, notices, tests and changes remain in that repository, not copied
into this application. The source audit and locked package graph reject mixed
registry/fork or revision identities. Controlled pack, zlib, diff,
tokenization and blame entrypoints expose inner-loop cancellation and admission;
configuration is request-local rather than operational environment mutation.
The physical provider discovers indexes and acquires selected packs on demand for
blame, preserving source holds and verified RAM pins. Owner-aware decoder buffers,
completed mapping hooks and byte-exact complete porcelain are integrated with the
root serving path. This does not replace native receive/upload or backup handling.

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

The C7 all-on profile passed the synthetic 2000-commit/20000-line performance
qualification: three prepared runs had a 4.443925 s median against the 5 s gate,
and three fresh hosted runs had a 4.391570 s median against the 8 s gate. Each
successful output matched all 6346057 native porcelain bytes; hosted runs observed
1999 actual mapping edges/computations and zero hits. All three full tokens-off
negative runs reached the unchanged 20 s deadline. Limits were a shared 1 GiB job,
256 MiB prepared/32 MiB hosted decoder budgets, and an aggregate ceiling of
4 GiB, 2 CPUs and 256 tasks. These are synthetic qualification results, not general
latency guarantees or a Stage 5 release. Full joined functional/authentication,
destructive-runtime and real-image CI qualification remain pending.

Run `make check` for the source audit, workspace formatting, strict Clippy and
application/core tests that are not ignored. `make vendor-check` separately
verifies owned origin, exact revision, clean tracked source and license notices
for both checkouts (`GITOXIDE_DIR` and `CODEC_DIR`, defaulting to `.local/gitoxide`
and `.local/iconv-native`). It runs five Gitoxide packages, strict Clippy and
explicit Gitoxide doctests, plus codec library/tests and strict Clippy. CI checks
out those immutable pins; build output remains under the application's target
directory. Cargo fetches the
same immutable source for application and Docker builds. Root contract tests cover
real sealed-RAM links, cross-pack bases,
wrong-object rejection, no-growth caller buffers, source lifetimes and cancellation.
These gates do not establish a Git-free runtime, hard realtime cancellation,
full-repository performance or production deployment readiness.

## Generality and scaling

The service is for arbitrary Git repositories: multiple files/directories,
branches, merges, renames, tags and nonmonotonic commit timestamps. Single-path or
linear-history optimizations need complete semantic alternatives. Test varied
history shapes and unrelated-path changes as well as large-file stress cases.

Native transport/maintenance still materializes the complete live pack catalog.
Blame instead uses demand-driven verified index/pack acquisition, with bounds of
4096 refs, 1 MiB of ref names, 4096 indexes, 128 MiB selected index bytes and
12 GiB selected pack/index bytes. Metadata retains its separate <=128 MiB bound.
This is bounded source access, not a claim that every repository fits one job or
that graph/index work is shared across all queries.

## Storage and failure semantics

`derived_cache` is connected to complete blame bodies and, with an explicit signing
key, authenticates both complete-body descriptors and completed-work checkpoints.
`cache-migrate` initializes/upgrades logged `git_rs_cache` tables to version 3
through an independent, bounded pool; source `migrate` is separate. Upgrades from
cache v1 or v2 are forward-only, with each migration transactional. Version 3 adds
the jobs reverse lookup and planner-visible retention predicates without changing
cache authority or retirement semantics. Old migrators reject v3; rollback,
transactional index-build blocking and namespace ownership policy are
documented in [operations](../OPERATIONS.md#cache-schema-upgrade-and-rollback).
The metadata text-row limit remains 8,192 bytes; artifacts use parameterized binary
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
the Git-core executor must qualify every inner algorithm separately.

The root blame scheduler gives actual work a finite independent lifetime and owns
its source hold, scratch and admission through cleanup and complete response
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

The cache CI job invokes the older `cache_gate.py` controller.
`make check` does not execute ignored contracts. The separate blame gate discovers
the exact ignored-test inventory from each compiled suite's `--ignored --list`,
including nested runtime modules, and records the selected names in its sealed
manifest. It excludes only the named subprocess helpers `artifact_process`,
`checkpoint_reuse_child`, `warm_cache_outage_child` and
`runtime_support::runtime_child`; helpers execute through their parent contracts.
There is no fixed contract-count acceptance threshold. These contracts require
explicit owned fixtures. The split job wires `tests/blame_artifacts.py` for
contracts/faults/migration; its legacy phase requires real old binaries and their
verified hashes. The complete actual public gate locally passed 58 selected
contracts, 11 artifact/migration groups, four runtime scenarios plus their parent,
and 31 focused serving checks. Cached-image build through completed measurement
took 954.561 seconds, including orchestration, under the 20-minute budget.
A green ordinary workspace test is not migration evidence.

CI now separates `blame-images` from `blame`: each job retains a 20-minute ceiling.
The former uses bounded legacy Docker workers for both exact shared-base targets,
then saves both immutable IDs into one uncompressed layer-deduplicated archive.
The same workflow/run/attempt artifact contains its SHA256 and current commit;
the latter job verifies archive descriptor/config image IDs and checkout before
loading, then retains separate 900-second preparation and measurement units.
Image-build/Cargo downloads and existing runtime fixtures use host networking; image
inspection remains offline. Cold image builds, artifact upload/download and the
first exact-HEAD hosted split run remain pending, not inferred from cached local
qualification. No deadline/resource limit is raised to accommodate those costs.

Native integration CI invokes the full integration harness and its concurrency
path; the split blame job additionally invokes focused `--blame-core-only` mode
using the shipped release executable. Root/checkpoint functional and
actual-work reuse proofs do not establish a Stage 5/8 production performance result.
There is no such performance result yet. Re-run joined-source functional,
authentication, migration and isolated runtime-codec gates before publication;
qualify performance and deployment/HA independently. Do not present existing CI
as coverage of every new contract.

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
