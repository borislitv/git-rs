# Fork-free backend and useful blame caching

## Task

Use unmodified maintained libraries behind small application boundaries. Cache
complete immutable blame answers, preserve current authorization and publication
contracts, and protect opaque library work at a qualified job boundary. Remove
fork-only allocation, interner and traversal-checkpoint requirements from the
target design. A missing optional hook does not justify a fork or copied internals.

The first useful cache correction is independent of the dependency migration.
The migration is complete only when all affected callers run qualified upstream
implementations and the resolved production dependency graph contains no forks.

## Structure

Four implementation packages, each followed by behavioral verification:
complete-body cache; upstream adapters; bounded executor and dependency cutover;
separately resourced warming. This document is a plan, not an executed verdict.
All paths are repository-relative. Files marked new are planned additions.

Read [the architecture decision](../docs/fork-free-architecture.md) and current
callers before each package. Run heavy builds/tests in independent bounded owners,
one heavy phase at a time, with immutable source/image inputs and exact cleanup.
Existing source identity, authorization, native-oracle and corruption tests remain
product acceptance; assertions of removed internal mechanisms are revised explicitly.

### Stage 1: Correct complete-body lookup

**Implement:**

- Version `artifact_key` and remove ordinary repository generation from immutable
  body identity. Keep repository UUID, principal, storage epoch, commit/path,
  options, representation and verified producer. Bind new helper sources in the
  producer inventory. Keep request-local generation/epoch checks and source holds.
- Share the warmer's existing exact leased branch-tip reachability proof with
  foreground lookup, including physical commit-kind validation. Other targets
  retain the general reachability path; do not weaken graph validation APIs.
- Look for the authorized complete body before attribution/WorkSet preparation.
  On a miss, preserve the normal computation path and account for all temporary
  preflight buffers. Report preflight work separately from attribution work.
- Add a one-edge shortcut for the fixed whole-file profile: exactly one parent,
  same byte path, same blob OID and mode, compatible authenticated parent body.
  Verify its original key/MAC; never re-sign cached bytes under the child key.
  Other cases use normal computation. Equal content in unrelated histories is
  not evidence of equal blame.

**Files:** `src/{blame.rs,blame_derive.rs,blame_memo.rs}`, new `src/blame_lookup.rs`,
`crates/git-core/src/blame.rs`, `docs/blame.md`.

**Examples/docs:** current `SourceAccess`, `Expected`, `Source::check`,
`crates/git-core/src/graph.rs`; public commit/tree APIs of the pinned library.

**Verify:** `cargo check --locked --workspace --all-targets`;
`cargo fmt --all -- --check`.

### Stage 2: Verify immutable-body behavior through real serving

**Implement tests:**

- Cache a commit, push another commit, restart serving and request the old reachable
  commit: identical native bytes, body hit and no new attribution.
- For an already cached current branch tip require no general history walk and no
  WorkSet preparation. For one unchanged child require parent-body reuse; compare
  complete child porcelain independently, including previous paths and grouping.
- Cover changed content, independent histories with equal blobs, merge, rename,
  mode change and missing parent body. Preserve ACL revoke, force-push reachability,
  epoch, signature/producer, source outage and ownership/drain checks.
- Keep in-flight generation fencing. Revise only tests that required a fresh
  durable body solely because generation changed between requests.
- Prove sensitivity with the old generation key and a removed authorization or
  unchanged-parent guard; restore correct code and rerun.

**Files:** `tests/blame_reuse.rs`,
`tests/support/{blame_body_auth_support.rs,blame_runtime_support.rs,blame_work_observer.rs}`,
`crates/git-core/tests/blame.rs`; suite registration in `tests/blame_gate.py`.

**Verify:** `cargo test --locked -p git-rs-core --test blame`;
`make blame-gate-prepare`; owned `make blame-gate-check` using its exported bundle,
unit, slice and manifest digest. Require executed ignored-case inventory.

### Stage 3: Implement fork-free adapters in an isolated qualification workspace

**Implement:**

- Create new `tests/upstream_probe` as a standalone workspace with its own lockfile
  and `#![forbid(unsafe_code)]`. It must not depend on the fork-backed application.
- Implement `BlameBackend::annotate` using ordinary `gix_blame::file`, a public
  object provider and owned output metadata. Expose complete annotation, not vendor
  interner IDs, allocation callbacks or a resumed traversal frontier.
- Implement `PackEntryDecoder` with public entry parsing/encoding and the upstream
  `File::from_data`/`decode_entry` external-base resolver. Build a bounded valid
  single-entry thin-pack input in memory from verified original ranges. Validate
  original OFS distances/entry boundaries before normalizing a base reference;
  preserve original CRC/hash checks and verify the final object OID. Do not read
  a whole large pack merely to satisfy `FileData` or copy private delta routines.
  Enforce original chain depth/cycle/byte limits in the source adapter and use
  `gix_pack::cache::Never` for synthetic one-entry inputs, avoiding collisions
  between their reused virtual pack paths/offsets.
- Implement `ArtifactCodec` with upstream zstd bounded streaming. Preserve raw/zstd
  compatibility, byte limits, exact frame completion, logical integrity and signing.
  Replace private workspace/header-inspection requirements with an explicit
  qualified operation/resource policy; do not write a second zstd parser.
- Implement `CommitTextCodec` with `gio::CharsetConverter`, caller-owned buffers,
  fallback disabled and ordinary conversion flags. Stop after consumed input
  without a final flush call; retain atomic raw fallback on invalid encoding.
  Validate names before calling the library and test its partial-progress behavior.
- Pin inspected official candidates: Gitoxide `b8e1eb2a3deaf67d82bdfec0ddad43b693639d8b`,
  zstd-rs `5c4970b9ca1735b1bb19008729d5f4538fbdbc23`, GIO 0.21.2 with an explicitly
  pinned GLib runtime. Record native licenses/features and actual image packages.

**Files:** new `tests/upstream_probe/{Cargo.toml,Cargo.lock,src/lib.rs}` and adapter
modules `src/{blame_backend.rs,pack_decoder.rs,artifact_codec.rs,commit_text.rs}`.

**Examples/docs:** public sources in `docs/fork-free-architecture.md`;
existing `crates/git-core/src/{blame.rs,pack_ranges.rs,porcelain.rs}` are behavioral
contracts, not library implementations to copy. No API-extension PR is required.

**Verify:** `cargo check --locked --manifest-path tests/upstream_probe/Cargo.toml`;
`cargo metadata --locked --format-version 1 --manifest-path tests/upstream_probe/Cargo.toml`.

### Stage 4: Qualify the public-API replacements

**Implement tests:**

- Compare complete native blame for roots, merges, whole-parent preference,
  renames, symlinks, repeated-line ties, raw paths and previous/grouping metadata.
- Exercise the actual pack adapter with OFS/REF chains, cross-pack/forward bases,
  cycles, invalid original offsets, noncanonical headers, malformed/trailing zlib,
  CRC/OID mismatches and source failures. A normalized input must not hide invalid
  original framing. Record retained memory and source bytes, not only final output.
- Compare whole-commit Latin-1/TSCII/stateful conversion, no-flush, malformed late
  input and output-full behavior with the independent native oracle.
- Decode old compressed fixtures with upstream zstd; cover exact-output/footer,
  extra output, corrupt/truncated/concatenated frames and bounded concurrent work.
  Document changed frame-policy cases instead of silently relaxing assertions.
- A failed product case blocks that adapter's cutover. Revise the application
  boundary or select another unmodified maintained implementation; never return
  to a fork. Record the exact unsupported behavior rather than a speculative hook.

**Files:** new `tests/upstream_probe/tests/{blame.rs,pack.rs,encoding.rs,codec.rs}`.
Use existing independent native fixtures, not fork-generated expected values.

**Verify:** in the bounded oracle fixture,
`cargo test --locked --manifest-path tests/upstream_probe/Cargo.toml -- --test-threads=1`;
`cargo tree --locked --manifest-path tests/upstream_probe/Cargo.toml`.

### Stage 5: Add one bounded executor and cut over qualified callers

**Implement:**

- Add `JobExecutor::run` with typed bounded inputs/results. Put opaque library
  computation in a non-root worker process under effective runtime CPU/memory/PID
  limits and an absolute deadline. Use safe process/IPC APIs, not library allocation
  hooks. One actual job per worker; retain admission and source ownership through
  termination, wait and I/O drain. No successful partial response/publication.
- Keep the existing parent orchestration responsible for permissions, source
  leases, HTTP framing and authoritative SQL publication. Bind child work to the
  authorized repository/query/snapshot. Limit simultaneous IPC/result buffers.
- Move qualified adapters into the shared core and wire blame, receive, upload,
  history, backup and truncation planning through the same execution boundary as
  needed. Source mutation/deletion stays in the existing fenced parent orchestration.
  Do not remove controlled APIs from a caller before its replacement is qualified.
- Remove fork-only mapping callbacks, mixed-object WorkSet persistence and vendor
  tuning requirements from the target path. Keep complete-body authentication and
  ordinary object caches through public APIs. Old producer artifacts become misses;
  they are not re-signed or reinterpreted as new artifacts.
- Remove advisory Kubert election and use the existing PostgreSQL campaign/fence
  authority for maintenance. Make PG connectivity/lease validity define candidacy
  and readiness; retain expiry, no-revival, source-job fencing and cleanup order.
  This explicitly changes the former Kubernetes-API-outage stop policy.
- Replace the four fork dependency edges with qualified upstream sources. Update
  provenance checks/notices and remove obsolete source-policy assertions. Add a
  production dependency-source gate which rejects forks, local patches and hidden
  fork fallbacks. Existing fork pins are debt until this gate actually passes.

**Files:** new `src/job_executor/{mod.rs,protocol.rs,owner.rs}` and
`crates/git-worker/{Cargo.toml,src/main.rs}`; `crates/git-core/src/`,
`src/{blame_derive.rs,blame_memo.rs,blame_batch.rs,object_source.rs,receive/pack.rs}`,
`src/derived_cache/codec.rs`, `src/maintenance/{leader.rs,runtime.rs}`, Cargo manifests,
lockfile, `Dockerfile`, charts, source records, `tools/check-core-source.py`.

**Examples/docs:** `src/cpu_jobs.rs`, operation/source ownership and existing
runtime-contract fixtures; `migrations/current_functions.sql` maintenance fences.

**Verify:** `cargo check --locked --workspace --all-targets`;
`cargo clippy --locked --workspace --all-targets -- -D warnings`;
`make core-source-check`; `cargo tree --locked`.

### Stage 6: Verify cutover, resources and zero-fork provenance

**Implement tests:**

- Verify worker crash, deadline, memory failure, blocked IPC, disconnect and owner
  death through the real executable. No early hold/slot release or partial success.
- Run native push/clone/fetch/history/backup and strict physical/graph validation,
  including all-or-nothing multi-ref publication and ambiguous-COMMIT confirmation.
- Test competing PG maintenance candidates, expired campaigns, stale fences,
  database failover/unavailability and drain-before-release. Replace Kubernetes
  candidacy assertions only as the explicit policy change from stage 5.
- Check resolved production package sources, runtime image contents and every
  caller: no fork package, copied implementation or fork-backed fallback remains.
  Run codec compatibility and changed native-encoding cases in the actual image.

**Files:** `tests/{runtime_exec.py,runtime_process.py,maintenance_leader.rs,metadata.rs}`;
`crates/git-core/tests/{pack.rs,receive.rs,history.rs,porcelain.rs,blame.rs}`;
`tests/cache_gate.py`, source-policy tests and new executor contracts.

**Verify:** `make check`; `make runtime-contracts-build`; owned sequential
`make receive upload history failures maintenance`; `make cache-build` followed by
owned `make cache-check`. Preparation does not replace executed runtime cases.

### Stage 7: Reconcile warm targets with the fork-free producer

**Implement:**

- Add `git-rs warm-reconcile` over protected durable target configuration:
  repository UUID, authorized principal, branch and byte path. Re-read current refs,
  confirm snapshot UUID and periodically ensure compatible complete bodies exist.
- Reuse artifact build leases; a background task skips an already claimed build.
  Coalesce superseded tips and repeat reconciliation after restart or eviction.
  No dependence on an in-process notification or a write in the push transaction.
- Run with its own worker resources/admission and ordinary read-only source
  credentials. It has no source-deletion authority. Reuse the same qualified
  backend and codec; publication success is distinct from completed computation.
- The durable unit is a completed target result. An interrupted opaque computation
  retries normally; vendor traversal state is not a required persistence format.

**Files:** new `src/blame_reconcile.rs`; `src/{main.rs,metadata.rs,blame_warm.rs}`;
worker chart/values, `OPERATIONS.md`, `docs/blame.md`.

**Verify:** `cargo check --locked --workspace --all-targets`; `make chart`.

### Stage 8: Verify usefulness and recovery end to end

**Implement tests/measurements:**

- Stop the warmer, advance refs, restart and obtain the latest authorized native-
  equal body. Cover lease-holder death, two replicas, ACL revoke, force-push,
  cache/source outage, eviction and foreground contention.
- Measure exact-body hits, unchanged-parent hits, changed-file cold computation
  and warming separately. Compare before/after with identical source/cache states,
  interleaved repetitions, CPU/wall time, source reads and peak retained resources.
- Run long-history synthetic diagnostics and separately authorized representative
  cases under their stated envelopes. A cold deadline remains a failed result,
  not a speedup explained away by warming. Do not infer causal speedups from self samples.

**Files:** new `tests/blame_reconcile.rs`; `tests/blame_reuse.rs`, runtime/image
registration and fresh private benchmark inputs. Public artifacts use synthetic data.

**Verify:** `make blame-gate-prepare`; owned `make blame-gate-check`; owned
`make failures` and `make kubernetes` with explicit worker case inventory;
final production source-policy gate and exact remote PR verification.
