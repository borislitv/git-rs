# Rust server data plane

## Required boundary

The target server uses maintained Rust Git libraries rather than the Git CLI for
wire protocols, object and pack handling, graph validation, history and blame.
Native Git remains an independent client and semantic oracle in the test harness.
The current implementation is partway through this migration; the runtime image
still requires Git until every remaining boundary below is replaced and qualified.

Safe Rust, byte-oriented Git names, request cancellation and bounded resources
remain requirements. A Rust implementation may use isolated Rust worker processes
for CPU, memory and crash isolation. Moving an algorithm into the serving process
does not by itself give it a hard cancellation boundary.

## Current replacement map

| Boundary | Current implementation | Replacement gate |
| --- | --- | --- |
| Empty request repository | `gix` bare initialization with isolated configuration | Native-readable SHA-1 layout; private cleanup; no copied hooks or request state |
| Snapshot refs and HEAD | `gix-ref` transactions and validated byte-exact HEAD; PostgreSQL remains authoritative | Exact byte-oriented refs, arbitrary default branch, empty/unborn repository, no reflogs |
| Ref-name validation | `gix` reference validation at command parsing and validation | Native-compatible valid and invalid names, including non-UTF-8 names |
| Object, path, metadata and ancestry reads | Native Git commands | Library reads with exact object identity, literal paths and complete graph semantics |
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

Newly migrated paths must not invoke Git as a hidden fallback. Existing native
paths remain explicit migration debt; they are not evidence that the final
Rust-only boundary has been met. Compatibility must be preserved through each
replacement rather than by disabling strict validation or narrowing accepted
repository shapes.

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
