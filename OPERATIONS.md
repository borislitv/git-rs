# Operations and recovery

git-rs is an experimental prerelease. Its local fault tests establish specific application
behaviors, not a guarantee of production HA or a coordinated storage recovery procedure.

## Configuration and trust boundary

`git-rs serve --socket PATH [--health ADDRESS]` runs the backend. The socket's parent must
be private (`0700`); the socket and owner lock are `0600`. One serving owner holds the lock
through cleanup, refuses a live socket and replaces only a stale socket. Removal checks its
recorded file identity. Serving needs no request repository, pack-file scratch or persistent
repository volume. A pod's `emptyDir` contains no authoritative Git data.

The runtime uses Tini as PID 1 to forward termination signals and reap descendants. The backend
does not implement application-level graceful signal draining: termination is a crash-safe stop,
and active clients can receive an unknown outcome. Durable leases expire independently of the pod.

| Environment | Meaning |
| --- | --- |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Tokio PostgreSQL primary metadata connection |
| `PGSSLMODE=verify-full`, `PGSSLROOTCERT` | required PostgreSQL certificate/hostname verification |
| `PGTARGETSESSIONATTRS=read-write` | select a writable primary |
| `PGSSLCERT`, `PGSSLKEY` | optional client certificate and unencrypted PKCS#8 key, configured together |
| `GIT_RS_S3_ENDPOINT`, `GIT_RS_S3_BUCKET`, `GIT_RS_S3_REGION` | HTTPS path-style S3 endpoint/bucket; default region `us-east-1` |
| `GIT_RS_S3_ACCESS_KEY`, `GIT_RS_S3_SECRET_KEY` | service S3 credentials |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | custom S3 TLS trust roots for the in-process transport |
| `GIT_RS_CACHE_BYTES` | reusable RAM cache capacity, default 67108864 bytes; `0` disables |
| `GIT_RS_METADATA_BYTES` | per-snapshot allocation-accounting budget, default/max 134217728 bytes; minimum 4096 |
| `GIT_RS_OPERATION_SECONDS` | operation deadline, integer `1..3600`, default 120 |
| `GIT_RS_BLAME_CONCURRENCY`, `GIT_RS_BLAME_WAITERS` | shared receive/upload/blame/history CPU runtime: 1 / 0; concurrency 1–4, waiters 0–3, total at most 4 |
| `GIT_RS_BLAME_CHECKPOINT_KEY_FILE` | optional protected absolute 32-byte signing-key file; requires explicit cache configuration |
| `GIT_RS_BLAME_WARM_PATH` | optional single literal percent-encoded path; requires authenticated checkpoint cache; disabled by default |
| `GIT_RS_DEBUG_TIMINGS=1` | opt-in fixed-field timing logs; initialized once at process entry, off by default |

The Helm chart obtains credentials from existing Secrets or bucket-claim outputs. Do not put
credentials, private keys or real identity maps in values files, command-line flags or source
control. S3 requests use the in-process signed HTTP transport without proxies,
redirects or automatic retries. Server paths do not execute native Git, client
hooks/configuration or external worker programs. Dependency diagnostics suppress
URLs, credentials and raw external responses.

SQL uses an eight-connection process-local deadpool-postgres pool and a two-worker Tokio runtime.
Pool waits, connection establishment, query streaming and session reset share the operation's
absolute deadline. Failed/cancelled sessions are discarded; cancellation also attempts a bounded
PostgreSQL CancelRequest. Successful sessions are rolled back/reset before reuse. No SQL subprocess
or libpq is used. The application retains its synchronous transport API; database I/O runs on Tokio.
The listed PG variables are supported explicitly, not the full libpq environment/service-file API.
Connection setup is capped at five seconds; statement/lock timeouts are 30/5 seconds. Configuration,
credentials and CA roots are loaded once per process; restart pods after rotating them.

Metadata decoding charges encoded row bytes, retained row overhead and a fixed reserve.
Data-plane source admission reserves its metadata budget before SQL and bounds retained
refs, pack/index entries and ref-name bytes. Four admitted transports may retain snapshots
while one shared CPU worker executes by default. These are collection/backing estimates,
not an allocator or total-RSS guarantee; driver/TLS/runtime overhead still needs headroom.
Capacity exhaustion fails the operation without partial source publication.

Only trusted operators holding database/service credentials may run administrative CLI commands.
The supplied actor argument is not an authentication mechanism for an administrative shell.
Untrusted SSH clients can request only `git-upload-pack`/`git-receive-pack`; OpenSSH authenticates
their key and the server-owned map selects their principal.

For loopback testing only, `--dev-http 127.0.0.1:PORT --dev-principal NAME` sets one server-selected
identity for an unauthenticated Smart HTTP listener. `GIT_RS_DEV_HTTP=1` permits loopback-only
plaintext PostgreSQL/S3 fixtures. It is not a production authentication scheme.

Complete immutable blame uses the authenticated HTTP identity and a leased, published read
snapshot. It admits one computation per process, accepts at most 256 MiB of complete output, rejects
blobs over 128 MiB and shares the operation deadline. Path resolution, ancestry,
attribution and complete porcelain use the pinned Rust core, without a native
blame subprocess or fallback. Receive/upload, history, bundles and maintenance use
the same Rust core and bounded source provider. See the
[endpoint and completed-work contract](docs/blame.md).

Remove `GIT_RS_BLAME_WORKER` and `GIT_RS_PACK_THREADS` from inherited environments:
even an empty setting is obsolete and causes serving startup to fail. Old worker,
`packThreads` and serving `scratch` chart settings are rejected. Use the matching
local chart/binary and inspect rendered manifests during an upgrade.

Production uses the qualified NativeRootV1 core profile with all five performance
tuning switches on. The vendor default and explicit conservative core profile
remain all-off. There is no production tuning/producer override.
The default is one executing job and no queue. Waiters have individual deadlines;
actual jobs have a finite independent lifetime and retain source holds,
buffers and admission through cleanup. Disconnecting a running waiter cannot
release another waiter's source. Abandoned queued work is cancelled cooperatively.
Increasing admission does not increase CPU, RAM, SQL or container allowances.

## Independently configured derived artifacts

Complete blame may reuse shared complete bodies through `derived_cache`. With
explicit authenticated configuration it can also reuse completed objects and
mapping checkpoints. It always requires a fresh authorized source and fences;
cache data is never source authority. Run `git-rs cache-migrate` independently of
source `migrate`. Both schemas use logged tables; no transaction spans their databases.

Bodies use bounded chunk/manifest storage. Payloads of at least 4 KiB are considered
for single-threaded zstd level 3 with a 1 MiB window. Compression is retained only
when it saves at least 128 bytes and 1/16 of the logical length; otherwise storage
remains raw. The stored size selects inline PostgreSQL versus separately configured
cache S3. This is optional reuse, not a claim that the maximum body fits default
cache quotas or that shared-database performance has been qualified.

Both stored and decoded payload sizes remain independently limited to 8 MiB.
Logical size/digest, artifact keys, signatures and namespace byte charges do not
change with encoding. Compressed frames require the declared size, checksum, no
dictionary, one standard frame and no trailing bytes. Decoding uses bounded chunks,
checks cancellation and admits native workspace and output before allocation.
PostgreSQL decoding consumes the borrowed stored Row bytes directly; S3 still owns
its bounded downloaded buffer. These are not end-to-end zero-copy or total-RSS claims.

Supply `GIT_RS_CACHE_NAMESPACE` and `GIT_RS_CACHE_OWNER` as stable, canonical
lowercase UUIDs. They bind an explicitly owned namespace and its immutable
`cache/<namespace>/` object prefix. Namespace creation records the database role
and quotas; reopening it with another owner/role or different quotas is rejected.
Create the dedicated `git_rs_cache` login with permission to create its schema,
then revoke database-wide CREATE permission after migration. It needs no access
to `git_rs` source tables. Migration and serving currently use the same cache role.
The owner UUID is an identity, not an authentication credential.

| Environment | Default / requirement |
| --- | --- |
| `GIT_RS_CACHE_DSN` | Optional independent PostgreSQL DSN; never log its value |
| `GIT_RS_CACHE_PGHOST`, `PGPORT`, `PGDATABASE` (all with `GIT_RS_CACHE_` prefix) | Fall back to source host/port/database |
| `GIT_RS_CACHE_PGUSER` | `git_rs_cache`; source username is not inherited |
| `GIT_RS_CACHE_PGPASSWORD` | Dedicated cache password; source password is not inherited |
| `GIT_RS_CACHE_PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY`, `PGTARGETSESSIONATTRS` (all with prefix) | Inherit source TLS policy when omitted; verified hostname/chain required outside loopback development |
| `GIT_RS_CACHE_CONNECTIONS` | 4; range 1–8, independent of source pool |
| `GIT_RS_CACHE_INLINE_BYTES` | 262144 stored bytes; raise up to maximum payload for bounded all-PG comparison |
| `GIT_RS_CACHE_MAX_PAYLOAD_BYTES` | 8388608; logical and stored payloads are each bounded by this limit, hard maximum 8 MiB |
| `GIT_RS_CACHE_MEMORY_BYTES` | 67108864; includes payload capacity and conservative transient reservations, not total process RSS |
| `GIT_RS_CACHE_NAMESPACE_BYTES`, `GIT_RS_CACHE_NAMESPACE_ROWS` | 1073741824 / 10000 artifact rows; fail closed on quota exhaustion; one fixed maintenance-authority row is reserved separately |
| `GIT_RS_CACHE_TTL_SECONDS` | 3600; range 1–86400 |
| `GIT_RS_CACHE_LEASE_SECONDS`, `GIT_RS_CACHE_JOB_SECONDS` | 15 / 60; job hard lifetime at most 120 seconds |
| `GIT_RS_CACHE_READ_SECONDS`, `GIT_RS_CACHE_GRACE_SECONDS` | 30 / 30; separate reader lifetime and retirement grace |
| `GIT_RS_CACHE_BATCH_ROWS` | 64; maximum 100 per maintenance batch |
| `GIT_RS_CACHE_S3_ENDPOINT`, `GIT_RS_CACHE_S3_BUCKET`, `GIT_RS_CACHE_S3_REGION`, `GIT_RS_CACHE_S3_ACCESS_KEY`, `GIT_RS_CACHE_S3_SECRET_KEY` | Explicit cache-only storage; no fallback to source S3 configuration |

Configure cache S3 credentials restricted to the cache bucket/prefix, without
access to authoritative `repos/` objects. With no cache S3 configuration, oversized
inline artifacts bypass publication. The coordinator keeps a bounded local result
or reports overload; cache failure does not authorize changing source commit status.
Separate schema/database/pool does not isolate shared-cluster CPU, I/O or WAL.

No serving path calls cache deletion. Maintenance closes admission by irreversible
retirement, then waits for valid read holds and grace. Object deletion requires a
never-versioned bucket. Retained tombstones are reconsidered after late PUTs; no
guessed network-settlement TTL permits deleting their identities. S3 reservations
and tombstone rows remain charged, even after physical deletion, so exhaustion is
bounded and explicit. Namespace rotation requires a fresh UUID/prefix and explicit
old-writer/reader quiescence; an empty replacement database cannot authorize purge.

Pool creation is capped at five seconds; cache statement/lock timeouts are 5/1
seconds and calls share the caller's remaining operation budget. Interrupted binary
responses destroy their sessions. Successful sessions are fully consumed/reset.
RAM accounting excludes allocator/runtime/TLS overhead; provision additional
headroom. Payload callbacks must buffer complete success and validate source/read
authority before returning it, rather than stream a result before the final check.

### Authenticated checkpoints and warming

Without a checkpoint key, optional cache configuration can retain complete bodies;
invalid/unavailable ordinary cache setup leaves the local derivation path available.
Setting `GIT_RS_BLAME_CHECKPOINT_KEY_FILE` instead makes startup validate the cache
configuration and signing file explicitly; invalid signing configuration fails
startup rather than silently selecting an unauthenticated checkpoint writer.

The same key authenticates complete response bodies in a separate HMAC domain.
A complete body is the cached line-porcelain response, including authorship and
commit metadata. Its signed descriptor binds the expected source/security scope,
producer/options and ordered chunk lengths/digests. A cache database writer
without the key cannot replace that response by repairing ordinary checksums.
Unsigned, altered, incompatible, foreign-scope or wrong-key bodies are cache
misses and trigger authorized recomputation. Signing mode has no unsigned fallback;
without signing, the cache writer remains trusted for complete-body semantics.

The signing file must be a regular non-symlink file at an absolute path, exactly
32 random bytes, owned by root or the service UID and with no group/other access.
Every ancestor must be a real directory owned by root or the service UID, without
group/other write permission. A private child directory under world-writable
`/tmp` does not meet this policy. Provision through secret management; never log
the bytes, embed them in values, or put them in command arguments. Symlink-based
Secret projections must be adapted to a protected real-file layout before use.

Authorized producers sharing checkpoints must share the namespace, immutable
cache owner UUID and key, and run the bound producer/options implementation.
HMAC-SHA256 authenticates completed objects/mappings and both complete byte-line
partitions, scoped to repository, principal, generation, storage epoch, query and
producer/options. Objects additionally verify kind, size and OID. Checksums or
well-formed mappings alone do not prove canonical Myers tie selection. No suspended
traversal frontier or vendor interner state is a reusable checkpoint.

Rotate by provisioning a new protected key file and restarting all producers.
There is no key-file hot reload; existing processes retain the old key in memory.
New instances treat old-key checkpoints and complete bodies as optional misses,
not authorization.
Changing the key does not rotate cache ownership or authorize purging old data.
For namespace rotation, use a fresh canonical UUID/prefix, declare its owner and
quotas explicitly, and quiesce old writers/readers before separately fenced cleanup.

The bounded WorkSet stores at most 16 MiB of framed records plus 3072-record
metadata, 2048 mappings, 4 MiB per mapping and 1 MiB per object. Capacity and
temporary copies share the 1 GiB job budget, not a process RSS guarantee. Imports
become visible only after cache-consumption and fresh source fences. Callbacks do
no SQL/network I/O. Complete-edge checkpoints occur after 256 successful mapping
insertions or five seconds, plus a final complete batch. Optional storage writes
may be skipped; source/codec errors and cancellation cannot become cache success.

`GIT_RS_BLAME_WARM_PATH` opts into one literal percent-encoded path, without tree
discovery. Only confirmed publication can schedule a changed branch tip. Warming
uses the same CPU pool and actual-work admission, one warm slot, and an independent
30-second deadline assigned before submission. Fresh authorization/generation/
epoch/tip checks happen after dequeue. It may consume query capacity; busy/stale/
failed warming cannot reject or roll back a confirmed push. Keep it disabled until
its resource impact is qualified for the deployment.

### Cache schema upgrade and rollback

The cache schema is now version 4; the authoritative source schema remains separate.
Use this forward-only procedure with the dedicated cache role and explicit
namespace/owner configuration:

1. Inventory all cache producers, readers and maintenance owners. Preserve the
   old cache database/object-prefix ownership records and plan rollback isolation.
2. Run `git-rs cache-migrate` using the new binary against the intended cache
   database. It serializes migration, creates v1 if absent, then transactionally
   applies v1-to-v2, v2-to-v3 and v3-to-v4 in order. Each failed upgrade transaction preserves
   its preceding version; a retry resumes from that committed version.
3. Verify version 4 and run the owned migration/functional acceptance harness before
   exposing new cache-backed features. Do not infer this from source `migrate` or
   ordinary non-ignored tests. The split public blame CI explicitly runs the
   migration harness; inspect its actual completed inventory and results.
4. For rollback, do not downgrade tables or reset version numbers. An old
   v1/v2/v3 `cache-migrate` binary rejects v4. Existing old opaque-cache runtimes may still
   have valid holds; rejection by their migrator is not proof that they stopped.
5. Run the old application with caching disabled, or provision a separate
   old-version-compatible cache database/schema with a fresh namespace UUID/object
   prefix and explicit ownership. A fresh UUID in the upgraded v4 database alone
   does not make it compatible with an old migrator. Never reuse old payload identities.
6. Retire old namespaces only after explicit writer/reader quiescence and the
   normal fenced read-hold/grace/deletion procedure. An empty replacement database
   cannot authorize deletion in the old object prefix.

Version 3 adds a namespace/payload reverse index for computing jobs and makes the
existing child-retention predicates visible to the retirement query planner. The
index is created transactionally and blocks jobs writers while it builds. Busy or
bloated databases can exceed the existing finite migration budget; failure rolls
back that migration, and operators can retry after reducing contention. This is
not an online-upgrade or universally bounded-latency promise. Existing clock,
claim, live-reader/build-pin and irreversible-retirement checks remain required.
Protected retirement still examines namespace candidates; a 64-row result limit
does not mean only 64 candidates are examined. Tombstone accounting and namespace
rotation requirements remain unchanged.

Version 4 adds explicit raw/zstd storage metadata and versioned read/publication
functions. New readers retain legacy raw compatibility. Old raw readers skip
compressed rows before acquiring a hold; this prevents interpreting a frame as
application data, but does not promise mixed-version cache availability. Existing
raw holds and safely fenced maintenance remain valid. Do not remove encoding
metadata or rewrite immutable payload identities to make a downgrade appear safe.
Migration cancellation uses bounded cleanup; an ambiguous committed outcome still
requires rereading the schema version and retrying the idempotent upgrade path.

### Verification and publication limits

CI defines bounded `tests/cache_gate.py` qualification, ordinary workspace checks
and prebuilt actual-image integration. Separate `blame-images` and dependent `blame`
jobs transfer four exact image roles, then explicitly run the ignored
blame contracts, `tests/blame_artifacts.py` contracts/faults/legacy-migration harness
and focused `--blame-core-only` integration. Preparation and each measured phase
retain 900-second bounds. The separate runtime gate discovers and serially executes
source/maintenance SQL contracts and uses fresh owned Kubernetes 1.37 fixtures.
See [runtime verification](docs/runtime-contracts.md). Wiring does not establish
successful execution: inspect the exact revision, selected inventory, results and
cleanup artifacts. See the [current coverage inventory](docs/rust-data-plane.md#current-ci-coverage-and-publication-gaps).
Legacy migration evidence requires actual old binaries with verified hashes.
Runtime codec rejection probes that modify runtime files require disposable
isolated containers, never the runner host. Root/memo evidence is functional and
actual-work reuse evidence only. Qualified synthetic native-parity/performance
measurements do not establish a general production latency, throughput, RSS or
deployment HA guarantee. Preserve the qualified default profile; changing it
requires fresh native, resource and performance qualification.

## Authenticated HTTP listener

`serve --http 0.0.0.0:8080 --http-credentials /etc/git-rs/http/credentials` enables the authenticated
Smart HTTP listener. It is mutually exclusive with `--dev-http`. Supply a read-only Secret file
with one `PRINCIPAL TOKEN` entry per line, separated by exactly one ASCII space. Blank lines are
allowed; comments are not. Principals use the SSH principal grammar. Tokens contain 32–128 ASCII
letters, digits, `_` or `-`; generate each from at least 32 cryptographically random bytes encoded
as unpadded Base64url. The file accepts 1–256 identities and at most 65,536 bytes; duplicate
principals/tokens and malformed records fail startup. Actual tokens do not belong in documentation
or Helm values. Credentials are loaded at startup; rotate the Secret and restart serving replicas.

HTTP Basic credentials select the principal on every discovery and RPC request before backend
access. Missing, malformed or incorrect credentials receive `401` and a Basic challenge. Forwarded
identity headers are ignored; a reader cannot become an owner through gateway-provided headers.
Repository permissions are shared with SSH. Use a Git credential helper, not credential-bearing URLs.

The application listener is plaintext. Require an HTTPS listener on the existing Gateway API
Gateway and a trusted/encrypted gateway-to-pod connection, such as correctly configured Istio mTLS.
HTTPRoute does not configure TLS or authentication, and the legacy Istio Gateway kind is a different
API. Preserve the Authorization header; do not automatically retry receive-pack POST requests.
Enabled HTTPRoute requires standard Gateway API v1.7+ and a controller supporting explicit
`retry.attempts: 0`. The chart sets this field: merely omitting retry configuration can leave
controller-default retries active. Verify the effective proxy configuration as well as the route.
Check the route's `Accepted` and `ResolvedRefs` status and perform an authenticated native Git smoke.
Keep diagnostics on their separate port; they are not the HTTPRoute backend.

## Repository administration

Run commands in a trusted backend/operator environment with the same TLS PostgreSQL connection:

```sh
git-rs migrate
git-rs create team/project alice
git-rs grant team/project alice bob reader refs/
git-rs grant team/project alice ci-agent writer refs/heads/builds/
git-rs grant team/project alice maintainer-agent maintainer refs/
git-rs grant team/project alice bob none refs/
```

`migrate` serializes initialization with a transaction advisory lock. It accepts source
schema v1/v2, transactionally creates/upgrades v2 and refreshes publication/maintenance
functions without resetting durable state. It does not migrate arbitrary unreleased layouts.
Metadata consumes a bounded driver stream rather than a second complete SQL wire image.
Cache migrations remain separate. Follow the two-phase worker upgrade below.

| Role | Read | Ref writes |
| --- | --- | --- |
| reader | whole repository | none |
| writer | whole repository | fast-forward/create in prefix; no deletion or existing-tag rewrite |
| maintainer | whole repository | also non-fast-forward/delete in prefix, except protected default branch |
| owner | whole repository | maintainer behavior plus grants and explicit truncate/cleanup |

`none` revokes the grant. Prefixes start with `refs/` and end in `/`. Default branch `main` cannot
be deleted/rewound through a push, including by an owner. Ref-prefix restrictions are write
restrictions, not an object confidentiality boundary. Already-authorized reads are not revoked
mid-stream; every publication rechecks current write authorization under the repository lock.

## Publication, crashes and retries

1. A request authorizes, acquires a committed shared repository lease and takes a fresh metadata
   snapshot. Rust reads catalogued physical objects through bounded authenticated ranges.
2. One complete immutable command batch is sealed before SQL/status. Controlled Rust ingestion
   validates all physical entries, typed graph closure and fast-forward facts; new pack/idx
   identities have durable staged intents before immutable uploads.
3. One SQL transaction locks the repository, validates the live lease/storage epoch, current
   permissions, old OIDs and complete ref namespace post-image, and publishes refs/catalog/audit.
4. If the COMMIT transport is lost, the same operation/manifest is confirmed with a fresh
   WAL-producing synchronous transaction. Without confirmation the client receives failure.

A failed client request can still have committed when confirmation also becomes unavailable.
Inspect remote refs and `git_rs.operations` after reconnecting; do not infer rollback from a lost
connection or blindly repeat a destructive maintenance command. Ordinary push retries use native
Git old-OID checks. SQL commit durability still depends on PostgreSQL/CNPG and storage configuration.

Committed leases cover the remaining absolute operation budget, rounded up to a second, plus
a 30-second actual-work cleanup margin. Both shared requests and exclusive maintenance
use that rule; no heartbeat or live database connection is required to retain a hold. Operations
default to 120 seconds and may be explicitly configured up to 3600 seconds. A crashed holder can
therefore delay maintenance for up to its remaining budget plus the margin. Maintenance waits at
most five seconds for admission, then reports busy; retry after active operations/crashed leases
finish. Expired leases cannot authorize publication, even if their process is still alive.

SSH and HTTP share the request's absolute lifetime; periodic client traffic cannot
extend it. Health retains its independent one-second budget. PostgreSQL connection/statement/lock
limits and the 110-second per-object S3 transfer limit still apply within a longer request budget.
Configure gateway timeouts consistently when opting into longer operations. Pod termination is
still a crash-safe stop, not application-level request draining.

Serving never deletes objects. Interrupted or rejected uploads are tracked as staged objects;
explicit owner cleanup enqueues elected work to retire/reclaim uncatalogued objects.
A PUT with an unknown outcome leaves a
planned tombstone even after deletion, because its remote write may finish late. Subsequent
cleanup retries the deletion; tombstone retention is currently unbounded.

## Native-compatible backups and ordered history

Any principal with read permission can export the entire visible repository as a native bundle:

```sh
umask 077
mkdir -p /secure/git-backups
git-rs backup team/project alice \
  --output /secure/git-backups/project-before-maintenance.bundle
git bundle verify /secure/git-backups/project-before-maintenance.bundle
```

The Rust bundle writer uses a leased snapshot, admitted object selection and streaming
pack output. The destination must be absolute with a private (`0700`) parent. The bundle is verified
privately, fsynced and published with create-if-absent semantics and mode `0600`. Existing backup
files are never overwritten. Store the verified artifact on durable storage outside the serving
pod before treating it as a backup; a pod-local bundle disappears with ephemeral storage.
The external `git bundle verify` command is an independent client/oracle check, not a
server subprocess.

`git-rs history team/project alice --ref refs/heads/main --max-commits 1000000`
prints ordered commit OIDs using maintained topological traversal. The limit accepts
`1..1000000`; an omitted ref selects the snapshot's actual default branch. The
completed result retains source holds and CPU admission through output consumption.

Bundles preserve native Git refs/objects, not PostgreSQL permissions/audit/operations. To restore
Git contents into a new repository, verify the bundle, mirror-clone it, create a fresh logical
repository/grant, then use ordinary `git push --mirror` with an authorized owner identity.

Full service recovery requires a coordinated PostgreSQL snapshot/WAL position **and** its complete
immutable S3 object set. Optional Barman configuration backs up PostgreSQL, not live Git packs.
Do not restore metadata to a point whose packs were physically deleted by later truncation.

## Physical history truncation

Truncation rewrites branch histories and permanently deletes former live pack/idx objects from
the live S3 bucket. A verified backup is mandatory. Retained commits receive new OIDs where parents
change; signatures/mergetag are removed. For merge histories, N is the topo-ordered count per branch,
not first-parent depth. Boundary commits become roots; each branch tip tree is preserved.

Ordered maintenance procedure:

1. Verify the owner identity, repository, intended N, destination durability and admitted resource headroom.
   Confirm that the bucket has never enabled versioning and no independent lifecycle deletes data.
2. Save any required old history and check clients/automation are prepared for rewritten OIDs.
3. Run the owner command with a new backup destination:

   ```sh
    git-rs truncate team/project alice --keep 100 \
     --backup /secure/git-backups/project-before-truncate.bundle
   ```

   If tags/notes/other nonbranch refs exist, the command refuses unless the operator explicitly adds
   `--drop-other-refs`. That option removes those refs from the live repository; they remain in the
   verified backup.
4. After success, fetch/clone anew, inspect retained history and verify the backup. If the command
   reports an unknown publication outcome, inspect current refs/catalog before any retry.
5. Keep the backup. Physical deletion is separate queued work; enqueue or inspect cleanup:

   ```sh
    git-rs cleanup team/project alice --wait 120
   ```

Cleanup returns `job-id<TAB>state<TAB>error-code`; without `--wait` it only enqueues.
Waiting accepts `1..300` seconds and does not run a worker. A separate elected worker
uses PostgreSQL authority, job tokens, reader fencing, durable retirement rows and
a storage epoch. Failed DELETEs remain retryable. Bucket versioning enabled,
suspended or unknown is rejected; version-aware permanent
deletion is not implemented. Physical live-object deletion is not secure erasure: backup bundles,
client clones, WAL and retained metadata backups can still contain old data.

## Elected source maintenance and schema upgrade

Serving and ordinary enqueue commands need no Kubernetes credentials. Deploy
`git-rs maintenance --scratch /scratch/private --health 0.0.0.0:8081` separately,
with two replicas and a distinct maintenance-only ServiceAccount. Scratch is an
explicit private ephemeral workspace, not a repository/cache source of truth.
`--once --wait SECONDS` is an elected fixture/operator mode, never an unfenced
one-shot deletion fallback.

The chart precreates one kept named Lease with `spec: {}`. The worker role grants
only GET/PATCH of that exact name; it cannot list/watch/create/delete Leases or
access Secrets/pods. The projected token, verified CA and namespace are mounted
read-only. Production uses in-cluster credentials, not inferred kubeconfig or
external credential plugins. Identity combines Pod UID/process nonce with a fresh
PostgreSQL campaign UUID.

Kubernetes candidacy is advisory. Only a confirmed, monotonic PostgreSQL fence
with a live DB-clock lease authorizes work. Logged job/result/publication journals
bind leader fence, fresh job token and claim generation. Expired campaigns cannot
revive. Loss/uncertainty stops claims, cancels actual CPU/I/O and forbids publication;
normal shutdown drains work before PostgreSQL release and Kubernetes vacate.
The two systems are not atomic and this is not an instantaneous zero-overlap claim.

| Maintenance setting | Default / contract |
| --- | --- |
| `GIT_RS_LEADER_LEASE_NAME`, `GIT_RS_LEADER_LEASE_NAMESPACE`, `GIT_RS_POD_UID` | Required exact named Lease and Pod UID |
| `GIT_RS_LEADER_LEASE_SECONDS`, `GIT_RS_LEADER_RENEW_GRACE_SECONDS`, `GIT_RS_LEADER_EXPIRY_SAFETY_SECONDS` | 30 / 15 / 2; validated API/deadline margins |
| `GIT_RS_LEADER_PG_FENCE_LEASE_SECONDS`, `GIT_RS_LEADER_PG_FENCE_HEARTBEAT_SECONDS` | 20 / 5; live DB-clock renewal and independent local expiry |
| `GIT_RS_MAINTENANCE_JOB_LEASE_SECONDS`, `GIT_RS_MAINTENANCE_JOB_SECONDS` | 20 / 120; separate job lease and hard operation lifetime |
| `GIT_RS_MAINTENANCE_CONCURRENCY` | Exactly 1 source job |
| `GIT_RS_MAINTENANCE_SCAN_BATCH`, `GIT_RS_MAINTENANCE_SCAN_SECONDS`, `GIT_RS_MAINTENANCE_SCAN_BUDGET_MILLIS` | 100 / 60 / 1000; durable cursor, maximum batch 1000 |
| `GIT_RS_MAINTENANCE_DELETE_BATCH` | 100; maximum 1000 |
| `GIT_RS_CACHE_BYTES` | Maintenance range cache 512 MiB, maximum 512 MiB |
| `GIT_RS_MAINTENANCE_SHUTDOWN_SECONDS` | 90; range 60–120, shorter than pod termination grace |

Compaction preserves every catalog object, including unreachable objects. Automatic
reachability `full_gc` terminates with `reachability_retention_policy_required`:
there is no implicit retention/backup-root policy. Unsupported bucket versioning
and capacity exhaustion are explicit dead-letter errors; other transient failures
retry with bounded attempts/backoff. Permanent retired identities, staged intents
and multipart cursors survive restart and successful physical deletion.

Upgrade in two operations:

1. Keep `maintenance.enabled=false`, deploy the new source binary/chart, run the
   serialized migration and verify committed source schema v2. Stop incompatible
   old maintenance processes; a schema-version check alone is not quiescence.
2. Enable `maintenance.enabled=true` using the same image digest, separate account
   and named Lease. Both eligible standbys must become ready; exactly one locally
   confirmed authority should answer `/leaderz` with 200. Validate a queued job and
   exact immutable result before accepting maintenance.

The post-upgrade migration hook runs after ordinary resources are applied; enabling
workers before the first schema upgrade can make rollout wait on an absent schema.
There is no v2-to-v1 downgrade/reset procedure. Preserve leadership fences and
journals; rollback requires a schema-v2-compatible binary and drained ownership.

## Resource and service checks

- Default 64 MiB RAM range cache is shared by a process, not replicas. Entries over 8 MiB
  bypass it. Reservations, metadata, in-flight fills and evicted reader pins retain actual
  capacity charges, rounded to 4 KiB. There are at most 256 lookup slots; invalidation drops
  lookups without refunding pinned readers or allowing a crossed fill to reinstall itself.
  Accounting excludes allocator/kernel/TLS/runtime overhead and is not total pod RSS.
- Four admitted transports share one CPU runtime, default one worker/no queue. Raising
  `GIT_RS_BLAME_CONCURRENCY`/`GIT_RS_BLAME_WAITERS` does not raise resource quotas.
- Physical packs use bounded ranges, admitted indexes and controlled delta resolution;
  graph/selection/output buffers and replacement overlap retain separate capacity charges.
- Absolute transport deadlines cover headers, SSH handshake and blocked response writes; the
  operation budget is not reset after parsing. Health has a separate eight-request admission cap
  and one-second absolute connection budget, so a single trickling client cannot starve probes.
- Data-plane bounds include 4096 refs/packs, 1 MiB ref names, 128 MiB aggregate indexes,
  an individual source pack up to 12 GiB and incoming pack/body up to 4 GiB. An incoming
  wire bound is not a promise that its expanded objects fit the shared 1 GiB CPU-job budget.
  Chart defaults are provisional, not a guarantee that worst-case concurrent work fits.
- Receive accepts at most 10,000 commands; truncation N is `1..10000` with retained
  commit metadata up to 64 MiB per branch. History traverses at most one million commits.
- `/livez` checks process responsiveness; `/readyz` checks a reachable writable PostgreSQL primary.
  Serving readiness does not certify the schema, S3 or replication health. Maintenance readiness
  additionally requires an eligible Lease observation and recent schema-v2 writable-primary check;
  `/leaderz` separately requires live local authority. Gate initial
  client exposure on successful migration and a native Git smoke test.
- `/metrics` on the health listener exposes request/phase/stage/dependency/subprocess histograms,
  failures, cache outcomes, S3 object bytes and admission rejections, alongside the active/cache
  gauges. See [metrics and PromQL](docs/metrics.md). Monitor pod/child RSS, CPU throttling,
  ephemeral storage and database replication separately.
- Enable `debugTimings: true` in the chart, or `GIT_RS_DEBUG_TIMINGS=1` before process start, for
  correlated stage records. A 1024-record lossy queue separates sink writes from request and SQL
  cleanup paths. Monitor `git_rs_timing_logs_dropped_total`; missing diagnostics never imply a
  successful or absent operation. Timing logs add profiling overhead; disable them for scoring.

Before production use, validate CNPG synchronous failover and unknown-commit behavior under the
actual topology, Ceph/RGW durability and deletion semantics, clock/deadline margins, namespace
admission policy, resource exhaustion, credential rotation and a coordinated restore drill.

CNPG `ANY 1` synchronous commit alone does not guarantee lossless automatic failover: the primary
and its acknowledged standby can become unavailable while a stale standby remains. Evaluate the
installed operator's experimental `postgresql.synchronous.failoverQuorum` option and test partitions
and promotion behavior. The chart does not claim that replica count or synchronous configuration
constitutes an HA acceptance test.
