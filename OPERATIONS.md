# Operations and recovery

git-rs is an experimental prerelease. Its local fault tests establish specific application
behaviors, not a guarantee of production HA or a coordinated storage recovery procedure.

## Configuration and trust boundary

`git-rs serve --scratch DIR --socket PATH [--health ADDRESS]` runs the backend. Scratch and the
socket's parent directory must be private (`0700`). The socket has one serving owner; a new owner
refuses a live socket and replaces only a stale socket, then sweeps the prior recorded instance
scratch. A pod's `emptyDir` contains no authoritative Git data.

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
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | custom S3 TLS trust roots passed to curl |
| `GIT_RS_CACHE_BYTES` | reusable RAM cache capacity, default 67108864 bytes; `0` disables |
| `GIT_RS_METADATA_BYTES` | per-snapshot allocation-accounting budget, default/max 134217728 bytes; minimum 4096 |
| `GIT_RS_OPERATION_SECONDS` | operation deadline, integer `1..3600`, default 120 |
| `GIT_RS_PACK_THREADS` | native Git `pack.threads` per request, integer `1..4`, default 1 |
| `GIT_RS_BLAME_WORKER` | optional absolute operator-owned executable; unset uses native Git |
| `GIT_RS_DEBUG_TIMINGS=1` | opt-in fixed-field timing logs; initialized once at process entry, off by default |

The Helm chart obtains credentials from existing Secrets or bucket-claim outputs. Do not put
credentials, private keys or real identity maps in values files, command-line flags or source
control. S3 credentials are passed to curl over stdin; child environments do not inherit arbitrary
Git configuration or proxies. Subprocess errors intentionally suppress raw external diagnostics.

SQL uses an eight-connection process-local deadpool-postgres pool and a two-worker Tokio runtime.
Pool waits, connection establishment, query streaming and session reset share the operation's
absolute deadline. Failed/cancelled sessions are discarded; cancellation also attempts a bounded
PostgreSQL CancelRequest. Successful sessions are rolled back/reset before reuse. No SQL subprocess
or libpq is used. The application retains its synchronous transport API; database I/O runs on Tokio.
The listed PG variables are supported explicitly, not the full libpq environment/service-file API.
Connection setup is capped at five seconds; statement/lock timeouts are 30/5 seconds. Configuration,
credentials and CA roots are loaded once per process; restart pods after rotating them.

Snapshots charge encoded row bytes plus 512 bytes per retained ref/pack and a 4096-byte fixed
reserve before decoding/insertion. Four admitted Git requests can retain up to four such snapshots
at once. This is conservative accounting for current collection layouts, not an allocator
or total-RSS guarantee; driver/TLS buffers, transient allocations, cache, Git children and scratch
need separate resource allowances. Exceeding the budget fails the request without partial data.

Only trusted operators holding database/service credentials may run administrative CLI commands.
The supplied actor argument is not an authentication mechanism for an administrative shell.
Untrusted SSH clients can request only `git-upload-pack`/`git-receive-pack`; OpenSSH authenticates
their key and the server-owned map selects their principal.

For loopback testing only, `--dev-http 127.0.0.1:PORT --dev-principal NAME` sets one server-selected
identity for an unauthenticated Smart HTTP listener. `GIT_RS_DEV_HTTP=1` permits loopback-only
plaintext PostgreSQL/S3 fixtures. It is not a production authentication scheme.

Complete immutable blame uses the authenticated HTTP identity and a leased, published read
snapshot. It admits one computation per process, accepts at most 256 MiB of complete output, rejects
blobs over 128 MiB and shares the operation deadline. An optional worker inherits no SQL/S3
credentials and must keep descendants in its owned process group. See the
[endpoint, output and worker contract](docs/blame.md).

## Independently configured derived artifacts

The `derived_cache` library is currently an opaque-artifact foundation; serving and
blame do not yet consume it. Run `git-rs cache-migrate` independently of source
`migrate`. Both namespaces use logged tables; no transaction spans their databases.

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
| `GIT_RS_CACHE_INLINE_BYTES` | 262144, provisional; raise up to maximum payload for bounded all-PG comparison |
| `GIT_RS_CACHE_MAX_PAYLOAD_BYTES` | 8388608, hard maximum 8 MiB |
| `GIT_RS_CACHE_MEMORY_BYTES` | 67108864; includes payload capacity and conservative transient reservations, not total process RSS |
| `GIT_RS_CACHE_NAMESPACE_BYTES`, `GIT_RS_CACHE_NAMESPACE_ROWS` | 1073741824 / 10000; fail closed on quota exhaustion |
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

`migrate` serializes initialization with a transaction advisory lock; repeated/concurrent execution
checks schema version 1 and refreshes its publication function without resetting stored data.
It does not migrate arbitrary older/unreleased database layouts. Metadata snapshots consume a
native query stream rather than retaining a second complete SQL wire image; the decoded catalog
has a separate allocation-accounting budget and request materialization still consumes resources.

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
   snapshot. Git reads/validates a private native repository reconstructed from catalogued packs.
2. Complete pre-receive/proc-receive command lists must match. Git normalizes packs, validates
   graph connectivity and computes fast-forward facts; new pack/idx objects are immutable uploads.
3. One SQL transaction locks the repository, validates the live lease/storage epoch, current
   permissions, old OIDs and complete ref namespace post-image, and publishes refs/catalog/audit.
4. If the COMMIT transport is lost, the same operation/manifest is confirmed with a fresh
   WAL-producing synchronous transaction. Without confirmation the client receives failure.

A failed client request can still have committed when confirmation also becomes unavailable.
Inspect remote refs and `git_rs.operations` after reconnecting; do not infer rollback from a lost
connection or blindly repeat a destructive maintenance command. Ordinary push retries use native
Git old-OID checks. SQL commit durability still depends on PostgreSQL/CNPG and storage configuration.

Committed leases cover the remaining absolute operation budget, rounded up to a second, plus
a 30-second subprocess-termination/cleanup margin. Both shared requests and exclusive maintenance
use that rule; no heartbeat or live database connection is required to retain a hold. Operations
default to 120 seconds and may be explicitly configured up to 3600 seconds. A crashed holder can
therefore delay maintenance for up to its remaining budget plus the margin. Maintenance waits at
most five seconds for admission, then reports busy; retry after active operations/crashed leases
finish. Expired leases cannot authorize publication, even if their process is still alive.

SSH, private hooks and HTTP share the request's absolute lifetime; periodic client traffic cannot
extend it. Health retains its independent one-second budget. PostgreSQL connection/statement/lock
limits and the 110-second per-object S3 transfer limit still apply within a longer request budget.
Configure gateway timeouts consistently when opting into longer operations. Pod termination is
still a crash-safe stop, not application-level request draining.

Serving never deletes objects. Interrupted or rejected uploads are tracked as staged objects;
explicit owner cleanup reclaims uncatalogued objects. A PUT with an unknown outcome leaves a
planned tombstone even after deletion, because its remote write may finish late. Subsequent
cleanup retries the deletion; tombstone retention is currently unbounded.

## Native Git backups

Any principal with read permission can export the entire visible repository as a native bundle:

```sh
umask 077
mkdir -p /secure/git-backups /private/request-scratch
git-rs backup team/project alice --scratch /private/request-scratch \
  --output /secure/git-backups/project-before-maintenance.bundle
git bundle verify /secure/git-backups/project-before-maintenance.bundle
```

The destination must be absolute with a private (`0700`) parent. The bundle is built and verified
privately, fsynced and published with create-if-absent semantics and mode `0600`. Existing backup
files are never overwritten. Store the verified artifact on durable storage outside the serving
pod before treating it as a backup; a pod-local bundle disappears with ephemeral storage.

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

1. Verify the owner identity, repository, intended N, destination durability and free scratch space.
   Confirm that the bucket has never enabled versioning and no independent lifecycle deletes data.
2. Save any required old history and check clients/automation are prepared for rewritten OIDs.
3. Run the owner command with a new backup destination:

   ```sh
   git-rs truncate team/project alice --keep 100 --scratch /private/request-scratch \
     --backup /secure/git-backups/project-before-truncate.bundle
   ```

   If tags/notes/other nonbranch refs exist, the command refuses unless the operator explicitly adds
   `--drop-other-refs`. That option removes those refs from the live repository; they remain in the
   verified backup.
4. After success, fetch/clone anew, inspect retained history and verify the backup. If the command
   reports an unknown publication outcome, inspect current refs/catalog before any retry.
5. If metadata publication succeeded but DELETE failed, keep the backup and rerun only cleanup:

   ```sh
   git-rs cleanup team/project alice --scratch /private/request-scratch
   ```

Cleanup uses an exclusive lease, durable retirement rows and a storage epoch. Failed DELETEs remain
retryable. Bucket versioning enabled, suspended or unknown is rejected; version-aware permanent
deletion is not implemented. Physical live-object deletion is not secure erasure: backup bundles,
client clones, WAL and retained metadata backups can still contain old data.

## Resource and service checks

- Default 64 MiB cache is shared by requests in one process, not replicas. Entries larger than
  8 MiB bypass it. Reservations and evicted-but-pinned files remain accounted against capacity,
  rounded to 4 KiB; at most 256 backing files may be live. Files are size-sealed before download,
  checked for actual written length and SHA-256, then write-sealed before publication. Native Git
  reopens request-local symlinks through `/proc/<serving-pid>/fd`; request pins outlive its children.
  The supplied Linux container/security profile supports this path. Memfd pages can be swapped;
  this accounting does not bound kernel metadata, total file descriptors or pod RSS.
- Four admitted operations per process; native Git/curl children consume additional memory,
  CPU and process slots. A cache metric is not total pod RSS.
- Native pack workers default to one per request. Explicitly set `GIT_RS_PACK_THREADS=2`, `3`
  or `4` only within the backend's CPU and memory allowance. Native Git's delta-base cache limit
  applies per worker, with object buffers in addition; up to four requests can run concurrently.
  This setting does not raise admission, container quotas, pack sizes or validation limits.
- Absolute transport deadlines cover headers, SSH handshake and blocked response writes; the
  operation budget is not reset after parsing. Health has a separate eight-request admission cap
  and one-second absolute connection budget, so a single trickling client cannot starve probes.
- A snapshot's pack+idx catalog is admitted up to 12 GiB, incoming pack/HTTP body up to 4 GiB;
  request materialization and concurrent requests need additional scratch headroom. Chart defaults
  are an evaluation starting point, not a guarantee that four worst-case requests fit.
- At most 10,000 refs/commands, truncation N `1..10000`, retained commit metadata up to 64 MiB.
- `/livez` checks process responsiveness; `/readyz` checks a reachable writable PostgreSQL primary.
  Readiness does not certify the current schema, S3 availability or replication health. Gate initial
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
