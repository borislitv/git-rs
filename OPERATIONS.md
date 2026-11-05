# Operations and recovery

git-rs is an experimental prerelease. Its local fault tests establish specific application
behaviors, not a guarantee of production HA or a coordinated storage recovery procedure.

## Configuration and trust boundary

`git-rs serve --scratch DIR --socket PATH [--health ADDRESS]` runs the backend. Scratch and the
socket's parent directory must be private (`0700`). The socket has one serving owner; a new owner
refuses a live socket and replaces only a stale socket, then sweeps the prior recorded instance
scratch. A pod's `emptyDir` contains no authoritative Git data.

| Environment | Meaning |
| --- | --- |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | libpq primary metadata connection |
| `PGSSLMODE=verify-full`, `PGSSLROOTCERT` | required PostgreSQL certificate/hostname verification |
| `PGTARGETSESSIONATTRS=read-write` | select a writable primary |
| `GIT_RS_S3_ENDPOINT`, `GIT_RS_S3_BUCKET`, `GIT_RS_S3_REGION` | HTTPS path-style S3 endpoint/bucket; default region `us-east-1` |
| `GIT_RS_S3_ACCESS_KEY`, `GIT_RS_S3_SECRET_KEY` | service S3 credentials |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | custom S3 TLS trust roots passed to curl |
| `GIT_RS_CACHE_BYTES` | reusable RAM cache capacity, default 67108864 bytes; `0` disables |
| `GIT_RS_OPERATION_SECONDS` | operation deadline, integer `1..120`, default 120 |

The Helm chart obtains credentials from existing Secrets or bucket-claim outputs. Do not put
credentials, private keys or real identity maps in values files, command-line flags or source
control. S3 credentials are passed to curl over stdin; child environments do not inherit arbitrary
Git configuration or proxies. Subprocess errors intentionally suppress raw external diagnostics.

Only trusted operators holding database/service credentials may run administrative CLI commands.
The supplied actor argument is not an authentication mechanism for an administrative shell.
Untrusted SSH clients can request only `git-upload-pack`/`git-receive-pack`; OpenSSH authenticates
their key and the server-owned map selects their principal.

For loopback testing only, `--dev-http 127.0.0.1:PORT --dev-principal NAME` sets one server-selected
identity for an unauthenticated Smart HTTP listener. `GIT_RS_DEV_HTTP=1` permits loopback-only
plaintext PostgreSQL/S3 fixtures. It is not a production authentication scheme.

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
checks schema version 1. It does not migrate arbitrary older/unreleased database layouts.

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

Leases live for 150 seconds; application operations are bounded by 120 seconds plus termination
margin. A crashed holder can delay maintenance until expiry. Do not increase the hard application
deadline without redesigning/revalidating the lease protocol. Maintenance waits at most five
seconds for admission, then reports busy; retry after active operations/crashed leases finish.

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
  8 MiB bypass it. Reservations and evicted-but-pinned buffers remain accounted against capacity.
- Four admitted operations per process; native Git/curl/psql children consume additional memory,
  CPU and process slots. A cache metric is not total pod RSS.
- A snapshot's pack+idx catalog is admitted up to 12 GiB, incoming pack/HTTP body up to 4 GiB;
  request materialization and concurrent requests need additional scratch headroom. Chart defaults
  are an evaluation starting point, not a guarantee that four worst-case requests fit.
- At most 10,000 refs/commands, truncation N `1..10000`, retained commit metadata up to 64 MiB.
- `/livez` checks process responsiveness; `/readyz` checks a reachable writable PostgreSQL primary.
  Readiness does not certify the current schema, S3 availability or replication health. Gate initial
  client exposure on successful migration and a native Git smoke test.
- `/metrics` reports `git_rs_active_requests` and `git_rs_cache_allocated_bytes`. Monitor pod/child
  RSS, ephemeral storage, database replication/leases and S3 errors separately.

Before production use, validate CNPG synchronous failover and unknown-commit behavior under the
actual topology, Ceph/RGW durability and deletion semantics, clock/deadline margins, namespace
admission policy, resource exhaustion, credential rotation and a coordinated restore drill.
