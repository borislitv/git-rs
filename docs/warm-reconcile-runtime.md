# Warm reconciliation: owned loopback integration contract

Run `tests/warm_reconcile_runtime.py` only against newly provisioned, disposable fixtures.
It drives the production `git-rs warm-reconcile --once` process, native Git HTTP pushes,
real PostgreSQL and a VersityGW POSIX backend. The restarted HTTP consumer is the existing
`blame-http-fixture` binary with the same Engine, default producer and signed cache settings;
its test-only observer distinguishes an exact `CacheHit` from a parent hit or recomputation.
This is a correctness contract, not evidence of periodic timing, resource isolation under load,
Kubernetes service readiness, or production qualification.

## Large signed-body I/O regression

Use a **fresh** fixture prepared below and run the same guarded command with
`tests/warm_body_io_runtime.py` in place of `tests/warm_reconcile_runtime.py`. This separate
test uses a synthetic 20 MiB source blob, `large-v1`, and 384 MiB of derived-cache RAM.
It delays the first real cache S3 exchange in each direction of the test by at least 2.20s
(source S3 is untouched). It requires the ordinary body-build lease to advance while the
staging exchange is blocked, the multi-chunk body to be durable before foreground starts,
and the restarted foreground to report an exact authenticated hit with native-exact bytes.
Run with the same 240s outer guard. A pass does not qualify 164 MB throughput.

For signed `large-v1` exact reads, a directory hint gets at most 2s. Only a ready key
qualifies for up to 24s **total** from probe start. The hint does not authenticate the root,
provide source access, replace read holds, or count an independently verified parent hit as
an exact hit. The
unchanged-parent read, baseline profile and unsigned cache mode retain the 2s allowance
per lookup. A ready-but-unavailable payload can still consume the longer bounded window.

For signed `large-v1` publication, the provisional serial-I/O envelope is 2s plus 0.5s per
started 4 MiB logical chunk, at most 34s. That is 22s for 164,000,000 bytes or 22.5s for
164 MiB. This covers staging, root CAS and confirmation, not attribution. A scoped heartbeat
renews the original cache build before staging and every 2s during it; each periodic renewal
RPC has at most 1s. The window also ends at the original operation/control/build deadline,
whichever is first. Neither source holds nor build hard limits are extended. Baseline and
unsigned publication keep their original 2s budget. For a 164 MB test, provision enough
cache RAM for the retained decoded body **plus** codec and transport workspace (the default
64 MiB is insufficient), and keep enough of the configured read hold for output consumption
after chunk verification. Budget arithmetic is not a latency measurement: capture real
chunk counts, staging/lookup wall times, cache allocation and SQL/S3 waits before tuning.

### Initial warming over multiple quanta

For an operator-controlled one-off run, put `GIT_RS_BLAME_RESOURCE_PROFILE=large-v1` and
`GIT_RS_WARM_INITIAL_SECONDS` in the scoped worker environment, then run
`git-rs warm-reconcile --once`. Without the initial-budget setting the ordinary small
job path is unchanged. Do not set it on the periodic worker or the serving deployment.

| Setting | Default when enabled | Maximum |
| --- | ---: | ---: |
| `GIT_RS_WARM_INITIAL_SECONDS` (aggregate scan work, all targets) | explicit | 7,200 s |
| `GIT_RS_WARM_JOB_SECONDS` (each quantum, including finalization) | 120 | 300 s |
| `GIT_RS_WARM_SEGMENT_EDGES` (all parent edges per segment, including unchanged edges) | 256 | 16,384 |
| `GIT_RS_WARM_INITIAL_SEGMENTS` (computed segments per target, plus one finalization) | 2,048 | 4,096 |
| `GIT_RS_WARM_INITIAL_MIB` (shared per-target admission for retained summary and active work) | 4,096 | 4,096 MiB |
| `GIT_RS_WARM_QUANTUM_PAUSE_MS` (no source hold during pause; optional pacing) | 0 | 5,000 ms |

The worker reduces each freshly completed segment into a single in-memory prefix using the
ordinary producer. The prefix has no serialization/import route; original allocation tokens
retain their capacity and controls. A new quantum uses a fresh session and deadline but must
match repository, principal, branch, tip, path, generation, epoch, and the core producer and
options scope. No warm source, cache or build hold crosses quanta. Revocation or a mismatch
discards the prefix. A merge, rename or boundary declined by the segment producer discards
the prefix and tries the normal bounded reconciliation path. Restart begins again from the
durable target, not from a partial body or persisted frontier.

Only the final quantum may sign the ordinary body, and only after the core finishes the
complete locally computed prefix and verifies its provenance. An existing exact body is a skip;
no imported maps or WorkSets are promoted into local signing authority. Finalization claims a
fresh ordinary body build and must confirm publication. The foreground budget is unchanged.
Admission is not an RSS guarantee; configure pod limits separately. The aggregate and quantum
deadlines are work deadlines; bounded lease cleanup may finish after them. Use an outer process
guard for a hard wall-clock limit. Run only one initial worker per target: replicas can duplicate
prefix work because no partial-build lease survives a quantum.

For the synthetic runtime contract use a fresh fixture below and substitute
`tests/warm_initial_runtime.py` in the same guarded command. It requires multiple committed
hold releases, tests ACL revoke and process termination between quanta, checks memory and
segment-count exhaustion, and only accepts a restarted foreground exact hit with native bytes.
Add `--cache-placement all-pg` to use a compressible multipart fixture. It omits cache S3
configuration, checks PostgreSQL placement and the stored and logical hashes of each data
chunk, then compares the fresh HTTP hit through socket EOF. The default tests S3-backed chunks.
The serial window size and budget must still be qualified on the 160k-edge target. If a segment,
or the final metadata/encoding/publication quantum, cannot fit, the job fails rather than
extending time or signing incomplete work.

## Prepare and run

1. Obtain **prebuilt, same-revision** Linux `git-rs` and `blame-http-fixture` (the release and
   fixture binaries produced by the regular runtime build), and a full native Git **2.56.0**
   installation including its HTTP helper. Record binary digests and PostgreSQL/VersityGW
   versions outside the secrets file. The harness does not build or download anything.
2. Start a fresh PostgreSQL primary and IAM-enabled VersityGW on `127.0.0.1` only, with
   private, empty data directories. Use PostgreSQL SCRAM for TCP, and use admin credentials
   only for provisioning. For a local PostgreSQL installation, a typical startup is
   `pg_ctl -D "$PGDATA" -o '-h 127.0.0.1 -p 55432' -w -t 10 start` after `initdb`.
   Use the VersityGW release's `posix` mode and listen-address option; if using the prebuilt
   repository fixture image, publish its port 7070 only to `127.0.0.1`. Do not reuse a
   shared gateway or its administrators' credentials in application environments.
3. As the fixture PG administrator, create the following logins and databases. Assign
   individual disposable SCRAM passwords using `psql`'s interactive `\password ROLE`
   command or your local fixture secret mechanism. Do not put real credentials in SQL
   history or a committed file. Save the **same** repository UUID in the two marker strings,
   the JSON and the VersityGW source-reader policy. Generate fresh UUIDs for another run.

   ```sql
   CREATE ROLE warm_source_owner LOGIN;
   CREATE ROLE warm_cache_owner LOGIN;
   CREATE ROLE warm_reader_example LOGIN NOINHERIT;
   CREATE DATABASE warm_src_example OWNER warm_source_owner;
   CREATE DATABASE warm_cache_example OWNER warm_cache_owner;
   COMMENT ON DATABASE warm_src_example IS 'git-rs-warm-runtime:ac000000-0000-0000-0000-000000000001';
   COMMENT ON DATABASE warm_cache_example IS 'git-rs-warm-runtime:ac000000-0000-0000-0000-000000000001';
   REVOKE CONNECT ON DATABASE warm_src_example FROM PUBLIC;
   REVOKE CONNECT ON DATABASE warm_cache_example FROM PUBLIC;
   GRANT CONNECT ON DATABASE warm_src_example TO warm_reader_example;
   ```

   Leave both application schemas absent. The script first creates the pre-warm v2 source
   schema and an existing repository row, then calls `git-rs migrate`, grants the narrow warm
   API, and reruns `migrate` to verify that it preserves both the existing repository and the
   grants. It uses the actual scoped login to list the configured slot and checks that it has
   no source table grants. The independent artifact DB is migrated with `git-rs cache-migrate`.
4. Provision two fresh VersityGW buckets and three distinct application access keys. Use
   the gateway's normal IAM/ACL administration for the exact pinned version. Inspect the
   installed policy before running; the script does not probe write or delete denials or
   infer prefix scope from an access-key string.

   | Credential | Required access |
   | --- | --- |
   | `source_s3` | Source bucket pack/index reads and publication writes, including the receive multipart operations. |
   | `warm_source_s3` | **Only** `s3:GetObject` on `arn:aws:s3:::warm-source-example/repos/ac000000-0000-0000-0000-000000000001/operations/*` (including range GET). No other source prefixes, listing, write or delete. |
   | `cache_s3` | Read/write, not delete, under `arn:aws:s3:::warm-cache-example/cache/ac000000-0000-0000-0000-000000000002/*`. |

   If the selected gateway configuration cannot enforce a prefix-only source-reader key,
   stop rather than substitute a broad identity. The cache PG login is intentionally shared
   between warmer and observer: the current cache DB guard binds a namespace to its database
   role as well as its owner UUID. This is separate from both source logins. The script uses
   the cache schema owner in the disposable cache DB for migration and expiry injection; it is
   **not** a production cache role recipe.
5. Save this JSON shape with synthetic fixture values in an owner-private `0600` file.
   Create a separate, empty `0700` work directory of at most 80 bytes in path length. Place
   it **under a protected real directory**, not under `/tmp` or other group/world-writable
   ancestors: the Rust signing-file check rejects those ancestors even if the final directory
   is private. A short `$HOME/warm-run` in a private account is suitable.

   ```json
   {
     "pg_port": 55432,
     "s3_port": 57070,
     "repository": "ac000000-0000-0000-0000-000000000001",
     "namespace": "ac000000-0000-0000-0000-000000000002",
     "owner": "ac000000-0000-0000-0000-000000000003",
     "source_db": {"database": "warm_src_example", "user": "warm_source_owner", "password": "REPLACE"},
     "cache_db": {"database": "warm_cache_example", "user": "warm_cache_owner", "password": "REPLACE"},
     "warm_role": {"user": "warm_reader_example", "password": "REPLACE"},
     "source_s3": {"bucket": "warm-source-example", "access": "REPLACE", "secret": "REPLACE"},
     "warm_source_s3": {"access": "REPLACE", "secret": "REPLACE"},
     "cache_s3": {"bucket": "warm-cache-example", "access": "REPLACE", "secret": "REPLACE"}
   }
   ```

6. From the APP root, run:

   ```sh
   timeout --kill-after=15s 240s python3 -B tests/warm_reconcile_runtime.py \
     --owned-loopback synthetic-only --config "$HOME/warm-fixture.json" \
     --workdir "$HOME/warm-run" --git /absolute/native-git-2.56.0/bin/git \
     --binary /absolute/prebuilt/git-rs --observer /absolute/prebuilt/blame-http-fixture
   ```

   Keep the logs private. Stop the fixture daemons, then dispose of only the two marked
   databases, two fixture buckets, the generated signing file and the test directories.
   Use new fixture state on reruns. A timeout wrapper must send SIGTERM first so the harness
   has a chance to stop its child process groups.

## Pass criteria

- The production receive listener reports `/readyz` before use. An initial native push
  produces no cache entry; one worker pass warms the initial raw-byte path. Two subsequent
  pushes change that file while no worker is alive. After receive stops, one new worker
  process publishes exactly the newest changed tip, not the intermediate tip. The exact
  canonical key and a live S3-backed child must exist before any HTTP request for the tip.
- A fresh foreground process returns native Git's exact 200 porcelain bytes and increments
  only the exact body-hit counter, with no foreground computation or body build claim.
  Neither that hit nor another `--once` scan changes the ready immutable body identity.
- Logical eviction expires **every** directory entry for the isolated namespace, including
  WorkSets, completed maps and seeds. The script waits for the old build leases on the DB
  clock; it does not delete S3 objects or manufacture cache payloads/signatures. A new worker
  must publish a fresh payload for the same exact key; another fresh observer must hit it.
- A revoked Alice cannot read her retained signed body; an ungranted Bob gets the same
  empty denial (`500` under the current fixed-principal handler). Rebinding the slot while
  Bob is ungranted publishes nothing. After granting Bob, the worker publishes a distinct
  principal-scoped key; Bob gets a native-exact hit and Alice remains denied. All read holds
  drain and warm passes never change source publication state.
