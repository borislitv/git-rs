# Complete immutable blame

The authenticated HTTP listener also serves complete annotations for a published commit:

```text
GET /team/project.git/blame/FULL_COMMIT_OID?path=PERCENT_ENCODED_PATH
```

Use a full, nonzero 40-digit SHA-1 commit ID and exactly one `path` parameter. The commit
must be reachable from the repository's published refs. Supplying an object ID does not grant
access to unreachable objects that happen to share a stored pack. Read access is repository-wide,
using the same authenticated principal and SQL permissions as Smart HTTP.

For example, curl can prompt for the server-issued token without putting it in the URL:

```sh
curl --fail-with-body --user alice --get \
  --data-urlencode 'path=src/main.rs' \
  --output blame.porcelain \
  'https://git.example.invalid/team/project.git/blame/0123456789abcdef0123456789abcdef01234567'
```

Percent encoding represents filename bytes, not form encoding: `+` is a literal plus.
Non-UTF-8 filenames are supported. Paths are repository-relative, nonempty and at most
4096 decoded bytes; NUL, absolute paths, empty slash components, `.` and `..` are rejected.
Git treats the path literally, including characters that otherwise have pathspec meaning.

## Output and limits

The serving implementation uses the pinned Rust core over a verified physical
snapshot, with native-root, whole-file Myers semantics. Native Git is a test
oracle, not a serving subprocess or fallback. HTTP 200 contains complete output as
`application/octet-stream`, with an exact `Content-Length` and `Cache-Control: no-store`.
Content lines can contain non-UTF-8 bytes. The application does not check out trees, run
text conversion or load client hooks. An independently configured derived cache
may retain complete bodies and authenticated completed-work checkpoints.

The default NativeRootV1 profile explicitly enables the five qualified request-local
tuning switches: line-token reuse, retained current-input tokens, unchanged-run reuse,
bounded resource reuse and change indexing. Its immutable canonical tuning byte is 31.
This does not enable persisted mapping/body reuse or add a production tuning override.
The vendor tuning default and explicit conservative core profile remain all-off.
The synthetic 2000-commit/20000-line C7 profile passed the prepared 5 s and hosted
8 s median gates under a 20 s deadline, 1 GiB job budget and 256 MiB prepared/32 MiB
hosted decoder budgets (aggregate ceiling 4 GiB/2 CPUs/256 tasks). This is not a
general latency guarantee; full joined authentication/runtime and real-image CI
qualification remain pending. See [qualification scope](rust-data-plane.md).

The complete body is buffered before success headers are sent. A failed, cancelled or oversized
worker cannot publish a partial successful annotation. A single query may annotate a blob up to
128 MiB and produce up to 256 MiB of output. These are payload limits, not total-RSS bounds:
allocation capacity, source mappings, caches and snapshot metadata need additional
memory and scratch allowances.

Each physical tree participating in blame may contain up to 128 MiB of tree
records, subject to a smaller session object bound and shared capacity admission.
The 64 KiB commit/tag metadata ceiling is not a total-directory-size limit.
Existing tree-entry framing and per-name validation remain enforced.

Commit-kind and uncompressed blob-size checks use physical Rust library headers,
without inflating complete object bodies or spawning `cat-file`. This includes
delta-compressed objects in verified, source-pinned packs acquired on demand.
These checks do not grant reachability or authorization: the leased published
snapshot, Rust reachability and literal path checks still precede annotation.

By default only one blame computation executes per serving process and another returns 503.
Operators may set `GIT_RS_BLAME_CONCURRENCY` (1..=4, default 1) and
`GIT_RS_BLAME_WAITERS` (0..=3, default 0); their sum must not exceed 4. For example,
concurrency 2 and waiters 1 permits two independent queries to execute and one to wait.
Excess queries return 503, without materialization or a SQL reader lease. Waiting occupies
an ordinary request admission but acquires no scratch, cache pins, reader lease or worker;
it honors cancellation and the original absolute deadline, without a fresh execution budget.
There is no FIFO guarantee. All queries still share the four ordinary request admissions;
health has independent admission. The absolute `GIT_RS_OPERATION_SECONDS` deadline covers the request,
source acquisition, computation and response writes. A queued job abandoned by its
last waiter is cancelled; its actual admission remains held through capture
destruction. An already-running job has an independent finite lifetime: one
departed waiter cannot cancel another waiter or release the job's source early.
Successful and failed jobs release their read hold and private scratch;
a crashed server's committed hold instead remains until its bounded expiry.

Increasing concurrency does not increase the process-private RAM cache budget, SQL pool,
Tokio threads or per-worker thread settings. It is not a memory/CPU isolation mechanism.
Choose it against the pod's aggregate CPU/memory/scratch limits and the selected worker's
own bounded CPU/memory settings, not the host core count. Each executing query can still
select up to the existing 12 GiB source-pack admission limit and buffer up to
256 MiB of output, in addition to worker memory. Completed response buffers can coexist
until their ordinary request admissions finish. Defaults deliberately preserve the old
resource envelope; do not multiply large-worker budgets without aggregate headroom.

| Status | Meaning |
| --- | --- |
| 200 | Complete annotation |
| 400 | Malformed immutable target, query or path |
| 401 | Missing or invalid HTTP credentials |
| 404 | Missing/non-commit/unreachable object, missing path or non-blob path |
| 413 | Uncompressed blob exceeds 128 MiB |
| 500 | Repository authorization/dependency/worker failure or output limit |
| 503 | Request admission or configured executing-plus-waiting blame capacity is occupied |

A deadline or broken connection may close the transport without an HTTP error body. Clients
must require a complete 200 response, not interpret a truncated body as an annotation.

## Authenticated completed-work reuse

`GIT_RS_BLAME_WORKER` is obsolete and rejected at startup. Production exposes no
HTTP tuning, producer override or native-worker selection.

The ordinary optional cache configuration can retain complete bodies. Typed
object/mapping checkpoints additionally require the explicit
`GIT_RS_BLAME_CHECKPOINT_KEY_FILE` setting, alongside the existing cache namespace,
owner and PostgreSQL/S3 configuration. The file must be an absolute path to
exactly 32 random bytes, owned by root or the service UID, with no group/other
permissions. All ancestors must be real protected directories owned by root or
the service UID, with no group/other write permission. A private subdirectory
under world-writable `/tmp` is deliberately insufficient. Provision the key with
the operator's secret-management mechanism; do not log it or pass it in URLs.

This is a trusted-producer MAC contract. Authorized producers sharing checkpoints
must share the namespace, authorized cache owner UUID and key. They must run the
bound producer/options implementation. A checksum, matching producer label or
structurally valid byte partition alone does not prove canonical Myers tie choice.
HMAC authenticates the complete frame and its repository/principal/generation/
storage-epoch/query/producer/options scope. Physical objects additionally verify
their kind, size and Git OID. Mapping units contain both complete byte-line
partitions; no vendor interner state or suspended DAG frontier is persisted.

Rotate the key by provisioning a new protected file and restarting/reloading all
producer instances. New signers reject old checkpoints as optional misses. An
already-running instance retains its old key until replacement; editing the file
does not revoke the key already held in memory.

One derivation imports at most one checkpoint into a temporary owned batch. It
becomes visible only after the outer cache consumption fence and a fresh source
fence succeed. Memory callbacks perform no SQL/network I/O. Complete-edge
boundaries publish only after 256 successful mapping insertions or five seconds,
plus a final complete batch. Metadata streaming cannot evict imported mappings
before their first use. Storage failure skips optional writes; real source,
cancellation and codec-runtime errors remain operation errors.

Bounds are 16 MiB framed record storage plus 3072-record metadata, 2048 retained
mappings, 4 MiB per mapping and 1 MiB per physical object. Actual backing capacity
and temporary copies share the original 1 GiB job capacity budget; these are not
whole-process RSS limits. Source catalogs additionally cap 4096 refs, 1 MiB of
ref names and 4096 indexes; metadata streaming has its existing <=128 MiB bound.
Selected indexes cap at 128 MiB. Graph and original deadline limits remain active.

## Optional post-confirmation warming

Set `GIT_RS_BLAME_WARM_PATH` to one literal percent-encoded repository-relative
path to opt in. It is disabled by default and requires authenticated checkpoint
configuration. It inspects at most 128 changed commands / 64 KiB of names and
selects one changed branch tip; it does not scan trees or discover paths.

Only confirmed `metadata::publish` results can schedule a hint. Warm jobs use the
same CPU pool and actual work admission as queries, plus a single warm admission.
They have no waiter, acquire a fresh authorized source only after dequeue, and
have an independent 30-second deadline assigned before submission. Queue time
consumes that deadline. Generation, storage epoch and branch tip are checked again
after acquisition. Source/output cleanup precedes admission refund.

Warming does not add CPU capacity or alter the default 1/0 rejection policy. It
can occupy that capacity when enabled. Busy scheduling, stale hints, cache/network
outage, warm cancellation and observation failures cannot reject or roll back a
confirmed push. A departed query socket has no ownership of a warm job.

## Actual-work observation

`CompleteResponse::work_statistics()` is optional: a final-body hit or disabled
memo path does not claim avoided diff work. `Observer::work_completed` has a
default no-op implementation and reports completed core traversal/mapping counts
and measured attribution/checkpoint wall time, excluding porcelain encoding.
Imported physical-object copies and imported mapping loads are distinguished from
job-local copies. Core `hits` and `computations`, not load counts, establish actual
mapping reuse. `completed` counts delivered complete units, not persisted rows.

The test-only HTTP fixture supports immutable tuning/reuse selection and actual
work metrics. Performance options force a ready-body miss. These switches and
synthetic signing-key fixtures are not production HTTP/configuration surfaces.

## Verification

Synthetic native integration covers reader credentials, rename history, non-UTF-8 paths and
content, rejection of a valid unreachable commit in a stored pack, the uncompressed-blob bound
and unchanged publication state. Root/core cases cover native output equivalence,
runtime rejection, admission, queued disconnect/expiry, held-response source fences
and resource cleanup. Separate-process checkpoint gates compare actual mapping
hits/computations and native output across a 2001-commit / 2000-edge history.
Authenticated frame, key-file, controlled-clock cadence and capacity-pressure
tests cover the new boundary. Real HTTP-push cases exercise warming and independent
cache-network outage. These do not establish a full-repository performance score
or production HA behavior.

The existing `make integration` and native integration CI job run the concurrency contract
using their already-provisioned disposable SQL/S3 fixtures. CI also explicitly compiles
the Rust HTTP fixture with `make blame-core-build`; this does not execute ignored
PostgreSQL/checkpoint contracts or the artifact/legacy-migration harness.

For the focused core/shared-cache/output/concurrency run, use the public Make targets
below. Prebuild outside measurement, then run `blame-core-check` in a separate bounded
systemd unit. Do not invoke focused provisioning directly or fabricate INVOCATION_ID:
systemd supplies it to both the runner and independent `ExecStopPost` cleanup.

Run from the checkout, with Docker access and the fixture images pinned by
`tests/integration.py` already available locally (focused provisioning uses
`--pull=never`). Set the shell inputs `cpu_quota`, `memory_max` and `tasks_max` to
explicit finite cgroup limits selected for the test host. They are inputs to this
operator wrapper, not git-rs configuration settings or qualified performance budgets.
The wrapper preserves the existing 900-second runtime bound and 40-second stop bound:

```sh
(
  set -eu
  : "${cpu_quota:?select a finite systemd CPUQuota}"
  : "${memory_max:?select a finite systemd MemoryMax}"
  : "${tasks_max:?select a finite systemd TasksMax}"
  checkout="$(pwd -P)"
  target_dir="$(python3 -c 'import os; from pathlib import Path; print(Path(os.environ.get("CARGO_TARGET_DIR", "target")).resolve())')"
  CARGO_TARGET_DIR="$target_dir" make blame-core-build
  cpus="$(python3 -c 'import os; print(",".join(map(str, sorted(os.sched_getaffinity(0))[:2])))')"
  unit="git-rs-blame-core-$(date +%s)-$$"
  cleanup() {
    if [ "$(systemctl show "$unit.service" --property=LoadState --value)" != not-found ]; then
      sudo systemctl stop "$unit.service"
    fi
  }
  trap cleanup EXIT
  sudo systemd-run --wait --pipe --unit="$unit" \
    --working-directory="$checkout" \
    --property="User=$(id -un)" --property="Group=$(id -gn)" \
    --property=RuntimeMaxSec=900 --property=TimeoutStopSec=40 \
    --property=KillMode=control-group --property=MemorySwapMax=0 \
    --property="CPUQuota=$cpu_quota" --property="MemoryMax=$memory_max" \
    --property="TasksMax=$tasks_max" --property="AllowedCPUs=$cpus" \
    --property="ExecStopPost=/usr/bin/make blame-core-cleanup" \
    --setenv="PATH=$PATH" --setenv="HOME=$HOME" \
    --setenv="CARGO_TARGET_DIR=$target_dir" \
    /usr/bin/make blame-core-check
)
```

The Make runner uses prebuilt binaries and retains invocation-specific synthetic
reports/logs. Cleanup removes only containers carrying that invocation's ownership
label. Docker fixtures have their own harness resource caps; the systemd cgroup
does not contain Docker-daemon-owned containers. The wrapper requires measured
runtime, failure/timeout cleanup and resource qualification before becoming a CI gate.
The full public gate has local qualification: 58 selected contracts, 11 migration/
artifact groups, four isolated runtime scenarios and 31 focused serving checks.
The split `blame-images` / `blame` workflow keeps both jobs at 20 minutes and
preparation/measurement units at 900 seconds. Image transfer verifies one
uncompressed archive's checksum, both immutable IDs and the current commit before
preparation; no registry credentials or Docker user configuration are exported.
The first exact-HEAD hosted split execution and cold image-job fit remain pending.
Do not infer those results from this standalone compile step or raise its budgets.

A socket-controlled test observer checks real query overlap/queuing, overflow and
cleanup without replacing attribution. Native Git independently supplies expected
annotations. Synchronization sockets and tuning selectors are test-only. The full
functional/authentication harness must be rerun after a root integration;
pre-join results are not evidence for the joined source. The Stage 5/8 production
performance result remains unqualified.
