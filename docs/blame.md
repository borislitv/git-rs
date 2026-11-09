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

The default implementation executes native Git with `blame --root --line-porcelain
--no-textconv --no-progress`. HTTP 200 contains the complete output as
`application/octet-stream`, with an exact `Content-Length` and `Cache-Control: no-store`.
Content lines can contain non-UTF-8 bytes. The application does not check out trees, run
text conversion or load client hooks. It does not retain annotation results between requests.

The complete body is buffered before success headers are sent. A failed, cancelled or oversized
worker cannot publish a partial successful annotation. A single query may annotate a blob up to
128 MiB and produce up to 256 MiB of output. These are payload limits, not total-RSS bounds:
allocation capacity, native processes, materialization and snapshot metadata need additional
memory and scratch allowances.

Commit-kind and uncompressed blob-size checks use physical Rust library headers,
without inflating complete object bodies or spawning `cat-file`. This includes
loose objects and delta-compressed objects in verified, request-pinned RAM packs.
These checks do not grant reachability or authorization: the leased published
snapshot, native reachability and literal path checks still precede annotation.

By default only one blame computation executes per serving process and another returns 503.
Operators may set `GIT_RS_BLAME_CONCURRENCY` (1..=4, default 1) and
`GIT_RS_BLAME_WAITERS` (0..=3, default 0); their sum must not exceed 4. For example,
concurrency 2 and waiters 1 permits two independent queries to execute and one to wait.
Excess queries return 503, without materialization or a SQL reader lease. Waiting occupies
an ordinary request admission but acquires no scratch, cache pins, reader lease or worker;
it honors cancellation and the original absolute deadline, without a fresh execution budget.
There is no FIFO guarantee. All queries still share the four ordinary request admissions;
health has independent admission. The absolute `GIT_RS_OPERATION_SECONDS` deadline covers the request,
materialization, computation and response writes. Client disconnection cancels the owned native
process group. Successful and failed requests release their read hold and private scratch;
a crashed server's committed hold instead remains until its bounded expiry.

Increasing concurrency does not increase the process-private RAM cache budget, SQL pool,
Tokio threads or per-worker thread settings. It is not a memory/CPU isolation mechanism.
Choose it against the pod's aggregate CPU/memory/scratch limits and the selected worker's
own bounded CPU/memory settings, not the host core count. Each executing query can still
materialize a snapshot up to the existing 12 GiB scratch admission limit and buffer up to
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

## Optional operator-owned worker

`GIT_RS_BLAME_WORKER` selects an existing absolute executable file at server startup.
Unset means native Git. The executable and its dependencies must be supplied by the operator's
runtime image; the application chart and standard image do not install a separate worker.
Relative, missing, directory and non-executable targets are rejected before serving state is
created. This setting is server configuration, never a command selected by a client.

The executable receives three separate OS arguments:

```text
WORKER PRIVATE_BARE_REPOSITORY FULL_COMMIT_OID REPOSITORY_RELATIVE_PATH
```

The application has already authorized and leased the snapshot, checked commit reachability,
resolved the literal blob path and enforced the uncompressed size limit. The worker must emit
a complete line-porcelain annotation to stdout and exit zero. Different valid grouping of
adjacent records is permitted; each line's commit, original position, metadata and content must
remain correct. Nonzero exit or oversized output fails the entire response; there is no automatic
retry or fallback after a worker error.

The child environment contains a fixed `PATH`, `HOME`, `LC_ALL` and
`GIT_RS_BLAME_REMAINING_MILLIS`, the remaining absolute budget at launch. Service SQL/S3
credentials and arbitrary parent environment variables are not inherited. Standard input is
closed and diagnostic stderr is suppressed. Worker descendants must stay in the inherited
process group so request cancellation can terminate them. The operator owns executable
deployment integrity, worker correctness and its CPU/memory limits; executable selection alone
does not establish annotation equivalence or a performance improvement.

## Verification

Synthetic native integration covers reader credentials, rename history, non-UTF-8 paths and
content, rejection of a valid unreachable commit in a stored pack, the uncompressed-blob bound
and unchanged publication state. Separate worker cases check native output equivalence,
credential isolation, failed/oversized output, occupied admission, client disconnect, deadline,
closed-output cancellation, descendant reaping, read-hold/scratch release and admission reuse.
An intermediate migration contract permits at most three native Git commands for
a valid annotation and two for an oversized blob; header type/size probes do not
consume a native subprocess. Library tests compare physical headers with native
Git and read 128 MiB loose/ref-delta blobs under a 96 MiB address-space limit.
These contracts do not establish a full-repository performance score or production HA behavior.

The existing `make integration` and native integration CI job run the concurrency contract
using their already-provisioned disposable SQL/S3 fixtures. For a focused run without the
large integration fixture, use `python3 tests/integration.py --provision --binary
target/debug/git-rs --blame-concurrency-only`. Omit `--provision` to use disposable endpoints
configured as for the integration harness (`PGPORT` and `GIT_RS_S3_ENDPOINT`, with the
synthetic bucket already created). The contract creates its own tiny synthetic repository.
A socket-controlled worker proves two distinct HTTP queries overlap, a third waits without
materialization, overflow returns 503, and success, worker failure, execution disconnect,
queued disconnect and queued absolute expiry return capacity. Native Git independently
supplies the expected complete annotations. The synchronization socket is test-only;
the serving executable boundary and environment remain unchanged.
