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

Only one blame computation executes per serving process; another returns 503 rather than
queuing behind it. It also occupies one of the four ordinary request admissions. Health has
independent admission. The absolute `GIT_RS_OPERATION_SECONDS` deadline covers the request,
materialization, computation and response writes. Client disconnection cancels the owned native
process group. Successful and failed requests release their read hold and private scratch;
a crashed server's committed hold instead remains until its bounded expiry.

| Status | Meaning |
| --- | --- |
| 200 | Complete annotation |
| 400 | Malformed immutable target, query or path |
| 401 | Missing or invalid HTTP credentials |
| 404 | Missing/non-commit/unreachable object, missing path or non-blob path |
| 413 | Uncompressed blob exceeds 128 MiB |
| 500 | Repository authorization/dependency/worker failure or output limit |
| 503 | Request admission or the single blame slot is occupied |

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
These contracts do not establish a full-repository performance score or production HA behavior.
