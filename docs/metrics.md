# Prometheus metrics

The serving process exposes Prometheus text format 0.0.4 at `/metrics` on its
`--health` listener. The chart binds that listener to pod port **8081**, named
`health`. Scrape every backend pod separately. The SSH/HTTP Service and HTTPRoute
do not expose this port. Use internal pod discovery and the appropriate network
policy; the health listener has no authentication.

No metrics library or background exporter is required. Collection uses bounded
process-local atomic counters and histograms. Labels are closed enumerations;
repository names, principals, refs, object keys, credentials, SQL and error text
never become labels. All series are initialized to zero and reset on process
restart. Scrapes do not perform SQL or S3 requests.

## Metric families

| Family | Type | Labels / meaning |
| --- | --- | --- |
| `git_rs_build_info` | Gauge | `version`, one per build |
| `git_rs_process_start_time_seconds` | Gauge | Instrumentation initialization time since Unix epoch |
| `git_rs_active_requests` | Gauge | Admitted Git transport connections; excludes health handlers |
| `git_rs_cache_allocated_bytes` | Gauge | Cache reservations, entries and reader-pinned files, rounded to 4 KiB; not total RSS |
| `git_rs_timing_logs_dropped_total` | Counter | Diagnostic records discarded by the bounded timing queue; not sink write errors |
| `git_rs_service_logs_dropped_total` | Counter | Fixed service error records discarded by the bounded service queue; not sink write errors |
| `git_rs_requests_total` | Counter | `transport=ssh,http`; `outcome=success,error,unauthorized,bad_request` |
| `git_rs_request_duration_seconds` | Histogram | `transport`; complete admitted connection lifetime including cleanup |
| `git_rs_phase_duration_seconds` | Histogram | `phase=snapshot,materialize,scratch_init,git,pre_receive,validate,graph,upload,publish` |
| `git_rs_phase_errors_total` | Counter | Failed phase outcomes, with the same `phase` labels |
| `git_rs_stage_duration_seconds` | Histogram | Fixed `stage` labels detailed below |
| `git_rs_stage_errors_total` | Counter | Unsuccessful completed/dropped stage spans, with the same `stage` labels |
| `git_rs_dependency_duration_seconds` | Histogram | `operation=sql,s3_get,s3_put,s3_delete,s3_versioning` |
| `git_rs_dependency_errors_total` | Counter | Failed dependency operations, with the same `operation` labels |
| `git_rs_subprocess_duration_seconds` | Histogram | `program=git,curl,sha256sum,other`; SQL, S3 and SHA-256 run in-process |
| `git_rs_subprocess_errors_total` | Counter | Failed spawn or nonzero exit, with the same `program` labels |
| `git_rs_cache_lookups_total` | Counter | `result=hit,miss,bypass`; miss means this caller attempted a fill |
| `git_rs_cache_wait_duration_seconds` | Histogram | Time waiting for another caller's single-flight fill; `_count` counts waits |
| `git_rs_admission_rejections_total` | Counter | `listener=ssh,http,health`; connections closed at the admission limit |
| `git_rs_s3_transferred_bytes_total` | Counter | `direction=download,upload`; verified downloaded / acknowledged uploaded object bytes |

Histograms expose cumulative `_bucket` series and `_sum`/`_count`, with finite
buckets at 1, 5, 10, 25, 50, 100, 250, 500 milliseconds and 1, 2.5, 5, 10, 30, 60,
120 seconds, plus `+Inf`. They measure wall time, not CPU time. Nested phases,
dependencies and subprocess durations **overlap** and must not be added together.

`snapshot` includes committing a reader lease and then reading a consistent
catalog on the same SQL connection. `git` includes the native RPC and its hooks;
`validate` includes graph checks, uploads and publication. `s3_get` includes size
and SHA-256 verification. Cache hits do not add S3 bytes. Failed, partial or
unknown-outcome transfers are not a wire-byte accounting mechanism.
The historical `curl` and `sha256sum` subprocess label values remain in the fixed metric schema;
the current S3 and hashing paths do not increment those subprocess series. S3 connection reuse
and signing remain included in the existing dependency observations.

HTTP discovery and each RPC are separate requests; one Git command can create
multiple requests. HTTP 400/401 responses have their own outcomes. Other parsing,
authorization, dependency or transport failures use `error`; this is not an HTTP
status-code metric. Request observations include failures before repository
selection and record unwinding as an error. Admission rejections have a separate
counter and no admitted-request duration. Subprocess histograms count
application-owned invocations, not every descendant fork inside native Git.
Successful RPC handling is not a successful ref publication: Git can report
rejected ref commands inside a successful RPC. Inspect validation/publication
phase errors as well. SQL measurements include pool acquisition, connection establishment when
needed, complete response consumption and session reset. One observation may send multiple SQL
messages. Background readiness queries and lease cleanup also contribute, so the count is not
simply the Git command count.

## Detailed stages

| `stage` | Inclusive wall-time boundary |
| --- | --- |
| `sql_init` | First process-local database configuration/runtime/pool initialization, not every checkout |
| `sql_acquire` | Pool wait and complete connection establishment when needed |
| `sql_query` | One protocol message, including complete stream consumption and row decoding/callbacks |
| `sql_reset` | Successful-session `ROLLBACK; RESET ALL;` before recycling |
| `lease_commit` | Serving snapshot's first SQL message through committed-lease response completion |
| `catalog_read` | Consistent snapshot message through full catalog consumption and COMMIT completion |
| `lease_release` | Bounded metadata cleanup, including its checkout/query/reset |
| `cache_fill` | Admitted memfd setup, load/verification/sealing and cache publication |
| `cache_link` | Link a verified, reader-pinned cached file into private request scratch |
| `object_hash` | SHA-256 input reads and hashing; a digest mismatch is reported by its enclosing transfer/fill |
| `restore_refs` | Build and execute native ref restoration, including an empty ref set |
| `restore_head` | Native HEAD restoration for non-main defaults; main reuses the empty template |
| `hook_setup` | Private hook socket/nonce and application-owned hook installation |
| `git_input` | Client input forwarding, potentially waiting for EOF; overlaps output and hooks |
| `git_output` | Native stdout/CGI forwarding and response flush |
| `git_wait` | Final child wait after output EOF, not the whole native RPC lifetime |
| `ref_validate` | Native ref-name and expected-old-OID checks, in both hook rounds |
| `tip_validate` | Branch-tip type and fast-forward classification; excludes the separate graph phase |
| `fast_forward` | Native ancestry checks; a non-ancestor's nonzero exit counts as unsuccessful |
| `scratch_cleanup` | Private directory removal, before reader pins are released |
| `process_spawn` | Application-owned child spawn only, separate from subprocess lifetime |

Stages overlap phases, dependencies and other stages. They are not an additive CPU profile.
An error, cancellation or unwind before a timer's successful finish counts as an error.
Administrative commands, probes and cleanup may contribute independently of transport requests.
The retained `program="sha256sum"` series is zero for current in-process hashing.

## Bounded service diagnostics

Serving startup always initializes a separate `tracing-appender` worker with a
1024-record lossy queue, even without debug timing logs. Request errors, hook
rejections and cleanup failures enqueue only fixed `git_rs_error event=...`
records, never formatted errors, credentials or request data. A stalled stderr
sink cannot retain request admission, cleanup owners or an unbounded number of
logging threads. Queue overflow drops records and increments
`git_rs_service_logs_dropped_total`; request/dependency metrics remain available.
The queue and its single sink worker are process-owned, not request-owned.
Its shutdown guard uses the same bounded flushing policy as timing logs below.
Startup status and CLI errors retain their ordinary synchronous stderr behavior.

## Opt-in correlated timing logs

Set `GIT_RS_DEBUG_TIMINGS=1` before starting the process, or chart value `debugTimings: true`.
The setting is initialized once; restart to change it. Prometheus instrumentation remains enabled
without debug logs. Library callers must initialize `metrics::init_timing_logs()` at entry and
retain its process-owned guard outside request paths.

Timing records target stderr and have only these fixed fields:

```text
git_rs_timing level=debug pid=123 operation=42 kind=stage name=sql_acquire elapsed_ms=1.200 duration_ms=0.050 outcome=success
```

`pid` plus the process-local numeric `operation` correlates request, worker, hook and independent
cleanup-budget spans. It is not a durable request identifier or a metric label. `operation=0`
means no bounded operation context; separately bounded probes can have their own IDs without a
transport request. `elapsed_ms` is the span's end relative to the operation origin, and
`duration_ms` is its inclusive duration; values are rounded to three decimal places. No wall
clock, repository, principal, ref, OID, object key, SQL, argv or raw error is recorded.

The maintained `tracing-appender` writer has one background thread and a **1024-record lossy
queue**. Request/drop/SQL paths never wait for sink capacity. Full or disconnected queues drop
records and increment `git_rs_timing_logs_dropped_total`; it does not detect every sink write
error. Crashes, shutdown or stalled sinks can lose records. At process shutdown a separate helper
attempts guard destruction, with at most 1200 ms acknowledgement wait; no blocked worker is
joined. The upstream appender may print a generic stdout shutdown notice when its queue is full.

Keep debug-enabled diagnostic trials separate from scored performance cohorts: formatting,
queueing and log collection add overhead. Use the histogram deltas, correlated span intervals
and container CPU/throttling together; do not sum overlapping span durations.

## Scraping

For a Prometheus installation supporting annotation-based pod discovery:

```yaml
podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8081"
  prometheus.io/path: /metrics
```

For an existing Prometheus Operator installation, a separately managed PodMonitor
can select `app.kubernetes.io/instance: example` and scrape port `health` at
`/metrics`. The chart does not install a monitoring operator or its CRDs.

## Useful PromQL

Request latency by transport:

```promql
histogram_quantile(0.95,
  sum by (le, transport) (rate(git_rs_request_duration_seconds_bucket[5m]))
)
```

Mean phase wall time and SQL invocations per second:

```promql
sum by (phase) (rate(git_rs_phase_duration_seconds_sum[5m]))
/
sum by (phase) (rate(git_rs_phase_duration_seconds_count[5m]))

sum(rate(git_rs_dependency_duration_seconds_count{operation="sql"}[5m]))
```

Separate pool/connect waits from query/reset costs:

```promql
sum by (stage) (rate(git_rs_stage_duration_seconds_sum{stage=~"sql_acquire|sql_query|sql_reset"}[5m]))
/
sum by (stage) (rate(git_rs_stage_duration_seconds_count{stage=~"sql_acquire|sql_query|sql_reset"}[5m]))
```

Cache hit fraction and admission rejection rate:

```promql
sum(rate(git_rs_cache_lookups_total{result="hit"}[5m]))
/
sum(rate(git_rs_cache_lookups_total[5m]))

sum by (listener) (rate(git_rs_admission_rejections_total[5m]))
```

Measure container CPU usage and throttling using the platform's cgroup/cAdvisor
metrics. In particular, a small SSH quota covers crypto, authentication helpers
and the bridge for **all** simultaneous connections. Spare backend/SQL/S3 quota
cannot be borrowed across container limits. Compare code changes with unchanged
allocations, and assess resource rebalancing separately at a fixed total budget.
