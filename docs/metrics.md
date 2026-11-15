# Prometheus metrics

The serving process exposes Prometheus text format 0.0.4 at `/metrics` on its
`--health` listener. The chart binds that listener to pod port **8081**, named
`health`. Scrape every backend pod separately. The SSH/HTTP Service and HTTPRoute
do not expose this port. Use internal pod discovery and the appropriate network
policy; the health listener has no authentication.

No metrics library or background exporter is required. Collection uses bounded
process-local atomic counters and histograms. Labels are closed enumerations;
repository names, principals, refs, object keys, credentials, SQL and error text
never become labels. Counters and histogram accumulators start at zero and reset
on process restart; gauges report current state or static metadata. Scrapes do
not perform SQL or S3 requests.

## Metric families

| Family | Type | Labels / meaning |
| --- | --- | --- |
| `git_rs_build_info` | Gauge | Cargo package `version`; does not distinguish builds of the same version |
| `git_rs_process_start_time_seconds` | Gauge | Instrumentation initialization time since Unix epoch |
| `git_rs_active_requests` | Gauge | Admitted Git transport connections; excludes health handlers |
| `git_rs_cache_allocated_bytes` | Gauge | RAM cache reservations, entries and reader-pinned buffers; not total RSS |
| `git_rs_timing_logs_dropped_total` | Counter | Diagnostic records discarded by the bounded timing queue; not sink write errors |
| `git_rs_service_logs_dropped_total` | Counter | Fixed service error records discarded by the bounded service queue; not sink write errors |
| `git_rs_requests_total` | Counter | `transport=ssh,http`; `outcome=success,error,unauthorized,bad_request` |
| `git_rs_request_duration_seconds` | Histogram | `transport`; complete admitted connection lifetime including cleanup |
| `git_rs_phase_duration_seconds` | Histogram | `phase=snapshot,materialize,scratch_init,git,pre_receive,validate,graph,upload,publish,receive_pack_prepare,upload_serve` |
| `git_rs_phase_errors_total` | Counter | Failed phase outcomes, with the same `phase` labels |
| `git_rs_stage_duration_seconds` | Histogram | Fixed `stage` labels detailed below |
| `git_rs_stage_errors_total` | Counter | Unsuccessful completed/dropped stage spans, with the same `stage` labels |
| `git_rs_dependency_duration_seconds` | Histogram | `operation=sql,s3_get,s3_put,s3_delete,s3_versioning` |
| `git_rs_dependency_errors_total` | Counter | Failed dependency operations, with the same `operation` labels |
| `git_rs_subprocess_duration_seconds` | Histogram | `program=git,curl,sha256sum,other`; SQL, S3 and SHA-256 run in-process |
| `git_rs_subprocess_errors_total` | Counter | Failed spawn or nonzero exit, with the same `program` labels |
| `git_rs_cache_lookups_total` | Counter | `result=hit,miss,bypass`; miss means this caller attempted a fill |
| `git_rs_cache_wait_duration_seconds` | Histogram | Time waiting for another caller's single-flight fill; `_count` counts waits |
| `git_rs_cache_waiters` | Gauge | Callers currently in a single-flight wait scope |
| `git_rs_admission_rejections_total` | Counter | `listener=ssh,http,health`; connections closed at the admission limit |
| `git_rs_s3_transferred_bytes_total` | Counter | `direction=download,upload`; length-checked downloaded payload / acknowledged uploaded payload, including multipart parts; not proof of object validation or publication |
| `git_rs_maintenance_active_jobs` | Gauge | Currently running maintenance jobs; maintenance reports zero Git transport active requests |
| `git_rs_maintenance_leader` | Gauge | Current maintenance leadership state |
| `git_rs_maintenance_jobs_completed_total` | Counter | Successfully completed maintenance jobs |
| `git_rs_maintenance_jobs_failed_total` | Counter | Failed maintenance jobs |

Histograms expose cumulative `_bucket` series and `_sum`/`_count`, with finite
buckets at 1, 5, 10, 25, 50, 100, 250, 500 milliseconds and 1, 2.5, 5, 10, 30, 60,
120 seconds, plus `+Inf`. They measure wall time, not CPU time. Nested phases,
dependencies and subprocess durations **overlap** and must not be added together.

`snapshot` includes authorized source acquisition, committing a reader lease and
reading a consistent catalog. `graph` measures Rust receive graph validation;
`upload` measures receive pack/index storage and `publish` measures metadata
publication. `receive_pack_prepare` measures the inclusive `pack::prepare` call,
including input reads, pack normalization and base resolution. `upload_serve`
measures Rust upload preparation, advertisement/negotiation, protocol writes and
final checks; it excludes earlier admission, source acquisition and HTTP body
decoding. Both are inclusive wall time, not CPU time.

The legacy phase labels `materialize`, `scratch_init`, `git`, `pre_receive` and
`validate` remain exported for compatibility but have no production observations
in the Rust data plane. All subprocess labels (`git`, `curl`, `sha256sum`, `other`)
and their counter/histogram families are retained. Zero subprocess/native-call
counters are not proof that native execution is impossible: that requires the
independent runtime/security gates, exact-image inspection and operation evidence.
Synthetic metric observations in tests do not establish production coverage.

S3 GET measurements include transport framing/length checks; whole-object reads
also verify SHA-256, while range bytes are counted before higher-level object
validation. Multipart upload bytes count acknowledged parts even if completion
later fails. Cache hits do not add S3 bytes. These counters are not wire-byte,
durability or ref-publication accounting.

Cache waits are observed exactly once per caller that encounters a busy flight,
including error, cancellation and unwind. Repeated timed wakeups do not create
additional observations. Nonwaiting cache hits/fills do not count as waits.

HTTP discovery and each RPC are separate requests; one Git command can create
multiple requests. HTTP 400/401 responses have their own outcomes. Other parsing,
authorization, dependency or transport failures use `error`; this is not an HTTP
status-code metric. Request observations include failures before repository
selection and record unwinding as an error. Admission rejections have a separate
counter and no admitted-request duration. Retained subprocess histograms must not
be interpreted as an exhaustive execution audit.
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
| `cache_fill` | Actual admitted RAM loader callback, including its verification; excludes allocation and cache publication |

The following stage labels remain exported but are inactive in production:
`cache_link`, `object_hash`, `restore_refs`, `restore_head`, `hook_setup`,
`git_input`, `git_output`, `git_wait`, `ref_validate`, `tip_validate`,
`fast_forward`, `scratch_cleanup`, `process_spawn`. They are not measurements
of their corresponding current Rust operations.

Stages overlap phases, dependencies and other stages. They are not an additive CPU profile.
An error, cancellation or unwind before a timer's successful finish counts as an error.
Administrative commands, probes and cleanup may contribute independently of transport requests.
The retained `program="sha256sum"` series is zero for current in-process hashing.

## Identity and live scrape freshness

No Prometheus instance or target is provisioned by this instrumentation. A local
CLI timing log, successful `/metrics` curl or static source inspection is not a
live scrape-freshness verdict.

`git_rs_build_info` contains package version only, not source revision or image
identity. `git_rs_process_start_time_seconds` is the registry initialization time,
not OS process birth time. It is stable during one registry lifetime. Capture
the exact running container image ID/start time/restart count, or Kubernetes Pod
UID/container ID/resolved imageID/start time/restart count, alongside the release
receipt. Resolve registry manifest digests versus platform/config image IDs using
that receipt; they are not interchangeable identifiers. Keep these identities in
the fixture receipt, not unbounded application metric labels.

For an explicitly selected, owned serving target and reachable Prometheus URL:

1. Read `/api/v1/targets?state=active`. Match one exact scrape URL and its
   `job`/`instance` labels to the runtime receipt. Require `health="up"`, empty
   `lastError`, recent `lastScrape`, and an advancing `lastScrape` on a later poll.
   Set a finite deadline and an explicit freshness allowance derived from the
   scrape interval and timeout, with synchronized clocks.
2. Query `/api/v1/query` for the following expressions, replacing both selectors
   with the exact target labels. Require a unique result, `up=1`, and recent,
   advancing sample timestamps. Read the value of `timestamp(...)`, not merely
   the query response's evaluation timestamp.

   ```promql
   up{job="FIXTURE_JOB",instance="EXACT_TARGET"}
   timestamp(git_rs_build_info{job="FIXTURE_JOB",instance="EXACT_TARGET"})
   timestamp(git_rs_requests_total{job="FIXTURE_JOB",instance="EXACT_TARGET",transport="http",outcome="success"})
   ```

3. With other transport traffic quiescent, baseline after a completed scrape.
   Send one successful synthetic HTTP discovery request. After a newer successful
   scrape require HTTP success and HTTP duration count each increase by exactly
   one. Repeat; then send one malformed request and require `bad_request` increase
   by one with success unchanged. Scrapes/probes alone must not change transport
   counts. A whole clone/push can issue multiple HTTP requests. Background SQL
   readiness/lease activity prevents exact SQL-count expectations.
4. Hold one admitted incomplete transport connection, synchronize on an active
   request sample, close it and require the gauge return to baseline. Exercise a
   controlled cold source read for cache/S3 evidence; idle unchanged gauges are
   not stale if their sample timestamps advance. Do not require retained cache
   buffers to drop to zero while entries/readers remain live.
5. In an authorized disposable fixture only, pre-increment a selected counter,
   record the receipt, and restart only the owned application. Require a new
   runtime receipt, newer instrumentation initialization, fresh target scrapes
   and a counter reset before repeating the controlled request. For unchanged
   series identity corroborate with a decrease or `resets(...[window])`; a new
   pod/instance creates a new series and need not show a PromQL reset. Inspect
   old and new target identities separately. Never reset shared/protected k3s.

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
