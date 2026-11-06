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
| `git_rs_cache_allocated_bytes` | Gauge | Cache reservations, entries and reader-pinned buffers; not total RSS |
| `git_rs_requests_total` | Counter | `transport=ssh,http`; `outcome=success,error,unauthorized,bad_request` |
| `git_rs_request_duration_seconds` | Histogram | `transport`; complete admitted connection lifetime including cleanup |
| `git_rs_phase_duration_seconds` | Histogram | `phase=snapshot,materialize,scratch_init,git,pre_receive,validate,graph,upload,publish` |
| `git_rs_phase_errors_total` | Counter | Failed phase outcomes, with the same `phase` labels |
| `git_rs_dependency_duration_seconds` | Histogram | `operation=sql,s3_get,s3_put,s3_delete,s3_versioning` |
| `git_rs_dependency_errors_total` | Counter | Failed dependency operations, with the same `operation` labels |
| `git_rs_subprocess_duration_seconds` | Histogram | `program=git,curl,sha256sum,other`; SQL has no subprocess |
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
