# Independent review and qualification

The rc.3 changes received separate review passes for metadata/publication/history and for
transport/authentication/resource ownership, followed by integration and deployment review.
These are engineering checks, not a security certification or production HA acceptance.

Follow-up review of the metrics and leased-snapshot changes found no blocking correctness issue.
Residual coverage gaps include simultaneous lease acquisition/revocation races, commit-boundary
lease cleanup loss, and cancellation-specific metric outcomes. Existing fencing, late-stream
failure, transport cancellation and publication-loss tests remain required.

The Tokio migration received two additional independent report-only passes using the shared
`yanet-revew` protocol, with its lifecycle/ownership/end-to-end checks adapted to Rust. Reviewers
consulted the official Rust, Tokio, driver and PostgreSQL documentation linked below. They found
and follow-up-verified fixes for full connection-creation timeout, retained snapshot accounting,
and a cancellation test that previously checked invisible writes without proving rollback.
The timeout and budget regressions were observed failing before their fixes and passing afterward.
Disabling CancelRequest and discarding successful sessions also made the cancellation and session-
reuse tests fail at their intended assertions; restoring the implementation passed all contracts.

## Confirmed issues and resolutions

| Issue | Resolution | Regression evidence |
| --- | --- | --- |
| Legal refs or a growing catalog exceeded the aggregate SQL capture limit | Stream one consistent transaction, bound each row and retained snapshot, and require successful completion | 8,100 long refs and 40,000 packs failed the former capture limit and pass streaming |
| Namespace validation performed a quadratic scan | Indexed byte-range descendant probes, with transactional namespace rejection preserved | Real PostgreSQL long-ref publication and namespace-boundary mutation checks |
| A slow health request starved all probes | Separate eight-request admission and absolute one-second lifetime | Concurrent real-socket liveness request, saturation and trickling-input regressions |
| Blocked output outlived an operation deadline | Absolute transport watchdog closes both socket directions; preserve the same budget through parsing and execution | Unread socket, reaped subprocess and restored admission; disabling interruption fails the test |
| Development HTTP headers could retain admission indefinitely | Start the absolute budget before header parsing | Trickling-header and subsequent-admission tests |
| Routing the development listener would expose a fixed unauthenticated identity | Separate opt-in Secret-backed HTTP Basic authentication and per-request repository ACLs | Missing/wrong credentials, distinct principals, reader writes and cross-repository access |
| Omitting HTTPRoute retries inherited Istio retry defaults | Explicit `retry.attempts: 0` | Effective Envoy route assertion failed before the change and passes afterward |
| HTTP could collide with the SSH listener or SSH Service port | Reject port 2222 and duplicate Service ports while SSH is enabled | Both render-rejection tests failed before their guards; HTTP-only use of port 2222 remains valid |
| Short-lived SQL calls launched processes, reconnected and read the full catalog twice | Use Tokio/tokio-postgres and a bounded deadpool-postgres pool; confirm an authorized hold before one consistent snapshot on the same connection | Session reuse/reset, cancellation rollback, real PostgreSQL authorization, maintenance fencing, late-read cleanup and unknown COMMIT recovery |
| A small SSH CPU limit serialized otherwise independent work | Increase the chart's default SSH limit to one CPU and document independent-container quota effects | Controlled fixed-total-budget resource rebalancing and cgroup throttling observations; application metrics expose phase costs separately |
| TCP connect timeout did not cover stalled PostgreSQL startup/TLS/authentication | Five-second deadpool create timeout, bounded by the remaining operation deadline | A TCP-accepting stalled startup fixture fails the prior implementation at ten seconds and passes with the five-second cap |
| Per-row streaming limit did not bound retained snapshot allocations | Conservative per-snapshot budget checked before ref/pack retention | Empty snapshot fits the minimum budget; growth fails, returns no partial snapshot, releases its known hold and recovers after deletion |
| Invisible uncommitted writes could falsely satisfy the cancellation regression | Capture the executing PID and require backend termination before checking rollback | Real PostgreSQL backend disappears before zero-row/readback and pool-capacity assertions |

The authenticated native-client test also found a fixture error: a shared credential store could
return another actor's valid credential after Git's challenge/retry. Actor-specific stores now
verify credential selection and require every remote ref to remain unchanged after a denied push.
The server did not accept a verified reader as an owner in this reproduction.

## Qualification scope

- Safe application Rust with Tokio and an in-process bounded PostgreSQL pool; complete hook command
  lists, native Git graph validation and confirmed transactional publication remain required.
- Rust 1.99.1 formatting, strict Clippy, core/authentication/transport tests, and opt-in real
  PostgreSQL tests. Verified HTTP authentication reaches metadata directly without a SQL CLI.
- Native Git/SSH and authenticated HTTP integration, backup/readback/truncation, concurrent writes,
  dependency faults, unknown PUT/COMMIT outcomes and cold process reconstruction.
- Helm 3/4 contracts and official pinned CNPG/Barman/OBC/Gateway API schemas. Invalid configuration
  tests include missing HTTP credentials, missing gateway parents and removed platform options.
- Real HTTPS routing on Istio 1.31.3 with Gateway API 1.7.0: current route/gateway conditions,
  certificate validation, authenticated native Git, restart readback, STRICT mTLS, gateway identity
  authorization, rejection of plaintext/non-mesh access, and effective retry-policy inspection.

The Tokio migration repeated native integration/fault recovery and the real k3s Helm workflow,
including positive PostgreSQL TLS and rejection of an untrusted CA or mismatched hostname.
The isolated Istio routing qualification above predates the driver migration and was not repeated.

`tests/istio.py` provisions its own disposable cluster and synthetic identities. It does not
install or modify platform resources in an application's production environment.

## Official-practice reassessment

- [Tokio synchronous bridging](https://tokio.rs/tokio/topics/bridging): a shared multi-thread runtime
  keeps pooled connection tasks running between calls; the transport's synchronous API can use
  `Runtime::block_on` without creating a runtime for every query.
- [Tokio cancellation](https://tokio.rs/tokio/tutorial/select) and
  [timeout cooperation](https://docs.rs/tokio/1.54.1/tokio/time/fn.timeout.html): dropping a query
  future does not by itself prove server rollback. Discard the session, attempt bounded cancellation,
  and check the operation while consuming rows, yielding periodically.
- [PostgreSQL connect timeout](https://docs.rs/tokio-postgres/0.7.18/tokio_postgres/config/struct.Config.html#method.connect_timeout)
  is socket-level; [deadpool create timeout](https://docs.rs/deadpool/0.13.1/deadpool/managed/struct.Timeouts.html#structfield.create)
  covers complete session creation.
- [Simple-query protocol](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY):
  a row or statement completion is not whole-batch/COMMIT confirmation. Fully consume replies;
  confirm lease creation in a separate message before potentially blocked catalog streaming.
- [Rust Vec guarantees](https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees): retained
  collection growth needs its own budget; bounded rows do not establish bounded consumer memory.
- [Git hooks](https://git-scm.com/docs/githooks): pre-receive acceptance is not publication;
  proc-receive owns the intercepted updates. Unconditional multi-ref atomicity must be tested
  independently of client `--atomic`.
- [Receive-pack](https://git-scm.com/docs/git-receive-pack): quarantine validation must not publish
  references to quarantined objects before the complete operation is accepted.
- [Rust child lifecycle](https://doc.rust-lang.org/std/process/struct.Child.html): dropping a child
  handle does not terminate or reap it. Deadline and error paths must own process-tree cleanup.
- [Docker builds](https://docs.docker.com/build/building/best-practices/): digest pins and archive
  hashes complement regular rebuilds; cached APT layers do not acquire later security fixes.
- [PostgreSQL synchronous commit](https://www.postgresql.org/docs/18/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)
  controls acknowledgement durability; it is not itself a complete failover protocol.
- [CNPG failover quorum](https://cloudnative-pg.io/docs/1.30/failover/#failover-quorum-quorum-based-failover):
  `ANY 1` cannot alone establish lossless promotion. Evaluate the operator's experimental quorum
  feature and test the actual partition/fencing topology.
- [Rook bucket claims](https://rook.io/docs/rook/v1.21/Storage-Configuration/Object-Storage-RGW/ceph-object-bucket-claim/):
  the application orders an OBC from an existing administrator-managed StorageClass and consumes
  its generated Secret/ConfigMap; it does not provision Ceph/RGW.
- [Gateway API routing](https://gateway-api.sigs.k8s.io/guides/user-guides/http-routing/) and
  [Istio Gateway API](https://istio.io/latest/docs/tasks/traffic-management/ingress/gateway-api/):
  route attachment, TLS, authenticated identity, mesh policy and effective retry behavior are
  separate contracts that need runtime checks.

## Residual limitations

Requests still materialize the complete catalog. The RAM cache bound is not a bound on total
subprocess memory, metadata, scratch or node resources. The backend does not implement graceful
request draining, lazy object access, version-aware purge, automatic compaction, coordinated PITR,
Git LFS or filtered clones. Production CNPG failover and Ceph/RGW durability/retention require
separate acceptance. An isolated routing test does not establish production mesh policy.

Performance must be measured against the intended workload with all dependencies included.
See [benchmark methodology](benchmarking.md); passing correctness tests does not establish a
universal speed advantage over GitLab or native Git.
