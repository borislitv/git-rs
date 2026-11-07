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

The shared-file/materialization work received separate ownership/cache and native-consumer review
passes. Reviewers followed direct curl output through sealing, integrity checks, LRU eviction,
request pins, `/proc` reopening and native Git. A separate observability/SQL review found blocking
stderr writes and an upstream logging-guard stdout fallback; both were reproduced and fixed with
a bounded lossy writer and off-thread, bounded-wait shutdown. Follow-up review found no remaining
blocker in those changes. This does not establish production memory or latency bounds.

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
| Cache hits copied complete payloads into private scratch | Verified sealed memfds with request-owned pins and private symlinks | Native lifecycle pauses a second pack after eviction, verifies the first pinned pack/idx with Git, then checks pin release and clone/fsck |
| Preallocated memfd length could conceal a short download | Check shared write offset before rewind/hash and before cache publication | Short, oversized and corrupt responses are rejected on cache and scratch paths, followed by successful recovery |
| Default Ubuntu timeout rejected legal non-UTF-8 native arguments | Install coexisting GNU coreutils and use `/usr/bin/gnutimeout` explicitly | Direct native comparison plus non-UTF-8/default/unborn HEAD roundtrips |
| Reused empty templates could leak request HEAD/hooks if captured too late | Capture only newly initialized application-owned bare state | A non-UTF-8 HEAD and installed hooks in one scratch do not affect another; removing template HEAD fails the regression |
| Synchronous timing output could pin request admission and SQL sessions | Bounded lossy `tracing-appender` queue, one complete fixed-field record per write | A full unread sink fails the synchronous implementation; the fixed path releases admission and completes real session reuse/cancellation contracts |
| Upstream logging guard could block on its stdout shutdown fallback | Process-owned guard destruction on a helper with 1200 ms acknowledgement wait | Handshake proves request completion first, then fills stdout and stderr; the former guard hangs and the bounded wrapper exits |
| Stateful test dependencies could overlap writers on a reused persistent volume during RollingUpdate | Use Recreate for the single-instance PostgreSQL/S3 fixtures | An isolated PVC-backed PostgreSQL replacement retains its repository and authorization metadata through native Git readback |

Additional failure-sensitivity checks temporarily removed write seals, short-fill checking, the
file cap and waiter deadline in one **combined** mutation run; all four corresponding regressions
failed. Separate mutations removed scratch pins, template HEAD, cleanup correlation and the lease
stage observation. Their intended assertions failed; every mutation was restored before final
verification. These are targeted checks, not an exhaustive mutation score.

The authenticated native-client test also found a fixture error: a shared credential store could
return another actor's valid credential after Git's challenge/retry. Actor-specific stores now
verify credential selection and require every remote ref to remain unchanged after a denied push.
The server did not accept a verified reader as an owner in this reproduction.

### In-process S3 follow-up

The shared HTTP transport replaces per-object curl processes with reqwest and the official
AWS SigV4 signer. The connection-reuse and cancellation regressions were first observed
failing on the subprocess path: three sequential calls opened three connections, and a
cancelled fill waited for the peer. Both pass with the shared client. Additional real-socket
tests cover redirect refusal, oversized bucket policy, failed-fill recovery and isolated
certificate trust/hostname verification. Deliberately permitting a redirect, increasing the
policy budget and disabling hostname verification made the corresponding regressions fail;
a separate digest-check mutation accepted corrupt bytes and failed its rejection assertion.
All mutations were restored. The complete native integration and dependency-fault
suites passed with 64 MiB synthetic data, signature verification by the real S3 fixture,
SSH/authenticated HTTP, cold reconstruction and unknown PUT/COMMIT behavior. These are
correctness observations, not comparative GitLab timings or a new HA qualification.

The final optimized binary repeated the 64 MiB integration and fault suites with
timing diagnostics disabled. The exact test image also passed the real Helm/k3s
smoke with two replicas, verified TLS PostgreSQL/S3, SSH and authenticated HTTP,
identity rejection, cold restart and migration on upgrade. This disposable smoke
uses a direct Docker bridge; it is not a new gateway/mesh or HA qualification.

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

The shared-file/observability changes passed 38 ordinary Rust tests and twelve opt-in PostgreSQL
contracts, with focused reruns after logging-guard fixes. Native integration repeated with 64 MiB
synthetic data and enabled timings; it verifies stage observations and request/worker/cleanup IDs
through SSH and HTTP. The default-off fault suite also passed. Helm 3/4 rejected thirty invalid
profiles and passed pinned operator schemas. Real k3s checked TLS PostgreSQL/S3, unknown SSH keys,
authenticated HTTP, cold restart and upgrade. The final runtime also repeated the isolated Istio
routing qualification above, including actual Envoy retry configuration and mTLS telemetry.

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
- [Linux memfd](https://man7.org/linux/man-pages/man2/memfd_create.2.html) and
  [proc file descriptors](https://man7.org/linux/man-pages/man5/proc_pid_fd.5.html): sealing and
  descriptor ownership, plus runtime access checks, govern native reopening and reader lifetime.
  [memfd 0.6.6](https://docs.rs/memfd/0.6.6/memfd/) supplies the maintained safe application API.
- [File cloning](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_clone) shares the
  write position; logical length on a preallocated, size-sealed file is not a completed transfer.
- [Git 2.56 update-ref](https://github.com/git/git/blob/v2.56.0/Documentation/git-update-ref.adoc):
  native transaction semantics rejected batching HEAD with its referent; the implementation keeps
  native symbolic-ref for non-main defaults instead of inventing a reference-file shortcut.
- [tracing-appender 0.2.5 source](https://docs.rs/tracing-appender/0.2.5/src/tracing_appender/non_blocking.rs.html):
  lossy mode uses a bounded `try_send`; its guard's stdout fallback must not run on request or main
  shutdown paths. The wrapper's worker transfer uses safe `ManuallyDrop` ownership and no join.
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
