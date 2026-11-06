# Independent review and qualification

The rc.3 changes received separate review passes for metadata/publication/history and for
transport/authentication/resource ownership, followed by integration and deployment review.
These are engineering checks, not a security certification or production HA acceptance.

Follow-up review of the metrics and leased-snapshot changes found no blocking correctness issue.
Residual coverage gaps include simultaneous lease acquisition/revocation races, commit-boundary
lease cleanup loss, and cancellation-specific metric outcomes. Existing fencing, late-stream
failure, transport cancellation and publication-loss tests remain required.

## Confirmed issues and resolutions

| Issue | Resolution | Regression evidence |
| --- | --- | --- |
| Legal refs or a growing catalog exceeded the aggregate SQL capture limit | Stream one consistent transaction through bounded cursor batches; bound each row; require successful completion | 8,100 long refs and 40,000 packs failed the former capture limit and pass streaming |
| Namespace validation performed a quadratic scan | Indexed byte-range descendant probes, with transactional namespace rejection preserved | Real PostgreSQL long-ref publication and namespace-boundary mutation checks |
| A slow health request starved all probes | Separate eight-request admission and absolute one-second lifetime | Concurrent real-socket liveness request, saturation and trickling-input regressions |
| Blocked output outlived an operation deadline | Absolute transport watchdog closes both socket directions; preserve the same budget through parsing and execution | Unread socket, reaped subprocess and restored admission; disabling interruption fails the test |
| Development HTTP headers could retain admission indefinitely | Start the absolute budget before header parsing | Trickling-header and subsequent-admission tests |
| Routing the development listener would expose a fixed unauthenticated identity | Separate opt-in Secret-backed HTTP Basic authentication and per-request repository ACLs | Missing/wrong credentials, distinct principals, reader writes and cross-repository access |
| Omitting HTTPRoute retries inherited Istio retry defaults | Explicit `retry.attempts: 0` | Effective Envoy route assertion failed before the change and passes afterward |
| HTTP could collide with the SSH listener or SSH Service port | Reject port 2222 and duplicate Service ports while SSH is enabled | Both render-rejection tests failed before their guards; HTTP-only use of port 2222 remains valid |
| Short-lived SQL calls repeatedly launched the distribution's Perl client wrapper and read the full catalog twice | Invoke the native PostgreSQL 18 client in the runtime; commit an authorized hold and read one consistent snapshot through one connection | Real PostgreSQL authorization, exclusive-maintenance rejection, late-read failure cleanup and native transport integration |
| A small SSH CPU limit serialized otherwise independent work | Increase the chart's default SSH limit to one CPU and document independent-container quota effects | Controlled fixed-total-budget resource rebalancing and cgroup throttling observations; application metrics expose phase costs separately |

The authenticated native-client test also found a fixture error: a shared credential store could
return another actor's valid credential after Git's challenge/retry. Actor-specific stores now
verify credential selection and require every remote ref to remain unchanged after a denied push.
The server did not accept a verified reader as an owner in this reproduction.

## Qualification scope

- Safe Rust, standard library only; complete hook command lists, native Git graph validation and
  confirmed transactional publication remain required.
- Rust 1.99.1 formatting, strict Clippy, core/authentication/transport tests, and opt-in real
  PostgreSQL tests. The compiler-only image intentionally lacks runtime programs; the psql-dependent
  authentication test is explicitly selected in runtime-capable CI.
- Native Git/SSH and authenticated HTTP integration, backup/readback/truncation, concurrent writes,
  dependency faults, unknown PUT/COMMIT outcomes and cold process reconstruction.
- Helm 3/4 contracts and official pinned CNPG/Barman/OBC/Gateway API schemas. Invalid configuration
  tests include missing HTTP credentials, missing gateway parents and removed platform options.
- Real HTTPS routing on Istio 1.31.3 with Gateway API 1.7.0: current route/gateway conditions,
  certificate validation, authenticated native Git, restart readback, STRICT mTLS, gateway identity
  authorization, rejection of plaintext/non-mesh access, and effective retry-policy inspection.

`tests/istio.py` provisions its own disposable cluster and synthetic identities. It does not
install or modify platform resources in an application's production environment.

## Official-practice reassessment

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
