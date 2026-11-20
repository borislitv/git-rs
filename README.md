# git-rs

Experimental stateless Git backend in safe Rust, backed by PostgreSQL and S3-compatible storage.

Ordinary Git clients connect through OpenSSH or authenticated Smart HTTP behind a TLS gateway.
Application replicas have no persistent repository
volume: PostgreSQL stores authoritative refs/permissions and S3 stores immutable native Git
pack/idx objects. Requests use bounded Rust-owned buffers and immutable object-store ranges. Optional
independently configured PostgreSQL/S3 derived artifacts can share complete blame
bodies and authenticated completed work; they never replace source authorization.

**Status: experimental prerelease, not a production HA certification.** Read
[operations, recovery and limits](OPERATIONS.md) before storing important data.

## Architecture

```text
Git client / ssh-agent
        | SSH public-key authentication
        v
OpenSSH sidecar -- restricted principal + private Unix socket --> safe Rust backend
                                                                 |         |
                                                   refs/catalog  |         | pack/idx
                                                                 v         v
                                                        PostgreSQL       HTTPS S3
                                                        (CNPG)           (Ceph RGW)
```

- Safe Rust with Tokio, tokio-postgres and a bounded SQL pool; `unsafe_code` is forbidden.
  SQL runs in-process, with verified TLS and no SQL subprocess. The shared core uses
  maintained Gitoxide pack, delta, ref-name, graph, traversal and blame libraries.
  Blame path resolution, metadata, ancestry, attribution and complete porcelain run
  in the pinned Rust core over demand-acquired verified objects. Header probes do
  not inflate full blobs or spawn Git.
  Receive/upload protocols, strict ingestion, history and bundle output run in Rust.
  The production image contains no native Git executable, helpers or client repositories;
  native Git is an external compatibility client and test oracle. See the
  [data-plane contracts and qualification scope](docs/rust-data-plane.md).
- S3 uses a shared in-process HTTP client and the official AWS SigV4 signer. Pack transfers stream
  through bounded buffers; connections are reused, and cancellation drops partial responses.
  HTTPS verifies the certificate chain and hostname; requests do not follow redirects or retry.
- A push uploads immutable objects, then publishes the complete ref update and pack catalog in
  one SQL transaction with authorization and old-OID compare-and-swap. Every push is all-or-nothing,
  including clients that do not request `--atomic`.
- Readers/writers take durable database leases. Explicit maintenance requires an exclusive lease;
  connection loss leaves the lease until expiry rather than prematurely releasing a reader.
- Concurrent operations share one bounded CPU runtime and a RAM LRU/singleflight range cache.
  Evicted reader pins retain their actual capacity charge. No request bare repository,
  pack-file materialization or filesystem cache is required; cold replicas read SQL/S3.
- Rust-generated native-compatible bundles, ordered history and owner-only history truncation
  preserve leased source ownership. Source cleanup is a durable job run by a separately elected
  same-binary maintenance Deployment, never synchronous serving-path deletion.
- Supported fetch behavior includes Git protocol v0/v1/v2, shallow clones, tags and notes.
  Smart HTTP verifies Secret-backed credentials for each request and applies the same repository
  permissions as SSH. An optional HTTPRoute attaches to an existing Istio Gateway API listener.

## Releases and Helm

This branch prepares **0.1.0-rc.3**. Evaluate its HTTPRoute, authenticated HTTP and OBC-only
configuration with an image built from this checkout and the local `charts/git-rs` chart.
The latest published prerelease is **0.1.0-rc.2**:

| Artifact | Location |
| --- | --- |
| Published runtime image, Linux amd64 | `ghcr.io/borislitv/git-rs:0.1.0-rc.2` |
| OCI application chart | `oci://ghcr.io/borislitv/charts/git-rs` |
| Chart archive, checksums, provenance | [GitHub Releases](https://github.com/borislitv/git-rs/releases) |

```sh
helm pull oci://ghcr.io/borislitv/charts/git-rs --version 0.1.0-rc.2
helm show values oci://ghcr.io/borislitv/charts/git-rs --version 0.1.0-rc.2
```

The chart is **independent, with `dependencies: []`**. It directly renders optional CNPG Cluster,
Barman ObjectStore/ScheduledBackup and a namespaced ObjectBucketClaim. Ceph, RGW, its bucket
StorageClass, operators and CRDs are existing platform infrastructure, not installed by this chart.
Configure the HTTPS S3 endpoint explicitly and use a platform-managed Retain bucket class.

- [Chart configuration and installation](charts/git-rs/README.md).
- [CNPG + existing Ceph/RGW bucket provisioning example](charts/git-rs/values-example.yaml).
- [External TLS PostgreSQL/S3 example](charts/git-rs/values-external.example.yaml).

The tag release workflow runs verification, publishes both GHCR packages and creates a draft
GitHub Release. It publishes the release only after anonymous pulls succeed. First-time GHCR
packages may require their owner to select **public** visibility in package settings and rerun the
failed release job. A public repository alone does not make new packages public. See
[GitHub package visibility](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility)
and [current package billing](https://docs.github.com/en/billing/concepts/product-billing/github-packages).

## Native client usage

Use the shared OpenSSH account `git-rs`; server-managed public-key mappings select each logical
principal. Existing OpenSSH agents and hardware-backed keys work through normal SSH authentication.
Verify the server host key through a trusted channel before connecting.

```sh
git clone git-rs@git.example.invalid:team/project.git
git remote add backend git-rs@git.example.invalid:team/project.git
git push backend main
git fetch backend
```

The supported URL form is `USER@HOST:relative/slug.git`. Slugs are logical names, not server
filesystem paths: slash-separated ASCII letters, digits, `_` and `-`, at most 200 bytes. Absolute
paths, traversal, arbitrary SSH commands and shell sessions are rejected.

For authenticated HTTPS, configure the chart's `http` and `httpRoute` settings and use an ordinary
Git credential helper with a server-issued principal/token. Do not embed tokens in URLs or flags:

```sh
git clone https://git.example.invalid/team/project.git
```

The gateway terminates HTTPS, preserves `Authorization`, and connects over a trusted/encrypted
path to the backend. The application HTTP listener is plaintext; HTTPRoute alone supplies neither
authentication nor transport encryption. See [credential setup](OPERATIONS.md#authenticated-http-listener).

The same authenticated listener provides [complete immutable blame](docs/blame.md) for a
published commit and literal file path. The Rust core produces complete line-porcelain
output with bounded admission, payload size and operation lifetime. External worker
selection is obsolete and rejected. The default NativeRootV1 profile enables its five
performance-qualified request-local tuning switches; this is not a Stage 5 release
or completion of the pending joined authentication/runtime and real-image CI gates.
See [profile and qualification scope](docs/rust-data-plane.md).
Optional authenticated checkpoints and post-confirmation
one-path warming are described in [operations](OPERATIONS.md#authenticated-checkpoints-and-warming);
persisted reuse remains independently configured, not enabled by the tuning profile.

Prometheus request, phase, detailed stage, dependency, subprocess and cache metrics are available
at `/metrics` on the internal health listener. Opt-in correlated timing records use a bounded,
lossy background writer. See [metrics, timing logs and scrape configuration](docs/metrics.md).

## Prebuilt VM evaluation

Use the supplied Linux amd64 VM package to try push, clone, fetch, pull and complete
blame without a Rust toolchain. Verify `SHA256SUMS`, load `git-rs-runtime.tar` and
select the immutable image ID from `release.json`. The persistent launcher preserves
its PostgreSQL/S3 volumes and credentials across stop/start. See the
[VM instructions](examples/playground/README.md) for the complete two-client flow,
loopback-only access, restart verification and explicit prebuilt-image upgrades.
A package manifest binds inputs; release qualification evidence is separate.

## Developer build and verification

Requires Linux, Rust **1.99+**, Python **3.13+** and OpenSSL development headers/pkg-config for builds/checks.
External compatibility fixtures use Git **2.56.0** and OpenSSH server/client in the
separate `test` image. The production runtime does not install native Git or GNU timeout.
Bounded-memory Rust tests also require util-linux **`/usr/bin/prlimit`**.
Build and runtime use Ubuntu **26.04**. The Dockerfile
checks official Rust/Git archive hashes, upgrades distribution packages and records the installed
runtime inventory. [Dependency qualification](docs/dependencies.md) distinguishes reproducibility
from security freshness; scheduled rebuilds are still necessary.

```sh
cargo fetch --locked
cargo build --release --offline --locked
make check
docker build --build-arg "VCS_REF=$(git rev-parse HEAD)" --target release -t git-rs:dev .
```

Black-box tests use only generated identities, synthetic repositories and disposable local
PostgreSQL/Versity containers. CI transfers four commit-bound image roles: the
builder, external client/oracle, production runtime and explicit Git-free fixture
runtime. Qualification consumes these prebuilt inputs; it never builds inside a
measurement phase. See [runtime contracts](docs/runtime-contracts.md) for the
independent systemd bounds, serial SQL contracts, Kubernetes 1.37 and exact cleanup.

```sh
make bench                 # release-mode RAM-cache microbenchmark
python3 -m pip install PyYAML==6.0.3 jsonschema==4.26.0
make chart                 # Helm 4.3.1 and 3.22.1 compatibility in CI
python3 tests/chart.py --operator-schemas  # opt-in network check of public pinned CRD schemas
```

The integration phase holds back a native-generated pack's final byte for
125 seconds, then requires confirmed two-ref publication, native clone/fsck and lease release
within a configured 180-second budget. CI includes this wall-clock deadline check.
The `test-server` image adds only explicitly selected observer/receive fixtures;
ordinary serving still runs the unmodified production image. Kubernetes smoke uses
an independent oracle pod, verifies runtime image digests, performs a schema-first
maintenance enablement and checks standby readiness, leader replacement and queued
cleanup. Candidate-only tests separately exercise named-Lease CAS/SSA/expiry behavior.
These are verification definitions, not a successful-run or production HA claim;
inspect the exact source, binary/image identities, inventory, outcomes and cleanup evidence.

## Current limits

- All data-plane paths use demand-acquired indexes and bounded immutable ranges.
  Object selection, indexes, graph work, decoded objects and output share explicit
  admission; exceeding a bound is an error, not a native fallback or whole-pack allocation.
- Git SHA-1 repositories only; four admitted requests per serving process; operation deadline
  defaults to 120 seconds and is configurable from 1 to 3600 seconds;
  incoming pack/body up to 4 GiB. Durable holds cover the remaining operation plus a
  30-second cleanup margin; longer operations also extend crash-recovery maintenance delays.
- `GIT_RS_PACK_THREADS` and `GIT_RS_BLAME_WORKER` are obsolete and rejected.
  `GIT_RS_BLAME_CONCURRENCY`/`GIT_RS_BLAME_WAITERS` configure the shared CPU runtime:
  default one worker/no queue, at most four total slots. Increasing admission does not raise quotas.
- Blame defaults to one executing job and no queue, with a 128 MiB uncompressed-blob
  limit and a 256 MiB complete-output limit. Optional complete-body/checkpoint reuse
  still requires source holds and fresh fences. Jobs have finite independent lifetimes;
  waiters retain their individual deadlines. Warming is disabled by default.
- Default RAM range cache 64 MiB, maximum cacheable entry 8 MiB, 4 KiB accounting increments
  and at most 256 lookup slots. Fills, metadata and evicted-but-live pins retain their charges.
  This is backing-capacity accounting, not a process/pod RSS or allocator-overhead guarantee.
- Default branch `main` is protected from deletion/rewind, including owner pushes. Read access
  is repository-wide; ref write prefixes do not hide objects or branches from readers.
- Source schema v2 adds durable maintenance fencing, jobs, result journals and permanent retired
  identities. Upgrade it with maintenance disabled before enabling the separate worker Deployment;
  downgrade to a schema-v1 binary is unsupported. Derived-cache migrations remain independent.
- No Git LFS service, filtered clone promises, operator-managed
  coordinated PITR, version-aware object purge or automatic serving-path garbage collection.
- Local integration tests do not prove CNPG synchronous failover, Ceph durability or recovery
  after losing an entire storage failure domain.
