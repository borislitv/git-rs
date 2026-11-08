# git-rs

Experimental stateless Git backend in safe Rust, backed by PostgreSQL and S3-compatible storage.

Ordinary Git clients connect through OpenSSH or authenticated Smart HTTP behind a TLS gateway.
Application replicas have no persistent repository
volume: PostgreSQL stores authoritative refs/permissions and S3 stores immutable native Git
pack/idx objects. Each request uses a private temporary bare repository; the only reusable
application cache is bounded process memory.

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
  SQL runs in-process, with verified TLS and no SQL subprocess. Request initialization,
  ref restoration and ref-name validation use isolated `gix` library operations.
  Wire protocols, pack/delta normalization and graph validation still use upstream Git.
  The required [Rust server data plane](docs/rust-data-plane.md) records replacement gates;
  native Git remains the compatibility client and test oracle in the target design.
- S3 uses a shared in-process HTTP client and the official AWS SigV4 signer. Pack transfers stream
  through bounded buffers; connections are reused, and cancellation drops partial responses.
  HTTPS verifies the certificate chain and hostname; requests do not follow redirects or retry.
- A push uploads immutable objects, then publishes the complete ref update and pack catalog in
  one SQL transaction with authorization and old-OID compare-and-swap. Every push is all-or-nothing,
  including clients that do not request `--atomic`.
- Readers/writers take durable database leases. Explicit maintenance requires an exclusive lease;
  connection loss leaves the lease until expiry rather than prematurely releasing a reader.
- Concurrent requests share a bounded RAM LRU/singleflight cache. Request scratch is not reused as
  a disk cache. Verified cache files are sealed Linux memfds, linked into private scratch and pinned
  until the request finishes; cache hits do not copy pack/idx payloads. An empty library-generated
  bare template is reused in RAM, before any request refs or hooks. Cold replicas reconstruct from SQL/S3.
- Native `git bundle` backups and owner-only physical history truncation are available.
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
authentication nor transport encryption. See [credential setup](OPERATIONS.md#authenticated-http).

The same authenticated listener provides [complete immutable blame](docs/blame.md) for a
published commit and literal file path. Native Git is the default worker; the response is
complete line-porcelain output with bounded admission, payload size and operation lifetime.

Prometheus request, phase, detailed stage, dependency, subprocess and cache metrics are available
at `/metrics` on the internal health listener. Opt-in correlated timing records use a bounded,
lossy background writer. See [metrics, timing logs and scrape configuration](docs/metrics.md).

## Build and verify

Requires Linux, Rust **1.99+**, OpenSSL development headers/pkg-config for builds, Git **2.56.0**,
OpenSSH server/client, GNU coreutils **`/usr/bin/gnutimeout`** and `kill`.
Ubuntu's coinstallable `gnu-coreutils` package supplies that explicit entrypoint: its default
uutils `timeout` rejects legal non-UTF-8 ref arguments before Git sees them.
Build and runtime use Ubuntu **26.04**. The Dockerfile
checks official Rust/Git archive hashes, upgrades distribution packages and records the installed
runtime inventory. [Dependency qualification](docs/dependencies.md) distinguishes reproducibility
from security freshness; scheduled rebuilds are still necessary.

```sh
cargo fetch --locked
cargo build --release --offline --locked
make check
docker build --target release -t git-rs:dev .
```

Black-box tests use only generated identities, synthetic repositories and disposable local
PostgreSQL/Versity containers. They require Docker, Python 3.11+, curl with AWS SigV4 support for
independent fixture administration, and the runtime programs above:

```sh
make integration           # 64 MiB synthetic data, native Git/SSH and cold restart
python3 tests/integration.py --provision --large-mib 64 --debug-timings
python3 tests/integration.py --provision --binary target/release/git-rs --long-operations
make failures              # real S3/PostgreSQL transport and maintenance faults
make bench                 # release-mode RAM-cache microbenchmark
python3 -m pip install PyYAML==6.0.3 jsonschema==4.26.0
make chart                 # Helm 4.3.1 and 3.22.1 compatibility in CI
python3 tests/chart.py --operator-schemas  # opt-in network check of public pinned CRD schemas
```

The opt-in long-operation regression holds back a native-generated pack's final byte for
125 seconds, then requires confirmed two-ref publication, native clone/fsck and lease release
within a configured 180-second budget. CI includes this wall-clock deadline check.
Native integration and dependency-fault fixtures exercise the maximum four-worker Git setting.

`tests/kubernetes.py --cluster-container NAME --image git-rs:dev` uses an explicitly selected
disposable Docker k3s cluster. It imports images and creates/deletes its own isolated namespace,
generates TLS/SSH/HTTP credentials, installs and upgrades the chart, pushes/clones through native
SSH and authenticated HTTP, rejects unknown identities and checks cold restart. It does not install CNPG/Rook or claim
to verify their failover behavior. CI also runs native tests inside the Docker `test` target so
they use the same pinned Git version as the runtime.

## Current limits

- Each request materializes the complete live pack catalog; this is a prototype, not a lazy
  object filesystem or a substitute for repository sharding.
- Git SHA-1 repositories only; four admitted requests per serving process; operation deadline
  defaults to 120 seconds and is configurable from 1 to 3600 seconds; snapshot admission up to
  12 GiB; incoming pack/body up to 4 GiB. Durable holds cover the remaining operation plus a
  30-second cleanup margin; longer operations also extend crash-recovery maintenance delays.
- `GIT_RS_PACK_THREADS` sets native Git's per-request `pack.threads` to `1..4`, default 1.
  Choose it within the backend CPU/memory budget; four admitted requests can each use that many
  workers, and Git's per-thread delta-base cache and object buffers need separate allowances.
- Blame executes one query per process, with a 128 MiB uncompressed-blob limit and a 256 MiB
  complete-output limit. Annotation results are not cached; computation uses the same absolute
  operation budget and private snapshot materialization as other reads.
- Default cache 64 MiB, maximum cacheable entry 8 MiB, 4 KiB accounting increments and at most
  256 live cache files, including fills and evicted reader pins. Linux `/proc/<pid>/fd` must be
  accessible to same-UID native Git children. Memfd is not a no-swap or total-RSS guarantee;
  subprocesses, metadata and scratch need additional resource allowances.
- Default branch `main` is protected from deletion/rewind, including owner pushes. Read access
  is repository-wide; ref write prefixes do not hide objects or branches from readers.
- The current schema is initialized/verified and its publication function refreshed. Future schema
  upgrades need explicit migrations.
- No Git LFS service, filtered clone promises, operator-managed
  coordinated PITR, version-aware object purge or automatic serving-path garbage collection.
- Local integration tests do not prove CNPG synchronous failover, Ceph durability or recovery
  after losing an entire storage failure domain.
