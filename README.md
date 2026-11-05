# git-rs

Experimental stateless Git backend in safe Rust, backed by PostgreSQL and S3-compatible storage.

Ordinary Git clients connect through OpenSSH. Application replicas have no persistent repository
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

- Standard-library-only Rust; `unsafe_code` is forbidden. Upstream Git handles wire protocols,
  pack/delta normalization and graph validation.
- A push uploads immutable objects, then publishes the complete ref update and pack catalog in
  one SQL transaction with authorization and old-OID compare-and-swap. Every push is all-or-nothing,
  including clients that do not request `--atomic`.
- Readers/writers take durable database leases. Explicit maintenance requires an exclusive lease;
  connection loss leaves the lease until expiry rather than prematurely releasing a reader.
- Concurrent requests share a bounded RAM LRU/singleflight cache. Request scratch is not reused as
  a disk cache. Cold replicas reconstruct everything from PostgreSQL and S3.
- Native `git bundle` backups and owner-only physical history truncation are available.
- Supported fetch behavior includes Git protocol v0/v1/v2, shallow clones, tags and notes. A
  loopback-only development Smart HTTP endpoint exists for tests; the Helm chart serves SSH.

## Releases and Helm

Versioned artifacts:

| Artifact | Location |
| --- | --- |
| Runtime image, Linux amd64 | `ghcr.io/borislitv/git-rs:0.1.0-rc.2` |
| OCI application chart | `oci://ghcr.io/borislitv/charts/git-rs` |
| Chart archive, checksums, provenance | [GitHub Releases](https://github.com/borislitv/git-rs/releases) |

```sh
helm pull oci://ghcr.io/borislitv/charts/git-rs --version 0.1.0-rc.2
helm show values oci://ghcr.io/borislitv/charts/git-rs --version 0.1.0-rc.2
```

The chart is **independent, with `dependencies: []`**. It directly renders optional CNPG Cluster,
Barman ObjectStore/ScheduledBackup, Rook CephCluster/CephObjectStore, bucket StorageClass and
ObjectBucketClaim resources. Install the applicable operators and CRDs separately.

- [Chart configuration and installation](charts/git-rs/README.md).
- [CNPG + existing Rook CephCluster example](charts/git-rs/values-example.yaml).
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

## Build and verify

Requires Linux, Rust 1.93+, Git **2.53.0**, OpenSSH server/client, PostgreSQL client, curl with
AWS SigV4 support, GNU coreutils and `kill`. The Dockerfile pins the base image and Git package
and checks the Git version; other distribution packages receive their current repository updates.

```sh
cargo build --release --offline --locked
make check
docker build --target release -t git-rs:dev .
```

Black-box tests use only generated identities, synthetic repositories and disposable local
PostgreSQL/Versity containers. They require Docker, Python 3.11+ and the runtime programs above:

```sh
make integration           # 64 MiB synthetic data, native Git/SSH and cold restart
make failures              # real S3/PostgreSQL transport and maintenance faults
make bench                 # release-mode RAM-cache microbenchmark
python3 -m pip install PyYAML==6.0.3 jsonschema==4.19.2
make chart                 # Helm 3.22.1; values/Secret/security contracts
python3 tests/chart.py --operator-schemas  # opt-in network check of public pinned CRD schemas
```

`tests/kubernetes.py --cluster-container NAME --image git-rs:dev` uses an explicitly selected
disposable Docker k3s cluster. It imports images and creates/deletes its own isolated namespace,
generates TLS/SSH keys, installs and upgrades the chart, pushes/clones through native SSH,
rejects an unknown identity and checks cold restart. The test does not install CNPG/Rook or claim
to verify their failover behavior. CI also runs native tests inside the Docker `test` target so
they use the same pinned Git version as the runtime.

## Current limits

- Each request materializes the complete live pack catalog; this is a prototype, not a lazy
  object filesystem or a substitute for repository sharding.
- Git SHA-1 repositories only; four admitted requests per serving process; operation deadline
  up to 120 seconds; snapshot admission up to 12 GiB; incoming pack/body up to 4 GiB.
- Default cache 64 MiB, maximum cacheable entry 8 MiB; process/subprocess RSS and scratch are
  additional resources. Kubernetes resource/scratch limits must match the intended workload.
- Default branch `main` is protected from deletion/rewind, including owner pushes. Read access
  is repository-wide; ref write prefixes do not hide objects or branches from readers.
- Only the current schema is initialized/verified. Future schema upgrades need explicit migrations.
- No production HTTP authentication, Git LFS service, filtered clone promises, operator-managed
  coordinated PITR, version-aware object purge or automatic serving-path garbage collection.
- Local integration tests do not prove CNPG synchronous failover, Ceph durability or recovery
  after losing an entire storage failure domain.
