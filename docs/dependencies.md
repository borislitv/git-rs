# Build and runtime dependency contracts

The runtime uses Ubuntu 26.04, pinned to the multi-platform index
`sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c`.
Builds explicitly upgrade APT packages before installation. Requalification builds
must bypass cached APT layers (`docker build --no-cache`); the pinned base alone
does not freeze security updates. `/usr/share/git-rs/packages.tsv` records the
actual runtime Debian package versions. These builds are not bit-reproducible.

External native clients use Git 2.56.0 built from the official kernel.org source
archive, not Ubuntu's older Git package. Its SHA-256 is
`26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3`, checked
against `https://www.kernel.org/pub/software/scm/git/sha256sums.asc` and the
downloaded archive. The separate `client-oracle`/`test` branch preserves `/usr/bin/git`
and `/usr/lib/git-core`; `/usr/share/git-rs/git-source.txt` records its provenance.
Neither its layers nor that installation enter `runtime`/`release`/`test-server`.
Git updates require fresh receive/upload, HTTP/SSH, pack/bundle, history and blame
oracle qualification, not merely changing a version string.

Controlled Rust ingestion validates the entire incoming physical pack, including
duplicate and unreachable entries, before publication. Typed closure checks graph
connectivity and `.gitmodules` safety; the immutable command batch is sealed before
SQL/status output. Existing delta bases come from the leased catalogued source,
not client-selected files. This relies on exact physical identity, bounded range
verification and preserving strict ingestion; connectivity alone is not content validation.
See the upstream [receive configuration](https://git-scm.com/docs/git-config#Documentation/git-config.txt-receivefsckObjects),
[index-pack](https://git-scm.com/docs/git-index-pack) and
[fsck](https://git-scm.com/docs/git-fsck) contracts.

Rust 1.99.1 uses official `static.rust-lang.org/dist` component archives, with
the independently published `.sha256` values pinned in the Dockerfile. Components
are installed without rustup or shell download pipelines.
The archive target and release platform are currently Linux x86-64 only.

CI selects Ubuntu 26.04, Rust 1.99.1, Helm 4.3.1, PyYAML 6.0.3 and jsonschema
4.26.0. Chart/schema checks also retain Helm 3.22.1 compatibility. The test image
installs its Python packages in `/opt/test-python`; use `python3` through its PATH
or `/opt/test-python/bin/python`, not `/usr/bin/python3`, for schema checks.
Checkout v7.0.1 and setup-helm v5.0.1 are pinned to resolved upstream tag commits.

The disposable PostgreSQL fixture uses the official `postgres:18.6-bookworm`
index `sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5`.
Opt-in metadata/driver and maintenance contracts run serially against a fresh disposable database;
`GIT_RS_DEV_HTTP=1` permits their loopback-only plaintext PostgreSQL connection.
CNPG 18.6 and k3s 1.37.2 pins are unchanged.

SQL uses Tokio 1.54.1, tokio-postgres 0.7.18 and deadpool-postgres 0.14.2, with
postgres-native-tls 0.5.3/native-tls 0.2.18 and OpenSSL 0.10.82. Cargo.lock pins
the complete dependency graph and registry checksums. Application code continues
to forbid unsafe Rust; maintained dependencies implement the runtime and TLS.
Builds need OpenSSL development headers and pkg-config. Fetch locked crates once
before an offline build; production images contain no PostgreSQL CLI or libpq.
The test-only image installs psql for independent fixture administration.

Gitoxide supplies controlled pack/index/delta, ref-name, object, traversal, diff
and blame primitives through a normal full-revision owned-fork dependency.
Default features are disabled and SHA-1 enabled; the lockfile retains SHA1_DC
support. No request repository, Git config/include discovery or symbolic-HEAD
filesystem workaround is needed. Kubert supplies named-Lease CAS/SSA candidacy;
only the separately confirmed PostgreSQL fence authorizes maintenance. See
[dependency sources](dependency-sources.md) and the [Rust data plane](rust-data-plane.md).

S3 uses reqwest 0.13.5 with native TLS and the official AWS request signer,
aws-sigv4 1.6.4 with aws-credential-types 1.3.1. No S3 subprocess is started.
The process-local Tokio runtime has two workers and at most four blocking workers;
eight active transfers share a client with at most four idle HTTP/1 connections per host,
retained for thirty seconds. Ordinary serving admission is still four Git requests.
Requests use borrowed bounded buffers; downloads retain charged RAM ranges and
validate exact length/physical identity. Large pack output uses admitted 8 MiB
multipart chunks, durable intents, bounded completion parsing and full readback;
neither direction requires a complete pack buffer or filesystem materialization.
The five-second connection timeout and 110-second transfer cap (thirty seconds for bucket
policy) are bounded by the remaining absolute operation deadline, including admission wait.
Cancellation is checked at most every twenty milliseconds while awaiting network I/O and
between response chunks. Local filesystem I/O and TLS initialization remain synchronous work;
these timeouts are not a hard CPU scheduling guarantee.

The HTTP client disables proxies, redirects, automatic retries and transparent decompression.
Native TLS verifies certificate chains and hostnames using the system trust store and the
OpenSSL `SSL_CERT_FILE`/`SSL_CERT_DIR` overrides. Endpoint credentials, query strings and
fragments are rejected. Signing headers are sensitive values, and transport errors suppress
URLs and credentials. The fixture's curl client remains an independent S3 administration tool.

The RAM cache retains admitted immutable backing buffers, live reader pins and
bounded singleflight metadata. Eviction does not refund still-live backing or let
an invalidated in-flight fill reinstall a poisoned lookup. `memfd` is test-only;
no production `/proc/<serving-pid>/fd` bridge or native child is required. SHA-256
runs in-process. Capacity accounting does not imply zero copies or bounded RSS.

Opt-in diagnostic output uses Tokio's `tracing-appender` 0.2.5, with an explicit 1024-record lossy
queue and one complete fixed-field record per write. Its guard can emit a stdout fallback when
shutdown cannot enqueue; a process-owned wrapper runs guard destruction off-thread and waits
at most 1200 ms for acknowledgement. Sink-stalled workers may remain until process exit.

Production has no GNU timeout/process-worker fallback. Operation deadlines and
cancellation are checked within maintained controlled algorithms and owned I/O.
External or historical fixtures may require GNU coreutils for byte-preserving
client arguments; that is not a production runtime dependency.

Compilation and runtime originate from one OS package transaction. Runtime pruning
must preserve the recorded libc/libc-bin/OpenSSL ABI tuple, including its matching
loader/gconv closure; never copy individual libc files or install a second distro
Git over the pinned oracle. Dependency notices/source records are exported from
the locked graph. Their presence is provenance, not dynamic-linkage or qualification proof.

Local Docker builds on the qualification VM require `--network host` because
outbound source downloads on the default Docker bridge time out. This is a local
build-network limitation, not a runtime requirement.
