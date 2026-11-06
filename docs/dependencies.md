# Qualified build dependencies

The runtime uses Ubuntu 26.04, pinned to the multi-platform index
`sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c`.
Builds explicitly upgrade APT packages before installation. Requalification builds
must bypass cached APT layers (`docker build --no-cache`); the pinned base alone
does not freeze security updates. `/usr/share/git-rs/packages.tsv` records the
actual runtime Debian package versions. These builds are not bit-reproducible.

Git 2.56.0 is built from the official kernel.org source archive, not Ubuntu's older
Git package. Its SHA-256 is
`26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3`, checked
against `https://www.kernel.org/pub/software/scm/git/sha256sums.asc` and the
downloaded archive. The build preserves `/usr/bin/git` and
`/usr/lib/git-core/git-http-backend`. `/usr/share/git-rs/git-source.txt` records
source provenance; release automation exports both records and observes the Git
version inside the built image. Git updates require requalifying the native
proc-receive/pre-receive, HTTP and SSH contracts, not merely changing a version.

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
The ten opt-in metadata/driver tests run serially against a fresh disposable database;
`GIT_RS_DEV_HTTP=1` permits their loopback-only plaintext PostgreSQL connection.
CNPG 18.6 and k3s 1.37.2 pins are unchanged.

SQL uses Tokio 1.54.1, tokio-postgres 0.7.18 and deadpool-postgres 0.14.2, with
postgres-native-tls 0.5.3/native-tls 0.2.18 and OpenSSL 0.10.82. Cargo.lock pins
the complete dependency graph and registry checksums. Application code continues
to forbid unsafe Rust; maintained dependencies implement the runtime and TLS.
Builds need OpenSSL development headers and pkg-config. Fetch locked crates once
before an offline build; production images contain no PostgreSQL CLI or libpq.
The test-only image installs psql for independent fixture administration.

Local Docker builds on the qualification VM require `--network host` because
outbound source downloads on the default Docker bridge time out. This is a local
build-network limitation, not a runtime requirement.
