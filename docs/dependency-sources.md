# Dependency sources and provenance

The Cargo manifests and `Cargo.lock` are the executable dependency definition.
Normal Git dependencies use an owned public fork and a full 40-hex revision;
registry dependencies retain lockfile checksums. There are no production path,
branch, tag, `[patch]` or `[replace]` substitutions for these sources. The only
application path dependency is the shared `crates/git-core` workspace member.

| Component | Maintained upstream / owned source | Purpose and provenance |
| --- | --- | --- |
| Gitoxide | [GitoxideLabs/gitoxide](https://github.com/GitoxideLabs/gitoxide), [borislitv/gitoxide](https://github.com/borislitv/gitoxide) | Controlled pack/delta/index, packet-line, ref/config/URL validation, admitted traversal, diff and blame; `gitoxide-source.json` binds the revision, controlled sources, package/license closure and upstream base |
| imara-diff | [pascalkuthe/imara-diff](https://github.com/pascalkuthe/imara-diff) | Maintained diff source within the Gitoxide fork; its `gix-imara-diff/UPSTREAM-PROVENANCE.tsv` and Apache-2.0 notice preserve origin |
| Kubert | [olix0r/kubert](https://github.com/olix0r/kubert), [borislitv/kubert](https://github.com/borislitv/kubert) | Named Kubernetes Lease resourceVersion CAS/SSA, bounded operations and monotonic local claim expiry; Apache-2.0, normal root `lease`/`openssl-tls` dependency; `kubert-source.json` and fork `FORK_PROVENANCE.md` |
| iconv-native | [borislitv/iconv-native-rs](https://github.com/borislitv/iconv-native-rs) | Safe wrapper for the matched GNU libc iconv runtime; only `libc-iconv`, no command-line fallback; `codec-source.json` |
| zstd | [gyscos/zstd-rs](https://github.com/gyscos/zstd-rs), [borislitv/zstd-rs](https://github.com/borislitv/zstd-rs) | Minimal admitted streaming codec; `zstd-source.json` binds Rust packages, selected features/licenses and exact Facebook zstd native gitlink |
| flate2 / zlib-rs | [rust-lang/flate2-rs](https://github.com/rust-lang/flate2-rs), [trifectatechfoundation/zlib-rs](https://github.com/trifectatechfoundation/zlib-rs) | Controlled HTTP gzip decode and maintained compression primitives; registry checksums/features/licenses in `gzip-source.json` |
| rustix | [bytecodealliance/rustix](https://github.com/bytecodealliance/rustix) | Safe owned descriptor, pipe, socket and poll I/O; registry checksum and minimal I/O profile in `rustix-source.json` |
| quick-xml / serde | [tafia/quick-xml](https://github.com/tafia/quick-xml), [serde-rs/serde](https://github.com/serde-rs/serde) | Bounded S3 protocol response parsing; exact resolved versions/checksums in the lockfile |
| kube-client / k8s-openapi | [kube-rs/kube](https://github.com/kube-rs/kube), [Arnavion/k8s-openapi](https://github.com/Arnavion/k8s-openapi) | Typed in-cluster TLS client and API objects; the compiled API feature does not replace qualification against the selected real Kubernetes server |

Fork changes and their upstream tests remain in the dependency repositories.
Do not copy a fork subtree into this application or add an upstream pull request
as an implicit part of application delivery. A pin update changes executable
behavior and requires its own source, API, native-oracle and resource review.
The `gix-imara-diff` lock edge retains `hashbrown 0.16.1`; resolving another Gitoxide
revision must not silently change that separately qualified diff dependency.

`tools/check-core-source.py` checks source records, normal manifest edges, locked
identities and selected feature profiles. Optional checkout arguments additionally
check the owned remote, exact clean revision, controlled source hashes and notices.
Resolved Cargo metadata is needed for feature-unification checks: direct manifest
features alone do not describe the complete graph. `make vendor-check` exercises
the selected maintained dependency packages separately from application tests.

`tools/dependency-notices.py` consumes offline locked Cargo metadata and exports
bounded source/license records and notice files into the production image under
`/usr/share/git-rs/dependencies/`. Machine-readable source records are also shipped
under `/usr/share/git-rs/sources/`. A metadata graph may include build/test edges;
it is not a dynamic-linkage manifest, license opinion or successful qualification
verdict. Preserve the original notices, including the native zstd selected license.

The Dockerfile pins the Ubuntu base and official Rust component archives. Compiler
and runtime share one OS package transaction, then runtime pruning verifies its
libc/libc-bin/OpenSSL ABI tuple. GNU libc loader/gconv compatibility must be checked
as a complete closure. A pinned base does not freeze later APT security updates;
actual package inventories bind each built image. See [build dependencies](dependencies.md).

The native Git 2.56.0 archive and hash belong only to the external client/oracle
image. They are neither a server dependency nor an acceptable fallback. The
production filesystem inventory, actual runtime lifecycle and independent native
client results are separate evidence; no one of them establishes the others.

Production cache compression stays single-threaded API level `3` (`zstd -3`), with
its declared window/size/checksum/dictionary constraints. Keep
`ZSTD_SYS_USE_PKG_CONFIG` absent, including an empty or `0` value; it must not select
an unrecorded system codec. Source records and qualification remain revision-bound.
