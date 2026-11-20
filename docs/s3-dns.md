# S3 DNS configuration and regression tests

## Resolver choice and compatibility

The S3 client installs a small `reqwest::dns::Resolve` adapter through reqwest
0.13.5's `ClientBuilder::dns_resolver`. It uses **hickory-resolver =0.26.0** with
only `system-config` and `tokio` features. `TokioResolver::builder_tokio()` reads
the system configuration during transport initialization; its error propagates
through the public `Store::from_env` constructor. There is no default/public
nameserver fallback. It no longer uses libc `getaddrinfo` on Tokio's blocking
pool. A running `spawn_blocking` lookup cannot be aborted;
four stalled lookups could exhaust that pool while timed-out/cancelled callers
refund their HTTP permits and enqueue further lookups. A thread-count limit is
not a queue bound. No new application resolver worker, protocol implementation,
subprocess or blocking admission gate is introduced.

The five-second connect timeout, operation deadline/cancellation polling, HTTP
admission, upload channel and response/range bounds are unchanged. IP literals
bypass lookup in Hyper's connector, but valid system configuration is required
for initial Store construction, even for literal endpoints. Hickory loads
`/etc/hosts` and the supported `/etc/resolv.conf` nameserver, search/domain and
resolver options; our adapter selects parallel IPv4/IPv6 lookups. This is not
libc NSS compatibility: arbitrary `nsswitch.conf` modules/order/actions,
LDAP/NIS, NSS-provided mDNS and hostnames
available only through those mechanisms are not preserved. Deployments must
provide ordinary DNS or hosts entries. System configuration/hosts are loaded
when the resolver is initialized, not promised to reload on every request.
As with other transport initialization failures, the process-global transport
caches the error; repair configuration and restart the process before retrying.

The adapter reads the system hosts file once using Hickory's maintained `Hosts`
parser and shares that immutable snapshot with the asynchronous resolver. It
checks the **original hostname** for both AAAA and A records before any DNS or
search-domain lookup. A hosts hit returns all available addresses immediately,
IPv6 before IPv4 and retaining the parser's order within each family. A missing
family does not trigger DNS. This also gives short hosts aliases precedence
over search-expanded DNS names. On a complete hosts miss, normal asynchronous
DNS/search handling applies. An unavailable hosts file is treated as empty,
matching Hickory's optional-hosts-file policy; system DNS configuration errors
still prevent Store initialization.

Reqwest's supplied Hickory adapter is deliberately **not** used: it falls back
to Google DNS if reading system resolver configuration fails. The S3 builder
calls `no_hickory_dns()` before supplying our explicit adapter, and does not
enable reqwest's `hickory-dns` feature. This also prevents an unused built-in
Hickory adapter from being selected if another dependency unifies that feature.

## Implementation references and ownership limits

- [Hickory 0.26.0 Hosts](https://docs.rs/crate/hickory-resolver/0.26.0/source/src/hosts.rs):
  system-file parsing, aliases and per-family `lookup_static_host` results.
- [Hickory 0.26.0 Resolver](https://docs.rs/hickory-resolver/0.26.0/hickory_resolver/struct.Resolver.html):
  system configuration, `set_hosts`, search domains and asynchronous `lookup_ip`.
- [Hickory 0.26.3 record data](https://docs.rs/hickory-proto/0.26.3/hickory_proto/rr/enum.RData.html#method.ip_addr):
  address extraction from maintained parsed records.
- [Reqwest DNS customization](https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html#method.dns_resolver):
  explicit resolver override and port-zero address handling. The default resolver
  is replaced before use; its unused constructor starts no lookup.

Avoiding the unabortable libc blocking-queue path does **not** establish
a join of every Hickory background driver, nor of Hyper's HTTP/TLS/body driver.
Dropping a request is still not SDK termination acknowledgment. Copied transport
bytes may remain until SDK teardown runs; no source/provider or borrowed output
is moved into those drivers. No durable SQL intent/publication claim is made.

## Real-DNS regression tests

Timeout/cancellation test:
`blob::transport::dns_tests::s3_dns_blackhole_cancel_recovery_without_blocking_backlog`.
It is ignored in ordinary workspace runs: system DNS must never be changed on
the operator's host. It calls the actual `Store::source_range` operation and
production singleton client, not a fake `Resolve`. The Python peer uses maintained
`dnslib==0.9.26` for DNS decoding/encoding, not a hand-written DNS protocol.

The fixture provides a real silent DNS peer. Its libc timeout is deliberately
30 seconds, longer than the unchanged five-second HTTP connect timeout. It checks:

- Literal IPv4 and a search-only short hostname return the exact immutable range
  bytes. Hosts-only names have separate tests, so their failure
  does not mask the existing timeout/cancellation/backlog gate.
- A blackholed lookup ends at the connect timeout without writing output.
- Twelve distinct queries are cancelled only after the DNS peer observes their
  packets. Each caller returns cancellation without writing output.
- After every failure/cancellation, a real `spawn_blocking` probe on the S3
  runtime completes within one second. Baseline libc lookups exhaust its four
  workers after the timeout and three cancellations; Hickory must not.
- The same last cancelled name resolves and serves exact bytes after peer
  recovery. Returned HTTP permits and shared resolver state must remain usable.

The probe is a dependency-capacity witness, not a measurement of every SDK task
or a proof of zero residual DNS sockets. The test does not cover TCP fallback,
IPv6 delivery, NSS plugins, TLS teardown, multipart abort or SQL intent durability.

### Prerequisites and build

Use a Linux test image containing Python 3, `dnslib==0.9.26`, and the runtime
libraries required by the prebuilt git-rs library test binary. Install
fixture dependencies while preparing that image, never inside the isolated run.
The following variables must name absolute existing paths: `APP`, `DNS_INPUTS`,
`TEST_BINARY` (the exact git-rs library test executable, not cargo), and
`DNS_TEST_IMAGE` (prefer an immutable image digest). No PG/S3/Kubernetes service
or credentials are needed. Build the test binary with the checked-in lockfile:

```sh
timeout 900s cargo test --manifest-path "$APP/Cargo.toml" --locked --lib --no-run
```

Create these three files in a dedicated fixture-input directory:

`resolv.conf` (exact contents, including final newline):

```text
nameserver 127.0.0.1
search dns-fixture.test
options ndots:1 timeout:30 attempts:1
```

`hosts` (exact UTF-8 bytes, LF line endings and final newline; IPv4 only):

```text
127.0.0.1 localhost
127.0.0.1 hosts.dns-fixture.test hostsalias
```

`nsswitch.conf`:

```text
passwd: files
group: files
hosts: files dns
```

Run only with private networking and these read-only mounts. The image must use
glibc for the libc baseline witness; network isolation prevents external DNS.
The runner bounds the child at 60 seconds and joins its own fixture servers;
the outer command bounds container setup/teardown too. It does not rewrite any
system configuration. Do not run the ignored test directly on the host.

```sh
timeout 90s docker run --rm --network none --read-only --user 0:0 \
  --cap-drop ALL --cap-add NET_BIND_SERVICE \
  --mount "type=bind,src=$APP/tests/support/s3_dns_fixture.py,dst=/fixture.py,readonly" \
  --mount "type=bind,src=$TEST_BINARY,dst=/test-binary,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/resolv.conf,dst=/etc/resolv.conf,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/hosts,dst=/etc/hosts,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/nsswitch.conf,dst=/etc/nsswitch.conf,readonly" \
  --entrypoint python3 "$DNS_TEST_IMAGE" /fixture.py /test-binary
```

### Hosts-only compatibility tests

Test names:

- `blob::transport::dns_tests::s3_dns_ipv4_hosts_only_does_not_query_dns`
- `blob::transport::dns_tests::s3_dns_short_hosts_alias_does_not_query_dns`

Run each in a fresh child using `--mode hosts-fqdn` and `--mode hosts-short`,
respectively. Use the exact same resolv.conf, hosts and nsswitch.conf mounts
above. The hosts file bytes are exactly
`b"127.0.0.1 localhost\n127.0.0.1 hosts.dns-fixture.test hostsalias\n"`.
The IPv4-only line and short alias are required, not optional convenience data.
Both tests call the actual public range operation and require `DATA` bytes plus
**zero** DNS queries corresponding to either hosts name or any of their search
expansions. The peer blackholes every such query, including A and AAAA, even
after recovery is enabled; it never returns NXDOMAIN for hosts-only names.
Its synchronized counter is checked before/after the range read and by the
runner on successful child exit. The runner also prints `hosts-dns-queries: N`
on a failed test to identify the actual dependency path.

Run each mode independently with the prebuilt test binary:

```sh
timeout 90s docker run --rm --network none --read-only --user 0:0 \
  --cap-drop ALL --cap-add NET_BIND_SERVICE \
  --mount "type=bind,src=$APP/tests/support/s3_dns_fixture.py,dst=/fixture.py,readonly" \
  --mount "type=bind,src=$TEST_BINARY,dst=/test-binary,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/resolv.conf,dst=/etc/resolv.conf,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/hosts,dst=/etc/hosts,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/nsswitch.conf,dst=/etc/nsswitch.conf,readonly" \
  --entrypoint python3 "$DNS_TEST_IMAGE" /fixture.py /test-binary --mode hosts-fqdn

timeout 90s docker run --rm --network none --read-only --user 0:0 \
  --cap-drop ALL --cap-add NET_BIND_SERVICE \
  --mount "type=bind,src=$APP/tests/support/s3_dns_fixture.py,dst=/fixture.py,readonly" \
  --mount "type=bind,src=$TEST_BINARY,dst=/test-binary,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/resolv.conf,dst=/etc/resolv.conf,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/hosts,dst=/etc/hosts,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/nsswitch.conf,dst=/etc/nsswitch.conf,readonly" \
  --entrypoint python3 "$DNS_TEST_IMAGE" /fixture.py /test-binary --mode hosts-short
```

Expected result: each range read returns `DATA`, and `hosts-dns-queries: 0`.
To check failure sensitivity, temporarily bypass only the original-name hosts
fast path in `SystemDns::resolve`, retaining the same fixture and async resolver.
The range read should fail after the five-second connect timeout with
`S3 HTTP request failed`, and the peer should record a corresponding DNS query.
A successful read with a nonzero counter must also fail the zero-query check.
Restore the fast path, rebuild and repeat both modes. Fixture setup errors,
missing tests and build failures are not behavior-regression evidence.

### Blocking-resolver comparison

In a separate mutation workspace, set `APP` to that workspace and
apply only this mutation, keeping the direct dependency, `no_hickory_dns()` and
fixture unchanged:

```diff
-                .dns_resolver(std::sync::Arc::new(system_dns::SystemDns::new()?))
```

Remove that complete builder call; do not just toggle `hickory_dns(false)`.
An explicit `dns_resolver` overrides that toggle. Removing the call also removes
eager adapter construction, so this mutant genuinely uses the default GAI path.

Rebuild the comparison binary:

```sh
timeout 900s cargo test --manifest-path "$APP/Cargo.toml" --locked --lib --no-run
```

Select that mutation binary as `TEST_BINARY` and execute the exact container
command above. Expected RED: `cancelled DNS retained all four
S3 blocking workers`, not a setup/build/DNS-observation failure. The mutation
uses reqwest's real GAI resolver, libc and real DNS packets; it reproduces the
blocking dependency path rather than a fake future's behavior. Restore the
explicit `dns_resolver` call, rebuild and rerun the same command for GREEN.
Record the locked resolver/net versions, binary hashes and comparison output
alongside the test results.

## Fail-closed configuration regressions

Test name:
`blob::transport::dns_tests::s3_dns_system_config_failure_is_fail_closed`.
Each case must run in its own fresh test child, because transport initialization
is process-global. The runner provides otherwise valid synthetic HTTPS settings
and invokes only this test. It exercises **public `Store::from_env`**, requiring
an actionable system-DNS initialization error within three seconds. It does not
call the adapter's private constructor and needs no running DNS/HTTP peer.

Use the same prebuilt binary/image. Prepare two additional bind inputs:

- `$DNS_INPUTS/invalid-resolv.conf`: exactly `search dns-fixture.test` followed
  by a newline, with no nameserver. This is rejected by the system-config
  loader even if its parser tolerates unknown directives.
- `$DNS_INPUTS/unreadable-resolv.conf`: ordinary valid configuration, owned by
  root with mode `000`. Its parent directories, the binary, fixture script and
  image's Python installation must be traversable/readable by UID/GID 65534.
  The runner checks that reading `/etc/resolv.conf` actually raises
  `PermissionError`; a wrong UID/mode is a setup failure, not a passing test.

Both run without network access or capabilities and as an unprivileged user:

```sh
timeout 90s docker run --rm --network none --read-only --user 65534:65534 \
  --cap-drop ALL \
  --mount "type=bind,src=$APP/tests/support/s3_dns_fixture.py,dst=/fixture.py,readonly" \
  --mount "type=bind,src=$TEST_BINARY,dst=/test-binary,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/invalid-resolv.conf,dst=/etc/resolv.conf,readonly" \
  --entrypoint python3 "$DNS_TEST_IMAGE" /fixture.py /test-binary --mode invalid-config

timeout 90s docker run --rm --network none --read-only --user 65534:65534 \
  --cap-drop ALL \
  --mount "type=bind,src=$APP/tests/support/s3_dns_fixture.py,dst=/fixture.py,readonly" \
  --mount "type=bind,src=$TEST_BINARY,dst=/test-binary,readonly" \
  --mount "type=bind,src=$DNS_INPUTS/unreadable-resolv.conf,dst=/etc/resolv.conf,readonly" \
  --entrypoint python3 "$DNS_TEST_IMAGE" /fixture.py /test-binary --mode unreadable-config
```

Failure sensitivity for these cases: use the same exact override-removal mutant
described above and rebuild. Run both commands again. Expected RED is
`invalid/unreadable system DNS must prevent Store initialization`: reqwest's
default GAI client constructs successfully without reading DNS config, whereas
the explicit adapter must reject it during Store construction. A loader failure
or an inaccessible prebuilt binary is not the expected RED. Restore the override,
rebuild and repeat both cases. Record assertion failures separately from fixture
setup or runtime-loader errors.
