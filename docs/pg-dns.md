# PostgreSQL DNS cancellation contract

PostgreSQL pool creation resolves names asynchronously with the system-configured
Hickory resolver. Resolution and all candidate connections share the existing
five-second pool creation budget. Socket connection timeouts remain SDK-owned.
Cancellation is not an acknowledgment of remote rollback or a driver task join.

Configured hosts are tried as groups: configured-host order (or its randomized
permutation), then that host's addresses (or their randomized permutation).
Numeric `hostaddr` controls dialing; the original hostname controls TLS identity.
The SDK records the selected numeric cancellation endpoint. Explicit cache-DSN
hostaddrs and literal hosts do not initialize DNS. Existing Unix/hostaddr-only
rejections remain; this change does not add `PGHOSTADDR` environment support.

The wrapper retains each successful SDK Manager and its ClientWrapper, delegating
create/recycle/detach. This preserves per-client statement caches and SDK driver
ownership. It does not expose a new pool-wide statement-cache invalidation API.
The application does not use such an API.

## Verification prerequisites

- Use the checked-in Cargo.lock and the project's supported Rust toolchain. The
  manager uses the locked deadpool 0.13.1 and rand 0.10.3 packages; verification
  does not require upgrading dependencies or changing dependency revisions.
- A disposable, owned PostgreSQL instance with real TLS and a CA trusted through
  `PGSSLROOTCERT`. Its certificate must cover `PGHOST`, a synthetic FQDN, but not
  `wrong-identity.invalid`. Use a writable disposable database and role with schema
  migration rights. The DNS and manager contracts consume this fixture; they do not
  provision PostgreSQL or certificates themselves.
- Set `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`,
  `PGSSLMODE=verify-full`, and `PGSSLROOTCERT`. Set `GIT_RS_PG_DNS_ADDRESS` to the
  fixture's reachable IPv4 address. Supply separate cache credentials with
  `GIT_RS_CACHE_PGUSER`/`GIT_RS_CACHE_PGPASSWORD` (they deliberately do not inherit
  source credentials). Use the same writable database/role for this disposable
  contract so the metadata observer can inspect and remove its namespace.
- For the DNS contract, unset `GIT_RS_CACHE_DSN`, `GIT_RS_CACHE_PGHOST`,
  `GIT_RS_CACHE_S3_ENDPOINT`, and any cache connection-policy overrides. The cache
  must actually resolve the configured FQDN, not bypass DNS through a DSN address.
- Run the DNS harness only in an owned isolated network/mount namespace. Route
  the numeric PG fixture address into that namespace. Bind UDP 53 and TCP 18054
  there. Install `dnslib==0.9.26` in the fixture Python environment.
- Bind a read-only `/etc/resolv.conf` containing exactly:

```text
nameserver 127.0.0.1
options ndots:1 timeout:30 attempts:1
```

- Its `/etc/nsswitch.conf` must use `hosts: files dns`. `/etc/hosts` must not
  contain the fixture's PG hostname. Do not alter the operator's host files.

## GREEN commands

Run from the project root with the disposable fixture configured as above. Build
the test executable before entering the isolated DNS fixture environment. Keep
build output and runtime receipts associated with the exact source revision.
The TLS manager tests require working fixture hostname resolution, except where
they explicitly supply hostaddrs. Apply the isolated blackhole DNS configuration
only when running the controlled-DNS harness, not while running unrelated TLS
manager or SQL session contracts.

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo test --locked --lib database::manager::tests::manager_rejects_unsupported_routes_and_cardinality_before_connecting
cargo test --locked --lib database::manager::tests::manager_preserves_startup_settings_sdk_statement_cache_and_detach -- --ignored --exact --test-threads=1
cargo test --locked --lib database::manager::tests::manager_host_fallback_preserves_tls_identity_and_cancel_endpoint -- --ignored --exact --test-threads=1
cargo test --locked --test pg_dns --no-run --message-format=json > pg-dns-build.jsonl
PG_DNS_TEST_BINARY="$(python3 -c 'import json, sys; rows = [json.loads(line) for line in open(sys.argv[1])]; matches = [row["executable"] for row in rows if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "pg_dns" and row.get("profile", {}).get("test") and row.get("executable")]; assert len(matches) == 1, matches; print(matches[0])' pg-dns-build.jsonl)"
```

Select the `executable` from the `compiler-artifact` JSON row whose target name is
`pg_dns` and whose profile has `test=true`. Pass that exact absolute path as
`PG_DNS_TEST_BINARY` to the isolated fixture environment:

```bash
python3 tests/support/pg_dns_fixture.py "$PG_DNS_TEST_BINARY"
```

The DNS contract is in the dedicated `tests/pg_dns.rs` target, separate from
ordinary metadata contracts discovered by the plaintext SQL runtime gate. Keep
that target boundary: the DNS test requires its actual TLS/DNS fixture and must
not skip itself or return success when the fixture is absent.

The contract uses one real public DerivedCache instance with two connections and
the unchanged cache runtime's two blocking workers after a separate healthy
TLS/SQL preflight cache is destroyed. Before stalling DNS, public metadata SQL
deletes only the generated namespace's `git_rs_cache.maintenance_claims` child
rows, then its `git_rs_cache.namespaces` row, and asserts that the namespace is
absent. This prevents the preflight row from satisfying the recovery witness.
For each of two rounds the contract
observes new A and AAAA DNS requests before cancelling the public migration call.
Old request identities remain blackholed while fresh request identities receive
the real PG address. Recovery must execute real SQL: public metadata SQL must
observe exactly one row with the generated namespace ID and owner, absent before
recovery and present afterward. Cleanup deletes that namespace's maintenance
claims before deleting its namespace row, respecting the foreign key without
removing unrelated fixture state. Only after the recovery attempt are old DNS
requests answered for cleanup. No S3 service or PostgreSQL protocol emulator is
used. Run in a fresh fixture process for every attempt.

The fixture keys requests by source endpoint, DNS ID, name and type. It observes
real resolver traffic rather than equating two A/AAAA packets with two blocking
workers. Hickory sharing/retry behavior is part of the executed gate; if a round
does not produce fresh observable traffic, report that fixture failure rather
than removing the observation requirement or claiming successful qualification.

## RED mutation

Use a separate disposable source copy and separate build output. In
`src/database/manager.rs`, temporarily replace only the custom manager's public
trait `create` body with:

```rust
self.connect(self.config.clone()).await
```

This deliberately restores SDK hostname/GAI creation while retaining the same
custom pool, SDK wrapper, cancellation, five-second timeout and actual two-worker
cache runtime. Do not change the authoritative runtime's blocking-thread limit.
Rebuild that copy's `pg_dns` test binary with the same locked no-run command and
JSON executable-selection command (assigning `MUTANT_PG_DNS_TEST_BINARY`), then
pass its exact executable to a fresh isolated fixture:

```bash
python3 tests/support/pg_dns_fixture.py "$MUTANT_PG_DNS_TEST_BINARY" --expect-starvation
```

The harness accepts RED only after a successful TLS/SQL preflight, both observed
cancellation rounds, at least two old DNS source endpoints, no fresh DNS query
during recovery, and the specific fresh cache acquisition failure.
Setup, TLS, SQL migration and observation failures are not accepted as RED.
Restore the correct source and rerun GREEN. A GREEN run does not establish RED
failure sensitivity; retain separate revision-bound results for both. These
instructions describe the executable contract, not a record of completed runs.

## SQL session regression contracts

The following existing ignored tests in `tests/metadata.rs` exercise the public
metadata API independently of the controlled-DNS harness:

| Exact test name | Observable contract |
| --- | --- |
| `sql_reuses_sessions_and_does_not_recycle_failed_transactions` | Reuses at most eight backend sessions; resets an unfinished transaction's local statement timeout to `30s`; subsequent borrowers succeed after a transaction error. |
| `cancelled_sql_rolls_back_and_returns_pool_capacity` | Observes an active transaction before cancellation, bounds the caller's return, waits for its backend to disappear, checks that the insert did not commit, and verifies subsequent acquisitions. |
| `large_sql_queries_release_their_oversized_connection_buffers` | Sends a query with a 4 MiB synthetic comment, waits for that backend to disappear instead of remaining pooled, and verifies a subsequent query. |
| `blocked_timing_sink_does_not_pin_sql_sessions` | Runs the reuse/reset and cancellation contracts in child processes with a saturated diagnostic socket; requires bounded successful child exit. |

Use a disposable PostgreSQL database configured through `PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD` when required by server authentication, and `PGDATABASE`.
For the TLS fixture set `PGSSLMODE=verify-full` and `PGSSLROOTCERT` to its CA; the
certificate must match `PGHOST`. These four tests do not require cache credentials,
the DNS control harness, dnslib, S3 or native Git. The cancellation test performs
source-schema migration and creates/drops a synthetic table, so its role needs
schema migration and DDL rights. Backend observations require visibility of the
role's own sessions in `pg_stat_activity`. The blocked-sink contract additionally
requires Unix domain sockets and permission to execute the metadata test binary
as a child process.

Run each exact test serially, preferably in a fresh test process because the
authoritative database pool and its configuration are process-static:

```bash
cargo test --locked --test metadata sql_reuses_sessions_and_does_not_recycle_failed_transactions -- --ignored --exact --test-threads=1
cargo test --locked --test metadata cancelled_sql_rolls_back_and_returns_pool_capacity -- --ignored --exact --test-threads=1
cargo test --locked --test metadata large_sql_queries_release_their_oversized_connection_buffers -- --ignored --exact --test-threads=1
cargo test --locked --test metadata blocked_timing_sink_does_not_pin_sql_sessions -- --ignored --exact --test-threads=1
```

Alternatively, invoke the prebuilt metadata executable with the same exact test
name and `--ignored --exact --test-threads=1` arguments. The blocked-sink test
launches its own children and enables their timing diagnostics automatically.
A backend-disappearance observation is not a Rust driver join receipt,
and these session tests do not replace the DNS starvation RED/GREEN contract.

## Additional compatibility and resource gates

Re-run the existing S3 DNS blackhole, hosts-FQDN, hosts-short, invalid-config and
unreadable-config contracts from `s3-dns.md` after the shared resolver extraction.
For explicit-hostaddr TLS manager tests, repeat with invalid/unreadable resolver
configuration: dialing and CancelRequest must still work without DNS. A wrong
TLS hostname must still fail. Run the SQL session regression contracts above,
then the affected workspace checks.
The startup-settings test can also be repeated with a literal `PGHOST` when the
fixture certificate has the matching IP SAN, again without usable DNS config.

The TLS manager contracts observe startup settings, SDK cache attach/detach,
configured-host port fallback, actual query cancellation and hostname rejection.
Client TCP keepalive/user-timeout settings and randomized host/address weighting
are additionally preserved by the complete public-Config reconstruction and
nested-loop implementation; these tests do not measure their kernel state or
prove a statistical distribution. Multi-address DNS fallback, repeated-cycle
resource measurements and exact-image shutdown remain separate runtime gates.
