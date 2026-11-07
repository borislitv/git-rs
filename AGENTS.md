# Development contract

- Application code is safe Rust: `#![forbid(unsafe_code)]`. SQL uses Tokio,
  tokio-postgres and a bounded connection pool; never execute a SQL subprocess.
- Prefer established, maintained Rust crates for standard functionality over
  custom implementations. This is not a restriction to the Rust standard library.
- Use upstream Git for protocols, packs, deltas and object validation. Never execute a shell
  with client input, check out client trees, or load client hooks/configuration.
- PostgreSQL owns refs and the pack catalog; immutable S3 objects own pack/idx bytes.
- A successful push requires uploads followed by one confirmed SQL transaction.
  Compare complete pre-receive and proc-receive command lists. Never fall through.
- All pushes are all-or-nothing, including pushes without `--atomic`.
- Caches live only in the serving process's memory. Scratch is private to one request.
- Serving never deletes objects. Explicit owner-only history truncation requires a verified
  native bundle backup, reader/writer fencing and durable retirement before object cleanup.
- Test native Git behavior, concurrent publications, dependency failures and cold restarts.
- Public artifacts use synthetic data, reserved example domains and public sources only.
- Keep unverified operational claims explicit. A local development test is not an HA proof.
