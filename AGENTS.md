# Development contract

- Application code is safe Rust: `#![forbid(unsafe_code)]`. SQL uses Tokio,
  tokio-postgres and a bounded connection pool; never execute a SQL subprocess.
- Prefer established, maintained Rust crates for standard functionality over
  custom implementations. This is not a restriction to the Rust standard library.
- The target server data plane uses maintained Rust libraries for protocols, packs,
  deltas, graph validation and blame. Native Git is an external compatibility client
  and test oracle, not the target server implementation. Existing native server
  boundaries are migration debt documented in docs/rust-data-plane.md; do not add
  new ones or remove their guarantees without a verified library replacement.
- Never execute a shell with client input, check out client trees, or load client
  hooks/configuration. Open application-owned repositories with isolated library options.
- PostgreSQL owns refs and the pack catalog; immutable S3 objects own pack/idx bytes.
- A successful push requires uploads followed by one confirmed SQL transaction.
  Compare complete pre-receive and proc-receive command lists. Never fall through.
- All pushes are all-or-nothing, including pushes without `--atomic`.
- Local caches are RAM-only by default. Optional derived artifacts use an independently
  migrated PostgreSQL schema/pool and explicitly separate cache S3 credentials/namespace.
  They never authorize source access or participate in authoritative publication.
  Cache claims do not replace source holds. Scratch is private to one request.
- Serving never deletes objects. Explicit owner-only history truncation requires a verified
  native bundle backup, reader/writer fencing and durable retirement before object cleanup.
- Test native Git behavior, concurrent publications, dependency failures and cold restarts.
- Blame path/ancestry/metadata, attribution and complete porcelain use the pinned Rust core;
  never reintroduce a native blame fallback. GIT_RS_BLAME_WORKER is obsolete and rejected.
  Native receive/upload and backup/history-maintenance boundaries remain explicit migration debt.
- BlameOptions::default selects the performance-qualified NativeRootV1 profile with all five
  BlameTuning switches on. Keep the vendor tuning default all-off and the explicit conservative
  core profile available. Production exposes no tuning/producer override; test-fixture selectors
  are not a serving API. Profile changes require fresh native, resource and performance qualification.
- Shared blame bodies and authenticated WorkSet objects/mappings are optional derivations.
  Preserve source authority, generation/epoch fences, complete line partitions and producer/key
  scope. Persist completed units only, never vendor interner IDs or a suspended traversal frontier.
  Keep SQL/network I/O out of memory callbacks. Do not weaken protected signing-file checks.
- Actual jobs retain finite lifetime, source holds and admission through cleanup; each waiter
  owns its cancellation/deadline. Post-confirmed opt-in warming uses the same CPU pool and work
  admission, fresh authorized source and its own finite deadline; it cannot fail a confirmed push.
- Cache v1-to-v2 migration is independent and forward-only. Old migrators reject v2; rollback
  requires caching disabled or a separate compatible cache schema/database and fresh owned prefix.
  Never infer old-writer quiescence from schema rejection or reuse retired payload identities.
- Ordinary workspace tests skip ignored contracts. Split blame CI wires the explicit compiled
  contracts, artifact/migration harness and focused serving/runtime gates; its same-workflow
  hosted execution is pending. Preserve locally qualified coverage and finite budgets.
  Root/memo proofs are not Stage 5/8 production performance results.
- Public artifacts use synthetic data, reserved example domains and public sources only.
- Keep unverified operational claims explicit. A local development test is not an HA proof.
