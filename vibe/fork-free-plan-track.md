# Progress tracker for the dependency migration and blame plan

`[X]` means the changes and the stage's required checks have completed.

- [X] Stage 1: Correct complete-body lookup
- [X] Stage 2: Verify immutable-body behavior through real serving
- [ ] Stage 3: Implement fork-free adapters in an isolated qualification workspace
- [ ] Stage 4: Qualify the public-API replacements
- [ ] Stage 5: Add one bounded executor and cut over qualified callers
- [ ] Stage 6: Verify cutover, resources and dependency provenance
- [ ] Stage 7: Reconcile warm targets with the selected producer
- [ ] Stage 8: Verify usefulness and recovery end to end

## Notes

- Plan validation: retain live source fences and named-Lease behavior. Treat
  changed-file cross-commit reuse and each upstream parity check as separate gates.
- Stage 1 (2026-11-17): `cargo fmt --all -- --check` and locked workspace
  `cargo check --workspace --all-targets` passed. Serving qualification is stage 2.
- Stage 2: the [full owned blame gate](https://github.com/borislitv/git-rs/actions/runs/44171006222/job/134358219594)
  passed on `5670ed2`, including real push/restart/force-push and ACL/epoch fences.
  The [unchanged-edge control](https://github.com/borislitv/git-rs/actions/runs/44171006222/job/134349754710)
  failed at its intended assertion with optimization disabled and passed on restoration.
  Earlier head `23db2fb` masked a source digest error; the unchanged integrity contract
  passed after `4a88308` propagated the original error from physical preflight.
- Stages 3–4 (2026-11-18): [standalone candidates, commands and results](../tests/upstream_probe/README.md).
  Final isolated run: 39 passed, 2 failed, none ignored. Strict upstream header
  inspection and payload streaming preserve the tested legacy frame policy.
  Physical origin-tree evidence recovers `previous` for the tested edited rename,
  including after a later change removes its last unchanged line. The stored-entry,
  encoding and codec suites passed; locked all-target check and Clippy passed.
  Two native-oracle comparisons disqualify the upstream blame candidates:
  incomplete coverage on an independent-root merge (Gitoxide, debug and release),
  and incorrect commit/path for same-path versus rename (libgit2).
- Blame backend selected: the existing owned Gitoxide pin `0354c999` passed both
  merge reproductions through `SnapshotSession` against Git 2.56 full porcelain.
  The full core blame test target passed 16/16. Keep the two upstream failures as
  comparison evidence. Changed-file +1/+100 reuse is a separate open gate.
- Changed-file prototype: the pinned core serializes a certified one-coordinate
  base and composes a verified linear suffix. The 20,000-commit test reloads the
  seed in a fresh session and matches Git 2.56 at +1/+100 with no older commit
  reads. It declines +101; ordinary blame triggers the old-history read guard.
  The [short owned HTTP gate](https://github.com/borislitv/git-rs/actions/runs/44246492811/job/134612013812)
  passed on `2c50adb`: both restarted processes returned native bytes with exact
  suffix counts and no attribution traversal. Observed response times were 463 ms
  at +1 and 1,715 ms at +100, on an eight-commit base. The +1 advance was a real
  HTTP push; +100 used the production catalog transaction after bulk HTTP receive
  returned an empty reply in an earlier local run. Bulk receive remains open.
  The 20,000-commit HTTP contract and wrong-signing-key control are part of the
  default owned gate's ignored-test inventory; their execution is tracked separately.
- On [`03a8d7a`](https://github.com/borislitv/git-rs/actions/runs/44250571496/job/134628008683),
  the eight-commit case passed with the wrong-key control: the wire stayed
  native-exact and the request used ordinary attribution. The 20,000-commit
  case hit its 120-second deadline during the cold base request, before either
  advance. That fixture used a 1 MiB source cache, below a single full 4 MiB
  source range. The next run uses the serving default 64 MiB for the long case
  and logs phase timings, retaining the deadline and output/reuse assertions.
