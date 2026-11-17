# Progress tracker for fork-free-plan.md

`[X]` means the changes and the stage's required checks have completed.

- [X] Stage 1: Correct complete-body lookup
- [X] Stage 2: Verify immutable-body behavior through real serving
- [ ] Stage 3: Implement fork-free adapters in an isolated qualification workspace
- [ ] Stage 4: Qualify the public-API replacements
- [ ] Stage 5: Add one bounded executor and cut over qualified callers
- [ ] Stage 6: Verify cutover, resources and zero-fork provenance
- [ ] Stage 7: Reconcile warm targets with the fork-free producer
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
  The locked workspace builds. Two native-oracle comparisons block blame cutover:
  incomplete coverage on an independent-root merge (Gitoxide, debug and release),
  and incorrect commit/path for same-path versus rename (libgit2). A captured
  legacy frame decodes; strict declared-header checks are still unavailable in
  the streaming-only probe. No production caller uses these candidates.
