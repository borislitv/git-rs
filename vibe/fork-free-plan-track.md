# Progress tracker for fork-free-plan.md

`[X]` means the changes and the stage's required checks have completed.

- [X] Stage 1: Correct complete-body lookup
- [ ] Stage 2: Verify immutable-body behavior through real serving
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
