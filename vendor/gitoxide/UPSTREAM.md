# Gitoxide source snapshot

Upstream: https://github.com/GitoxideLabs/gitoxide

Base revision: `dd39fb66e3b1551c9fe849043e39b357a9799ca1`.

This is an upstream-derived source snapshot, not a new upstream commit. The eight ordered generic patches in `patches/` provide prepared line tokens, bounded resource retention, unchanged-run reuse, appended delta output, indexed changes, Myers heuristics, current-input token credit and borrowed interning. The selected source hashes are recorded in `source-manifest.json`.

The source-only workspace follows all local normal, optional, target, build and test-helper manifest paths from the selected library entrypoints. Upstream CLI workspace members are excluded; root lint definitions and original package library source/notices are retained. The four changed packages and test helper retain their original tests/benchmarks/examples; other packages omit those unrelated trees and disable/remove their corresponding Cargo targets. Fuzz workspaces are omitted. The root workspace manifest is normalized; each package also explicitly names that workspace to avoid inheriting the consuming application's lint settings. Tracked Git objects are read directly to retain original tests and license links regardless of export attributes. The two newly introduced source/test files are included explicitly.

Library APIs retain upstream feature gates. SHA1, blob/blame, pack generation, streaming input and blocking packetline IO must be selected explicitly by consumers. Declared baseline MSRV is Rust 1.88; optional sirocco requires 1.89. The application pins its toolchain independently. Original dependency unsafe internals are not a claim of unsafe-free transitive code.

This initial export does not yet implement request-local inner-loop cancellation or replace research environment reads with per-job options. It is not evidence of native-compatible server/blame/history cutover.

Regeneration: `python3 tools/vendor-gitoxide.py --source SOURCE --patch-dir PATCHES --output OUTPUT`. Input directories are operator-supplied and are never embedded. Default export refuses an existing destination; `--check` compares without writing. `tools/check-core-source.py` verifies the exact inventory, original notices and root Cargo source wiring. Intentional downstream source changes require its explicit `--record-local-changes REASON` operation, which records a reviewed delta without replacing the baseline identity.
