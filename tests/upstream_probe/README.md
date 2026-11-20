# Fork-free dependency qualification

This standalone Cargo workspace exercises public library APIs against synthetic
objects. Run it separately from the server workspace. A failing parity test is a
cutover blocker: it must continue to compare against the independent oracle.

## Reproduce

From `tests/upstream_probe`, use Rust/Cargo 1.99.1, Git 2.56.0 on `PATH`, a C
toolchain, `pkg-config`, GNU `timeout`, and matching GIO/GLib development and
runtime libraries. The recorded Linux run used Ubuntu `2.88.0-1ubuntu0.1` for
GIO/GLib and `LC_ALL=C` for the Git subprocesses. Run under a finite process,
memory and time budget; the adapters' own byte counters do not bound native RSS.

```sh
git --version
pkg-config --modversion gio-2.0 glib-2.0
cargo fetch --locked
cargo fmt --all -- --check
cargo metadata --locked --offline --format-version 1
cargo check --locked --offline --all-targets
cargo clippy --locked --offline --all-targets -- -D warnings
cargo tree --locked --offline
cargo test --locked --offline --no-fail-fast -- --test-threads=1
cargo test --release --locked --offline --test blame native_full_independent_root_merge -- --exact --test-threads=1
cargo test --locked --offline --test git2_candidate git2_attribution_merge_prefers_same_path_before_rename -- --exact --nocapture --test-threads=1
```

`Cargo.lock` pins official Gitoxide `b8e1eb2a` (`gix-blame` 0.20.0, `gix-pack`
0.77.0), zstd-rs `5c4970b9` (0.14.4, vendored libzstd 1.5.7, experimental API),
[`zstandard` 0.1.9](https://docs.rs/zstandard/0.1.9/zstandard/fn.parse_frame_header.html)
(header inspection only, no default features, MIT OR Apache-2.0), `gio` 0.21.2,
and dev-only `git2` 0.21.0 with bundled libgit2 1.9.7 and no transport features.
The native libraries have their own licenses: [libgit2](https://github.com/libgit2/libgit2/blob/v1.9.7/COPYING),
[libzstd](https://github.com/facebook/zstd/blob/v1.5.7/LICENSE), and
[GLib/GIO](https://github.com/GNOME/glib/blob/2.88.0/COPYING). A production
image would need its own package inventory and resource qualification.

## Results on the pinned inputs

| Debug test target | Pass / fail / ignored | What the comparison establishes |
| --- | --- | --- |
| `blame` | 13 / 1 / 0 | Full fixture porcelain for roots, edits, grouping, same-path whole-parent preference, symlink, repeated-line tie and two rename cases. Includes a separate ranges-only control. |
| `git2_candidate` | 1 / 1 / 0 | In-memory libgit2 attribution only: commit, raw path, both line starts and group length. |
| `pack` | 8 / 0 / 0 | Stored-entry reads with OFS, forward REF and cross-pack bases. Checks original header widths, spans, CRC/OID, catalog cycles, short/corrupt reads and byte limits. |
| `encoding` | 6 / 0 / 0 | Git metadata fields, including Latin-1 aliases, unknown encoding, late malformed input, TSCII and stateful conversion. |
| `codec` | 11 / 0 / 0 | Strict frame acceptance, legacy bytes at exact limits, independent digests, frame completion and two overlapping job lifetimes. |

The full debug run returned nonzero: **39 passed, 2 failed, 0 ignored**.
Formatting, locked metadata, all-target compilation, Clippy (`-D warnings`) and
dependency-tree checks passed. Two valid histories distinguish the candidates:

- **Two independent roots each contribute a line.** Git 2.56 attributes both.
  Gitoxide leaves target `0..1` unblamed in debug; in release the adapter rejects
  the incomplete coverage. libgit2 matches both groups on this fixture.
- **Same-path parent competes with an exact rename.** Git chooses the later
  same-path parent at `f\t\xfe`; libgit2 chooses the earlier rename candidate at
  `old\t\xff`. The coordinates and lengths agree; the commits and path bytes do not.

## What changed after the first run

- The previous `26 / 2 / 0` matrix is recorded at `b19c6c4`. The strict codec
  now checks at most 18 bytes **before** creating the decoder or allocating the
  output. It requires a standard frame, matching declared size, window ≤ 1 MiB,
  no nonzero dictionary ID and a checksum. The streaming decoder still checks
  the footer and exact input/output lengths. A control that removed only the
  checksum requirement accepted a checksum-less frame; restoring the check
  passed the same assertion. The test includes an independently captured frame
  from the application's legacy writer at `3eda408`.
- Native `index-pack --strict` accepted a redundant size encoding. The reader
  retains the exact original header and uses its physical width to locate the
  compressed payload, even when representing an OFS base as a REF in memory.
  The gate covers stored entries with a held v2 index; it does not qualify
  receive intake, pack/index writing or process-level resource limits.
- The renderer no longer uses a traversal log for `previous`. In one fixture,
  a later edit removes the last unchanged line from a rename; Git still names
  the old path. This full-byte comparison failed before the fix and passed
  afterwards. The adapter examines the **attributed commit's own trees**. Its
  rename proof is limited to a single physical parent, one regular file in
  each flat tree, bounded verified objects, a majority of identical complete
  byte spans and one matching public tree-rewrite result. It cannot establish
  a predecessor for arbitrary renames or merges.

Neither candidate qualifies as the complete `NativeRootV1` backend. Selecting a
library by the shape of the requested tip does not resolve a merge deeper in
history. A date cutoff can change attribution when a non-root is older than a
root. [`jj-lib`'s annotator](https://github.com/jj-vcs/jj/blob/v0.47.1/lib/src/annotate.rs)
was inspected but not executed: it walks a fixed path and exposes no predecessor
path. These results do not establish serving latency, a production resource
boundary or cross-commit reuse for a changed file.
