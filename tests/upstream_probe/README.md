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
cargo tree --locked --offline
cargo test --locked --offline --no-fail-fast -- --test-threads=1
cargo test --release --locked --offline --test blame native_full_independent_root_merge -- --exact --test-threads=1
cargo test --locked --offline --test git2_candidate git2_attribution_merge_prefers_same_path_before_rename -- --exact --nocapture --test-threads=1
```

`Cargo.lock` pins official Gitoxide `b8e1eb2a` (`gix-blame` 0.20.0, `gix-pack`
0.77.0), zstd-rs `5c4970b9` (0.14.4, vendored libzstd 1.5.7, experimental API),
`gio` 0.21.2, and dev-only `git2` 0.21.0 with bundled libgit2 1.9.7 and no
transport features. The native libraries have their own licenses: [libgit2](https://github.com/libgit2/libgit2/blob/v1.9.7/COPYING),
[libzstd](https://github.com/facebook/zstd/blob/v1.5.7/LICENSE), and
[GLib/GIO](https://github.com/GNOME/glib/blob/2.88.0/COPYING). A production
image would need its own package inventory and resource qualification.

## Results on the pinned inputs

| Debug test target | Pass / fail / ignored | What the comparison establishes |
| --- | --- | --- |
| `blame` | 7 / 1 / 0 | Full fixture porcelain for roots, ordinary edit, total replacement with `previous`, raw-path rename/edit and a grouping gap. Includes a separate ranges-only control. |
| `git2_candidate` | 1 / 1 / 0 | In-memory libgit2 attribution only: commit, raw path, both line starts and group length. |
| `pack` | 4 / 0 / 0 | Stored-entry reads including OFS, forward REF and cross-pack bases, span/CRC/OID checks, source failure and byte limits. Receive and pack/index writers are separate callers. |
| `encoding` | 6 / 0 / 0 | Git metadata fields, including Latin-1 aliases, unknown encoding, late malformed input, TSCII and stateful conversion. |
| `codec` | 8 / 0 / 0 | Streaming, byte caps, integrity and decoding a frame captured from the actual legacy writer. The strict-header capability test reports unavailable at this revision. |

The full debug run returned nonzero: **26 passed, 2 failed, 0 ignored**.
Formatting, locked metadata, all-target compilation and dependency-tree checks
passed. Two valid histories distinguish the candidates:

- **Two independent roots each contribute a line.** Git 2.56 attributes both.
  Gitoxide leaves target `0..1` unblamed in debug; in release the adapter rejects
  the incomplete coverage. libgit2 matches both groups on this fixture.
- **Same-path parent competes with an exact rename.** Git chooses the later
  same-path parent at `f\t\xfe`; libgit2 chooses the earlier rename candidate at
  `old\t\xff`. The coordinates and lengths agree; the commits and path bytes do not.

The renderer is fixture-specific. A debug traversal record is not general
`previous` evidence, and matching a blob or group length does not prove origin.
Neither candidate qualifies as the complete `NativeRootV1` backend. The results
do not establish serving latency or cross-commit reuse for a changed file.
