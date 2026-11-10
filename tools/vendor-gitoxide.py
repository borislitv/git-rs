#!/usr/bin/env python3
"""Export the reviewed Gitoxide source selection without copying a working tree.

Only public upstream Git objects and the explicitly qualified source overlays are
read. Input locations are supplied by the operator and are never recorded. The
output is a source snapshot, not a claim that downstream runtime cutover is ready.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys
import tomllib


BASE = "dd39fb66e3b1551c9fe849043e39b357a9799ca1"
UPSTREAM = "https://github.com/GitoxideLabs/gitoxide"
SEEDS = ("gix", "gix-blame", "gix-pack", "gix-object", "gix-packetline", "gix-traverse")
QUALIFIED = {
    "gix-blame/src/file/function.rs": "b7885967957312e5f9e760c0edc58c09663212bf7035b1c2f478ba2348a0175b",
    "gix-blame/src/file/mod.rs": "0516eedb5127177e0217d858823e5300f0a6761f5197029512685b80f6d53902",
    "gix-blame/src/file/prepared.rs": "8aafcb2a1dfbcc038826fc8d5b918e237ca71d65f773f0b41f686657297b5732",
    "gix-blame/src/file/tests.rs": "fba7ef22cc85c16187daf0a725435d27eaeb7d72a2d4e2636dfe9bc942167292",
    "gix-diff/src/blob/platform.rs": "064045d96d7d20fe56f706287e371f51a161174e57bda03cece40b310d154382",
    "gix-diff/tests/diff/blob/platform.rs": "a710219e3b508124539c4b32ccc21caf1a26cdadbee1624a44d9bf2560118e76",
    "gix-imara-diff/src/intern.rs": "cd5c402a16f40be046b01f80936a8882d6e1c49c57659774d214f2872321d64b",
    "gix-imara-diff/src/myers/middle_snake.rs": "df5c668d0b0a3f617abe46c2a3c5bbcba0d14757ad3ee9577a5d0eacd49fcb89",
    "gix-imara-diff/tests/fixtures/make_myers_baseline.sh": "09dbad99c7bd4bdf9bca66994e6663b7159d5977b41523b6b3601ee254f624bd",
    "gix-imara-diff/tests/integration/interner.rs": "3f1b1f2cadffb8cf6b677b9709b22a2203774b5addd583fab2ee8b296408b8ad",
    "gix-imara-diff/tests/integration/main.rs": "edee5b5f38828f208b119ab268d9487181b4e081345c83abd8c3862a79c00c7d",
    "gix-pack/src/cache/lru.rs": "4724360e30a3f537f52ee308f36792c2fabedb4ada7cd335d34544fd6e855310",
    "gix-pack/src/cache/mod.rs": "7c76e876ce9500774ba47b4783ca6f627c67617375d6040e9d750052ae55eb38",
    "gix-pack/src/data/delta.rs": "ed8062eedf2f0c1fe5ca415ea575ff2b0abd4959bd45eb3ce7bb86976bf7d81e",
    "gix-pack/src/data/file/decode/entry.rs": "4561717f4830ba17e27fb2d0f11b2c57c4406f0925c87a1bfa91cc796fdf2940",
    "gix-pack/src/data/file/decode/mod.rs": "f6f2081ff7185f23319dfe6c1ae2d105472f38b420643d011fe370f9576aae41",
    "gix-pack/tests/pack/data/file.rs": "65d39febb38b422f48f8e1d17671a4ef94e2779986ee7e2e23db5588eadd5303",
}
PATCHES = (
    ("prepared-lines", "48a878fa47b5dee1273f2827ebbef3a601509ffa414e93a0ed867d0623a68c21"),
    ("loading", "68e97594f6be383fd6ad5406e0ddeddf313cfe2b965546ff33dacf6a2ec578c1"),
    ("unchanged-runs", "8ee9089104ae0c4ac773f88de8b89fd9c54da4fe0b0ef970f6aac75f45088563"),
    ("appended-output", "b91ce45dce228e1d07d09f6520b9055c25841c074d5c7e096527d9df2fc0e907"),
    ("indexed-changes", "ffc8dc629bc585a2d1b6221c586037b41a9dff6dc4b3efe424f2e8496b878b11"),
    ("myers-heuristics", "0c94feb3e9bdd4b74b991d554aafa7ca1a581a10ad82efc9c6d0e21f2605b258"),
    ("current-input-credit", "fd2094ae5f947505e230095a942276c6fa5e180c8f7c6d0cfbd4c9a0a9306a91"),
    ("borrowed-intern", "546d2ffbc7d00aca1366b1c5fc2f274fe9fdc6368b7fa8dcd59a8b790d0fe677"),
)
UNTRACKED = {"gix-blame/src/file/prepared.rs", "gix-imara-diff/tests/integration/interner.rs"}
TEST_PACKAGES = {"gix-blame", "gix-diff", "gix-imara-diff", "gix-pack", "tests/tools"}
FIXTURE_ROOT = "gix-pack/fuzz/artifacts"
FIXTURE_PROVENANCE = f"{FIXTURE_ROOT}/UPSTREAM.json"
FIXTURE_TREES = {
    "data_file": "1d5f4421ec4a868dc19463780c716b6b85426706",
    "index_file": "53a6da8581b5bff25d2ca0c53d7751a643e7fb9d",
    "multi_index_file": "516ccbfc6d397b9a88a699cbfe85bad74c040dc1",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encode_json(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def git(source, *args):
    return subprocess.run(
        ["git", "-C", str(source), *args], check=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=60,
    ).stdout


def upstream_fixture_files(source):
    """Read only authentic tracked artifacts from the pinned public Git tree."""
    files, records, targets = {}, {}, {}
    for target, expected_tree in FIXTURE_TREES.items():
        directory = f"{FIXTURE_ROOT}/{target}"
        actual_tree = git(source, "rev-parse", f"{BASE}:{directory}").decode().strip()
        if actual_tree != expected_tree:
            raise ValueError(f"upstream fixture tree differs: {target}")
        tree = git(source, "ls-tree", "-rz", BASE, "--", directory)
        count, total = 0, 0
        for entry in tree.split(b"\0"):
            if not entry:
                continue
            metadata, name = entry.split(b"\t", 1)
            mode, kind, blob = metadata.decode().split()
            name = name.decode()
            safe_name(name)
            if mode != "100644" or kind != "blob" or posixpath.dirname(name) != directory:
                raise ValueError(f"unexpected upstream fixture entry: {name}")
            data = git(source, "show", f"{BASE}:{name}")
            files[name] = (data, mode)
            records[name] = {"sha256": digest(data), "mode": mode, "git_blob": blob, "bytes": len(data)}
            count += 1
            total += len(data)
        targets[target] = {"git_tree": expected_tree, "files": count, "bytes": total}
    provenance = {
        "format": 1, "repository": UPSTREAM, "base": BASE,
        "license": "MIT OR Apache-2.0",
        "notices": ["gix-pack/LICENSE-MIT", "gix-pack/LICENSE-APACHE"],
        "description": "Original tracked public regression artifacts; no untracked fuzz inputs are exported.",
        "targets": targets, "files": records,
    }
    files[FIXTURE_PROVENANCE] = (encode_json(provenance), "100644")
    return files, provenance


def restore_upstream_fixtures(source, output):
    files, provenance = upstream_fixture_files(source)
    # Validate the entire intended addition before creating any files. Never
    # overwrite existing source, fixtures, manifests, or downstream patches.
    for name, (data, _) in files.items():
        path = output / name
        for parent in path.parents:
            if parent == output.parent:
                break
            if parent.is_symlink():
                raise ValueError(f"fixture destination traverses a symlink: {name}")
        if path.is_symlink() or (path.exists() and path.read_bytes() != data):
            raise ValueError(f"refusing to replace differing fixture: {name}")
    added = 0
    for name, (data, _) in files.items():
        path = output / name
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as handle:
                handle.write(data)
            path.chmod(0o644)
            added += 1
    print(f"Restored public upstream artifacts: {len(provenance['files'])} inputs, "
          f"{sum(t['bytes'] for t in provenance['targets'].values())} bytes; {added} new files including provenance")


def manifest_closure(source):
    """Follow all local manifest paths, including optional/target/dev paths."""
    todo = list(SEEDS)
    manifests = {}
    while todo:
        name = todo.pop()
        if name in manifests:
            continue
        raw = git(source, "show", f"{BASE}:{name}/Cargo.toml")
        manifest = tomllib.loads(raw.decode())
        manifests[name] = manifest
        for group in [manifest, *manifest.get("target", {}).values()]:
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for dependency in group.get(section, {}).values():
                    if isinstance(dependency, dict) and "path" in dependency:
                        target = posixpath.normpath(f"{name}/{dependency['path']}")
                        if target.startswith("../") or target.startswith("/"):
                            raise ValueError("dependency path escapes upstream root")
                        todo.append(target)
    if len(manifests) != 61:
        raise ValueError(f"unexpected manifest closure size: {len(manifests)}")
    return manifests


def qualified_inputs(source, patch_dir):
    if git(source, "rev-parse", "HEAD").decode().strip() != BASE:
        raise ValueError("source HEAD is not the reviewed upstream base")
    changed = set(git(source, "diff", "--name-only", BASE, "--").decode().splitlines())
    untracked = set(git(source, "ls-files", "--others", "--exclude-standard").decode().splitlines())
    if changed != set(QUALIFIED) - UNTRACKED or untracked != UNTRACKED:
        raise ValueError("source changed/untracked paths differ from the reviewed selection")
    overlays = {}
    for name, expected in QUALIFIED.items():
        path = source / name
        if path.is_symlink():
            raise ValueError(f"qualified source is a symlink: {name}")
        data = path.read_bytes()
        if digest(data) != expected:
            raise ValueError(f"qualified source digest mismatch: {name}")
        overlays[name] = data
    patches = {}
    for index, (name, expected) in enumerate(PATCHES, 1):
        data = (patch_dir / f"diagnostic-gix-{name}.patch").read_bytes()
        if digest(data) != expected:
            raise ValueError(f"patch digest mismatch: {name}")
        patches[f"patches/{index:02}-{name}.patch"] = data
    return overlays, patches


def safe_name(name):
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or str(path) != name:
        raise ValueError(f"unsafe archive path: {name}")


def export_files(source, patch_dir):
    overlays, patches = qualified_inputs(source, patch_dir)
    manifests = manifest_closure(source)
    # Read tracked objects directly. `git archive` can omit tests/notices via
    # export-ignore attributes and cannot represent the two new overlay files.
    tree = git(source, "ls-tree", "-rz", BASE, "--", *sorted(manifests), "LICENSE-APACHE", "LICENSE-MIT")
    entries = []
    for record in tree.split(b"\0"):
        if not record:
            continue
        metadata, name = record.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        if kind != "blob" or mode not in {"100644", "100755", "120000"}:
            raise ValueError("unsupported source tree member")
        entries.append((name.decode(), mode, oid))
    contents = subprocess.run(
        ["git", "-C", str(source), "cat-file", "--batch"],
        input="".join(f"{oid}\n" for _, _, oid in entries).encode(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=60,
    ).stdout
    files = {}
    offset = 0
    for name, mode, oid in entries:
        end = contents.index(b"\n", offset)
        header_oid, kind, size = contents[offset:end].decode().split()
        if header_oid != oid or kind != "blob":
            raise ValueError("source blob response differs from tree")
        size = int(size)
        data = contents[end + 1:end + 1 + size]
        offset = end + size + 2
        safe_name(name)
        owner = next((crate for crate in manifests if name.startswith(crate + "/")), None)
        if owner is not None:
            within = PurePosixPath(name[len(owner) + 1:]).parts
            if (within[0] == "fuzz" and not any(
                name.startswith(f"{FIXTURE_ROOT}/{target}/") for target in FIXTURE_TREES
            )) or (
                owner not in TEST_PACKAGES and within[0] in {"tests", "benches", "examples"}
            ):
                continue
        if mode == "120000":
            target = posixpath.normpath(posixpath.join(posixpath.dirname(name), data.decode()))
            if target.startswith("../") or target.startswith("/"):
                raise ValueError(f"symlink escapes exported source: {name}")
        files[name] = (data, mode)
    if offset != len(contents):
        raise ValueError("unconsumed source blob response")
    fixtures, _ = upstream_fixture_files(source)
    files.update(fixtures)
    for name, data in overlays.items():
        files[name] = (data, "100755" if name.endswith(".sh") else "100644")
    for name, data in patches.items():
        files[name] = (data, "100644")
    for name in manifests:
        manifest_path = f"{name}/Cargo.toml"
        data, mode = files[manifest_path]
        workspace_path = posixpath.relpath(".", name)
        text = data.decode()
        if name not in TEST_PACKAGES:
            # These packages are retained as library dependencies, not as a copy
            # of the upstream workspace's unrelated test/benchmark corpora.
            text = re.sub(
                r"(?ms)^\[\[(?:bin|bench|test|example)\]\]\n.*?(?=^\[|\Z)", "", text,
            )
            text = re.sub(r"(?m)^auto(?:bins|benches|tests|examples)\s*=.*\n", "", text)
            text = text.replace(
                "[package]\n",
                "[package]\nautobins = false\nautobenches = false\nautotests = false\nautoexamples = false\n",
                1,
            )
        text = text.replace(
            "[package]\n", f"[package]\nworkspace = {json.dumps(workspace_path)}\n", 1,
        )
        files[manifest_path] = (text.encode(), mode)

    upstream_root = tomllib.loads(git(source, "show", f"{BASE}:Cargo.toml").decode())
    # Keep the original lint block verbatim; the CLI package/workspace members are
    # deliberately not imported into this source-only workspace.
    root_text = git(source, "show", f"{BASE}:Cargo.toml").decode()
    lint_text = root_text[root_text.index("[workspace.lints.rust]"):]
    if "lints" not in upstream_root["workspace"]:
        raise ValueError("upstream workspace lint definitions are missing")
    workspace = "[workspace]\nresolver = \"2\"\nmembers = [\n"
    workspace += "".join(f"  {json.dumps(name)},\n" for name in sorted(manifests))
    workspace += "]\n\n" + lint_text
    files["Cargo.toml"] = (workspace.encode(), "100644")
    packages = {
        m["package"]["name"]: {"path": name, "version": m["package"]["version"],
                                "license": m["package"]["license"]}
        for name, m in manifests.items()
    }
    notices = sorted(name for name in files if PurePosixPath(name).name.startswith("LICENSE"))
    license_doc = "# Gitoxide source licenses\n\n"
    license_doc += "Original per-package license files and symlinks are preserved.\n\n"
    license_doc += "| Package | Version | SPDX expression | Directory |\n| --- | --- | --- | --- |\n"
    license_doc += "".join(
        f"| {name} | {p['version']} | {p['license']} | `{p['path']}` |\n"
        for name, p in sorted(packages.items())
    )
    files["LICENSES.md"] = (license_doc.encode(), "100644")
    files["UPSTREAM.md"] = ((
        "# Gitoxide source snapshot\n\n"
        f"Upstream: {UPSTREAM}\n\nBase revision: `{BASE}`.\n\n"
        "This is an upstream-derived source snapshot, not a new upstream commit. "
        "The eight ordered generic patches in `patches/` provide prepared line tokens, "
        "bounded resource retention, unchanged-run reuse, appended delta output, indexed "
        "changes, Myers heuristics, current-input token credit and borrowed interning. "
        "The selected source hashes are recorded in `source-manifest.json`.\n\n"
        "The source-only workspace follows all local normal, optional, target, build "
        "and test-helper manifest paths from the selected library entrypoints. "
        "Upstream CLI workspace members are excluded; root lint definitions and original "
        "package library source/notices are retained. The four changed packages and "
        "test helper retain their original tests/benchmarks/examples; other packages "
        "omit those unrelated trees and disable/remove their corresponding Cargo "
        "targets. Fuzz workspaces are omitted, but the three original gix-pack "
        "artifact input directories are retained with exact public Git provenance "
        "in `gix-pack/fuzz/artifacts/UPSTREAM.json`. The root workspace manifest is "
        "normalized; each package also explicitly names that workspace "
        "to avoid inheriting the consuming application's lint settings. "
        "Tracked Git objects are read directly "
        "to retain original tests and license links regardless of export attributes. "
        "The two newly introduced source/test files are included explicitly.\n\n"
        "Library APIs retain upstream feature gates. SHA1, blob/blame, pack generation, "
        "streaming input and blocking packetline IO must be selected explicitly by "
        "consumers. Declared baseline MSRV is Rust 1.88; optional sirocco requires 1.89. "
        "The application pins its toolchain independently. Original dependency unsafe "
        "internals are not a claim of unsafe-free transitive code.\n\n"
        "This initial export does not yet implement request-local inner-loop "
        "cancellation or replace research environment reads with per-job options. "
        "It is not evidence of native-compatible server/blame/history cutover.\n\n"
        "Regeneration: `python3 tools/vendor-gitoxide.py --source SOURCE "
        "--patch-dir PATCHES --output OUTPUT`. Input directories are operator-supplied "
        "and are never embedded. Default export refuses an existing destination; "
        "`--check` compares without writing. `tools/check-core-source.py` verifies "
        "the exact inventory, original notices and root Cargo source wiring. "
        "Intentional downstream source changes require its explicit "
        "`--record-local-changes REASON` operation, which records a reviewed delta "
        "without replacing the baseline identity.\n"
    ).encode(), "100644")
    manifest = {
        "format": 1, "upstream": {"repository": UPSTREAM, "base": BASE},
        "qualified_source_hashes": QUALIFIED,
        "patches": [{"path": name, "sha256": digest(data)} for name, data in sorted(patches.items())],
        "packages": packages,
        "original_notices": {name: {"sha256": digest(files[name][0]), "mode": files[name][1]} for name in notices},
        "files": {name: {"sha256": digest(data), "mode": mode} for name, (data, mode) in sorted(files.items())},
        "local_changes": [],
    }
    manifest["baseline_inventory_sha256"] = digest(encode_json(manifest["files"]))
    manifest["baseline_files"] = manifest["files"]
    files["source-manifest.json"] = (encode_json(manifest), "100644")
    # Do not serialize input paths, even accidentally in an upstream file/patch.
    for name, (data, _) in files.items():
        if any(str(path).encode() in data for path in (source, patch_dir)):
            raise ValueError(f"operator input location leaked into exported file: {name}")
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--patch-dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("vendor/gitoxide"))
    parser.add_argument("--check", action="store_true", help="compare the initial export without writing")
    parser.add_argument("--refresh-initial", action="store_true", help="regenerate only an unchanged initial export")
    parser.add_argument("--restore-upstream-fixtures", action="store_true",
                        help="add only missing pinned upstream artifacts; never refresh source/manifests")
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.source.resolve()) or (
        args.patch_dir is not None and args.output.resolve().is_relative_to(args.patch_dir.resolve())
    ):
        raise ValueError("output must not modify an input source/patch directory")
    if args.restore_upstream_fixtures:
        if args.check or args.refresh_initial:
            raise ValueError("fixture restoration cannot refresh/check the initial export")
        restore_upstream_fixtures(args.source.resolve(), args.output.resolve())
        return
    if args.patch_dir is None:
        raise ValueError("initial export requires --patch-dir")
    files = export_files(args.source.resolve(), args.patch_dir.resolve())
    if args.check:
        actual = {p.relative_to(args.output).as_posix() for p in args.output.rglob("*") if not p.is_dir() or p.is_symlink()}
        if actual != set(files):
            raise ValueError("exported path inventory differs from expected initial snapshot")
        for name, (data, mode) in files.items():
            path = args.output / name
            actual = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
            actual_mode = "120000" if path.is_symlink() else "100755" if path.stat().st_mode & 0o111 else "100644"
            if actual != data or actual_mode != mode:
                raise ValueError(f"export differs: {name}")
    else:
        if args.output.exists():
            if not args.refresh_initial:
                raise ValueError("destination exists; use a fresh destination or --check")
            previous = json.loads((args.output / "source-manifest.json").read_text())
            if previous["local_changes"]:
                raise ValueError("cannot refresh an export with downstream changes")
            actual_paths = {p.relative_to(args.output).as_posix() for p in args.output.rglob("*") if not p.is_dir() or p.is_symlink()}
            if actual_paths != set(previous["files"]) | {"source-manifest.json"}:
                raise ValueError("cannot refresh an export with unexpected/missing files")
            for name, expected in previous["files"].items():
                path = args.output / name
                data = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
                mode = "120000" if path.is_symlink() else "100755" if path.stat().st_mode & 0o111 else "100644"
                if {"sha256": digest(data), "mode": mode} != expected:
                    raise ValueError(f"cannot refresh modified export: {name}")
            for name in sorted(actual_paths - set(files)):
                (args.output / name).unlink()
            # Remove empty directories left by the narrower source selection.
            for directory, _, _ in os.walk(args.output, topdown=False, followlinks=False):
                path = Path(directory)
                if path != args.output and not path.is_symlink() and not any(path.iterdir()):
                    path.rmdir()
        else:
            args.output.mkdir(parents=True)
        for name, (data, mode) in files.items():
            path = args.output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if mode == "120000":
                if not path.is_symlink():
                    path.symlink_to(data.decode())
            else:
                path.write_bytes(data)
                path.chmod(0o755 if mode == "100755" else 0o644)
    print(f"Gitoxide initial export {'matches' if args.check else 'written'}: {len(files)} files")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"vendor export failed: {error}", file=sys.stderr)
        sys.exit(1)
