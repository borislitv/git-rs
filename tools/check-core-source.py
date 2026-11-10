#!/usr/bin/env python3
"""Audit the exact vendored source, notices, and Cargo source identity.

The default operation never updates expectations. Intentional downstream edits
must be recorded explicitly with --record-local-changes and a reviewable reason.
The test helper's exact generated fixture scratch directories are not source.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import sys
import tomllib


MANIFEST = "source-manifest.json"
GENERATED_FIXTURE_ROOTS = {
    f"{package}/tests/fixtures/generated-do-not-edit"
    for package in ("gix-blame", "gix-diff", "gix-imara-diff", "gix-pack", "tests/tools")
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encode_json(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def selection():
    spec = importlib.util.spec_from_file_location("vendor_gitoxide", Path(__file__).with_name("vendor-gitoxide.py"))
    module = importlib.util.module_from_spec(spec)
    # Do not create __pycache__ in the public source tree during a read-only audit.
    sys.dont_write_bytecode = True
    spec.loader.exec_module(module)
    return module


def inventory(root):
    result = {}
    if root.is_symlink() or not root.is_dir():
        raise ValueError("vendor root must be an ordinary directory")
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = [name for name in dirs
                   if (Path(directory) / name).relative_to(root).as_posix() not in GENERATED_FIXTURE_ROOTS]
        for name in [*dirs, *files]:
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if path.is_symlink():
                target = os.readlink(path)
                normalized = posixpath.normpath(posixpath.join(posixpath.dirname(relative), target))
                if normalized.startswith("../") or normalized.startswith("/"):
                    raise ValueError(f"vendor symlink escapes root: {relative}")
                data, mode = target.encode(), "120000"
            elif path.is_dir():
                continue
            elif path.is_file():
                if relative == MANIFEST:
                    continue
                data = path.read_bytes()
                mode = "100755" if path.stat().st_mode & 0o111 else "100644"
            else:
                raise ValueError(f"unsupported source entry: {relative}")
            result[relative] = {"sha256": digest(data), "mode": mode}
    return dict(sorted(result.items()))


def upstream_fixture_inventory(root):
    initial = selection()
    path = root / initial.FIXTURE_PROVENANCE
    if path.is_symlink() or path.stat().st_mode & 0o111:
        raise ValueError("upstream fixture provenance must be an ordinary file")
    provenance = json.loads(path.read_text())
    if (provenance["format"] != 1 or provenance["repository"] != initial.UPSTREAM
            or provenance["base"] != initial.BASE
            or provenance["license"] != "MIT OR Apache-2.0"
            or provenance["notices"] != ["gix-pack/LICENSE-MIT", "gix-pack/LICENSE-APACHE"]
            or set(provenance["targets"]) != set(initial.FIXTURE_TREES)):
        raise ValueError("unexpected upstream fixture provenance")
    expected_entries = set(initial.FIXTURE_TREES) | {PurePosixPath(initial.FIXTURE_PROVENANCE).name}
    if {p.name for p in path.parent.iterdir()} != expected_entries:
        raise ValueError("unexpected input outside the three upstream fixture targets")
    if not all((root / notice).is_file() for notice in provenance["notices"]):
        raise ValueError("original upstream fixture license notices are missing")
    actual_paths = set()
    records = {}
    for target, tree_id in initial.FIXTURE_TREES.items():
        directory = root / initial.FIXTURE_ROOT / target
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError(f"missing ordinary upstream fixture directory: {target}")
        tree, count, total = bytearray(), 0, 0
        for artifact in sorted(directory.iterdir(), key=lambda p: p.name.encode()):
            name = artifact.relative_to(root).as_posix()
            if artifact.is_symlink() or not artifact.is_file():
                raise ValueError(f"unexpected upstream fixture entry: {name}")
            data = artifact.read_bytes()
            blob = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
            record = {"sha256": digest(data), "mode": "100644", "git_blob": blob, "bytes": len(data)}
            if provenance["files"].get(name) != record or artifact.stat().st_mode & 0o111:
                raise ValueError(f"upstream fixture digest/mode mismatch: {name}")
            tree.extend(b"100644 " + artifact.name.encode() + b"\0" + bytes.fromhex(blob))
            records[name] = {"sha256": record["sha256"], "mode": record["mode"]}
            actual_paths.add(name)
            count += 1
            total += len(data)
        computed_tree = hashlib.sha1(f"tree {len(tree)}\0".encode() + tree).hexdigest()
        if computed_tree != tree_id or provenance["targets"][target] != {
            "git_tree": tree_id, "files": count, "bytes": total,
        }:
            raise ValueError(f"upstream fixture tree/count differs: {target}")
    if actual_paths != set(provenance["files"]):
        raise ValueError("upstream fixture path inventory differs")
    records[initial.FIXTURE_PROVENANCE] = {"sha256": digest(path.read_bytes()), "mode": "100644"}
    return records, provenance


def record_upstream_fixtures(root):
    """Add a provenance extension; never rewrite the original baseline or sources."""
    manifest = json.loads((root / MANIFEST).read_text())
    validate_history(manifest, selection())
    records, provenance = upstream_fixture_inventory(root)
    if all(manifest["files"].get(name) == info for name, info in records.items()):
        print("Public upstream fixture origin is already recorded")
        return
    if any(name in manifest["files"] for name in records):
        raise ValueError("fixture origin addition overlaps existing source inventory")
    addition = {
        "repository": provenance["repository"], "base": provenance["base"],
        "provenance": selection().FIXTURE_PROVENANCE, "files": records,
    }
    manifest.setdefault("upstream_additions", []).append(addition)
    manifest["files"].update(records)
    (root / MANIFEST).write_bytes(encode_json(manifest))
    print(f"Recorded public upstream fixture origin: {len(records) - 1} inputs; original baseline unchanged")


def validate_history(manifest, initial):
    if manifest["format"] != 1 or manifest["upstream"] != {"repository": initial.UPSTREAM, "base": initial.BASE}:
        raise ValueError("unexpected source format/upstream identity")
    if manifest["qualified_source_hashes"] != initial.QUALIFIED:
        raise ValueError("qualified source identity differs from reviewed selection")
    baseline = manifest["baseline_files"]
    if digest(encode_json(baseline)) != manifest["baseline_inventory_sha256"]:
        raise ValueError("baseline inventory digest mismatch")
    for name, expected in initial.QUALIFIED.items():
        if baseline.get(name, {}).get("sha256") != expected:
            raise ValueError(f"baseline qualified source mismatch: {name}")
    expected_patches = [
        {"path": f"patches/{i:02}-{name}.patch", "sha256": sha}
        for i, (name, sha) in enumerate(initial.PATCHES, 1)
    ]
    if manifest["patches"] != expected_patches:
        raise ValueError("ordered patch identity differs from reviewed selection")
    for patch in expected_patches:
        if baseline.get(patch["path"], {}).get("sha256") != patch["sha256"]:
            raise ValueError(f"baseline original patch mismatch: {patch['path']}")
    notices = {name: info for name, info in baseline.items()
               if name != "LICENSES.md" and PurePosixPath(name).name.startswith("LICENSE")}
    if manifest["original_notices"] != notices:
        raise ValueError("original notice inventory differs from baseline")
    current = dict(baseline)
    for addition in manifest.get("upstream_additions", []):
        if (addition["repository"] != initial.UPSTREAM or addition["base"] != initial.BASE
                or addition["provenance"] != initial.FIXTURE_PROVENANCE):
            raise ValueError("unexpected additive upstream fixture origin")
        if addition["provenance"] not in addition["files"]:
            raise ValueError("upstream fixture origin does not inventory its provenance file")
        for name, info in addition["files"].items():
            if name in current or not name.startswith(initial.FIXTURE_ROOT + "/"):
                raise ValueError("upstream fixture addition overlaps/escapes its source scope")
            current[name] = info
    for index, change in enumerate(manifest["local_changes"], 1):
        if not change["reason"].strip() or not change["changes"]:
            raise ValueError(f"empty downstream change record: {index}")
        for name, versions in change["changes"].items():
            if current.get(name) != versions["before"]:
                raise ValueError(f"downstream change chain mismatch: {name}")
            if versions["after"] is None:
                current.pop(name, None)
            else:
                current[name] = versions["after"]
    if current != manifest["files"]:
        raise ValueError("current inventory has no matching explicit change history")


def validate_notices(root, manifest, actual):
    for name, expected in manifest["original_notices"].items():
        if actual.get(name) != expected or not (root / name).is_file():
            raise ValueError(f"original license notice missing/modified: {name}")
    for package in manifest["packages"].values():
        path = root / package["path"]
        parsed = tomllib.loads((path / "Cargo.toml").read_text())["package"]
        if parsed["version"] != package["version"] or parsed["license"] != package["license"]:
            raise ValueError(f"package version/license identity changed: {package['path']}")
        if not any(PurePosixPath(name).parent.as_posix() == package["path"] for name in manifest["original_notices"]):
            raise ValueError(f"package has no original license notice: {package['path']}")


def validate_manifests(root, manifest):
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
    if set(workspace["members"]) != {package["path"] for package in manifest["packages"].values()}:
        raise ValueError("vendor workspace members differ from source closure")
    for package in manifest["packages"].values():
        directory = root / package["path"]
        parsed = tomllib.loads((directory / "Cargo.toml").read_text())
        for group in [parsed, *parsed.get("target", {}).values()]:
            for section in ("dependencies", "dev-dependencies", "build-dependencies"):
                for dep in group.get(section, {}).values():
                    if isinstance(dep, dict) and "path" in dep:
                        target = directory / dep["path"] / "Cargo.toml"
                        if not target.is_file() or not target.resolve().is_relative_to(root.resolve()):
                            raise ValueError(f"missing/escaping manifest dependency: {package['path']}")
        for kind in ("bin", "bench", "test", "example"):
            for target in parsed.get(kind, []):
                if "path" in target and not (directory / target["path"]).is_file():
                    raise ValueError(f"missing explicit {kind} target: {package['path']}/{target['path']}")


def validate_cargo(project, root, manifest):
    cargo = tomllib.loads((project / "Cargo.toml").read_text())
    patches = cargo["patch"]["crates-io"]
    for name, package in manifest["packages"].items():
        patch = patches.get(name, {})
        if "path" not in patch or (project / patch["path"]).resolve() != (root / package["path"]).resolve():
            raise ValueError(f"missing/inconsistent single-source Cargo patch: {name}")
    validate_lock(project / "Cargo.lock", manifest)


def validate_lock(path, manifest):
    lock = tomllib.loads(path.read_text())
    seen = set()
    for package in lock["package"]:
        name = package["name"]
        if name not in manifest["packages"]:
            if name == "gitoxide" or name.startswith("gix-"):
                raise ValueError(f"unmanifested Gitoxide package in Cargo.lock: {name}")
            continue
        if name in seen:
            raise ValueError(f"duplicate Gitoxide Cargo.lock instance: {name}")
        seen.add(name)
        if "source" in package or "checksum" in package:
            raise ValueError(f"registry/git source remains for patched package: {name}")
        if package["version"] != manifest["packages"][name]["version"]:
            raise ValueError(f"Cargo.lock package version differs: {name}")
    if "gix" not in seen:
        raise ValueError("Cargo.lock has no vendored gix instance")


def audit(root, project, vendor_only=False):
    manifest = json.loads((root / MANIFEST).read_text())
    validate_history(manifest, selection())
    if (root / selection().FIXTURE_PROVENANCE).exists():
        fixture_records, _ = upstream_fixture_inventory(root)
        for addition in manifest.get("upstream_additions", []):
            if addition["files"] != fixture_records:
                raise ValueError("recorded upstream fixture origin differs from actual pinned Git trees")
    actual = inventory(root)
    validate_notices(root, manifest, actual)
    if actual != manifest["files"]:
        missing = sorted(set(manifest["files"]) - set(actual))
        extra = sorted(set(actual) - set(manifest["files"]))
        changed = sorted(name for name in set(actual) & set(manifest["files"]) if actual[name] != manifest["files"][name])
        raise ValueError(f"source inventory mismatch: missing={missing[:5]}, extra={extra[:5]}, modified={changed[:5]}")
    validate_manifests(root, manifest)
    if (root / "Cargo.lock").exists():
        validate_lock(root / "Cargo.lock", manifest)
    if not vendor_only:
        validate_cargo(project, root, manifest)
    return manifest


def record_changes(root, reason):
    manifest = json.loads((root / MANIFEST).read_text())
    validate_history(manifest, selection())
    actual = inventory(root)
    validate_notices(root, manifest, actual)
    changes = {
        name: {"before": manifest["files"].get(name), "after": actual.get(name)}
        for name in sorted(set(actual) | set(manifest["files"]))
        if actual.get(name) != manifest["files"].get(name)
    }
    if not reason.strip() or not changes:
        raise ValueError("recording requires a nonempty reason and actual source changes")
    # Original generic patches and notices are immutable provenance, not an
    # escape hatch for accepting arbitrary new upstream input.
    originals = {patch["path"] for patch in manifest["patches"]}
    if any(name in originals or name in manifest["original_notices"] for name in changes):
        raise ValueError("cannot rewrite original patches/notices as downstream changes")
    if any(name.startswith("patches/") and not name.startswith("patches/local/") for name in changes):
        raise ValueError("new downstream patch files belong under patches/local/")
    validate_manifests(root, manifest)
    manifest["local_changes"].append({"reason": reason, "changes": changes})
    manifest["files"] = actual
    (root / MANIFEST).write_bytes(encode_json(manifest))
    print(f"Recorded explicit downstream changes: {len(changes)} paths")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--vendor-root", type=Path)
    parser.add_argument("--vendor-only", action="store_true", help="initial snapshot check before application workspace wiring")
    parser.add_argument("--record-local-changes", metavar="REASON")
    parser.add_argument("--record-upstream-fixtures", action="store_true",
                        help="record an additive pinned fixture origin, retaining the original baseline")
    parser.add_argument("--check-upstream-fixtures", action="store_true",
                        help="read-only artifact closure audit, independent of pending source changes")
    args = parser.parse_args()
    root = args.vendor_root or args.project_root / "vendor/gitoxide"
    if args.check_upstream_fixtures:
        if args.record_upstream_fixtures or args.record_local_changes is not None:
            raise ValueError("fixture-only verification cannot record source changes")
        _, provenance = upstream_fixture_inventory(root)
        print(f"Public upstream fixture closure verified: {len(provenance['files'])} inputs, "
              f"{sum(t['bytes'] for t in provenance['targets'].values())} bytes, three pinned Git trees")
        return
    if args.record_upstream_fixtures:
        record_upstream_fixtures(root)
    if args.record_local_changes is not None:
        record_changes(root, args.record_local_changes)
    manifest = audit(root, args.project_root, args.vendor_only)
    print(f"Core source audit passed: {len(manifest['files'])} files, {len(manifest['packages'])} packages; "
          + ("vendor-only (root Cargo.lock not checked)" if args.vendor_only else "single Cargo source/lock"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as error:
        print(f"core source audit failed: {error}", file=sys.stderr)
        sys.exit(1)
