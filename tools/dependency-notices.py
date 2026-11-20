#!/usr/bin/env python3
"""Package public Cargo identities and available upstream license notices.

Input is the build's locked, offline Cargo metadata. Source paths remain local;
only package identities, features, license declarations and notice bytes ship.
The inventory includes the resolved build/test graph, not a runtime linkage claim.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
NOTICE_NAMES = (
    "LICENSE", "LICENSE.txt", "LICENSE.md", "LICENSE-MIT", "LICENSE-APACHE",
    "LICENSE-BSD", "LICENSE.BSD-2-Clause", "LICENSE.BSD-3-Clause", "COPYING",
    "COPYRIGHT", "NOTICE", "NOTICE.txt", "LICENSE-Apache-2.0_WITH_LLVM-exception",
)


def source_root(package):
    directory = Path(package["manifest_path"]).resolve().parent
    if not (package.get("source") or "").startswith("git+"):
        return directory
    for parent in (directory, *list(directory.parents)[:7]):
        if (parent / ".git").exists():
            return parent
    raise ValueError(f"Git source root unavailable for {package['name']}")


def notices(package, provenance):
    root = source_root(package)
    directory = Path(package["manifest_path"]).resolve().parent
    candidates = {parent / name for parent in (directory, root) for name in NOTICE_NAMES}
    if package.get("license_file"):
        candidates.add(directory / package["license_file"])
    for document in provenance:
        source = document.get("source")
        if not isinstance(source, dict) or not source.get("git"):
            continue
        prefix = "git+" + source["git"] + "?rev=" + source["rev"] + "#"
        if not (package.get("source") or "").startswith(prefix):
            continue
        candidates.update(root / name for name in document.get("notices_in_source", []))
        if document.get("fork_provenance_in_source"):
            candidates.add(root / document["fork_provenance_in_source"])
        native = document.get("native", {})
        candidates.update(root / native["path"] / name for name in native.get("notices_in_source", []))
    result = []
    for candidate in sorted(candidates):
        if not candidate.is_file():
            continue
        path = candidate.resolve()
        relative = path.relative_to(root)
        if path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError(f"License notice exceeds byte bound: {package['name']}/{relative}")
        result.append((relative, path.read_bytes()))
    return result


def write(metadata, destination):
    if metadata.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("Cargo metadata exceeds byte bound")
    graph = json.loads(metadata.read_text())
    provenance = [json.loads(path.read_text()) for path in sorted((ROOT / "docs").glob("*-source.json"))]
    features = {node["id"]: sorted(node["features"]) for node in graph["resolve"]["nodes"]}
    destination.mkdir(parents=True, exist_ok=False)
    packages = []
    for package in sorted(graph["packages"], key=lambda row: (row["name"], row["version"], row.get("source") or "")):
        name, version = package["name"], package["version"]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name) or not re.fullmatch(r"[A-Za-z0-9.+-]+", version):
            raise ValueError("Invalid Cargo package identity")
        source = package.get("source") or "workspace"
        identity = hashlib.sha256(f"{name}\0{version}\0{source}".encode()).hexdigest()[:16]
        record = {key: package.get(key) for key in ("name", "version", "license", "repository")}
        record.update(source=source, features=features.get(package["id"], []), notices=[])
        for relative, data in notices(package, provenance):
            target = Path("licenses") / f"{name}-{version}-{identity}" / relative
            path = destination / target
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            record["notices"].append({"path": target.as_posix(), "sha256": hashlib.sha256(data).hexdigest()})
        packages.append(record)
    manifest = {"format": 1, "scope": "locked Cargo metadata, including build/test dependencies",
                "packages": packages}
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (destination / "README.txt").write_text(
        "Package license declarations and available upstream notices are recorded in manifest.json.\n"
        "The graph includes build/test dependencies; it is not a binary linkage inventory.\n"
        "An absent license declaration or notice is recorded, not replaced with an invented license.\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    write(arguments.metadata, arguments.output)


if __name__ == "__main__":
    main()
