#!/usr/bin/env python3
"""Read-only audit of the pinned owned Git dependency, lock and optional checkout."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "https://github.com/borislitv/gitoxide"
GITOXIDE_REV = "7ce87f63e03ef116cb2d3df097cd688e6d19c35b"
CODEC_REPOSITORY = "https://github.com/borislitv/iconv-native-rs"
CODEC_REV = "f8022d89dfe9e9fd2a6e3ce0bac5a68a4c77e89f"


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True,
                          capture_output=True, text=True, timeout=20).stdout.strip()


def audit(root, dependency=None, codec_dependency=None):
    provenance = json.loads((root / "docs/gitoxide-source.json").read_text())
    source = provenance["source"]
    rev = source["rev"]
    if source["git"] != REPOSITORY or not re.fullmatch(r"[0-9a-f]{40}", rev):
        raise ValueError("Gitoxide requires the owned repository and a full immutable revision")
    if rev != GITOXIDE_REV:
        raise ValueError("Gitoxide requires the qualified completed-mapping revision")
    if provenance["licenses"] != {"default_gitoxide": "MIT OR Apache-2.0", "gix-imara-diff": "Apache-2.0"}:
        raise ValueError("Gitoxide license identity differs")
    expected = f"git+{REPOSITORY}?rev={rev}#{rev}"
    codec = json.loads((root / "docs/codec-source.json").read_text())
    if codec["source"] != {"git": CODEC_REPOSITORY, "rev": CODEC_REV} or codec["license"] != "MIT OR Apache-2.0":
        raise ValueError("codec requires the qualified immutable owned pin and original license")
    codec_source = f"git+{CODEC_REPOSITORY}?rev={CODEC_REV}#{CODEC_REV}"
    manifests = [tomllib.loads((root / path).read_text()) for path in
                 ("Cargo.toml", "crates/git-core/Cargo.toml")]
    direct = set()
    codec_direct = 0
    for manifest in manifests:
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            for name, spec in manifest.get(section, {}).items():
                if name == "iconv-native":
                    required = {"git": CODEC_REPOSITORY, "rev": CODEC_REV, "version": "=0.1.0",
                                "default-features": False, "features": ["libc-iconv"]}
                    if section != "dependencies" or spec != required:
                        raise ValueError("codec requires a normal direct Git dependency without fallback")
                    codec_direct += 1
                if name == "gix" or name.startswith("gix-"):
                    if not isinstance(spec, dict) or spec.get("git") != REPOSITORY or spec.get("rev") != rev:
                        raise ValueError(f"{name}: missing or mixed immutable Gitoxide pin")
                    if any(key in spec for key in ("path", "branch", "tag")):
                        raise ValueError(f"{name}: alternate Gitoxide source is forbidden")
                    direct.add(name)
        if "patch" in manifest or "replace" in manifest:
            raise ValueError("dependency overrides must not bypass the pinned source")
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    codecs = [p for p in lock["package"] if p["name"] == "iconv-native"]
    if codec_direct != 1 or len(codecs) != 1 or codecs[0].get("source") != codec_source or codecs[0]["version"] != "0.1.0":
        raise ValueError("missing, duplicate or inconsistent codec Git lock identity")
    packages = [p for p in lock["package"] if p["name"] == "gix" or p["name"].startswith("gix-")]
    names = [p["name"] for p in packages]
    if not direct or not direct <= set(names) or len(names) != len(set(names)):
        raise ValueError("missing or duplicate Gitoxide lock packages")
    if any(p.get("source") != expected for p in packages):
        raise ValueError("registry, mixed revision or alternate Gitoxide source in Cargo.lock")
    if (root / "vendor").exists():
        raise ValueError("copied dependency source must not remain in the application")
    if dependency is not None:
        if git(dependency, "rev-parse", "HEAD") != rev:
            raise ValueError("external Gitoxide checkout is not the pinned revision")
        if git(dependency, "remote", "get-url", "origin").removesuffix(".git") != REPOSITORY:
            raise ValueError("external Gitoxide checkout is not from the owned repository")
        if git(dependency, "status", "--porcelain", "--untracked-files=no"):
            raise ValueError("external Gitoxide checkout has modified tracked files")
        for notice in provenance["notices_in_source"]:
            if not (dependency / notice).is_file():
                raise ValueError(f"external source license notice missing: {notice}")
        for package in ("gix", "gix-imara-diff"):
            manifest = tomllib.loads((dependency / package / "Cargo.toml").read_text())
            expected_license = provenance["licenses"].get(package, provenance["licenses"]["default_gitoxide"])
            if manifest["package"]["license"] != expected_license:
                raise ValueError(f"{package}: external license differs")
        origin = provenance["imara_origin"]
        ledger = (dependency / origin["ledger_in_source"]).read_text()
        if origin["rev"] not in ledger or origin["git"] not in ledger:
            raise ValueError("imara source provenance missing")
    if codec_dependency is not None:
        if git(codec_dependency, "rev-parse", "HEAD") != CODEC_REV or git(codec_dependency, "remote", "get-url", "origin").removesuffix(".git") != CODEC_REPOSITORY:
            raise ValueError("external codec checkout is not the qualified owned revision")
        if git(codec_dependency, "status", "--porcelain", "--untracked-files=no"):
            raise ValueError("external codec checkout has modified tracked files")
        manifest = tomllib.loads((codec_dependency / "Cargo.toml").read_text())
        if manifest["package"]["license"] != codec["license"] or any(not (codec_dependency / notice).is_file() for notice in codec["notices_in_source"]):
            raise ValueError("external codec license/notice differs")
    print(f"Core source audit passed: {len(packages)} Gitoxide packages, {REPOSITORY}@{rev}"
          + ("; external checkout/licenses verified" if dependency else "")
          + f"; codec {CODEC_REPOSITORY}@{CODEC_REV}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--dependency-root", type=Path)
    parser.add_argument("--codec-root", type=Path)
    args = parser.parse_args()
    audit(args.project_root, args.dependency_root, args.codec_root)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"core source audit failed: {error}", file=sys.stderr)
        sys.exit(1)
