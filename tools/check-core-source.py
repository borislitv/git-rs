#!/usr/bin/env python3
"""Read-only audit of the pinned owned Git dependency, lock and optional checkout."""
import argparse
import json
import os
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
ZSTD_REPOSITORY = "https://github.com/borislitv/zstd-rs"
ZSTD_REV = "66f4b27ef27742d3df45826bc85c4ed423483cf0"
ZSTD_NATIVE_REV = "f8745da6ff1ad1e7bab384bd1f9d742439278e99"
ZSTD_PACKAGES = {"zstd": "0.14.3", "zstd-safe": "8.2.0", "zstd-sys": "2.1.2+zstd.1.5.7"}
ZSTD_LICENSES = {"zstd": "BSD-3-Clause", "zstd-safe": "BSD-3-Clause",
                 "zstd-sys": "BSD-3-Clause AND BSD-2-Clause"}
ZSTD_FEATURES = {"zstd": ["experimental", "vendored"],
                 "zstd-safe": ["alloc", "experimental", "std", "vendored"],
                 "zstd-sys": ["experimental", "std", "vendored"]}
ZSTD_NOTICES = ["LICENSE", "zstd-safe/LICENSE", "zstd-safe/zstd-sys/LICENSE",
                "zstd-safe/zstd-sys/LICENSE.BSD-2-Clause", "zstd-safe/zstd-sys/LICENSE.BSD-3-Clause"]


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True,
                          capture_output=True, text=True, timeout=20).stdout.strip()


def audit_zstd(root, manifests, lock, dependency=None, metadata=None):
    if "ZSTD_SYS_USE_PKG_CONFIG" in os.environ:
        raise ValueError("ZSTD_SYS_USE_PKG_CONFIG must be absent, even when set to 0")
    provenance = json.loads((root / "docs/zstd-source.json").read_text())
    native = provenance["native"]
    if (provenance["source"] != {"git": ZSTD_REPOSITORY, "rev": ZSTD_REV}
            or provenance["packages"] != ZSTD_PACKAGES
            or provenance["licenses"] != ZSTD_LICENSES
            or provenance["notices_in_source"] != ZSTD_NOTICES
            or provenance["fork_provenance_in_source"] != "zstd-safe/FORK_PROVENANCE.md"
            or provenance["upstream"] != {"git": "https://github.com/gyscos/zstd-rs",
                                          "rev": "fa82b4c286e9a95e6681e59df5d0892ed89e75b9"}
            or provenance["default_features"] is not False
            or provenance["features"] != ZSTD_FEATURES["zstd"]
            or provenance["resolved_features"] != ZSTD_FEATURES
            or provenance["forbidden_environment"] != ["ZSTD_SYS_USE_PKG_CONFIG"]):
        raise ValueError("zstd provenance requires the qualified owned pin, features and licenses")
    if native != {"git": "https://github.com/facebook/zstd", "rev": ZSTD_NATIVE_REV,
                  "version": "1.5.7", "path": "zstd-safe/zstd-sys/zstd",
                  "license": "BSD-3-Clause OR GPL-2.0-only", "selected_license": "BSD-3-Clause",
                  "notices_in_source": ["LICENSE", "COPYING"]}:
        raise ValueError("zstd native provenance differs")
    required = {"git": ZSTD_REPOSITORY, "rev": ZSTD_REV, "default-features": False,
                "features": ZSTD_FEATURES["zstd"]}
    direct = []
    for index, manifest in enumerate(manifests):
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            for name, spec in manifest.get(section, {}).items():
                if name in ZSTD_PACKAGES or (isinstance(spec, dict) and spec.get("package") in ZSTD_PACKAGES):
                    if index != 0 or section != "dependencies" or name != "zstd" or spec != required:
                        raise ValueError("zstd requires one normal root Git dependency with exact minimal features")
                    direct.append(name)
    if direct != ["zstd"]:
        raise ValueError("zstd requires one normal root Git dependency with exact minimal features")
    source = f"git+{ZSTD_REPOSITORY}?rev={ZSTD_REV}#{ZSTD_REV}"
    packages = [p for p in lock["package"] if p["name"] in ZSTD_PACKAGES]
    if (len(packages) != 3 or {p["name"]: p["version"] for p in packages} != ZSTD_PACKAGES
            or any(p.get("source") != source for p in packages)):
        raise ValueError("zstd requires three unique same-revision owned Git lock packages")
    if metadata is not None:
        graph = json.loads(metadata.read_text())
        ids = {p["id"]: p["name"] for p in graph["packages"] if p["name"] in ZSTD_PACKAGES}
        resolved = {ids[n["id"]]: sorted(n["features"]) for n in graph["resolve"]["nodes"] if n["id"] in ids}
        if (len(ids) != 3 or resolved != ZSTD_FEATURES
                or any(p.get("source") != source or p["version"] != ZSTD_PACKAGES[p["name"]]
                       for p in graph["packages"] if p["id"] in ids)):
            raise ValueError("zstd resolved Cargo source/features differ from the minimal profile")
    if dependency is not None:
        if (git(dependency, "rev-parse", "HEAD") != ZSTD_REV
                or git(dependency, "remote", "get-url", "origin").removesuffix(".git") != ZSTD_REPOSITORY
                or git(dependency, "status", "--porcelain", "--untracked-files=no")):
            raise ValueError("external zstd checkout is not the clean qualified owned revision")
        paths = {"zstd": "Cargo.toml", "zstd-safe": "zstd-safe/Cargo.toml",
                 "zstd-sys": "zstd-safe/zstd-sys/Cargo.toml"}
        for name, path in paths.items():
            package = tomllib.loads((dependency / path).read_text())["package"]
            if package["version"] != ZSTD_PACKAGES[name] or package["license"] != ZSTD_LICENSES[name]:
                raise ValueError(f"{name}: external zstd package identity/license differs")
        native_root = dependency / native["path"]
        if (git(dependency, "ls-tree", "HEAD", native["path"]) !=
                f"160000 commit {ZSTD_NATIVE_REV}\t{native['path']}"
                or git(native_root, "rev-parse", "HEAD") != ZSTD_NATIVE_REV
                or git(native_root, "status", "--porcelain", "--untracked-files=no")
                or git(dependency, "config", "--file", ".gitmodules", "--get",
                       "submodule.zstd-safe/zstd-sys/zstd.url") != native["git"]):
            raise ValueError("external zstd native submodule is not the clean pinned gitlink")
        notices = [dependency / p for p in provenance["notices_in_source"]]
        notices += [native_root / p for p in native["notices_in_source"]]
        notices.append(dependency / provenance["fork_provenance_in_source"])
        if any(not path.is_file() for path in notices):
            raise ValueError("external zstd license/provenance notice missing")


def audit(root, dependency=None, codec_dependency=None, zstd_dependency=None, metadata=None):
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
    audit_zstd(root, manifests, lock, zstd_dependency, metadata)
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
          + f"; codec {CODEC_REPOSITORY}@{CODEC_REV}"
          + f"; zstd {ZSTD_REPOSITORY}@{ZSTD_REV} (3 packages)"
          + ("; resolved zstd features verified" if metadata else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--dependency-root", type=Path)
    parser.add_argument("--codec-root", type=Path)
    parser.add_argument("--zstd-root", type=Path)
    parser.add_argument("--cargo-metadata", type=Path,
                        help="cargo metadata --locked --format-version 1 output; verifies feature unification")
    args = parser.parse_args()
    audit(args.project_root, args.dependency_root, args.codec_root, args.zstd_root, args.cargo_metadata)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"core source audit failed: {error}", file=sys.stderr)
        sys.exit(1)
