#!/usr/bin/env python3
"""Read-only audit of the pinned owned Git dependency, lock and optional checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "https://github.com/borislitv/gitoxide"
GITOXIDE_REV = "512750f176546e7064b05616be8805caac92d260"
GITOXIDE_CONTROLLED_SOURCES = {
    "gix-object/src/tree/ref_iter.rs",
    "gix-pack/src/bundle/write/admitted.rs", "gix-pack/src/bundle/write/controlled.rs",
    "gix-pack/src/bundle/write/mod.rs", "gix-pack/src/data/delta.rs",
    "gix-pack/src/data/entry/decode.rs", "gix-pack/src/data/file/decode/entry.rs",
    "gix-pack/src/index/encode.rs", "gix-pack/src/index/init.rs", "gix-pack/src/index/write/mod.rs",
    "gix-pack/src/lib.rs", "gix-pack/src/memory.rs", "gix-revwalk/src/queue.rs",
    "gix-traverse/src/commit/mod.rs", "gix-traverse/src/commit/topo/admission.rs",
    "gix-traverse/src/commit/topo/init.rs", "gix-traverse/src/commit/topo/iter.rs",
    "gix-traverse/src/commit/topo/mod.rs", "gix-utils/src/decode.rs",
    "gix-url/src/fsck.rs", "gix-url/src/lib.rs", "gix-validate/src/path.rs", "gix-zlib/src/lib.rs",
}
CODEC_REPOSITORY = "https://github.com/borislitv/iconv-native-rs"
CODEC_REV = "f8022d89dfe9e9fd2a6e3ce0bac5a68a4c77e89f"
KUBERT_REPOSITORY = "https://github.com/borislitv/kubert"
KUBERT_REV = "5ebc70e236e14852c82ace1dc772e3c48c1839f7"
KUBERT_FEATURES = ["lease", "openssl-tls"]
ZSTD_REPOSITORY = "https://github.com/borislitv/zstd-rs"
ZSTD_REV = "a6adf4a49ab3b43e2b0279a4257f79dcc164478f"
ZSTD_NATIVE_REV = "f8745da6ff1ad1e7bab384bd1f9d742439278e99"
ZSTD_PACKAGES = {"zstd": "0.14.3", "zstd-safe": "8.2.0", "zstd-sys": "2.1.2+zstd.1.5.7"}
ZSTD_LICENSES = {"zstd": "BSD-3-Clause", "zstd-safe": "BSD-3-Clause",
                 "zstd-sys": "BSD-3-Clause AND BSD-2-Clause"}
ZSTD_FEATURES = {"zstd": ["experimental", "vendored"],
                 "zstd-safe": ["alloc", "experimental", "std", "vendored"],
                 "zstd-sys": ["experimental", "std", "vendored"]}
ZSTD_NOTICES = ["LICENSE", "zstd-safe/LICENSE", "zstd-safe/zstd-sys/LICENSE",
                "zstd-safe/zstd-sys/LICENSE.BSD-2-Clause", "zstd-safe/zstd-sys/LICENSE.BSD-3-Clause"]
RECEIVE_DEPENDENCIES = {
    "gix-config": {"version": "=0.62.1"},
    "gix-pack": {"version": "=0.77.0", "features": ["generate", "streaming-input"]},
    "gix-packetline": {"version": "=0.24.1", "features": ["blocking-io"]},
    "gix-validate": {"version": "=0.13.1"},
    "gix-url": {"version": "=0.40.1"},
    "gix-traverse": {"version": "=0.64.0"},
}
REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
GZIP_PACKAGES = {
    "flate2": {"version": "1.1.10", "license": "MIT OR Apache-2.0",
               "checksum": "6e634e2e0ebac1ee034020da1ca582e17ffe4e0f5e985823721e168928136dcb"},
    "zlib-rs": {"version": "0.6.8", "license": "Zlib",
                "checksum": "b268e58e7c693d7c271f93ffc4ba3b380412554231c85bf61ca7af91042a4112"},
}
GZIP_FEATURES = {"flate2": ["any_impl", "any_zlib", "zlib-rs"],
                 "zlib-rs": ["c-allocator", "default", "rust-allocator", "std"]}
RUSTIX_CHECKSUM = "e5e76742e5294153d4dde1207dbec5bdd81eef6fabdc0d2118babadabfd858aa"
RUSTIX_LICENSE = "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT"
RUSTIX_FEATURES = ["std", "event", "fs", "net", "pipe"]
RUSTIX_RESOLVED_FEATURES = ["alloc", "default", "event", "fs", "net", "pipe", "std"]


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


def audit_gzip(root, manifests, lock, metadata=None, registry=None):
    provenance = json.loads((root / "docs/gzip-source.json").read_text())
    if (provenance["source"] != REGISTRY_SOURCE or provenance["default_features"] is not False
            or provenance["features"] != ["zlib-rs"] or provenance["resolved_features"] != GZIP_FEATURES):
        raise ValueError("gzip requires the qualified registry/backend feature profile")
    if manifests[0].get("dependencies", {}).get("flate2") != {
            "version": "=1.1.10", "default-features": False, "features": ["zlib-rs"]}:
        raise ValueError("gzip requires the exact root flate2 zlib-rs dependency without defaults")
    for name, expected in GZIP_PACKAGES.items():
        declared = provenance["packages"][name]
        if any(declared[key] != value for key, value in expected.items()):
            raise ValueError(f"{name}: gzip provenance identity/license differs")
        packages = [p for p in lock["package"] if p["name"] == name]
        if (len(packages) != 1 or packages[0].get("source") != REGISTRY_SOURCE
                or packages[0]["version"] != expected["version"]
                or packages[0].get("checksum") != expected["checksum"]):
            raise ValueError(f"{name}: gzip registry lock identity differs")
        if registry is not None:
            path = registry / f"{name}-{expected['version']}"
            package = tomllib.loads((path / "Cargo.toml").read_text())["package"]
            if (package["version"] != expected["version"] or package["license"] != expected["license"]
                    or package["repository"] != declared["upstream"]
                    or any(not (path / notice).is_file() for notice in declared["notices_in_source"])):
                raise ValueError(f"{name}: external gzip manifest/license/notice differs")
    if metadata is not None:
        graph = json.loads(metadata.read_text())
        packages = [p for p in graph["packages"] if p["name"] in GZIP_PACKAGES]
        ids = {p["id"]: p["name"] for p in packages}
        features = {ids[n["id"]]: sorted(n["features"]) for n in graph["resolve"]["nodes"] if n["id"] in ids}
        if (len(packages) != 2 or features != GZIP_FEATURES
                or any(p.get("source") != REGISTRY_SOURCE or p["version"] != GZIP_PACKAGES[p["name"]]["version"]
                       or p["license"] != GZIP_PACKAGES[p["name"]]["license"] for p in packages)):
            raise ValueError("gzip resolved Cargo source/version/license/backend features differ")


def audit_rustix(root, manifests, lock, metadata=None, dependency=None):
    provenance = json.loads((root / "docs/rustix-source.json").read_text())
    expected = {"source": REGISTRY_SOURCE, "version": "1.1.6", "checksum": RUSTIX_CHECKSUM,
                "license": RUSTIX_LICENSE, "upstream": "https://github.com/bytecodealliance/rustix",
                "notices_in_source": ["LICENSE-APACHE", "LICENSE-Apache-2.0_WITH_LLVM-exception", "LICENSE-MIT", "COPYRIGHT"],
                "default_features": False, "features": RUSTIX_FEATURES,
                "resolved_features": RUSTIX_RESOLVED_FEATURES}
    if provenance != expected:
        raise ValueError("rustix requires the qualified registry/license/feature provenance")
    if manifests[0].get("dependencies", {}).get("rustix") != {
            "version": "=1.1.6", "default-features": False, "features": RUSTIX_FEATURES}:
        raise ValueError("rustix requires the exact root I/O dependency without defaults")
    packages = [p for p in lock["package"] if p["name"] == "rustix"]
    if (len(packages) != 1 or packages[0]["version"] != "1.1.6"
            or packages[0].get("source") != REGISTRY_SOURCE or packages[0].get("checksum") != RUSTIX_CHECKSUM):
        raise ValueError("rustix registry lock identity differs")
    if metadata is not None:
        graph = json.loads(metadata.read_text())
        packages = [p for p in graph["packages"] if p["name"] == "rustix"]
        ids = {p["id"] for p in packages}
        features = [sorted(n["features"]) for n in graph["resolve"]["nodes"] if n["id"] in ids]
        if (len(packages) != 1 or packages[0]["version"] != "1.1.6"
                or packages[0].get("source") != REGISTRY_SOURCE or packages[0]["license"] != RUSTIX_LICENSE
                or features != [RUSTIX_RESOLVED_FEATURES]):
            raise ValueError("rustix resolved Cargo source/license/I/O features differ")
    if dependency is not None:
        manifest = tomllib.loads((dependency / "Cargo.toml").read_text())
        package = manifest["package"]
        if (package["name"] != "rustix" or package["version"] != "1.1.6"
                or package["license"] != RUSTIX_LICENSE or package["repository"] != expected["upstream"]
                or not set(RUSTIX_FEATURES) <= manifest["features"].keys()
                or any(not (dependency / notice).is_file() for notice in expected["notices_in_source"])):
            raise ValueError("external rustix version/license/features/notices differ")


def audit_kubert(root, manifests, lock, metadata=None, dependency=None):
    provenance = json.loads((root / "docs/kubert-source.json").read_text())
    source = {"git": KUBERT_REPOSITORY, "rev": KUBERT_REV}
    identity = {"name": "kubert", "version": "0.25.0", "license": "Apache-2.0"}
    paths = {"kubert/src/lease.rs", "kubert/src/admin/diagnostics/lease.rs", "kubert/Cargo.toml"}
    hashes = provenance["controlled_source_sha256"]
    if (provenance["format"] != 1 or provenance["source"] != source or provenance["package"] != identity
            or provenance["upstream"] != {"git": "https://github.com/olix0r/kubert", "rev": "a2ff424f072a0de77d98ab97592d6f66ae1d9546"}
            or provenance["default_features"] is not False or provenance["features"] != KUBERT_FEATURES
            or provenance["notices_in_source"] != ["LICENSE", "FORK_PROVENANCE.md"]
            or set(hashes) != paths or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes.values())):
        raise ValueError("kubert requires the selected owned source, controlled hashes, minimal features and license provenance")
    required = {**source, "version": "=0.25.0", "default-features": False, "features": KUBERT_FEATURES}
    direct = []
    for index, manifest in enumerate(manifests):
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            for name, spec in manifest.get(section, {}).items():
                if name == "kubert" or (isinstance(spec, dict) and spec.get("package") == "kubert"):
                    if index != 0 or section != "dependencies" or name != "kubert" or spec != required:
                        raise ValueError("kubert requires one normal root owned Git dependency with minimal Lease/TLS features")
                    direct.append(name)
    if direct != ["kubert"]:
        raise ValueError("kubert requires one normal root owned Git dependency with minimal Lease/TLS features")
    expected = f"git+{KUBERT_REPOSITORY}?rev={KUBERT_REV}#{KUBERT_REV}"
    packages = [package for package in lock["package"] if package["name"] == "kubert"]
    if len(packages) != 1 or packages[0]["version"] != "0.25.0" or packages[0].get("source") != expected:
        raise ValueError("kubert requires one unique selected owned Git lock identity")
    if metadata is not None:
        graph = json.loads(metadata.read_text())
        packages = [package for package in graph["packages"] if package["name"] == "kubert"]
        ids = {package["id"] for package in packages}
        features = [sorted(node["features"]) for node in graph["resolve"]["nodes"] if node["id"] in ids]
        if (len(packages) != 1 or packages[0]["version"] != "0.25.0" or packages[0].get("source") != expected
                or packages[0]["license"] != "Apache-2.0" or features != [KUBERT_FEATURES]):
            raise ValueError("kubert resolved Cargo source/license/Lease features differ")
    if dependency is not None:
        if (git(dependency, "rev-parse", "HEAD") != KUBERT_REV
                or git(dependency, "remote", "get-url", "origin").removesuffix(".git") != KUBERT_REPOSITORY
                or git(dependency, "status", "--porcelain", "--untracked-files=all")):
            raise ValueError("external kubert checkout is not the clean selected owned revision")
        package = tomllib.loads((dependency / "kubert/Cargo.toml").read_text())["package"]
        if any(package[key] != value for key, value in identity.items()):
            raise ValueError("external kubert version/license differs")
        if any(not (dependency / path).is_file() for path in provenance["notices_in_source"]):
            raise ValueError("external kubert license/provenance notice missing")
        for path, digest in hashes.items():
            if hashlib.sha256((dependency / path).read_bytes()).hexdigest() != digest:
                raise ValueError(f"{path}: controlled Lease source hash differs")


def audit(root, dependency=None, codec_dependency=None, zstd_dependency=None, metadata=None, gzip_registry=None,
          rustix_dependency=None, kubert_dependency=None):
    provenance = json.loads((root / "docs/gitoxide-source.json").read_text())
    source = provenance["source"]
    rev = source["rev"]
    if source["git"] != REPOSITORY or not re.fullmatch(r"[0-9a-f]{40}", rev):
        raise ValueError("Gitoxide requires the owned repository and a full immutable revision")
    if rev != GITOXIDE_REV:
        raise ValueError("Gitoxide requires the selected controlled mapping, range and traversal revision")
    if provenance["licenses"] != {"default_gitoxide": "MIT OR Apache-2.0", "gix-imara-diff": "Apache-2.0"}:
        raise ValueError("Gitoxide license identity differs")
    if (provenance.get("fork_provenance_in_source") != "FORK_PROVENANCE.md"
            or provenance["notices_in_source"] != ["LICENSE-MIT", "LICENSE-APACHE", "gix-imara-diff/LICENSE", "FORK_PROVENANCE.md"]):
        raise ValueError("Gitoxide requires its original license notices and controlled fork provenance")
    hashes = provenance["controlled_source_sha256"]
    if (set(hashes) != GITOXIDE_CONTROLLED_SOURCES
            or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes.values())):
        raise ValueError("Gitoxide requires controlled submodule URL source hashes and the exact pack/traversal source inventory")
    expected = f"git+{REPOSITORY}?rev={rev}#{rev}"
    codec = json.loads((root / "docs/codec-source.json").read_text())
    if codec["source"] != {"git": CODEC_REPOSITORY, "rev": CODEC_REV} or codec["license"] != "MIT OR Apache-2.0":
        raise ValueError("codec requires the qualified immutable owned pin and original license")
    codec_source = f"git+{CODEC_REPOSITORY}?rev={CODEC_REV}#{CODEC_REV}"
    manifests = [tomllib.loads((root / path).read_text()) for path in
                  ("Cargo.toml", "crates/git-core/Cargo.toml")]
    for name, profile in RECEIVE_DEPENDENCIES.items():
        required = {"git": REPOSITORY, "rev": rev, "default-features": False, **profile}
        if manifests[1].get("dependencies", {}).get(name) != required:
            raise ValueError(f"{name}: receive requires the exact normal owned dependency/profile")
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
    if manifests[0].get("dependencies", {}).get("gix-packetline") != {
            "git": REPOSITORY, "rev": rev, "default-features": False, **RECEIVE_DEPENDENCIES["gix-packetline"]}:
        raise ValueError("receive requires the exact root packetline blocking-io dependency")
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    audit_zstd(root, manifests, lock, zstd_dependency, metadata)
    audit_gzip(root, manifests, lock, metadata, gzip_registry)
    audit_rustix(root, manifests, lock, metadata, rustix_dependency)
    codecs = [p for p in lock["package"] if p["name"] == "iconv-native"]
    if codec_direct != 1 or len(codecs) != 1 or codecs[0].get("source") != codec_source or codecs[0]["version"] != "0.1.0":
        raise ValueError("missing, duplicate or inconsistent codec Git lock identity")
    audit_kubert(root, manifests, lock, metadata, kubert_dependency)
    packages = [p for p in lock["package"] if p["name"] == "gix" or p["name"].startswith("gix-")]
    names = [p["name"] for p in packages]
    if not direct or not direct <= set(names) or len(names) != len(set(names)):
        raise ValueError("missing or duplicate Gitoxide lock packages")
    if any(p.get("source") != expected for p in packages):
        raise ValueError("registry, mixed revision or alternate Gitoxide source in Cargo.lock")
    imara = next((p for p in packages if p["name"] == "gix-imara-diff"), None)
    if imara is None or [edge for edge in imara.get("dependencies", [])
                         if edge.split()[0] == "hashbrown"] != ["hashbrown 0.16.1"]:
        raise ValueError("gix-imara-diff requires its retained hashbrown 0.16.1 lock edge")
    # The digest covers compact sorted-key JSON records, sorted by package name.
    closure = sorted(({"name": p["name"], "version": p["version"],
                       "license": provenance["licenses"].get(p["name"], provenance["licenses"]["default_gitoxide"])}
                      for p in packages), key=lambda p: p["name"])
    closure_hash = hashlib.sha256(json.dumps(closure, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if len(packages) != provenance["resolved_package_count"] or closure_hash != provenance["resolved_packages_sha256"]:
        raise ValueError("Gitoxide resolved package/version/license closure differs")
    if metadata is not None:
        graph = json.loads(metadata.read_text())
        resolved = [p for p in graph["packages"] if p["name"] == "gix" or p["name"].startswith("gix-")]
        ids = {p["id"]: p["name"] for p in resolved}
        if (len(resolved) != len(packages)
                or sorted((p["name"], p["version"], p.get("source")) for p in resolved) !=
                   sorted((p["name"], p["version"], p.get("source")) for p in packages)
                or any(p["license"] != provenance["licenses"].get(p["name"], provenance["licenses"]["default_gitoxide"])
                       for p in resolved)):
            raise ValueError("Gitoxide resolved Cargo source/version/license graph differs")
        features = {ids[n["id"]]: n["features"] for n in graph["resolve"]["nodes"] if n["id"] in ids}
        for name, profile in RECEIVE_DEPENDENCIES.items():
            if not set(profile.get("features", [])) <= set(features.get(name, [])):
                raise ValueError(f"{name}: receive features missing from resolved Cargo graph")
    if (root / "vendor").exists():
        raise ValueError("copied dependency source must not remain in the application")
    if dependency is not None:
        if git(dependency, "rev-parse", "HEAD") != rev:
            raise ValueError("external Gitoxide checkout is not the pinned revision")
        if git(dependency, "remote", "get-url", "origin").removesuffix(".git") != REPOSITORY:
            raise ValueError("external Gitoxide checkout is not from the owned repository")
        if git(dependency, "status", "--porcelain", "--untracked-files=all"):
            raise ValueError("external Gitoxide checkout has modified or untracked source files")
        for notice in provenance["notices_in_source"]:
            if not (dependency / notice).is_file():
                raise ValueError(f"external source license notice missing: {notice}")
        for package in closure:
            manifest = tomllib.loads((dependency / package["name"] / "Cargo.toml").read_text())["package"]
            if manifest["license"] != package["license"] or manifest["version"] != package["version"]:
                raise ValueError(f"{package['name']}: external version/license differs")
        for path, digest in provenance["controlled_source_sha256"].items():
            if hashlib.sha256((dependency / path).read_bytes()).hexdigest() != digest:
                raise ValueError(f"{path}: controlled Git source hash differs")
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
          + "; gzip flate2 1.1.10 / zlib-rs 0.6.8"
           + "; rustix 1.1.6 I/O profile"
          + f"; kubert {KUBERT_REPOSITORY}@{KUBERT_REV} Lease/TLS profile"
          + ("; resolved zstd features verified" if metadata else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--dependency-root", type=Path)
    parser.add_argument("--codec-root", type=Path)
    parser.add_argument("--zstd-root", type=Path)
    parser.add_argument("--gzip-registry-root", type=Path,
                        help="registry source directory containing flate2-1.1.10 and zlib-rs-0.6.8")
    parser.add_argument("--rustix-root", type=Path)
    parser.add_argument("--kubert-root", type=Path)
    parser.add_argument("--cargo-metadata", type=Path,
                        help="cargo metadata --locked --format-version 1 output; verifies feature unification")
    args = parser.parse_args()
    audit(args.project_root, args.dependency_root, args.codec_root, args.zstd_root, args.cargo_metadata,
          args.gzip_registry_root, args.rustix_root, args.kubert_root)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"core source audit failed: {error}", file=sys.stderr)
        sys.exit(1)
