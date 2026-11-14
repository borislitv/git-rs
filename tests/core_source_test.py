#!/usr/bin/env python3
"""Fast CLI source-policy regressions; no Cargo, network or dependency checkout."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
REPO = "https://github.com/borislitv/gitoxide"
REV = "512750f176546e7064b05616be8805caac92d260"
SOURCE = f"git+{REPO}?rev={REV}#{REV}"
CODEC_REPO = "https://github.com/borislitv/iconv-native-rs"
CODEC_REV = "f8022d89dfe9e9fd2a6e3ce0bac5a68a4c77e89f"
CODEC_SOURCE = f"git+{CODEC_REPO}?rev={CODEC_REV}#{CODEC_REV}"
KUBERT_REPO = "https://github.com/borislitv/kubert"
KUBERT_REV = "5ebc70e236e14852c82ace1dc772e3c48c1839f7"
KUBERT_SOURCE = f"git+{KUBERT_REPO}?rev={KUBERT_REV}#{KUBERT_REV}"
ZSTD_REPO = "https://github.com/borislitv/zstd-rs"
ZSTD_REV = "a6adf4a49ab3b43e2b0279a4257f79dcc164478f"
ZSTD_SOURCE = f"git+{ZSTD_REPO}?rev={ZSTD_REV}#{ZSTD_REV}"
ZSTD_PACKAGES = {"zstd": "0.14.3", "zstd-safe": "8.2.0", "zstd-sys": "2.1.2+zstd.1.5.7"}
REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
GZIP_PACKAGES = {
    "flate2": ("1.1.10", "6e634e2e0ebac1ee034020da1ca582e17ffe4e0f5e985823721e168928136dcb"),
    "zlib-rs": ("0.6.8", "b268e58e7c693d7c271f93ffc4ba3b380412554231c85bf61ca7af91042a4112"),
}
RUSTIX_CHECKSUM = "e5e76742e5294153d4dde1207dbec5bdd81eef6fabdc0d2118babadabfd858aa"


class SourcePolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "docs").mkdir()
        (self.root / "crates/git-core").mkdir(parents=True)
        (self.root / "docs/gitoxide-source.json").write_bytes(
            (ROOT / "docs/gitoxide-source.json").read_bytes())
        (self.root / "docs/codec-source.json").write_bytes(
            (ROOT / "docs/codec-source.json").read_bytes())
        (self.root / "docs/zstd-source.json").write_bytes(
            (ROOT / "docs/zstd-source.json").read_bytes())
        (self.root / "docs/gzip-source.json").write_bytes(
            (ROOT / "docs/gzip-source.json").read_bytes())
        (self.root / "docs/rustix-source.json").write_bytes(
            (ROOT / "docs/rustix-source.json").read_bytes())
        (self.root / "docs/kubert-source.json").write_bytes(
            (ROOT / "docs/kubert-source.json").read_bytes())
        for path, name in (("Cargo.toml", "gix"), ("crates/git-core/Cargo.toml", "gix-object")):
            (self.root / path).write_text(f'[dependencies]\n{name} = {{ git = "{REPO}", rev = "{REV}" }}\n')
        self.manifest = self.root / "Cargo.toml"
        self.manifest.write_text(self.manifest.read_text() +
            f'zstd = {{ git = "{ZSTD_REPO}", rev = "{ZSTD_REV}", default-features = false, features = ["experimental", "vendored"] }}\n')
        self.manifest.write_text(self.manifest.read_text() +
            f'gix-packetline = {{ git = "{REPO}", rev = "{REV}", version = "=0.24.1", default-features = false, features = ["blocking-io"] }}\n'
            'flate2 = { version = "=1.1.10", default-features = false, features = ["zlib-rs"] }\n')
        self.manifest.write_text(self.manifest.read_text() +
            'rustix = { version = "=1.1.6", default-features = false, features = ["std", "event", "fs", "net", "pipe"] }\n')
        self.manifest.write_text(self.manifest.read_text() +
            f'kubert = {{ git = "{KUBERT_REPO}", rev = "{KUBERT_REV}", version = "=0.25.0", default-features = false, features = ["lease", "openssl-tls"] }}\n')
        self.core_manifest = self.root / "crates/git-core/Cargo.toml"
        self.core_manifest.write_text(self.core_manifest.read_text() +
            f'iconv-native = {{ git = "{CODEC_REPO}", rev = "{CODEC_REV}", version = "=0.1.0", default-features = false, features = ["libc-iconv"] }}\n')
        for name, version, features in (("gix-pack", "0.77.0", ', features = ["generate", "streaming-input"]'),
                                        ("gix-packetline", "0.24.1", ', features = ["blocking-io"]'),
                                        ("gix-config", "0.62.1", ""),
                                         ("gix-url", "0.40.1", ""),
                                        ("gix-traverse", "0.64.0", ""),
                                        ("gix-validate", "0.13.1", "")):
            self.core_manifest.write_text(self.core_manifest.read_text() +
                f'{name} = {{ git = "{REPO}", rev = "{REV}", version = "={version}", default-features = false{features} }}\n')
        self.gitoxide = [p for p in tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]
                         if p["name"] == "gix" or p["name"].startswith("gix-")]
        self.lock = "".join(f'[[package]]\nname = "{p["name"]}"\nversion = "{p["version"]}"\nsource = "{SOURCE}"\n'
                            + ('dependencies = ["bstr", "hashbrown 0.16.1"]\n'
                               if p["name"] == "gix-imara-diff" else "")
                            for p in self.gitoxide)
        self.lock += "".join(f'[[package]]\nname = "{name}"\nversion = "{version}"\nsource = "{ZSTD_SOURCE}"\n'
                             for name, version in ZSTD_PACKAGES.items())
        self.lock += "".join(f'[[package]]\nname = "{name}"\nversion = "{version}"\nsource = "{REGISTRY_SOURCE}"\nchecksum = "{checksum}"\n'
                             for name, (version, checksum) in GZIP_PACKAGES.items())
        self.lock += f'[[package]]\nname = "rustix"\nversion = "1.1.6"\nsource = "{REGISTRY_SOURCE}"\nchecksum = "{RUSTIX_CHECKSUM}"\n'
        self.lock += f'[[package]]\nname = "kubert"\nversion = "0.25.0"\nsource = "{KUBERT_SOURCE}"\n'
        self.lock += f'[[package]]\nname = "iconv-native"\nversion = "0.1.0"\nsource = "{CODEC_SOURCE}"\n'
        (self.root / "Cargo.lock").write_text(self.lock)

    def invoke(self, *args, override=None):
        environment = dict(os.environ)
        environment.pop("ZSTD_SYS_USE_PKG_CONFIG", None)
        environment.update(override or {})
        return subprocess.run([sys.executable, "-B", str(ROOT / "tools/check-core-source.py"),
                               "--project-root", str(self.root), *args], env=environment,
                              capture_output=True, text=True, timeout=5)

    def test_one_owned_pin_is_accepted(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("49 Gitoxide packages", result.stdout)

    def test_mixed_revision_is_rejected(self):
        (self.root / "Cargo.lock").write_text(self.lock.replace(SOURCE, SOURCE[:-40] + "b" * 40, 1))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mixed revision", result.stderr)

    def test_coherent_old_revision_is_not_a_qualified_receive_producer(self):
        paths = ("docs/gitoxide-source.json", "Cargo.toml", "crates/git-core/Cargo.toml", "Cargo.lock")
        originals = {path: (self.root / path).read_text() for path in paths}
        for old in ("339d3b0c8e276737c03eeffeb84119f1f9c9974e",
                    "7ce87f63e03ef116cb2d3df097cd688e6d19c35b",
                    "f8df02ef299e8f37a4335211f46dc98b4101311c",
                    "3db735ef3f90bf130a55b535e75a5134a9c66186",
                    "f71257f3c3c57b4cb5b98322391196c9e871c072",
                    "0d1d6a3e1bf4b5e698fe026d00438f34f49908c3",
                    "dadd76048c50c1d504baddd54ecd9ea64af4711c"):
            with self.subTest(old=old):
                for path in paths:
                    (self.root / path).write_text(originals[path].replace(REV, old))
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("selected controlled mapping, range and traversal revision", result.stderr)

    def test_registry_gitoxide_is_rejected(self):
        (self.root / "Cargo.lock").write_text(self.lock.replace(SOURCE, "registry+https://github.com/rust-lang/crates.io-index", 1))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("registry", result.stderr)

    def test_imara_hashbrown_edge_cannot_drift_during_a_git_source_update(self):
        for changed in ('"hashbrown 0.17.1"', '"hashbrown"', '"bstr"'):
            with self.subTest(dependency=changed):
                (self.root / "Cargo.lock").write_text(self.lock.replace('"hashbrown 0.16.1"', changed))
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("retained hashbrown 0.16.1 lock edge", result.stderr)

    def test_missing_manifest_pin_is_rejected(self):
        (self.root / "Cargo.toml").write_text(f'[dependencies]\ngix = {{ git = "{REPO}" }}\n')
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing or mixed", result.stderr)

    def test_duplicate_gitoxide_types_are_rejected(self):
        (self.root / "Cargo.lock").write_text(self.lock + f'[[package]]\nname = "gix"\nversion = "2.0.0"\nsource = "{SOURCE}"\n')
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate", result.stderr)

    def test_codec_source_cannot_be_registry_missing_or_duplicate(self):
        for lock in (
            self.lock.replace(CODEC_SOURCE, "registry+https://github.com/rust-lang/crates.io-index"),
            self.lock[:self.lock.index('[[package]]\nname = "iconv-native"')],
            self.lock + f'[[package]]\nname = "iconv-native"\nversion = "0.1.0"\nsource = "{CODEC_SOURCE}"\n',
        ):
            with self.subTest(lock=lock):
                (self.root / "Cargo.lock").write_text(lock)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("codec Git lock identity", result.stderr)

    def test_codec_path_or_mixed_revision_is_rejected(self):
        original = self.core_manifest.read_text()
        for manifest in (original.replace(f'git = "{CODEC_REPO}"', 'path = "vendor/iconv"'),
                         original.replace(CODEC_REV, "b" * 40)):
            with self.subTest(manifest=manifest):
                self.core_manifest.write_text(manifest)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("normal direct Git dependency", result.stderr)

    def test_patch_cannot_override_codec_source(self):
        with (self.root / "Cargo.toml").open("a") as manifest:
            manifest.write('\n[patch.crates-io]\niconv-native = { path = "vendor/iconv" }\n')
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("overrides", result.stderr)

    def test_zstd_registry_mixed_missing_duplicate_and_wrong_version_are_rejected(self):
        entry = f'[[package]]\nname = "zstd"\nversion = "0.14.3"\nsource = "{ZSTD_SOURCE}"\n'
        for lock in (
            self.lock.replace(ZSTD_SOURCE, "registry+https://github.com/rust-lang/crates.io-index", 1),
            self.lock.replace(ZSTD_SOURCE, ZSTD_SOURCE[:-40] + "b" * 40, 1),
            self.lock.replace(entry, ""),
            self.lock + entry,
            self.lock.replace('version = "0.14.3"', 'version = "0.14.2"'),
        ):
            with self.subTest(lock=lock):
                (self.root / "Cargo.lock").write_text(lock)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("three unique same-revision", result.stderr)

    def test_zstd_manifest_profile_and_source_are_enforced(self):
        original = self.manifest.read_text()
        for manifest in (
            original.replace('default-features = false', 'default-features = true', 1),
            original.replace('"experimental", "vendored"', '"vendored"'),
            original.replace('"experimental", "vendored"', '"experimental", "vendored", "zstdmt"'),
            original.replace(f'git = "{ZSTD_REPO}"', 'path = ".local/zstd-rs"'),
            original.replace(ZSTD_REV, "b" * 40),
            original + 'zstd-safe = "8.2.0"\n',
        ):
            with self.subTest(manifest=manifest):
                self.manifest.write_text(manifest)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("one normal root Git dependency", result.stderr)

    def test_zstd_environment_override_is_rejected_even_zero_or_empty(self):
        for value in ("0", "", "1"):
            with self.subTest(value=value):
                result = self.invoke(override={"ZSTD_SYS_USE_PKG_CONFIG": value})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be absent", result.stderr)

    def test_zstd_provenance_license_and_native_revision_are_enforced(self):
        path = self.root / "docs/zstd-source.json"
        original = path.read_text()
        for wrong in (original.replace('BSD-3-Clause AND BSD-2-Clause', 'BSD-3-Clause'),
                      original.replace('f8745da6ff1ad1e7bab384bd1f9d742439278e99', 'b' * 40)):
            with self.subTest(provenance=wrong):
                path.write_text(wrong)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("provenance", result.stderr)

    def test_zstd_resolved_feature_unification_is_enforced(self):
        # An otherwise valid direct manifest must not hide transitive feature additions.
        graph = {
            "packages": [{"id": name, "name": name, "version": version, "source": ZSTD_SOURCE}
                         for name, version in ZSTD_PACKAGES.items()],
            "resolve": {"nodes": [
                {"id": "zstd", "features": ["experimental", "vendored"]},
                {"id": "zstd-safe", "features": ["alloc", "experimental", "std", "vendored"]},
                {"id": "zstd-sys", "features": ["experimental", "std", "vendored"]},
            ]},
        }
        for package in self.gitoxide:
            name = package["name"]
            graph["packages"].append({"id": name, "name": name, "version": package["version"],
                                      "source": SOURCE,
                                      "license": "Apache-2.0" if name == "gix-imara-diff" else "MIT OR Apache-2.0"})
            features = {"gix-pack": ["generate", "streaming-input"], "gix-packetline": ["blocking-io"]}.get(name, [])
            graph["resolve"]["nodes"].append({"id": name, "features": features})
        for name, (version, _) in GZIP_PACKAGES.items():
            graph["packages"].append({"id": name, "name": name, "version": version, "source": REGISTRY_SOURCE,
                                      "license": "Zlib" if name == "zlib-rs" else "MIT OR Apache-2.0"})
            features = ["any_impl", "any_zlib", "zlib-rs"] if name == "flate2" else ["c-allocator", "default", "rust-allocator", "std"]
            graph["resolve"]["nodes"].append({"id": name, "features": features})
        graph["packages"].append({"id": "rustix", "name": "rustix", "version": "1.1.6", "source": REGISTRY_SOURCE,
                                  "license": "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT"})
        graph["resolve"]["nodes"].append({"id": "rustix", "features": ["alloc", "default", "event", "fs", "net", "pipe", "std"]})
        graph["packages"].append({"id": "kubert", "name": "kubert", "version": "0.25.0", "source": KUBERT_SOURCE,
                                  "license": "Apache-2.0"})
        graph["resolve"]["nodes"].append({"id": "kubert", "features": ["lease", "openssl-tls"]})
        path = self.root / "metadata.json"
        path.write_text(json.dumps(graph))
        result = self.invoke("--cargo-metadata", str(path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("resolved zstd features verified", result.stdout)
        for feature in ("default", "zstdmt", "bindgen", "pkg-config", "rust-backend"):
            with self.subTest(feature=feature):
                graph["resolve"]["nodes"][2]["features"] = ["experimental", "std", "vendored", feature]
                path.write_text(json.dumps(graph))
                result = self.invoke("--cargo-metadata", str(path))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("resolved Cargo source/features", result.stderr)
        graph["resolve"]["nodes"][2]["features"] = ["experimental", "std", "vendored"]
        gzip_node = next(n for n in graph["resolve"]["nodes"] if n["id"] == "flate2")
        for features in (["any_impl", "any_zlib", "zlib-rs", "rust_backend"],
                         ["any_impl", "any_zlib", "zlib-rs", "default"],
                         ["any_impl", "any_zlib", "zlib"]):
            with self.subTest(gzip_features=features):
                gzip_node["features"] = features
                path.write_text(json.dumps(graph))
                result = self.invoke("--cargo-metadata", str(path))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("gzip resolved Cargo", result.stderr)
        gzip_node["features"] = ["any_impl", "any_zlib", "zlib-rs"]
        rustix_node = next(n for n in graph["resolve"]["nodes"] if n["id"] == "rustix")
        for features in (["alloc", "default", "fs", "net", "pipe", "std"],
                         ["alloc", "default", "event", "fs", "net", "pipe", "runtime", "std"]):
            with self.subTest(rustix_features=features):
                rustix_node["features"] = features
                path.write_text(json.dumps(graph))
                result = self.invoke("--cargo-metadata", str(path))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("rustix resolved Cargo", result.stderr)

    def test_receive_dependency_profiles_are_required(self):
        original = self.core_manifest.read_text()
        for bad in (original.replace('["generate", "streaming-input"]', '["streaming-input"]'),
                    original.replace('["blocking-io"]', '[]'),
                    original.replace('version = "=0.13.1"', 'version = "=0.13.0"'),
                    original.replace('default-features = false, features = ["generate"',
                                     'default-features = true, features = ["generate"')):
            with self.subTest(manifest=bad):
                self.core_manifest.write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("receive requires the exact", result.stderr)

    def test_transitive_gitoxide_closure_cannot_lose_packages_or_change_versions(self):
        transitive = next(p for p in self.gitoxide if p["name"] == "gix-bitmap")
        entry = f'[[package]]\nname = "gix-bitmap"\nversion = "{transitive["version"]}"\nsource = "{SOURCE}"\n'
        for bad in (self.lock.replace(entry, ""),
                    self.lock.replace(entry, entry.replace(transitive["version"], "0.0.0"))):
            with self.subTest(lock=bad):
                (self.root / "Cargo.lock").write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("resolved package/version/license closure", result.stderr)

    def test_root_packetline_wire_profile_is_required(self):
        original = self.manifest.read_text()
        for bad in (original.replace('features = ["blocking-io"]', 'features = []'),
                    original.replace('version = "=0.24.1"', 'version = "=0.24.0"')):
            with self.subTest(manifest=bad):
                self.manifest.write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("exact root packetline", result.stderr)

    def test_gzip_root_minimal_backend_profile_is_required(self):
        original = self.manifest.read_text()
        for bad in (original.replace('version = "=1.1.10"', 'version = "1.1.10"'),
                    original.replace('default-features = false, features = ["zlib-rs"]',
                                     'default-features = true, features = ["zlib-rs"]'),
                    original.replace('features = ["zlib-rs"]', 'features = ["rust_backend"]')):
            with self.subTest(manifest=bad):
                self.manifest.write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("exact root flate2", result.stderr)

    def test_gzip_lock_and_license_cannot_drift(self):
        for bad in (self.lock.replace('version = "0.6.8"', 'version = "0.6.9"'),
                    self.lock.replace(GZIP_PACKAGES["flate2"][1], "0" * 64),
                    self.lock + f'[[package]]\nname = "flate2"\nversion = "1.1.10"\nsource = "{REGISTRY_SOURCE}"\n'):
            with self.subTest(lock=bad):
                (self.root / "Cargo.lock").write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("gzip registry lock identity", result.stderr)
        (self.root / "Cargo.lock").write_text(self.lock)
        path = self.root / "docs/gzip-source.json"
        path.write_text(path.read_text().replace('"license": "Zlib"', '"license": "MIT"'))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("gzip provenance identity/license", result.stderr)

    def test_rustix_exact_io_profile_is_required(self):
        original = self.manifest.read_text()
        for bad in (original.replace('version = "=1.1.6"', 'version = "1.1.6"'),
                    original.replace('default-features = false, features = ["std", "event"',
                                     'default-features = true, features = ["std", "event"'),
                    original.replace('["std", "event", "fs", "net", "pipe"]', '["std", "fs", "net", "pipe"]')):
            with self.subTest(manifest=bad):
                self.manifest.write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("exact root I/O dependency", result.stderr)

    def test_rustix_lock_and_license_cannot_drift(self):
        entry = f'[[package]]\nname = "rustix"\nversion = "1.1.6"\nsource = "{REGISTRY_SOURCE}"\nchecksum = "{RUSTIX_CHECKSUM}"\n'
        for bad in (self.lock.replace(entry, ""), self.lock + entry,
                    self.lock.replace(entry, entry.replace('1.1.6', '1.1.7')),
                    self.lock.replace(RUSTIX_CHECKSUM, "0" * 64)):
            with self.subTest(lock=bad):
                (self.root / "Cargo.lock").write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("rustix registry lock identity", result.stderr)
        (self.root / "Cargo.lock").write_text(self.lock)
        path = self.root / "docs/rustix-source.json"
        path.write_text(path.read_text().replace('Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT', 'MIT'))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("qualified registry/license/feature provenance", result.stderr)

    def test_gitmodules_config_and_url_direct_profiles_are_required(self):
        original = self.core_manifest.read_text()
        for name, version in (("gix-config", "0.62.1"), ("gix-url", "0.40.1")):
            line = next(line for line in original.splitlines(keepends=True) if line.startswith(name + " ="))
            for bad in (original.replace(line, ""),
                        original.replace(line, line.replace(f'"={version}"', f'"{version}"')),
                        original.replace(line, line.replace('default-features = false', 'default-features = true'))):
                with self.subTest(package=name, manifest=bad):
                    self.core_manifest.write_text(bad)
                    result = self.invoke()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(f"{name}: receive requires the exact", result.stderr)

    def test_gitmodules_url_source_hash_inventory_is_required(self):
        path = self.root / "docs/gitoxide-source.json"
        original = path.read_text()
        for missing in ("gix-url/src/fsck.rs", "gix-url/src/lib.rs"):
            with self.subTest(missing=missing):
                changed = json.loads(original)
                del changed["controlled_source_sha256"][missing]
                path.write_text(json.dumps(changed))
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("controlled submodule URL source hashes", result.stderr)
        changed = json.loads(original)
        changed["controlled_source_sha256"]["gix-url/src/fsck.rs"] = "not-a-source-hash"
        path.write_text(json.dumps(changed))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("controlled submodule URL source hashes", result.stderr)

    def test_range_and_topological_source_inventory_cannot_be_omitted_or_expanded(self):
        path = self.root / "docs/gitoxide-source.json"
        original = json.loads(path.read_text())
        for name in ("gix-pack/src/data/delta.rs", "gix-pack/src/index/init.rs",
                     "gix-pack/src/index/write/mod.rs", "gix-pack/src/memory.rs",
                     "gix-revwalk/src/queue.rs", "gix-traverse/src/commit/mod.rs",
                     "gix-traverse/src/commit/topo/admission.rs", "gix-traverse/src/commit/topo/init.rs",
                     "gix-traverse/src/commit/topo/iter.rs", "gix-traverse/src/commit/topo/mod.rs", "unexpected.rs"):
            with self.subTest(source=name):
                changed = json.loads(json.dumps(original))
                if name == "unexpected.rs":
                    changed["controlled_source_sha256"][name] = "a" * 64
                else:
                    del changed["controlled_source_sha256"][name]
                path.write_text(json.dumps(changed))
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("exact pack/traversal source inventory", result.stderr)

    def test_kubert_requires_its_normal_owned_lease_dependency(self):
        original = self.manifest.read_text()
        line = next(line for line in original.splitlines(keepends=True) if line.startswith("kubert ="))
        for bad in (original.replace(line, ""),
                    original.replace(line, line.replace(f'git = "{KUBERT_REPO}"', 'path = "local/kubert"')),
                    original.replace(line, line.replace(KUBERT_REV, "b" * 40)),
                    original.replace(line, line.replace('default-features = false', 'default-features = true')),
                    original.replace(line, line.replace('["lease", "openssl-tls"]', '["runtime", "lease", "openssl-tls"]'))):
            with self.subTest(manifest=bad):
                self.manifest.write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("one normal root owned Git dependency", result.stderr)

    def test_kubert_lock_identity_cannot_be_missing_mixed_or_duplicated(self):
        entry = f'[[package]]\nname = "kubert"\nversion = "0.25.0"\nsource = "{KUBERT_SOURCE}"\n'
        for bad in (self.lock.replace(entry, ""), self.lock + entry,
                    self.lock.replace(KUBERT_SOURCE, REGISTRY_SOURCE),
                    self.lock.replace(KUBERT_SOURCE, KUBERT_SOURCE[:-40] + "b" * 40),
                    self.lock.replace(entry, entry.replace('version = "0.25.0"', 'version = "0.24.0"'))):
            with self.subTest(lock=bad):
                (self.root / "Cargo.lock").write_text(bad)
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("unique selected owned Git lock identity", result.stderr)

    def test_coherent_unhardened_kubert_revision_is_rejected(self):
        for name in ("docs/kubert-source.json", "Cargo.toml", "Cargo.lock"):
            path = self.root / name
            path.write_text(path.read_text().replace(KUBERT_REV, "a2ff424f072a0de77d98ab97592d6f66ae1d9546"))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("selected owned source", result.stderr)

    def test_kubert_license_and_controlled_source_inventory_cannot_drift(self):
        path = self.root / "docs/kubert-source.json"
        original = json.loads(path.read_text())
        for missing in ("kubert/src/lease.rs", "kubert/src/admin/diagnostics/lease.rs", "kubert/Cargo.toml"):
            with self.subTest(missing=missing):
                changed = json.loads(json.dumps(original))
                del changed["controlled_source_sha256"][missing]
                path.write_text(json.dumps(changed))
                result = self.invoke()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("controlled hashes", result.stderr)
        changed = json.loads(json.dumps(original))
        changed["package"]["license"] = "MIT"
        path.write_text(json.dumps(changed))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("license provenance", result.stderr)


if __name__ == "__main__":
    unittest.main()
