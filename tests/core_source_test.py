#!/usr/bin/env python3
"""Fast CLI source-policy regressions; no Cargo, network or dependency checkout."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
REPO = "https://github.com/borislitv/gitoxide"
REV = "7ce87f63e03ef116cb2d3df097cd688e6d19c35b"
SOURCE = f"git+{REPO}?rev={REV}#{REV}"
CODEC_REPO = "https://github.com/borislitv/iconv-native-rs"
CODEC_REV = "f8022d89dfe9e9fd2a6e3ce0bac5a68a4c77e89f"
CODEC_SOURCE = f"git+{CODEC_REPO}?rev={CODEC_REV}#{CODEC_REV}"
ZSTD_REPO = "https://github.com/borislitv/zstd-rs"
ZSTD_REV = "66f4b27ef27742d3df45826bc85c4ed423483cf0"
ZSTD_SOURCE = f"git+{ZSTD_REPO}?rev={ZSTD_REV}#{ZSTD_REV}"
ZSTD_PACKAGES = {"zstd": "0.14.3", "zstd-safe": "8.2.0", "zstd-sys": "2.1.2+zstd.1.5.7"}


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
        for path, name in (("Cargo.toml", "gix"), ("crates/git-core/Cargo.toml", "gix-object")):
            (self.root / path).write_text(f'[dependencies]\n{name} = {{ git = "{REPO}", rev = "{REV}" }}\n')
        self.manifest = self.root / "Cargo.toml"
        self.manifest.write_text(self.manifest.read_text() +
            f'zstd = {{ git = "{ZSTD_REPO}", rev = "{ZSTD_REV}", default-features = false, features = ["experimental", "vendored"] }}\n')
        self.core_manifest = self.root / "crates/git-core/Cargo.toml"
        self.core_manifest.write_text(self.core_manifest.read_text() +
            f'iconv-native = {{ git = "{CODEC_REPO}", rev = "{CODEC_REV}", version = "=0.1.0", default-features = false, features = ["libc-iconv"] }}\n')
        self.lock = "".join(f'[[package]]\nname = "{name}"\nversion = "1.0.0"\nsource = "{SOURCE}"\n'
                            for name in ("gix", "gix-object"))
        self.lock += "".join(f'[[package]]\nname = "{name}"\nversion = "{version}"\nsource = "{ZSTD_SOURCE}"\n'
                             for name, version in ZSTD_PACKAGES.items())
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
        self.assertIn("2 Gitoxide packages", result.stdout)

    def test_mixed_revision_is_rejected(self):
        (self.root / "Cargo.lock").write_text(self.lock.replace(SOURCE, SOURCE[:-40] + "b" * 40, 1))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mixed revision", result.stderr)

    def test_coherent_old_revision_is_not_a_qualified_memo_producer(self):
        old = "339d3b0c8e276737c03eeffeb84119f1f9c9974e"
        for path in ("docs/gitoxide-source.json", "Cargo.toml", "crates/git-core/Cargo.toml", "Cargo.lock"):
            target = self.root / path
            target.write_text(target.read_text().replace(REV, old))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("qualified completed-mapping revision", result.stderr)

    def test_registry_gitoxide_is_rejected(self):
        (self.root / "Cargo.lock").write_text(self.lock.replace(SOURCE, "registry+https://github.com/rust-lang/crates.io-index", 1))
        result = self.invoke()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("registry", result.stderr)

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
            original.replace('default-features = false', 'default-features = true'),
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


if __name__ == "__main__":
    unittest.main()
