#!/usr/bin/env python3
"""Fast CLI source-policy regressions; no Cargo, network or dependency checkout."""
from pathlib import Path
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
        for path, name in (("Cargo.toml", "gix"), ("crates/git-core/Cargo.toml", "gix-object")):
            (self.root / path).write_text(f'[dependencies]\n{name} = {{ git = "{REPO}", rev = "{REV}" }}\n')
        self.core_manifest = self.root / "crates/git-core/Cargo.toml"
        self.core_manifest.write_text(self.core_manifest.read_text() +
            f'iconv-native = {{ git = "{CODEC_REPO}", rev = "{CODEC_REV}", version = "=0.1.0", default-features = false, features = ["libc-iconv"] }}\n')
        self.lock = "".join(f'[[package]]\nname = "{name}"\nversion = "1.0.0"\nsource = "{SOURCE}"\n'
                            for name in ("gix", "gix-object"))
        self.lock += f'[[package]]\nname = "iconv-native"\nversion = "0.1.0"\nsource = "{CODEC_SOURCE}"\n'
        (self.root / "Cargo.lock").write_text(self.lock)

    def invoke(self):
        return subprocess.run([sys.executable, "-B", str(ROOT / "tools/check-core-source.py"),
                               "--project-root", str(self.root)],
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


if __name__ == "__main__":
    unittest.main()
