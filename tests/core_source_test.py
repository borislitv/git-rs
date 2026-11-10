#!/usr/bin/env python3
"""Fast CLI source-policy regressions; no Cargo, network or dependency checkout."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
REPO = "https://github.com/borislitv/gitoxide"
REV = "c3282a004f677b4d9b0b6bacb507462edf4b9e0a"
SOURCE = f"git+{REPO}?rev={REV}#{REV}"


class SourcePolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "docs").mkdir()
        (self.root / "crates/git-core").mkdir(parents=True)
        (self.root / "docs/gitoxide-source.json").write_bytes(
            (ROOT / "docs/gitoxide-source.json").read_bytes())
        for path, name in (("Cargo.toml", "gix"), ("crates/git-core/Cargo.toml", "gix-object")):
            (self.root / path).write_text(f'[dependencies]\n{name} = {{ git = "{REPO}", rev = "{REV}" }}\n')
        self.lock = "".join(f'[[package]]\nname = "{name}"\nversion = "1.0.0"\nsource = "{SOURCE}"\n'
                            for name in ("gix", "gix-object"))
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


if __name__ == "__main__":
    unittest.main()
