#!/usr/bin/env python3
"""Istio watcher/report failure ownership, with no external processes."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import istio


class LifecycleContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.stopped = []

    def watcher(self, name, error=None):
        stopped = self.stopped

        class Watcher:
            def close(self):
                stopped.append(name)
                if error is not None:
                    raise error

        return Watcher()

    def test_lifecycle_failure_invalidates_success_and_attempts_all_watchers(self):
        report = {"status": "passed"}
        with self.assertRaises(BaseExceptionGroup):
            istio.finish(self.base, report, [self.watcher("first", RuntimeError("watch failed")), self.watcher("second")])
        self.assertEqual(self.stopped, ["first", "second"])
        saved = json.loads((self.base / "report.json").read_text())
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["lifecycle_errors"], ["RuntimeError"])
        self.assertEqual((self.base / "report.json").stat().st_mode & 0o777, 0o600)

    def test_cleanup_failure_preserves_the_foreground_error_and_failed_report(self):
        original = RuntimeError("HTTPS operation failed")
        try:
            try:
                raise original
            finally:
                istio.finish(self.base, {"status": "failed"}, [self.watcher("watch", OSError("watch close failed"))])
        except RuntimeError as error:
            self.assertIs(error, original)
        self.assertEqual(self.stopped, ["watch"])
        self.assertEqual(json.loads((self.base / "report.json").read_text())["status"], "failed")
        self.assertTrue(any("OSError" in note for note in original.__notes__))

    def test_report_creation_failure_does_not_replace_the_foreground_error(self):
        original = RuntimeError("push failed")
        with patch.object(istio.tempfile, "mkstemp", side_effect=OSError("no space")):
            try:
                try:
                    raise original
                finally:
                    istio.finish(self.base, {"status": "failed"}, [self.watcher("watch")])
            except RuntimeError as error:
                self.assertIs(error, original)
        self.assertEqual(self.stopped, ["watch"])
        self.assertTrue(any("OSError" in note for note in original.__notes__))
        self.assertFalse((self.base / "report.json").exists())


class PlatformImageContracts(unittest.TestCase):
    def test_platform_binding_accepts_only_the_exact_imported_config_or_manifest(self):
        reference = "docker.io/istio/pilot:synthetic"
        config, manifest = "sha256:" + "a" * 64, "sha256:" + "b" * 64
        for before in (config, manifest, "sha256:" + "c" * 64):
            with self.subTest(docker_id=before):
                replies = [json.dumps([{"Id": before}]).encode(), json.dumps([{"Id": before}]).encode(),
                           ("REF TYPE DIGEST SIZE\n" + reference + " manifest " + manifest + " 1\n").encode(),
                           json.dumps({"status": {"id": config}}).encode()]
                with patch.object(istio, "import_image"), \
                        patch.object(istio, "command", side_effect=[SimpleNamespace(stdout=body) for body in replies]):
                    if before in {config, manifest}:
                        bound = istio.platform_image("owned-cluster", reference)
                        self.assertEqual(bound, {"docker_id": before, "reference": "docker.io/istio/pilot@" + manifest,
                                                 "cri_id": config})
                    else:
                        with self.assertRaisesRegex(AssertionError, "image identity changed"):
                            istio.platform_image("owned-cluster", reference)


if __name__ == "__main__":
    unittest.main()
