#!/usr/bin/env python3
"""Istio watcher/report failure ownership, with no external processes."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

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


class BootstrapContracts(unittest.TestCase):
    def install(self, directory, version):
        values = {"replicaCount": 2, "migration": {"podAnnotations": {"sidecar.istio.io/inject": "false"}}}
        path, observed = Path(directory) / "values.yaml", []

        def helm(arguments):
            observed.append((arguments[1], yaml.safe_load(path.read_text())))
            return SimpleNamespace(stdout=b"")

        def kubectl(*arguments):
            if arguments[0] == "exec":
                return SimpleNamespace(stdout=version)
            self.assertEqual(arguments, ("wait", "--for=delete", "pod", "-l", "job-name=backend-migrate", "--timeout=60s"))
            return SimpleNamespace(stdout=b"")

        with patch.object(istio, "command", side_effect=helm):
            try:
                istio.install_application(SimpleNamespace(kubectl=kubectl), ["helm"], values, path)
            except AssertionError:
                self.assertEqual(observed[0][0], "install")
                self.assertEqual(len(observed), 1, "failed schema validation must not enable both replicas")
                raise
        return observed

    def test_real_migration_bootstraps_before_two_mesh_replicas_are_enabled(self):
        with tempfile.TemporaryDirectory() as directory:
            observed = self.install(directory, b"2\n")
        self.assertEqual([kind for kind, _ in observed], ["install", "upgrade"])
        self.assertEqual(observed[0][1]["replicaCount"], 1)
        self.assertTrue(observed[0][1]["migration"].get("enabled", True))
        self.assertEqual(observed[1][1]["replicaCount"], 2)
        self.assertFalse(observed[1][1]["migration"]["enabled"])
        self.assertEqual(observed[1][1]["migration"]["podAnnotations"], {"sidecar.istio.io/inject": "false"})

    def test_wrong_schema_does_not_start_the_second_mesh_replica(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(AssertionError, "bootstrap schema"):
            self.install(directory, b"1\n")


class PlatformImageContracts(unittest.TestCase):
    def test_platform_binding_accepts_only_the_exact_imported_config_or_manifest(self):
        reference = "docker.io/istio/pilot:synthetic"
        config, manifest = "sha256:" + "a" * 64, "sha256:" + "b" * 64
        immutable = "docker.io/istio/pilot@" + manifest
        cluster = ["docker", "exec", "owned-cluster"]
        for before in (config, manifest, "sha256:" + "c" * 64):
            with self.subTest(docker_id=before):
                registered = set()

                def command(arguments):
                    if arguments == ["docker", "image", "inspect", reference]:
                        body = json.dumps([{"Id": before}]).encode()
                    elif arguments == cluster + ["ctr", "images", "list", "name==" + reference]:
                        body = ("REF TYPE DIGEST SIZE\n" + reference + " manifest " + manifest + " 1\n").encode()
                    elif arguments == cluster + ["ctr", "images", "tag", reference, immutable]:
                        registered.add(immutable)
                        body = b""
                    elif arguments == cluster + ["crictl", "inspecti", immutable]:
                        self.assertIn(immutable, registered, "CRI lookup requires the immutable imported alias")
                        body = json.dumps({"status": {"id": config}}).encode()
                    else:
                        self.fail(f"unexpected image command: {arguments}")
                    return SimpleNamespace(stdout=body)

                with patch.object(istio, "import_image"), \
                        patch.object(istio, "command", side_effect=command):
                    if before in {config, manifest}:
                        bound = istio.platform_image("owned-cluster", reference)
                        self.assertEqual(bound, {"docker_id": before, "reference": immutable,
                                                 "cri_id": config})
                    else:
                        with self.assertRaisesRegex(AssertionError, "image identity changed"):
                            istio.platform_image("owned-cluster", reference)


if __name__ == "__main__":
    unittest.main()
