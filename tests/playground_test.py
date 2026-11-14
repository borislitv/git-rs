#!/usr/bin/env python3
"""Persistent VM state and loopback-daemon contracts without Docker or network."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "playground", Path(__file__).resolve().parents[1] / "examples/playground/playground.py")
PLAYGROUND = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PLAYGROUND)


class PlaygroundContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name)
        self.config = {"format": 2, "owner": "a" * 32, "image": "sha256:" + "b" * 64}
        self.path = self.state / "config.json"
        self.path.write_text(json.dumps(self.config))
        self.path.chmod(0o600)
        override = patch.object(PLAYGROUND, "STATE", self.state)
        override.start()
        self.addCleanup(override.stop)

    def test_saved_configuration_is_read_without_changing_persistent_inputs(self):
        before = self.path.read_bytes()
        with patch.object(PLAYGROUND, "run", side_effect=AssertionError("unexpected process")):
            self.assertEqual(PLAYGROUND.configuration(False, None), self.config)
        self.assertEqual(self.path.read_bytes(), before)

    def test_bad_configuration_files_fail_without_starting_or_repairing_services(self):
        for kind in ("symlink", "oversized", "public"):
            with self.subTest(kind=kind):
                self.path.unlink()
                if kind == "symlink":
                    target = self.state / "saved.json"
                    target.write_text(json.dumps(self.config))
                    target.chmod(0o600)
                    self.path.symlink_to(target)
                else:
                    self.path.write_text("x" * (64 * 1024 + 1) if kind == "oversized" else json.dumps(self.config))
                    self.path.chmod(0o644 if kind == "public" else 0o600)
                with patch.object(PLAYGROUND, "run", side_effect=AssertionError("unexpected process")):
                    with self.assertRaises((RuntimeError, OSError)):
                        PLAYGROUND.configuration(False, None)
                self.assertTrue(self.path.is_symlink() if kind == "symlink" else self.path.exists())

    def test_a_fifo_configuration_fails_before_reading_or_invoking_docker(self):
        self.path.unlink()
        os.mkfifo(self.path, mode=0o600)
        code = ("import importlib.util,pathlib,sys; "
                "spec=importlib.util.spec_from_file_location('playground',sys.argv[1]); "
                "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
                "module.STATE=pathlib.Path(sys.argv[2]); "
                "module.run=lambda *args,**kwargs: sys.exit(86); "
                "sys.argv=['playground','status']; module.main()")
        result = subprocess.run([sys.executable, "-B", "-c", code, SPEC.origin, str(self.state)],
                                capture_output=True, text=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bounded private mode 0600 file", result.stderr)
        self.assertNotEqual(result.returncode, 86, "invalid input invoked the process boundary")

    def test_local_loopback_services_cannot_be_sent_to_an_inherited_remote_daemon(self):
        with patch.dict(os.environ, DOCKER_HOST="tcp://docker.example.invalid:2376", DOCKER_CONTEXT="foreign"), \
                patch.object(PLAYGROUND.subprocess, "run") as process:
            PLAYGROUND.run("docker", "image", "inspect", self.config["image"], capture=True)
        arguments, options = process.call_args
        self.assertEqual(arguments[0], ("docker", "image", "inspect", self.config["image"]))
        self.assertEqual(options["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", options["env"])

    def test_restart_targets_the_observed_immutable_container_not_its_reusable_name(self):
        identity = "c" * 64
        with patch.object(PLAYGROUND, "owned", return_value={"Id": identity, "State": {"Running": False}}), \
                patch.object(PLAYGROUND, "run") as process:
            PLAYGROUND.start(self.config, "app", ["unused"])
        process.assert_called_once_with("docker", "start", identity)


if __name__ == "__main__":
    unittest.main()
