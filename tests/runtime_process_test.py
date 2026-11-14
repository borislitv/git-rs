#!/usr/bin/env python3
"""Immutable server-image and namespace/ownership contracts; no Docker required."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import runtime_process as runtime


class RuntimeContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.revision, self.image = "a" * 40, "sha256:" + "b" * 64
        self.owner = "c" * 32
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def image_record(self, **labels):
        return {"Id": self.image, "Os": "linux", "Architecture": "amd64", "Config": {"Labels": {
            "io.git-rs.role": "runtime", "org.opencontainers.image.revision": self.revision, **labels}}}

    def make_runtime(self):
        with patch.object(runtime, "run", return_value=subprocess.CompletedProcess([], 0,
                           json.dumps([self.image_record()]).encode(), b"")):
            return runtime.Runtime(self.directory, self.owner, "loaded-server", None, self.revision, Mock())

    def test_mutable_name_resolves_once_but_wrong_role_or_revision_never_starts(self):
        for labels in [{"io.git-rs.role": "external-client-oracle"},
                       {"io.git-rs.role": "runtime-fixture"},
                       {"org.opencontainers.image.revision": "d" * 40}]:
            with self.subTest(labels=labels), patch.object(runtime, "run", return_value=
                    subprocess.CompletedProcess([], 0, json.dumps([self.image_record(**labels)]).encode(), b"")):
                with self.assertRaises(AssertionError):
                    runtime.Runtime(self.directory, self.owner, "wrong-image", None, self.revision, Mock())
        instance = self.make_runtime()
        self.assertEqual(instance.image, self.image)

    def test_cli_mounts_only_private_server_data_and_keeps_secrets_out_of_argv(self):
        instance = self.make_runtime()
        secret = "synthetic-password-1234"
        seen = []

        def daemon(argv, **options):
            seen.append(argv)
            self.assertIn(self.image, argv)
            self.assertNotIn(secret, " ".join(map(str, argv)))
            mounts = [argv[index + 1] for index, value in enumerate(argv) if value == "--mount"]
            self.assertEqual(mounts, [f"type=bind,src={self.directory},dst={self.directory}"])
            path = Path(argv[argv.index("--env-file") + 1])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertIn("PGPASSWORD=" + secret + "\n", path.read_text())
            self.assertNotIn("GIT_CONFIG_GLOBAL", path.read_text())
            return subprocess.CompletedProcess([], 0, b"synthetic result\n", b"")

        with patch.object(runtime, "run", side_effect=daemon):
            result = instance.execute(["history", "team/project", "alice"],
                                      {"PGPASSWORD": secret, "GIT_CONFIG_GLOBAL": "/foreign/config"})
        self.assertEqual(result.stdout, b"synthetic result\n")
        self.assertEqual(len(seen), 1)
        self.assertEqual(list(self.directory.iterdir()), [], "credential file must be removed after execution")

    def test_missing_fixture_image_never_falls_back_to_the_oracle(self):
        instance = self.make_runtime()
        with patch.object(runtime, "run") as external:
            with self.assertRaisesRegex(AssertionError, "runtime-fixture"):
                instance.start([], {}, None, program="/usr/local/bin/examples/blame-http-fixture")
        external.assert_not_called()

    def test_maintenance_credentials_are_not_part_of_a_serving_mount(self):
        server = self.directory / "server"
        server.mkdir(mode=0o700)
        account = self.directory / "maintenance" / "account"
        account.parent.mkdir(mode=0o700)
        account.mkdir(mode=0o700)
        with patch.object(runtime, "run", return_value=subprocess.CompletedProcess([], 0,
                           json.dumps([self.image_record()]).encode(), b"")):
            instance = runtime.Runtime(server, self.owner, "loaded-server", None, self.revision, Mock())
        ordinary, _, _ = instance.arguments(None)
        maintenance, _, _ = instance.arguments(None, service_account=account)
        self.assertNotIn(str(account), " ".join(ordinary))
        self.assertNotIn("serviceaccount", " ".join(ordinary))
        self.assertIn(f"type=bind,src={account},dst=/var/run/secrets/kubernetes.io/serviceaccount,readonly", maintenance)

    def test_failed_container_attach_retains_exact_cleanup_identity(self):
        instance = self.make_runtime()
        identifier = "d" * 64
        with patch.object(runtime, "run", return_value=subprocess.CompletedProcess([], 0, (identifier + "\n").encode(), b"")), \
                patch.object(runtime.subprocess, "Popen", side_effect=OSError("attach failed")):
            with self.assertRaises(OSError):
                instance.start(["serve"], {}, None)
        instance.register.assert_called_once_with(identifier)
        self.assertEqual(list(self.directory.iterdir()), [], "failed startup must remove its credential file")

    def test_attach_uses_the_same_local_daemon_as_container_creation(self):
        instance = self.make_runtime()
        identifier = "d" * 64
        with patch.dict(os.environ, DOCKER_HOST="tcp://foreign.example.invalid:2375", DOCKER_CONTEXT="foreign"), \
                patch.object(runtime, "run", return_value=subprocess.CompletedProcess([], 0, (identifier + "\n").encode(), b"")), \
                patch.object(runtime.subprocess, "Popen") as attached:
            process = instance.start(["serve"], {}, None)
        self.assertEqual(process.identifier, identifier)
        self.assertEqual(attached.call_args.kwargs["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", attached.call_args.kwargs["env"])
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_foreign_container_cannot_be_signalled(self):
        process = runtime.RuntimeProcess("d" * 64, self.owner, Mock())
        row = {"Id": process.identifier, "Config": {"Labels": {runtime.LABEL: "e" * 32}},
               "State": {"Running": True}}
        with patch.object(runtime, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps([row]).encode(), b"")) as external:
            with self.assertRaisesRegex(AssertionError, "foreign"):
                process.kill()
        self.assertEqual(external.call_count, 1)
        self.assertEqual(external.call_args.args[0][1], "inspect")

    def test_memory_status_comes_from_the_backend_namespace_not_docker_attach(self):
        process = runtime.RuntimeProcess("d" * 64, self.owner, Mock())
        row = {"Id": process.identifier, "Config": {"Labels": {runtime.LABEL: self.owner}},
               "State": {"Running": True}}

        def daemon(argv, **options):
            if argv[1] == "inspect":
                data = json.dumps([row]).encode()
            elif argv[-1].endswith("/children"):
                data = b"7 "
            else:
                self.assertEqual(argv[-1], "/proc/7/status")
                data = b"Name:\tgit-rs\nVmHWM:\t128 kB\n"
            return subprocess.CompletedProcess([], 0, data, b"")

        with patch.object(runtime, "run", side_effect=daemon):
            self.assertEqual(process.process_status(), "Name:\tgit-rs\nVmHWM:\t128 kB\n")

    def test_actual_cri_tasks_must_be_under_the_aggregate_not_only_the_outer_cluster(self):
        root = "/owned.slice/pods"
        for group in (root + "/kubepods/child", "/unrelated.slice/child", root + "-neighbor/child"):
            def daemon(argv, **_):
                if argv[-3:] == ["ps", "-o", "json"]:
                    data = json.dumps({"containers": [{"id": "d" * 64, "state": "CONTAINER_RUNNING"}]}).encode()
                elif argv[-2] == "inspect":
                    data = b'{"info":{"pid":123}}'
                elif argv[-1].endswith("/cgroup"):
                    self.assertEqual(argv[-1], "/proc/123/cgroup")
                    data = ("0::" + group + "\n").encode()
                else:
                    self.assertEqual(argv[-3:], ["pods", "-o", "json"])
                    data = b'{"items":[]}'
                return subprocess.CompletedProcess([], 0, data, b"")

            with self.subTest(group=group), patch.object(runtime, "run", side_effect=daemon):
                if group == root + "/kubepods/child":
                    self.assertEqual(runtime.cluster_cgroups("selected-cluster", root),
                                     [{"id": "d" * 64, "cgroup": group}])
                else:
                    with self.assertRaisesRegex(AssertionError, "escaped"):
                        runtime.cluster_cgroups("selected-cluster", root)


if __name__ == "__main__":
    unittest.main()
