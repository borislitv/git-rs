#!/usr/bin/env python3
"""Persistent VM state and loopback-daemon contracts without Docker or network."""
import importlib.util
import hashlib
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
        item = {"Id": identity, "State": {"Running": False},
                "HostConfig": {"Memory": 2415919104, "MemorySwap": 2415919104,
                               "NanoCpus": 1250000000, "PidsLimit": 112},
                "Config": {"Env": ["GIT_RS_BLAME_CONCURRENCY=1", "GIT_RS_BLAME_WAITERS=0"]}}
        with patch.object(PLAYGROUND, "owned", return_value=item), \
                patch.object(PLAYGROUND, "run") as process:
            PLAYGROUND.start(self.config, "app", ["unused"])
        process.assert_called_once_with("docker", "start", identity)


class LifecycleContracts(unittest.TestCase):
    """CLI lifecycle with private real state and a local Docker boundary fake."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state = Path(temporary.name) / "state"
        self.image = "sha256:" + "b" * 64
        self.containers = {}
        self.volumes = {}
        self.mutations = []
        self.cli_commands = []
        self.next_identity = 0
        for override in (patch.object(PLAYGROUND, "STATE", self.state),
                         patch.object(PLAYGROUND, "run", side_effect=self.docker),
                         patch.object(PLAYGROUND.socket, "socket"),
                         patch.dict(os.environ, {}, clear=True)):
            override.start()
            self.addCleanup(override.stop)

    def main(self, *arguments):
        with patch.object(sys, "argv", ["playground.py", *arguments]):
            PLAYGROUND.main()

    def docker(self, *args, **kwargs):
        def result(value="", code=0, error=""):
            return subprocess.CompletedProcess(args, code, value, error)

        if args[0] == "curl":
            return result()
        self.assertEqual(args[0], "docker")
        action = args[1]
        if action == "info":
            return result()
        if action in ("image", "container", "volume") and args[2] == "inspect":
            name = args[3]
            if action == "image":
                item = {"Id": name, "Config": {"Labels": {"io.git-rs.role": "runtime"}}}
            else:
                item = (self.containers if action == "container" else self.volumes).get(name)
            return result(json.dumps([item])) if item else result(code=1, error="No such object")
        if action == "volume" and args[2] == "create":
            self.mutations.append(args)
            key, value = args[4].split("=", 1)
            self.volumes[args[5]] = {"Labels": {key: value}}
            return result()
        if action == "run":
            self.mutations.append(args)
            env = Path(args[args.index("--env-file") + 1]).read_text().splitlines()
            if "--name" not in args:
                self.cli_commands.append(args)
                return result()
            name = args[args.index("--name") + 1]
            key, value = args[args.index("--label") + 1].split("=", 1)
            memory = int(args[args.index("--memory") + 1][:-1]) * 1024 * 1024
            image = next(value for value in args if value.startswith("sha256:") or value in
                         (PLAYGROUND.PG_IMAGE, PLAYGROUND.S3_IMAGE))
            self.next_identity += 1
            self.containers[name] = {
                "Id": format(self.next_identity, "064x"), "Image": image,
                "State": {"Running": True},
                "Config": {"Labels": {key: value}, "Env": env},
                "HostConfig": {"Memory": memory, "MemorySwap": memory,
                               "NanoCpus": int(float(args[args.index("--cpus") + 1]) * 1e9),
                               "PidsLimit": int(args[args.index("--pids-limit") + 1])}}
            return result()
        if action == "exec":
            return result("1" if args[3] == "psql" else "")
        if action in ("stop", "start", "rm"):
            self.mutations.append(args)
            name, item = next((name, item) for name, item in self.containers.items()
                              if item["Id"] == args[-1])
            if action == "rm":
                del self.containers[name]
            else:
                item["State"]["Running"] = action == "start"
            return result()
        raise AssertionError(f"Unexpected Docker operation: {args[:3]}")

    def app(self):
        return self.containers["git-rs-playground-app"]

    def config(self):
        return json.loads((self.state / "config.json").read_text())

    def test_default_and_large_caps_are_persisted_and_used_by_actual_launches(self):
        for profile, memory, cpu, tasks in ((None, 2415919104, 1250000000, 112),
                                             ("large-v1", 6442450944, 3000000000, 96)):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                self.state = Path(directory) / "state"
                self.containers.clear()
                self.volumes.clear()
                with patch.object(PLAYGROUND, "STATE", self.state):
                    selection = ["--resource-profile", profile] if profile else []
                    self.main("up", "--image", self.image, *selection)
                    self.assertEqual(self.config()["resource_profile"], profile or "baseline-v1")
                    self.assertEqual(self.app()["Image"], self.image)
                    self.assertEqual(self.app()["HostConfig"], {
                        "Memory": memory, "MemorySwap": memory, "NanoCpus": cpu, "PidsLimit": tasks})
                    for service, cap, quota, count in (("pg", 1073741824, 500000000, 64),
                                                       ("s3", 536870912, 250000000, 48)):
                        self.assertEqual(self.containers["git-rs-playground-" + service]["HostConfig"],
                                         {"Memory": cap, "MemorySwap": cap,
                                          "NanoCpus": quota, "PidsLimit": count})
                    self.assertIn("GIT_RS_BLAME_RESOURCE_PROFILE=" + (profile or "baseline-v1"),
                                  self.app()["Config"]["Env"])
                    self.assertIn("GIT_RS_BLAME_CONCURRENCY=1", self.app()["Config"]["Env"])
                    self.assertIn("GIT_RS_BLAME_WAITERS=0", self.app()["Config"]["Env"])
                    original = self.app()["Id"]
                    self.main("stop")
                    self.main("up")
                    self.assertEqual(self.app()["Id"], original)
                    self.assertTrue(self.app()["State"]["Running"])

    def test_conflicting_or_ambient_selection_does_not_mutate_existing_resources(self):
        self.main("up", "--image", self.image)
        before = (self.state / "config.json").read_bytes()
        self.mutations.clear()
        with self.assertRaisesRegex(RuntimeError, "differs from saved"):
            self.main("up", "--resource-profile", "large-v1")
        with patch.dict(os.environ, GIT_RS_BLAME_RESOURCE_PROFILE="large-v1"):
            with self.assertRaisesRegex(RuntimeError, "ambient"):
                self.main("up")
        self.assertEqual(self.mutations, [])
        self.assertEqual((self.state / "config.json").read_bytes(), before)

    def test_unknown_saved_profile_and_conflicting_private_environment_fail_closed(self):
        self.main("up", "--image", self.image)
        config = self.config()
        config["resource_profile"] = "unrecognized-v2"
        (self.state / "config.json").write_text(json.dumps(config))
        self.mutations.clear()
        with self.assertRaisesRegex(RuntimeError, "Unknown saved"):
            self.main("up")
        self.assertEqual(self.mutations, [])
        config["resource_profile"] = "baseline-v1"
        (self.state / "config.json").write_text(json.dumps(config))
        environment = self.state / "app.env"
        environment.write_text(environment.read_text() + "GIT_RS_BLAME_WAITERS=3\n")
        with self.assertRaisesRegex(RuntimeError, "settings differ"):
            self.main("up")
        self.assertEqual(self.mutations, [])

    def test_stale_caps_and_container_environment_fail_before_restart_or_migration(self):
        self.main("up", "--image", self.image, "--resource-profile", "large-v1")
        self.main("stop")
        cases = (("Memory", 2415919104), ("MemorySwap", -1),
                 ("NanoCpus", 1250000000), ("PidsLimit", 112), ("CpuQuota", 100000))
        for key, value in cases:
            with self.subTest(key=key):
                previous = self.app()["HostConfig"].copy()
                self.app()["HostConfig"][key] = value
                self.mutations.clear()
                with self.assertRaisesRegex(RuntimeError, "caps differ"):
                    self.main("up")
                self.assertEqual(self.mutations, [])
                self.app()["HostConfig"] = previous
        self.app()["Config"]["Env"].append("GIT_RS_BLAME_RESOURCE_PROFILE=baseline-v1")
        with self.assertRaisesRegex(RuntimeError, "settings differ"):
            self.main("up")
        self.assertEqual(self.mutations, [])

    def test_upgrade_requires_all_owned_services_stopped_and_preserves_credentials(self):
        self.main("up", "--image", self.image)
        before = (self.state / "app.env").read_bytes()
        self.mutations.clear()
        with self.assertRaisesRegex(RuntimeError, "Stop all"):
            self.main("upgrade", "--resource-profile", "large-v1")
        self.assertEqual(self.mutations, [])
        self.assertEqual((self.state / "app.env").read_bytes(), before)
        self.main("stop")
        dependency_ids = {name: item["Id"] for name, item in self.containers.items()
                          if not name.endswith("-app")}
        volumes = self.volumes.copy()
        credentials = {name: (self.state / name).read_bytes()
                       for name in ("pg.env", "s3.env", "s3.curl")}
        self.main("upgrade", "--resource-profile", "large-v1")
        self.assertNotIn("git-rs-playground-app", self.containers)
        self.assertEqual(self.config()["resource_profile"], "large-v1")
        self.main("up")
        self.assertEqual(self.app()["HostConfig"]["Memory"], 6442450944)
        for line in before.decode().splitlines():
            if not line.startswith("GIT_RS_BLAME_RESOURCE_PROFILE="):
                self.assertIn(line, self.app()["Config"]["Env"])
        self.assertEqual(self.volumes, volumes)
        for name, identity in dependency_ids.items():
            self.assertEqual(self.containers[name]["Id"], identity)
        for name, content in credentials.items():
            self.assertEqual((self.state / name).read_bytes(), content)
        self.main("stop")
        replacement = "sha256:" + "d" * 64
        self.main("upgrade", "--image", replacement)
        self.assertEqual(self.config()["resource_profile"], "large-v1")
        self.main("up")
        self.assertEqual(self.app()["Image"], replacement)
        self.assertIn("GIT_RS_BLAME_RESOURCE_PROFILE=large-v1", self.app()["Config"]["Env"])

    def test_stale_dependency_caps_block_upgrade_without_removing_application(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        self.containers["git-rs-playground-pg"]["HostConfig"]["PidsLimit"] = 128
        before = (self.state / "config.json").read_bytes()
        self.mutations.clear()
        with self.assertRaisesRegex(RuntimeError, "caps differ"):
            self.main("upgrade", "--resource-profile", "large-v1")
        self.assertEqual(self.mutations, [])
        self.assertEqual((self.state / "config.json").read_bytes(), before)
        self.assertIn("git-rs-playground-app", self.containers)

    def saved_lifecycle(self):
        return {
            "files": {name: (self.state / name).read_bytes() for name in
                      ("config.json", "app.env", "pg.env", "s3.env", "s3.curl")},
            "containers": json.loads(json.dumps(self.containers)),
            "volumes": json.loads(json.dumps(self.volumes)),
        }

    def restore_lifecycle(self, saved):
        for name, content in saved["files"].items():
            (self.state / name).write_bytes(content)
        self.containers = json.loads(json.dumps(saved["containers"]))
        self.volumes = json.loads(json.dumps(saved["volumes"]))

    def test_image_only_upgrade_rejects_stale_actual_app_caps_without_mutation(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        initial = self.saved_lifecycle()
        for key, value in (("Memory", 6442450944), ("MemorySwap", -1),
                           ("NanoCpus", 3000000000), ("PidsLimit", 96)):
            with self.subTest(cap=key):
                self.restore_lifecycle(initial)
                self.app()["HostConfig"][key] = value
                before = self.saved_lifecycle()
                self.mutations.clear()
                with self.assertRaises(RuntimeError):
                    self.main("upgrade", "--image", "sha256:" + "d" * 64)
                self.assertEqual(self.mutations, [], "upgrade mutated Docker before rejecting stale caps")
                self.assertEqual(self.saved_lifecycle(), before)

    def test_image_only_upgrade_rejects_conflicting_private_or_container_settings(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        environment = self.state / "app.env"
        original_container_env = self.app()["Config"]["Env"][:]
        initial = self.saved_lifecycle()
        for location in ("private", "container"):
            for key, value in (("GIT_RS_BLAME_RESOURCE_PROFILE", "large-v1"),
                               ("GIT_RS_BLAME_CONCURRENCY", "2"),
                               ("GIT_RS_BLAME_WAITERS", "3")):
                with self.subTest(location=location, setting=key):
                    self.restore_lifecycle(initial)
                    lines = [line for line in original_container_env
                             if not line.startswith(key + "=")]
                    lines.append(key + "=" + value)
                    if location == "private":
                        environment.write_text("\n".join(lines) + "\n")
                    else:
                        self.app()["Config"]["Env"] = lines
                    before = self.saved_lifecycle()
                    self.mutations.clear()
                    with self.assertRaises(RuntimeError):
                        self.main("upgrade", "--image", "sha256:" + "d" * 64)
                    self.assertEqual(self.mutations, [], "conflicting settings authorized app replacement")
                    self.assertEqual(self.saved_lifecycle(), before)

    def test_explicit_profile_upgrade_retry_recovers_after_config_persist_failure(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        before = self.saved_lifecycle()
        replace = PLAYGROUND.private_replace

        def fail_config_write(path, content):
            if path == self.state / "config.json":
                raise OSError("synthetic config replacement failure")
            return replace(path, content)

        for attempt in range(2):
            with self.subTest(interrupted_attempt=attempt):
                with patch.object(PLAYGROUND, "private_replace", side_effect=fail_config_write):
                    with self.assertRaisesRegex(OSError, "synthetic config replacement failure"):
                        self.main("upgrade", "--resource-profile", "large-v1")
                self.assertNotIn("git-rs-playground-app", self.containers)
                self.assertEqual((self.state / "config.json").read_bytes(), before["files"]["config.json"])
                self.assertEqual(self.volumes, before["volumes"])
                for name in ("pg.env", "s3.env", "s3.curl"):
                    self.assertEqual((self.state / name).read_bytes(), before["files"][name])
        self.assertNotIn("git-rs-playground-app", self.containers)
        self.assertEqual((self.state / "config.json").read_bytes(), before["files"]["config.json"])
        target_environment = (self.state / "app.env").read_bytes()
        self.assertIn(b"GIT_RS_BLAME_RESOURCE_PROFILE=large-v1\n", target_environment)
        self.assertIn(b"GIT_RS_BLAME_CONCURRENCY=1\n", target_environment)
        self.assertIn(b"GIT_RS_BLAME_WAITERS=0\n", target_environment)
        interrupted = self.saved_lifecycle()
        self.mutations.clear()
        with self.assertRaises(RuntimeError):
            self.main("up")
        self.assertEqual(self.mutations, [], "partial profile persistence started services")
        self.assertEqual(self.saved_lifecycle(), interrupted)
        with self.assertRaises(RuntimeError):
            self.main("upgrade", "--resource-profile", "baseline-v1")
        self.assertEqual(self.mutations, [], "a different explicit profile adopted the partial environment")
        self.assertEqual(self.saved_lifecycle(), interrupted)
        self.main("upgrade", "--resource-profile", "large-v1")
        self.assertEqual(self.config()["resource_profile"], "large-v1")
        self.assertEqual(self.config()["image"], self.image)
        self.assertEqual(self.config()["owner"], json.loads(before["files"]["config.json"])["owner"])
        self.assertEqual((self.state / "app.env").read_bytes(), target_environment)
        self.assertEqual(self.volumes, before["volumes"])
        for name, item in before["containers"].items():
            if not name.endswith("-app"):
                self.assertEqual(self.containers[name], item)
        for name in ("pg.env", "s3.env", "s3.curl"):
            self.assertEqual((self.state / name).read_bytes(), before["files"][name])
        credential_lines = [line for line in before["files"]["app.env"].splitlines()
                            if not line.startswith(b"GIT_RS_BLAME_")]
        for line in credential_lines:
            self.assertIn(line, target_environment.splitlines())
        self.main("up")
        self.assertTrue(self.app()["State"]["Running"])
        self.assertNotEqual(self.app()["Id"], before["containers"]["git-rs-playground-app"]["Id"])
        self.assertEqual(self.app()["HostConfig"], {
            "Memory": 6442450944, "MemorySwap": 6442450944,
            "NanoCpus": 3000000000, "PidsLimit": 96})

    def test_image_only_upgrade_cannot_adopt_unknown_or_partial_resource_settings(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        environment = self.state / "app.env"
        original = environment.read_text()
        # App absence alone is not authorization to reconcile a partially written
        # target. Only an explicit known profile selection can authorize recovery.
        del self.containers["git-rs-playground-app"]
        initial = self.saved_lifecycle()
        for value in ("unknown-v2", "large-v1"):
            with self.subTest(profile=value):
                self.restore_lifecycle(initial)
                environment.write_text(original.replace("GIT_RS_BLAME_RESOURCE_PROFILE=baseline-v1",
                                                       "GIT_RS_BLAME_RESOURCE_PROFILE=" + value))
                before = self.saved_lifecycle()
                self.mutations.clear()
                with self.assertRaises(RuntimeError):
                    self.main("upgrade", "--image", "sha256:" + "d" * 64)
                self.assertEqual(self.mutations, [])
                self.assertEqual(self.saved_lifecycle(), before)

    def test_image_only_upgrade_rejects_unknown_saved_profile_without_adopting_it(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        config = self.config()
        config["resource_profile"] = "unknown-v2"
        (self.state / "config.json").write_text(json.dumps(config))
        before = self.saved_lifecycle()
        self.mutations.clear()
        with self.assertRaises(RuntimeError):
            self.main("upgrade", "--image", "sha256:" + "d" * 64)
        self.assertEqual(self.mutations, [])
        self.assertEqual(self.saved_lifecycle(), before)

    def test_legacy_image_upgrade_accepts_missing_profile_and_removes_obsolete_pack_threads(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        config = self.config()
        del config["resource_profile"]
        (self.state / "config.json").write_text(json.dumps(config))
        environment = self.state / "app.env"
        legacy = [line for line in environment.read_text().splitlines()
                  if not line.startswith("GIT_RS_BLAME_RESOURCE_PROFILE=")]
        legacy.append("GIT_RS_PACK_THREADS=2")
        environment.write_text("\n".join(legacy) + "\n")
        self.app()["Config"]["Env"] = legacy[:]
        before = self.saved_lifecycle()
        replacement = "sha256:" + "d" * 64
        self.main("upgrade", "--image", replacement)
        self.assertEqual(self.config()["resource_profile"], "baseline-v1")
        self.assertEqual(self.volumes, before["volumes"])
        for name in ("pg.env", "s3.env", "s3.curl"):
            self.assertEqual((self.state / name).read_bytes(), before["files"][name])
        self.assertNotIn("GIT_RS_PACK_THREADS=", environment.read_text())
        self.main("up")
        self.assertEqual(self.app()["Image"], replacement)
        self.assertEqual(self.app()["HostConfig"]["Memory"], 2415919104)
        self.assertIn("GIT_RS_BLAME_RESOURCE_PROFILE=baseline-v1", self.app()["Config"]["Env"])

    def test_failed_migration_keeps_selected_profile_credentials_and_dependency_volumes(self):
        def fail_migration(*args, **kwargs):
            if args[:2] == ("docker", "run") and args[-1] == "migrate":
                raise subprocess.CalledProcessError(1, ["docker", "run"])
            return self.docker(*args, **kwargs)

        with patch.object(PLAYGROUND, "run", side_effect=fail_migration):
            with self.assertRaises(subprocess.CalledProcessError):
                self.main("up", "--image", self.image, "--resource-profile", "large-v1")
        self.assertNotIn("git-rs-playground-app", self.containers)
        self.assertEqual(self.config()["resource_profile"], "large-v1")
        credentials = (self.state / "app.env").read_bytes()
        volumes = self.volumes.copy()
        self.main("up")
        self.assertEqual(self.volumes, volumes)
        self.assertEqual((self.state / "app.env").read_bytes(), credentials)
        self.assertIn("GIT_RS_BLAME_RESOURCE_PROFILE=large-v1", self.app()["Config"]["Env"])

    def test_foreign_stopped_service_blocks_upgrade_without_replacing_app_or_state(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        self.containers["git-rs-playground-s3"]["Config"]["Labels"][PLAYGROUND.LABEL] = "foreign"
        before = (self.state / "app.env").read_bytes()
        self.mutations.clear()
        with self.assertRaisesRegex(RuntimeError, "Refusing foreign"):
            self.main("upgrade", "--resource-profile", "large-v1")
        self.assertEqual(self.mutations, [])
        self.assertIn("git-rs-playground-app", self.containers)
        self.assertEqual((self.state / "app.env").read_bytes(), before)

    def test_legacy_missing_profile_restarts_as_baseline_without_rewriting_state(self):
        self.main("up", "--image", self.image)
        self.main("stop")
        config = self.config()
        del config["resource_profile"]
        path = self.state / "config.json"
        path.write_text(json.dumps(config))
        environment = self.state / "app.env"
        environment.write_text("\n".join(line for line in environment.read_text().splitlines()
                                        if not line.startswith("GIT_RS_BLAME_RESOURCE_PROFILE=")) + "\n")
        self.app()["Config"]["Env"] = environment.read_text().splitlines()
        before = path.read_bytes()
        self.main("up")
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.app()["HostConfig"]["Memory"], 2415919104)
        self.assertTrue(self.app()["State"]["Running"])


class PackageContracts(unittest.TestCase):
    def test_export_copies_and_hashes_the_probe_and_refuses_stale_runtime(self):
        spec = importlib.util.spec_from_file_location(
            "vm_release", Path(SPEC.origin).parents[2] / "tools/vm-release.py")
        release = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(release)
        image = "sha256:" + "b" * 64
        revision = "a" * 40
        inspected_revision = revision

        def command(args, timeout=20):
            if args[0] == "git":
                return revision if args[-2:] == ["rev-parse", "HEAD"] else ""
            if args[1:3] == ["image", "inspect"]:
                return json.dumps([{"Id": image, "Os": "linux", "Architecture": "amd64",
                                    "Config": {"Labels": {"io.git-rs.role": "runtime",
                                        "org.opencontainers.image.revision": inspected_revision}}}])
            if args[1:3] == ["image", "save"]:
                Path(args[args.index("--output") + 1]).write_bytes(b"synthetic image archive")
                return ""
            raise AssertionError("unexpected package process")

        with tempfile.TemporaryDirectory() as directory, patch.object(release, "command", side_effect=command):
            destination = Path(directory) / "package"
            release.export(image, destination)
            relative = "examples/playground/metrics_freshness.py"
            copied = (destination / relative).read_bytes()
            self.assertEqual(copied, (release.ROOT / relative).read_bytes())
            manifest = json.loads((destination / "release.json").read_text())
            self.assertEqual(manifest["files"][relative], {
                "sha256": hashlib.sha256(copied).hexdigest(), "bytes": len(copied)})
            self.assertIn(hashlib.sha256(copied).hexdigest() + "  " + relative + "\n",
                          (destination / "SHA256SUMS").read_text())
            self.assertIn("Python 3.10", (destination / "README.md").read_text())
            self.assertEqual(manifest["image"]["id"], image)
            with self.assertRaises(FileExistsError):
                release.export(image, destination)
            inspected_revision = "c" * 40
            refused = Path(directory) / "stale"
            with self.assertRaisesRegex(ValueError, "role/revision"):
                release.export(image, refused)
            self.assertFalse(refused.exists())


if __name__ == "__main__":
    unittest.main()
