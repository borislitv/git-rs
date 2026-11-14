#!/usr/bin/env python3
"""Supervisor cleanup and completion contracts without Docker or systemd."""
import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import runtime_gate as runtime


class Daemon:
    token = "a" * 32
    cluster, controller, backend, foreign = (digit * 64 for digit in "1234")

    def __init__(self):
        self.containers = {}
        for identity, name, owner in ((self.cluster, runtime.cluster_name(self.token), self.token),
                                     (self.controller, "owned-controller", self.token),
                                     (self.backend, "owned-backend", self.token),
                                     (self.foreign, "unrelated", "b" * 32)):
            self.containers[identity] = {"Id": identity, "Name": "/" + name,
                                        "Config": {"Labels": {runtime.gate.LABEL[:-1]: owner}}}
        self.preserved = copy.deepcopy(self.containers[self.foreign])
        self.replaced = False

    def output(self, argv, **_):
        if argv[1] == "ps":
            if argv[-1].startswith("id="):
                identity = argv[-1].removeprefix("id=")
                return identity if identity in self.containers else ""
            return "\n".join(identity for identity, row in self.containers.items()
                             if row["Config"]["Labels"].get(runtime.gate.LABEL[:-1]) == self.token)
        assert argv[1] == "inspect"
        rows = [copy.deepcopy(self.containers[identifier]) for identifier in argv[2:]]
        if self.replaced:
            for row in rows:
                row["Config"]["Labels"][runtime.gate.LABEL[:-1]] = "b" * 32
        return json.dumps(rows)

    def command(self, argv, **_):
        assert argv[:4] == ["docker", "rm", "-f", "-v"]
        for identity in argv[4:]:
            assert identity != self.foreign
            del self.containers[identity]


class SupervisorContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.daemon = Daemon()
        for boundary in (patch.object(runtime, "DIRECTORIES", self.root),
                         patch.dict(os.environ, {"INVOCATION_ID": self.daemon.token}),
                         patch.object(runtime.gate, "output", side_effect=self.daemon.output),
                         patch.object(runtime.gate, "command", side_effect=self.daemon.command)):
            boundary.start()
            self.addCleanup(boundary.stop)
        self.local = self.root / self.daemon.token
        self.local.mkdir(mode=0o700)
        runtime.save_cluster(self.local, self.daemon.token, self.daemon.cluster)
        fixture = self.local / "fixture"
        fixture.mkdir(mode=0o700)
        maintenance = fixture / "maintenance-fixture"
        maintenance.mkdir(mode=0o700)
        self.journal = maintenance / "maintenance-fixture.json"
        self.journal.write_text("synthetic exact-owned journal")

    def test_cleanup_reaps_servers_and_cluster_before_credentials_preserving_unrelated_state(self):
        def namespace(path, *, destroyed_cluster):
            self.assertEqual(path, self.journal)
            self.assertEqual(destroyed_cluster, self.daemon.cluster)
            self.assertEqual(set(self.daemon.containers), {self.daemon.foreign})
            path.unlink()

        with patch.object(runtime, "confirm_drain"), patch.object(runtime, "cleanup_namespace", side_effect=namespace):
            runtime.cleanup()
        self.assertEqual(self.daemon.containers, {self.daemon.foreign: self.daemon.preserved})
        self.assertFalse(self.journal.exists())

    def test_interrupted_credential_cleanup_retries_from_the_durable_removed_cluster_identity(self):
        with patch.object(runtime, "confirm_drain"), \
                patch.object(runtime, "cleanup_namespace", side_effect=RuntimeError("credential cleanup interrupted")):
            with self.assertRaisesRegex(RuntimeError, "credential cleanup interrupted"):
                runtime.cleanup()
        self.assertEqual(set(self.daemon.containers), {self.daemon.foreign})
        self.assertTrue(self.journal.exists())
        with patch.object(runtime, "confirm_drain"), \
                patch.object(runtime, "cleanup_namespace", side_effect=lambda path, **_: path.unlink()):
            runtime.cleanup()
        self.assertEqual(self.daemon.containers, {self.daemon.foreign: self.daemon.preserved})

    def test_default_cleanup_budget_accepts_a_fractional_monotonic_origin(self):
        # Subtracting the sampled origin from its rounded sum can exceed 35s.
        with patch.object(runtime.time, "monotonic", return_value=29.019), \
                patch.object(runtime, "confirm_drain"), \
                patch.object(runtime, "cleanup_namespace", side_effect=lambda path, **_: path.unlink()):
            runtime.cleanup()
        self.assertEqual(self.daemon.containers, {self.daemon.foreign: self.daemon.preserved})
        self.assertFalse(self.journal.exists())

    def test_changed_owner_is_not_removed_after_label_discovery(self):
        before = copy.deepcopy(self.daemon.containers)
        self.daemon.replaced = True
        with self.assertRaisesRegex(ValueError, "ownership changed"):
            runtime.cleanup()
        self.assertEqual(self.daemon.containers, before)
        self.assertTrue(self.journal.exists())

    def test_expired_cleanup_budget_preserves_unremoved_resources_and_journals(self):
        before = copy.deepcopy(self.daemon.containers)
        with patch.object(runtime.time, "monotonic", side_effect=[0, runtime.CLEANUP_SECONDS]):
            with self.assertRaisesRegex(ValueError, "cleanup absolute deadline expired"):
                runtime.cleanup()
        self.assertEqual(self.daemon.containers, before)
        self.assertTrue(self.journal.exists())

    def test_container_removal_alone_does_not_acknowledge_credential_cleanup(self):
        runtime.cleanup(containers_only=True)
        self.assertEqual(self.daemon.containers, {self.daemon.foreign: self.daemon.preserved})
        self.assertTrue(self.journal.exists())
        with self.assertRaises(FileNotFoundError):
            runtime.cleanup()
        self.assertTrue(self.journal.exists(), "actual-owner drain must precede credential cleanup")

    def test_failed_actual_owner_drain_preserves_credentials_for_independent_retry(self):
        with patch.object(runtime, "confirm_drain", side_effect=ValueError("actual owners still running")):
            with self.assertRaisesRegex(ValueError, "still running"):
                runtime.cleanup()
        self.assertTrue(self.journal.exists())
        self.assertEqual(self.daemon.containers, {self.daemon.foreign: self.daemon.preserved})

    def test_an_existing_recorded_cluster_that_lost_its_label_is_not_removed_or_claimed_destroyed(self):
        self.daemon.containers[self.daemon.cluster]["Config"]["Labels"] = {}
        before = copy.deepcopy(self.daemon.containers[self.daemon.cluster])
        with self.assertRaisesRegex(ValueError, "unconfirmed|owner changed"):
            runtime.cleanup()
        self.assertEqual(self.daemon.containers[self.daemon.cluster], before)
        self.assertTrue(self.journal.exists())

    @contextmanager
    def phase(self, *, work_error=None, cleanup_error=None):
        # A fresh invocation owns a fresh output directory.
        token = "c" * 32
        args = SimpleNamespace(phase="integration", unit="owned.service", slice="owned.slice",
            revision="d" * 40, runtime_image="runtime", fixture_image="fixture", oracle_image="oracle",
            traced=True, deadline=time.monotonic() + 600)
        local = self.root / token
        local.mkdir(mode=0o700)
        ready = local / "trace-ready"
        ready.touch(mode=0o644)
        real_stat = Path.stat
        real_lstat = Path.lstat

        def stat(path, *a, **kw):
            return SimpleNamespace(st_gid=123) if str(path) == "/var/run/docker.sock" else real_stat(path, *a, **kw)

        def lstat(path, *a, **kw):
            if path == ready:
                return SimpleNamespace(st_mode=0o100644, st_uid=0)
            return real_lstat(path, *a, **kw)

        with patch.object(runtime, "observed_scope", return_value={"owner": token, "group": "/owned.slice"}), \
                patch.object(runtime, "inspect_image", side_effect=["sha256:" + digit * 64 for digit in "567"]), \
                patch.object(runtime.gate, "output", return_value="/owned.slice"), \
                patch.object(runtime.gate, "command", side_effect=work_error), patch.object(Path, "stat", stat), patch.object(Path, "lstat", lstat), \
                patch.object(runtime, "cleanup", side_effect=cleanup_error) as cleanup:
            yield args, local, cleanup

    def test_successful_work_with_failed_cleanup_cannot_publish_completion(self):
        with self.phase(cleanup_error=RuntimeError("cleanup unconfirmed")) as (args, local, _):
            with self.assertRaisesRegex(RuntimeError, "cleanup unconfirmed"):
                runtime.run(args)
        report = json.loads((local / "report.json").read_text())
        self.assertTrue(report["work_completed"])
        self.assertFalse(report["completed"])
        self.assertNotIn("cleanup_completed", report)
        self.assertEqual(report["cleanup_errors"], ["RuntimeError"])

    def test_cleanup_failure_preserves_the_foreground_error_and_private_report(self):
        original = ValueError("runtime work failed")
        with self.phase(work_error=original, cleanup_error=OSError("daemon unavailable")) as (args, local, cleanup):
            with self.assertRaises(ValueError) as raised:
                runtime.run(args)
            self.assertIs(raised.exception, original)
            cleanup.assert_called_once_with(containers_only=True)
        report = json.loads((local / "report.json").read_text())
        self.assertEqual(report["failure"], "ValueError")
        self.assertEqual(report["cleanup_errors"], ["OSError"])
        self.assertFalse(report["completed"])
        self.assertNotIn("containers_stopped", report)
        self.assertTrue(any("OSError" in note for note in original.__notes__))
        self.assertEqual((local / "report.json").stat().st_mode & 0o777, 0o600)

    def test_report_creation_failure_does_not_replace_the_work_error_or_skip_cleanup(self):
        original = ValueError("runtime work failed")
        with self.phase(work_error=original) as (args, local, cleanup), \
                patch.object(runtime.tempfile, "mkstemp", side_effect=OSError("no space")):
            with self.assertRaises(ValueError) as raised:
                runtime.run(args)
            self.assertIs(raised.exception, original)
            cleanup.assert_called_once_with(containers_only=True)
        self.assertTrue(any("OSError" in note for note in original.__notes__))
        self.assertFalse((local / "report.json").exists())

    def test_successful_work_is_not_successful_when_its_report_cannot_be_saved(self):
        with self.phase() as (args, local, cleanup), \
                patch.object(runtime.tempfile, "mkstemp", side_effect=OSError("no space")):
            with self.assertRaisesRegex(OSError, "no space"):
                runtime.run(args)
            cleanup.assert_called_once_with(containers_only=True)
        self.assertFalse((local / "report.json").exists())

    def test_an_untraced_entrypoint_cannot_start_a_runtime_phase(self):
        with self.assertRaisesRegex(ValueError, "successful-exec observer"):
            runtime.run(SimpleNamespace(traced=False))

    def test_istio_dispatch_keeps_native_clients_in_the_oracle_and_consumes_prepared_inputs(self):
        environment = {"INVOCATION_ID": self.daemon.token, "GIT_RS_TEST_RUNTIME_IMAGE": "sha256:" + "1" * 64,
                       "GIT_RS_TEST_ORACLE_IMAGE": "sha256:" + "2" * 64, "GIT_RS_TEST_REVISION": "a" * 40}
        with patch.dict(os.environ, environment), patch.object(runtime, "Path", return_value=self.root / "bin"), \
                patch.object(runtime.os, "execv", side_effect=RuntimeError("dispatch captured")) as dispatch:
            with self.assertRaisesRegex(RuntimeError, "dispatch captured"):
                runtime.inside(SimpleNamespace(phase="istio", cluster_container=self.daemon.cluster))
        executable, argv = dispatch.call_args.args
        self.assertEqual(executable, runtime.sys.executable)
        self.assertEqual(argv[2], "/opt/git-rs/tests/istio.py")
        for option, value in (("--cluster-container", self.daemon.cluster),
                              ("--image", environment["GIT_RS_TEST_RUNTIME_IMAGE"]),
                              ("--oracle-image", environment["GIT_RS_TEST_ORACLE_IMAGE"]),
                              ("--helm", "/usr/local/bin/helm"), ("--artifacts", "/opt/git-rs/istio-inputs")):
            self.assertEqual(argv[argv.index(option) + 1], value)
        self.assertNotIn("--prepare", argv)
        self.assertNotIn("--provision", argv)


class ObservedScopeContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.token = "f" * 32
        self.unit = "git-rs-blame-observer-fixture123.service"
        self.slice = "git-rs-blame-observer-fixture123.slice"
        self.group = "/" + self.slice
        self.controller = self.group + "/" + self.unit
        local = self.root / self.token
        local.mkdir()
        self.path = local / "unit-proof.json"
        self.proof = {"owner": self.token, "unit": self.unit, "slice": self.slice,
                      "group": self.group, "controller": self.controller}
        self.path.write_text(json.dumps(self.proof))
        self.path.chmod(0o644)
        self.owner = 0
        cgroup = self.root / "cgroups" / self.slice
        cgroup.mkdir(parents=True)
        for name, value in {"memory.max": str(4 * 1024**3), "pids.max": "256",
                            "cpu.max": "200000 100000", "memory.swap.max": "0",
                            "cpuset.cpus.effective": "24-25"}.items():
            (cgroup / name).write_text(value)
        self.cgroup = cgroup
        real_read_text, real_fstat = Path.read_text, os.fstat

        def read_text(path, *args, **kwargs):
            return "0::" + self.controller if str(path) == "/proc/self/cgroup" else real_read_text(path, *args, **kwargs)

        def metadata(descriptor):
            value = real_fstat(descriptor)
            return SimpleNamespace(st_uid=self.owner, st_mode=value.st_mode, st_size=value.st_size)

        # Simulate the root file-owner boundary on unprivileged CI accounts too.
        for boundary in (patch.object(runtime, "DIRECTORIES", self.root),
                         patch.object(runtime, "CGROUPS", self.root / "cgroups"),
                         patch.object(runtime.gate, "invocation", return_value=self.token),
                         patch.object(Path, "read_text", read_text),
                         patch.object(runtime.os, "fstat", side_effect=metadata),
                         patch.object(runtime.gate, "output", side_effect=AssertionError("system bus access"))):
            boundary.start()
            self.addCleanup(boundary.stop)

    def test_exact_root_proof_and_live_limits_work_without_system_bus_access(self):
        self.assertEqual(runtime.observed_scope(self.unit, self.slice), self.proof)

    def test_image_owned_or_replayed_proof_and_changed_limits_are_rejected(self):
        for fault in ("owner", "token", "controller", "unit", "mode", "limits"):
            with self.subTest(fault=fault):
                proof = dict(self.proof)
                self.owner = 10001 if fault == "owner" else 0
                self.path.chmod(0o600 if fault == "mode" else 0o644)
                (self.cgroup / "memory.swap.max").write_text("1" if fault == "limits" else "0")
                if fault == "token":
                    proof["owner"] = "e" * 32
                elif fault == "controller":
                    proof["controller"] += ".different"
                elif fault == "unit":
                    proof["unit"] += ".different"
                self.path.write_text(json.dumps(proof))
                with self.assertRaises(ValueError):
                    runtime.observed_scope(self.unit, self.slice)

    def test_linked_attestation_is_not_followed(self):
        target = self.path.with_name("replacement")
        self.path.rename(target)
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            runtime.observed_scope(self.unit, self.slice)


class CompiledInputContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.target = self.root / "compiled"
        deps = self.target / "debug/deps"
        deps.mkdir(parents=True)
        for name in runtime.CONTRACT_SUITES:
            path = deps / (name + "-a1b2")
            path.write_bytes(b"\x7fELFsynthetic-test-identity:" + name.encode())
            path.chmod(0o555)
        self.revision = "a" * 40
        self.sealed = self.root / "sealed"
        with patch.object(runtime.gate, "checkout_identity", return_value=self.revision):
            runtime.export_contracts(SimpleNamespace(target_dir=self.target, destination=self.sealed))

    def test_sealed_inputs_reject_a_changed_binary_and_revision(self):
        runtime.verify_contracts(self.sealed, self.revision)
        with self.assertRaisesRegex(ValueError, "inventory/revision"):
            runtime.verify_contracts(self.sealed, "b" * 40)
        binary = self.sealed / "debug/deps/maintenance-a1b2"
        binary.chmod(0o755)
        binary.write_bytes(b"\x7fELFchanged")
        with self.assertRaisesRegex(ValueError, "identity differs"):
            runtime.verify_contracts(self.sealed, self.revision)

    def test_linked_contract_input_is_not_accepted_as_the_sealed_elf(self):
        binary = self.sealed / "debug/deps/metadata-a1b2"
        binary.unlink()
        binary.symlink_to(self.target / "debug/deps/metadata-a1b2")
        with self.assertRaisesRegex(ValueError, "identity differs"):
            runtime.verify_contracts(self.sealed, self.revision)

    def test_sql_contracts_have_separate_databases_and_exact_discovered_invocations(self):
        observations, created, dropped = [], set(), set()
        inventories = {"metadata": ["first_contract", "second_contract"], "maintenance": ["queue_contract"],
                       "maintenance_leader": ["lock_contract"]}
        local = self.root / "sql"
        local.mkdir(mode=0o700)

        def output(argv, **_):
            if argv[:2] == ["docker", "create"]:
                return "1" * 64
            if argv[:2] == ["docker", "inspect"]:
                return json.dumps([{"NetworkSettings": {"Ports": {"5432/tcp": [{"HostPort": "54321"}]}}}])
            if argv[-2:] == ["--ignored", "--list"]:
                binary = argv[argv.index("--entrypoint") + 1]
                suite = Path(binary).name.split("-", 1)[0]
                return "\n".join(name + ": test" for name in inventories[suite])
            self.assertIn("FROM pg_stat_activity", argv[-1])
            return "0"

        def command(argv, **kwargs):
            if argv[:2] == ["docker", "start"]:
                return
            if "--exact" in argv:
                name = argv[argv.index("--exact") + 1]
                database = next(value.removeprefix("PGDATABASE=") for value in argv
                                if isinstance(value, str) and value.startswith("PGDATABASE="))
                self.assertIn(database, created - dropped)
                observations.append((name, database))
                kwargs["stdout"].write((f"test {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n").encode())
                kwargs["stdout"].flush()
            elif argv[-1].startswith("CREATE DATABASE "):
                database = argv[-1].split()[-1]
                self.assertNotIn(database, created)
                created.add(database)
            else:
                self.assertTrue(argv[-1].startswith("DROP DATABASE "))
                dropped.add(argv[-1].split()[-1])

        with patch.object(runtime.gate, "output", side_effect=output), \
                patch.object(runtime.gate, "command", side_effect=command), \
                patch.object(runtime.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=b"1")):
            results = runtime.serial_sql(SimpleNamespace(target_dir=self.sealed, slice="owned.slice"), local,
                                         "oracle", "a" * 32, "0,1", time.monotonic() + 60)
        self.assertEqual([name for name, _ in observations], ["first_contract", "second_contract", "queue_contract", "lock_contract"])
        self.assertEqual(len({database for _, database in observations}), 4)
        self.assertEqual(created, dropped)
        self.assertFalse((local / ".metadata-env").exists())
        self.assertEqual({name: row["executed"] for name, row in results.items()},
                         {"metadata": 2, "maintenance": 1, "maintenance_leader": 1})


if __name__ == "__main__":
    unittest.main()
