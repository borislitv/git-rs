#!/usr/bin/env python3
"""Light gate guard/fault tests: no Docker, systemd, Git, Cargo or network."""
import hashlib
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import blame_gate as gate


class GateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def seal_manifest(self, bundle, manifest):
        manifest.pop("namespace_sha256", None)
        manifest["namespace_sha256"] = hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        path = bundle / "manifest.json"
        if path.exists():
            path.chmod(0o600)
        path.write_text(json.dumps(manifest))
        path.chmod(0o444)

    def bundle(self):
        bundle = self.root / "bundle"
        bundle.mkdir()
        (bundle / "bin").mkdir()
        binaries = {}
        for name in ("current", "fixture", "cache_process", "legacy", "legacy_process", *gate.SUITES):
            data = ("synthetic executable " + name).encode()
            path = bundle / "bin" / name
            path.write_bytes(data)
            path.chmod(0o555)
            binaries[name] = {"path": "bin/" + name, "sha256": hashlib.sha256(data).hexdigest()}
        manifest = {"format": 1, "current_commit": "e" * 40,
                    "legacy": {"repository": gate.LEGACY_REPOSITORY,
                               "commit": gate.LEGACY_COMMIT,
                               "cargo_manifest_sha256": "e" * 64,
                               "cargo_lock_sha256": "f" * 64},
                    "git_source": gate.GIT_SOURCE, "builder_packages": "same OS",
                    "runtime_packages": "same OS", "runtime_image": "sha256:" + "a" * 64,
                    "builder_image": "sha256:" + "b" * 64, "binaries": binaries,
                    "harness_sources": {name: "c" * 64 for name in gate.HARNESS_FILES},
                    "inventory": {suite: [gate.ROOT_RUNTIME] if suite == "blame_core"
                                  else ["actual_fixture_case"] for suite in gate.SUITES}}
        self.seal_manifest(bundle, manifest)
        (bundle / "bin").chmod(0o555)
        bundle.chmod(0o555)
        return bundle, manifest

    def cgroup(self, **changes):
        directory = self.root / "cgroup"
        directory.mkdir(exist_ok=True)
        values = {"memory.max": str(4 * 1024**3), "pids.max": "256",
                  "cpu.max": "200000 100000", "memory.swap.max": "0",
                  "cpuset.cpus.effective": "24-25"}
        values.update(changes)
        for name, value in values.items():
            (directory / name).write_text(value)
        return directory

    def test_missing_invocation_refuses_cleanup_before_any_external_call(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(gate, "output") as run:
            with self.assertRaisesRegex(ValueError, "INVOCATION_ID"):
                gate.cleanup()
            run.assert_not_called()

    def test_focused_pass_executes_shipped_release_paths(self):
        with patch.object(gate, "command") as run:
            gate.focused_gate()
        argv = run.call_args.args[0]
        self.assertEqual(argv[argv.index("--binary") + 1], "/usr/local/bin/git-rs")
        self.assertEqual(argv[argv.index("--blame-fixture-binary") + 1],
                         "/usr/local/bin/examples/blame-http-fixture")

    def test_root_source_corruption_credentials_match_source_s3_not_cache(self):
        class Fixture:
            endpoint = "http://127.0.0.1:8000"

            def __init__(self):
                self.owned = []

            def test_environment(self):
                return {"GIT_RS_CACHE_S3_ENDPOINT": self.endpoint,
                        "GIT_RS_CACHE_S3_ACCESS_KEY": "test-cache-access",
                        "GIT_RS_CACHE_S3_SECRET_KEY": "synthetic-cache-secret"}

            def container(self, memory, cpu, pids, arguments):
                self.owned.append((memory, cpu, pids, arguments))
                return "source-container"

            def port(self, container, port):
                self_case.assertEqual((container, port), ("source-container", "7070/tcp"))
                return "9000"

        self_case = self
        fixture = Fixture()
        with patch("derived_cache.run", return_value=SimpleNamespace(returncode=0)) as request:
            env = gate.root_environment(fixture)
        self.assertEqual(env["GIT_RS_S3_ENDPOINT"], "http://127.0.0.1:9000")
        self.assertEqual(env["GIT_RS_S3_ACCESS_KEY"], "test-access")
        self.assertEqual(env["GIT_RS_S3_SECRET_KEY"], "synthetic-secret-key")
        self.assertEqual(env["GIT_RS_CACHE_S3_ENDPOINT"], fixture.endpoint)
        self.assertEqual(env["GIT_RS_CACHE_S3_ACCESS_KEY"], "test-cache-access")
        self.assertEqual(env["GIT_RS_CACHE_S3_SECRET_KEY"], "synthetic-cache-secret")
        self.assertEqual(len(fixture.owned), 1)
        memory, cpu, pids, argv = fixture.owned[0]
        self.assertEqual((memory, cpu, pids), ("256m", .25, 32))
        self.assertIn("ROOT_ACCESS_KEY_ID=test-access", argv)
        self.assertIn("ROOT_SECRET_ACCESS_KEY=synthetic-secret-key", argv)
        self.assertIn("test-access:synthetic-secret-key", request.call_args.args[0])
        self.assertEqual(request.call_args.args[0][-1], "http://127.0.0.1:9000/source-test")

    def test_preparation_timeout_or_partial_create_cleans_only_owned_containers(self):
        token = "b" * 32
        label = "git-rs.blame-preparation=" + token
        env = {"BLAME_GATE_PREP_TOKEN": token,
               "BLAME_GATE_PREP_SLICE": "git-rs-blame-prep-12345678.slice"}
        for failure in (subprocess.TimeoutExpired("docker", 60),
                        subprocess.CalledProcessError(125, "docker"), KeyboardInterrupt()):
            containers = {"abcdef123456": "unrelated"}

            def daemon(argv, **kwargs):
                if argv[:2] == ["docker", "run"]:
                    # The daemon has created a container, but the client never gets its ID.
                    containers["123456abcdef"] = label
                    self.assertIn(label, argv)
                    self.assertEqual(argv[argv.index("--cgroup-parent") + 1], env["BLAME_GATE_PREP_SLICE"])
                    self.assertEqual(argv[argv.index("--memory-swap") + 1], "4g")
                    self.assertEqual(argv[argv.index("--cpuset-cpus") + 1], "24,25")
                    self.assertLessEqual(kwargs["timeout"], 900)
                    raise failure
                if argv[:2] == ["docker", "ps"]:
                    selected = argv[-1].removeprefix("label=")
                    return "\n".join(key for key, owner in containers.items() if owner == selected)
                raise AssertionError(argv)

            def remove(argv, **kwargs):
                self.assertEqual(argv[:4], ["docker", "rm", "-f", "-v"])
                for key in argv[4:]:
                    del containers[key]

            with self.subTest(failure=type(failure).__name__), patch.dict(os.environ, env), \
                    patch.object(gate, "output", side_effect=daemon), \
                    patch.object(gate, "command", side_effect=remove), \
                    patch.object(gate.os, "sched_getaffinity", return_value={24, 25}):
                with self.assertRaises(type(failure)):
                    gate.image_output("sha256:image", ["/bin/true"])
                self.assertEqual(containers, {"abcdef123456": "unrelated"})
                gate.cleanup_preparation(token)
                gate.cleanup_preparation(token)
                self.assertEqual(containers, {"abcdef123456": "unrelated"})

    def test_preparation_ownership_refuses_invalid_nonce_before_daemon_calls(self):
        for token in ("", "a" * 31, "A" * 32, "--all"):
            with self.subTest(token=token), patch.object(gate, "output") as query, \
                    patch.object(gate, "command") as remove:
                with self.assertRaisesRegex(ValueError, "nonce"):
                    gate.cleanup_preparation(token)
                query.assert_not_called()
                remove.assert_not_called()

    def test_preparation_requires_attested_parent_before_building(self):
        env = {"BLAME_GATE_PREP_TOKEN": "b" * 32, "INVOCATION_ID": "a" * 32}
        with patch.dict(os.environ, env, clear=True), patch.object(gate, "output") as external, \
                patch.object(gate, "prepare_bundle") as build:
            with self.assertRaisesRegex(ValueError, "slice"):
                gate.prepare(SimpleNamespace())
            external.assert_not_called()
            build.assert_not_called()

    def test_preparation_cargo_and_inspection_share_owned_runner(self):
        env = {"BLAME_GATE_PREP_TOKEN": "b" * 32,
               "BLAME_GATE_PREP_SLICE": "git-rs-blame-prep-12345678.slice"}
        launches = []

        def daemon(argv, **kwargs):
            if argv[:2] == ["docker", "ps"]:
                return ""
            launches.append(argv)
            return ""

        with patch.dict(os.environ, env), patch.object(gate, "output", side_effect=daemon), \
                patch.object(gate.os, "sched_getaffinity", return_value={24, 25}):
            gate.image_output("sha256:image", ["/bin/true"])
            gate.cargo_artifacts("sha256:builder", self.root, self.root / "target", ["build"])
        self.assertEqual(len(launches), 2)
        for argv in launches:
            self.assertIn("git-rs.blame-preparation=" + env["BLAME_GATE_PREP_TOKEN"], argv)
            self.assertEqual(argv[argv.index("--cgroup-parent") + 1], env["BLAME_GATE_PREP_SLICE"])

    def test_preparation_build_failure_still_reaps_daemon_owned_container(self):
        token = "b" * 32
        containers = {"abcdef123456": "unrelated", "123456abcdef": token}

        def query(argv, **kwargs):
            self.assertEqual(argv[-1], "label=git-rs.blame-preparation=" + token)
            return "\n".join(key for key, owner in containers.items() if owner == token)

        def remove(argv, **kwargs):
            for key in argv[4:]:
                del containers[key]

        with patch.dict(os.environ, {"BLAME_GATE_PREP_TOKEN": token}), \
                patch.object(gate, "verify_unit"), \
                patch.object(gate, "prepare_bundle", side_effect=RuntimeError("build failed")), \
                patch.object(gate, "output", side_effect=query), \
                patch.object(gate, "command", side_effect=remove):
            with self.assertRaisesRegex(RuntimeError, "build failed"):
                gate.prepare(SimpleNamespace())
        self.assertEqual(containers, {"abcdef123456": "unrelated"})

    def test_cleanup_removes_only_invocation_labeled_ids_and_rechecks(self):
        token = "a" * 32
        with patch.dict(os.environ, {"INVOCATION_ID": token}), \
                patch.object(gate, "output", side_effect=["123456abcdef", ""]) as query, \
                patch.object(gate, "command") as remove:
            gate.cleanup()
        expected = ["docker", "ps", "-aq", "--filter", "label=git-rs.blame-gate=" + token]
        self.assertEqual(query.call_count, 2)
        self.assertEqual(query.call_args_list[0].args[0], expected)
        self.assertEqual(query.call_args_list[1].args[0], expected)
        remove.assert_called_once_with(["docker", "rm", "-f", "-v", "123456abcdef"], timeout=20)

    def test_independent_cleanup_reaps_anonymous_volumes_but_preserves_named_shared_and_foreign_state(self):
        token = "a" * 32
        owned, foreign = "1" * 64, "2" * 64
        for label, cleanup in ((gate.LABEL + token, gate.cleanup),
                               (gate.PREPARATION_LABEL + token, lambda: gate.cleanup_preparation(token))):
            with self.subTest(label=label):
                containers = {owned: {"label": label, "volumes": {"anonymous", "named", "shared"}},
                              foreign: {"label": "foreign", "volumes": {"named", "shared", "foreign"}}}
                sentinel = copy.deepcopy(containers[foreign])
                volumes = {"anonymous", "named", "shared", "foreign"}
                removed = []

                def query(argv, **kwargs):
                    self.assertEqual(argv, ["docker", "ps", "-aq", "--filter", "label=" + label])
                    return "\n".join(key for key, value in containers.items() if value["label"] == label)

                def remove(argv, **kwargs):
                    self.assertEqual(kwargs["timeout"], 20)
                    self.assertEqual(argv[:3], ["docker", "rm", "-f"])
                    ids = [item for item in argv[3:] if item != "-v"]
                    self.assertEqual(ids, [owned])
                    for identity in ids:
                        mounts = containers.pop(identity)["volumes"]
                        removed.append(identity)
                        if "-v" in argv:
                            # Docker removes only anonymous volumes with no remaining users.
                            for volume in mounts - {"named"}:
                                if not any(volume in value["volumes"] for value in containers.values()):
                                    volumes.remove(volume)

                with patch.dict(os.environ, {"INVOCATION_ID": token}), \
                        patch.object(gate, "output", side_effect=query), \
                        patch.object(gate, "command", side_effect=remove):
                    cleanup()
                    cleanup()
                self.assertEqual(removed, [owned])
                self.assertEqual(containers, {foreign: sentinel})
                self.assertEqual(volumes, {"named", "shared", "foreign"})

    def test_failed_or_invalid_query_never_falls_through_to_removal(self):
        for result in (subprocess.TimeoutExpired("docker", 5), "--all", "123456abcdef --all"):
            with self.subTest(result=type(result).__name__), \
                    patch.dict(os.environ, {"INVOCATION_ID": "a" * 32}), \
                    patch.object(gate, "output", side_effect=result if isinstance(result, Exception)
                                 else [result]), patch.object(gate, "command") as remove:
                with self.assertRaises((ValueError, subprocess.TimeoutExpired)):
                    gate.cleanup()
                remove.assert_not_called()

    def test_cleanup_failure_is_not_success(self):
        with patch.dict(os.environ, {"INVOCATION_ID": "a" * 32}), \
                patch.object(gate, "output", side_effect=["123456abcdef", "123456abcdef"]), \
                patch.object(gate, "command"):
            with self.assertRaisesRegex(ValueError, "remain"):
                gate.cleanup()

    def test_sealed_complete_bundle_is_accepted_without_running_binaries(self):
        bundle, expected = self.bundle()
        self.assertEqual(gate.load_manifest(bundle)["binaries"], expected["binaries"])

    def test_modified_binary_is_rejected_even_when_manifest_is_unchanged(self):
        bundle, _ = self.bundle()
        binary = bundle / "bin/legacy"
        binary.chmod(0o755)
        binary.write_bytes(b"substituted old executable")
        binary.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "digest"):
            gate.load_manifest(bundle)

    def test_coherent_wrong_legacy_commit_is_rejected(self):
        bundle, manifest = self.bundle()
        manifest["legacy"]["commit"] = "d" * 40
        self.seal_manifest(bundle, manifest)
        with self.assertRaisesRegex(ValueError, "legacy provenance"):
            gate.load_manifest(bundle)

    def test_missing_test_binary_and_mutable_manifest_are_rejected(self):
        bundle, manifest = self.bundle()
        manifest["binaries"].pop("blame_checkpoints")
        self.seal_manifest(bundle, manifest)
        with self.assertRaisesRegex(ValueError, "binary set"):
            gate.load_manifest(bundle)
        (bundle / "manifest.json").chmod(0o644)
        with self.assertRaisesRegex(ValueError, "immutable"):
            gate.load_manifest(bundle)

    def test_path_escape_and_namespace_corruption_are_rejected(self):
        bundle, manifest = self.bundle()
        manifest["binaries"]["legacy"]["path"] = "../outside"
        self.seal_manifest(bundle, manifest)
        with self.assertRaisesRegex(ValueError, "unsafe bundle path"):
            gate.load_manifest(bundle)
        manifest["namespace_sha256"] = "0" * 64
        path = bundle / "manifest.json"
        path.chmod(0o600)
        path.write_text(json.dumps(manifest))
        path.chmod(0o444)
        with self.assertRaisesRegex(ValueError, "namespace binding"):
            gate.load_manifest(bundle)

    def test_mismatched_build_runtime_os_is_rejected(self):
        bundle, manifest = self.bundle()
        manifest["runtime_packages"] = "different OS"
        self.seal_manifest(bundle, manifest)
        with self.assertRaisesRegex(ValueError, "OS package closure"):
            gate.load_manifest(bundle)

    def test_inventory_excludes_only_named_subprocess_helpers_and_has_no_fixed_count(self):
        self.assertEqual(gate.inventory("z_case: test\nartifact_process: test\na_case: test\n",
                                        "blame_artifacts"), ["a_case", "z_case"])
        self.assertEqual(gate.inventory("checkpoint_reuse_child: test\nreal_case: test\n",
                                         "blame_reuse"), ["real_case"])
        self.assertEqual(gate.inventory("runtime_support::runtime_child: test\n"
                                        "runtime_support::new_nested_contract: test\n", "blame_core"),
                         ["runtime_support::new_nested_contract"])
        with self.assertRaisesRegex(ValueError, "no ignored"):
            gate.inventory("artifact_process: test\n", "blame_artifacts")

    def test_cargo_recipe_uses_machine_reported_executables_and_separate_target(self):
        target = self.root / "old-target"
        message = json.dumps({"reason": "compiler-artifact", "target": {"name": "cache-process"},
                              "executable": "/target/debug/examples/cache-process"})
        with patch.object(gate, "preparation_output", return_value=message) as build:
            paths = gate.cargo_artifacts("sha256:builder", self.root, target,
                                         ["build", "--example", "cache-process"])
        self.assertEqual(paths, {"cache-process": target / "debug/examples/cache-process"})
        argv = build.call_args.args[0]
        self.assertIn("--locked", argv)
        self.assertIn("--message-format=json", argv)
        self.assertIn(str(target) + ":/target", argv)
        self.assertIn(str(self.root) + ":/source:ro", argv)

    def test_cargo_report_cannot_escape_or_ambiguously_replace_executables(self):
        for index, executable in enumerate(("/outside/bin", "/target/debug/git-rs")):
            message = json.dumps({"reason": "compiler-artifact", "target": {"name": "git-rs"},
                                  "executable": executable})
            report = message if index == 0 else message + "\n" + message
            with self.subTest(executable=executable), patch.object(gate, "preparation_output", return_value=report):
                with self.assertRaises(ValueError):
                    gate.cargo_artifacts("builder", self.root, self.root / str(index), ["build"])

    def test_exact_aggregate_caps_are_accepted(self):
        gate.verify_limits(self.cgroup())

    def test_overbudget_or_unbounded_aggregate_limits_are_rejected(self):
        faults = [{"memory.max": "max"}, {"memory.max": str(4 * 1024**3 + 1)},
                  {"pids.max": "257"}, {"pids.max": "max"}, {"cpu.max": "300000 100000"},
                  {"cpu.max": "max 100000"}, {"memory.swap.max": "1"},
                  {"cpuset.cpus.effective": "24-26"}]
        for fault in faults:
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                gate.verify_limits(self.cgroup(**fault))

    def test_systemd_duration_parser_rejects_infinity_and_preserves_900_bound(self):
        self.assertEqual(gate.seconds("15min"), 900)
        self.assertEqual(gate.seconds("40s"), 40)
        for value in ("infinity", "900", "garbage15min"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                gate.seconds(value)

    def test_unit_ownership_shutdown_and_timeout_guards(self):
        token = "a" * 32
        slice_name = "git-rs-blame-12345678.slice"
        unit = "git-rs-blame-12345678.service"
        parent = "/owned.slice"
        base = {"InvocationID": token, "ControlGroup": parent + "/" + unit,
                "Slice": slice_name, "RuntimeMaxUSec": "15min", "TimeoutStopUSec": "40s",
                "KillMode": "control-group", "ExecStopPost": "/usr/bin/python3 -B tests/blame_gate.py cleanup"}
        directory = self.cgroup()
        read = Path.read_text

        def read_host(path, *args, **kwargs):
            if str(path) == "/proc/self/cgroup":
                return "0::" + base["ControlGroup"] + "\n"
            if str(path).startswith("/sys/fs/cgroup/owned.slice/"):
                return read(directory / path.name, *args, **kwargs)
            return read(path, *args, **kwargs)

        for fault in ({}, {"InvocationID": "b" * 32}, {"RuntimeMaxUSec": "16min"},
                      {"TimeoutStopUSec": "41s"}, {"ExecStopPost": ""}, {"KillMode": "process"},
                      {"Slice": "unrelated.slice"}):
            props = dict(base, **fault)

            def external(argv, **_):
                if argv[0] == "docker":
                    return "systemd 2"
                if "--property=ControlGroup" in argv and "--value" in argv:
                    return parent
                return "\n".join(k + "=" + v for k, v in props.items())

            with self.subTest(fault=fault), patch.dict(os.environ, {"INVOCATION_ID": token}), \
                    patch.object(gate, "output", side_effect=external), patch.object(Path, "read_text", read_host):
                if fault:
                    with self.assertRaises(ValueError):
                        gate.verify_unit(unit, slice_name)
                else:
                    self.assertEqual(gate.verify_unit(unit, slice_name), token)

    def test_proxy_injects_parent_and_owner_without_touching_harness_arguments(self):
        env = {"INVOCATION_ID": "a" * 32, "GIT_RS_GATE_SLICE": "git-rs-blame-12345678.slice"}
        with patch.dict(os.environ, env), patch.object(gate.os, "execv") as execute:
            gate.docker_proxy(["run", "-d", "--memory", "256m", "pinned-fixture"])
        argv = execute.call_args.args[1]
        self.assertEqual(argv[:5], ["/usr/bin/docker", "run", "--cgroup-parent",
                                   env["GIT_RS_GATE_SLICE"], "--pull=never"])
        self.assertIn("git-rs.blame-gate=" + env["INVOCATION_ID"], argv)
        self.assertEqual(argv[-4:], ["-d", "--memory", "256m", "pinned-fixture"])

    def test_proxy_rejects_cgroup_escape_before_exec(self):
        env = {"INVOCATION_ID": "a" * 32, "GIT_RS_GATE_SLICE": "git-rs-blame-12345678.slice"}
        for flag in ("--privileged", "--cgroup-parent=other.slice", "--pid=host"):
            with self.subTest(flag=flag), patch.dict(os.environ, env), \
                    patch.object(gate.os, "execv") as execute:
                with self.assertRaisesRegex(ValueError, "escape"):
                    gate.docker_proxy(["run", flag, "fixture"])
                execute.assert_not_called()

    def test_docker_transport_is_pinned_to_the_attested_local_daemon(self):
        environment = {"DOCKER_HOST": "tcp://unrelated.invalid:2375", "DOCKER_CONTEXT": "remote"}
        with patch.dict(os.environ, environment), patch.object(gate.subprocess, "run") as run:
            gate.command(["docker", "info"])
        actual = run.call_args.kwargs["env"]
        self.assertEqual(actual["DOCKER_HOST"], "unix:///var/run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", actual)

    def test_failed_cleanup_cannot_publish_completed_report(self):
        bundle, _ = self.bundle()
        (self.root / ".local").mkdir()
        args = SimpleNamespace(bundle=bundle, unit="owned.service", slice="owned.slice",
                               manifest_sha256=hashlib.sha256((bundle / "manifest.json").read_bytes()).hexdigest())
        token = "a" * 32
        real_stat = Path.stat

        def stat(path, *a, **kw):
            if str(path) == "/var/run/docker.sock":
                return SimpleNamespace(st_gid=123)
            return real_stat(path, *a, **kw)

        with patch.object(gate, "ROOT", self.root), patch.object(gate, "verify_unit", return_value=token), \
                patch.object(gate.os, "sched_getaffinity", return_value={24, 25}), \
                patch.object(gate, "command"), patch.object(gate, "cleanup", side_effect=ValueError("cleanup failed")), \
                patch.object(Path, "stat", stat):
            with self.assertRaisesRegex(ValueError, "cleanup failed"):
                gate.run_gate(args)
        report = json.loads((self.root / (".local/blame-gate-" + token) / "report.json").read_text())
        self.assertFalse(report["completed"])
        self.assertTrue(report["work_completed"])


class RuntimeDaemon:
    """Stateful Docker boundary: running allocations and removal acknowledgements."""
    token = "a" * 32
    outer = "1" * 64
    sentinel = "2" * 64
    child = "3" * 64
    slice = "git-rs-blame-runtime12345678.slice"

    def __init__(self, fault=None):
        self.fault = fault
        self.namespace = os.readlink("/proc/self/ns/pid")
        self.containers = {
            self.outer: {"Id": self.outer, "Name": "/git-rs-blame-" + self.token,
                         "State": {"Running": True},
                         "Config": {"Hostname": self.outer[:12], "Labels": {gate.LABEL[:-1]: self.token},
                                    "Env": ["INVOCATION_ID=" + self.token, "GIT_RS_GATE_SLICE=" + self.slice]},
                         "HostConfig": {"NanoCpus": 1000000000, "CgroupParent": self.slice}},
            self.sentinel: {"Id": self.sentinel, "Name": "/unowned-sentinel",
                            "State": {"Running": True}, "Config": {"Labels": {}},
                            "HostConfig": {"NanoCpus": 50000000}},
        }
        self.preserved = copy.deepcopy(self.containers[self.sentinel])
        self.observations = []
        self.start_observation = None

    def fail(self, point):
        if self.fault == point or isinstance(self.fault, tuple) and point in self.fault:
            raise subprocess.TimeoutExpired("docker " + point, 5)

    def command(self, argv, **kwargs):
        assert 0 < kwargs["timeout"] <= 300
        result = ""
        operation = argv[1]
        if operation == "inspect":
            matches = [value for key, value in self.containers.items()
                       if key == argv[-1] or value["Name"] == "/" + argv[-1]]
            if not matches:
                raise subprocess.CalledProcessError(1, argv)
            result = json.dumps(matches)
        elif operation == "ps":
            selector = argv[-1]
            if selector.startswith("name=^/"):
                name = selector.removeprefix("name=^").removesuffix("$")
                result = "\n".join(key for key, value in self.containers.items() if value["Name"] == name)
            else:
                assert selector.startswith("id=")
                result = "\n".join(key for key in self.containers if key == selector[3:])
        elif operation == "exec":
            assert argv == ["docker", "exec", self.outer, "/usr/bin/readlink", "/proc/self/ns/pid"]
            result = self.namespace
        elif operation == "update":
            assert argv[-1] == self.outer, "never update a discovered/unowned container"
            restoring = argv[-2] == "1"
            self.fail("restore-before" if restoring else "donate-before")
            if restoring:
                assert self.child not in self.containers, "CPU restored before actual removal acknowledgement"
            self.containers[self.outer]["HostConfig"]["NanoCpus"] = 1000000000 if restoring else 250000000
            self.fail("restore-after" if restoring else "donate-after")
        elif operation == "create":
            self.fail("create-before")
            labels = dict(argv[index + 1].split("=", 1) for index, item in enumerate(argv) if item == "--label")
            environment = [argv[index + 1] for index, item in enumerate(argv) if item == "--env"]
            assert argv[argv.index("--cpus") + 1] == "1"
            assert argv[argv.index("--memory") + 1] == "256m"
            assert argv[argv.index("--memory-swap") + 1] == "256m"
            assert argv[argv.index("--pids-limit") + 1] == "32"
            assert argv[argv.index("--volumes-from") + 1] == self.outer + ":ro"
            self.containers[self.child] = {
                "Id": self.child, "Name": "/" + argv[argv.index("--name") + 1], "State": {"Running": False},
                "Config": {"Labels": labels, "Env": environment},
                "HostConfig": {"NanoCpus": 1000000000, "Memory": 256 * 1024**2,
                               "MemorySwap": 256 * 1024**2, "PidsLimit": 32, "CgroupParent": self.slice}}
            self.fail("create-after")  # Daemon committed, client never received ID.
            result = "malformed ID" if self.fault == "create-bad-id" else self.child
        elif operation == "start":
            assert argv[-1] == self.child
            self.containers[self.child]["State"]["Running"] = True
            self.start_observation = copy.deepcopy(self.containers)
            self.check_allowance()
            if self.fault == "unowned-child":
                self.containers[self.child]["Config"]["Labels"] = {}
            if self.fault == "outer-replaced":
                self.containers[self.outer]["Name"] = "/changed-owner"
            self.fail("start-timeout")  # Child is still alive, not just an exception.
            if self.fault == "interrupted":
                raise KeyboardInterrupt()
            if self.fault == "start-exit":
                self.containers[self.child]["State"]["Running"] = False
                raise subprocess.CalledProcessError(101, argv)
            self.containers[self.child]["State"]["Running"] = False
        elif operation == "rm":
            assert argv[2:4] == ["-f", "-v"] and argv[-1] == self.child
            self.fail("remove-timeout")
            if self.fault != "false-removal-ack":
                del self.containers[self.child]
        else:
            raise AssertionError(argv)
        self.check_allowance()
        self.observations.append(copy.deepcopy(self.containers))
        return SimpleNamespace(stdout=result, returncode=0)

    def check_allowance(self):
        pair = [value for key, value in self.containers.items() if key != self.sentinel]
        assert sum(value["HostConfig"]["NanoCpus"] for value in pair if value["State"]["Running"]) <= 1250000000
        assert self.containers[self.sentinel] == self.preserved


class RuntimeHandoffTests(unittest.TestCase):
    def run_case(self, daemon):
        with patch.dict(os.environ, {"INVOCATION_ID": daemon.token, "GIT_RS_GATE_SLICE": daemon.slice,
                                     "HOSTNAME": daemon.outer[:12]}), patch.object(gate, "command", side_effect=daemon.command):
            return gate.run_root_runtime({"runtime_image": "sha256:" + "f" * 64},
                                         Path("/gate/bin/blame_core"), {}, None)

    def test_runtime_handoff_preserves_budget_and_nonowned_state_through_acknowledged_removal(self):
        daemon = RuntimeDaemon()
        self.assertEqual(self.run_case(daemon).returncode, 0)
        during = daemon.start_observation
        self.assertEqual(during[daemon.outer]["HostConfig"]["NanoCpus"], 250000000)
        self.assertEqual(during[daemon.child]["HostConfig"]["NanoCpus"], 1000000000)
        self.assertEqual(set(daemon.containers), {daemon.outer, daemon.sentinel})
        self.assertEqual(daemon.containers[daemon.outer]["HostConfig"]["NanoCpus"], 1000000000)
        self.assertEqual(daemon.containers[daemon.sentinel], daemon.preserved)

    def test_create_update_and_running_failures_reap_actual_child_before_restoring(self):
        for fault in ("donate-before", "donate-after", "create-before", "create-after", "create-bad-id",
                      "start-timeout", "start-exit", "interrupted"):
            with self.subTest(fault=fault):
                daemon = RuntimeDaemon(fault)
                with self.assertRaises((subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError, KeyboardInterrupt)):
                    self.run_case(daemon)
                self.assertEqual(set(daemon.containers), {daemon.outer, daemon.sentinel})
                self.assertEqual(daemon.containers[daemon.outer]["HostConfig"]["NanoCpus"], 1000000000)

    def test_unacknowledged_or_unowned_child_refuses_restoration_and_keeps_failure_visible(self):
        for fault in ("remove-timeout", "false-removal-ack", "unowned-child"):
            with self.subTest(fault=fault):
                daemon = RuntimeDaemon(fault)
                with self.assertRaises((subprocess.TimeoutExpired, ValueError)):
                    self.run_case(daemon)
                self.assertIn(daemon.child, daemon.containers)
                self.assertEqual(daemon.containers[daemon.outer]["HostConfig"]["NanoCpus"], 250000000)
                self.assertEqual(daemon.containers[daemon.sentinel], daemon.preserved)

    def test_restore_failure_is_reported_after_child_absence_and_never_changes_wrong_outer(self):
        for fault, expected in (("restore-before", 250000000), ("restore-after", 1000000000),
                                ("outer-replaced", 250000000)):
            with self.subTest(fault=fault):
                daemon = RuntimeDaemon(fault)
                with self.assertRaises((subprocess.TimeoutExpired, ValueError)):
                    self.run_case(daemon)
                self.assertNotIn(daemon.child, daemon.containers)
                self.assertEqual(daemon.containers[daemon.outer]["HostConfig"]["NanoCpus"], expected)

    def test_work_failure_and_failed_removal_both_survive_without_unsafe_restoration(self):
        daemon = RuntimeDaemon(("start-timeout", "remove-timeout"))
        with self.assertRaises(BaseExceptionGroup) as raised:
            self.run_case(daemon)
        self.assertEqual(len(raised.exception.exceptions), 2)
        self.assertTrue(all(isinstance(error, subprocess.TimeoutExpired) for error in raised.exception.exceptions))
        self.assertTrue(daemon.containers[daemon.child]["State"]["Running"])
        self.assertEqual(daemon.containers[daemon.outer]["HostConfig"]["NanoCpus"], 250000000)

    def test_real_invocation_owner_namespace_and_original_cpu_are_required_before_any_update(self):
        for field in ("label", "invocation", "namespace", "shared-pids", "cpu", "slice", "name"):
            with self.subTest(field=field):
                daemon = RuntimeDaemon()
                outer = daemon.containers[daemon.outer]
                if field == "label": outer["Config"]["Labels"] = {}
                if field == "invocation": outer["Config"]["Env"] = []
                if field == "namespace": daemon.namespace = "pid:[foreign]"
                if field == "shared-pids": outer["HostConfig"]["PidMode"] = "host"
                if field == "cpu": outer["HostConfig"]["NanoCpus"] = 500000000
                if field == "slice": outer["HostConfig"]["CgroupParent"] = "foreign.slice"
                if field == "name": outer["Name"] = "/foreign"
                expected = copy.deepcopy(daemon.containers)
                with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                    self.run_case(daemon)
                self.assertEqual(daemon.containers, expected)


if __name__ == "__main__":
    unittest.main()
