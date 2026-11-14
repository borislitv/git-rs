#!/usr/bin/env python3
"""Lifecycle evidence and exact cgroup drain contracts without BPF or Docker."""
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import runtime_exec as trace


class ExecEvidenceContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="git-rs-checkout-")
        self.addCleanup(temporary.cleanup)
        self.local = Path(temporary.name) / ("a" * 32)
        self.local.mkdir(mode=0o700)
        self.observer = self.local / "observer"
        self.observer.mkdir(mode=0o700)
        self.images = {role: "sha256:" + digit * 64 for role, digit in zip(("runtime", "fixture", "oracle"), "123")}
        self.backend, self.client = "b" * 64, "c" * 64
        self.group = "/git-rs-blame-synthetic123.slice"
        self.rows = [f"C\t1\t{self.group}/owned.service", f"C\t2\t{self.group}/docker-{self.backend}.scope",
                     f"C\t3\t{self.group}/docker-{self.client}.scope", f"E\t1\t11\t{self.local}/exec-probe\ttrue",
                     "E\t2\t12\t/usr/local/bin/git-rs\tgit-rs",
                     "E\t3\t13\t/usr/lib/git-core/git-remote-http\tgit-remote-http", "Z"]
        self.events = []
        for identity, image in ((self.backend, "runtime"), (self.client, "oracle")):
            self.events.append({"Type": "container", "Action": "create", "Actor": {"ID": identity, "Attributes": {
                trace.runtime.gate.LABEL[:-1]: self.local.name, "image": self.images[image]}}})
        (self.observer / "exec.stderr").write_text("")
        (self.observer / "docker.stderr").write_text("")

    def inspect(self, phase="integration"):
        (self.observer / "exec.trace").write_text("\n".join(self.rows) + "\n")
        (self.observer / "docker.trace").write_text("\n".join(map(json.dumps, self.events)) + "\n")
        # Evidence parsing runs on all CI accounts. Root-file ownership is a
        # separate filesystem boundary, covered by the real read tests below.
        with patch.object(trace, "private"), patch.object(trace, "read", side_effect=lambda path, **_: path.read_text()):
            return trace.inspect_trace(self.local, self.group, self.images, phase)

    def test_checkout_directory_is_not_native_git_and_only_bound_backend_execs_count(self):
        result = self.inspect()
        self.assertEqual(result["external_native_execs"], 1)
        self.assertEqual(result["bound_backend_programs"], ["git-rs"])
        self.assertTrue(result["probe_seen"])

    def test_a_single_short_native_helper_in_the_backend_or_controller_invalidates_the_gate(self):
        for group in (1, 2):
            with self.subTest(group=group):
                self.rows.insert(-1, f"E\t{group}\t14\t/usr/lib/git-core/git-upload-pack\tgit-upload-pack")
                with self.assertRaisesRegex(ValueError, "outside a bound external"):
                    self.inspect()
                self.rows.pop(-2)

    def test_oracle_ssh_bridge_is_not_server_execution_coverage(self):
        self.rows[4] = "E\t3\t12\t/usr/local/bin/git-rs\tgit-rs"
        with self.assertRaisesRegex(ValueError, "coverage missing"):
            self.inspect()

    def test_descriptor_reexec_of_the_actual_ssh_session_is_identified(self):
        self.rows.insert(-1, "E\t2\t14\t/proc/self/fd/6\tsshd-session")
        self.assertEqual(self.inspect()["bound_backend_programs"], ["git-rs", "sshd-session"])

    def test_container_runtime_descriptor_bootstrap_is_separate_from_backend_work(self):
        self.rows.insert(-1, "E\t2\t14\t/proc/self/fd/6\trunc")
        result = self.inspect()
        self.assertEqual(result["container_runtime_execs"], 1)
        self.assertEqual(result["bound_backend_programs"], ["git-rs"])
        self.rows[4] = "E\t2\t12\t/proc/self/fd/6\trunc"
        with self.assertRaisesRegex(ValueError, "coverage missing"):
            self.inspect()

    def test_runtime_name_without_the_observed_descriptor_path_is_not_a_bootstrap(self):
        self.rows.insert(-1, "E\t2\t14\t/usr/local/bin/runc\trunc")
        with self.assertRaisesRegex(ValueError, "unexpected backend executable"):
            self.inspect()

    def test_descriptor_native_git_is_rejected_even_behind_an_allowed_requested_name(self):
        original = list(self.rows)
        for executable in ("/proc/self/fd/6", "/dev/fd/7", "/usr/local/bin/git-rs"):
            with self.subTest(executable=executable):
                self.rows = list(original)
                self.rows.insert(-1, f"E\t2\t14\t{executable}\tgit")
                with self.assertRaisesRegex(ValueError, "outside a bound external"):
                    self.inspect()

    def test_descriptor_unknown_backend_and_invalid_kernel_names_are_rejected(self):
        original = list(self.rows)
        for name in ("unknown-worker", "", "a" * 256, "path/sshd-session", ".", ".."):
            with self.subTest(name=name):
                self.rows = list(original)
                self.rows.insert(-1, f"E\t2\t14\t/proc/self/fd/6\t{name}")
                with self.assertRaises(ValueError):
                    self.inspect()

    def test_descriptor_client_execution_does_not_supply_backend_coverage(self):
        self.rows[4] = "E\t3\t12\t/proc/self/fd/6\tgit-rs"
        with self.assertRaisesRegex(ValueError, "coverage missing"):
            self.inspect()

    def test_istio_requires_actual_runtime_execution_as_well_as_native_client_work(self):
        self.rows[4] = "E\t3\t12\t/usr/local/bin/git-rs\tgit-rs"
        with self.assertRaisesRegex(ValueError, "coverage missing"):
            self.inspect("istio")

    def test_an_unknown_backend_program_and_a_neighbor_container_id_are_rejected(self):
        original = list(self.rows)
        self.rows[4] = "E\t2\t12\t/usr/local/bin/renamed-worker\trenamed-worker"
        with self.assertRaisesRegex(ValueError, "unexpected backend"):
            self.inspect()
        self.rows = original
        self.rows[2] = f"C\t3\t{self.group}/docker-{self.client}00.scope"
        with self.assertRaisesRegex(ValueError, "outside a bound external"):
            self.inspect()

    def test_kubernetes_short_lived_containers_join_the_actual_imported_image(self):
        fixture = self.local / "fixture"
        fixture.mkdir(mode=0o700)
        bound = {"docker_id": self.images["runtime"], "reference": "runtime.fixture.invalid/runtime@sha256:" + "d" * 64,
                 "cri_id": "sha256:" + "e" * 64}
        self.events = self.events[1:]
        self.rows[1] = f"C\t2\t{self.group}/pods/kubepods/{self.backend}"
        record = {"id": self.backend, "role": "runtime", "image_id": bound["cri_id"], "image": bound}
        path = fixture / "cri-containers.jsonl"
        path.write_text(json.dumps(record) + "\n")
        for phase in ("kubernetes", "istio"):
            self.assertEqual(self.inspect(phase)["bound_backend_programs"], ["git-rs"])
        record["image_id"] = "sha256:" + "f" * 64
        path.write_text(json.dumps(record) + "\n")
        with self.assertRaisesRegex(ValueError, "image identity differs"):
            self.inspect("kubernetes")

    def test_lost_events_missing_cgroups_and_missing_end_marker_never_pass(self):
        (self.observer / "exec.stderr").write_text("lost 1 event\n")
        with self.assertRaisesRegex(ValueError, "lost events"):
            self.inspect()
        (self.observer / "exec.stderr").write_text("")
        self.rows.remove(self.rows[1])
        with self.assertRaisesRegex(ValueError, "no recorded cgroup"):
            self.inspect()
        self.rows.pop()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.inspect()

    def test_bpftrace_terminal_spacing_preserves_complete_execution_evidence(self):
        self.rows.extend(["", ""])
        result = self.inspect()
        self.assertTrue(result["probe_seen"])
        self.assertEqual(result["external_native_execs"], 1)
        self.assertEqual(result["bound_backend_programs"], ["git-rs"])

    def test_blank_records_before_end_and_records_after_end_are_not_decoration(self):
        original = list(self.rows)
        for fault in ("middle", "late-exec", "extra-padding"):
            with self.subTest(fault=fault):
                self.rows = list(original)
                if fault == "middle":
                    self.rows.insert(-1, "")
                elif fault == "late-exec":
                    self.rows.append("E\t2\t14\t/usr/local/bin/git-rs\tgit-rs")
                else:
                    self.rows.extend(["", "", ""])
                with self.assertRaises(ValueError):
                    self.inspect()


class FilesystemContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_trace_reads_require_regular_unlinked_bounded_files_and_exact_owner_mode(self):
        path = self.root / "trace"
        path.write_text("synthetic trace\n")
        path.chmod(0o600)
        self.assertEqual(trace.read(path, owner=os.getuid(), mode=0o600), "synthetic trace\n")
        with self.assertRaisesRegex(ValueError, "owner changed"):
            trace.read(path, owner=os.getuid() + 1)
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "mode changed"):
            trace.read(path, mode=0o600)
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            trace.read(link)
        pipe = self.root / "pipe"
        os.mkfifo(pipe)
        with self.assertRaisesRegex(ValueError, "invalid or oversized"):
            trace.read(pipe)
        with path.open("r+b") as output:
            output.truncate(trace.MAX_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "oversized"):
            trace.read(path)

    def test_drain_kills_only_the_recorded_pod_subtree_and_rechecks_population(self):
        group = "/git-rs-blame-synthetic123.slice"
        controller = group + "/owned.service"
        base = self.root / group.lstrip("/")
        service, pods, foreign = base / "owned.service", base / "pods", self.root / "foreign.slice"
        for directory in (service, pods, foreign):
            directory.mkdir(parents=True)
            (directory / "cgroup.events").write_text("populated 1\nfrozen 0\n")
        (base / "cgroup.procs").write_text("")
        original = trace.runtime.populated

        def kernel(directory):
            if directory == pods and (pods / "cgroup.kill").exists():
                (pods / "cgroup.events").write_text("populated 0\nfrozen 0\n")
            return original(directory)

        with patch.object(trace.runtime, "CGROUPS", self.root), patch.object(trace.runtime, "populated", side_effect=kernel):
            trace.drain({"group": group, "controller": controller}, time.monotonic() + 1)
            self.assertTrue(trace.runtime.aggregate_empty(group, controller))
        self.assertEqual((pods / "cgroup.kill").read_text(), "1\n")
        self.assertFalse((foreign / "cgroup.kill").exists())
        self.assertEqual((foreign / "cgroup.events").read_text(), "populated 1\nfrozen 0\n")


if __name__ == "__main__":
    unittest.main()
