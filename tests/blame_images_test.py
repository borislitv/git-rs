#!/usr/bin/env python3
"""Image handoff identity and exact cgroup ownership regressions, without Docker."""
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import blame_images as images


class ImageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.commit = "a" * 40
        self.slice = "git-rs-blame-images-fixture1234.slice"

    def bundle(self, oci=False):
        ids = {}
        with tarfile.open(self.directory / "images.tar", "w") as stream:
            rows = []
            for target in images.TARGETS:
                data = json.dumps({"fixture": target}).encode()
                digest = hashlib.sha256(data).hexdigest()
                name = "blobs/sha256/" + digest
                header = tarfile.TarInfo(name)
                header.size = len(data)
                stream.addfile(header, io.BytesIO(data))
                ids[target] = "sha256:" + digest
                rows.append({"Config": name, "Layers": [], "RepoTags": None})
            data = json.dumps(rows).encode()
            header = tarfile.TarInfo("manifest.json")
            header.size = len(data)
            stream.addfile(header, io.BytesIO(data))
            if oci:
                descriptors = []
                for target in images.TARGETS:
                    data = json.dumps({"schemaVersion": 2, "config": {"digest": ids[target]},
                                       "layers": []}).encode()
                    digest = hashlib.sha256(data).hexdigest()
                    header = tarfile.TarInfo("blobs/sha256/" + digest)
                    header.size = len(data)
                    stream.addfile(header, io.BytesIO(data))
                    ids[target] = "sha256:" + digest
                    descriptors.append({"digest": ids[target]})
                data = json.dumps({"schemaVersion": 2, "manifests": descriptors}).encode()
                header = tarfile.TarInfo("index.json")
                header.size = len(data)
                stream.addfile(header, io.BytesIO(data))
        archive = self.directory / "images.tar"
        manifest = {"format": 2, "commit": self.commit, "images": ids,
                    "dockerfile_sha256": images.gate.digest(images.ROOT / "Dockerfile"),
                    "archive_sha256": images.gate.digest(archive), "archive_bytes": archive.stat().st_size}
        (self.directory / "manifest.json").write_text(json.dumps(manifest))
        return manifest

    def test_valid_archive_binds_all_ids_and_only_then_exports_environment(self):
        manifest = self.bundle()
        env_file = self.directory / "environment"
        calls = []

        def output(argv, **kwargs):
            return self.commit if argv[0] == "git" else argv[-1]

        with patch.object(images, "output", side_effect=output), \
                patch.object(images, "run", side_effect=lambda argv, **kw: calls.append(argv)), \
                patch("sys.stdout", new=io.StringIO()):
            images.load(SimpleNamespace(directory=self.directory, commit=self.commit, env_file=env_file))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:3], ["docker", "image", "load"])
        self.assertEqual(env_file.read_text(), "BLAME_GATE_BUILDER_IMAGE=" + manifest["images"]["test-fixture-build"]
                         + "\nBLAME_GATE_IMAGE=" + manifest["images"]["test"]
                         + "\nGIT_RS_TEST_RUNTIME_IMAGE=" + manifest["images"]["release"]
                         + "\nGIT_RS_TEST_RUNTIME_FIXTURE_IMAGE=" + manifest["images"]["test-server"]
                         + "\nGIT_RS_TEST_REVISION=" + self.commit + "\n")

    def test_containerd_manifest_ids_are_verified_without_changing_declared_identity(self):
        manifest = self.bundle(oci=True)
        env_file = self.directory / "environment"
        with patch.object(images, "output", side_effect=lambda argv, **kw:
                          self.commit if argv[0] == "git" else argv[-1]), \
                patch.object(images, "run"), patch("sys.stdout", new=io.StringIO()):
            images.load(SimpleNamespace(directory=self.directory, commit=self.commit, env_file=env_file))
        self.assertIn(manifest["images"]["test"], env_file.read_text())

    def test_corrupt_archive_foreign_commit_or_declared_id_never_loads_or_exports(self):
        for fault in ("checksum", "commit", "image", "dockerfile"):
            with self.subTest(fault=fault):
                manifest = self.bundle()
                if fault == "checksum":
                    with (self.directory / "images.tar").open("ab") as stream:
                        stream.write(b"tampered")
                elif fault == "commit":
                    manifest["commit"] = "b" * 40
                elif fault == "image":
                    manifest["images"]["test"] = "sha256:" + "f" * 64
                else:
                    manifest["dockerfile_sha256"] = "f" * 64
                (self.directory / "manifest.json").write_text(json.dumps(manifest))
                env_file = self.directory / "environment"
                with patch.object(images, "output", return_value=self.commit), \
                        patch.object(images, "run") as run:
                    with self.assertRaises(ValueError):
                        images.load(SimpleNamespace(directory=self.directory, commit=self.commit,
                                                    env_file=env_file))
                run.assert_not_called()
                self.assertFalse(env_file.exists())

    def test_cleanup_removes_only_exact_slice_workers_and_is_repeatable(self):
        owned, foreign = "a" * 64, "b" * 64
        containers = {owned: self.slice, foreign: self.slice + ".unrelated"}

        def output(argv, **kwargs):
            return "\n".join(containers)

        def run(argv, **kwargs):
            if argv[1] == "inspect":
                return SimpleNamespace(returncode=0, stdout=json.dumps([
                    {"Id": key, "HostConfig": {"CgroupParent": value}}
                    for key, value in containers.items()]), stderr="")
            self.assertEqual(argv, ["docker", "rm", "-f", "-v", owned])
            del containers[owned]

        with patch.object(images, "output", side_effect=output), patch.object(images, "run", side_effect=run):
            images.cleanup(self.slice)
            images.cleanup(self.slice)
        self.assertEqual(containers, {foreign: self.slice + ".unrelated"})

    def test_invalid_slice_and_daemon_error_fail_closed_without_removal(self):
        with patch.object(images, "output") as output:
            with self.assertRaises(ValueError):
                images.cleanup("system.slice")
            output.assert_not_called()
        with patch.object(images, "output", return_value="a" * 64), \
                patch.object(images, "run", return_value=SimpleNamespace(
                    returncode=1, stdout="[]", stderr="permission denied")) as run:
            with self.assertRaisesRegex(ValueError, "inspection failed"):
                images.cleanup(self.slice)
            self.assertEqual(run.call_count, 1)

    def test_build_uses_supported_legacy_limits_and_refuses_missing_backend_flags(self):
        args = SimpleNamespace(unit="unit", slice=self.slice, directory=self.directory,
                               target="test", tag="local/fixture:test")
        argv_seen = []

        def run(argv, **kwargs):
            argv_seen.append(argv)
            Path(argv[argv.index("--iidfile") + 1]).write_text("sha256:" + "c" * 64)

        help_text = "--cgroup-parent --cpu-period --cpu-quota --cpuset-cpus --memory-swap --force-rm --iidfile --network --target"
        with patch.object(images.gate, "verify_unit"), patch.object(images.gate, "checkout_identity", return_value=self.commit), \
                patch.object(images, "output", return_value=help_text), patch.object(images, "run", side_effect=run), \
                patch.object(images, "cleanup"), patch.object(os, "sched_getaffinity", return_value={2, 3}):
            images.build(args)
        argv = argv_seen[0]
        for flag, value in [("--cgroup-parent", self.slice), ("--network", "host"),
                             ("--build-arg", "VCS_REF=" + self.commit),
                            ("--cpu-quota", "180000"), ("--memory", "3712m"),
                            ("--memory-swap", "3712m"), ("--cpuset-cpus", "2,3")]:
            self.assertEqual(argv[argv.index(flag) + 1], value)
        with patch.object(images.gate, "verify_unit"), patch.object(images.gate, "checkout_identity"), \
                patch.object(images, "output", return_value="unsupported"), patch.object(images, "run") as run:
            with self.assertRaisesRegex(ValueError, "does not support"):
                images.build(args)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
