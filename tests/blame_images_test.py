#!/usr/bin/env python3
"""Image handoff identity and exact cgroup ownership regressions, without Docker."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
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

    def bundle(self, oci=False, targets=images.TARGETS, *, partial_index=False):
        ids = {}
        with tarfile.open(self.directory / "images.tar", "w") as stream:
            rows = []
            for target in targets:
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
                for target in (targets[-1:] if partial_index else targets):
                    data = json.dumps({"schemaVersion": 2, "config": {"digest": ids[target]},
                                       "layers": []}).encode()
                    digest = hashlib.sha256(data).hexdigest()
                    header = tarfile.TarInfo("blobs/sha256/" + digest)
                    header.size = len(data)
                    stream.addfile(header, io.BytesIO(data))
                    if not partial_index:
                        ids[target] = "sha256:" + digest
                    descriptors.append({"digest": "sha256:" + digest})
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
            images.load(SimpleNamespace(directory=self.directory, commit=self.commit, env_file=env_file, bundle="final"))
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
            images.load(SimpleNamespace(directory=self.directory, commit=self.commit, env_file=env_file, bundle="final"))
        self.assertIn(manifest["images"]["test"], env_file.read_text())

    def test_classic_save_by_id_keeps_every_config_even_with_partial_oci_index(self):
        manifest = self.bundle(oci=True, partial_index=True)
        env_file = self.directory / "environment"
        args = SimpleNamespace(directory=self.directory, commit=self.commit, env_file=env_file, bundle="final")
        with patch.object(images, "output", side_effect=lambda argv, **kw:
                          self.commit if argv[0] == "git" else argv[-1]), \
                patch.object(images, "run") as run, patch("sys.stdout", new=io.StringIO()):
            images.load(args)
            self.assertIn(manifest["images"]["test"], env_file.read_text())
            self.assertEqual(run.call_count, 1)
            # A missing OCI descriptor cannot be interpreted as an OCI ID.
            manifest["images"]["test"] = "sha256:" + "f" * 64
            (self.directory / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "archive image IDs differ"):
                images.load(args)
            self.assertEqual(run.call_count, 1)

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
                                                     env_file=env_file, bundle="final"))
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
            return subprocess.CompletedProcess(argv, 0, owned, "")

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

    def test_cleanup_confirms_a_worker_removed_concurrently_by_force_rm(self):
        owned = "a" * 64
        containers = {owned: self.slice}

        def output(argv, **kwargs):
            return "\n".join(containers)

        def run(argv, **kwargs):
            if argv[1] == "inspect":
                return subprocess.CompletedProcess(argv, 0, json.dumps([
                    {"Id": key, "HostConfig": {"CgroupParent": value}}
                    for key, value in containers.items()]), "")
            del containers[owned]
            error = "Error response from daemon: removal of container " + owned + " is already in progress\n"
            result = subprocess.CompletedProcess(argv, 1, "", error)
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, argv, stderr=error)
            return result

        with patch.object(images, "output", side_effect=output), patch.object(images, "run", side_effect=run):
            images.cleanup(self.slice)
        self.assertEqual(containers, {})

    def test_cleanup_does_not_accept_a_removal_permission_failure(self):
        owned = "a" * 64

        def run(argv, **kwargs):
            if argv[1] == "inspect":
                return subprocess.CompletedProcess(argv, 0, json.dumps([
                    {"Id": owned, "HostConfig": {"CgroupParent": self.slice}}]), "")
            result = subprocess.CompletedProcess(argv, 1, "", "permission denied")
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, argv, stderr=result.stderr)
            return result

        with patch.object(images, "output", return_value=owned), patch.object(images, "run", side_effect=run):
            with self.assertRaisesRegex(ValueError, "worker removal failed"):
                images.cleanup(self.slice)

    def test_successful_remove_without_actual_drain_expires_the_original_budget(self):
        owned = "a" * 64

        def run(argv, **kwargs):
            if argv[1] == "inspect":
                return subprocess.CompletedProcess(argv, 0, json.dumps([
                    {"Id": owned, "HostConfig": {"CgroupParent": self.slice}}]), "")
            return subprocess.CompletedProcess(argv, 0, owned, "")

        with patch.object(images, "output", return_value=owned), patch.object(images, "run", side_effect=run), \
                patch.object(images.time, "monotonic", side_effect=[0, 1, 2, 3, 4, 21]), \
                patch.object(images.time, "sleep"):
            with self.assertRaisesRegex(ValueError, "cleanup absolute deadline expired"):
                images.cleanup(self.slice)

    def test_build_uses_supported_legacy_limits_and_refuses_missing_backend_flags(self):
        args = SimpleNamespace(unit="unit", slice=self.slice, directory=self.directory,
                               target="test", tag="local/fixture:test", azure_archive=True, prepared=None)
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
                             ("--cpu-quota", "180000"), ("--memory", "3712m"),
                             ("--memory-swap", "3712m"), ("--cpuset-cpus", "2,3")]:
            self.assertEqual(argv[argv.index(flag) + 1], value)
        self.assertEqual([value for flag, value in zip(argv, argv[1:]) if flag == "--build-arg"],
                         ["VCS_REF=" + self.commit, "GIT_RS_CI_AZURE_ARCHIVE=1"])
        with patch.object(images.gate, "verify_unit"), patch.object(images.gate, "checkout_identity"), \
                patch.object(images, "output", return_value="unsupported"), patch.object(images, "run") as run:
            with self.assertRaisesRegex(ValueError, "does not support"):
                images.build(args)
            run.assert_not_called()

    def test_acquisition_archive_cannot_substitute_for_compilation_or_final_images(self):
        self.bundle(oci=True, targets=images.INPUT_TARGETS)
        input_args = SimpleNamespace(directory=self.directory, commit=self.commit, env_file=None, bundle="inputs")
        final_env = self.directory / "environment"
        with patch.object(images, "output", side_effect=lambda argv, **kw:
                          self.commit if argv[0] == "git" else argv[-1]), \
                patch.object(images, "run") as run, patch("sys.stdout", new=io.StringIO()):
            images.load(input_args)
            with self.assertRaisesRegex(ValueError, "wrong image targets"):
                images.load(SimpleNamespace(directory=self.directory, commit=self.commit,
                                             env_file=None, bundle="compiled"))
            with self.assertRaisesRegex(ValueError, "wrong image targets"):
                images.load(SimpleNamespace(directory=self.directory, commit=self.commit,
                                             env_file=final_env, bundle="final"))
        self.assertEqual(run.call_count, 1)
        self.assertFalse(final_env.exists())

    def test_resumed_builder_uses_the_correct_stage_inventory(self):
        manifest = self.bundle(targets=images.INPUT_TARGETS)
        compiled = {key: manifest["images"][key] for key in ("codec-base", "toolchain")}
        compiled["build"] = "sha256:" + "d" * 64
        args = SimpleNamespace(unit="unit", slice=self.slice, directory=self.directory,
                               prepared=self.directory, target="build", tag="local/fixture:build", azure_archive=False)
        help_text = "--file --cgroup-parent --cpu-period --cpu-quota --cpuset-cpus --memory-swap --force-rm --iidfile --network --target"
        tags, builds = {}, []

        def run(argv, **kw):
            if argv[:3] == ["docker", "image", "tag"]:
                tags[argv[4]] = argv[3]
            elif argv[:2] == ["docker", "build"]:
                builds.append((argv, kw["input"]))
                Path(argv[argv.index("--iidfile") + 1]).write_text("sha256:" + "e" * 64)
            else:
                self.fail(f"unexpected command {argv}")

        def output(argv, **kw):
            return help_text if argv[-1] == "--help" else tags[argv[-1]]

        with patch.object(images.gate, "verify_unit"), \
                patch.object(images.gate, "checkout_identity", return_value=self.commit), \
                patch.object(images, "output", side_effect=output), patch.object(images, "run", side_effect=run), \
                patch.object(images, "cleanup"):
            images.build(args)
            args.target, args.tag = "test", "local/fixture:test"
            with self.assertRaisesRegex(ValueError, "wrong image targets"):
                images.build(args)
            (self.directory / "manifest.json").write_text(json.dumps(dict(manifest, images=compiled)))
            images.build(args)
            # A consumer on a different daemon must fail before attempting a build.
            args.target = "release"
            with patch.object(images, "output", side_effect=lambda argv, **kw:
                              help_text if argv[-1] == "--help" else "sha256:" + "0" * 64):
                with self.assertRaisesRegex(ValueError, "prepared image identity mismatch"):
                    images.build(args)
        [(compile_argv, compile_recipe), (argv, recipe)] = builds
        self.assertEqual(compile_argv[compile_argv.index("--target") + 1], "build")
        self.assertIn(b"RUN cargo build --release --offline --locked ", compile_recipe)
        self.assertEqual(argv[argv.index("--file") + 1], "-")
        self.assertNotIn(b"apt-get update", recipe)
        self.assertNotIn(b"cargo fetch", recipe)
        self.assertNotIn(b"RUN cargo build --release --offline --locked \\", recipe)
        self.assertIn(b"FROM build AS test-fixture-build", recipe)
        self.assertIn(b"apt-get --no-download autoremove", recipe)
        self.assertIn(b"FROM client-oracle AS test", recipe)
        self.assertIn(b"FROM runtime AS release", recipe)
        for target, identity in compiled.items():
            [tag] = [tag for tag, value in tags.items() if value == identity]
            self.assertIn(f"FROM {tag} AS {target}\n".encode(), recipe)


if __name__ == "__main__":
    unittest.main()
