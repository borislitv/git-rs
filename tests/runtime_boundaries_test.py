#!/usr/bin/env python3
"""Synthetic filesystem and stalled-pipe contracts; no Docker daemon required."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location(
    "runtime_boundaries", Path(__file__).resolve().parents[1] / "tools/check-runtime-boundaries.py")
RUNTIME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNTIME)


def filesystem(extra=(), *, pax_headers=None):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", pax_headers=pax_headers) as archive:
        for path, data, mode, link in [
            ("usr/local/bin/git-rs", b"\x7fELFsynthetic-server", 0o755, None),
            ("usr/share/git-rs/packages.tsv", b"libc6\t1\nlibssl3t64\t1\n", 0o644, None),
            *extra,
        ]:
            member = tarfile.TarInfo(path)
            member.mode = mode
            if link is not None:
                member.type = tarfile.SYMTYPE
                member.linkname = link
                archive.addfile(member)
            else:
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
    stream.seek(0)
    return stream


class RuntimeInventory(unittest.TestCase):
    def test_inventory_hashes_actual_elf_and_preserves_non_path_executables(self):
        report = RUNTIME.scan(filesystem([
            ("opt/hidden-helper", b"\x7fELFsynthetic-helper", 0o644, None),
            ("usr/local/bin/entrypoint", b"#!/bin/sh\nexec /usr/local/bin/git-rs\n", 0o755, None),
        ]))
        entries = {row["path"]: row for row in report["executables"]}
        self.assertEqual(set(entries), {"usr/local/bin/git-rs", "opt/hidden-helper", "usr/local/bin/entrypoint"})
        self.assertTrue(entries["opt/hidden-helper"]["elf"])
        self.assertEqual(entries["usr/local/bin/git-rs"]["sha256"], hashlib.sha256(b"\x7fELFsynthetic-server").hexdigest())

    def test_native_helpers_are_rejected_even_outside_path_or_through_links(self):
        for path, link in [
            ("opt/hidden/git-upload-pack", None), ("usr/share/git-core/templates/HEAD", None),
            ("root/.git/config", None), ("opt/scalar", None),
            ("usr/local/bin/innocent", "/opt/private/git-receive-pack"),
        ]:
            with self.subTest(path=path, link=link), self.assertRaisesRegex(ValueError, "forbidden runtime path"):
                RUNTIME.scan(filesystem([(path, b"data", 0o644, link)]))

    def test_package_inventory_cannot_hide_native_git_by_renaming_a_binary(self):
        for package in ["git", "git-man", "gitk", "libgit2-1.9"]:
            with self.subTest(package=package), self.assertRaisesRegex(ValueError, "native Git package"):
                RUNTIME.scan(filesystem([("usr/share/git-rs/packages.tsv", f"{package}\t1\n".encode(), 0o644, None)]))

    def test_hidden_metadata_is_counted_and_bounded_before_parsing(self):
        with patch.object(RUNTIME, "MAX_FILES", 2):
            with self.assertRaisesRegex(ValueError, "inventory bound"):
                RUNTIME.scan(filesystem(pax_headers={"comment": "extra hidden header"}))
        with patch.object(RUNTIME, "MAX_TAR_METADATA", 1024):
            with self.assertRaisesRegex(ValueError, "metadata exceeded its byte bound"):
                RUNTIME.scan(filesystem(pax_headers={"comment": "x" * 4096}))

    def test_successive_global_headers_share_one_metadata_bound(self):
        headers = b"".join(tarfile.TarInfo.create_pax_global_header({key: "x" * 600}) for key in ("one", "two"))
        with patch.object(RUNTIME, "MAX_TAR_METADATA", 1024):
            with self.assertRaisesRegex(ValueError, "global tar metadata exceeded"):
                RUNTIME.scan(io.BytesIO(headers + filesystem().getvalue()))

    def test_pax_sparse_maps_are_not_expanded_before_the_member_policy(self):
        stream = filesystem().getvalue()
        # A real GNU sparse 1.0 member: zero map entries in one padded map block.
        # This is intentionally outside the Git-free filesystem input contract.
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            with tarfile.open(fileobj=io.BytesIO(stream), mode="r:") as original:
                for member in original:
                    archive.addfile(member, original.extractfile(member))
            member = tarfile.TarInfo("opt/sparse")
            member.size = 512
            member.pax_headers = {"GNU.sparse.major": "1", "GNU.sparse.minor": "0",
                                  "GNU.sparse.realsize": "0"}
            archive.addfile(member, io.BytesIO(b"0\n".ljust(512, b"\0")))
        output.seek(0)
        with self.assertRaisesRegex(ValueError, "unsupported sparse"):
            RUNTIME.scan(output)

    def test_tar_end_does_not_skip_the_remaining_export_wire(self):
        class TrailingFailure(io.BytesIO):
            def read(self, size=-1):
                if self.tell() == len(self.getvalue()):
                    raise TimeoutError("export did not close after tar end")
                return super().read(size)

        with self.assertRaisesRegex(TimeoutError, "after tar end"):
            RUNTIME.scan(TrailingFailure(filesystem().getvalue()))

    def test_stalled_pipe_observes_the_absolute_export_deadline(self):
        reader, writer = os.pipe()
        try:
            with os.fdopen(reader, "rb") as pipe:
                with self.assertRaises(TimeoutError):
                    RUNTIME.ExportStream(pipe, seconds=0).read(512)
        finally:
            os.close(writer)

    def test_export_stream_preserves_fragmented_bytes_and_real_eof(self):
        reader, writer = os.pipe()
        os.write(writer, b"archive")
        os.close(writer)
        with os.fdopen(reader, "rb") as pipe:
            stream = RUNTIME.ExportStream(pipe, seconds=5)
            self.assertEqual(stream.read(3), b"arc")
            self.assertEqual(stream.read(512), b"hive")
            self.assertEqual(stream.read(512), b"")


class InventoryLifecycle(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / "inventory"
        self.image, self.revision = "sha256:" + "a" * 64, "b" * 40
        self.removed = False

    def inventory(self, *, invalid=False, removal_error=False, close_error=False):
        extra = [("opt/git-upload-pack", b"forbidden", 0o755, None)] if invalid else ()
        with tempfile.TemporaryFile() as source:
            source.write(filesystem(extra).getvalue())
            source.seek(0)

            class Pipe:
                def fileno(self):
                    return source.fileno()

                def close(self):
                    source.close()
                    if close_error:
                        raise OSError("export pipe close failed")

            exporter = Mock(stdout=Pipe())
            exporter.wait.return_value = 0
            exporter.poll.return_value = 0
            current = None

            def command(arguments, *, check=True):
                nonlocal current
                if arguments[:3] == ["docker", "image", "inspect"]:
                    image = {"Id": self.image, "Os": "linux", "Architecture": "amd64",
                             "Config": {"Labels": {"io.git-rs.role": "runtime",
                                                    "org.opencontainers.image.revision": self.revision}}}
                    return subprocess.CompletedProcess(arguments, 0, json.dumps([image]).encode(), b"")
                if arguments[:2] == ["docker", "create"]:
                    owner = arguments[arguments.index("--label") + 1].split("=", 1)[1]
                    current = {"Id": "c" * 64, "Image": self.image, "State": {"Running": False},
                               "Config": {"Labels": {RUNTIME.LABEL: owner}}}
                elif arguments[:3] == ["docker", "container", "inspect"]:
                    if current is None:
                        return subprocess.CompletedProcess(arguments, 1, b"", b"No such container")
                    return subprocess.CompletedProcess(arguments, 0, json.dumps([current]).encode(), b"")
                elif arguments[:2] == ["docker", "rm"]:
                    if removal_error:
                        raise OSError("owned container removal failed")
                    self.assertEqual(arguments[2], "c" * 64)
                    current, self.removed = None, True
                else:
                    self.fail("unexpected inventory boundary: " + repr(arguments))
                return subprocess.CompletedProcess(arguments, 0, b"", b"")

            with patch.object(RUNTIME, "command", side_effect=command), \
                    patch.object(RUNTIME.subprocess, "Popen", return_value=exporter) as process, \
                    patch("sys.stdout", new=io.StringIO()):
                RUNTIME.inventory(self.image, self.revision, self.directory)
                self.export_environment = process.call_args.kwargs["env"]

    def test_success_requires_owned_removal_and_a_private_report(self):
        with patch.dict(os.environ, DOCKER_HOST="tcp://docker.example.invalid:2376", DOCKER_CONTEXT="foreign"):
            self.inventory()
        report = self.directory / "report.json"
        self.assertEqual(report.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(report.read_text())["status"], "passed")
        self.assertTrue(json.loads(report.read_text())["cleanup"])
        self.assertTrue(self.removed)
        self.assertEqual(self.export_environment["DOCKER_HOST"], "unix:///var/run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", self.export_environment)

    def test_inventory_commands_cannot_use_an_inherited_remote_daemon(self):
        result = subprocess.CompletedProcess([], 0, b"[]", b"")
        with patch.dict(os.environ, DOCKER_HOST="tcp://docker.example.invalid:2376", DOCKER_CONTEXT="foreign"), \
                patch.object(RUNTIME.subprocess, "run", return_value=result) as process:
            RUNTIME.command(["docker", "image", "inspect", self.image])
        self.assertEqual(process.call_args.kwargs["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock")
        self.assertNotIn("DOCKER_CONTEXT", process.call_args.kwargs["env"])

    def test_cleanup_failure_preserves_the_invalid_filesystem_error(self):
        with self.assertRaisesRegex(ValueError, "forbidden runtime path") as caught:
            self.inventory(invalid=True, removal_error=True)
        self.assertIn("OSError", " ".join(caught.exception.__notes__))
        report = json.loads((self.directory / "report.json").read_text())
        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["cleanup"])
        self.assertEqual(report["cleanup_errors"], ["OSError"])
        self.assertFalse(self.removed)

    def test_pipe_close_failure_still_removes_the_owned_container(self):
        with self.assertRaisesRegex(OSError, "export pipe close"):
            self.inventory(close_error=True)
        self.assertTrue(self.removed)
        self.assertEqual(json.loads((self.directory / "report.json").read_text())["status"], "failed")

    def test_report_failure_keeps_the_foreground_error_and_cleanup_attempt(self):
        with patch.object(RUNTIME.tempfile, "mkstemp", side_effect=OSError("report unavailable")):
            with self.assertRaisesRegex(ValueError, "forbidden runtime path") as caught:
                self.inventory(invalid=True)
        self.assertTrue(self.removed)
        self.assertIn("OSError", " ".join(caught.exception.__notes__))

    def test_special_cleanup_journals_fail_without_waiting_for_a_fifo_writer(self):
        journal = self.root / "ownership.json"
        os.mkfifo(journal, 0o600)
        result = subprocess.run([sys.executable, "-B", str(Path(RUNTIME.__file__)), "--cleanup", str(journal)],
                                capture_output=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"not a private regular file", result.stderr)


if __name__ == "__main__":
    unittest.main()
