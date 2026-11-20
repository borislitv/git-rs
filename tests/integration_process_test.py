#!/usr/bin/env python3
"""Regression coverage for process ownership at the procfs read boundary."""
import errno
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from integration import Fixture, process_owned


class ProcessOwnershipTests(unittest.TestCase):
    def test_disappearance_before_or_during_read_is_not_owned(self):
        for error in (FileNotFoundError(errno.ENOENT, "gone"),
                      ProcessLookupError(errno.ESRCH, "exited during read")):
            with self.subTest(error=type(error).__name__):
                with patch("integration.Path.read_text", side_effect=error):
                    self.assertFalse(process_owned({"pid": 123, "startTicks": 987}))

    def test_pid_reuse_is_not_the_original_worker(self):
        # Linux stat fields 1..22, including a closing parenthesis in comm.
        stat = "123 (worker name)) S 1 123 123 0 0 0 0 0 0 0 0 0 0 0 20 0 1 0 987\n"
        with patch("integration.Path.read_text", return_value=stat):
            self.assertTrue(process_owned({"pid": 123, "startTicks": 987}))
            self.assertFalse(process_owned({"pid": 123, "startTicks": 986}))

    def test_permission_error_is_not_reported_as_reaped(self):
        error = PermissionError(errno.EACCES, "procfs denied")
        with patch("integration.Path.read_text", side_effect=error):
            with self.assertRaises(PermissionError):
                process_owned({"pid": 123, "startTicks": 987})


class FixtureCleanupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.fixture = Fixture(Path(temporary.name), "/unused/git-rs")

    def test_relabelled_container_is_not_removed_or_reported_as_clean(self):
        fixture = self.fixture
        identifier = "d" * 64
        fixture.containers_started = True
        fixture.containers.append(identifier)

        def daemon(arguments, **_):
            if arguments[1] == "inspect":
                result = [{"Id": identifier, "Config": {"Labels": {"git-rs.integration-fixture": "foreign"}}}]
                return subprocess.CompletedProcess(arguments, 0, json.dumps(result).encode(), b"")
            self.assertEqual(arguments[1], "ps", "must not remove a foreign container")
            return subprocess.CompletedProcess(arguments, 0, b"", b"")

        with patch("integration.command", side_effect=daemon):
            with self.assertRaisesRegex(AssertionError, "foreign integration container"):
                fixture.close()
        self.assertNotIn("owned_containers_removed", fixture.results)
        self.assertEqual(fixture.results["status"], "failed")

    def test_one_failed_stop_does_not_skip_other_actual_owners(self):
        failed, healthy, log = Mock(), Mock(), Mock()
        failed.poll.return_value = healthy.poll.return_value = None
        failed.terminate.side_effect = OSError("synthetic stop failure")
        self.fixture.processes = [healthy, failed]
        self.fixture.logs = [log]
        with self.assertRaisesRegex(OSError, "synthetic stop failure"):
            self.fixture.close()
        healthy.terminate.assert_called_once()
        healthy.wait.assert_called_once_with(timeout=5)
        failed.wait.assert_called_once_with(timeout=5)
        log.close.assert_called_once()

    def test_cleanup_failure_preserves_the_foreground_error(self):
        process = Mock()
        process.poll.side_effect = PermissionError("synthetic ownership inspection failure")
        self.fixture.processes = [process]
        with self.assertRaisesRegex(RuntimeError, "foreground failure") as failure:
            try:
                raise RuntimeError("foreground failure")
            finally:
                self.fixture.close()
        process.wait.assert_called_once_with(timeout=5)
        self.assertIn("PermissionError", failure.exception.__notes__[0])
        self.assertEqual(self.fixture.results["status"], "failed")

    def test_failed_work_cannot_leave_a_passed_report_after_successful_cleanup(self):
        self.fixture.results["status"] = "passed"
        with self.assertRaisesRegex(RuntimeError, "foreground failure"):
            try:
                raise RuntimeError("foreground failure")
            finally:
                self.fixture.close()
        self.assertEqual(self.fixture.results["status"], "failed")


if __name__ == "__main__":
    unittest.main()
