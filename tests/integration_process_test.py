#!/usr/bin/env python3
"""Regression coverage for process ownership at the procfs read boundary."""
import errno
import unittest
from unittest.mock import patch

from integration import process_owned


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


if __name__ == "__main__":
    unittest.main()
