#!/usr/bin/env python3
"""Fast public-controller observability checks; no Cargo, Docker or services."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cache_gate


class CacheGateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "tests").mkdir()
        debug = self.root / "target/debug"
        (debug / "examples").mkdir(parents=True)
        (debug / "deps").mkdir()
        for name in ("git-rs", "examples/cache-process", "deps/cache_contract-aa",
                     "deps/cache_coordinator-bb", "deps/cache_budget-cc"):
            path = debug / name
            path.write_text("#!/bin/sh\nexit 99\n")
            path.chmod(0o700)
        for script in ("derived_cache.py", "cache_storage_cost_test.py", "cache_storage_cost.py"):
            (self.root / "tests" / script).write_text("print('synthetic success')\n")
        self.invocation = "a" * 32
        self.directory = self.root / ".local" / ("cache-gate-" + self.invocation)

    def invoke(self, output):
        with patch.object(cache_gate, "ROOT", self.root), patch.dict(os.environ, {
                "INVOCATION_ID": self.invocation, "SYNTHETIC_TEST_TOKEN": "synthetic-redaction-marker"}), \
                patch("sys.argv", ["cache_gate.py", "run", "--target-dir", str(self.root / "target")]), \
                patch.object(cache_gate.signal, "signal"), \
                patch.object(cache_gate, "cleanup") as cleanup, contextlib.redirect_stdout(output):
            try:
                cache_gate.main()
            finally:
                cleanup.assert_called_once_with("git-rs.cache-gate=" + self.invocation)

    def test_failed_child_exposes_bounded_safe_tail_and_retains_full_log(self):
        (self.root / "tests/derived_cache.py").write_text(
            "import os, sys\n"
            "print('x' * 40000, flush=True)\n"
            "print('synthetic stdout assertion', flush=True)\n"
            "print('synthetic stderr assertion ::error::not-a-workflow-command ' + "
            "os.environ['SYNTHETIC_TEST_TOKEN'], file=sys.stderr, flush=True)\n"
            "sys.exit(7)\n")
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, "exited 7"):
            self.invoke(output)
        console = output.getvalue()
        self.assertIn("synthetic stdout assertion", console)
        self.assertIn("synthetic stderr assertion", console)
        self.assertIn("[REDACTED]", console)
        self.assertNotIn("synthetic-redaction-marker", console)
        excerpts = [line.removeprefix("cache-gate | ") for line in console.splitlines()
                    if line.startswith("cache-gate | ")]
        self.assertEqual(len(excerpts), 1)
        self.assertLessEqual(len(excerpts[0].encode()), cache_gate.FAILURE_EXCERPT_BYTES)
        self.assertFalse(any(line.startswith("::") for line in console.splitlines()))
        log = (self.directory / "0.log").read_text()
        self.assertGreater(len(log), 40000)
        self.assertIn("synthetic-redaction-marker", log)
        report = json.loads((self.directory / "report.json").read_text())
        self.assertFalse(report["completed"])
        self.assertEqual(report["stages"][0]["exception"], "RuntimeError")
        self.assertEqual(report["stages"][0]["script"], "derived_cache.py")
        self.assertFalse((self.directory / "1.log").exists())

    def test_success_retains_all_logs_without_failure_excerpt(self):
        output = io.StringIO()
        self.invoke(output)
        report = json.loads((self.directory / "report.json").read_text())
        self.assertTrue(report["completed"])
        self.assertEqual(len(report["stages"]), 3)
        self.assertTrue(all(stage["completed"] for stage in report["stages"]))
        for index in range(3):
            self.assertEqual((self.directory / f"{index}.log").read_text(), "synthetic success\n")
        self.assertNotIn("cache-gate | ", output.getvalue())


if __name__ == "__main__":
    unittest.main()
