#!/usr/bin/env python3
"""Fast acceptance-scorer contracts; no Docker, Cargo or source corpus required."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cache_storage_cost import CostFixture, ResourceProbe, SourceProbe, distribution


class CostBudgetTests(unittest.TestCase):
    def setUp(self):
        self.fixture = object.__new__(CostFixture)
        self.fixture.budget = json.loads((Path(__file__).parent /
                                         "fixtures/cache-storage-budget.json").read_text())
        self.baseline = {"p99": 10}
        self.row = {
            "size": 16384, "mode": "all-pg", "source_unchanged": True,
            "source_errors": [], "source_latency_ms": {"count": 100, "p99": 10},
            "latency_ms": {"cold_write": {"max": 20}}, "lag_bytes": [0, 8192],
            "replica_catchup_seconds": 1,
            "wal_bytes": 1048576, "relation_growth_bytes": 1048576,
            "wire_bytes": {"pg": {"request": 70000, "response": 300000},
                           "s3": {"request": 0, "response": 0}},
            "storage_after": {"tables": [{"estimated_dead": 16}]},
            "primary_resources": [{"memory_headroom_fraction": .8}],
            "verified_digest_input_bytes": 294912,
            "payload_layout": {"rows": 4, "declared_bytes": 65536,
                               "inline_bytes": 65536, "object_rows": 0},
        }

    def test_generous_functional_limits_accept_both_storage_policies(self):
        self.fixture.score(self.row, self.baseline)
        self.row.update(mode="pg-s3", payload_layout={"rows": 4, "declared_bytes": 65536,
                                                     "inline_bytes": 0, "object_rows": 4})
        self.row["wire_bytes"]["s3"] = {"request": 70000, "response": 300000}
        self.fixture.score(self.row, self.baseline)

    def test_independently_bad_observations_are_rejected(self):
        scenarios = [
            ("source mutation", {"source_unchanged": False}, "authoritative"),
            ("source error", {"source_errors": ["timeout"]}, "source request"),
            ("source undersampled", {"source_latency_ms": {"count": 1, "p99": 10}}, "too few"),
            ("source tail", {"source_latency_ms": {"count": 100, "p99": 351}}, "source tail"),
            ("cache hang", {"latency_ms": {"cold_write": {"max": 10001}}}, "operation"),
            ("replica backlog", {"lag_bytes": [67108865]}, "replica lag"),
            ("replica replay timeout", {"replica_catchup_seconds": 11}, "final catchup"),
            ("WAL runaway", {"wal_bytes": 8912897}, "WAL amplification"),
            ("storage runaway", {"relation_growth_bytes": 8650753}, "relation amplification"),
            ("wire runaway", {"wire_bytes": {"pg": {"request": 20000000}, "s3": {"request": 0}}}, "wire amplification"),
            ("vacuum debt", {"storage_after": {"tables": [{"estimated_dead": 5001}]}}, "vacuum debt"),
            ("memory exhausted", {"primary_resources": [{"memory_headroom_fraction": .09}]}, "headroom"),
            ("payload lost", {"payload_layout": {"rows": 3, "declared_bytes": 65536}}, "payload set"),
            ("wrong route", {"payload_layout": {"rows": 4, "declared_bytes": 65536,
                                                "inline_bytes": 0, "object_rows": 4}}, "storage policy"),
            ("unexpected S3", {"wire_bytes": {"pg": {"request": 70000, "response": 300000},
                                             "s3": {"request": 1, "response": 0}}}, "transport"),
        ]
        for name, update, message in scenarios:
            with self.subTest(name=name):
                row = copy.deepcopy(self.row)
                row.update(update)
                with self.assertRaisesRegex(AssertionError, message):
                    self.fixture.score(row, self.baseline)

    def test_nearest_rank_quantiles_do_not_hide_single_tail(self):
        self.assertEqual(distribution(list(range(1, 101))),
                         {"p50": 50, "p95": 95, "p99": 99, "count": 100, "max": 100})
        self.assertEqual(distribution([7])["p99"], 7)
        with self.assertRaisesRegex(AssertionError, "empty"):
            distribution([])


class CostDiagnosticsTests(unittest.TestCase):
    def test_first_source_failure_preserves_bounded_actual_reply_without_samples(self):
        class Source:
            def ask(self, command):
                return "error synthetic-source-unavailable " + "x" * 4096

        observations = {}
        probe = SourceProbe(Source(), "source 0 0", observations)
        with probe:
            probe.thread.join(timeout=2)
            self.assertFalse(probe.thread.is_alive())
        self.assertEqual(probe.samples, [])
        self.assertIn("synthetic-source-unavailable", probe.errors[0])
        self.assertLessEqual(len(probe.errors[0]), 1100)
        with self.assertRaisesRegex(AssertionError, "empty latency sample"):
            distribution(probe.samples)
        self.assertEqual(observations.get("source_sample_count"), 0)
        self.assertEqual(observations.get("source_errors"), probe.errors)

    def test_monitor_error_does_not_replace_primary_failure(self):
        class Fixture:
            def lag(self):
                raise RuntimeError("synthetic monitor SQL failure")

        observations = {"lag_bytes": [], "primary_resources": []}
        probe = ResourceProbe(Fixture(), observations)
        with self.assertRaisesRegex(AssertionError, "synthetic publication failure"):
            with probe:
                probe.thread.join(timeout=2)
                self.assertFalse(probe.thread.is_alive())
                raise AssertionError("synthetic publication failure")
        self.assertEqual(observations.get("resource_probe_errors"),
                         ["synthetic monitor SQL failure"])

    def test_monitor_error_alone_still_rejects_measurement(self):
        class Fixture:
            def lag(self):
                raise RuntimeError("synthetic monitor SQL failure")

        observations = {"lag_bytes": [], "primary_resources": []}
        probe = ResourceProbe(Fixture(), observations)
        with self.assertRaisesRegex(AssertionError, "resource probe failed"):
            with probe:
                probe.thread.join(timeout=2)
                self.assertFalse(probe.thread.is_alive())
        self.assertEqual(observations.get("resource_probe_errors"),
                         ["synthetic monitor SQL failure"])


class StandbyReadinessTests(unittest.TestCase):
    def setUp(self):
        self.fixture = object.__new__(CostFixture)
        self.fixture.results = {}
        self.fixture.env = {}
        self.fixture.standby = "synthetic-owned-standby"
        self.now = 0
        self.clock = SimpleNamespace(monotonic=lambda: self.now, sleep=self.advance)

    def advance(self, seconds):
        self.now += seconds

    @staticmethod
    def reply(value):
        return SimpleNamespace(returncode=0, stdout=value.encode(), stderr=b"")

    def test_basebackup_streaming_without_replay_feedback_is_not_a_standby(self):
        # Real basebackup -X stream reports write/flush LSNs but no replay LSN.
        # The replay-qualified primary query must therefore return zero first.
        replies = [self.reply("0\n"), self.reply("1\n"), self.reply("t\n")]
        with patch("cache_storage_cost.run", side_effect=replies) as run, \
                patch("cache_storage_cost.time", self.clock):
            self.fixture.wait_for_standby(seconds=1)
        self.assertEqual(self.fixture.results["standby_readiness"]["attempts"], 2)
        self.assertIn(b"replay_lsn IS NOT NULL", run.call_args_list[0].kwargs["data"])
        self.assertEqual(self.fixture.results["standby_readiness"]["last_standby_reply"], "t")

    def test_replay_feedback_does_not_bypass_sql_readiness(self):
        replies = [self.reply("1\n"), AssertionError("synthetic standby socket not ready"),
                   self.reply("1\n"), self.reply("t\n")]
        with patch("cache_storage_cost.run", side_effect=replies), \
                patch("cache_storage_cost.time", self.clock):
            self.fixture.wait_for_standby(seconds=1)
        state = self.fixture.results["standby_readiness"]
        self.assertEqual(state["attempts"], 2)
        self.assertIn("socket not ready", state["last_error"])
        self.assertEqual(state["last_standby_reply"], "t")

    def test_blocking_probe_is_bounded_by_remaining_startup_deadline(self):
        def blocked(command, **kwargs):
            self.advance(kwargs["timeout"])
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])

        with patch("cache_storage_cost.run", side_effect=blocked), \
                patch("cache_storage_cost.time", self.clock):
            with self.assertRaisesRegex(AssertionError, "deadline: actual streaming standby"):
                self.fixture.wait_for_standby(seconds=.1)
        self.assertEqual(self.now, .1)
        self.assertEqual(self.fixture.results["standby_readiness"]["attempts"], 1)
        self.assertIn("TimeoutExpired", self.fixture.results["standby_readiness"]["last_error"])

    def test_public_setup_does_not_return_during_basebackup_wal_streaming(self):
        # These responses match the real rate-limited-basebackup regression:
        # a streaming sender exists before the standby postmaster/socket does.
        primary = iter(["0\n", "1\n", "1\n"])
        sql_replies = iter([AssertionError("synthetic standby socket absent"), "t\n"])
        sql_ready = []

        def external(command, **kwargs):
            if command[0] == "psql":
                return self.reply(next(primary))
            if command[:2] == ["docker", "inspect"]:
                return self.reply(json.dumps([{"NetworkSettings": {"Networks": {
                    "bridge": {"IPAddress": "127.0.0.1"}}}}]))
            if "SELECT pg_is_in_recovery()" in command:
                value = next(sql_replies)
                if isinstance(value, Exception):
                    raise value
                sql_ready.append(True)
                return self.reply(value)
            return self.reply("999\n")

        fixture = self.fixture
        fixture.pg, fixture.pg2 = "synthetic-primary", "synthetic-unused-primary"
        fixture.sql = lambda query: "/synthetic/pg_hba.conf" if query == "SHOW hba_file" else "1"
        fixture.container = lambda *args: "synthetic-owned-standby"
        with tempfile.TemporaryDirectory() as directory:
            fixture.base = Path(directory)
            with patch("cache_storage_cost.run", side_effect=external), \
                    patch("cache_storage_cost.time", self.clock):
                fixture.setup_standby()
        self.assertTrue(sql_ready, "setup returned before a real standby SQL success")
        self.assertEqual(fixture.results["standby_readiness"]["last_standby_reply"], "t")


if __name__ == "__main__":
    unittest.main()
