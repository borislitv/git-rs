#!/usr/bin/env python3
"""Fast acceptance-scorer contracts; no Docker, Cargo or source corpus required."""
import copy
import hashlib
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
            "size": 16384, "mode": "all-pg", "profile": "random",
            "inline_threshold_bytes": 8 * 1024 * 1024, "source_unchanged": True,
            "source_errors": [], "source_latency_ms": {"count": 100, "p99": 10},
            "latency_ms": {"cold_write": {"max": 20}}, "lag_bytes": [0, 8192],
            "replica_catchup_seconds": 1,
            "wal_bytes": 1048576, "relation_growth_bytes": 1048576,
            "wire_bytes": {"pg": {"request": 70000, "response": 300000},
                           "s3": {"request": 0, "response": 0}},
            "storage_after": {"tables": [{"estimated_dead": 16}]},
            "primary_resources": [{"memory_headroom_fraction": .8}],
            "fixture_materialized_bytes": 65536,
            "verified_digest_input_bytes": 294912,
            "payload_layout": {"rows": 4, "declared_bytes": 65536,
                               "capacity_bytes": 65536, "stored_bytes": 65536,
                               "inline_bytes": 65536, "object_rows": 0, "payloads": []},
        }
        digest = hashlib.sha256(hashlib.shake_256(b"scorer-raw").digest(16384)).hexdigest()
        self.row["expected_payloads"] = [{"id": str(i), "logical_size": 16384,
            "logical_digest": digest, "reads": 6 if i == 0 else 4} for i in range(4)]
        self.row["payload_layout"]["payloads"] = [dict(p, encoding="raw-v1",
            stored_size=16384, stored_digest=digest, physical_fields_present=False,
            inline_size=16384, inline_digest=digest, object_key=None)
            for p in self.row["expected_payloads"]]

    @staticmethod
    def force_s3(row):
        row.update(mode="pg-s3", inline_threshold_bytes=0)
        row["payload_layout"].update(inline_bytes=0, object_rows=4)
        for payload in row["payload_layout"]["payloads"]:
            payload.update(inline_size=None, inline_digest=None, object_key="object/" + payload["id"])

    def compressed_row(self):

        # Independent checksummed zstd frame of the matrix's 16 KiB repeated
        # SHAKE block, produced/decoded with Python's reference zstd implementation.
        # Four frames reproduce CI's 608 physical versus 65536 logical bytes.
        frame = bytes.fromhex(
            "28b52ffd64003f550400040849569580ce2c646043ea9bbe78dbb773d2dc94390d528fce"
            "b778fb49aee34c2f7a9ce3d088d674529d3f6c3f266c7997588bd0fac244b9d5da1ec1"
            "833b237ea189eb2cb5e384e7b7c285bcf561750bc72602ddaab999d661ed8ea97b4324e2"
            "9fe9c1693a704755b525acfc3d213c43991012a7e58ff3b378a3f0bcfbaf45aa0b010080"
            "be3fe08d1469fd460a")
        self.assertEqual(len(frame), 152)
        self.assertEqual(hashlib.sha256(frame).hexdigest(),
                         "b9259671bf91b0e468d1c146a517bbe37fff3b32d9c56dee4b798147e23b5b02")
        logical = hashlib.shake_256(b"cache-cost-v1/repeated-block/16384/0").digest(128) * 128
        digest = hashlib.sha256(logical).hexdigest()
        self.assertEqual(digest, "2422e1069b0eb085c71a2fdac30beb42cc425268534a35e0c695de38ea23bb30")
        row = copy.deepcopy(self.row)
        row["profile"] = "repeated-block"
        row["payload_layout"].update(inline_bytes=len(frame) * 4, stored_bytes=len(frame) * 4)
        for expected, payload in zip(row["expected_payloads"], row["payload_layout"]["payloads"]):
            expected["logical_digest"] = digest
            payload.update(logical_digest=digest, encoding="zstd-v1", stored_size=len(frame),
                stored_digest=hashlib.sha256(frame).hexdigest(), physical_fields_present=True,
                inline_size=len(frame), inline_digest=hashlib.sha256(frame).hexdigest())
        # Synthetic protocol counters deliberately below logical volume; never
        # substitute these expected payload counts for measured runtime counters.
        row["wire_bytes"]["pg"] = {"request": 4000, "response": 12000}
        return row

    def test_actual_compressed_inline_bytes_are_not_logical_capacity_bytes(self):
        row = self.compressed_row()
        self.fixture.score(row, self.baseline)
        self.assertEqual(row["payload_io_bytes"], {"logical_write": 65536, "logical_read": 294912,
                         "stored_write": 608, "stored_read": 2736})

    def test_compressed_frames_are_forced_to_s3_despite_fitting_default_inline_limit(self):
        row = self.compressed_row()
        self.force_s3(row)
        row["wire_bytes"]["s3"] = {"request": 4000, "response": 12000}
        self.fixture.score(row, self.baseline)
        self.assertEqual(row["payload_io_bytes"]["stored_read"], 2736)

    def test_generous_functional_limits_accept_both_storage_policies(self):
        self.fixture.score(self.row, self.baseline)
        self.force_s3(self.row)
        self.row["wire_bytes"]["s3"] = {"request": 70000, "response": 300000}
        self.fixture.score(self.row, self.baseline)

    def test_independently_bad_observations_are_rejected(self):
        wrong_route = copy.deepcopy(self.row["payload_layout"])
        wrong_route.update(inline_bytes=0, object_rows=4)
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
            ("wrong route", {"payload_layout": wrong_route}, "storage policy"),
            ("unexpected S3", {"wire_bytes": {"pg": {"request": 70000, "response": 300000},
                                             "s3": {"request": 1, "response": 0}}}, "transport"),
        ]
        for name, update, message in scenarios:
            with self.subTest(name=name):
                row = copy.deepcopy(self.row)
                row.update(update)
                with self.assertRaisesRegex(AssertionError, message):
                    self.fixture.score(row, self.baseline)

    def test_wrong_logical_and_physical_metadata_are_rejected_independently(self):
        scenarios = [
            ("logical size", lambda r: r["payload_layout"]["payloads"][0].update(logical_size=152), "logical payload"),
            ("logical digest", lambda r: r["payload_layout"]["payloads"][0].update(logical_digest="0" * 64), "logical payload"),
            ("capacity charged compressed", lambda r: r["payload_layout"].update(capacity_bytes=608), "logical capacity"),
            ("physical sum", lambda r: r["payload_layout"].update(stored_bytes=65536), "stored byte accounting"),
            ("raw inline assumption", lambda r: r["payload_layout"].update(inline_bytes=65536), "storage policy"),
            ("stored fields absent", lambda r: r["payload_layout"]["payloads"][0].update(physical_fields_present=False), "compressed stored metadata"),
            ("zero compressed frame", lambda r: r["payload_layout"]["payloads"][0].update(stored_size=0), "compressed stored metadata"),
            ("nonbeneficial encoding", lambda r: r["payload_layout"]["payloads"][0].update(stored_size=16384), "compressed stored metadata"),
            ("stored digest", lambda r: r["payload_layout"]["payloads"][0].update(stored_digest="not-a-hash"), "stored digest"),
            ("inline digest", lambda r: r["payload_layout"]["payloads"][0].update(inline_digest="0" * 64), "physical integrity"),
            ("inline length", lambda r: r["payload_layout"]["payloads"][0].update(inline_size=16384), "physical integrity"),
            ("double placement", lambda r: r["payload_layout"]["payloads"][0].update(object_key="also-external"), "storage policy"),
            ("unknown encoding", lambda r: r["payload_layout"]["payloads"][0].update(encoding="guess-magic"), "encoding/profile"),
            ("compression not exercised", lambda r: r["payload_layout"]["payloads"][0].update(encoding="raw-v1"), "encoding/profile"),
            ("wrong threshold", lambda r: r.update(inline_threshold_bytes=256 * 1024), "physical threshold"),
            ("digest accounting", lambda r: r.update(verified_digest_input_bytes=2736), "logical materialization/digest"),
            ("input accounting", lambda r: r.update(fixture_materialized_bytes=608), "logical materialization/digest"),
            ("wrong payload", lambda r: r["payload_layout"]["payloads"][0].update(id="foreign"), "payload identities"),
            ("duplicate payload", lambda r: r["payload_layout"]["payloads"][1].update(id=r["payload_layout"]["payloads"][0]["id"]), "payload identities"),
        ]
        for name, change, message in scenarios:
            with self.subTest(name=name):
                row = self.compressed_row()
                change(row)
                with self.assertRaisesRegex(AssertionError, message):
                    self.fixture.score(row, self.baseline)
        for field, value in (("stored_size", 152), ("stored_digest", "0" * 64)):
            row = copy.deepcopy(self.row)
            row["payload_layout"]["payloads"][0][field] = value
            with self.subTest(raw_field=field), self.assertRaisesRegex(AssertionError, "raw physical identity"):
                self.fixture.score(row, self.baseline)

    def test_small_compressed_frames_cannot_silently_turn_s3_mode_into_inline(self):
        row = self.compressed_row()
        row.update(mode="pg-s3", inline_threshold_bytes=0)
        row["payload_layout"].update(inline_bytes=0, object_rows=4)
        row["wire_bytes"]["s3"] = {"request": 4000, "response": 12000}
        with self.assertRaisesRegex(AssertionError, "S3 storage policy"):
            self.fixture.score(row, self.baseline)

    def test_wire_requires_stored_uploads_and_weighted_reads_in_both_modes(self):
        for mode, transport in (("all-pg", "pg"), ("pg-s3", "s3")):
            for direction, insufficient in (("request", 607), ("response", 2735)):
                with self.subTest(mode=mode, direction=direction):
                    row = self.compressed_row()
                    if mode == "pg-s3":
                        self.force_s3(row)
                        row["wire_bytes"]["s3"] = {"request": 4000, "response": 12000}
                    row["wire_bytes"][transport][direction] = insufficient
                    with self.assertRaisesRegex(AssertionError, "transport"):
                        self.fixture.score(row, self.baseline)

    def test_stored_read_floor_weights_the_frequently_read_artifact_not_an_average(self):
        # Another independently generated valid checksummed frame, this time
        # 16 KiB of A. Key zero has six reads while each other key has four.
        frame = bytes.fromhex("28b52ffd64003f4d00001041410100fb9f0758411c2f3f")
        self.assertEqual(len(frame), 23)
        logical_digest = hashlib.sha256(b"A" * 16384).hexdigest()
        stored_digest = hashlib.sha256(frame).hexdigest()
        for mode, transport in (("all-pg", "pg"), ("pg-s3", "s3")):
            with self.subTest(mode=mode):
                row = self.compressed_row()
                row["expected_payloads"][0]["logical_digest"] = logical_digest
                row["payload_layout"]["payloads"][0].update(logical_digest=logical_digest,
                    stored_size=23, stored_digest=stored_digest, inline_size=23,
                    inline_digest=stored_digest)
                row["payload_layout"].update(stored_bytes=479, inline_bytes=479)
                if mode == "pg-s3":
                    self.force_s3(row)
                row["wire_bytes"][transport] = {"request": 479, "response": 1962}
                self.fixture.score(row, self.baseline)
                self.assertEqual(row["payload_io_bytes"]["stored_read"], 1962)
                row["wire_bytes"][transport]["response"] = 1961
                with self.assertRaisesRegex(AssertionError, "transport"):
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
