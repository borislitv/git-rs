#!/usr/bin/env python3
"""Fast acceptance-scorer contracts; no Docker, Cargo or source corpus required."""
import copy
import json
from pathlib import Path
import unittest

from cache_storage_cost import CostFixture, distribution


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


if __name__ == "__main__":
    unittest.main()
