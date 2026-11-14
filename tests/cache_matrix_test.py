#!/usr/bin/env python3
"""Matrix input, evidence completeness and failure-preserving cleanup contracts."""
import copy
import json
from pathlib import Path
import socket
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import cache_matrix
from cache_matrix import Matrix, ROOT, WireProxy, cleanup_all, load_config, require_compaction


class ConfigurationContracts(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "tests/fixtures/cache-matrix.json")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "config.json"

    def rejected(self, change, reason):
        value = copy.deepcopy(self.config)
        change(value)
        self.path.write_text(json.dumps(value))
        with self.assertRaisesRegex(AssertionError, reason):
            load_config(self.path)

    def test_relaxed_budgets_and_boolean_counts_are_not_an_accepted_version(self):
        self.rejected(lambda value: value["limits"].update(request_seconds=120), "acceptance budget")
        self.rejected(lambda value: value["fixture"].update(branches=True), "synthetic branches")
        self.rejected(lambda value: value.update(repetitions=True), "repetition count")

    def test_dropped_cells_changed_paths_and_unknown_options_are_rejected(self):
        self.rejected(lambda value: value["placements"].pop(), "matrix inventory")
        self.rejected(lambda value: value["paths"].append("../foreign"), "query inventory")
        self.rejected(lambda value: value.update(endpoint="https://example.invalid"), "matrix configuration")

    def test_symlinked_config_is_rejected_before_following_it(self):
        self.path.symlink_to(ROOT / "tests/fixtures/cache-matrix.json")
        with self.assertRaisesRegex(AssertionError, "regular matrix configuration"):
            load_config(self.path)


class EvidenceContracts(unittest.TestCase):
    def setUp(self):
        config = load_config(ROOT / "tests/fixtures/cache-matrix.json")
        self.matrix = Matrix(SimpleNamespace(results={}), config)
        self.row = {"placement": "all-pg", "profile": "derivation", "source_probe_errors": [],
                    "source_sample_count": 3, "source_snapshot_ms": {"p99": 12}, "requests": [],
                    "mixed": {"before": {"generation": 8, "epoch": 0, "packs": 8, "operations": []},
                              "after": {"generation": 10, "epoch": 1, "packs": 1, "operations": [{
                                  "operation": "synthetic-operation", "job": "synthetic-job", "state": "succeeded",
                                  "attempt": 1, "current_attempt": 1, "input_generation": 9,
                                  "result_generation": 10, "input_epoch": 0, "result_epoch": 1}]}}}
        for phase in ("cold", "local-repeat", "cross-process", "restart", "cache-outage"):
            for path in config["paths"]:
                self.row["requests"].append(self.event(phase, path))
        for phase in ("mixed", "contention-owner", "contention-peer"):
            self.row["requests"].append(self.event(phase, "large.txt"))

    @staticmethod
    def event(phase, path):
        cold = phase in {"cold", "cache-outage", "contention-owner", "contention-peer"}
        return {"phase": phase, "path": path, "work_completions": 1, "body_hits": 0,
                "seconds": .02, "cpu_ns": 1000,
                "work": {"mapping_computations": 3 if cold else 0,
                         "mapping_hits": 0 if cold else 3, "imported_mapping_loads": 0 if cold else 3}}

    def test_missing_or_duplicate_restart_requests_cannot_satisfy_the_matrix(self):
        row = copy.deepcopy(self.row)
        row["requests"] = [event for event in row["requests"] if event["phase"] != "restart"]
        with self.assertRaisesRegex(AssertionError, "request inventory"):
            self.matrix.score(row)
        row = copy.deepcopy(self.row)
        row["requests"].append(copy.deepcopy(row["requests"][0]))
        with self.assertRaisesRegex(AssertionError, "request inventory"):
            self.matrix.score(row)

    def test_ready_body_and_job_local_hits_do_not_prove_distributed_derivation_reuse(self):
        row = copy.deepcopy(self.row)
        row["requests"][4]["body_hits"] = 1
        with self.assertRaisesRegex(AssertionError, "ready body contaminated"):
            self.matrix.score(row)
        row = copy.deepcopy(self.row)
        for event in row["requests"]:
            if event["phase"] == "restart":
                event["work"]["imported_mapping_loads"] = 0
        with self.assertRaisesRegex(AssertionError, "avoid actual fresh derivation"):
            self.matrix.score(row)

    def test_build_plus_reads_uses_the_same_query_inventory_in_every_placement(self):
        self.matrix.score(self.row)
        self.assertAlmostEqual(self.row["build_plus_reads_seconds"], .32)
        self.assertEqual(self.row["daemon_cpu_ns_by_phase"]["restart"], 4000)

    def test_successful_stale_job_without_catalog_publication_is_not_compaction(self):
        before = self.row["mixed"]["before"]
        after = {"generation": 9, "epoch": 0, "packs": 8, "operations": []}
        with self.assertRaisesRegex(AssertionError, "physical compaction publication"):
            require_compaction(before, after)
        row = copy.deepcopy(self.row)
        row["mixed"]["after"] = after
        with self.assertRaisesRegex(AssertionError, "physical compaction publication"):
            self.matrix.score(row)

    def test_old_operation_cannot_stand_in_for_the_post_receive_catalog(self):
        before, after = self.row["mixed"]["before"], copy.deepcopy(self.row["mixed"]["after"])
        after["operations"][0]["input_generation"] = 8
        with self.assertRaisesRegex(AssertionError, "post-receive catalog"):
            require_compaction(before, after)
        after = copy.deepcopy(self.row["mixed"]["after"])
        after["operations"][0]["current_attempt"] = 2
        with self.assertRaisesRegex(AssertionError, "post-receive catalog"):
            require_compaction(before, after)

    def test_cleanup_attempts_all_owners_and_preserves_the_foreground_failure(self):
        observed, record = [], {}

        def failing():
            observed.append("first")
            raise OSError("synthetic close failure")

        with self.assertRaisesRegex(RuntimeError, "foreground failure"):
            try:
                raise RuntimeError("foreground failure")
            finally:
                cleanup_all([failing, lambda: observed.append("second")], record)
        self.assertEqual(observed, ["first", "second"])
        self.assertEqual(record["cleanup_errors"], ["OSError"])
        with self.assertRaisesRegex(OSError, "synthetic close failure"):
            cleanup_all([failing, lambda: observed.append("third")], record)
        self.assertEqual(observed[-1], "third")

    def test_cell_cleanup_attempts_both_databases_and_role_and_persists_the_original_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = SimpleNamespace(base=Path(directory), env={"PGDATABASE": "original"}, results={}, daemons={},
                                      cli=Mock(side_effect=RuntimeError("foreground migration failure")))
            matrix = Matrix(fixture, load_config(ROOT / "tests/fixtures/cache-matrix.json"))
            created, dropped, roles = [], [], []

            def database(_fixture, query, _environment=None):
                if query.startswith("CREATE DATABASE "):
                    created.append(query.split()[-1])
                elif query.startswith("DROP DATABASE "):
                    dropped.append(query.split()[-1])
                    if len(dropped) == 1:
                        raise OSError("synthetic database cleanup failure")
                elif query.startswith("DROP ROLE "):
                    roles.append(query.split()[-1])
                return "0"

            with patch.object(cache_matrix, "sql", side_effect=database):
                with self.assertRaisesRegex(RuntimeError, "foreground migration failure"):
                    matrix.cell(0, 0, "disabled", "derivation", None, None, None, None, None)
            self.assertEqual(dropped, created)
            self.assertEqual(len(roles), 1)
            self.assertEqual(fixture.env, {"PGDATABASE": "original"})
            row = json.loads((fixture.base / "report.json").read_text())["cells"][0]
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["failure"], "RuntimeError")
            self.assertEqual(row["cleanup_errors"], ["OSError"])

    def test_wire_close_reaps_live_forwarding_owners_after_counting_actual_bytes(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        self.addCleanup(listener.close)
        errors = []

        def upstream():
            try:
                with listener.accept()[0] as peer:
                    peer.settimeout(5)
                    with peer.makefile("rb") as source:
                        self.assertEqual(source.read(4), b"ping")
                        peer.sendall(b"pong")
                        self.assertEqual(source.read(1), b"")
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=upstream)
        thread.start()
        proxy = WireProxy("127.0.0.1", listener.getsockname()[1])
        self.addCleanup(proxy.close)
        with socket.create_connection(("127.0.0.1", proxy.port), timeout=5) as client:
            client.sendall(b"ping")
            with client.makefile("rb") as response:
                self.assertEqual(response.read(4), b"pong")
                proxy.close()
                self.assertEqual(response.read(1), b"")
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(proxy.snapshot(), [4, 4])
        self.assertEqual(proxy.workers, set())


if __name__ == "__main__":
    unittest.main()
