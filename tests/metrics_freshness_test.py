"""Protocol fixtures for the read-only probe; no Prometheus/containers required."""

import dataclasses
import datetime
import importlib.util
import http.server
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import threading
import unittest
import urllib.parse


PATH = Path(__file__).resolve().parents[1] / "examples/playground/metrics_freshness.py"
SPEC = importlib.util.spec_from_file_location("metrics_freshness_probe", PATH)
probe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)

NOW = 1_800_000_000.0
START = NOW - 100
VERSION = "0.1.0-rc.3"


class Clock:
    def __init__(self):
        self.elapsed = 0

    def now(self):
        return NOW + self.elapsed

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds


def serving_text(success=0, count=0, version=VERSION, start=START):
    # Independent fixture contract: no production schema/parser helpers are used.
    lines = []

    def family(name, kind, rows):
        lines.extend([f"# HELP {name} Fixture contract.", f"# TYPE {name} {kind}"])
        for labels, value in rows:
            suffix = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}" if labels else ""
            lines.append(f"{name}{suffix} {value}")

    def histogram(name, dimension=None, values=(None,), observed=0):
        lines.extend([f"# HELP {name} Fixture histogram.", f"# TYPE {name} histogram"])
        for value in values:
            label = f'{dimension}="{value}",' if dimension else ""
            total = observed if value == "http" else 0
            for bound in ("0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5",
                          "1", "2.5", "5", "10", "30", "60", "120", "+Inf"):
                lines.append(f'{name}_bucket{{{label}le="{bound}"}} {total}')
            suffix = "{" + label.rstrip(",") + "}" if dimension else ""
            lines.extend([f"{name}_count{suffix} {total}", f"{name}_sum{suffix} {total * 0.001}"])

    family("git_rs_build_info", "gauge", [({"version": version}, 1)])
    family("git_rs_process_start_time_seconds", "gauge", [({}, start)])
    for name in ("active_requests", "cache_allocated_bytes", "cache_waiters"):
        family("git_rs_" + name, "gauge", [({}, 0)])
    for name in ("timing_logs_dropped_total", "service_logs_dropped_total"):
        family("git_rs_" + name, "counter", [({}, 0)])
    family("git_rs_requests_total", "counter", [
        ({"transport": transport, "outcome": outcome}, success if (transport, outcome) == ("http", "success") else 0)
        for transport in ("ssh", "http") for outcome in ("success", "error", "unauthorized", "bad_request")
    ])
    histogram("git_rs_request_duration_seconds", "transport", ("ssh", "http"), count)
    for name, label, values in [
        ("phase", "phase", ("snapshot", "materialize", "scratch_init", "git", "pre_receive", "validate",
                            "graph", "upload", "publish", "receive_pack_prepare", "upload_serve")),
        ("stage", "stage", ("sql_init", "sql_acquire", "sql_query", "sql_reset", "lease_commit", "catalog_read",
                            "lease_release", "cache_fill", "cache_link", "object_hash", "restore_refs", "restore_head",
                            "hook_setup", "git_input", "git_output", "git_wait", "ref_validate", "tip_validate",
                            "fast_forward", "scratch_cleanup", "process_spawn")),
        ("dependency", "operation", ("sql", "s3_get", "s3_put", "s3_delete", "s3_versioning")),
        ("subprocess", "program", ("git", "curl", "sha256sum", "other")),
    ]:
        histogram(f"git_rs_{name}_duration_seconds", label, values)
        family(f"git_rs_{name}_errors_total", "counter", [({label: v}, 0) for v in values])
    family("git_rs_cache_lookups_total", "counter", [({"result": v}, 0) for v in ("hit", "miss", "bypass")])
    histogram("git_rs_cache_wait_duration_seconds")
    family("git_rs_admission_rejections_total", "counter", [({"listener": v}, 0) for v in ("ssh", "http", "health")])
    family("git_rs_s3_transferred_bytes_total", "counter", [({"direction": v}, 0) for v in ("download", "upload")])
    return ("\n".join(lines) + "\n").encode()


class PrometheusHTTP:
    """Small HTTP-protocol fake: routes and Prometheus vector bodies, not call scripts."""

    def __init__(self, clock, config, mode="ok"):
        self.clock, self.config, self.mode = clock, config, mode
        self.discovery_gets = 0
        self.success = 0

    def get(self, address, limit, timeout):
        assert 0 < timeout <= self.config.http_timeout
        parsed = urllib.parse.urlsplit(address)
        elapsed = self.clock.elapsed
        observed_time = self.clock.now() + {
            "age_boundary": -13, "age_outside": -13.001,
            "future_boundary": 1, "future_outside": 1.001,
        }.get(self.mode, -1)
        version = "wrong-version" if self.mode == "wrong_version" else VERSION
        start = START + 1 if self.mode == "restart" and elapsed else START
        if address == self.config.metrics_url:
            body = serving_text(self.success, self.success, version, start)
            if self.mode == "raw_private_label":
                body = body.replace(b'phase="snapshot"', b'phase="private-repository"')
            elif self.mode == "raw_type":
                body = body.replace(b"# TYPE git_rs_requests_total counter", b"# TYPE git_rs_requests_total gauge")
            elif self.mode == "raw_duplicate":
                body += b"git_rs_active_requests 0\n"
            elif self.mode == "raw_nonfinite":
                body = body.replace(b"git_rs_active_requests 0", b"git_rs_active_requests NaN")
            elif self.mode == "raw_missing":
                body = body.replace(b"git_rs_active_requests 0\n", b"")
            elif self.mode == "oversized":
                body = b"x" * (limit + 1)
            elif self.mode == "counter_reset":
                body = body.replace(b"git_rs_timing_logs_dropped_total 0",
                                    b"git_rs_timing_logs_dropped_total " + (b"0" if elapsed else b"3"))
            return body
        if address == self.config.discovery_url:
            self.discovery_gets += 1
            if self.mode != "no_delta":
                self.success += 2 if self.mode == "excess_delta" else 1
            return b"001e# service=git-upload-pack\n0000"
        if parsed.path == "/api/v1/targets":
            if self.mode == "bad_json":
                return b'{"status":'
            if self.mode == "duplicate_json":
                return b'{"status":"success","status":"success","data":{}}'
            if self.mode == "oversized_json":
                return b"x" * (limit + 1)
            last = observed_time
            if self.mode in ("frozen_target", "stale_target"):
                last = NOW - (100 if self.mode == "stale_target" else 1)
            if self.mode == "future_target":
                last = self.clock.now() + 2
            target = {
                "labels": {"job": self.config.job, "instance": self.config.instance},
                "scrapePool": "fixture-pool", "scrapeUrl": self.config.metrics_url,
                "lastScrape": datetime.datetime.fromtimestamp(last, datetime.timezone.utc).isoformat(),
                "health": "down" if self.mode == "unhealthy" else "up",
                "lastError": "private-upstream-message" if self.mode == "last_error" else "",
            }
            if self.mode == "wrong_target":
                target["scrapeUrl"] = "http://other.invalid/metrics"
            if self.mode == "identity_change" and elapsed:
                target["labels"]["pod"] = "replacement"
            rows = [] if self.mode == "missing_target" else [target]
            if self.mode == "duplicate_target":
                rows *= 2
            data = {"activeTargets": rows, "droppedTargets": []}
        elif parsed.path == "/api/v1/query":
            expression = urllib.parse.parse_qs(parsed.query)["query"][0]
            is_time = expression.startswith("timestamp(")
            inner = expression[10:-1] if is_time else expression
            name = inner.split("{", 1)[0]
            labels = {k: json.loads(v) for k, v in re.findall(r'(\w+)=("(?:[^"\\]|\\.)*")', inner)}
            assert labels["job"] == self.config.job and labels["instance"] == self.config.instance
            if name == "git_rs_build_info":
                labels["version"] = "other-version" if self.mode == "scraped_wrong_version" else version
            values = {"up": 0 if self.mode == "up_zero" else 1, "git_rs_build_info": 1,
                      "git_rs_process_start_time_seconds": start, "git_rs_requests_total": self.success,
                      "git_rs_request_duration_seconds_count": self.success}
            value = observed_time if is_time else values[name]
            if not is_time and name == "git_rs_process_start_time_seconds" and self.mode == "scraped_start_mismatch":
                value += 1
            if is_time and self.mode == "frozen_sample":
                value = NOW - 1
            if is_time and self.mode == "stale_sample":
                value = self.clock.now() - 100
            if is_time and self.mode == "future_sample":
                value = self.clock.now() + 2
            if self.mode == "nonfinite_sample":
                value = "NaN"
            if is_time and self.mode == "nonfinite_timestamp":
                value = "NaN"
            if self.mode == "wrong_query_identity":
                labels["instance"] = "other.invalid:8081"
            if self.mode == "duration_missing_delta" and name == "git_rs_request_duration_seconds_count" and not is_time:
                value = 0
            rows = [{"metric": labels, "value": [self.clock.now(), str(value)]}]
            if self.mode == "missing_sample":
                rows = []
            elif self.mode == "duplicate_sample":
                rows *= 2
            data = {"resultType": "vector", "result": rows}
        else:
            raise AssertionError("unexpected protocol endpoint")
        return json.dumps({"status": "success", "data": data}).encode()


class ClockAwareHTTP(PrometheusHTTP):
    """Responses are current when received, but sequential GETs consume real budget."""

    def __init__(self, clock, config, slow_until=None):
        super().__init__(clock, config)
        self.slow_until = slow_until

    def get(self, address, limit, timeout):
        slow = self.slow_until is None or self.clock.elapsed < self.slow_until
        self.clock.sleep(min(1 if slow else 0.01, timeout))
        return super().get(address, limit, timeout)


def config(**changes):
    return dataclasses.replace(probe.Config(
        prometheus_url="http://prometheus.example.invalid", metrics_url="http://backend.example.invalid/metrics",
        job="fixture", instance="backend.example.invalid:8081", expected_version=VERSION,
        max_wait=20, scrape_interval=5, http_timeout=2, clock_skew=1,
    ), **changes)


class FreshnessTests(unittest.TestCase):
    def run_fixture(self, mode="ok", **changes):
        settings = config(**changes)
        clock = Clock()
        http = PrometheusHTTP(clock, settings, mode)
        return probe.run_probe(settings, http, clock), http, clock

    def test_advancing_target_and_actual_sample_values_are_verified(self):
        receipt, http, clock = self.run_fixture(expected_profile="NativeRootV1")
        self.assertEqual(receipt["status"], "verified")
        self.assertGreater(receipt["last_scrape"], receipt["first_last_scrape"])
        self.assertEqual(receipt["sample_timestamps"]["build"], NOW + 4)
        self.assertEqual(receipt["instrumentation_start_seconds"], START)
        self.assertEqual(receipt["runtime_image_identity"], "not_externally_bound")
        self.assertIn("effective_blame_profile", receipt["not_verified"])
        self.assertEqual(http.discovery_gets, 0)
        self.assertEqual(clock.elapsed, 5)

    def test_one_optional_discovery_get_has_two_exact_counter_deltas(self):
        receipt, http, _ = self.run_fixture(
            discovery_url="http://backend.example.invalid/demo/repo.git/info/refs?service=git-upload-pack")
        self.assertEqual(http.discovery_gets, 1)
        self.assertEqual(receipt["controlled_http_delta"], {"success": 1, "count": 1})

    def test_target_and_metric_protocol_failures_are_closed_and_bounded(self):
        for mode in ("missing_target", "duplicate_target", "wrong_target", "identity_change",
                     "wrong_query_identity", "missing_sample", "duplicate_sample", "nonfinite_sample",
                     "wrong_version", "restart", "counter_reset", "future_target", "future_sample",
                     "raw_private_label", "raw_type", "raw_duplicate", "raw_nonfinite", "oversized",
                     "raw_missing", "scraped_wrong_version", "scraped_start_mismatch", "nonfinite_timestamp",
                     "bad_json", "duplicate_json", "oversized_json",
                     "unhealthy", "last_error", "up_zero", "stale_target", "stale_sample",
                     "frozen_target", "frozen_sample"):
            with self.subTest(mode=mode):
                settings, clock = config(), Clock()
                http = PrometheusHTTP(clock, settings, mode)
                with self.assertRaises(probe.ProbeError) as error:
                    probe.run_probe(settings, http, clock)
                self.assertLessEqual(clock.elapsed, settings.max_wait)
                self.assertNotIn("private", str(error.exception))
                self.assertNotIn("http", str(error.exception))

    def test_query_envelope_time_does_not_rescue_frozen_or_stale_sample_values(self):
        for mode in ("stale_sample", "frozen_sample"):
            with self.subTest(mode=mode), self.assertRaises(probe.ProbeError):
                self.run_fixture(mode)

    def test_sequential_get_latency_cannot_verify_expired_observation(self):
        # Every GET takes one second, below the two-second HTTP timeout. The
        # five-second freshness allowance must still hold when the observation
        # finishes, not just when each individual response was received.
        settings = config(max_wait=40, scrape_interval=1, http_timeout=2, clock_skew=1)
        clock = Clock()
        http = ClockAwareHTTP(clock, settings)
        try:
            receipt = probe.run_probe(settings, http, clock)
        except probe.ProbeError as error:
            self.assertIn(str(error), ("probe_deadline", "freshness_deadline", "stale_timestamp"))
        else:
            self.assertEqual(receipt["status"], "verified")
            # A future implementation may complete a fresh observation with
            # fewer round trips; reject expired success, not that optimization.
            self.assertLessEqual(clock.now() - receipt["last_scrape"], 5)
            for observed in receipt["sample_timestamps"].values():
                self.assertLessEqual(clock.now() - observed, 5)
        self.assertLessEqual(clock.elapsed, settings.max_wait)

    def test_transient_slow_observation_retries_and_recovers_before_deadline(self):
        settings = config(max_wait=40, scrape_interval=1, http_timeout=2, clock_skew=1)
        clock = Clock()
        # Only the initial time window is slow. The transport does not inspect
        # probe method calls or know which observation is being assembled.
        http = ClockAwareHTTP(clock, settings, slow_until=11)
        receipt = probe.run_probe(settings, http, clock)
        self.assertEqual(receipt["status"], "verified")
        self.assertLess(clock.elapsed, settings.max_wait)
        self.assertGreater(receipt["last_scrape"], receipt["first_last_scrape"])
        # The accepted baseline itself must be coherent: by the time its last
        # timestamp response arrived, its target timestamp could not be expired.
        # Keeping the stale initial observation as baseline violates this even
        # when the final fast observation happens to be fresh.
        self.assertLessEqual(
            max(receipt["first_sample_timestamps"].values()) - receipt["first_last_scrape"],
            5,
        )
        self.assertLessEqual(clock.now() - receipt["last_scrape"], 5)
        for name, observed in receipt["sample_timestamps"].items():
            self.assertGreater(observed, receipt["first_sample_timestamps"][name])
            self.assertLessEqual(clock.now() - observed, 5)

    def test_discovery_is_not_retried_when_delta_is_missing_or_excessive(self):
        for mode in ("no_delta", "excess_delta", "duration_missing_delta"):
            settings = config(discovery_url="http://backend.example.invalid/repo.git/info/refs?service=git-upload-pack")
            clock = Clock()
            http = PrometheusHTTP(clock, settings, mode)
            with self.subTest(mode=mode), self.assertRaises(probe.ProbeError):
                probe.run_probe(settings, http, clock)
            self.assertEqual(http.discovery_gets, 1)

    def test_selectors_are_promql_string_escaped(self):
        receipt, _, _ = self.run_fixture(job='fixture"} or up{job="x', instance='host\\"x:8081')
        self.assertEqual(receipt["status"], "verified")
        self.assertNotIn("fixture", json.dumps(receipt))

    def test_freshness_and_future_clock_boundaries(self):
        # Allowance = two scrape intervals + HTTP timeout + clock skew = 13 s.
        for mode in ("age_boundary", "future_boundary"):
            with self.subTest(mode=mode):
                self.assertEqual(self.run_fixture(mode)[0]["status"], "verified")
        for mode in ("age_outside", "future_outside"):
            with self.subTest(mode=mode), self.assertRaises(probe.ProbeError):
                self.run_fixture(mode)

    def test_credentials_url_queries_and_non_discovery_gets_are_rejected(self):
        for changes in (
            {"prometheus_url": "http://user:private@example.invalid"},
            {"metrics_url": "http://user@example.invalid/metrics"},
            {"prometheus_url": "http://example.invalid?token=private"},
            {"metrics_url": "http://example.invalid/metrics?token=private"},
            {"metrics_url": "http://example.invalid:bad/metrics"},
            {"metrics_url": "http://example.invalid/metrics#private"},
            {"discovery_url": "http://example.invalid/repo.git/git-receive-pack"},
            {"discovery_url": "http://example.invalid/info/refs?service=git-upload-pack&token=private"},
            {"max_wait": float("inf")}, {"http_timeout": 0}, {"clock_skew": -0.1},
        ):
            with self.subTest(changes=changes), self.assertRaises(probe.ProbeError):
                config(**changes).validate()

    def test_http_transport_bounds_bodies_and_refuses_redirects_and_encoding(self):
        seen = []
        release = threading.Event()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                if self.path == "/blocked":
                    release.wait(5)
                    return
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/private-location")
                    self.end_headers()
                    return
                self.send_response(200)
                if self.path == "/declared-large":
                    self.send_header("Content-Length", "10000")
                if self.path == "/encoded":
                    self.send_header("Content-Encoding", "gzip")
                self.end_headers()
                try:
                    self.wfile.write(b"x" * (65 if self.path == "/actual-large" else 2))
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            base = "http://127.0.0.1:" + str(server.server_port)
            client = probe.Http()
            self.assertEqual(client.get(base + "/ok", 64, 2), b"xx")
            for path in ("/redirect", "/declared-large", "/actual-large", "/encoded"):
                with self.subTest(path=path), self.assertRaises(probe.ProbeError):
                    client.get(base + path, 64, 2)
            with self.assertRaises(probe.ProbeError):
                client.get(base + "/blocked", 64, 0.1)
            self.assertNotIn("/private-location", seen)
        finally:
            release.set()
            server.shutdown()
            worker.join()
            server.server_close()

    def test_argparse_never_echoes_rejected_argument_values(self):
        import contextlib
        import io

        stderr = io.StringIO()
        parser = probe.Parser()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            parser.parse_args(["--secret", "private-value"])
        self.assertNotIn("private-value", stderr.getvalue())
        self.assertIn("invalid_arguments", stderr.getvalue())

    def test_http_rejects_complete_payload_when_content_length_is_short_read(self):
        # Both payloads are complete and independently valid. Only their HTTP
        # framing is truncated, so JSON/exposition parsing cannot detect the EOF.
        payloads = {
            "/json": b'{"status":"success","data":{"activeTargets":[]}}',
            "/metrics": serving_text(),
        }

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = payloads[self.path]
                self.send_response(200)
                self.send_header("Content-Length", str(len(body) + 10))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                self.close_connection = True

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            base = "http://127.0.0.1:" + str(server.server_port)
            client = probe.Http()
            for path, body in payloads.items():
                with self.subTest(path=path), self.assertRaisesRegex(probe.ProbeError, r"^[a-z_]+$"):
                    client.get(base + path, len(body) + 32, 2)
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_receipt_is_private_exclusive_and_contains_no_endpoint_or_label_values(self):
        receipt, _, _ = self.run_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            probe.write_receipt(path, receipt)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            text = path.read_text()
            self.assertEqual(json.loads(text)["status"], "verified")
            self.assertNotIn("example.invalid", text)
            with self.assertRaises(probe.ProbeError):
                probe.write_receipt(path, receipt)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(probe.ProbeError):
                probe.write_receipt(link, receipt)
            os.chmod(directory, 0o755)
            with self.assertRaises(probe.ProbeError):
                probe.write_receipt(Path(directory) / "other", receipt)


if __name__ == "__main__":
    unittest.main()
