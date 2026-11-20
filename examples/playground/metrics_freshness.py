#!/usr/bin/env python3
"""Read-only, explicit-target Prometheus verification; never provisions a fixture."""

import argparse
import datetime
import hashlib
import http.client
import itertools
import json
import math
import os
from pathlib import Path
import queue
import re
import stat
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


MAX_JSON = 2 * 1024 * 1024
MAX_METRICS = 128 * 1024
BUCKETS = ("0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25",
           "0.5", "1", "2.5", "5", "10", "30", "60", "120", "+Inf")
PHASES = ("snapshot", "materialize", "scratch_init", "git", "pre_receive", "validate",
          "graph", "upload", "publish", "receive_pack_prepare", "upload_serve")
STAGES = ("sql_init", "sql_acquire", "sql_query", "sql_reset", "lease_commit", "catalog_read",
          "lease_release", "cache_fill", "cache_link", "object_hash", "restore_refs",
          "restore_head", "hook_setup", "git_input", "git_output", "git_wait", "ref_validate",
          "tip_validate", "fast_forward", "scratch_cleanup", "process_spawn")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}\Z")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\n]|\\[\\"n])*)"')
SAMPLE = re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? ([^ ]+)\Z')


class ProbeError(Exception):
    """Only fixed, non-sensitive codes cross the CLI error boundary."""


class Pending(ProbeError):
    """A well-formed observation which may become fresh before the deadline."""


def fail(code):
    raise ProbeError(code)


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            fail("duplicate_json_key")
        result[name] = value
    return result


def number(value, code="invalid_number"):
    if isinstance(value, bool):
        fail(code)
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        fail(code)
    if not math.isfinite(result):
        fail(code)
    return result


def url(value, discovery=False, base=False):
    try:
        if not isinstance(value, str) or len(value) > 4096:
            fail("invalid_url")
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.fragment or any(ord(c) <= 32 or ord(c) == 127 for c in value)
                or "\\" in value or (port is not None and port == 0)):
            fail("invalid_url")
        if discovery:
            if (not parsed.path.endswith("/info/refs")
                    or parsed.query != "service=git-upload-pack"):
                fail("invalid_discovery_url")
        elif parsed.query:
            fail("url_query_forbidden")
        if not base and not discovery and parsed.path != "/metrics":
            fail("metrics_path_required")
    except ValueError:
        fail("invalid_url")
    return value.rstrip("/") if base else value


@dataclass(frozen=True)
class Config:
    prometheus_url: str
    metrics_url: str
    job: str
    instance: str
    expected_version: str
    max_wait: float = 60
    scrape_interval: float = 5
    http_timeout: float = 5
    clock_skew: float = 1
    discovery_url: str | None = None
    expected_profile: str | None = None

    def validate(self):
        url(self.prometheus_url, base=True)
        url(self.metrics_url)
        if self.discovery_url:
            url(self.discovery_url, discovery=True)
        for value in (self.job, self.instance):
            if not isinstance(value, str) or not 1 <= len(value) <= 256 or any(
                    ord(c) < 32 or ord(c) == 127 for c in value):
                fail("invalid_selector")
        if not isinstance(self.expected_version, str) or not VERSION.fullmatch(self.expected_version):
            fail("invalid_expected_version")
        if self.expected_profile is not None and not VERSION.fullmatch(self.expected_profile):
            fail("invalid_expected_profile")
        for value, lower, upper in ((self.max_wait, 0, 600), (self.scrape_interval, 0, 120),
                                    (self.http_timeout, 0, 30), (self.clock_skew, -1, 5)):
            if not lower < number(value, "invalid_budget") <= upper:
                fail("invalid_budget")
        if self.max_wait <= self.scrape_interval or self.http_timeout > self.max_wait:
            fail("invalid_budget")
        if self.clock_skew < 0:
            fail("invalid_budget")


class Clock:
    now = staticmethod(time.time)
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fail("redirect_forbidden")


class Http:
    def __init__(self):
        # No ambient proxy credentials, redirects, cookies, netrc or auth handlers.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def get(self, address, limit, timeout):
        # Socket timeouts alone do not bound DNS/header trickles. One daemon
        # worker bounds the entire GET, and any timeout aborts the probe (no retry).
        # At most one unresolved worker can remain when this one-shot CLI exits.
        result = queue.Queue(maxsize=1)

        def receive():
            try:
                result.put(self._get(address, limit, timeout))
            except ProbeError as error:
                result.put(error)
            except Exception:
                result.put(ProbeError("http_request_failed"))

        threading.Thread(target=receive, daemon=True).start()
        try:
            response = result.get(timeout=timeout)
        except queue.Empty:
            fail("http_timeout")
        if isinstance(response, ProbeError):
            raise response
        return response

    def _get(self, address, limit, timeout):
        request = urllib.request.Request(address, headers={"Accept": "application/json, text/plain"})
        try:
            with self.opener.open(request, timeout=timeout) as response:
                if response.status != 200:
                    fail("http_status_not_ok")
                declared = response.headers.get("Content-Length")
                if declared is not None and (not declared.isdecimal() or int(declared) > limit):
                    fail("oversized_response")
                # Chunked framing takes precedence over Content-Length. For
                # close-delimited responses there is no declared length to prove.
                expected_length = None if response.chunked or declared is None else int(declared)
                if response.headers.get("Content-Encoding", "identity") != "identity":
                    fail("encoded_response_forbidden")
                body = bytearray()
                deadline = time.monotonic() + timeout
                while True:
                    chunk = response.read1(min(16384, limit + 1 - len(body)))
                    if time.monotonic() > deadline:
                        fail("http_timeout")
                    body.extend(chunk)
                    if len(body) > limit:
                        fail("oversized_response")
                    if not chunk:
                        if expected_length is not None and len(body) != expected_length:
                            fail("http_response_truncated")
                        return bytes(body)
        except ProbeError:
            raise
        except (OSError, ValueError, urllib.error.URLError, http.client.HTTPException):
            fail("http_request_failed")


def schema():
    families = {}
    for name in ("build_info", "process_start_time_seconds", "active_requests",
                 "cache_allocated_bytes", "cache_waiters"):
        families["git_rs_" + name] = ("gauge", {})
    families["git_rs_build_info"] = ("gauge", {"version": None})
    for name in ("timing_logs_dropped_total", "service_logs_dropped_total"):
        families["git_rs_" + name] = ("counter", {})
    families["git_rs_requests_total"] = ("counter", {
        "transport": ("ssh", "http"),
        "outcome": ("success", "error", "unauthorized", "bad_request"),
    })
    families["git_rs_request_duration_seconds"] = ("histogram", {"transport": ("ssh", "http")})
    for prefix, label, values in (
        ("phase", "phase", PHASES), ("stage", "stage", STAGES),
        ("dependency", "operation", ("sql", "s3_get", "s3_put", "s3_delete", "s3_versioning")),
        ("subprocess", "program", ("git", "curl", "sha256sum", "other")),
    ):
        families[f"git_rs_{prefix}_duration_seconds"] = ("histogram", {label: values})
        families[f"git_rs_{prefix}_errors_total"] = ("counter", {label: values})
    families["git_rs_cache_lookups_total"] = ("counter", {"result": ("hit", "miss", "bypass")})
    families["git_rs_cache_wait_duration_seconds"] = ("histogram", {})
    families["git_rs_admission_rejections_total"] = ("counter", {"listener": ("ssh", "http", "health")})
    families["git_rs_s3_transferred_bytes_total"] = ("counter", {"direction": ("download", "upload")})
    return families


def key(name, labels):
    return name, tuple(sorted(labels.items()))


def label_sets(labels, version):
    names = sorted(labels)
    values = [(version,) if labels[name] is None else labels[name] for name in names]
    return [dict(zip(names, row)) for row in itertools.product(*values)]


def parse_labels(text):
    labels = {}
    position = 0
    while position < len(text):
        match = LABEL.match(text, position)
        if not match or match[1] in labels:
            fail("invalid_metric_labels")
        labels[match[1]] = re.sub(r'\\([\\"n])', lambda m: "\n" if m[1] == "n" else m[1], match[2])
        position = match.end()
        if position == len(text):
            break
        if text[position] != "," or position + 1 == len(text):
            fail("invalid_metric_labels")
        position += 1
    return labels


def exposition(body, expected_version, now, skew):
    if len(body) > MAX_METRICS:
        fail("oversized_response")
    try:
        text = body.decode("utf-8")
    except UnicodeError:
        fail("invalid_exposition")
    if not text.endswith("\n"):
        fail("invalid_exposition")
    types, helps, samples = {}, set(), {}
    for line in text.splitlines():
        if line.startswith("# TYPE "):
            parts = line.split()
            if len(parts) != 4 or parts[2] in types:
                fail("invalid_metric_type")
            types[parts[2]] = parts[3]
        elif line.startswith("# HELP "):
            parts = line.split(maxsplit=3)
            if len(parts) != 4 or not parts[3] or parts[2] in helps:
                fail("invalid_metric_help")
            helps.add(parts[2])
        elif line:
            match = SAMPLE.fullmatch(line)
            if not match:
                fail("invalid_exposition")
            labels = parse_labels(match[2] or "")
            identity = key(match[1], labels)
            if identity in samples:
                fail("duplicate_metric")
            if not re.fullmatch(r"[+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", match[3]):
                fail("invalid_metric_number")
            value = number(match[3], "nonfinite_metric")
            if value < 0:
                fail("negative_metric")
            samples[identity] = value
    expected, counters = set(), set()
    families = schema()
    if types != {name: kind for name, (kind, _) in families.items()} or helps != set(families):
        fail("metric_schema_mismatch")
    for name, (kind, dimensions) in families.items():
        for labels in label_sets(dimensions, expected_version):
            if kind == "histogram":
                cells = [key(name + "_bucket", dict(labels, le=bound)) for bound in BUCKETS]
                count = key(name + "_count", labels)
                identities = cells + [count, key(name + "_sum", labels)]
                if any(item not in samples for item in identities):
                    fail("missing_metric")
                values = [samples[item] for item in cells]
                if values != sorted(values) or values[-1] != samples[count]:
                    fail("invalid_histogram")
                if any(samples[item] != int(samples[item]) for item in cells + [count]):
                    fail("invalid_counter")
                counters.update(identities)
            else:
                identities = [key(name, labels)]
                if kind == "counter":
                    counters.update(identities)
            expected.update(identities)
    if set(samples) != expected:
        fail("metric_schema_mismatch")
    if any(samples[item] != int(samples[item]) or samples[item] >= 2 ** 53
           for item in counters if not item[0].endswith("_sum")):
        fail("invalid_counter")
    if samples[key("git_rs_build_info", {"version": expected_version})] != 1:
        fail("invalid_build_info")
    if any(samples[key("git_rs_" + name, {})] != int(samples[key("git_rs_" + name, {})])
           for name in ("active_requests", "cache_allocated_bytes", "cache_waiters")):
        fail("invalid_gauge")
    start = samples[key("git_rs_process_start_time_seconds", {})]
    if not 0 < start <= now + skew:
        fail("invalid_process_start")
    return samples, counters, start


def timestamp(value):
    if not isinstance(value, str) or len(value) > 64:
        fail("invalid_last_scrape")
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            fail("invalid_last_scrape")
        return number(parsed.timestamp())
    except (ValueError, OverflowError):
        fail("invalid_last_scrape")


class Probe:
    def __init__(self, config, http, clock):
        config.validate()
        self.config, self.http, self.clock = config, http, clock
        self.deadline = clock.monotonic() + config.max_wait
        self.identity = None
        self.start = None

    def get(self, address, limit=MAX_JSON):
        remaining = self.deadline - self.clock.monotonic()
        if remaining <= 0:
            fail("probe_deadline")
        body = self.http.get(address, limit, min(self.config.http_timeout, remaining))
        if self.clock.monotonic() >= self.deadline:
            fail("probe_deadline")
        if len(body) > limit:
            fail("oversized_response")
        return body

    def api(self, path, query=None):
        address = self.config.prometheus_url.rstrip("/") + "/api/v1/" + path
        if query:
            address += "?" + urllib.parse.urlencode(query)
        try:
            data = json.loads(self.get(address), object_pairs_hook=unique_object,
                              parse_constant=lambda _: fail("nonfinite_json"))
        except (ValueError, UnicodeError, RecursionError):
            fail("invalid_prometheus_json")
        if not isinstance(data, dict) or data.get("status") != "success" or data.get("warnings"):
            fail("prometheus_api_failed")
        return data.get("data")

    def recent(self, observed):
        now = self.clock.now()
        if observed > now + self.config.clock_skew:
            fail("future_timestamp")
        allowance = 2 * self.config.scrape_interval + self.config.http_timeout + self.config.clock_skew
        if observed <= 0 or now - observed > allowance:
            raise Pending("stale_timestamp")

    def target(self):
        data = self.api("targets", {"state": "active"})
        if not isinstance(data, dict) or not isinstance(data.get("activeTargets"), list):
            fail("invalid_targets")
        matches = []
        for target in data["activeTargets"]:
            if not isinstance(target, dict) or not isinstance(target.get("labels"), dict):
                fail("invalid_targets")
            labels = target["labels"]
            if labels.get("job") == self.config.job and labels.get("instance") == self.config.instance:
                matches.append(target)
        if len(matches) > 1:
            fail("duplicate_target")
        if not matches:
            fail("missing_target")
        target = matches[0]
        if target.get("scrapeUrl") != self.config.metrics_url:
            fail("wrong_target_url")
        labels = target["labels"]
        if (len(labels) > 64 or not all(
                isinstance(k, str) and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", k)
                and isinstance(v, str) and len(v) <= 1024 for k, v in labels.items())
                or not isinstance(target.get("scrapePool"), str)):
            fail("invalid_target_labels")
        identity = {"url": target["scrapeUrl"], "pool": target.get("scrapePool"), "labels": labels}
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        if self.identity is not None and self.identity != digest:
            fail("target_identity_changed")
        self.identity = digest
        if target.get("health") != "up" or target.get("lastError") != "":
            raise Pending("target_unhealthy")
        last = timestamp(target.get("lastScrape"))
        self.recent(last)
        return labels, last

    def query(self, expression, target_labels, local_labels, sample_time=False):
        data = self.api("query", {"query": expression})
        if not isinstance(data, dict) or data.get("resultType") != "vector":
            fail("invalid_query_result")
        rows = data.get("result")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            fail("missing_or_duplicate_sample")
        row = rows[0]
        labels, value = row.get("metric"), row.get("value")
        if not isinstance(labels, dict) or not isinstance(value, list) or len(value) != 2:
            fail("invalid_query_result")
        expected = dict(target_labels, **local_labels)
        if {k: v for k, v in labels.items() if k != "__name__"} != expected:
            fail("query_identity_mismatch")
        if number(value[0], "nonfinite_query_time") > self.clock.now() + self.config.clock_skew:
            fail("future_query_time")
        result = number(value[1], "nonfinite_sample")
        # For timestamp(metric), the actual scrape timestamp is the VALUE.
        if sample_time:
            self.recent(result)
        return result

    def observe(self):
        labels, last = self.target()
        samples, counters, start = exposition(self.get(self.config.metrics_url, MAX_METRICS),
                                              self.config.expected_version, self.clock.now(),
                                              self.config.clock_skew)
        if self.start is not None and start != self.start:
            fail("process_identity_changed")
        self.start = start
        selector = "job=" + json.dumps(self.config.job) + ",instance=" + json.dumps(self.config.instance)
        metrics = {
            "build": ("git_rs_build_info", {"version": self.config.expected_version}),
            "start": ("git_rs_process_start_time_seconds", {}),
            "success": ("git_rs_requests_total", {"transport": "http", "outcome": "success"}),
            "count": ("git_rs_request_duration_seconds_count", {"transport": "http"}),
        }
        if self.query("up{" + selector + "}", labels, {}) != 1:
            raise Pending("target_down")
        times, values = {}, {}
        for field, (name, local) in metrics.items():
            # Build query deliberately omits version: multiple versions must not be hidden.
            dimensions = {k: v for k, v in local.items() if k != "version"}
            suffix = "".join("," + k + "=" + json.dumps(v) for k, v in dimensions.items())
            expression = name + "{" + selector + suffix + "}"
            times[field] = self.query("timestamp(" + expression + ")", labels, local, True)
            values[field] = self.query(expression, labels, local)
        if values["build"] != 1 or values["start"] != start:
            fail("scraped_identity_mismatch")
        if any(observed + self.config.clock_skew < start for observed in times.values()):
            fail("sample_predates_process")
        for field in ("success", "count"):
            if values[field] < 0 or values[field] >= 2 ** 53 or values[field] != int(values[field]):
                fail("invalid_counter")
        # Sequential GETs can consume the freshness allowance after an earlier
        # response passed its check. Reject the entire expired observation and
        # let wait() retry under the original, unchanged deadline.
        self.recent(last)
        for observed in times.values():
            self.recent(observed)
        return {"last_scrape": last, "sample_times": times, "values": values,
                "raw": samples, "counters": counters}

    def wait(self, previous=None, delta=False):
        while True:
            try:
                observed = self.observe()
                if previous is not None:
                    if any(observed["raw"][item] < previous["raw"][item] for item in observed["counters"]):
                        fail("counter_reset_during_observation")
                    if (observed["last_scrape"] <= previous["last_scrape"] or any(
                            observed["sample_times"][name] <= old
                            for name, old in previous["sample_times"].items())):
                        raise Pending("scrape_not_advanced")
                    if delta:
                        changes = [observed["values"][name] - previous["values"][name]
                                   for name in ("success", "count")]
                        if any(change < 0 or change > 1 for change in changes):
                            fail("unexpected_request_delta")
                        if changes != [1, 1]:
                            raise Pending("request_delta_pending")
                return observed
            except Pending:
                remaining = self.deadline - self.clock.monotonic()
                if remaining <= 0:
                    fail("freshness_deadline")
                self.clock.sleep(min(self.config.scrape_interval, remaining))


def run_probe(config, http=None, clock=None):
    """Public protocol boundary; injected transport/clock permit deterministic fixtures."""
    probe = Probe(config, http or Http(), clock or Clock())
    first = probe.wait()
    second = probe.wait(first)
    delta = None
    if config.discovery_url:
        # One GET only, never retried: a retry could itself change request counters.
        probe.get(config.discovery_url, MAX_METRICS)
        last = probe.wait(second, delta=True)
        delta = {name: last["values"][name] - second["values"][name]
                 for name in ("success", "count")}
    else:
        last = second
    return {
        "format": 1, "status": "verified", "target_identity_sha256": probe.identity,
        "version": config.expected_version, "instrumentation_start_seconds": probe.start,
        "first_last_scrape": first["last_scrape"], "last_scrape": last["last_scrape"],
        "first_sample_timestamps": first["sample_times"], "sample_timestamps": last["sample_times"],
        "controlled_http_delta": delta,
        "budgets": {"max_wait_seconds": config.max_wait,
                    "scrape_interval_seconds": config.scrape_interval,
                    "http_timeout_seconds": config.http_timeout,
                    "clock_skew_seconds": config.clock_skew,
                    "freshness_allowance_seconds": 2 * config.scrape_interval + config.http_timeout + config.clock_skew},
        "observed_at_unix_seconds": probe.clock.now(),
        "caller_expected_profile": config.expected_profile,
        "profile_verification": "not_observable_in_current_exposition",
        "runtime_image_identity": "not_externally_bound",
        "scope": ["unique_healthy_explicit_target", "advancing_recent_scrapes_and_sample_timestamps",
                  "serving_exposition_closed_schema", "stable_version_and_instrumentation_start",
                  "no_observed_counter_decrease"] + (
                      ["one_discovery_get_and_exact_http_success_and_duration_count_deltas"] if delta else []),
        "not_verified": ["image_or_source_provenance", "effective_blame_profile", "no_native_execution",
                         "restart_counter_reset", "controlled_active_request_or_cache_gauge_change"] + (
                             [] if delta else ["controlled_http_request_delta"]),
    }


def receipt_path(path):
    path = Path(path)
    try:
        parent = path.parent.lstat()
    except OSError:
        fail("receipt_directory_not_private")
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_mode & 0o077
            or parent.st_uid != os.getuid()):
        fail("receipt_directory_not_private")
    if os.path.lexists(path):
        fail("receipt_already_exists")
    return path


def write_receipt(path, receipt):
    path = receipt_path(path)
    data = (json.dumps(receipt, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    try:
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            parent = os.fstat(directory)
            if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
                fail("receipt_directory_not_private")
            fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=directory)
            with os.fdopen(fd, "wb") as output:
                output.write(data)
        finally:
            os.close(directory)
    except OSError:
        fail("receipt_write_failed")


class Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default error includes rejected argv values.
        self.exit(2, "metrics freshness failed: invalid_arguments (use --help)\n")


def main():
    parser = Parser(description=__doc__)
    for name in ("prometheus-url", "metrics-url", "job", "instance", "expected-version", "output-receipt"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--expected-profile", help="caller receipt assertion only; not observable in current metrics")
    parser.add_argument("--discovery-url", help="one optional unauthenticated Git upload discovery GET")
    for name, default in (("max-wait", 60), ("scrape-interval", 5), ("http-timeout", 5), ("clock-skew", 1)):
        parser.add_argument("--" + name, default=default)
    args = vars(parser.parse_args())
    output = args.pop("output_receipt")
    try:
        receipt_path(output)
        for name in ("max_wait", "scrape_interval", "http_timeout", "clock_skew"):
            args[name] = number(args[name], "invalid_budget")
        receipt = run_probe(Config(**args))
        write_receipt(output, receipt)
    except ProbeError as error:
        print("metrics freshness failed: " + str(error))
        return 1
    except (OSError, ValueError, TypeError):
        print("metrics freshness failed: invalid_input_or_receipt")
        return 1
    print("metrics freshness verified; private receipt written; image/profile provenance not verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
