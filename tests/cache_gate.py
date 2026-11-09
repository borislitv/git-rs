#!/usr/bin/env python3
"""Run prebuilt cache gates inside an independently bounded systemd unit."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
FAILURE_EXCERPT_BYTES = 16 * 1024


def failed_child_excerpt(log, env, error, elapsed):
    """Only expose this synthetic gate's bounded log tail, never its environment."""
    print(f"Cache gate child failed: log={log} elapsed={elapsed:.3f}s "
          f"exception={type(error).__name__}", flush=True)
    try:
        with log.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            stream.seek(max(0, size - FAILURE_EXCERPT_BYTES))
            text = stream.read(FAILURE_EXCERPT_BYTES).decode("utf-8", errors="replace")
        for key, value in env.items():
            if len(value) >= 8 and re.search(r"TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH|ACCESS_KEY", key, re.I):
                text = text.replace(value, "[REDACTED]")
        # Escape terminal controls and workflow-command syntax onto one prefixed
        # line. Cap the encoded excerpt too, including pathological binary logs.
        escaped = json.dumps(text, ensure_ascii=True)
        excerpt = escaped[-FAILURE_EXCERPT_BYTES:]
        print(f"Cache gate log tail (last {min(size, FAILURE_EXCERPT_BYTES)} of {size} bytes; "
              f"escaped excerpt <= {FAILURE_EXCERPT_BYTES} bytes):", flush=True)
        print("cache-gate | " + excerpt, flush=True)
    except OSError as diagnostic_error:
        print(f"Cache gate log unavailable: {type(diagnostic_error).__name__}", flush=True)


def owner_label():
    invocation = os.environ.get("INVOCATION_ID", "")
    if not re.fullmatch(r"[0-9a-f]{32}", invocation):
        raise ValueError("run inside systemd-run with RuntimeMaxSec and ExecStopPost="
                         "'/usr/bin/python3 <checkout>/tests/cache_gate.py cleanup'; "
                         "a systemd INVOCATION_ID (32 lowercase hex digits) is required")
    return "git-rs.cache-gate=" + invocation


def cleanup(label):
    command = ["docker", "ps", "-aq", "--filter", "label=" + label]
    ids = subprocess.run(command, check=True, capture_output=True, text=True,
                         timeout=10).stdout.split()
    if ids:
        subprocess.run(["docker", "rm", "-f", *ids], check=True, timeout=20)
    if subprocess.run(command, check=True, capture_output=True, timeout=10).stdout.strip():
        raise RuntimeError("owned cache containers remain after cleanup")


def executable(path):
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError(f"prebuilt executable required: {path}; run make cache-build first")
    return path


def contracts(debug, name):
    candidates = sorted(path for path in (debug / "deps").glob(name + "-*")
                        if path.is_file() and os.access(path, os.X_OK)
                        and re.fullmatch(name + r"-[0-9a-f]+", path.name))
    if len(candidates) != 1:
        raise ValueError(f"expected exactly one executable for {name} in {debug / 'deps'}, "
                         f"found {candidates}; build into a fresh CARGO_TARGET_DIR")
    return candidates[0]


def child(command, env, log, deadline):
    started = time.monotonic()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("cache gate absolute deadline expired")
    try:
        with log.open("xb") as output:
            process = subprocess.Popen(command, env=env, cwd=ROOT, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                status = process.wait(timeout=remaining)
                if status:
                    raise RuntimeError(f"cache gate exited {status}; see {log}")
            finally:
                # Also reap descendants left behind by an exited controller child.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
    except BaseException as error:
        failed_child_excerpt(log, env, error, time.monotonic() - started)
        raise


def run(args, label):
    debug = args.target_dir.resolve() / "debug"
    binary = executable(debug / "git-rs")
    example = executable(debug / "examples/cache-process")
    suites = [(flag, contracts(debug, name)) for flag, name in (
        ("--contract-binary", "cache_contract"),
        ("--coordinator-contract-binary", "cache_coordinator"),
        ("--budget-contract-binary", "cache_budget"))]
    directory = ROOT / ".local" / ("cache-gate-" + os.environ["INVOCATION_ID"])
    directory.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GIT_RS_TEST_OWNER_LABEL=label)
    common = ["--provision", "--binary", str(binary), "--process-binary", str(example)]
    deadline = time.monotonic() + 840
    commands = [
        [sys.executable, "-B", str(ROOT / "tests/derived_cache.py"), *common,
         *[str(arg) for pair in suites for arg in pair]],
        [sys.executable, "-B", str(ROOT / "tests/cache_storage_cost_test.py")],
        [sys.executable, "-B", str(ROOT / "tests/cache_storage_cost.py"), *common],
    ]
    report = {"completed": False, "owner_label": label, "commands": commands, "stages": []}
    try:
        for index, command in enumerate(commands):
            stage = {"script": Path(command[2]).name, "log": f"{index}.log", "completed": False}
            report["stages"].append(stage)
            started = time.monotonic()
            try:
                child(command, env, directory / stage["log"], deadline)
                stage["completed"] = True
            except BaseException as error:
                stage["exception"] = type(error).__name__
                raise
            finally:
                stage["elapsed_seconds"] = time.monotonic() - started
        report["completed"] = True
    finally:
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"Cache gate reports: {directory}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "cleanup"))
    parser.add_argument("--target-dir", type=Path,
                        default=Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")))
    args = parser.parse_args()
    label = owner_label()
    if args.command == "cleanup":
        cleanup(label)
        return

    def interrupted(signum, _frame):
        raise InterruptedError(f"cache gate interrupted by {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        run(args, label)
    finally:
        cleanup(label)


if __name__ == "__main__":
    main()
