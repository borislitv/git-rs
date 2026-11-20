"""Check that the public unchanged-parent contract detects a disabled optimization."""
import argparse
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]
CASE = "blame_preflight_unchanged_child_matches_full_native_parent_porcelain"


def check(commit):
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if head != commit:
        raise ValueError("control requires the checked-out commit")
    with tempfile.TemporaryDirectory(dir=os.environ.get("RUNNER_TEMP")) as tmp:
        src = Path(tmp) / "source"
        src.mkdir()
        with tempfile.TemporaryFile(dir=tmp) as archive:
            subprocess.run(["git", "archive", "--format=tar", head], cwd=ROOT, stdout=archive,
                           check=True, timeout=30)
            archive.seek(0)
            with tarfile.open(fileobj=archive) as tree:
                tree.extractall(src, filter="data")

        file = src / "crates/git-core/src/blame.rs"
        original = file.read_text()
        success = "        Ok(Some(parent))\n    }"
        if original.count(success) != 1:
            raise ValueError("review the location of the one-edge optimization control")
        cmd = ["cargo", "test", "--locked", "--offline", "--color", "never", "-j2",
               "-p", "git-rs-core", "--test", "blame", "--", CASE, "--exact"]
        env = dict(os.environ, CARGO_TARGET_DIR=str(Path(tmp) / "target"), RUST_BACKTRACE="0")
        for disable in (True, False):
            file.write_text(original.replace(success, "        Ok(None)\n    }") if disable else original)
            result = subprocess.run(cmd, cwd=src, env=env, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True, timeout=300)
            expected = ("0 passed; 1 failed", "unchanged child must resolve to its parent",
                        "left: None", "right: Some(") if disable else ("1 passed; 0 failed",)
            if (result.returncode != (101 if disable else 0)
                    or not all(s in result.stdout for s in (CASE, "running 1 test", *expected))):
                raise RuntimeError(result.stdout[-8192:])
            print(f"{head}: optimization {'disabled: expected assertion failed' if disable else 'restored: contract passed'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    check(parser.parse_args().commit)
