#!/usr/bin/env python3
"""Exercise the running demo with synthetic native Git clients; keep proof locally."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


STATE = Path(__file__).resolve().parent / ".state"
URL = "http://127.0.0.1:18080/demo/repo.git"
ENV = dict(os.environ, LC_ALL="C", GIT_CONFIG_NOSYSTEM="1",
           GIT_CONFIG_GLOBAL="/dev/null", GIT_TERMINAL_PROMPT="0")


def command(*args, cwd=None):
    return subprocess.run(args, cwd=cwd, env=ENV, check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=120).stdout


def git(path, *args):
    return command("git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false",
                   "-c", "init.templateDir=", "-C", str(path), *args)


def compare_blame(path, commit, output, url):
    start = time.monotonic()
    server = command("curl", "--disable", "--fail-with-body", "--silent", "--show-error",
                     "--max-time", "120", "--get", "--data-urlencode", "path=hello.txt",
                     url + "/blame/" + commit)
    elapsed = time.monotonic() - start
    native = git(path, "blame", "--root", "--line-porcelain", "--diff-algorithm=myers",
                 "--no-textconv", commit, "--", "hello.txt")
    (output / "server-blame.porcelain").write_bytes(server)
    (output / "native-blame.porcelain").write_bytes(native)
    if server != native:
        raise RuntimeError("Complete server/native blame bytes differ; inspect retained output")
    print(f"PASS complete blame byte parity: {len(server)} bytes; HTTP elapsed {elapsed:.3f}s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-existing", action="store_true",
                        help="verify saved refs, a new clone, and blame after stop/up")
    parser.add_argument("--url", default=URL, help="empty repository URL for the initial smoke")
    args = parser.parse_args()
    os.umask(0o077)
    if not STATE.is_dir():
        parser.error("Run playground.py up first")
    print(command("git", "--version").decode().strip())
    record = STATE / "smoke.json"
    if args.verify_existing:
        saved = json.loads(record.read_text())
        root = STATE / saved["directory"]
        clone = Path(tempfile.mkdtemp(prefix="restarted-", dir=root))
        command("git", "-c", "init.templateDir=", "clone", saved["url"], str(clone))
        if git(clone, "rev-parse", "HEAD").decode().strip() != saved["commit"]:
            raise RuntimeError("Published HEAD changed across restart")
        git(clone, "fsck", "--full")
        compare_blame(clone, saved["commit"], clone, saved["url"])
        print("PASS restart persistence: fresh clone, same HEAD, fsck and blame")
        return
    if command("git", "ls-remote", args.url).strip():
        parser.error("smoke needs an empty demo/repo; use the manual README flow for existing history")
    root = Path(tempfile.mkdtemp(prefix="smoke-", dir=STATE))
    first, second = root / "first", root / "second"
    first.mkdir()
    git(first, "init", "-b", "main")
    git(first, "config", "user.name", "Playground Alice")
    git(first, "config", "user.email", "alice@example.invalid")
    (first / "hello.txt").write_text("hello from git-rs\nsecond line\n")
    git(first, "add", "hello.txt")
    git(first, "commit", "-m", "Add greeting")
    git(first, "remote", "add", "origin", args.url)
    git(first, "push", "-u", "origin", "main")
    command("git", "-c", "init.templateDir=", "clone", args.url, str(second))
    git(second, "config", "user.name", "Playground Bob")
    git(second, "config", "user.email", "bob@example.invalid")
    with (second / "hello.txt").open("a") as output:
        output.write("third line from another checkout\n")
    git(second, "add", "hello.txt")
    git(second, "commit", "-m", "Extend greeting")
    git(second, "push", "origin", "main")
    commit = git(second, "rev-parse", "HEAD").decode().strip()
    git(first, "fetch", "origin")
    git(first, "pull", "--ff-only")
    if git(first, "rev-parse", "HEAD").decode().strip() != commit:
        raise RuntimeError("Pull did not publish the second client's commit")
    if (first / "hello.txt").read_bytes() != (second / "hello.txt").read_bytes():
        raise RuntimeError("Client contents differ after pull")
    git(first, "fsck", "--full")
    record.write_text(json.dumps({"directory": root.name, "commit": commit, "url": args.url}) + "\n")
    compare_blame(first, commit, root, args.url)
    print(f"PASS push/clone/second push/fetch/pull/fsck; HEAD {commit}")
    print(f"Proof retained in {root}")


if __name__ == "__main__":
    main()
