#!/usr/bin/env python3
"""External native Git client for the synthetic Helm fixture.

Only the client pod contains this program, native Git, credentials and client
repositories. Backend pods expose the production Git-free runtime unchanged.
"""
import argparse
import base64
import http.client
import json
import os
from pathlib import Path
import shlex
import subprocess


BASE = Path("/work/oracle")
SECRETS = Path("/oracle-secrets")
PAYLOAD = b"synthetic Kubernetes/SSH payload\n" * 16384
HTTP_PAYLOAD = b"authenticated chart HTTP push\n"


def command(arguments, *, env=None, data=None, ok=True):
    result = subprocess.run(list(map(str, arguments)), input=data, env=env,
                            capture_output=True, timeout=90)
    if ok and result.returncode:
        raise AssertionError(f"external oracle command failed: {arguments[:2]}: {result.stderr[-4000:]!r}")
    return result


def environment(args, *, unknown=False, http=False):
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "LANG"}}
    env.update(HOME=str(BASE), LC_ALL="C", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Test Author", GIT_COMMITTER_NAME="Test Author",
               GIT_AUTHOR_EMAIL="author@example.invalid", GIT_COMMITTER_EMAIL="author@example.invalid")
    key = BASE / ("unknown-key" if unknown else "client-key")
    env["GIT_SSH_COMMAND"] = shlex.join([
        "ssh", "-p", str(args.ssh_port), "-i", str(key), "-o", "IdentitiesOnly=yes",
        "-o", "IdentityAgent=none", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "UserKnownHostsFile=" + str(SECRETS / "known-hosts"),
    ])
    if http:
        credential = (SECRETS / "http-credentials").read_bytes().strip()
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                   GIT_CONFIG_VALUE_0="Authorization: Basic " + base64.b64encode(credential).decode())
    return env


def git(path, *arguments, env, ok=True):
    return command(["/usr/bin/git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false",
                    "-C", path, *arguments], env=env, ok=ok)


def clone(url, name, env, *, http=False):
    path = BASE / name
    command(["/usr/bin/git", "clone", url, path], env=env)
    assert (path / "payload.txt").read_bytes() == PAYLOAD
    if http:
        assert (path / "http.txt").read_bytes() == HTTP_PAYLOAD
    git(path, "fsck", "--full", env=env)
    return git(path, "rev-parse", "HEAD", env=env).stdout.decode().strip()


def exercise(args):
    env = environment(args)
    ssh_url = f"git-rs@{args.address}:team/project.git"
    http_url = f"http://{args.address}:{args.http_port}/team/project.git"
    state_path = BASE / "state.json"
    if args.phase == "probes":
        probes = []
        assert len(args.maintenance_address) == 2
        for address in args.maintenance_address:
            observed = {}
            for path in ("/readyz", "/leaderz"):
                peer = http.client.HTTPConnection(address, 8081, timeout=3)
                try:
                    peer.request("GET", path)
                    response = peer.getresponse()
                    observed[path] = response.status
                    assert len(response.read(8193)) <= 8192
                finally:
                    peer.close()
            probes.append({"address": address, **observed})
        return {"phase": args.phase, "probes": probes}
    if args.phase == "bucket":
        assert not BASE.exists()
        BASE.mkdir(mode=0o700)
        assert command(["/usr/bin/git", "--version"], env=env).stdout == b"git version 2.56.0\n"
        assert command(["/usr/bin/git", "--exec-path"], env=env).stdout.strip() == b"/usr/lib/git-core"
        assert Path("/usr/lib/git-core/git-remote-http").is_file()
        for name in ("client-key", "unknown-key"):
            descriptor = os.open(BASE / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write((SECRETS / name).read_bytes())
        configuration = (f'url = "{args.s3_endpoint}/packs"\n'
                         'aws-sigv4 = "aws:amz:us-east-1:s3"\n'
                         'user = "test-access:synthetic-secret-key"\n').encode()
        command(["curl", "--disable", "--silent", "--fail", "--max-time", "10", "--cacert",
                 SECRETS / "ca.crt", "--request", "PUT", "--config", "-"], data=configuration, env=env)
        return {"phase": args.phase, "git": "2.56.0"}
    if args.phase == "ssh":
        source = BASE / "source"
        command(["/usr/bin/git", "init", "-b", "main", source], env=env)
        (source / "payload.txt").write_bytes(PAYLOAD)
        git(source, "add", ".", env=env)
        git(source, "commit", "-m", "Synthetic chart smoke", env=env)
        git(source, "push", ssh_url, "main", env=env)
        expected = git(source, "rev-parse", "HEAD", env=env).stdout.decode().strip()
        assert clone(ssh_url, "ssh-clone", env) == expected
        rejected = command(["/usr/bin/git", "ls-remote", ssh_url],
                           env=environment(args, unknown=True), ok=False)
        assert rejected.returncode and b"Permission denied" in rejected.stderr, rejected.stderr
        state_path.write_text(json.dumps({"head": expected}))
    elif args.phase == "http":
        http_env = environment(args, http=True)
        assert clone(http_url, "http-clone", http_env) == json.loads(state_path.read_text())["head"]
        source = BASE / "http-clone"
        (source / "http.txt").write_bytes(HTTP_PAYLOAD)
        git(source, "add", "http.txt", env=http_env)
        git(source, "commit", "-m", "Synthetic HTTP chart push", env=http_env)
        git(source, "push", "origin", "main", env=http_env)
        expected = git(source, "rev-parse", "HEAD", env=http_env).stdout.decode().strip()
        assert clone(http_url, "http-pushed", http_env, http=True) == expected
        for header in (None, "X-Forwarded-User: alice", "Authorization: Basic " + base64.b64encode(
                b"alice:wrong-chart-token-0123456789abcdef").decode()):
            negative_env = dict(env)
            if header is not None:
                negative_env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader", GIT_CONFIG_VALUE_0=header)
            rejected = command(["/usr/bin/git", "ls-remote", http_url], env=negative_env, ok=False)
            assert rejected.returncode and (b"401" in rejected.stderr or b"could not read Username" in rejected.stderr), rejected.stderr
        history = git(source, "rev-list", "--topo-order", "main", env=http_env).stdout.decode().splitlines()
        state_path.write_text(json.dumps({"head": expected, "history": history}))
    elif args.phase == "cold":
        expected = json.loads(state_path.read_text())["head"]
        assert clone(ssh_url, "ssh-cold-clone", env, http=True) == expected
        assert clone(http_url, "http-cold-clone", environment(args, http=True), http=True) == expected
    elif args.phase == "upgrade":
        expected = json.loads(state_path.read_text())["head"].encode()
        for url, client_env in ((ssh_url, env), (http_url, environment(args, http=True))):
            rows = command(["/usr/bin/git", "ls-remote", url, "refs/heads/main"], env=client_env).stdout.split()
            assert rows == [expected, b"refs/heads/main"], rows
    return {"phase": args.phase, **json.loads(state_path.read_text())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("bucket", "ssh", "http", "cold", "upgrade", "probes"))
    parser.add_argument("--address", required=True)
    parser.add_argument("--ssh-port", type=int, required=True)
    parser.add_argument("--http-port", type=int, required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--maintenance-address", action="append", default=[])
    print(json.dumps(exercise(parser.parse_args())))


if __name__ == "__main__":
    main()
