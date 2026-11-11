#!/usr/bin/env python3
"""Bound legacy Docker workers and transfer two commit-bound CI images."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile

import blame_gate as gate

ROOT = gate.ROOT
TARGETS = ("test-fixture-build", "test")
ENV = dict(os.environ, DOCKER_BUILDKIT="0", DOCKER_HOST="unix:///var/run/docker.sock")
ENV.pop("DOCKER_CONTEXT", None)


def run(argv, timeout=20, **kwargs):
    return subprocess.run(list(map(str, argv)), env=ENV, timeout=timeout, **kwargs)


def output(argv, timeout=20):
    return run(argv, timeout, check=True, capture_output=True, text=True).stdout.strip()


def slice_name(value):
    gate.require(re.fullmatch(r"git-rs-blame-images-[a-zA-Z0-9_-]{8,80}\.slice", value),
                 "dedicated image-worker slice required")
    return value


def workers(slice_value):
    ids = output(["docker", "ps", "-aq", "--no-trunc"]).split()
    gate.require(all(re.fullmatch(r"[0-9a-f]{64}", item) for item in ids), "invalid container IDs")
    if not ids:
        return []
    result = run(["docker", "inspect", *ids], capture_output=True, text=True)
    if result.returncode:
        # A build's --force-rm may race inspection, but daemon/access failures
        # must not be mistaken for an empty owned set.
        lines = result.stderr.splitlines()
        gate.require(lines and all(re.fullmatch(
            r"(?:Error response from daemon: No such container:|error: no such object:) [0-9a-f]{12,64}",
            line) for line in lines), "container inspection failed")
    return [row["Id"] for row in json.loads(result.stdout)
            if row["HostConfig"].get("CgroupParent") == slice_value]


def cleanup(value):
    value = slice_name(value)
    ids = workers(value)
    gate.require(all(re.fullmatch(r"[0-9a-f]{64}", item) for item in ids), "invalid worker IDs")
    if ids:
        run(["docker", "rm", "-f", "-v", *ids], timeout=20, check=True)
    gate.require(not workers(value), "owned image workers remain")


def commit(value):
    gate.require(re.fullmatch(r"[0-9a-f]{40}", value), "full commit required")
    gate.require(output(["git", "--no-optional-locks", "-C", ROOT, "rev-parse", "HEAD"]) == value,
                 "image bundle commit differs from checkout")
    return value


def image_id(value):
    gate.require(re.fullmatch(r"sha256:[0-9a-f]{64}", value), "immutable image ID required")
    return value


def build(args):
    gate.verify_unit(args.unit, slice_name(args.slice),
                     cleanup_command="blame_images.py cleanup --slice " + args.slice)
    gate.checkout_identity(ROOT)
    help_text = output(["docker", "build", "--help"])
    for option in ("--cgroup-parent", "--cpu-period", "--cpu-quota", "--cpuset-cpus",
                   "--memory-swap", "--force-rm", "--iidfile", "--network", "--target"):
        gate.require(option in help_text, "legacy Docker build does not support " + option)
    args.directory.mkdir(parents=True, exist_ok=True)
    iid = args.directory / (args.target + ".iid")
    gate.require(not iid.exists(), "image identity already exists")
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
    try:
        run(["docker", "build", "--target", args.target, "--tag", args.tag,
             "--network", "host", "--cgroup-parent", args.slice, "--cpuset-cpus", cpus,
             "--cpu-period", "100000", "--cpu-quota", "180000", "--memory", "3712m",
             "--memory-swap", "3712m", "--force-rm", "--iidfile", iid, ROOT],
            timeout=840, check=True)
        image_id(iid.read_text().strip())
    finally:
        cleanup(args.slice)


def export(args):
    revision = commit(args.commit)
    ids = {target: image_id((args.directory / (target + ".iid")).read_text().strip())
           for target in TARGETS}
    archive = args.directory / "images.tar"
    gate.require(not archive.exists(), "image archive already exists")
    run(["docker", "image", "save", "--output", archive, *ids.values()], timeout=180, check=True)
    manifest = {"format": 1, "commit": revision, "images": ids,
                "dockerfile_sha256": gate.digest(ROOT / "Dockerfile"),
                "archive_sha256": gate.digest(archive), "archive_bytes": archive.stat().st_size}
    (args.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


def load(args):
    revision = commit(args.commit)
    manifest_file = args.directory / "manifest.json"
    gate.require(manifest_file.stat().st_size <= 64 * 1024, "image bundle manifest too large")
    manifest = json.loads(manifest_file.read_text())
    gate.require(manifest["format"] == 1 and manifest["commit"] == revision, "wrong image bundle commit")
    gate.require(manifest["dockerfile_sha256"] == gate.digest(ROOT / "Dockerfile"), "wrong Dockerfile")
    gate.require(set(manifest["images"]) == set(TARGETS), "wrong image targets")
    ids = {target: image_id(manifest["images"][target]) for target in TARGETS}
    gate.require(len(set(ids.values())) == 2, "image targets must be distinct")
    archive = args.directory / "images.tar"
    gate.require(archive.stat().st_size == manifest["archive_bytes"]
                 and gate.digest(archive) == manifest["archive_sha256"], "image archive checksum mismatch")
    # Inspect only metadata/config bytes, never extract archive paths. Reject a
    # declared-ID substitution before importing anything into the daemon.
    with tarfile.open(archive, "r:") as stream:
        def metadata(name, limit):
            member = stream.getmember(name)
            gate.require(member.isfile() and 0 <= member.size <= limit, "invalid archive metadata")
            return stream.extractfile(member).read()
        records = json.loads(metadata("manifest.json", 64 * 1024))
        gate.require(len(records) == 2 and all(not row.get("RepoTags") for row in records),
                     "archive must contain only two untagged image IDs")
        actual = {"sha256:" + hashlib.sha256(metadata(row["Config"], 1024 * 1024)).hexdigest()
                  for row in records}
        identities = [actual]
        if "index.json" in stream.getnames():
            # Docker's containerd image store exposes the OCI manifest digest
            # as .Id; the classic store uses the config digest. Validate both
            # archive representations, then require the daemon's exact ID below.
            index = json.loads(metadata("index.json", 64 * 1024))
            descriptors = index["manifests"]
            gate.require(len(descriptors) == 2, "wrong OCI image inventory")
            configs, manifests = set(), set()
            for descriptor in descriptors:
                identity = image_id(descriptor["digest"])
                data = metadata("blobs/sha256/" + identity.removeprefix("sha256:"), 64 * 1024)
                gate.require("sha256:" + hashlib.sha256(data).hexdigest() == identity,
                             "OCI manifest digest mismatch")
                configs.add(json.loads(data)["config"]["digest"])
                manifests.add(identity)
            gate.require(configs == actual, "OCI configs differ from archive inventory")
            identities.append(manifests)
    gate.require(set(ids.values()) in identities, "archive image IDs differ from declaration")
    run(["docker", "image", "load", "--input", archive], timeout=180, check=True)
    for identity in ids.values():
        gate.require(output(["docker", "image", "inspect", "--format", "{{.Id}}", identity]) == identity,
                     "loaded image identity mismatch")
    with args.env_file.open("a") as stream:
        stream.write("BLAME_GATE_BUILDER_IMAGE=" + ids["test-fixture-build"] + "\n")
        stream.write("BLAME_GATE_IMAGE=" + ids["test"] + "\n")
    print(json.dumps({"verified_commit": revision, "images": ids}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    builder = sub.add_parser("build")
    builder.add_argument("--target", choices=TARGETS, required=True)
    builder.add_argument("--tag", required=True)
    builder.add_argument("--unit", required=True)
    builder.add_argument("--slice", required=True)
    builder.add_argument("--directory", type=Path, required=True)
    for action in ("export", "load"):
        command = sub.add_parser(action)
        command.add_argument("--directory", type=Path, required=True)
        command.add_argument("--commit", required=True)
        if action == "load":
            command.add_argument("--env-file", type=Path, required=True)
    cleaner = sub.add_parser("cleanup")
    cleaner.add_argument("--slice", required=True)
    args = parser.parse_args()
    {"build": build, "export": export, "load": load,
     "cleanup": lambda value: cleanup(value.slice)}[args.action](args)


if __name__ == "__main__":
    main()
