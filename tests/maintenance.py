#!/usr/bin/env python3
"""Real elected source-worker faults over prebuilt Git-free containers.

The external supervisor owns Kubernetes 1.37, aggregate limits and independent
cleanup. This client owns only synthetic repositories and a named-Lease account.
"""
import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from urllib.parse import quote
from xml.etree import ElementTree

from failures import CommitGateway, FaultGateway
from integration import Fixture, ROOT, command, raw_push


def until(check, description, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("timed out: " + description)


def worker(f, name, **overrides):
    environment = {**f.env, **f.lease_fixture.environment,
                   "GIT_RS_POD_UID": "fixture-" + uuid.uuid4().hex, **overrides}
    log = (f.base / (name + ".log")).open("xb")
    f.logs.append(log)
    process = f.runtime.start(["maintenance", "--scratch", str(f.server / name),
                               "--health", "127.0.0.1:0"], environment, log,
                              service_account=f.lease_fixture.account)
    f.processes.append(process)
    return process, environment["GIT_RS_POD_UID"]


def stop(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=35)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise AssertionError("elected worker did not drain within the fixture stop bound")


def catalog_objects(f, label, slug="maintenance/source"):
    directory = f.base / label
    directory.mkdir(mode=0o700)
    rows = f.sql("SELECT native_hash,pack_key,idx_key FROM git_rs.packs WHERE repo_id="
                 f"(SELECT id FROM git_rs.repositories WHERE slug='{slug}') ORDER BY native_hash")
    objects = set()
    for row in rows.splitlines():
        identity, pack, index = row.split("|")
        for key, extension in ((pack, ".pack"), (index, ".idx")):
            body = f.s3("GET", key).stdout
            assert len(body) < 1024 * 1024, "keep the external physical-object oracle small"
            path = directory / ("pack-" + identity + extension)
            path.write_bytes(body)
            path.chmod(0o600)
        result = f.git(directory, "verify-pack", "-v", directory / ("pack-" + identity + ".idx")).stdout
        for line in result.splitlines():
            fields = line.split()
            if fields and len(fields[0]) == 40 and all(byte in b"0123456789abcdef" for byte in fields[0]):
                objects.add(fields[0])
    return objects


def pending_uploads(f, repo, *, prefix=None):
    prefix = prefix or f"repos/{repo}/operations/"
    assert prefix.startswith(f"repos/{repo}/operations/")
    response = f.s3("GET", "?uploads&max-uploads=4&prefix=" +
                    quote(prefix, safe="/")).stdout
    assert len(response) <= 16384, "synthetic multipart listing exceeded its bound"
    listing = ElementTree.fromstring(response)
    assert listing.findtext("{*}IsTruncated") == "false", "multipart oracle must observe the complete small inventory"
    return {(item.findtext("{*}Key"), item.findtext("{*}UploadId"))
            for item in listing.findall("{*}Upload")}


def multipart_faults(f):
    print("multipart crash outcomes, embedded errors and retired-upload reconciliation", flush=True)
    slug = "maintenance/multipart"
    f.cli("create", slug, "alice")
    source = f.base / "multipart-source"
    f.git(f.base, "init", "--initial-branch=main", source)
    _, port = f.start("multipart-publisher")
    url = f"http://127.0.0.1:{port}/{slug}.git"
    for ordinal in range(8):
        (source / "data.txt").write_text(f"multipart synthetic generation {ordinal}\n")
        f.git(source, "add", ".")
        f.git(source, "commit", "-qm", f"Multipart generation {ordinal}")
        f.git(source, "push", url, "main")
    repo = f.sql("SELECT id FROM git_rs.repositories WHERE slug='maintenance/multipart'")
    job = f.sql(f"SELECT id FROM git_rs.maintenance_jobs WHERE repo_id='{repo}' AND kind='compact'")
    assert len(job.splitlines()) == 1
    f.sql(f"UPDATE git_rs.maintenance_jobs SET priority=100 WHERE id='{job}'")
    catalog = f.sql(f"SELECT native_hash,pack_key,idx_key FROM git_rs.packs WHERE repo_id='{repo}' ORDER BY native_hash")
    advertised = f.git(source, "ls-remote", url).stdout
    physical = catalog_objects(f, "multipart-before", slug)
    foreign_key = f"repos/{repo}/operations/{uuid.uuid4()}/unretired.pack"
    gateway = None
    foreign_upload = None
    foreign_started = False
    try:
        gateway = FaultGateway(f.env["GIT_RS_S3_ENDPOINT"])
        foreign_started = True
        initiated = f.s3("POST", foreign_key + "?uploads").stdout
        assert len(initiated) <= 16384
        foreign_upload = ElementTree.fromstring(initiated).findtext("{*}UploadId")
        assert foreign_upload

        def dispatch(name):
            environment = {**f.env, **f.lease_fixture.environment,
                "GIT_RS_POD_UID": "fixture-" + uuid.uuid4().hex,
                "GIT_RS_S3_ENDPOINT": gateway.endpoint}
            result = f.runtime.execute(["maintenance", "--scratch", str(f.server / name),
                "--health", "127.0.0.1:0", "--once", "--wait", "30"], environment,
                timeout=90, service_account=f.lease_fixture.account)
            (f.base / (name + ".log")).write_bytes(result.stdout + result.stderr)

        output_keys = []
        unknown_uploads = set()
        for generation, mode in enumerate(("unknown-multipart-init", "multipart-complete-error", "unknown-idx-complete"), 1):
            until(lambda: f.sql(f"SELECT available_at<=clock_timestamp() FROM git_rs.maintenance_jobs WHERE id='{job}'") == "t",
                  "actual multipart retry backoff", 30)
            gateway.arm(mode)
            dispatch("multipart-fault-" + str(generation))
            assert gateway.reached.is_set(), (mode, "actual multipart boundary was not reached")
            state = f.sql(f"SELECT state,attempts,claim_generation,error_code FROM git_rs.maintenance_jobs WHERE id='{job}'")
            assert state == f"retry|{generation}|{generation}|worker_failed", (mode, state)
            token = f.sql(f"SELECT owner_token FROM git_rs.maintenance_job_results WHERE job_id='{job}' AND claim_generation={generation} AND outcome='retry'")
            keys = f.sql(f"SELECT object_key FROM git_rs.staged_objects WHERE repo_id='{repo}' AND operation_id='{token}' AND state='planned' ORDER BY object_key").splitlines()
            assert len(keys) == 2, (mode, keys)
            output_keys.extend(keys)
            assert f.sql(f"SELECT count(*) FROM git_rs.maintenance_operations WHERE job_id='{job}'") == "0"
            assert f.sql(f"SELECT native_hash,pack_key,idx_key FROM git_rs.packs WHERE repo_id='{repo}' ORDER BY native_hash") == catalog
            assert f.git(source, "ls-remote", url).stdout == advertised
            if mode == "unknown-multipart-init":
                unknown_uploads = pending_uploads(f, repo) - {(foreign_key, foreign_upload)}
                assert len(unknown_uploads) == 1 and next(iter(unknown_uploads))[0] in keys
            elif mode == "unknown-idx-complete":
                assert all(f.s3("HEAD", key).stdout == b"200" for key in keys), "unknown Complete must retain both actual object intents"
        gateway.reset()
        until(lambda: f.sql(f"SELECT available_at<=clock_timestamp() FROM git_rs.maintenance_jobs WHERE id='{job}'") == "t",
              "final multipart retry backoff", 30)
        dispatch("multipart-recovery")
        assert f.sql(f"SELECT state,attempts,claim_generation FROM git_rs.maintenance_jobs WHERE id='{job}'") == "succeeded|4|4"
        assert f.sql(f"SELECT count(*) FROM git_rs.maintenance_operations WHERE job_id='{job}'") == "1"
        assert catalog_objects(f, "multipart-after", slug) == physical
        f.git(source, "clone", "--mirror", url, f.base / "multipart-restored.git")
        f.git(f.base / "multipart-restored.git", "fsck", "--strict", "--full")
        f.cleanup(slug)
        assert pending_uploads(f, repo) == {(foreign_key, foreign_upload)}, "GC must abort retired unknown uploads and preserve unretired identities"
        assert all(f.s3("HEAD", key, ok=False).stdout == b"404" for key in output_keys)
        assert int(f.sql(f"SELECT count(*) FROM git_rs.retired_objects WHERE repo_id='{repo}' AND reason='orphan-intent'")) == len(output_keys)
        f.results["multipart_crash_reconciliation"] = {"faults": 3, "durable_planned_keys": len(output_keys),
            "unknown_uploads_reclaimed": len(unknown_uploads), "unretired_upload_preserved": True,
            "successful_exact_attempt": 4, "all_catalog_objects_preserved": len(physical)}
    finally:
        original, failures = sys.exception(), []
        try:
            if foreign_started:
                # A lost Init response still leaves an upload owned by this
                # exact synthetic key. Discover its identity before cleanup.
                uploads = {(foreign_key, foreign_upload)} if foreign_upload else pending_uploads(
                    f, repo, prefix=foreign_key)
                for key, upload in uploads:
                    assert key == foreign_key and upload, "multipart cleanup escaped its owned key"
                    f.s3("DELETE", key + "?uploadId=" + quote(upload, safe=""))
        except BaseException as error:
            failures.append(error)
        if gateway is not None:
            try:
                gateway.close()
            except BaseException as error:
                failures.append(error)
        if failures:
            if original is not None:
                original.add_note("Multipart fixture cleanup also failed: " + "; ".join(
                    str(error)[:1024] for error in failures))
            elif len(failures) == 1:
                raise failures[0]
            else:
                raise BaseExceptionGroup("Multipart fixture cleanup failed", failures)


def exercise(f):
    f.cli("migrate")
    slug = "maintenance/source"
    f.cli("create", slug, "alice")
    source = f.base / "source"
    f.git(f.base, "init", "--initial-branch=main", source)
    _, port = f.start("publisher")
    url = f"http://127.0.0.1:{port}/{slug}.git"
    for ordinal in range(7):
        (source / "data.txt").write_text(f"synthetic generation {ordinal}\n")
        f.git(source, "add", ".")
        f.git(source, "commit", "-qm", f"Synthetic generation {ordinal}")
        f.git(source, "push", url, "main")
    tip = f.git(source, "rev-parse", "HEAD").stdout.strip()
    hidden = command(["git", "-C", source, "hash-object", "-w", "--stdin"], env=f.env,
                     data=b"unreachable physical object retained through compaction\n").stdout.strip()
    pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env, data=hidden + b"\n").stdout
    assert b"ok refs/heads/carrier" in raw_push(port, tip, [b"refs/heads/carrier"], pack=pack, slug=slug)
    before = f.git(source, "ls-remote", url).stdout
    physical = catalog_objects(f, "before-compaction")
    assert hidden in physical
    job = f.sql("SELECT id FROM git_rs.maintenance_jobs WHERE kind='compact' AND state='queued'")
    assert job and len(job.splitlines()) == 1

    gateway = FaultGateway(f.env["GIT_RS_S3_ENDPOINT"])
    proxy = None
    old = successor = None
    paused = False
    try:
        proxy = CommitGateway(int(f.env["PGPORT"]))
        gateway.arm("pause-get")
        proxy.arm("after", operation=b"SELECT maintenance_acquire(")
        old, old_pod = worker(f, "old-worker", GIT_RS_S3_ENDPOINT=gateway.endpoint,
                              PGPORT=str(proxy.server_address[1]))
        assert gateway.reached.wait(30), "compaction did not acquire its source and reach GET"
        assert proxy.dropped.is_set(), "leadership acquisition COMMIT response was not intercepted"
        state = f.sql("SELECT claim_generation,leader_fence FROM git_rs.maintenance_jobs WHERE id='" + job + "'").split("|")
        generation, fence = map(int, state)
        assert generation == 1
        assert int(f.sql("SELECT confirmations FROM git_rs.maintenance_leadership WHERE name='source'")) >= 1
        assert f.sql("SELECT count(*) FROM git_rs.maintenance_jobs WHERE state='running'") == "1"
        old.state()  # Validate the exact immutable ID and owner before freezing.
        command(["docker", "pause", old.identifier])
        paused = True
        # The process is now stopped at the observed GET. Release the proxy so
        # takeover tests the expired process, not the proxy's 30-second gate.
        gateway.reset()
        successor, new_pod = worker(f, "successor-worker", GIT_RS_S3_ENDPOINT=gateway.endpoint)

        def taken_over():
            row = f.sql("SELECT fence,coalesce(owner_identity,'') FROM git_rs.maintenance_leadership WHERE name='source'").split("|")
            return int(row[0]) > fence and row[1].startswith(new_pod + "/")

        until(taken_over, "both Kubernetes and PostgreSQL takeover gates")
        assert f.sql("SELECT count(*) FROM git_rs.maintenance_operations WHERE job_id='" + job + "' AND leader_fence=" + str(fence)) == "0"
        old.state()
        command(["docker", "unpause", old.identifier])
        paused = False
        gateway.reset()
        until(lambda: f.sql("SELECT state FROM git_rs.maintenance_jobs WHERE id='" + job + "'") == "succeeded",
              "successor compaction confirmation")
        assert f.sql("SELECT count(*) FROM git_rs.maintenance_operations WHERE job_id='" + job + "'") == "1"
        assert f.sql("SELECT count(*) FROM git_rs.maintenance_operations WHERE job_id='" + job + "' AND leader_fence=" + str(fence)) == "0"
        assert int(f.sql("SELECT claim_generation FROM git_rs.maintenance_jobs WHERE id='" + job + "'")) > generation
        assert f.sql("SELECT count(*) FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='maintenance/source')") == "1"
        assert catalog_objects(f, "after-compaction") == physical, "compaction must preserve every physical catalog object"
        assert f.git(source, "ls-remote", url).stdout == before
        mirror = f.base / "mirror.git"
        f.git(source, "clone", "--mirror", url, mirror)
        f.git(mirror, "fsck", "--strict", "--full")
        assert f.cli("history", slug, "alice", "--ref", "refs/heads/main").stdout == f.git(source, "rev-list", "--topo-order", "main").stdout
        f.cli("grant", slug, "alice", "bob", "reader", "refs/")
        assert f.cli("cleanup", slug, "bob", ok=False).returncode
        holder = f.sql("SELECT fence,owner_identity FROM git_rs.maintenance_leadership WHERE name='source'")
        requested = f.cli("cleanup", slug, "alice").stdout.decode().rstrip("\n").split("\t")
        assert len(requested) == 3 and requested[1] == "queued"
        until(lambda: f.sql("SELECT state FROM git_rs.maintenance_jobs WHERE id='" + requested[0] + "'") == "succeeded",
              "owner enqueue while deployment leader is running")
        assert f.sql("SELECT fence,owner_identity FROM git_rs.maintenance_leadership WHERE name='source'") == holder
        f.results["source_worker_takeover"] = {"unknown_acquire_commit_confirmed": True,
            "paused_owner": old_pod, "successor": new_pod, "old_attempt_did_not_publish": True,
            "all_catalog_objects_preserved": len(physical), "native_clone_fsck": True,
            "owner_enqueue_without_election": True}
    finally:
        original, failures = sys.exception(), []
        try:
            if paused:
                old.state()
                command(["docker", "unpause", old.identifier])
        except BaseException as error:
            failures.append(error)
        gateway.reset()
        for process in (old, successor):
            if process is not None:
                try:
                    stop(process)
                except BaseException as error:
                    failures.append(error)
        for resource in (proxy, gateway):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    failures.append(error)
        if failures:
            if original is not None:
                original.add_note("Maintenance fixture cleanup also failed: " + "; ".join(
                    str(error)[:1024] for error in failures))
            elif len(failures) == 1:
                raise failures[0]
            else:
                raise BaseExceptionGroup("Maintenance fixture cleanup failed", failures)

    queued = f.cli("cleanup", slug, "alice", "--wait", "1").stdout.decode().rstrip("\n").split("\t")
    assert queued[1:] == ["queued", ""], "a disabled worker cannot report deletion success"
    assert f.sql("SELECT state FROM git_rs.maintenance_jobs WHERE id='" + queued[0] + "'") == "queued"
    f.results["requester_wait_does_not_cancel_durable_job"] = True
    multipart_faults(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/git-rs")
    parser.add_argument("--runtime-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"))
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"))
    parser.add_argument("--cluster-container", required=True)
    parser.add_argument("--fixture-dir", type=Path)
    args = parser.parse_args()
    assert args.runtime_image and args.revision and args.provision
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    if args.fixture_dir:
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(str(args.fixture_dir))
    else:
        import tempfile
        context = tempfile.TemporaryDirectory(prefix="maintenance-", dir=local)
    with context as directory:
        fixture = Fixture(Path(directory), args.binary, runtime_image=args.runtime_image, revision=args.revision)
        try:
            fixture.configure_maintenance(args.cluster_container)
            fixture.setup(True)
            exercise(fixture)
            fixture.results["status"] = "passed"
        finally:
            try:
                fixture.close()
            finally:
                (Path(directory) / "report.json").write_text(json.dumps(fixture.results, indent=2) + "\n")


if __name__ == "__main__":
    main()
