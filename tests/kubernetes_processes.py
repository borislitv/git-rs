"""Bind short-lived Kubernetes containers to the exact imported image roles.

A resource-version watch starts before Helm can create a migration or init
container. Finite watch responses resume without relisting; an expired version,
malformed record or stream failure invalidates the evidence instead of creating
an unobserved interval. Only image/container identities enter the private log.
"""
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import threading
import time
from urllib.parse import urlencode


MAX_BYTES = 16 * 1024 * 1024
MAX_EVENT = 1024 * 1024
MAX_CONTAINERS = 1024


class ProcessInventory:
    def __init__(self, directory, images):
        self.images = {image["reference"]: (role, image) for role, image in images.items()}
        assert set(images) == {"runtime", "external-client-oracle"}
        self.path = Path(directory) / "cri-containers.jsonl"
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self.output = os.fdopen(descriptor, "w")
        self.containers = {}

    def record(self, pod):
        spec, status = pod["spec"], pod.get("status", {})
        images = {row["name"]: row["image"] for key in ("containers", "initContainers") for row in spec.get(key, [])}
        for key in ("containerStatuses", "initContainerStatuses"):
            for row in status.get(key, []):
                image = images[row["name"]]
                if image not in self.images or not row.get("containerID"):
                    continue
                role, bound = self.images[image]
                identity = row["containerID"].removeprefix("containerd://")
                actual = row["imageID"].removeprefix("containerd://")
                assert re.fullmatch(r"[0-9a-f]{64}", identity), "invalid CRI process identity"
                assert actual in {bound["reference"], bound["cri_id"]}, "CRI image identity changed"
                assert row.get("restartCount", 0) == 0, "an unrecorded container restart invalidates lifecycle evidence"
                value = {"id": identity, "role": role, "image_id": actual, "image": bound}
                if identity in self.containers:
                    assert self.containers[identity] == value, "CRI process image binding changed"
                else:
                    assert len(self.containers) < MAX_CONTAINERS, "CRI process inventory exceeded its bound"
                    self.containers[identity] = value
                    self.output.write(json.dumps(value) + "\n")
                    self.output.flush()

    def close(self):
        if not self.output.closed:
            self.output.flush()
            os.fsync(self.output.fileno())
            self.output.close()


class PodProcessWatch:
    def __init__(self, cluster, directory, images):
        metadata = Path(directory).lstat()
        assert stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid()
        assert stat.S_IMODE(metadata.st_mode) == 0o700
        self.cluster = cluster
        self.inventory = ProcessInventory(directory, images)
        self.stopping = threading.Event()
        self.error = None
        self.bytes = 0
        self.process = None
        self.thread = None
        try:
            snapshot = json.loads(cluster.kubectl("get", "pods", "-o", "json").stdout)
            self.version = snapshot["metadata"]["resourceVersion"]
            for pod in snapshot["items"]:
                self.inventory.record(pod)
            self.thread = threading.Thread(target=self.observe, name="pod-process-identities")
            self.thread.start()
        except BaseException:
            self.inventory.close()
            raise

    def event(self, line):
        assert 0 < len(line) <= MAX_EVENT, "invalid pod watch event size"
        event = json.loads(line)
        kind, value = event["type"], event["object"]
        assert kind in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK"}, "pod lifecycle watch failed or expired"
        version = value["metadata"]["resourceVersion"]
        assert isinstance(version, str) and 0 < len(version) <= 256, "invalid pod watch resource version"
        if kind != "BOOKMARK":
            assert value["metadata"]["namespace"] == self.cluster.namespace, "foreign pod watch event"
            self.inventory.record(value)
        self.version = version

    def response(self):
        query = urlencode({"watch": "true", "resourceVersion": self.version,
                           "timeoutSeconds": 3, "allowWatchBookmarks": "true"})
        path = "/api/v1/namespaces/" + self.cluster.namespace + "/pods?" + query
        environment = dict(os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
        environment.pop("DOCKER_CONTEXT", None)
        process = subprocess.Popen(["docker", "exec", self.cluster.container, "kubectl", "get", "--raw", path,
                                    "--request-timeout=8s"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
        self.process = process
        pending, errors = bytearray(), bytearray()
        deadline = time.monotonic() + 10
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "events")
                selector.register(process.stderr, selectors.EVENT_READ, "errors")
                while selector.get_map():
                    assert time.monotonic() < deadline, "pod lifecycle watch response deadline expired"
                    for key, _ in selector.select(timeout=0.1):
                        chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        self.bytes += len(chunk)
                        assert self.bytes <= MAX_BYTES, "pod lifecycle watch exceeded its byte bound"
                        if key.data == "errors":
                            errors.extend(chunk)
                            assert len(errors) <= 64 * 1024, "pod lifecycle watch diagnostics exceeded their bound"
                            continue
                        pending.extend(chunk)
                        while b"\n" in pending:
                            line, _, tail = pending.partition(b"\n")
                            pending[:] = tail
                            if line.strip():
                                self.event(line)
                        assert len(pending) <= MAX_EVENT, "unterminated pod lifecycle event exceeded its bound"
            assert not pending.strip(), "truncated pod lifecycle watch response"
            assert process.wait(timeout=1) == 0 and not errors.strip(), "pod lifecycle watch request failed"
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=2)
            process.stdout.close()
            process.stderr.close()
            self.process = None

    def observe(self):
        try:
            while True:
                # The final finite response also replays events committed in the
                # gap after the preceding response, before close was requested.
                final = self.stopping.is_set()
                self.response()
                if final:
                    break
        except BaseException as error:
            self.error = error

    def close(self):
        try:
            self.stopping.set()
            self.thread.join(timeout=22)
            if self.thread.is_alive():
                if self.process is not None:
                    self.process.kill()
                self.thread.join(timeout=3)
                raise RuntimeError("pod lifecycle watcher did not stop within its bound")
            if self.error is not None:
                raise self.error
        finally:
            self.inventory.close()
