#!/usr/bin/env python3
"""Image identity evidence includes init/terminated containers without native tools."""
import copy
import json
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from kubernetes_processes import PodProcessWatch, ProcessInventory


class PodIdentityContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.images = {role: {"reference": f"runtime.fixture.invalid/{role}@sha256:" + digit * 64,
                             "docker_id": "sha256:" + digit * 64, "cri_id": "sha256:" + other * 64}
                       for role, digit, other in (("runtime", "a", "b"), ("external-client-oracle", "c", "d"))}
        self.inventory = ProcessInventory(self.root, self.images)
        self.addCleanup(self.inventory.close)

    def pod(self):
        image = self.images["runtime"]
        return {"spec": {"containers": [{"name": "backend", "image": image["reference"]}],
                         "initContainers": [{"name": "private-directories", "image": image["reference"]}]},
                "status": {"initContainerStatuses": [{"name": "private-directories", "containerID": "containerd://" + "1" * 64,
                    "imageID": image["reference"], "state": {"terminated": {"exitCode": 0}}, "restartCount": 0}],
                    "containerStatuses": [{"name": "backend", "containerID": "containerd://" + "2" * 64,
                    "imageID": image["cri_id"], "state": {"running": {}}, "restartCount": 0}]}}

    def test_completed_init_and_deleted_hook_identities_survive_without_a_live_pid(self):
        pod = self.pod()
        self.inventory.record(pod)
        self.inventory.record(copy.deepcopy(pod))
        pod["status"]["containerStatuses"][0]["state"] = {"terminated": {"exitCode": 0}}
        self.inventory.record(pod)
        self.inventory.close()
        rows = [json.loads(line) for line in self.inventory.path.read_text().splitlines()]
        self.assertEqual({row["id"] for row in rows}, {"1" * 64, "2" * 64})
        self.assertTrue(all(row["role"] == "runtime" and row["image"] == self.images["runtime"] for row in rows))
        self.assertEqual(self.inventory.path.stat().st_mode & 0o777, 0o600)

    def test_image_mismatch_and_hidden_restart_do_not_produce_accepted_evidence(self):
        for field, value in (("imageID", "sha256:" + "f" * 64), ("restartCount", 1), ("containerID", "containerd://short")):
            with self.subTest(field=field):
                pod = self.pod()
                pod["status"]["containerStatuses"][0][field] = value
                with self.assertRaises(AssertionError):
                    self.inventory.record(pod)
        self.assertEqual(self.inventory.containers, {})

    def test_the_same_container_id_cannot_change_from_server_to_external_oracle(self):
        self.inventory.record(self.pod())
        oracle = self.images["external-client-oracle"]
        pod = {"spec": {"containers": [{"name": "oracle", "image": oracle["reference"]}]},
               "status": {"containerStatuses": [{"name": "oracle", "containerID": "containerd://" + "2" * 64,
                           "imageID": oracle["cri_id"], "restartCount": 0}]}}
        with self.assertRaisesRegex(AssertionError, "binding changed"):
            self.inventory.record(pod)

    def watch(self, responses):
        # Real pipes preserve byte-stream/EOF semantics; only the external
        # Docker/Kubernetes endpoint is replaced. No native process is spawned.
        complete = threading.Event()
        requests = []

        class Response:
            def __init__(self, payload):
                self.returncode = 0
                self.stdout = self.pipe(payload)
                self.stderr = self.pipe(b"")

            def pipe(self, payload):
                reader, writer = os.pipe()
                os.write(writer, payload)
                os.close(writer)
                return os.fdopen(reader, "rb", buffering=0)

            def wait(self, **_):
                return self.returncode

            def poll(self):
                return self.returncode

            def kill(self):
                self.returncode = -9

        def endpoint(arguments, **_):
            requests.append(arguments)
            index = len(requests) - 1
            if index == len(responses) - 1:
                complete.set()
            return Response(responses[min(index, len(responses) - 1)])

        cluster = SimpleNamespace(container="owned-k3s", namespace="owned-namespace",
            kubectl=lambda *_: SimpleNamespace(stdout=b'{"metadata":{"resourceVersion":"10"},"items":[]}'))
        # The existing inventory tests have their own output file.
        directory = self.root / "watch"
        directory.mkdir(mode=0o700)
        boundary = patch("kubernetes_processes.subprocess.Popen", side_effect=endpoint)
        boundary.start()
        self.addCleanup(boundary.stop)
        watch = PodProcessWatch(cluster, directory, self.images)
        self.assertTrue(complete.wait(timeout=2), "watch endpoint did not receive the final response")
        return watch, requests

    def test_watch_resumes_the_last_version_and_refuses_an_expired_history_instead_of_relisting(self):
        pod = self.pod()
        pod["metadata"] = {"namespace": "owned-namespace", "resourceVersion": "11"}
        added = json.dumps({"type": "ADDED", "object": pod}).encode() + b"\n"
        expired = b'{"type":"ERROR","object":{"code":410,"reason":"Expired"}}\n'
        watch, requests = self.watch([added, expired])
        with self.assertRaisesRegex(AssertionError, "failed or expired"):
            watch.close()
        self.assertIn("resourceVersion=10", requests[0][-2])
        self.assertIn("resourceVersion=11", requests[1][-2])
        rows = [json.loads(line) for line in watch.inventory.path.read_text().splitlines()]
        self.assertEqual({row["id"] for row in rows}, {"1" * 64, "2" * 64})

    def test_truncated_watch_record_invalidates_the_evidence(self):
        watch, _ = self.watch([b'{"type":"ADDED"'])
        with self.assertRaisesRegex(AssertionError, "truncated"):
            watch.close()


if __name__ == "__main__":
    unittest.main()
