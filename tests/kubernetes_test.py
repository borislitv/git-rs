#!/usr/bin/env python3
"""Immutable dependency handoff contracts at the external image boundary."""
import copy
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import kubernetes


class DependencyImageContracts(unittest.TestCase):
    def test_cluster_requests_execute_in_the_controller_with_the_bound_configuration(self):
        environment = {"GIT_RS_TEST_KUBECTL": "/usr/local/bin/kubectl",
                       "GIT_RS_TEST_KUBECONFIG": "/private/controller-kubeconfig.json",
                       "GIT_RS_TEST_CLUSTER_CONTAINER": "a" * 64}
        cluster = kubernetes.Cluster("a" * 64, "owned-namespace")
        with patch.dict(os.environ, environment), patch.object(kubernetes, "command") as endpoint:
            cluster.kubectl("apply", "-f", "-", data=b"synthetic document")
        self.assertEqual(endpoint.call_args.args[0], ["/usr/local/bin/kubectl", "--kubeconfig",
            "/private/controller-kubeconfig.json", "-n", "owned-namespace", "apply", "-f", "-"])
        self.assertEqual(endpoint.call_args.kwargs["data"], b"synthetic document")

    def test_dependency_pods_never_pull_an_unprepared_image(self):
        documents = kubernetes.dependency("postgres", "example.invalid/pg@sha256:" + "a" * 64,
                                          {}, [], [], [])
        deployment = next(row for row in documents if row["kind"] == "Deployment")
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["imagePullPolicy"], "Never")

    def test_mutable_dependency_reference_is_rejected_before_external_work(self):
        with patch.object(kubernetes, "command") as command:
            with self.assertRaisesRegex(AssertionError, "pinned dependency"):
                kubernetes.bound_dependency("owned-cluster", "example.invalid/pg:mutable")
        command.assert_not_called()

    def test_existing_alias_with_an_unrelated_identity_is_preserved(self):
        image = "example.invalid/pg@sha256:" + "a" * 64
        identity, foreign = "sha256:" + "b" * 64, "sha256:" + "c" * 64
        alias = "git-rs.fixture.invalid/dependency:" + "b" * 64
        images = {image: identity, alias: foreign}
        before = copy.deepcopy(images)

        def command(arguments, **_):
            self.assertEqual(arguments[:3], ["docker", "image", "inspect"])
            return SimpleNamespace(returncode=0, stdout=json.dumps([{"Id": images[arguments[3]]}]).encode())

        with patch.object(kubernetes, "command", side_effect=command), \
                patch.object(kubernetes, "import_image") as import_image:
            with self.assertRaises(AssertionError):
                kubernetes.bound_dependency("owned-cluster", image)
        import_image.assert_not_called()
        self.assertEqual(images, before)


if __name__ == "__main__":
    unittest.main()
