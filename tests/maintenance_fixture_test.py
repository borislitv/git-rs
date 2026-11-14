#!/usr/bin/env python3
"""Namespace ownership, interrupted setup and credential cleanup; no cluster required."""
import base64
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import maintenance_fixture as fixture


class Api:
    """Stateful API boundary, including create ambiguity and UID preconditions."""
    token = b"synthetic-short-lived-account-token"

    def __init__(self, directory):
        self.directory = directory
        self.namespaces = {"unrelated": {"name": "unrelated", "uid": "foreign", "labels": {}}}
        self.documents, self.calls = [], []
        self.lose_create, self.fail_delete = False, False

    def __call__(self, argv, *, data=None, ok=True):
        self.calls.append((list(argv), data))
        if argv[:2] == ["docker", "inspect"]:
            return self.result([{"NetworkSettings": {"Ports": {"6443/tcp": [{"HostPort": "12345"}]}}}])
        arguments = argv[argv.index("kubectl") + 2:]
        if arguments[:1] == ["-n"]:
            arguments = arguments[2:]
        if arguments[0] == "version":
            return self.result({"serverVersion": {"gitVersion": "v1.37.2+k3s1"}})
        if arguments[0] == "create" and "-f" in arguments:
            document = json.loads(data)
            if document["kind"] == "Namespace":
                journals = list(self.directory.glob("*-fixture.json"))
                assert len(journals) == 1, "cleanup identity must precede resource creation"
                journal = json.loads(journals[0].read_text())
                assert journal["uid"] is None and journal["namespace"] == document["metadata"]["name"]
                metadata = copy.deepcopy(document["metadata"])
                metadata["uid"] = "11111111-1111-1111-1111-111111111111"
                self.namespaces[metadata["name"]] = metadata
                if self.lose_create:
                    raise subprocess.TimeoutExpired("namespace create", 10)
                return self.result({"metadata": metadata})
            self.documents = document["items"]
            return self.result({})
        if arguments[:2] == ["auth", "can-i"]:
            return self.result(b"yes\n")
        if arguments[:2] == ["create", "token"]:
            return self.result(self.token + b"\n")
        if arguments[:2] == ["config", "view"]:
            return self.result({"clusters": [{"cluster": {"certificate-authority-data": base64.b64encode(b"synthetic CA").decode()}}]})
        if arguments[:2] == ["get", "namespace"]:
            metadata = self.namespaces.get(arguments[2])
            return self.result({"metadata": metadata}) if metadata else self.result(b"", code=1, error=b"NotFound")
        if arguments[:2] == ["delete", "--raw"]:
            if self.fail_delete:
                raise subprocess.TimeoutExpired("namespace delete", 10)
            namespace = arguments[2].rsplit("/", 1)[1]
            assert json.loads(data)["preconditions"]["uid"] == self.namespaces[namespace]["uid"]
            del self.namespaces[namespace]
            return self.result({})
        if arguments[:2] == ["wait", "--for=delete"]:
            assert arguments[2].removeprefix("namespace/") not in self.namespaces
            return self.result(b"deleted\n")
        raise AssertionError(arguments)

    @staticmethod
    def result(value, *, code=0, error=b""):
        return subprocess.CompletedProcess([], code, value if isinstance(value, bytes) else json.dumps(value).encode(), error)


class NamespaceContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.api = Api(self.directory)
        boundary = patch.object(fixture, "command", self.api)
        boundary.start()
        self.addCleanup(boundary.stop)

    def test_lost_create_response_is_cleaned_from_the_preexisting_exact_journal(self):
        for kind in (fixture.NamespaceFixture, fixture.LeaseFixture, fixture.CandidateFixture):
            with self.subTest(kind=kind.__name__):
                owner = kind("selected-cluster", self.directory)
                self.api.lose_create = True
                with self.assertRaises(subprocess.TimeoutExpired):
                    owner.setup()
                self.assertIsNone(json.loads(owner.journal.read_text())["uid"])
                self.assertIn(owner.namespace, self.api.namespaces)
                fixture.cleanup(owner.journal)
                self.assertEqual(set(self.api.namespaces), {"unrelated"})
                self.assertFalse(owner.journal.exists())

    def test_replaced_uid_or_owner_never_deletes_the_namespace_or_credentials(self):
        owner = fixture.LeaseFixture("selected-cluster", self.directory).setup()
        original = copy.deepcopy(self.api.namespaces[owner.namespace])
        for replacement in ({**original, "uid": "22222222-2222-2222-2222-222222222222"},
                            {**original, "labels": {fixture.LABEL: "f" * 32}}):
            with self.subTest(replacement=replacement):
                self.api.namespaces[owner.namespace] = replacement
                with self.assertRaisesRegex(AssertionError, "UID|foreign"):
                    fixture.cleanup(owner.journal)
                self.assertIn(owner.namespace, self.api.namespaces)
                self.assertTrue(owner.journal.exists())
                self.assertEqual((owner.account / "token").read_bytes(), self.api.token)
        self.api.namespaces[owner.namespace] = original
        fixture.cleanup(owner.journal)
        self.assertEqual(set(self.api.namespaces), {"unrelated"})
        self.assertFalse(owner.account.exists())

    def test_failed_delete_retains_evidence_and_a_later_independent_cleanup_finishes(self):
        owner = fixture.LeaseFixture("selected-cluster", self.directory).setup()
        self.api.fail_delete = True
        with self.assertRaises(subprocess.TimeoutExpired):
            owner.close()
        self.assertTrue(owner.journal.is_file())
        self.assertTrue((owner.account / "token").is_file())
        self.api.fail_delete = False
        fixture.cleanup(owner.journal)
        self.assertFalse(owner.journal.exists())
        self.assertFalse(owner.account.exists())
        self.assertEqual(set(self.api.namespaces), {"unrelated"})

    def test_maintenance_token_is_private_and_has_only_named_lease_authority(self):
        owner = fixture.LeaseFixture("selected-cluster", self.directory).setup()
        documents = {item["kind"]: item for item in self.api.documents}
        self.assertEqual(documents["Role"]["rules"], [{"apiGroups": ["coordination.k8s.io"],
            "resources": ["leases"], "resourceNames": ["source"], "verbs": ["get", "patch"]}])
        self.assertFalse(documents["ServiceAccount"]["automountServiceAccountToken"])
        self.assertEqual(documents["Lease"]["spec"], {})
        self.assertEqual(owner.account.stat().st_mode & 0o777, 0o700)
        for name in ("token", "ca.crt", "namespace"):
            self.assertEqual((owner.account / name).stat().st_mode & 0o777, 0o600)
        self.assertEqual(owner.environment["KUBERNETES_SERVICE_HOST"], "127.0.0.1")
        self.assertNotIn(self.api.token.decode(), json.dumps([argv for argv, _ in self.api.calls]))
        owner.close()

    def test_linked_or_world_readable_cleanup_journal_is_rejected_before_api_access(self):
        owner = fixture.NamespaceFixture("selected-cluster", self.directory).setup()
        owner.journal.chmod(0o644)
        before = len(self.api.calls)
        with self.assertRaises(AssertionError):
            fixture.cleanup(owner.journal)
        self.assertEqual(len(self.api.calls), before)
        owner.journal.chmod(0o600)
        linked = self.directory / "linked.json"
        linked.symlink_to(owner.journal)
        with self.assertRaises(OSError):
            fixture.cleanup(linked)
        self.assertEqual(len(self.api.calls), before)
        owner.close()

    def test_confirmed_cluster_removal_only_cleans_credentials_for_its_exact_immutable_id(self):
        identity = "c" * 64
        owner = fixture.LeaseFixture(identity, self.directory).setup()
        before = len(self.api.calls)
        with self.assertRaises(AssertionError):
            fixture.cleanup(owner.journal, destroyed_cluster="d" * 64)
        self.assertTrue(owner.journal.exists())
        self.assertTrue(owner.account.exists())
        self.assertEqual(len(self.api.calls), before)
        # The supervisor has removed this cluster. No API request can be used to
        # confirm namespace deletion now; the preexisting immutable ID binds it.
        fixture.cleanup(owner.journal, destroyed_cluster=identity)
        self.assertFalse(owner.journal.exists())
        self.assertFalse(owner.account.exists())
        self.assertEqual(len(self.api.calls), before)


if __name__ == "__main__":
    unittest.main()
