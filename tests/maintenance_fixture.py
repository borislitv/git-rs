"""Exact-owned Kubernetes namespaces and scoped maintenance credentials.

The selected Kubernetes 1.37 cluster is supplied by the supervisor. No cluster
lifecycle, administrative kubeconfig or external credential plugin reaches a
backend container. The non-secret journal supports independent exact cleanup.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import uuid


LABEL = "git-rs.maintenance-fixture"
KINDS = {"maintenance": (LABEL, "git-rs-maintenance-"),
         "smoke": ("git-rs.smoke-fixture", "git-rs-smoke-"),
         "candidate": ("git-rs.candidate-owner", "git-rs-candidate-")}


def command(arguments, *, data=None, ok=True):
    environment = dict(os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
    environment.pop("DOCKER_CONTEXT", None)
    result = subprocess.run(list(map(str, arguments)), input=data, capture_output=True, timeout=70, env=environment)
    if ok and result.returncode:
        # TokenRequest and config responses must never enter diagnostics.
        raise RuntimeError(f"maintenance fixture API command failed ({result.returncode})")
    return result


class NamespaceFixture:
    kind = "smoke"

    def __init__(self, cluster, directory):
        assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", cluster)
        self.cluster = cluster
        self.directory = Path(directory).resolve(strict=True)
        metadata = self.directory.stat()
        assert stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid() and metadata.st_mode & 0o777 == 0o700
        self.token = uuid.uuid4().hex
        self.label, prefix = KINDS[self.kind]
        self.namespace = prefix + self.token
        self.uid = None
        self.journal = self.directory / (self.kind + "-fixture.json")

    def kubectl(self, *arguments, **options):
        return command(["docker", "exec", "-i", self.cluster, "kubectl", "--request-timeout=10s", *arguments], **options)

    def save(self):
        value = {"format": 1, "kind": self.kind, "cluster": self.cluster, "namespace": self.namespace,
                 "owner": self.token, "uid": self.uid}
        temporary = self.journal.with_suffix(".partial")
        with temporary.open("x", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.journal)
        descriptor = os.open(self.directory, os.O_DIRECTORY | os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def setup(self):
        assert not self.journal.exists()
        version = json.loads(self.kubectl("version", "-o", "json").stdout)["serverVersion"]["gitVersion"]
        assert re.fullmatch(r"v1\.37\.[0-9]+(?:[+.-].*)?", version), "Kubernetes 1.37 is required"
        self.save()  # Before the first potentially ambiguous resource creation.
        namespace = {"apiVersion": "v1", "kind": "Namespace",
                     "metadata": {"name": self.namespace, "labels": {self.label: self.token}}}
        created = json.loads(self.kubectl("create", "-f", "-", "-o", "json", data=json.dumps(namespace).encode()).stdout)
        self.uid = created["metadata"]["uid"]
        self.save()
        return self

    def cleanup_local(self):
        (self.directory / "kubeconfig").unlink(missing_ok=True)
        (self.directory / "kubeconfig.json").unlink(missing_ok=True)

    def close(self):
        if not self.journal.exists():
            return
        current = self.kubectl("get", "namespace", self.namespace, "-o", "json", ok=False)
        if not current.returncode:
            metadata = json.loads(current.stdout)["metadata"]
            assert metadata.get("labels", {}).get(self.label) == self.token, "foreign fixture namespace"
            assert self.uid is None or metadata["uid"] == self.uid, "fixture namespace UID changed"
            options = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": metadata["uid"]}}
            self.kubectl("delete", "--raw", "/api/v1/namespaces/" + self.namespace, "-f", "-",
                         data=json.dumps(options).encode())
            self.kubectl("wait", "--for=delete", "namespace/" + self.namespace, "--timeout=60s")
        else:
            assert b"NotFound" in current.stderr, "fixture namespace cleanup unconfirmed"
        self.cleanup_local()
        self.journal.unlink()


class LeaseFixture(NamespaceFixture):
    kind = "maintenance"

    def __init__(self, cluster, directory):
        super().__init__(cluster, directory)
        self.account = self.directory / "maintenance-account"
        self.environment = {}

    def setup(self):
        assert not self.account.exists()
        super().setup()
        account, name = "source-maintenance", "source"
        documents = [
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": account},
             "automountServiceAccountToken": False},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": account},
             "rules": [{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
                        "resourceNames": [name], "verbs": ["get", "patch"]}]},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": account},
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": account},
             "subjects": [{"kind": "ServiceAccount", "name": account, "namespace": self.namespace}]},
            {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": {"name": name}, "spec": {}},
        ]
        self.kubectl("-n", self.namespace, "create", "-f", "-",
                     data=json.dumps({"apiVersion": "v1", "kind": "List", "items": documents}).encode())
        subject = f"system:serviceaccount:{self.namespace}:{account}"
        deadline = time.monotonic() + 10
        while self.kubectl("-n", self.namespace, "auth", "can-i", "patch", "lease/source",
                           "--as=" + subject, ok=False).stdout.strip() != b"yes":
            assert time.monotonic() < deadline, "named-Lease RBAC did not propagate"
            time.sleep(0.05)
        credential = self.kubectl("-n", self.namespace, "create", "token", account, "--duration=15m").stdout.strip()
        assert credential and len(credential) <= 16384 and not any(byte in credential for byte in b" \r\n\t")
        raw = json.loads(self.kubectl("config", "view", "--raw", "--flatten", "-o", "json").stdout)
        ca = base64.b64decode(raw["clusters"][0]["cluster"]["certificate-authority-data"], validate=True)
        assert ca and len(ca) <= 65536
        cluster = json.loads(command(["docker", "inspect", self.cluster]).stdout)[0]
        port = int(cluster["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"])
        self.account.mkdir(mode=0o700)
        for name, data in [("token", credential), ("ca.crt", ca), ("namespace", self.namespace.encode())]:
            descriptor = os.open(self.account / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
        self.environment = {"KUBERNETES_SERVICE_HOST": "127.0.0.1", "KUBERNETES_SERVICE_PORT": str(port),
                            "GIT_RS_LEADER_LEASE_NAME": "source", "GIT_RS_LEADER_LEASE_NAMESPACE": self.namespace}
        return self

    def cleanup_local(self):
        for name in ("token", "ca.crt", "namespace"):
            (self.account / name).unlink(missing_ok=True)
        if self.account.exists():
            self.account.rmdir()


class CandidateFixture(NamespaceFixture):
    kind = "candidate"


def cleanup(path, *, destroyed_cluster=None):
    """Clean an exact namespace, or its files after its owned cluster is removed.

    The supervisor-only alternative requires the exact immutable Docker ID from
    the namespace journal. Its caller must have confirmed removal of that owned
    cluster; a name, replacement cluster or API failure is not such confirmation.
    """
    path = Path(path)
    parent = path.parent.stat()
    assert stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid() and parent.st_mode & 0o777 == 0o700
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "r") as stream:
        metadata = os.fstat(stream.fileno())
        assert stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid() and metadata.st_mode & 0o777 == 0o600
        data = stream.read(4097)
    assert len(data) <= 4096
    value = json.loads(data)
    assert value["format"] == 1 and re.fullmatch(r"[0-9a-f]{32}", value["owner"])
    kind = value["kind"]
    assert kind in KINDS and value["namespace"] == KINDS[kind][1] + value["owner"]
    assert value["uid"] is None or re.fullmatch(r"[A-Za-z0-9-]{1,64}", value["uid"])
    fixture_class = {"maintenance": LeaseFixture, "smoke": NamespaceFixture, "candidate": CandidateFixture}[kind]
    fixture = fixture_class(value["cluster"], path.parent)
    fixture.token, fixture.namespace, fixture.uid = value["owner"], value["namespace"], value["uid"]
    assert fixture.journal == path.resolve(), "unexpected cleanup journal name"
    if destroyed_cluster is None:
        fixture.close()
    else:
        assert re.fullmatch(r"[0-9a-f]{64}", destroyed_cluster) and fixture.cluster == destroyed_cluster
        fixture.cleanup_local()
        fixture.journal.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup", type=Path, required=True, help="private exact-namespace cleanup journal")
    cleanup(parser.parse_args().cleanup)


if __name__ == "__main__":
    main()
