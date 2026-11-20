#!/usr/bin/env python3
"""Qualify prebuilt candidate contracts against an exact-owned Kubernetes 1.37 namespace.

Only this namespace, its two named Leases and its short-lived account are created.
The selected Docker k3s cluster is never created, reset, stopped or deleted here.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from maintenance_fixture import CandidateFixture


TESTS = (
    "real_named_lease_cas_expiry_and_transition_contracts",
    "real_adapter_has_eligible_standbys_and_releases_only_after_stop",
)


def command(arguments, *, data=None, timeout=20, check=True):
    result = subprocess.run(list(map(str, arguments)), input=data, capture_output=True, timeout=timeout)
    if check and result.returncode:
        # Do not include kubectl responses: TokenRequest/configuration calls can
        # contain credentials, even when the command itself has no secret argv.
        raise RuntimeError(f"candidate fixture command failed ({result.returncode})")
    return result


def run(args, directory):
    owner = CandidateFixture(args.cluster_container, directory)
    token, namespace = owner.token, owner.namespace
    name, account = "candidate-" + token, "candidate"
    label = {owner.label: token}
    prefix = ["docker", "exec", "-i", args.cluster_container, "kubectl"]

    def kubectl(*arguments, data=None, **options):
        return command([*prefix, *arguments], data=data, **options)

    version = json.loads(kubectl("version", "-o", "json").stdout)["serverVersion"]["gitVersion"]
    if not re.fullmatch(r"v1\.37\.[0-9]+(?:[+.-].*)?", version):
        raise RuntimeError("candidate CAS/SSA qualification requires Kubernetes 1.37")
    report = {"format": 1, "server_version": version, "status": "running", "contracts": [], "cleanup": False}
    try:
        owner.setup()
        documents = [
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": account, "namespace": namespace},
             "automountServiceAccountToken": False},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
             "metadata": {"name": account, "namespace": namespace},
             "rules": [{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
                        "resourceNames": [name, name + "-adapter"], "verbs": ["get", "patch"]}]},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
             "metadata": {"name": account, "namespace": namespace},
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": account},
             "subjects": [{"kind": "ServiceAccount", "name": account, "namespace": namespace}]},
        ]
        documents.extend({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                          "metadata": {"name": lease, "namespace": namespace, "labels": label}, "spec": {}}
                         for lease in [name, name + "-adapter"])
        kubectl("create", "-f", "-", data=json.dumps({"apiVersion": "v1", "kind": "List", "items": documents}).encode())

        subject = f"system:serviceaccount:{namespace}:{account}"
        for verb, resource, expected in [
            ("get", "leases/" + name, True), ("patch", "leases/" + name, True),
            ("get", "leases/other", False), ("list", "leases", False), ("watch", "leases", False),
            ("create", "leases", False), ("delete", "leases/" + name, False), ("get", "secrets", False),
        ]:
            deadline = time.monotonic() + 10
            while True:
                answer = kubectl("auth", "can-i", verb, resource, "--as=" + subject, "-n", namespace, check=False)
                if answer.stdout.strip() == (b"yes" if expected else b"no"):
                    break
                if not expected or time.monotonic() >= deadline:
                    raise RuntimeError("named-Lease authorization does not match the candidate contract")
                time.sleep(0.05)

        credentials = kubectl("-n", namespace, "create", "token", account, "--duration=10m").stdout.decode().strip()
        if not credentials or len(credentials) > 16384 or any(character.isspace() for character in credentials):
            raise RuntimeError("invalid short-lived candidate credential response")
        raw = json.loads(kubectl("config", "view", "--raw", "--flatten", "-o", "json").stdout)
        ca = raw["clusters"][0]["cluster"]["certificate-authority-data"]
        if not base64.b64decode(ca, validate=True):
            raise RuntimeError("fixture cluster must supply a verified CA")
        container = json.loads(command(["docker", "inspect", args.cluster_container]).stdout)[0]
        port = container["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
        configuration = {"apiVersion": "v1", "kind": "Config", "current-context": "candidate",
            "clusters": [{"name": "candidate", "cluster": {"server": f"https://127.0.0.1:{int(port)}",
                                                          "certificate-authority-data": ca}}],
            "users": [{"name": "candidate", "user": {"token": credentials}}],
            "contexts": [{"name": "candidate", "context": {"cluster": "candidate", "user": "candidate", "namespace": namespace}}]}
        kubeconfig = directory / "kubeconfig.json"
        fd = os.open(kubeconfig, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(configuration, stream)
        try:
            environment = {key: value for key, value in os.environ.items() if key in {"PATH", "LANG"}}
            environment.update(HOME=str(directory), KUBECONFIG=str(kubeconfig),
                GIT_RS_TEST_LEASE_NAMESPACE=namespace, GIT_RS_TEST_LEASE_NAME=name)
            for test in TESTS:
                try:
                    result = subprocess.run([str(args.test_binary), "--ignored", "--exact", test, "--nocapture", "--test-threads=1"],
                        env=environment, capture_output=True, timeout=120)
                except subprocess.TimeoutExpired as error:
                    log = (error.stdout or b"") + (error.stderr or b"")
                    (directory / (test + ".log")).write_bytes(log.replace(credentials.encode(), b"[REDACTED]"))
                    report["contracts"].append({"name": test, "returncode": None, "timed_out": True})
                    raise
                log = result.stdout + result.stderr
                log = log.replace(credentials.encode(), b"[REDACTED]")
                (directory / (test + ".log")).write_bytes(log)
                report["contracts"].append({"name": test, "returncode": result.returncode})
                if result.returncode or b"1 passed; 0 failed" not in result.stdout:
                    raise RuntimeError(f"prebuilt candidate contract failed or was not executed: {test}")
        finally:
            kubeconfig.unlink(missing_ok=True)
        with args.test_binary.open("rb") as stream:
            report["test_binary_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = type(error).__name__
        raise
    finally:
        try:
            owner.close()
            report["cleanup"] = True
        except BaseException as error:
            report["status"] = "failed"
            report["cleanup_failure"] = type(error).__name__
            raise
        finally:
            (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-container", required=True)
    parser.add_argument("--test-binary", type=Path, required=True, help="prebuilt leader_dependency test ELF; never compiled here")
    parser.add_argument("--directory", type=Path, required=True, help="new private evidence directory, retained on failure")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", args.cluster_container):
        parser.error("invalid cluster container name")
    args.test_binary = args.test_binary.resolve(strict=True)
    with args.test_binary.open("rb") as stream:
        executable = os.access(args.test_binary, os.X_OK) and stream.read(4) == b"\x7fELF"
    if not executable:
        parser.error("--test-binary must be an executable prebuilt ELF")
    args.directory.mkdir(mode=0o700)
    run(args, args.directory.resolve())


if __name__ == "__main__":
    main()
