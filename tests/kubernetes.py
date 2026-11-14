#!/usr/bin/env python3
"""Real Helm/SSH/authenticated HTTP smoke in a selected disposable Docker k3s cluster.

Uses synthetic credentials, generated TLS/SSH keys, and an isolated namespace.
It does not install CNPG/Rook operators or establish production durability.
"""
import argparse
import base64
from contextlib import nullcontext
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import tomllib

import yaml

from integration import PG_IMAGE, ROOT, S3_IMAGE, command, wait_for
from maintenance_fixture import NamespaceFixture
from kubernetes_processes import PodProcessWatch
from runtime_process import cluster_cgroups, inspect_image


class Cluster:
    def __init__(self, container, namespace):
        self.container, self.namespace = container, namespace
        self.process_watch = None

    def kubectl(self, *args, data=None, ok=True):
        return command(["docker", "exec", "-i", self.container, "kubectl", "-n",
                        self.namespace, *args], data=data, ok=ok)

    def apply(self, documents):
        self.kubectl("apply", "-f", "-", data=yaml.safe_dump_all(documents).encode())

    def secret(self, name, values):
        return {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name},
                "data": {key: base64.b64encode(value).decode() for key, value in values.items()}}

    def logs(self):
        print(self.kubectl("get", "pods", "-o", "wide", ok=False).stdout.decode())
        print(self.kubectl("get", "events", "--sort-by=.lastTimestamp", ok=False).stdout.decode())
        pods = json.loads(self.kubectl("get", "pods", "-o", "json").stdout)["items"]
        for pod in pods:
            for container in pod["spec"].get("initContainers", []) + pod["spec"]["containers"]:
                name = pod["metadata"]["name"]
                print(name, container["name"])
                print(self.kubectl("logs", name, "-c", container["name"], "--tail=80",
                                   ok=False).stdout.decode())


def import_image(cluster, image):
    save = subprocess.Popen(["docker", "save", image], stdout=subprocess.PIPE)
    try:
        result = subprocess.run(["docker", "exec", "-i", cluster, "ctr", "images",
                                 "import", "--digests", "-"], stdin=save.stdout, capture_output=True, timeout=120)
        assert result.returncode == 0, result.stderr.decode()
        assert save.wait(timeout=10) == 0, "Docker image export failed"
    finally:
        save.stdout.close()
        if save.poll() is None:
            save.terminate()
            try:
                save.wait(timeout=5)
            except subprocess.TimeoutExpired:
                save.kill()
                save.wait(timeout=5)


def bound_image(cluster, image, revision, role):
    identity = inspect_image(image, revision, role)
    repository = "git-rs.fixture.invalid/" + role
    # Unique content-bound aliases let containerd import an untagged Docker-ID
    # handoff without giving a mutable tag to the Helm deployment.
    alias = repository + ":" + revision + "-" + identity.removeprefix("sha256:")
    existing = command(["docker", "image", "inspect", alias], ok=False)
    created = bool(existing.returncode)
    if created:
        command(["docker", "tag", identity, alias])
    try:
        assert inspect_image(alias, revision, role) == identity
        import_image(cluster, alias)
        assert inspect_image(alias, revision, role) == identity, "image alias changed during export"
    finally:
        if created:
            assert inspect_image(alias, revision, role) == identity, "refusing to remove a replaced image alias"
            command(["docker", "image", "rm", alias])
    rows = command(["docker", "exec", cluster, "ctr", "images", "list", "name==" + alias]).stdout.decode().splitlines()
    matches = [row.split() for row in rows if row.split() and row.split()[0] == alias]
    assert len(matches) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", matches[0][2]), "imported manifest not found"
    reference = repository + "@" + matches[0][2]
    imported = json.loads(command(["docker", "exec", cluster, "crictl", "inspecti", reference]).stdout)
    labels = imported["info"]["imageSpec"]["config"]["Labels"]
    assert labels["io.git-rs.role"] == role and labels["org.opencontainers.image.revision"] == revision
    return {"docker_id": identity, "reference": reference, "cri_id": imported["status"]["id"]}


def assert_runtime_pods(cluster, image):
    pods = json.loads(cluster.kubectl("get", "pods", "-l",
        "app.kubernetes.io/instance=smoke,app.kubernetes.io/component=serving", "-o", "json").stdout)["items"]
    assert len(pods) == 2 and all(not pod["metadata"].get("deletionTimestamp") for pod in pods)
    for pod in pods:
        assert {item["name"] for item in pod["spec"]["initContainers"]} == {"private-directories", "private-host-key"}
        assert {item["name"] for item in pod["spec"]["containers"]} == {"backend", "ssh"}
        for container in pod["spec"]["containers"] + pod["spec"]["initContainers"]:
            assert container["image"] == image["reference"] and container["imagePullPolicy"] == "Never"
        for status in pod["status"]["containerStatuses"] + pod["status"]["initContainerStatuses"]:
            resolved = status["imageID"].removeprefix("containerd://")
            assert resolved in {image["cri_id"], image["reference"]}, "pod image differs from the imported runtime"
            if status["name"] in {"backend", "ssh"}:
                assert status["ready"]
            else:
                assert status["state"]["terminated"]["exitCode"] == 0
    if root := os.environ.get("GIT_RS_TEST_POD_CGROUP_ROOT"):
        assert cluster_cgroups(cluster.container, root), "missing actual CRI task observations"
    return {pod["metadata"]["name"] for pod in pods}


def oracle_pod(image):
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "native-oracle"}, "spec": {
        "automountServiceAccountToken": False, "restartPolicy": "Never", "activeDeadlineSeconds": 900,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001},
        "containers": [{"name": "oracle", "image": image["reference"], "imagePullPolicy": "Never",
            "command": ["/usr/bin/tini", "--", "/usr/bin/sleep", "900"],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]}},
            "resources": {"requests": {"cpu": "25m", "memory": "64Mi"},
                          "limits": {"cpu": "250m", "memory": "256Mi", "ephemeral-storage": "256Mi"}},
            "volumeMounts": [{"name": "work", "mountPath": "/work"},
                             {"name": "tmp", "mountPath": "/tmp"},
                             {"name": "secrets", "mountPath": "/oracle-secrets", "readOnly": True}]}],
        "volumes": [{"name": "work", "emptyDir": {"sizeLimit": "128Mi"}},
                    {"name": "tmp", "emptyDir": {"medium": "Memory", "sizeLimit": "32Mi"}},
                    {"name": "secrets", "secret": {"secretName": "fixture-oracle", "defaultMode": 0o440}}]}}


def dependency(name, image, env, args, volumes, mounts, security=None):
    # Persistent fixture volumes must never be opened by overlapping writers.
    return [
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name},
         "spec": {"selector": {"fixture": name},
                  "ports": [{"port": 5432 if name == "postgres" else 7070}]}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
         "spec": {"replicas": 1, "strategy": {"type": "Recreate"},
                  "selector": {"matchLabels": {"fixture": name}},
                  "template": {"metadata": {"labels": {"fixture": name}}, "spec": {
                      "securityContext": security or {}, "automountServiceAccountToken": False,
                      "containers": [{"name": name, "image": image, "args": args,
                                      "env": [{"name": key, "value": value} for key, value in env.items()],
                                      "volumeMounts": mounts,
                                      "readinessProbe": {"tcpSocket": {"port": 5432 if name == "postgres" else 7070},
                                                         "periodSeconds": 2}}], "volumes": volumes}}}},
    ]


def wait_ssh(address, port):
    def listening():
        try:
            with socket.create_connection((address, port), timeout=1) as stream:
                return stream.recv(256).startswith(b"SSH-2.0-")
        except OSError:
            return False

    wait_for(listening, "SSH service dataplane readiness")


def maintenance_contract(cluster, runtime, values, values_path, install, client):
    def sql(query):
        return cluster.kubectl("exec", "deployment/postgres", "--", "psql", "-X", "-qAt", "-U", "git_rs",
            "-d", "git_rs", "-v", "ON_ERROR_STOP=1", "-c", query).stdout.decode().strip()

    assert sql("SELECT version FROM git_rs.schema_version") == "2", "upgrade schema before enabling workers"
    values["maintenance"] = {"enabled": True, "replicaCount": 2, "cacheBytes": 8 * 1024 * 1024,
        "scratch": {"medium": "Memory", "sizeLimit": "64Mi"},
        "resources": {"requests": {"cpu": "25m", "memory": "64Mi", "ephemeral-storage": "64Mi"},
                      "limits": {"cpu": "250m", "memory": "512Mi", "ephemeral-storage": "128Mi"}}}
    values_path.write_text(yaml.safe_dump(values))
    command(install)
    cluster.kubectl("rollout", "status", "deployment/backend-maintenance", "--timeout=120s")

    def pods():
        result = json.loads(cluster.kubectl("get", "pods", "-l",
            "app.kubernetes.io/instance=smoke,app.kubernetes.io/component=maintenance", "-o", "json").stdout)["items"]
        assert len(result) == 2 and not any(pod["metadata"].get("deletionTimestamp") for pod in result)
        for pod in result:
            assert not pod["spec"]["automountServiceAccountToken"]
            assert pod["spec"]["serviceAccountName"] != "backend"
            assert len(pod["spec"]["containers"]) == 1 and pod["spec"]["containers"][0]["image"] == runtime["reference"]
            status = pod["status"]["containerStatuses"][0]
            assert status["ready"] and status["imageID"].removeprefix("containerd://") in {runtime["reference"], runtime["cri_id"]}
        return result

    current = pods()
    account = current[0]["spec"]["serviceAccountName"]
    role = json.loads(cluster.kubectl("get", "role/" + account, "-o", "json").stdout)
    assert role["rules"] == [{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
                              "resourceNames": ["backend-source"], "verbs": ["get", "patch"]}]

    def leadership():
        row = sql("SELECT fence,coalesce(owner_identity,''),coalesce(lease_until>clock_timestamp(),false) "
                  "FROM git_rs.maintenance_leadership WHERE name='source'").split("|")
        return {"fence": int(row[0]), "owner": row[1], "live": row[2] == "t"}

    def elected(previous=0):
        leader = leadership()
        return leader["live"] and leader["fence"] > previous

    wait_for(elected, "committed PostgreSQL maintenance fence")
    leader = leadership()
    probes = client("probes", [argument for pod in current for argument in ("--maintenance-address", pod["status"]["podIP"])])["probes"]
    assert all(probe["/readyz"] == 200 for probe in probes), "both eligible standbys must be ready"
    assert sorted(probe["/leaderz"] for probe in probes) == [200, 503], "exactly one locally authoritative leader expected"
    lease_before = json.loads(cluster.kubectl("get", "lease/backend-source", "-o", "json").stdout)
    assert lease_before["spec"]["holderIdentity"] == leader["owner"]

    def cleanup_job():
        queued = cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--", "/usr/local/bin/git-rs",
            "cleanup", "team/project", "alice").stdout.decode().rstrip("\n").split("\t")
        assert len(queued) == 3 and queued[1:] == ["queued", ""], queued
        identifier = queued[0]
        assert re.fullmatch(r"[0-9a-f-]{36}", identifier)

        def finished():
            state = sql("SELECT state,coalesce(error_code,'') FROM git_rs.maintenance_jobs WHERE id='" + identifier + "'").split("|")
            assert state[0] in {"queued", "running", "succeeded"}, state
            return state == ["succeeded", ""]

        wait_for(finished, "elected same-binary cleanup completion")
        assert sql("SELECT count(*) FROM git_rs.maintenance_job_results WHERE job_id='" + identifier +
                   "' AND requested_outcome='succeeded' AND outcome='succeeded'") == "1"
        return identifier

    first_job = cleanup_job()
    # Reapplying the chart's empty Lease spec must preserve the controller's
    # acquired fields and its ownership rather than seeding a second campaign.
    command(install)
    lease_after = json.loads(cluster.kubectl("get", "lease/backend-source", "-o", "json").stdout)
    assert lease_after["metadata"]["uid"] == lease_before["metadata"]["uid"]
    for key in ("holderIdentity", "leaseTransitions", "acquireTime"):
        assert lease_after["spec"][key] == lease_before["spec"][key], ("Helm altered live Lease state", key)
    assert leadership()["fence"] == leader["fence"]
    owner_uid = leader["owner"].split("/", 1)[0]
    holder = next(pod for pod in current if pod["metadata"]["uid"] == owner_uid)
    options = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": owner_uid}}
    cluster.kubectl("delete", "--raw", f"/api/v1/namespaces/{cluster.namespace}/pods/{holder['metadata']['name']}",
                    "-f", "-", data=json.dumps(options).encode())
    cluster.kubectl("wait", "--for=delete", "pod/" + holder["metadata"]["name"], "--timeout=120s")
    cluster.kubectl("rollout", "status", "deployment/backend-maintenance", "--timeout=120s")
    wait_for(lambda: elected(leader["fence"]), "new PostgreSQL fence after leader replacement")
    successor = leadership()
    assert successor["owner"].split("/", 1)[0] != owner_uid
    second_job = cleanup_job()
    pods()
    assert_runtime_pods(cluster, runtime)
    client("upgrade")
    return {"eligible_replicas": 2, "first_fence": leader["fence"], "replacement_fence": successor["fence"],
            "jobs": [first_job, second_job], "standby_probes": probes,
            "upgrade_preserved_lease": True, "replacement_completed_work": True}


def exercise(args, base, cluster):
    print("Importing distinct production runtime and external native-client images", flush=True)
    runtime = bound_image(args.cluster_container, args.runtime_image, args.revision, "runtime")
    oracle = bound_image(args.cluster_container, args.oracle_image, args.revision, "external-client-oracle")
    assert runtime["docker_id"] != oracle["docker_id"]
    for image in (PG_IMAGE, S3_IMAGE):
        import_image(args.cluster_container, image)
    config = yaml.safe_load(command(["docker", "exec", args.cluster_container, "kubectl",
                                     "config", "view", "--raw", "--flatten"]).stdout)
    inspection = json.loads(command(["docker", "inspect", args.cluster_container]).stdout)[0]
    port = inspection["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    config["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{port}"
    kubeconfig = base / "kubeconfig"
    kubeconfig.write_text(yaml.safe_dump(config))
    kubeconfig.chmod(0o600)
    command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
             "-subj", "/CN=git-rs-test", "-addext",
             f"subjectAltName=DNS:postgres.{cluster.namespace}.svc,DNS:s3.{cluster.namespace}.svc",
             "-keyout", base / "tls.key", "-out", base / "tls.crt"])
    for name in ["host-key", "client-key", "unknown-key"]:
        command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", base / name])
    cluster.apply([
        cluster.secret("fixture-tls", {"tls.crt": (base / "tls.crt").read_bytes(),
                                       "tls.key": (base / "tls.key").read_bytes()}),
        cluster.secret("fixture-ca", {"ca.crt": (base / "tls.crt").read_bytes()}),
        cluster.secret("fixture-pg", {"password": b"synthetic-password"}),
        cluster.secret("fixture-s3", {"AWS_ACCESS_KEY_ID": b"test-access",
                                      "AWS_SECRET_ACCESS_KEY": b"synthetic-secret-key"}),
        cluster.secret("fixture-host-keys", {"ssh_host_ed25519_key": (base / "host-key").read_bytes()}),
        cluster.secret("fixture-principals", {"principals": b"alice " + (base / "client-key.pub").read_bytes()}),
        cluster.secret("fixture-http", {"credentials": b"alice synthetic-chart-token-0123456789abcdef\n"}),
    ])
    tls_volume = {"name": "tls", "secret": {"secretName": "fixture-tls", "defaultMode": 0o440}}
    tls_mount = {"name": "tls", "mountPath": "/fixture-tls", "readOnly": True}
    cluster.apply(dependency("postgres", PG_IMAGE,
                             {"POSTGRES_USER": "git_rs", "POSTGRES_DB": "git_rs",
                              "POSTGRES_PASSWORD": "synthetic-password"},
                             ["-c", "ssl=on", "-c", "ssl_cert_file=/fixture-tls/tls.crt",
                              "-c", "ssl_key_file=/fixture-tls/tls.key"],
                             [tls_volume, {"name": "data", "emptyDir": {}}],
                             [tls_mount, {"name": "data", "mountPath": "/var/lib/postgresql"}],
                             {"runAsUser": 999, "runAsGroup": 999, "fsGroup": 999}))
    cluster.apply(dependency("s3", S3_IMAGE,
                             {"ROOT_ACCESS_KEY_ID": "test-access", "ROOT_SECRET_ACCESS_KEY": "synthetic-secret-key"},
                             ["--quiet", "--cert", "/fixture-tls/tls.crt", "--key", "/fixture-tls/tls.key",
                              "posix", "/data"],
                             [tls_volume, {"name": "data", "emptyDir": {}}],
                             [tls_mount, {"name": "data", "mountPath": "/data"}]))
    for name in ["postgres", "s3"]:
        cluster.kubectl("rollout", "status", "deployment/" + name, "--timeout=120s")

    # Start from an explicit resource version before a short-lived Helm hook or
    # init container can disappear. The trace consumer joins only these bound
    # image/container identities, not arbitrary pods that happen to run git-rs.
    cluster.process_watch = PodProcessWatch(cluster, base, {"runtime": runtime, "external-client-oracle": oracle})
    repository, digest = runtime["reference"].rsplit("@", 1)
    values = {"fullnameOverride": "backend", "replicaCount": 2,
              "image": {"repository": repository, "digest": digest, "pullPolicy": "Never"},
              "service": {"type": "NodePort"}, "cnpg": {"enabled": False},
              "postgresql": {"host": f"postgres.{cluster.namespace}.svc", "passwordSecret": "fixture-pg",
                             "serverCASecret": "fixture-ca"},
              "s3": {"endpoint": f"https://s3.{cluster.namespace}.svc:7070", "bucket": "packs",
                     "credentialsSecret": "fixture-s3", "caSecret": "fixture-ca"},
              "objectBucket": {"enabled": False},
              "http": {"enabled": True, "credentialsSecret": "fixture-http"},
              "ssh": {"hostKeysSecret": "fixture-host-keys", "authorizedKeysSecret": "fixture-principals"},
              "resources": {"requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "64Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "128Mi"}},
              "cpu": {"concurrency": 1, "waiters": 0}}
    values_path = base / "values.yaml"
    values_path.write_text(yaml.safe_dump(values))
    helm = [args.helm, "--kubeconfig", kubeconfig, "--namespace", cluster.namespace]
    install = [*helm, "upgrade", "--install", "smoke", ROOT / "charts/git-rs", "--values", values_path,
               "--wait", "--timeout", "120s"]
    print("Installing the independent chart with verified TLS PostgreSQL/S3", flush=True)
    command(install)
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    assert cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--",
                           "/usr/local/bin/git-rs", "--version").stdout == f"git-rs {version}\n".encode()
    print("In-process SQL rejects an untrusted CA and a mismatched TLS hostname", flush=True)
    pg_service = json.loads(cluster.kubectl("get", "service/postgres", "-o", "json").stdout)
    for setting in ["PGSSLROOTCERT=/etc/ssl/certs/ca-certificates.crt",
                    "PGHOST=" + pg_service["spec"]["clusterIP"]]:
        result = cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--",
            "/usr/bin/env", setting, "/usr/local/bin/git-rs", "migrate", ok=False)
        assert result.returncode != 0, "PostgreSQL TLS verification must reject " + setting.split("=")[0]

    service = json.loads(cluster.kubectl("get", "service/backend", "-o", "json").stdout)
    ssh_port = next(p["nodePort"] for p in service["spec"]["ports"] if p["name"] == "ssh")
    http_port = next(p["nodePort"] for p in service["spec"]["ports"] if p["name"] == "http")
    address = next(iter(inspection["NetworkSettings"]["Networks"].values()))["IPAddress"]
    cluster.apply([cluster.secret("fixture-oracle", {
        "client-key": (base / "client-key").read_bytes(), "unknown-key": (base / "unknown-key").read_bytes(),
        "known-hosts": (f"[{address}]:{ssh_port} " + (base / "host-key.pub").read_text()).encode(),
        "ca.crt": (base / "tls.crt").read_bytes(),
        "http-credentials": b"alice:synthetic-chart-token-0123456789abcdef",
    }), oracle_pod(oracle)])
    cluster.kubectl("wait", "--for=condition=Ready", "pod/native-oracle", "--timeout=60s")

    def client(phase, extra=()):
        result = cluster.kubectl("exec", "native-oracle", "--", "/usr/bin/python3", "-B",
            "/opt/git-rs/tests/kubernetes_oracle.py", phase, "--address", address,
            "--ssh-port", str(ssh_port), "--http-port", str(http_port),
            "--s3-endpoint", f"https://s3.{cluster.namespace}.svc:7070", *extra)
        return json.loads(result.stdout)

    client("bucket")
    assert_runtime_pods(cluster, runtime)
    cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--",
                    "/usr/local/bin/git-rs", "create", "team/project", "alice")
    print("Native SSH push/clone, unknown identity rejection, cold restart and Helm upgrade", flush=True)
    wait_ssh(address, ssh_port)
    client("ssh")
    # Plaintext is confined to this disposable Docker bridge fixture. Production
    # requires gateway HTTPS plus the encrypted trusted gateway-to-pod path.
    def http_listening():
        try:
            with socket.create_connection((address, http_port), timeout=1):
                return True
        except OSError:
            return False

    wait_for(http_listening, "authenticated HTTP service dataplane readiness")
    print("Native authenticated HTTP clone/push and unauthorized/forged identity rejection", flush=True)
    state = client("http")
    history = cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--",
        "/usr/local/bin/git-rs", "history", "team/project", "alice", "--ref", "refs/heads/main").stdout.decode().splitlines()
    assert history == state["history"], "Rust history differs from the independent native traversal"
    old_names = assert_runtime_pods(cluster, runtime)
    selector = "app.kubernetes.io/instance=smoke,app.kubernetes.io/component=serving"
    cluster.kubectl("rollout", "restart", "deployment/backend")
    cluster.kubectl("rollout", "status", "deployment/backend", "--timeout=120s")
    # Deployment readiness does not wait for terminating pods or for kube-proxy
    # to observe EndpointSlices. Establish the cold-start/transport preconditions
    # explicitly; never retry the Git operation whose result is under test.
    wait_for(lambda: not old_names.intersection(
        pod["metadata"]["name"] for pod in json.loads(
            cluster.kubectl("get", "pods", "-l", selector, "-o", "json").stdout)["items"]),
        "old serving pods to disappear before cold clone")
    wait_ssh(address, ssh_port)
    assert_runtime_pods(cluster, runtime)
    client("cold")
    command(install)
    wait_ssh(address, ssh_port)
    client("upgrade")
    assert_runtime_pods(cluster, runtime)
    maintenance = maintenance_contract(cluster, runtime, values, values_path, install, client)
    print(json.dumps({"status": "passed", "profile": "external TLS PostgreSQL/S3",
                      "runtime": runtime, "external_native_oracle": oracle, "history": "native topo-order equality",
                      "maintenance": maintenance,
                      "replicas": 2, "ssh": "generated identity, strict host verification, unknown key rejected",
                      "http": "Secret-mounted Basic credentials, native clone/push, unauthenticated and forged identity rejected",
                      "gateway": "not tested: direct disposable Docker bridge only, no TLS/mesh proof",
                      "cold_restart": "passed", "helm_upgrade_migration": "passed"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-container", required=True)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--runtime-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"),
                        help="prebuilt production Git-free image")
    parser.add_argument("--oracle-image", default=os.environ.get("BLAME_GATE_IMAGE"),
                        help="prebuilt external client/oracle image")
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"))
    parser.add_argument("--fixture-dir", type=Path, help="new private evidence directory retained on failure")
    args = parser.parse_args()
    assert args.runtime_image and args.oracle_image and re.fullmatch(r"[0-9a-f]{40}", args.revision or "")
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    if args.fixture_dir:
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(args.fixture_dir)
    else:
        context = tempfile.TemporaryDirectory(prefix="kubernetes-", dir=local)
    with context as directory:
        base = Path(directory)
        namespace = NamespaceFixture(args.cluster_container, base)
        cluster = Cluster(args.cluster_container, namespace.namespace)
        try:
            namespace.setup()
            exercise(args, base, cluster)
        except Exception:
            try:
                cluster.logs()
            except Exception as diagnostic_error:
                print("Kubernetes diagnostics unavailable:", type(diagnostic_error).__name__)
            raise
        finally:
            try:
                if cluster.process_watch:
                    cluster.process_watch.close()
            finally:
                try:
                    namespace.close()
                finally:
                    (base / "kubeconfig").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
