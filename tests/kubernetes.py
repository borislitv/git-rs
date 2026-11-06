#!/usr/bin/env python3
"""Real Helm/SSH/authenticated HTTP smoke in a selected disposable Docker k3s cluster.

Uses synthetic credentials, generated TLS/SSH keys, and an isolated namespace.
It does not install CNPG/Rook operators or establish production durability.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import tempfile
import tomllib
import uuid

import yaml

from integration import PG_IMAGE, ROOT, S3_IMAGE, command, wait_for


class Cluster:
    def __init__(self, container, namespace):
        self.container, self.namespace = container, namespace

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
                                 "import", "-"], stdin=save.stdout, capture_output=True, timeout=120)
    finally:
        save.stdout.close()
        if save.poll() is None:
            save.terminate()
        save.wait(timeout=10)
    assert result.returncode == 0, result.stderr.decode()


def dependency(name, image, env, args, volumes, mounts, security=None):
    return [
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name},
         "spec": {"selector": {"fixture": name},
                  "ports": [{"port": 5432 if name == "postgres" else 7070}]}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
         "spec": {"replicas": 1, "selector": {"matchLabels": {"fixture": name}},
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


def exercise(args, base, cluster):
    if not args.skip_image_import:
        print("Importing application and synthetic dependency images into k3s", flush=True)
        for image in [args.image, PG_IMAGE, S3_IMAGE]:
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

    repository, tag = args.image.rsplit(":", 1)
    values = {"fullnameOverride": "backend", "replicaCount": 2,
              "image": {"repository": repository, "tag": tag, "pullPolicy": "Never"},
              "service": {"type": "NodePort"}, "cnpg": {"enabled": False},
              "postgresql": {"host": f"postgres.{cluster.namespace}.svc", "passwordSecret": "fixture-pg",
                             "serverCASecret": "fixture-ca"},
              "s3": {"endpoint": f"https://s3.{cluster.namespace}.svc:7070", "bucket": "packs",
                     "credentialsSecret": "fixture-s3", "caSecret": "fixture-ca"},
              "objectBucket": {"enabled": False},
              "http": {"enabled": True, "credentialsSecret": "fixture-http"},
              "ssh": {"hostKeysSecret": "fixture-host-keys", "authorizedKeysSecret": "fixture-principals"},
              "resources": {"requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "256Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "2Gi"}},
              "scratch": {"sizeLimit": "1Gi"}}
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
    cluster.kubectl("exec", "-i", "deployment/backend", "-c", "backend", "--", "curl", "--disable",
                    "--silent", "--fail", "--cacert", "/etc/git-rs/s3-ca/ca.crt", "--request", "PUT",
                    "--config", "-", data=(f'url = "https://s3.{cluster.namespace}.svc:7070/packs"\n'
                                           'aws-sigv4 = "aws:amz:us-east-1:s3"\n'
                                           'user = "test-access:synthetic-secret-key"\n').encode())
    cluster.kubectl("exec", "deployment/backend", "-c", "backend", "--",
                    "/usr/local/bin/git-rs", "create", "team/project", "alice")
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
    known_hosts = base / "known-hosts"
    known_hosts.write_text(f"[{address}]:{ssh_port} " + (base / "host-key.pub").read_text())
    env = {key: value for key, value in os.environ.items() if key in {"PATH", "LANG"}}
    env.update(HOME=str(base), GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Test Author", GIT_COMMITTER_NAME="Test Author",
               GIT_AUTHOR_EMAIL="author@example.invalid", GIT_COMMITTER_EMAIL="author@example.invalid")

    def ssh(key):
        return shlex.join(["ssh", "-p", str(ssh_port), "-i", str(base / key), "-o", "IdentitiesOnly=yes",
                           "-o", "IdentityAgent=none", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                           "-o", "UserKnownHostsFile=" + str(known_hosts)])

    env["GIT_SSH_COMMAND"] = ssh("client-key")
    # Repository names are relative logical slugs, not server filesystem paths.
    url = f"git-rs@{address}:team/project.git"
    source = base / "source"
    command(["git", "init", "-b", "main", source], env=env)
    payload = b"synthetic Kubernetes/SSH payload\n" * 16384
    (source / "payload.txt").write_bytes(payload)
    command(["git", "-C", source, "add", "."], env=env)
    command(["git", "-C", source, "-c", "commit.gpgsign=false", "commit", "-m", "Synthetic chart smoke"], env=env)
    print("Native SSH push/clone, unknown identity rejection, cold restart and Helm upgrade", flush=True)
    wait_ssh(address, ssh_port)
    command(["git", "-C", source, "push", url, "main"], env=env)
    command(["git", "clone", url, base / "clone"], env=env)
    assert (base / "clone/payload.txt").read_bytes() == payload
    rejected = command(["git", "ls-remote", url], env=dict(env, GIT_SSH_COMMAND=ssh("unknown-key")), ok=False)
    assert rejected.returncode and b"Permission denied" in rejected.stderr, rejected.stderr
    # Plaintext is confined to this disposable Docker bridge fixture. Production
    # requires gateway HTTPS plus the encrypted trusted gateway-to-pod path.
    http_url = f"http://{address}:{http_port}/team/project.git"
    auth = base64.b64encode(b"alice:synthetic-chart-token-0123456789abcdef").decode()
    http_env = dict(env, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                    GIT_CONFIG_VALUE_0="Authorization: Basic " + auth)

    def http_listening():
        try:
            with socket.create_connection((address, http_port), timeout=1):
                return True
        except OSError:
            return False

    wait_for(http_listening, "authenticated HTTP service dataplane readiness")
    print("Native authenticated HTTP clone/push and unauthorized/forged identity rejection", flush=True)
    command(["git", "clone", http_url, base / "http-clone"], env=http_env)
    assert (base / "http-clone/payload.txt").read_bytes() == payload
    (base / "http-clone/http.txt").write_bytes(b"authenticated chart HTTP push\n")
    command(["git", "-C", base / "http-clone", "add", "http.txt"], env=http_env)
    command(["git", "-C", base / "http-clone", "-c", "commit.gpgsign=false", "commit",
             "-m", "Synthetic HTTP chart push"], env=http_env)
    command(["git", "-C", base / "http-clone", "push", "origin", "main"], env=http_env)
    command(["git", "clone", http_url, base / "http-pushed"], env=http_env)
    assert (base / "http-pushed/http.txt").read_bytes() == b"authenticated chart HTTP push\n"
    for extra in [{}, {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.extraHeader",
                       "GIT_CONFIG_VALUE_0": "X-Forwarded-User: alice"},
                  {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.extraHeader",
                   "GIT_CONFIG_VALUE_0": "Authorization: Basic " + base64.b64encode(
                       b"alice:wrong-chart-token-0123456789abcdef").decode()}]:
        rejected = command(["git", "ls-remote", http_url], env=dict(env, **extra), ok=False)
        assert rejected.returncode and (b"401" in rejected.stderr or b"could not read Username" in rejected.stderr), rejected.stderr
    selector = "app.kubernetes.io/instance=smoke,app.kubernetes.io/name=git-rs"
    old_names = {pod["metadata"]["name"] for pod in json.loads(
        cluster.kubectl("get", "pods", "-l", selector, "-o", "json").stdout)["items"]}
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
    command(["git", "clone", url, base / "cold-clone"], env=env)
    assert (base / "cold-clone/payload.txt").read_bytes() == payload
    command(["git", "clone", http_url, base / "http-cold-clone"], env=http_env)
    assert (base / "http-cold-clone/http.txt").read_bytes() == b"authenticated chart HTTP push\n"
    command(install)
    wait_ssh(address, ssh_port)
    command(["git", "ls-remote", url], env=env)
    print(json.dumps({"status": "passed", "profile": "external TLS PostgreSQL/S3",
                      "replicas": 2, "ssh": "generated identity, strict host verification, unknown key rejected",
                      "http": "Secret-mounted Basic credentials, native clone/push, unauthenticated and forged identity rejected",
                      "gateway": "not tested: direct disposable Docker bridge only, no TLS/mesh proof",
                      "cold_restart": "passed", "helm_upgrade_migration": "passed"}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-container", required=True)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--image", default="git-rs:dev", help="already built local Docker image")
    parser.add_argument("--skip-image-import", action="store_true",
                        help="reuse images already imported by a previous local test run")
    args = parser.parse_args()
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    namespace = "git-rs-smoke-" + uuid.uuid4().hex[:8]
    cluster = Cluster(args.cluster_container, namespace)
    with tempfile.TemporaryDirectory(prefix="kubernetes-", dir=local) as directory:
        base = Path(directory)
        cluster.kubectl("create", "namespace", namespace)
        try:
            exercise(args, base, cluster)
        except Exception:
            cluster.logs()
            raise
        finally:
            cluster.kubectl("delete", "namespace", namespace, "--wait=false", ok=False)


if __name__ == "__main__":
    main()
