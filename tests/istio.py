#!/usr/bin/env python3
"""Native Git through a real HTTPS Istio Gateway in an exclusively owned k3s.

Requires Docker (privileged containers), Helm 3, Git, OpenSSL, PyYAML and network
access to public release artifacts/images. Never attaches to an existing cluster.
All credentials and data are synthetic. The owned cluster is removed on exit
unless explicitly retained for failure debugging with --keep-on-failure.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile
import time
import urllib.request

import yaml

from integration import PG_IMAGE, ROOT, S3_IMAGE, command
from kubernetes import Cluster, dependency, import_image

K3S = "rancher/k3s:v1.37.2-k3s1"
ISTIO = "1.31.3"
GATEWAY_API = "v1.7.0"
ARTIFACTS = {
    "istio-1.31.3-linux-amd64.tar.gz": (
        "https://github.com/istio/istio/releases/download/1.31.3/istio-1.31.3-linux-amd64.tar.gz",
        "b32bba09bc6138d738fd19a09ef6d0fcdd5c5c013efeb72969663c24a2bddd47"),
    "gateway-api-v1.7.0.yaml": (
        "https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml",
        "4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1"),
}
APP = "git-app"
INGRESS = "ingress"
HOST = "git.example.invalid"
TOKEN = "synthetic-istio-token-0123456789abcdef"


def wait(check, description, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(1)
    raise AssertionError(f"timed out: {description}")


def artifacts(cache):
    cache.mkdir(parents=True, exist_ok=True)
    for name, (url, digest) in ARTIFACTS.items():
        path = cache / name
        if not path.exists():
            with urllib.request.urlopen(url, timeout=90) as response:
                path.write_bytes(response.read())
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, f"checksum: {name}"
    # Helm uses the official charts shipped in this checksum-pinned distribution,
    # not a copied/reimplemented control-plane manifest or a mutable repo index.
    with tarfile.open(cache / "istio-1.31.3-linux-amd64.tar.gz") as archive:
        archive.extractall(cache, filter="data")
    return cache / "istio-1.31.3"


def read(cluster, resource):
    return json.loads(cluster.kubectl("get", resource, "-o", "json").stdout)


def conditions_current(document, conditions, required):
    observed = {c["type"] for c in conditions if c["status"] == "True"
                and c.get("observedGeneration") == document["metadata"]["generation"]}
    return required <= observed


def dictionaries(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from dictionaries(child)
    elif isinstance(value, list):
        for child in value:
            yield from dictionaries(child)


def certificate(base):
    command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
             "-subj", "/CN=git-rs-synthetic-ca", "-addext", "basicConstraints=critical,CA:TRUE",
             "-keyout", base / "ca.key", "-out", base / "ca.crt"])
    command(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={HOST}",
             "-keyout", base / "tls.key", "-out", base / "tls.csr"])
    (base / "extensions").write_text(
        f"subjectAltName=DNS:{HOST},DNS:postgres.{APP}.svc,DNS:s3.{APP}.svc\n"
        "extendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n")
    command(["openssl", "x509", "-req", "-in", base / "tls.csr", "-CA", base / "ca.crt",
             "-CAkey", base / "ca.key", "-CAcreateserial", "-days", "1", "-extfile",
             base / "extensions", "-out", base / "tls.crt"])


def exercise(args, base, distribution):
    app, ingress = Cluster(args.cluster_container, APP), Cluster(args.cluster_container, INGRESS)
    wait(lambda: command(["docker", "exec", args.cluster_container, "kubectl", "get", "--raw",
                          "/readyz"], ok=False).returncode == 0, "owned k3s API")
    wait(lambda: read(app, "nodes")["items"], "owned k3s node registration")
    app.kubectl("wait", "--for=condition=Ready", "node", "--all", "--timeout=120s")
    for namespace in [APP, INGRESS, "istio-system"]:
        app.kubectl("create", "namespace", namespace)
    app.kubectl("label", "namespace", APP, "istio-injection=enabled")
    app.kubectl("apply", "--server-side", "-f", "-",
                data=(args.artifacts / "gateway-api-v1.7.0.yaml").read_bytes())
    inspection = json.loads(command(["docker", "inspect", args.cluster_container]).stdout)[0]
    config = yaml.safe_load(app.kubectl("config", "view", "--raw", "--flatten").stdout)
    api_port = inspection["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    config["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{api_port}"
    kubeconfig = base / "kubeconfig"
    kubeconfig.write_text(yaml.safe_dump(config))
    kubeconfig.chmod(0o600)
    helm = [args.helm, "--kubeconfig", kubeconfig]
    platform_values = base / "istiod.yaml"
    platform_values.write_text(yaml.safe_dump({"gatewayClasses": {"istio": {
        "service": {"spec": {"type": "NodePort"}}}}}))
    print(f"Installing official Istio {ISTIO}, Gateway API {GATEWAY_API}", flush=True)
    for release, chart, extra in [
        ("istio-base", "base", []),
        ("istiod", "istio-control/istio-discovery", ["-f", platform_values]),
    ]:
        command([*helm, "install", release, distribution / "manifests/charts" / chart,
                 "-n", "istio-system", "--wait", "--timeout", "120s", *extra])
    for image in [args.image, PG_IMAGE, S3_IMAGE]:
        import_image(args.cluster_container, image)
    certificate(base)
    tls = {"tls.crt": (base / "tls.crt").read_bytes(), "tls.key": (base / "tls.key").read_bytes()}
    ingress.apply([ingress.secret("gateway-tls", tls), {
        "apiVersion": "gateway.networking.k8s.io/v1", "kind": "Gateway",
        "metadata": {"name": "public-gateway"}, "spec": {"gatewayClassName": "istio",
        "listeners": [{"name": "https", "hostname": HOST, "port": 443, "protocol": "HTTPS",
                       "tls": {"mode": "Terminate", "certificateRefs": [{"name": "gateway-tls"}]},
                       "allowedRoutes": {"namespaces": {"from": "All"}}}]}}])
    app.apply([app.secret("fixture-tls", tls),
               app.secret("fixture-ca", {"ca.crt": (base / "ca.crt").read_bytes()}),
               app.secret("fixture-pg", {"password": b"synthetic-password"}),
               app.secret("fixture-s3", {"AWS_ACCESS_KEY_ID": b"test-access",
                                          "AWS_SECRET_ACCESS_KEY": b"synthetic-secret-key"}),
               app.secret("fixture-http", {"credentials": f"alice {TOKEN}\n".encode()}),
               {"apiVersion": "security.istio.io/v1", "kind": "PeerAuthentication",
                "metadata": {"name": "strict"}, "spec": {"mtls": {"mode": "STRICT"}}}])
    volume = {"name": "tls", "secret": {"secretName": "fixture-tls", "defaultMode": 0o440}}
    mount = {"name": "tls", "mountPath": "/fixture-tls", "readOnly": True}
    fixtures = dependency("postgres", PG_IMAGE, {
        "POSTGRES_USER": "git_rs", "POSTGRES_DB": "git_rs", "POSTGRES_PASSWORD": "synthetic-password"},
        ["-c", "ssl=on", "-c", "ssl_cert_file=/fixture-tls/tls.crt", "-c", "ssl_key_file=/fixture-tls/tls.key"],
        [volume, {"name": "data", "emptyDir": {}}],
        [mount, {"name": "data", "mountPath": "/var/lib/postgresql"}],
        {"runAsUser": 999, "runAsGroup": 999, "fsGroup": 999})
    fixtures += dependency("s3", S3_IMAGE, {
        "ROOT_ACCESS_KEY_ID": "test-access", "ROOT_SECRET_ACCESS_KEY": "synthetic-secret-key"},
        ["--quiet", "--cert", "/fixture-tls/tls.crt", "--key", "/fixture-tls/tls.key", "posix", "/data"],
        [volume, {"name": "data", "emptyDir": {}}], [mount, {"name": "data", "mountPath": "/data"}])
    for doc in fixtures:
        if doc["kind"] == "Deployment":
            doc["spec"]["template"]["metadata"]["annotations"] = {"sidecar.istio.io/inject": "false"}
    app.apply(fixtures)
    for name in ["postgres", "s3"]:
        app.kubectl("rollout", "status", "deployment/" + name, "--timeout=120s")
    repository, tag = args.image.rsplit(":", 1)
    values = {"fullnameOverride": "backend", "replicaCount": 2,
              "image": {"repository": repository, "tag": tag, "pullPolicy": "Never"},
              "ssh": {"enabled": False}, "cnpg": {"enabled": False}, "objectBucket": {"enabled": False},
              "http": {"enabled": True, "credentialsSecret": "fixture-http"},
              "httpRoute": {"enabled": True, "parentRefs": [{"name": "public-gateway", "namespace": INGRESS,
                                                            "sectionName": "https"}], "hostnames": [HOST]},
              "postgresql": {"host": f"postgres.{APP}.svc", "passwordSecret": "fixture-pg", "serverCASecret": "fixture-ca"},
              "s3": {"endpoint": f"https://s3.{APP}.svc:7070", "bucket": "packs", "credentialsSecret": "fixture-s3", "caSecret": "fixture-ca"},
              "resources": {"requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "256Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "2Gi"}},
              "scratch": {"sizeLimit": "1Gi"}}
    values_path = base / "values.yaml"
    values_path.write_text(yaml.safe_dump(values))
    command([*helm, "install", "smoke", ROOT / "charts/git-rs", "-n", APP,
             "-f", values_path, "--wait", "--timeout", "120s"])

    def gateway_ready():
        doc = read(ingress, "gateway/public-gateway")
        return conditions_current(doc, doc.get("status", {}).get("conditions", []), {"Accepted", "Programmed"})

    def route_ready():
        doc = read(app, "httproute/backend")
        return any(parent.get("controllerName") == "istio.io/gateway-controller" and
                   parent["parentRef"]["name"] == "public-gateway" and
                   conditions_current(doc, parent.get("conditions", []), {"Accepted", "ResolvedRefs"})
                   for parent in doc.get("status", {}).get("parents", []))

    wait(gateway_ready, "Gateway Accepted and Programmed")
    wait(route_ready, "HTTPRoute Accepted and ResolvedRefs")
    assert read(app, "httproute/backend")["spec"]["rules"][0].get("retry") == {"attempts": 0}, \
        "installed Gateway API CRD pruned explicit retry disabling"
    gateway_deployment = read(ingress, "deployment/public-gateway-istio")
    account = gateway_deployment["spec"]["template"]["spec"]["serviceAccountName"]
    principal = f"cluster.local/ns/{INGRESS}/sa/{account}"
    app.apply([{"apiVersion": "security.istio.io/v1", "kind": "AuthorizationPolicy",
                "metadata": {"name": "gateway-only"}, "spec": {
                    "selector": {"matchLabels": {"app.kubernetes.io/name": "git-rs"}},
                    "action": "ALLOW", "rules": [{"from": [{"source": {"principals": [principal]}}]}]}}])
    pods = read(app, "pods")["items"]
    backends = [p for p in pods if p["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "git-rs"
                and p["metadata"].get("labels", {}).get("app.kubernetes.io/component") != "migration"
                and p["metadata"].get("ownerReferences", [{}])[0].get("kind") == "ReplicaSet"]
    assert len(backends) == 2
    for pod in backends:
        containers = pod["spec"]["containers"] + pod["spec"].get("initContainers", [])
        assert any(c["name"] == "istio-proxy" for c in containers), "sidecar injection missing"
    app.kubectl("exec", "-i", "deployment/backend", "-c", "backend", "--", "curl", "--disable", "--silent",
                "--fail", "--cacert", "/etc/git-rs/s3-ca/ca.crt", "--request", "PUT", "--config", "-",
                data=(f'url = "https://s3.{APP}.svc:7070/packs"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
                      'user = "test-access:synthetic-secret-key"\n').encode())
    app.kubectl("exec", "deployment/backend", "-c", "backend", "--", "git-rs", "create", "team/project", "alice")
    service = read(ingress, "service/public-gateway-istio")
    port = next(p["nodePort"] for p in service["spec"]["ports"] if p["port"] == 443)
    address = next(iter(inspection["NetworkSettings"]["Networks"].values()))["IPAddress"]
    url = f"https://{HOST}:{port}/team/project.git"
    resolve = f"{HOST}:{port}:{address}"
    auth = "Authorization: Basic " + base64.b64encode(f"alice:{TOKEN}".encode()).decode()
    curl = ["curl", "--disable", "--noproxy", "*", "--silent", "--show-error", "--max-time", "5",
            "--cacert", base / "ca.crt", "--resolve", resolve]
    discovery = url + "/info/refs?service=git-upload-pack"
    wait(lambda: command([*curl, "-o", "/dev/null", "-w", "%{http_code}", discovery], ok=False).stdout == b"401",
         "HTTPS route dataplane and application authentication")
    gateway_config = json.loads(ingress.kubectl("exec", "deployment/public-gateway-istio", "--",
                                               "pilot-agent", "request", "GET", "config_dump").stdout)
    actions = [item["route"] for item in dictionaries(gateway_config)
               if isinstance(item.get("route"), dict)
               and item["route"].get("cluster") == f"outbound|8080||backend.{APP}.svc.cluster.local"]
    assert actions, "gateway has no route to chart HTTP service"
    print("HTTPRoute rule:", json.dumps(read(app, "httproute/backend")["spec"]["rules"]), flush=True)
    print("Envoy backend route:", json.dumps(actions), flush=True)
    assert all(action.get("retry_policy", action.get("retryPolicy", {})).get(
        "num_retries", action.get("retryPolicy", {}).get("numRetries", 0)) == 0 for action in actions), \
        "gateway would automatically retry Git POST requests"
    # A negative trust control proves the client isn't silently accepting this CA.
    untrusted = command(["curl", "--disable", "--noproxy", "*", "--silent", "--max-time", "5",
                         "--resolve", resolve, discovery], ok=False)
    assert untrusted.returncode == 60, untrusted.stderr
    for headers in [[], ["X-Forwarded-User: alice"], ["X-Remote-User: alice"],
                    ["Authorization: Basic " + base64.b64encode(b"alice:wrong-token-0123456789abcdef012345").decode()]]:
        extra = [arg for header in headers for arg in ["-H", header]]
        rejected = command([*curl, *extra, "-o", "/dev/null", "-w", "%{http_code}", discovery])
        assert rejected.stdout == b"401", (headers, rejected.stdout)
    env = {k: v for k, v in os.environ.items() if k in {"PATH", "LANG"}}
    env.update(HOME=str(base), GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null", GIT_TERMINAL_PROMPT="0",
               GIT_AUTHOR_NAME="Synthetic Author", GIT_COMMITTER_NAME="Synthetic Author",
               GIT_AUTHOR_EMAIL="author@example.invalid", GIT_COMMITTER_EMAIL="author@example.invalid",
               GIT_CONFIG_COUNT="4", GIT_CONFIG_KEY_0="http.curloptResolve", GIT_CONFIG_VALUE_0=resolve,
               GIT_CONFIG_KEY_1="http.sslCAInfo", GIT_CONFIG_VALUE_1=str(base / "ca.crt"),
               GIT_CONFIG_KEY_2="http.extraHeader", GIT_CONFIG_VALUE_2=auth,
               GIT_CONFIG_KEY_3="http.sslVerify", GIT_CONFIG_VALUE_3="true")
    source = base / "source"
    command(["git", "init", "-b", "main", source], env=env)
    payload = b"synthetic Git through HTTPS Istio and strict mTLS\n" * 16384
    (source / "payload.txt").write_bytes(payload)
    command(["git", "-C", source, "add", "."], env=env)
    command(["git", "-C", source, "-c", "commit.gpgsign=false", "commit", "-m", "Synthetic Istio push"], env=env)
    # Mutations are never retried. Readiness polling above doesn't push or publish.
    command(["git", "-C", source, "push", url, "main"], env=env)
    command(["git", "clone", url, base / "clone"], env=env)
    assert (base / "clone/payload.txt").read_bytes() == payload
    expected = command(["git", "-C", source, "rev-parse", "HEAD"], env=env).stdout
    assert command(["git", "-C", base / "clone", "rev-parse", "HEAD"], env=env).stdout == expected
    # Actual destination telemetry must identify the authenticated mesh peer.
    metrics = b"".join(app.kubectl("exec", p["metadata"]["name"], "-c", "istio-proxy", "--",
                                  "pilot-agent", "request", "GET", "stats/prometheus").stdout for p in backends)
    assert any(b'connection_security_policy="mutual_tls"' in line and principal.encode() in line
               and b'istio_requests_total{' in line and b'reporter="destination"' in line
               for line in metrics.splitlines()), "missing gateway-to-backend mutual TLS telemetry"
    # A non-mesh client with otherwise valid app credentials cannot bypass STRICT.
    app.apply([{"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": "plaintext-probe", "annotations": {"sidecar.istio.io/inject": "false"}},
        "spec": {"automountServiceAccountToken": False, "restartPolicy": "Never", "containers": [
            {"name": "probe", "image": args.image, "imagePullPolicy": "Never", "command": ["sleep", "600"]}]}}])
    app.kubectl("wait", "--for=condition=Ready", "pod/plaintext-probe", "--timeout=60s")
    plaintext = app.kubectl("exec", "plaintext-probe", "--", "curl", "--disable", "--silent", "--show-error", "--max-time", "5",
                            "-H", auth, f"http://backend:8080/team/project.git/info/refs?service=git-upload-pack", ok=False)
    assert plaintext.returncode in {52, 56}, ("expected STRICT connection rejection, not DNS/setup failure",
                                            plaintext.returncode, plaintext.stderr)
    old_names = {p["metadata"]["name"] for p in backends}
    app.kubectl("rollout", "restart", "deployment/backend")
    app.kubectl("rollout", "status", "deployment/backend", "--timeout=120s")
    wait(lambda: not old_names.intersection(p["metadata"]["name"] for p in read(app, "pods")["items"]),
         "old backend pods removed")
    wait(lambda: command([*curl, "-H", auth, "-o", "/dev/null", "-w", "%{http_code}", discovery], ok=False).stdout == b"200",
         "restarted mesh endpoints")
    command(["git", "clone", url, base / "cold-clone"], env=env)
    assert (base / "cold-clone/payload.txt").read_bytes() == payload
    assert command(["git", "-C", base / "cold-clone", "rev-parse", "HEAD"], env=env).stdout == expected
    report = {"status": "passed", "k3s": read(app, "nodes")["items"][0]["status"]["nodeInfo"]["kubeletVersion"],
              "istio": ISTIO, "gateway_api": GATEWAY_API, "image": command([
                  "docker", "inspect", args.image, "--format", "{{.Id}}"]).stdout.decode().strip(),
              "helm": command([args.helm, "version", "--short"]).stdout.decode().strip(),
              "gateway": "Accepted + Programmed (current generation)",
              "http_route": "Accepted + ResolvedRefs (current generation)",
              "git": "HTTPS verified CA/hostname, HTTP Basic push/clone and restart readback",
              "negative_controls": "untrusted CA; missing/wrong auth; spoofed identity; non-mesh plaintext rejected",
              "mesh": "STRICT + gateway-only principal policy + destination mutual_tls telemetry",
              "gateway_retries": "no configured retries on actual backend route",
              "plaintext_probe_exit": plaintext.returncode,
              "artifacts_sha256": {name: digest for name, (_, digest) in ARTIFACTS.items()}}
    print(json.dumps(report, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default="helm", help="official Helm 3 executable")
    parser.add_argument("--image", default="git-rs:dev", help="existing local production image; never rebuilt")
    parser.add_argument("--cluster-container", default="git-rs-istio-test", choices=["git-rs-istio-test"],
                        help="exclusive disposable cluster name; existing containers are never reused")
    parser.add_argument("--artifacts", type=Path, default=ROOT / ".local/istio-artifacts")
    parser.add_argument("--keep-on-failure", action="store_true",
                        help="retain only this owned disposable cluster for explicit debugging")
    args = parser.parse_args()
    assert command(["docker", "container", "inspect", args.cluster_container], ok=False).returncode != 0, \
        "refusing to reuse or remove an existing cluster"
    distribution = artifacts(args.artifacts)
    with tempfile.TemporaryDirectory(prefix="istio-", dir=ROOT / ".local") as directory:
        base = Path(directory)
        container_id = None
        failed = False
        try:
            container_id = command(["docker", "create", "--name", args.cluster_container, "--privileged",
                                    "--label", "git-rs.test=istio", "--tmpfs", "/run", "--tmpfs", "/var/run",
                                    "-p", "127.0.0.1::6443", K3S, "server", "--disable=traefik",
                                    "--disable=servicelb", "--disable=gateway-api-crd",
                                    "--disable=metrics-server", "--disable=local-storage"]).stdout.decode().strip()
            command(["docker", "start", container_id])
            exercise(args, base, distribution)
        except Exception:
            failed = True
            if container_id:
                for namespace in [APP, INGRESS, "istio-system"]:
                    try:
                        Cluster(args.cluster_container, namespace).logs()
                    except Exception as error:
                        print(f"diagnostics unavailable for {namespace}: {error}", flush=True)
            raise
        finally:
            if container_id and not (failed and args.keep_on_failure):
                command(["docker", "stop", "--timeout", "30", container_id])
                command(["docker", "rm", "-v", container_id])


if __name__ == "__main__":
    main()
