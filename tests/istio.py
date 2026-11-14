#!/usr/bin/env python3
"""External native clients through HTTPS Istio in a supervisor-owned aggregate.

Prepare checksum-pinned public artifacts separately. The measured phase consumes
prebuilt server/oracle inputs and the supervisor's fresh Kubernetes 1.37 cluster;
its independent cleanup owns cluster shutdown and actual CRI task drain.
"""
import argparse
import base64
from contextlib import nullcontext
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time

import yaml

from integration import PG_IMAGE, ROOT, S3_IMAGE, command
from istio_artifacts import ARTIFACTS, GATEWAY_API, IMAGES, ISTIO, artifacts, digest, prepare_artifacts, read_artifact
from kubernetes import Cluster, bound_image, dependency, import_image
from kubernetes_processes import PodProcessWatch

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


def platform_image(cluster, reference):
    before = json.loads(command(["docker", "image", "inspect", reference]).stdout)[0]["Id"]
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", before), "immutable platform image ID required"
    import_image(cluster, reference)
    assert json.loads(command(["docker", "image", "inspect", reference]).stdout)[0]["Id"] == before, "platform image changed during import"
    rows = command(["docker", "exec", cluster, "ctr", "images", "list", "name==" + reference]).stdout.decode().splitlines()
    matches = [row.split() for row in rows if row.split() and row.split()[0] == reference]
    assert len(matches) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", matches[0][2]), "imported Istio manifest missing"
    bound = reference.rsplit(":", 1)[0] + "@" + matches[0][2]
    imported = json.loads(command(["docker", "exec", cluster, "crictl", "inspecti", bound]).stdout)
    # Classic Docker identifies a config; its containerd image store identifies
    # the OCI manifest. Both must join this exact imported image, not a tag alone.
    assert before in {imported["status"]["id"], matches[0][2]}, "imported Istio image identity changed"
    return {"docker_id": before, "reference": bound, "cri_id": imported["status"]["id"]}


def finish(base, report, watchers):
    original, failures = sys.exception(), []
    for watcher in watchers:
        try:
            watcher.close()
        except BaseException as error:
            failures.append(error)
    if failures:
        report.update(status="failed", lifecycle_errors=[type(error).__name__ for error in failures])
    report["cleanup"] = "cluster and CRI drain owned by the outer runtime supervisor"
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".report-", dir=base)
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(json.dumps(report, indent=2) + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, base / "report.json")
        parent = os.open(base, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    except BaseException as error:
        failures.append(error)
    finally:
        if temporary is not None:
            try:
                Path(temporary).unlink(missing_ok=True)
            except BaseException as error:
                failures.append(error)
    if failures:
        if original is not None:
            original.add_note("Istio lifecycle/report cleanup also failed: " + ", ".join(type(error).__name__ for error in failures))
        else:
            raise BaseExceptionGroup("Istio lifecycle/report cleanup failed", failures)


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


def exercise(args, base, distribution, watchers):
    app, ingress = Cluster(args.cluster_container, APP), Cluster(args.cluster_container, INGRESS)
    wait(lambda: command(["docker", "exec", args.cluster_container, "kubectl", "get", "--raw",
                          "/readyz"], ok=False).returncode == 0, "owned k3s API")
    wait(lambda: read(app, "nodes")["items"], "owned k3s node registration")
    app.kubectl("wait", "--for=condition=Ready", "node", "--all", "--timeout=120s")
    for namespace in [APP, INGRESS, "istio-system"]:
        app.kubectl("create", "namespace", namespace)
    app.kubectl("label", "namespace", APP, "istio-injection=enabled")
    app.kubectl("apply", "--server-side", "-f", "-",
                data=read_artifact(args.artifacts, "gateway-api-" + GATEWAY_API + ".yaml"))
    inspection = json.loads(command(["docker", "inspect", args.cluster_container]).stdout)[0]
    config = yaml.safe_load(app.kubectl("config", "view", "--raw", "--flatten").stdout)
    api_port = inspection["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostPort"]
    config["clusters"][0]["cluster"]["server"] = f"https://127.0.0.1:{api_port}"
    kubeconfig = base / "kubeconfig"
    kubeconfig.write_text(yaml.safe_dump(config))
    kubeconfig.chmod(0o600)
    helm = [args.helm, "--kubeconfig", kubeconfig]
    platform = {name: platform_image(args.cluster_container, reference) for name, reference in zip(("pilot", "proxy"), IMAGES)}
    platform_values = base / "istiod.yaml"
    platform_values.write_text(yaml.safe_dump({
        "image": platform["pilot"]["reference"], "autoscaleEnabled": False, "replicaCount": 1,
        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"cpu": "500m", "memory": "512Mi"}},
        "global": {"imagePullPolicy": "Never", "proxy": {"image": platform["proxy"]["reference"],
            "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "250m", "memory": "256Mi"}}},
            "proxy_init": {"image": platform["proxy"]["reference"]}},
        "meshConfig": {"defaultConfig": {"concurrency": 1}},
        "gatewayClasses": {"istio": {"service": {"spec": {"type": "NodePort"}}}}}))
    print(f"Installing official Istio {ISTIO}, Gateway API {GATEWAY_API}", flush=True)
    for release, chart, extra in [
        ("istio-base", "base", []),
        ("istiod", "istio-control/istio-discovery", ["-f", platform_values]),
    ]:
        command([*helm, "install", release, distribution / "manifests/charts" / chart,
                 "-n", "istio-system", "--wait", "--timeout", "120s", *extra])
    runtime = bound_image(args.cluster_container, args.image, args.revision, "runtime")
    oracle = bound_image(args.cluster_container, args.oracle_image, args.revision, "external-client-oracle")
    # Subscribe before application Helm hooks or init containers can execute.
    watchers.append(PodProcessWatch(app, base, {"runtime": runtime, "external-client-oracle": oracle}))
    for image in [PG_IMAGE, S3_IMAGE]:
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
            doc["spec"]["template"]["spec"]["containers"][0]["resources"] = {
                "requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "250m", "memory": "256Mi"}}
    app.apply(fixtures)
    for name in ["postgres", "s3"]:
        app.kubectl("rollout", "status", "deployment/" + name, "--timeout=120s")
    repository, digest = runtime["reference"].rsplit("@", 1)
    values = {"fullnameOverride": "backend", "replicaCount": 2,
              "cacheBytes": 8 * 1024 * 1024,
              "migration": {"podAnnotations": {"sidecar.istio.io/inject": "false"}},
              "image": {"repository": repository, "digest": digest, "pullPolicy": "Never"},
              "ssh": {"enabled": False}, "cnpg": {"enabled": False}, "objectBucket": {"enabled": False},
              "http": {"enabled": True, "credentialsSecret": "fixture-http"},
              "httpRoute": {"enabled": True, "parentRefs": [{"name": "public-gateway", "namespace": INGRESS,
                                                            "sectionName": "https"}], "hostnames": [HOST]},
              "postgresql": {"host": f"postgres.{APP}.svc", "passwordSecret": "fixture-pg", "serverCASecret": "fixture-ca"},
              "s3": {"endpoint": f"https://s3.{APP}.svc:7070", "bucket": "packs", "credentialsSecret": "fixture-s3", "caSecret": "fixture-ca"},
              "resources": {"requests": {"cpu": "50m", "memory": "64Mi", "ephemeral-storage": "256Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi", "ephemeral-storage": "2Gi"}}}
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
    app.apply([{"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": "plaintext-probe", "annotations": {"sidecar.istio.io/inject": "false"}},
        "spec": {"automountServiceAccountToken": False, "restartPolicy": "Never", "activeDeadlineSeconds": 900,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001},
            "containers": [{"name": "probe", "image": oracle["reference"], "imagePullPolicy": "Never",
                "command": ["/usr/bin/tini", "--", "/usr/bin/sleep", "900"],
                "securityContext": {"readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]}},
                "resources": {"requests": {"cpu": "25m", "memory": "32Mi"},
                              "limits": {"cpu": "250m", "memory": "128Mi"}},
                "volumeMounts": [{"name": "ca", "mountPath": "/fixture-ca", "readOnly": True}]}],
            "volumes": [{"name": "ca", "secret": {"secretName": "fixture-ca", "defaultMode": 0o440}}]}}])
    app.kubectl("wait", "--for=condition=Ready", "pod/plaintext-probe", "--timeout=60s")
    app.kubectl("exec", "-i", "plaintext-probe", "--", "curl", "--disable", "--silent",
                "--fail", "--cacert", "/fixture-ca/ca.crt", "--request", "PUT", "--config", "-",
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
              "istio": ISTIO, "gateway_api": GATEWAY_API, "runtime": runtime, "external_probe": oracle,
              "platform_images": platform,
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
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default="helm", help="prebuilt Helm executable")
    parser.add_argument("--image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"), help="existing production runtime; never rebuilt")
    parser.add_argument("--oracle-image", default=os.environ.get("BLAME_GATE_IMAGE"), help="prebuilt external native-client image")
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"))
    parser.add_argument("--cluster-container", help="immutable cluster ID created by the runtime supervisor")
    parser.add_argument("--artifacts", type=Path, default=ROOT / ".local/istio-artifacts")
    parser.add_argument("--prepare", action="store_true", help="download pinned public artifacts outside measurement")
    parser.add_argument("--fixture-dir", type=Path)
    args = parser.parse_args()
    if args.prepare:
        prepare_artifacts(args.artifacts)
        return
    from blame_gate import invocation, LABEL
    from runtime_process import cluster_cgroups
    token = invocation()
    assert re.fullmatch(r"[0-9a-f]{64}", args.cluster_container or ""), "supervisor-owned immutable cluster ID required"
    cluster = json.loads(command(["docker", "inspect", args.cluster_container]).stdout)[0]
    assert cluster["Id"] == args.cluster_container and (cluster["Config"].get("Labels") or {}).get(LABEL[:-1]) == token
    root = os.environ["GIT_RS_TEST_POD_CGROUP_ROOT"]
    cluster_cgroups(args.cluster_container, root)
    assert args.image and args.oracle_image and re.fullmatch(r"[0-9a-f]{40}", args.revision or "")
    assert digest(args.helm) == os.environ["GIT_RS_TEST_HELM_SHA256"], "prebuilt Helm identity changed"
    assert command(["git", "--version"]).stdout == b"git version 2.56.0\n", "run the external client with the pinned native Git"
    if args.fixture_dir:
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(str(args.fixture_dir))
    else:
        context = tempfile.TemporaryDirectory(prefix="istio-", dir=ROOT / ".local")
    with context as directory:
        base = Path(directory)
        watchers = []
        report = {"status": "running", "revision": args.revision, "cleanup": False}
        try:
            distribution = artifacts(args.artifacts, base / "distribution")
            report.update(exercise(args, base, distribution, watchers))
            cluster_cgroups(args.cluster_container, root)
        except BaseException as error:
            report.update(status="failed", failure=type(error).__name__)
            raise
        finally:
            finish(base, report, watchers)


if __name__ == "__main__":
    main()
