#!/usr/bin/env python3
"""Render/contract tests for the independent application Helm chart."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import tomllib
import urllib.request

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts/git-rs"
ENDPOINT = {"s3": {"endpoint": "https://objects.example.invalid"}}


def render(helm, values=None, *, file=None, error=None):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "values.json"
        path.write_text(json.dumps(values or {}))
        base = Path(directory) / "base.json"
        base.write_text(json.dumps(ENDPOINT))
        command = [helm, "template", "contract", str(CHART), "--namespace", "git-test",
                   "--kube-version", "1.37.0", "--values", str(base)]
        if file:
            command += ["--values", str(file)]
        result = subprocess.run(command + ["--values", str(path)], capture_output=True,
                                text=True, timeout=30)
    if error:
        assert result.returncode and error in result.stderr, (values, error, result.stderr)
        return []
    assert result.returncode == 0, result.stderr
    documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert not {"CephCluster", "CephObjectStore", "StorageClass"}.intersection(
        doc["kind"] for doc in documents)
    return documents


def resource(documents, kind, name=None):
    found = [doc for doc in documents if doc["kind"] == kind
             and (name is None or doc["metadata"]["name"] == name)]
    assert len(found) == 1, (kind, len(found))
    return found[0]


def maintenance_contracts(helm):
    documents = render(helm, {"maintenance": {"enabled": True}})
    serving = resource(documents, "Deployment", "contract-git-rs")
    worker = resource(documents, "Deployment", "contract-git-rs-maintenance")
    service = resource(documents, "Service")["spec"]["selector"]
    serving_pod = serving["spec"]["template"]
    worker_pod = worker["spec"]["template"]
    assert all(serving_pod["metadata"]["labels"].get(key) == value for key, value in service.items())
    assert not all(worker_pod["metadata"]["labels"].get(key) == value for key, value in service.items())
    assert not all(worker_pod["metadata"]["labels"].get(key) == value
                   for key, value in serving["spec"]["selector"]["matchLabels"].items())
    assert worker["spec"]["replicas"] == 2
    pod = worker_pod["spec"]
    assert pod["serviceAccountName"] != serving_pod["spec"]["serviceAccountName"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["terminationGracePeriodSeconds"] == 110
    assert pod["securityContext"]["runAsUser"] == 10001
    backend = pod["containers"][0]
    assert len(pod["containers"]) == 1
    assert backend["image"] == serving_pod["spec"]["containers"][0]["image"]
    assert backend["args"] == ["maintenance", "--scratch", "/scratch/private", "--health", "0.0.0.0:8081"]
    assert backend["securityContext"]["readOnlyRootFilesystem"]
    assert backend["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert backend["readinessProbe"]["httpGet"]["path"] == "/readyz"
    env = {item["name"]: item for item in backend["env"]}
    assert env["GIT_RS_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    assert env["GIT_RS_LEADER_LEASE_NAMESPACE"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.namespace"
    assert env["GIT_RS_LEADER_LEASE_NAME"]["value"] == "contract-git-rs-source"
    assert env["GIT_RS_MAINTENANCE_CONCURRENCY"]["value"] == "1"
    assert env["GIT_RS_MAINTENANCE_SHUTDOWN_SECONDS"]["value"] == "90"
    assert env["PGTARGETSESSIONATTRS"]["value"] == "read-write"
    volumes = {volume["name"]: volume for volume in pod["volumes"]}
    assert volumes["scratch"]["emptyDir"] == {"medium": "Memory", "sizeLimit": "2Gi"}
    assert not any("persistentVolumeClaim" in volume for volume in pod["volumes"])
    projection = volumes["identity"]["projected"]
    assert projection["defaultMode"] == 0o440
    assert projection["sources"] == [
        {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}]}},
    ]
    lease = resource(documents, "Lease")
    assert lease["metadata"]["name"] == "contract-git-rs-source"
    assert lease["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert lease["spec"] == {}, "chart upgrades must never declare controller-owned live Lease fields"
    role = resource(documents, "Role")
    assert role["rules"] == [{"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
                              "resourceNames": [lease["metadata"]["name"]], "verbs": ["get", "patch"]}]
    binding = resource(documents, "RoleBinding")
    assert binding["roleRef"] == {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role["metadata"]["name"]}
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": pod["serviceAccountName"], "namespace": "git-test"}]
    assert not any(doc["kind"] in {"ClusterRole", "ClusterRoleBinding", "Secret"} for doc in documents)
    configured = render(helm, {"maintenance": {"enabled": True, "leaseName": "existing.source",
        "tokenAudience": "api.example.invalid", "serviceAccount": {"create": False, "name": "source-worker"},
        "scratch": {"medium": "", "sizeLimit": "3Gi"}}})
    configured_pod = resource(configured, "Deployment", "contract-git-rs-maintenance")["spec"]["template"]["spec"]
    assert configured_pod["serviceAccountName"] == "source-worker"
    assert not any(doc["kind"] == "ServiceAccount" and doc["metadata"]["name"] == "source-worker" for doc in configured)
    volumes = {volume["name"]: volume for volume in configured_pod["volumes"]}
    assert volumes["scratch"]["emptyDir"] == {"sizeLimit": "3Gi"}
    assert volumes["identity"]["projected"]["sources"][0]["serviceAccountToken"]["audience"] == "api.example.invalid"
    assert resource(configured, "Role")["rules"][0]["resourceNames"] == ["existing.source"]


def validate_operator_schemas(profiles):
    # Explicit opt-in network contract test against pinned public operator CRDs.
    sources = [
        "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/config/crd/bases/postgresql.cnpg.io_clusters.yaml",
        "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/config/crd/bases/postgresql.cnpg.io_scheduledbackups.yaml",
        "https://raw.githubusercontent.com/cloudnative-pg/plugin-barman-cloud/v0.15.1/config/crd/bases/barmancloud.cnpg.io_objectstores.yaml",
        "https://raw.githubusercontent.com/rook/rook/v1.21.1/deploy/examples/crds.yaml",
        "https://raw.githubusercontent.com/kubernetes-sigs/gateway-api/v1.7.0/config/crd/standard/gateway.networking.k8s.io_httproutes.yaml",
    ]
    schemas = {}
    for url in sources:
        with urllib.request.urlopen(url, timeout=30) as response:
            for crd in yaml.safe_load_all(response.read()):
                if not crd or crd.get("kind") != "CustomResourceDefinition":
                    continue
                spec = crd["spec"]
                for version in spec["versions"]:
                    schemas[(spec["group"] + "/" + version["name"], spec["names"]["kind"])] = version["schema"]["openAPIV3Schema"]
    checked = set()
    for documents in profiles:
        for doc in documents:
            key = (doc["apiVersion"], doc["kind"])
            if key in schemas:
                jsonschema.Draft4Validator(schemas[key]).validate(doc)
                checked.add(doc["kind"])
    assert {"Cluster", "ScheduledBackup", "ObjectStore", "ObjectBucketClaim", "HTTPRoute"} <= checked, checked
    print("Operator CRD schemas passed: " + ", ".join(sorted(checked)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--operator-schemas", action="store_true",
                        help="validate rendered resources against pinned public CNPG/Barman/OBC/Gateway CRDs")
    args = parser.parse_args()
    subprocess.run([args.helm, "lint", str(CHART), "--strict", "--values",
                    str(CHART / "values-example.yaml")], check=True, timeout=30)
    metadata = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert metadata.get("dependencies", []) == []
    assert not (CHART / "charts").exists()
    assert metadata["version"] == metadata["appVersion"]
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    assert metadata["appVersion"] == manifest["package"]["version"]
    application = next(package for package in lock["package"]
                       if package["name"] == manifest["package"]["name"])
    assert application["version"] == metadata["appVersion"]
    defaults = render(args.helm)
    deployment = resource(defaults, "Deployment")
    pod = deployment["spec"]["template"]["spec"]
    backend = next(c for c in pod["containers"] if c["name"] == "backend")
    env = {item["name"]: item for item in backend["env"]}
    assert env["GIT_RS_CACHE_BYTES"]["value"] == "67108864", env["GIT_RS_CACHE_BYTES"]
    assert "GIT_RS_PACK_THREADS" not in env
    assert env["GIT_RS_BLAME_CONCURRENCY"]["value"] == "1"
    assert env["GIT_RS_BLAME_WAITERS"]["value"] == "0"
    assert env["PGSSLMODE"]["value"] == "verify-full"
    assert "GIT_RS_DEBUG_TIMINGS" not in env
    assert env["PGTARGETSESSIONATTRS"]["value"] == "read-write"
    assert env["PGHOST"]["value"] == "contract-git-rs-pg-rw"
    assert "valueFrom" in env["GIT_RS_S3_BUCKET"]
    assert "valueFrom" in env["GIT_RS_S3_SECRET_KEY"]
    assert env["GIT_RS_S3_ENDPOINT"]["value"] == "https://objects.example.invalid"
    assert env["GIT_RS_S3_BUCKET"]["valueFrom"]["configMapKeyRef"] == {
        "name": "contract-git-rs-packs", "key": "BUCKET_NAME"}
    assert env["GIT_RS_S3_ACCESS_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": "contract-git-rs-packs", "key": "AWS_ACCESS_KEY_ID"}
    assert env["GIT_RS_S3_SECRET_KEY"]["valueFrom"]["secretKeyRef"]["key"] == "AWS_SECRET_ACCESS_KEY"
    assert "--http" not in backend["args"]
    assert not any(doc["kind"] == "HTTPRoute" for doc in defaults)
    assert not pod["automountServiceAccountToken"]
    assert backend["securityContext"]["runAsNonRoot"]
    assert backend["securityContext"]["readOnlyRootFilesystem"]
    assert not any("persistentVolumeClaim" in volume for volume in pod["volumes"])
    assert resource(defaults, "Service")["spec"]["selector"] == {
        **deployment["spec"]["selector"]["matchLabels"], "app.kubernetes.io/component": "serving"}
    assert "--scratch" not in backend["args"]
    assert not any(volume["name"] == "scratch" for volume in pod["volumes"])
    assert not any(doc["kind"] in {"Lease", "Role", "RoleBinding"} for doc in defaults)
    assert not any(item["name"].startswith("GIT_RS_LEADER_") for item in backend["env"])
    pg = resource(defaults, "Cluster")
    assert pg["spec"]["bootstrap"]["initdb"] == {"database": "git_rs", "owner": "git_rs"}
    assert pg["spec"]["postgresql"]["synchronous"] == {"method": "any", "number": 1, "dataDurability": "required"}
    assert pg["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert resource(defaults, "ObjectBucketClaim")["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert resource(defaults, "ObjectBucketClaim")["metadata"]["annotations"]["argocd.argoproj.io/sync-options"] == "Prune=false,Delete=false"
    assert resource(defaults, "ObjectBucketClaim")["metadata"]["namespace"] == "git-test"
    assert "AuthorizedKeysFile none" in resource(defaults, "ConfigMap")["data"]["sshd_config"]
    assert "AuthorizedKeysCommandUser git-rs" in resource(defaults, "ConfigMap")["data"]["sshd_config"]
    assert resource(defaults, "Job")["metadata"]["annotations"]["helm.sh/hook"] == "post-install,post-upgrade"
    migration = resource(render(args.helm, {"migration": {"podAnnotations": {"sidecar.istio.io/inject": "false"}}}), "Job")
    assert migration["spec"]["template"]["metadata"]["annotations"] == {"sidecar.istio.io/inject": "false"}
    assert migration["spec"]["template"]["spec"]["containers"][0]["args"] == ["migrate"]
    assert backend["image"] == f"ghcr.io/borislitv/git-rs:{manifest['package']['version']}"
    assert pg["spec"]["imageName"].endswith("@sha256:605c6e45e733ce0e908505e9ac3c441637d6794d330ec0e9d8d6f631b6918b1e")

    external_file = CHART / "values-external.example.yaml"
    external = render(args.helm, file=external_file)
    assert not any(doc["kind"] in {"Cluster", "ObjectBucketClaim", "CephObjectStore",
                                  "StorageClass", "ObjectStore"} for doc in external)
    pod = resource(external, "Deployment")["spec"]["template"]["spec"]
    env = {item["name"]: item for item in pod["containers"][0]["env"]}
    assert env["PGHOST"]["value"] == "postgres.example.invalid"
    assert env["GIT_RS_S3_BUCKET"]["value"] == "git-rs-packs"
    assert env["GIT_RS_S3_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"] == "git-rs-s3"
    assert next(v for v in pod["volumes"] if v["name"] == "pg-ca")["secret"]["secretName"] == "git-rs-postgresql-ca"

    routed = render(args.helm, file=CHART / "values-example.yaml")
    claim = resource(routed, "ObjectBucketClaim")
    assert claim["spec"] == {"storageClassName": "ceph-bucket", "generateBucketName": "contract-git-rs-packs"}
    route = resource(routed, "HTTPRoute")
    assert route["apiVersion"] == "gateway.networking.k8s.io/v1"
    assert route["spec"]["parentRefs"] == [{"name": "public-gateway", "namespace": "ingress", "sectionName": "https"}]
    assert route["spec"]["hostnames"] == ["git.example.invalid"]
    rule = route["spec"]["rules"][0]
    assert rule["backendRefs"] == [{"name": "contract-git-rs", "port": 8080}]
    assert rule["timeouts"] == {"request": "300s", "backendRequest": "300s"}
    assert rule["retry"] == {"attempts": 0} and "filters" not in rule
    pod = resource(routed, "Deployment")["spec"]["template"]["spec"]
    assert pod["containers"][0]["args"][-4:] == ["--http", "0.0.0.0:8080", "--http-credentials", "/etc/git-rs/http/credentials"]
    assert {"name": "http-credentials", "mountPath": "/etc/git-rs/http", "readOnly": True} in pod["containers"][0]["volumeMounts"]
    assert next(v for v in pod["volumes"] if v["name"] == "http-credentials")["secret"] == {
        "secretName": "git-rs-http", "defaultMode": 0o440, "items": [{"key": "credentials", "path": "credentials"}]}
    assert {"name": "http", "port": 8080, "targetPort": "http"} in resource(routed, "Service")["spec"]["ports"]
    http_only = render(args.helm, {"ssh": {"enabled": False}, "http": {
        "enabled": True, "credentialsSecret": "existing-http", "credentialsKey": "tokens", "port": 9000}})
    assert resource(http_only, "Service")["spec"]["ports"] == [{"name": "http", "port": 9000, "targetPort": "http"}]
    pod = resource(http_only, "Deployment")["spec"]["template"]["spec"]
    assert "0.0.0.0:9000" in pod["containers"][0]["args"]
    assert {"name": "http", "containerPort": 9000} in pod["containers"][0]["ports"]
    assert next(v for v in pod["volumes"] if v["name"] == "http-credentials")["secret"]["items"] == [{"key": "tokens", "path": "credentials"}]
    http_on_ssh_port = render(args.helm, {"ssh": {"enabled": False}, "http": {
        "enabled": True, "credentialsSecret": "existing-http", "port": 2222}})
    assert resource(http_on_ssh_port, "Service")["spec"]["ports"] == [
        {"name": "http", "port": 2222, "targetPort": "http"}]

    customized = render(args.helm, {
        "fullnameOverride": "isolated", "cacheBytes": 0, "debugTimings": True, "cpu": {"concurrency": 4, "waiters": 0},
        "image": {"repository": "example.invalid/git-rs", "digest": "sha256:" + "a" * 64},
        "cnpg": {"name": "metadata-db"},
        "objectBucket": {"name": "packs-claim", "bucketName": "unique-packs", "storageClassName": "retained-buckets", "additionalConfig": {"maxObjects": "1000"}},
        "serviceAccount": {"create": False, "name": "existing"},
        "ssh": {"enabled": False}, "migration": {"enabled": False},
    })
    pod = resource(customized, "Deployment")["spec"]["template"]["spec"]
    assert resource(customized, "Deployment")["metadata"]["name"] == "isolated"
    assert pod["serviceAccountName"] == "existing"
    assert len(pod["containers"]) == 1
    assert pod["containers"][0]["image"] == "example.invalid/git-rs@sha256:" + "a" * 64
    assert not any(doc["kind"] in {"Service", "ConfigMap", "ServiceAccount", "Job"} for doc in customized)
    assert resource(customized, "ObjectBucketClaim")["spec"]["bucketName"] == "unique-packs"
    assert resource(customized, "ObjectBucketClaim")["spec"]["additionalConfig"] == {"maxObjects": "1000"}
    assert resource(customized, "ObjectBucketClaim")["spec"]["storageClassName"] == "retained-buckets"
    env = {item["name"]: item for item in pod["containers"][0]["env"]}
    assert env["GIT_RS_CACHE_BYTES"]["value"] == "0"
    assert "GIT_RS_PACK_THREADS" not in env
    assert env["GIT_RS_BLAME_CONCURRENCY"]["value"] == "4"
    assert env["GIT_RS_DEBUG_TIMINGS"]["value"] == "1"
    assert env["PGHOST"]["value"] == "metadata-db-rw"
    assert env["GIT_RS_S3_BUCKET"]["valueFrom"]["configMapKeyRef"]["name"] == "packs-claim"

    long_operations = render(args.helm, {"operationSeconds": 3600})
    pod = resource(long_operations, "Deployment")["spec"]["template"]["spec"]
    backend = next(container for container in pod["containers"] if container["name"] == "backend")
    env = {item["name"]: item for item in backend["env"]}
    assert env["GIT_RS_OPERATION_SECONDS"]["value"] == "3600"

    backup = {"enabled": True, "spec": {"configuration": {
        "destinationPath": "s3://git-rs-backup/metadata", "s3Credentials": {
            "accessKeyId": {"name": "backup-creds", "key": "access"},
            "secretAccessKey": {"name": "backup-creds", "key": "secret"}}}}}
    backed_up = render(args.helm, {"cnpg": {"backup": backup}})
    assert resource(backed_up, "ObjectStore")["spec"] == backup["spec"]
    pg = resource(backed_up, "Cluster")
    plugin = pg["spec"]["plugins"][0]
    assert plugin["name"] == "barman-cloud.cloudnative-pg.io" and plugin["isWALArchiver"]
    assert plugin["parameters"]["barmanObjectName"] == resource(backed_up, "ObjectStore")["metadata"]["name"]
    scheduled = resource(backed_up, "ScheduledBackup")["spec"]
    assert scheduled["method"] == "plugin" and scheduled["cluster"]["name"] == pg["metadata"]["name"]

    maintenance_contracts(args.helm)

    invalid = [
        ({"cacheByte": 1}, "cacheByte"),
        ({"replicaCount": 0}, "replicaCount"),
        ({"operationSeconds": 0}, "operationSeconds"),
        ({"operationSeconds": 3601}, "operationSeconds"),
        ({"packThreads": 0}, "packThreads"),
        ({"packThreads": 5}, "packThreads"),
        ({"packThreads": "auto"}, "packThreads"),
        ({"cpu": {"concurrency": 0}}, "concurrency"),
        ({"cpu": {"concurrency": 5}}, "concurrency"),
        ({"cpu": {"concurrency": 2, "waiters": 3}}, "at most four"),
        ({"maintenance": {"enabled": True, "renewGraceSeconds": 12}}, "lease margins"),
        ({"maintenance": {"enabled": True, "leaseSeconds": 27}}, "lease margins"),
        ({"maintenance": {"enabled": True, "pgFenceLeaseSeconds": 17}}, "PG/job leases"),
        ({"maintenance": {"enabled": True, "jobLeaseSeconds": 17}}, "PG/job leases"),
        ({"maintenance": {"enabled": True, "terminationGracePeriodSeconds": 95}}, "terminationGracePeriodSeconds"),
        ({"maintenance": {"enabled": True, "serviceAccount": {"create": False}}}, "maintenance.serviceAccount.name"),
        ({"maintenance": {"enabled": True, "serviceAccount": {"name": "contract-git-rs"}}}, "distinct from serving"),
        ({"maintenance": {"sourceConcurrency": 2}}, "sourceConcurrency"),
        ({"maintenance": {"leaseName": "bad..name"}}, "leaseName"),
        ({"maintenance": {"leaseName": "a" * 64}}, "leaseName"),
        ({"maintenance": {"serviceAccount": {"name": "bad/name"}}}, "name"),
        ({"maintenance": {"scratch": {"medium": "PersistentVolume"}}}, "medium"),
        ({"maintenance": {"scanBudgetMillis": 1001}}, "scanBudgetMillis"),
        ({"debugTimings": "true"}, "debugTimings"),
        ({"migration": {"podAnnotations": {"sidecar.istio.io/inject": False}}}, "sidecar.istio.io/inject"),
        ({"image": {"digest": "sha256:bad"}}, "digest"),
        ({"s3": {"endpoint": "http://s3.example.invalid"}}, "endpoint"),
        ({"cnpg": {"enabled": False}}, "postgresql.host is required"),
        ({"s3": {"endpoint": ""}}, "s3.endpoint is required"),
        ({"objectBucket": {"enabled": False}}, "s3.bucket is required"),
        ({"objectBucket": {"enabled": False}, "s3": {"bucket": "packs"}}, "s3.credentialsSecret is required"),
        ({"rookCeph": {}}, "rookCeph"),
        ({"objectBucket": {"storageClassName": ""}}, "storageClassName"),
        ({"objectBucket": {"additionalConfig": {"maxObjects": 1000}}}, "maxObjects"),
        ({"http": {"enabled": True}}, "http.credentialsSecret is required"),
        ({"http": {"enabled": True, "credentialsSecret": "creds", "port": 8081}}, "collide"),
        ({"http": {"enabled": True, "credentialsSecret": "creds", "port": 2222}}, "collide"),
        ({"http": {"enabled": True, "credentialsSecret": "creds"}, "service": {"sshPort": 8080}}, "collide"),
        ({"http": {"port": 80}}, "port"),
        ({"http": {"credentialsKey": ""}}, "credentialsKey"),
        ({"httpRoute": {"enabled": True}}, "httpRoute requires http.enabled"),
        ({"http": {"enabled": True, "credentialsSecret": "creds"}, "httpRoute": {"enabled": True}}, "httpRoute.parentRefs is required"),
        ({"http": {"enabled": True, "credentialsSecret": "creds"}, "httpRoute": {"enabled": True, "parentRefs": [{"name": "gateway", "sectionName": "https"}]}}, "httpRoute.hostnames is required"),
        ({"httpRoute": {"parentRefs": [{"name": "gateway"}]}}, "sectionName"),
        ({"httpRoute": {"parentRefs": [{"name": "gateway", "sectionName": "https", "group": "networking.istio.io"}]}}, "group"),
        ({"httpRoute": {"timeouts": {"request": "infinite"}}}, "request"),
        ({"httpRoute": {"retries": 3}}, "retries"),
        ({"cnpg": {"spec": {"bootstrap": {"initdb": {"database": "other", "owner": "git_rs"}}}}}, "must match"),
        ({"cnpg": {"backup": {"enabled": True}}}, "requires a Barman ObjectStore"),
        ({"cnpg": {"enabled": False, "backup": backup}}, "cnpg.backup requires"),
        ({"cnpg": {"backup": backup, "spec": {"plugins": [{"name": "other", "isWALArchiver": True}]}}}, "another WAL archiver"),
    ]
    for values, message in invalid:
        render(args.helm, values, error=message)
    print(f"Helm contracts passed: configured defaults, external, OBC, HTTP, Gateway, overrides, Barman, isolated maintenance; {len(invalid)} invalid profiles rejected")
    if args.operator_schemas:
        validate_operator_schemas([defaults, routed, backed_up])


if __name__ == "__main__":
    main()
