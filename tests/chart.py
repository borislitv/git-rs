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


def render(helm, values=None, *, file=None, error=None):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "values.json"
        path.write_text(json.dumps(values or {}))
        command = [helm, "template", "contract", str(CHART), "--namespace", "git-test"]
        if file:
            command += ["--values", str(file)]
        result = subprocess.run(command + ["--values", str(path)], capture_output=True,
                                text=True, timeout=30)
    if error:
        assert result.returncode and error in result.stderr, (values, error, result.stderr)
        return []
    assert result.returncode == 0, result.stderr
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def resource(documents, kind):
    found = [doc for doc in documents if doc["kind"] == kind]
    assert len(found) == 1, (kind, len(found))
    return found[0]


def validate_operator_schemas(profiles):
    # Explicit opt-in network contract test against pinned public operator CRDs.
    sources = [
        "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/config/crd/bases/postgresql.cnpg.io_clusters.yaml",
        "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/config/crd/bases/postgresql.cnpg.io_scheduledbackups.yaml",
        "https://raw.githubusercontent.com/cloudnative-pg/plugin-barman-cloud/v0.15.1/config/crd/bases/barmancloud.cnpg.io_objectstores.yaml",
        "https://raw.githubusercontent.com/rook/rook/v1.21.1/deploy/examples/crds.yaml",
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
    assert {"Cluster", "ScheduledBackup", "ObjectStore", "CephCluster", "CephObjectStore", "ObjectBucketClaim"} <= checked, checked
    print("Operator CRD schemas passed: " + ", ".join(sorted(checked)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--operator-schemas", action="store_true",
                        help="download pinned public CNPG/Barman/Rook CRDs and validate rendered resources")
    args = parser.parse_args()
    subprocess.run([args.helm, "lint", str(CHART), "--strict"], check=True)
    metadata = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert metadata.get("dependencies", []) == []
    assert not (CHART / "charts").exists()
    assert metadata["version"] == metadata["appVersion"]
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    assert metadata["appVersion"] == manifest["package"]["version"]
    assert lock["package"][0]["version"] == metadata["appVersion"]
    defaults = render(args.helm)
    deployment = resource(defaults, "Deployment")
    pod = deployment["spec"]["template"]["spec"]
    backend = next(c for c in pod["containers"] if c["name"] == "backend")
    env = {item["name"]: item for item in backend["env"]}
    assert env["GIT_RS_CACHE_BYTES"]["value"] == "67108864", env["GIT_RS_CACHE_BYTES"]
    assert env["PGSSLMODE"]["value"] == "verify-full"
    assert env["PGTARGETSESSIONATTRS"]["value"] == "read-write"
    assert env["PGHOST"]["value"] == "contract-git-rs-pg-rw"
    assert "valueFrom" in env["GIT_RS_S3_BUCKET"]
    assert "valueFrom" in env["GIT_RS_S3_SECRET_KEY"]
    assert not pod["automountServiceAccountToken"]
    assert backend["securityContext"]["runAsNonRoot"]
    assert backend["securityContext"]["readOnlyRootFilesystem"]
    assert not any("persistentVolumeClaim" in volume for volume in pod["volumes"])
    assert resource(defaults, "Service")["spec"]["selector"] == deployment["spec"]["selector"]["matchLabels"]
    pg = resource(defaults, "Cluster")
    assert pg["spec"]["bootstrap"]["initdb"] == {"database": "git_rs", "owner": "git_rs"}
    assert pg["spec"]["postgresql"]["synchronous"] == {"method": "any", "number": 1, "dataDurability": "required"}
    assert pg["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert resource(defaults, "ObjectBucketClaim")["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert "AuthorizedKeysFile none" in resource(defaults, "ConfigMap")["data"]["sshd_config"]
    assert "AuthorizedKeysCommandUser git-rs" in resource(defaults, "ConfigMap")["data"]["sshd_config"]
    assert resource(defaults, "Job")["metadata"]["annotations"]["helm.sh/hook"] == "post-install,post-upgrade"
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

    rook = render(args.helm, file=CHART / "values-example.yaml")
    store = resource(rook, "CephObjectStore")
    storage = resource(rook, "StorageClass")
    claim = resource(rook, "ObjectBucketClaim")
    assert store["metadata"]["namespace"] == "rook-ceph"
    assert store["spec"]["preservePoolsOnDelete"]
    assert store["spec"]["gateway"]["securePort"] == 443
    assert storage["reclaimPolicy"] == "Retain"
    assert storage["parameters"]["objectStoreName"] == store["metadata"]["name"]
    assert claim["spec"]["storageClassName"] == storage["metadata"]["name"]

    customized = render(args.helm, {
        "fullnameOverride": "isolated", "cacheBytes": 0,
        "image": {"repository": "example.invalid/git-rs", "digest": "sha256:" + "a" * 64},
        "cnpg": {"name": "metadata-db"},
        "rookCeph": {"bucket": {"name": "packs-claim", "bucketName": "unique-packs"}},
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
    env = {item["name"]: item for item in pod["containers"][0]["env"]}
    assert env["GIT_RS_CACHE_BYTES"]["value"] == "0"
    assert env["PGHOST"]["value"] == "metadata-db-rw"
    assert env["GIT_RS_S3_BUCKET"]["valueFrom"]["configMapKeyRef"]["name"] == "packs-claim"

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

    ceph_spec = {"cephVersion": {"image": "quay.io/ceph/ceph:v20.2.0"},
                 "dataDirHostPath": "/var/lib/rook", "mon": {"count": 3},
                 "storage": {"useAllNodes": False, "useAllDevices": False,
                             "nodes": [{"name": "storage-1", "devices": [{"name": "vdb"}]}]}}
    ceph = render(args.helm, {"rookCeph": {"cluster": {"enabled": True, "spec": ceph_spec}}})
    assert resource(ceph, "CephCluster")["spec"] == ceph_spec

    invalid = [
        ({"cacheByte": 1}, "cacheByte"),
        ({"replicaCount": 0}, "replicaCount"),
        ({"operationSeconds": 121}, "operationSeconds"),
        ({"image": {"digest": "sha256:bad"}}, "digest"),
        ({"s3": {"endpoint": "http://s3.example.invalid"}}, "endpoint"),
        ({"cnpg": {"enabled": False}}, "postgresql.host is required"),
        ({"rookCeph": {"bucket": {"enabled": False}}}, "s3.bucket is required"),
        ({"rookCeph": {"cluster": {"enabled": True}}}, "rookCeph.cluster.spec must explicitly"),
        ({"rookCeph": {"objectStore": {"enabled": True, "spec": {"gateway": {"securePort": 0}}}}}, "TLS securePort"),
        ({"cnpg": {"spec": {"bootstrap": {"initdb": {"database": "other", "owner": "git_rs"}}}}}, "must match"),
        ({"cnpg": {"backup": {"enabled": True}}}, "requires a Barman ObjectStore"),
        ({"cnpg": {"enabled": False, "backup": backup}}, "cnpg.backup requires"),
        ({"cnpg": {"backup": backup, "spec": {"plugins": [{"name": "other", "isWALArchiver": True}]}}}, "another WAL archiver"),
    ]
    for values, message in invalid:
        render(args.helm, values, error=message)
    print(f"Helm contracts passed: defaults, external, Rook, overrides, Barman, Ceph; {len(invalid)} invalid profiles rejected")
    if args.operator_schemas:
        validate_operator_schemas([defaults, rook, backed_up, ceph])


if __name__ == "__main__":
    main()
