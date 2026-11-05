# git-rs application chart

An independent Helm v3 chart with **no chart dependencies**. Kubernetes standard resources run
the stateless backend and an OpenSSH sidecar; optional operator resources are rendered directly.
Chart/app version and the Cargo package version must match.

## Prerequisites and profiles

| Profile | Values | Required infrastructure |
| --- | --- | --- |
| Defaults | `values.yaml` | CNPG operator/CRDs, existing Rook TLS object store and `ceph-bucket` StorageClass, SSH Secrets |
| Dedicated RGW/bucket | `values-example.yaml` | CNPG + Rook operators/CRDs, existing CephCluster, storage class, TLS/SSH Secrets |
| Existing services | `values-external.example.yaml` | TLS PostgreSQL primary, HTTPS S3 bucket, PostgreSQL/S3/SSH Secrets |

Standard application resources require Kubernetes 1.30+; operators have their own Kubernetes
compatibility requirements. Contract checks use the public schemas from CNPG v1.30.1, Rook
v1.21.1 and optional Barman Cloud plugin v0.15.1. The real application smoke test uses k3s
v1.37.2+k3s1 with external test PostgreSQL/S3, not those operators.

`cnpg.enabled` defaults to true. The default Cluster has three instances and synchronous `any 1`
with `dataDurability: required`. `cnpg.spec` is the upstream `Cluster.spec`; absent `bootstrap`,
the chart inserts `initdb` with `postgresql.database`/`username`. Explicit `bootstrap` is passed
through for recovery. Supply matching credentials and CA Secret overrides when restoring.

`rookCeph.bucket.enabled` defaults to true and consumes generated `BUCKET_NAME` from the claim's
ConfigMap and AWS credentials from its Secret. Other Rook resources are opt-in:

- `rookCeph.objectStore.enabled`: create a TLS RGW store on an existing CephCluster.
- `rookCeph.storageClass.enabled`: create a Retain bucket StorageClass, with the explicit operator
  provisioner and object-store namespace.
- `rookCeph.cluster.enabled`: render a deliberately configured `CephCluster.spec`. An empty spec
  is rejected; the chart never chooses host disks automatically. Prefer an existing platform cluster.
- `cnpg.backup.enabled`: create a Barman ObjectStore/ScheduledBackup and attach its WAL archiver;
  install the plugin separately and provide a nonempty upstream ObjectStore spec. Use a dedicated
  metadata-backup bucket, separate from live Git packs.

Cluster-scoped StorageClass and resources in the Rook namespace require corresponding installer
permissions. Their namespaces must already exist. CNPG Cluster, Ceph resources and bucket claims
are kept on uninstall; RGW pools are preserved and the generated bucket StorageClass uses Retain.

## SSH identity Secrets

Create host keys once per service identity, keep the private key outside version control, and
distribute the public host key/fingerprint through a trusted channel. For a generated local setup:

```sh
umask 077
mkdir -p .local/keys
ssh-keygen -t ed25519 -N '' -C git-rs-host -f .local/keys/ssh_host_ed25519_key
kubectl create namespace git-backend
kubectl -n git-backend create secret generic git-rs-ssh-host-keys \
  --from-file=ssh_host_ed25519_key=.local/keys/ssh_host_ed25519_key
```

The `git-rs-ssh-authorized-keys` Secret contains a **`principals`** file. Each noncomment line is
`PRINCIPAL KEY_TYPE BASE64_PUBLIC_KEY [COMMENT]`, for example using an existing public key:

```sh
{ printf 'alice '; cat "$HOME/.ssh/id_ed25519.pub"; } > .local/keys/principals
kubectl -n git-backend create secret generic git-rs-ssh-authorized-keys \
  --from-file=principals=.local/keys/principals
```

Use only public keys here. Principals contain ASCII letters, digits, `-_.@`, at most 128 bytes.
One key mapping to different principals is rejected. The helper produces `restrict` plus a fixed
forced command; clients cannot choose their principal. Interactive shells, forwarding, passwords
and PTYs are disabled. All clients use the OpenSSH login `git-rs`.

The backend runs as UID/GID 10001 with no capabilities and a read-only root filesystem. The SSH
listener uses root only for authentication/privilege separation, with `SETUID`, `SETGID` and
`SYS_CHROOT`. An unprivileged init container prepares private scratch/socket directories; a
capability-free root init container copies the host key to a root-private RAM volume so `fsGroup`
does not make its live key group-readable. Account for this in namespace admission policy.

## TLS PostgreSQL/S3 Secrets

CNPG normally creates `<cluster>-app` (password) and `<cluster>-ca` (`ca.crt`) Secrets. External
PostgreSQL requires explicit host/password/CA Secret names. The certificate SAN must match
`postgresql.host`; the backend always uses `verify-full` and targets a read-write primary.

For external services, create Secrets from files, then replace example endpoints in a local
values file with your own TLS service names:

```sh
kubectl -n git-backend create secret generic git-rs-postgresql \
  --from-file=password=/secure/postgresql-password
kubectl -n git-backend create secret generic git-rs-postgresql-ca \
  --from-file=ca.crt=/secure/postgresql-ca.crt
kubectl -n git-backend create secret generic git-rs-s3 \
  --from-file=AWS_ACCESS_KEY_ID=/secure/s3-access-key \
  --from-file=AWS_SECRET_ACCESS_KEY=/secure/s3-secret-key
kubectl -n git-backend create secret generic git-rs-s3-ca \
  --from-file=ca.crt=/secure/s3-ca.crt
cp charts/git-rs/values-external.example.yaml .local/values.yaml
```

The dedicated RGW example additionally needs the `git-rs-rgw-tls` certificate/key Secret in
`rook-ceph` and its CA Secret in the application namespace. Its certificate must cover
`rook-ceph-rgw-git-rs.rook-ceph.svc`, matching the derived S3 endpoint. A publicly trusted S3
endpoint can omit `s3.caSecret`. Plain HTTP and certificate-verification bypasses are rejected.

Live pack buckets need read/write/list permissions; explicit maintenance additionally needs
bucket-versioning inspection and delete permissions. Physical cleanup supports only a bucket
that has never enabled versioning. Prevent independent lifecycle deletion of catalogued objects.

## Install and initialize

Edit `.local/values.yaml` for the selected profile and precreate the required Secrets/operators:

```sh
helm upgrade --install git-rs oci://ghcr.io/borislitv/charts/git-rs \
  --version 0.1.0-rc.1 --namespace git-backend -f .local/values.yaml \
  --wait --timeout 10m
kubectl -n git-backend exec deployment/git-rs-git-rs -c backend -- \
  git-rs create team/project alice
```

A post-install/post-upgrade Job serializes schema initialization and verifies the schema version.
For a preinitialized database, `migration.enabled=false` disables it. Serving replicas do not run
migrations. Do not route client traffic until the Helm operation and migration Job have succeeded.

Use a TCP load balancer for SSH or a controlled local port-forward for evaluation. Port 22 maps
to sidecar port 2222. Verify the server host key before using the shared account:

```sh
kubectl -n git-backend port-forward service/git-rs-git-rs 2222:22
# In another terminal, after provisioning trusted known_hosts:
GIT_SSH_COMMAND='ssh -p 2222 -o StrictHostKeyChecking=yes' \
  git clone git-rs@127.0.0.1:team/project.git
```

Set `image.digest` to deploy an exact published image. `image.tag` otherwise defaults to appVersion.
Changing host keys requires a controlled pod rollout because the init container makes a private
copy. Identity-map Secrets are read by the authorization helper on authentication; Secret projection
updates follow Kubernetes propagation delays. Rotate database/S3 credentials with a controlled
rollout because they are process environment values.

See [operations and recovery](https://github.com/borislitv/git-rs/blob/v0.1.0-rc.1/OPERATIONS.md)
for backup/truncation semantics, resource limits and unverified production properties.
