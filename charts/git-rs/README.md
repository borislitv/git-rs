# git-rs application chart

An independent Helm v3/v4 chart with **no chart dependencies**. Kubernetes standard resources run
the stateless backend and an OpenSSH sidecar; optional operator resources are rendered directly.
Chart/app version and the Cargo package version must match.

## Prerequisites and profiles

| Profile | Values | Required infrastructure |
| --- | --- | --- |
| Application bucket and HTTPS route | `values-example.yaml` | CNPG and bucket provisioner/CRDs, existing retained bucket class and HTTPS object endpoint, SSH/HTTP/CA Secrets, existing Gateway API Gateway |
| Existing services | `values-external.example.yaml` | TLS PostgreSQL primary, HTTPS S3 bucket, PostgreSQL/S3/SSH Secrets |

Standard application resources require Kubernetes 1.30+; operators have their own Kubernetes
compatibility requirements. Contract checks use the public schemas from CNPG v1.30.1, Rook
v1.21.1 (OBC), Gateway API v1.7.0 and optional Barman Cloud plugin v0.15.1. The real application smoke test uses k3s
v1.37.2+k3s1 with external test PostgreSQL/S3, not those operators.

`cnpg.enabled` defaults to true. The default Cluster has three instances and synchronous `any 1`
with `dataDurability: required`. `cnpg.spec` is the upstream `Cluster.spec`; absent `bootstrap`,
the chart inserts `initdb` with `postgresql.database`/`username`. Explicit `bootstrap` is passed
through for recovery. Supply matching credentials and CA Secret overrides when restoring.

`objectBucket.enabled` defaults to true. The chart creates only a namespaced ObjectBucketClaim;
the storage platform, bucket class, provisioner and TLS object endpoint already exist. The
platform-managed class **must use `reclaimPolicy: Retain`**. The chart cannot verify that policy
offline: inspect the existing class before installing. It never installs storage infrastructure.

| Value | Default | Contract |
| --- | --- | --- |
| `objectBucket.enabled` | `true` | Create the application claim; disable for external S3 |
| `objectBucket.name` | `<fullname>-packs` | Claim, generated Secret and ConfigMap share this name |
| `objectBucket.storageClassName` | `ceph-bucket` | Existing retained bucket class |
| `objectBucket.bucketName` | empty | Otherwise use `generateBucketName: <claim>` |
| `objectBucket.additionalConfig` | `{}` | Provisioner-specific string map in claim spec |
| `objectBucket.annotations` | `Prune=false,Delete=false` sync option | GitOps deletion protection; Helm keep is always added |
| `s3.endpoint` | empty, **required** | Explicit HTTPS endpoint with a matching certificate; no endpoint is inferred |

The claim-generated ConfigMap supplies `BUCKET_NAME`; its same-named Secret supplies
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. `s3.credentialsSecret` can explicitly override
the credential source. Without a claim, supply `s3.bucket` and `s3.credentialsSecret`.

- `cnpg.backup.enabled`: create a Barman ObjectStore/ScheduledBackup and attach its WAL archiver;
  install the plugin separately and provide a nonempty upstream ObjectStore spec. Use a dedicated
  metadata-backup bucket, separate from live Git packs.

CNPG Cluster and bucket claims are kept on uninstall. Keep the platform retention policy and
GitOps protections in place; annotations do not replace storage retention or backups.

### Durability is not failover proof

The default `ANY 1`/`required` waits for one standby and stalls writes if none are available.
It does not prove no-loss failover: if primary and the acknowledged standby disappear together,
promoting a lagging remaining standby can lose acknowledged data. CNPG v1.30.1 offers
experimental `cnpg.spec.postgresql.synchronous.failoverQuorum: true`; this chart leaves it
unset, not silently enabled. See the pinned upstream
[failover documentation](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.1/docs/src/failover.md#failover-quorum-quorum-based-failover).
Evaluate the feature explicitly with the installed operator: it prevents promotion without a
sufficient read/write quorum and may sacrifice availability. Do not use manual promotion to
bypass that check without a separate recovery decision. Neither render tests nor replica count
establish HA; test partitions, primary loss, fencing and recovery before a production claim.

## Authenticated HTTP and Gateway API

HTTP is opt-in. `http.enabled: true` requires an existing `http.credentialsSecret`;
`http.credentialsKey` defaults to `credentials`. The Secret file contains one `PRINCIPAL TOKEN`
mapping per line. Tokens are 32–128 base64url characters; use independent high-entropy tokens
and keep the file out of values and version control. HTTP Basic uses the principal as username
and token as password. Authorization is checked on every request, not taken from identity headers.
The chart mounts the Secret read-only at `/etc/git-rs/http/credentials` (directory projection,
not subPath, so rotation propagates). `http.port` defaults to 8080 and controls both Service and
backend listener; 8081 is reserved for health probes. HTTP does not disable SSH.

`httpRoute.enabled` defaults to false. Enabling it requires HTTP, explicit nonempty `parentRefs`
and `hostnames`. Each parent must name the **HTTPS listener's `sectionName`** on an existing
`gateway.networking.k8s.io` Gateway managed by Istio (not a legacy Istio Gateway).
`parentRefs[].namespace` is optional; the Gateway must allow this route's namespace. The chart
creates only a v1 HTTPRoute, never a Gateway or TLS Secret. Optional string `annotations` and
Gateway API `timeouts.request`/`timeouts.backendRequest` pass through; configure timeouts for
large pushes. The rule explicitly sets `retry.attempts: 0`: omitting retry configuration inherits
automatic retries in Istio 1.31.3. Routing requires Gateway API **standard CRDs v1.7+** and a
controller supporting retry disabling (verified with Istio 1.31.3). **Do not add automatic POST
retries** at the Gateway, mesh or client, because a lost response can follow a committed push.

Terminate trusted HTTPS at the Gateway and provide a trusted, encrypted Istio-to-pod path
(for example sidecar injection with strict mTLS and appropriate authorization policy). The chart
does not install mesh security policy. Do not expose port 8080 through an external plaintext
listener or bypass TLS verification; Basic credentials are not encrypted by HTTP itself.
An isolated local fixture may use plaintext solely to test the authenticated backend contract,
not as a deployable external profile. Gateway attachment, `Accepted`/`ResolvedRefs` conditions,
TLS trust and real native Git through Istio require runtime verification; Helm rendering proves
none of them. The example uses reserved domains and must be configured before installation.

### Optional real Istio verification

On a Linux Docker host with an already built `git-rs:dev` production image, run
`python3 tests/istio.py --helm /path/to/helm3` (or `make istio HELM=/path/to/helm3`).
It requires privileged Docker, reachable Docker bridge addresses, Git, curl, OpenSSL, Python
and PyYAML, plus public artifact/image access. It exclusively creates `git-rs-istio-test`, refuses
an existing container with that name, and removes its own container/volumes on exit. It never
attaches to the separate Kubernetes/benchmark fixture. `--keep-on-failure` is for explicit debugging.

The test pins/checksums official Istio 1.31.3 distribution charts and Gateway API v1.7.0 standard
CRDs on k3s v1.37.2+k3s1. It supplies a platform-owned HTTPS Gateway, CA-signed certificate,
STRICT mTLS and gateway-principal authorization policy outside the application chart. It checks
current-generation Gateway `Accepted`/`Programmed` and HTTPRoute `Accepted`/`ResolvedRefs`,
then performs native Git push/clone and restart readback with strict CA/hostname verification and
Secret-backed HTTP Basic. Negative controls reject an untrusted CA, absent/wrong credentials,
spoofed identity headers and non-mesh plaintext traffic. Destination telemetry must show the
gateway principal and `mutual_tls`; actual Envoy route configuration must have no retry policy.
These checks passed with Helm 3.22.1. This is an isolated routing/security integration check,
not a production deployment, load benchmark, CNPG failover or storage-retention proof.

### Breaking values migration

The previous `rookCeph` values API is removed and rejected. Migrate application claims to
`objectBucket` and set an explicit `s3.endpoint`. Storage platform resources must be managed
separately. Preserve existing claim identity with `objectBucket.name` and its bucket/class
settings; do not delete protected resources as part of upgrading. Defaults intentionally do not
invent a production endpoint, so even strict lint needs configured values, for example:

```sh
helm lint charts/git-rs --strict -f charts/git-rs/values-example.yaml
```

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

The bucket example needs an existing HTTPS object endpoint and its CA Secret in the application
namespace. Its certificate must cover the configured `s3.endpoint`. A publicly trusted S3 endpoint
can omit `s3.caSecret`. Plain HTTP and certificate-verification bypasses are rejected.

Live pack buckets need read/write/list permissions; explicit maintenance additionally needs
bucket-versioning inspection and delete permissions. Physical cleanup supports only a bucket
that has never enabled versioning. Prevent independent lifecycle deletion of catalogued objects.

## Install and initialize

Edit `.local/values.yaml` for the selected profile and precreate the required Secrets/operators:

```sh
helm upgrade --install git-rs oci://ghcr.io/borislitv/charts/git-rs \
  --version 0.1.0-rc.2 --namespace git-backend -f .local/values.yaml \
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

See [operations and recovery](https://github.com/borislitv/git-rs/blob/v0.1.0-rc.2/OPERATIONS.md)
for backup/truncation semantics, resource limits and unverified production properties.
