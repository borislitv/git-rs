# Prebuilt runtime verification

Verification consumes independently prepared inputs. The four image roles are:

- `test-fixture-build`: matched toolchain/build input, outside measurement.
- `test`: external native Git 2.56.0 client/oracle and Python controller.
- `release`: production Git-free runtime, role `runtime`.
- `test-server`: the same Git-free runtime plus explicitly selected observer and
  receive fixtures, role `runtime-fixture`.

Host preparation and the external Python controller require Python 3.13 or newer.
Archive readers use the standard library's non-caching stream mode with bounded
PAX/GNU metadata; the pinned Ubuntu controller supplies that interpreter.

`tests/blame_images.py` transfers them as one layer-deduplicated archive with the
full source revision, Dockerfile/archive hashes and immutable local image IDs.
CI selects the artifact by its exact workflow/run/attempt. Loading validates archive
descriptors/config identities before exporting the
IDs. A mutable tag is never the identity recorded by a measured phase. Runtime,
fixture and oracle roles must be distinct and revision-matched.

## Independent bounds and ownership

`tests/runtime_exec.py run` requires a real systemd invocation and verifies its
unit, dedicated slice, current process cgroup, `RuntimeMaxSec<=900`,
`TimeoutStopSec<=40`, `KillMode=control-group` and exact independent
`ExecStopPost=.../tests/runtime_exec.py cleanup --unit ... --slice ...`. It rejects an unbounded/remote
Docker context and requires the local systemd/cgroup-v2 Docker driver.

The aggregate slice must enforce at most 4 GiB memory, 2 CPUs, 256 tasks and zero
extra swap. CPU affinity selects at most two available CPUs. The controller,
backend, PostgreSQL/S3, owned k3s and actual CRI tasks belong to that aggregate;
Docker daemon overhead is explicitly outside the measurement scope. Individual
container limits alone do not prove aggregate placement. The fixture checks real
CRI process/sandbox cgroups below the explicit kubelet cgroupfs root.

Each phase uses a new private owned `0700` directory under
`.local/runtime-gates/<INVOCATION_ID>`, one exact Docker owner label and prebuilt
inputs. The external BPF observer and its exact-owned cgroup cleanup use root;
the actual work child drops its GID/UID to 10001 and replaces its supplementary
groups with only the image and Docker groups. Its private bind-mounted paths keep
that same owner. The external controller can access Docker; a backend can
mount only its private server directory and, for an elected worker, a separate
read-only scoped service-account directory. No client repository, oracle Git
installation, Docker socket or administrative kubeconfig enters the backend.

The Docker proxy injects slice/owner/pull-never arguments into both `run` and
`create` and rejects cgroup/PID/privileged escapes. The outer supervisor alone
creates its fresh privileged k3s with an explicit cgroup root. It never reuses,
resets or deletes a preexisting cluster.

## Successful-exec lifecycle evidence

The external `bpftrace` observer attaches before work. It seeds the already-created
service/slice and records new descendant cgroups in the kernel before successful
`sched_process_exec` events, including short-lived helpers. A successful-exec probe
must be observed before the work child is released. Tracer errors, lost-event
diagnostics, missing cgroup joins, truncated/oversized records and premature event
streams fail the gate. Executable paths and cgroup IDs are recorded; argv,
environment variables and credential values are not.

Exact-owner Docker create events bind container IDs to the revision-matched image
roles. A resource-version Kubernetes pod watch starts before Helm hooks and records
actual image/container identities, including terminated init/migration containers.
Finite watch responses resume from the last version; an expired version or failed
stream is not repaired by an unobserved relist. Native Git/helpers may execute only
in the bound external client/oracle containers. Bound backend executions use an
explicit program inventory, and client-side `git-rs ssh` does not count as backend
coverage. Names in a checkout directory are not executable identities.

Raw observer inputs are root-owned/private, separate from the work child's files.
The observer remains active through work, container shutdown, actual CRI drain and
credential cleanup. Only then may the outer observer mark the phase complete. This
combines the filesystem inventory with process evidence; neither check alone is
used as a substitute for the other.

## Phase inventory

| Phase | Required observable result |
| --- | --- |
| `runtime` | Stopped production and fixture image exports: bounded path/link/package/executable inventories, no native Git/helpers/config/repositories; exact inventory-container cleanup |
| `metadata` | Exact compiled ignored `metadata`, `maintenance` and `maintenance_leader` inventories, run serially with a fresh PostgreSQL database for each exact test, executable hashes and complete test counts |
| `integration` | Production runtime plus external native SSH/HTTP clients: pushes, clones, restarts, complete blame, ordered history, backup/truncation and long publication within unchanged budgets |
| `receive` | Explicit Git-free receive fixture plus ordinary production serving: strict physical/graph/native-client cases and owned socket/error paths |
| `upload` | Native v0/v1/v2 clone, deepen/unshallow, merge/tag closure, denied physically present hidden wants and restricted SSH clone/fetch/strict fsck |
| `history` | Full native ordered history and bounded prefix, all-ref bundle clone/closure, mode/no-overwrite, owner/other-ref policy and physical truncation/backup parity |
| `failures` | Real S3/PostgreSQL faults, corrupt immutable ranges, cache recovery and queued cleanup by a real same-binary elected worker using scoped credentials |
| `maintenance` | Unknown leadership-acquisition COMMIT, paused old worker and real takeover, rejected stale-attempt publication, native all-catalog compaction equality and owner enqueue while the elected worker remains holder |
| `cache-matrix` | Fresh rotated source/cache databases, native-complete blame bytes, separate actual mapping/body reuse, two-process claim contention, restart/outage and mixed receive/fetch/elected compaction; source snapshot tails, physical placement, CPU/RSS, wire/WAL and actual streaming replay |
| `leader` | Prebuilt candidate contracts against Kubernetes 1.37: cold named Lease, CAS/SSA, expiry/clock/overflow and eligible standbys |
| `kubernetes` | Imported content-bound runtime/oracle digests, two serving replicas, external SSH/HTTP native clients, cold restart/history equality, schema-first maintenance enablement, standby/leader probes and successor job completion |
| `istio` | Checksum-pinned official charts/CRDs, imported digest-bound mesh images, verified HTTPS native push/clone/restart, current Gateway/HTTPRoute conditions, disabled retries, STRICT mesh authentication and negative trust/identity/plaintext controls |

The metadata phase never runs Cargo. `make runtime-contracts-build` prepares the
compiled inputs outside measurement and seals their revision, suite names and ELF
hashes. `--target-dir` points at that handoff directory with `contracts.json` and
`debug/deps`; changed, linked or mismatched executables are rejected. CI selects
the compiled artifact by workflow/run/attempt and runs phases as serialized matrix
jobs, retaining the same 900-second per-unit and aggregate bounds. Production
images are not used as native client images. Candidate tests use the selected
prebuilt ELF and least-privilege credentials, not a hidden backend implementation.
`make leader-dependency-check` selects that supervised `leader` phase and its fresh
owned cluster; `make leader-dependency-build` prepares the same sealed handoff as
`make runtime-contracts-build`.

Kubernetes image import binds a local Docker config identity to a containerd
manifest digest and verifies labels/actual pod image IDs. Both runtime init and
serving/maintenance containers must retain the production digest. Native Git and
curl run only in the separate oracle pod. The smoke deployment uses synthetic
credentials and TLS PG/S3; direct disposable-bridge HTTP does not prove a production
Gateway/mTLS route. The separate `istio` phase supplies that synthetic routing scope
through the same aggregate supervisor and successful-exec observer.

`make istio-prepare` downloads the checksum-pinned public distribution and CRDs
before measurement. `tests/runtime_gate.py images --phase istio` lists the public
dependency images to pull beforehand. The measured controller receives those
prepared files and a hash-bound prebuilt Linux Helm executable as read-only mounts;
it extracts only bounded official charts and performs no artifact download or build.
Istio pilot/proxy images are imported from local Docker, bound to their actual CRI
config/manifest identities and selected by digest with pull policy `Never`. Native
clients execute in the revision-bound external oracle, not on the host supervisor.
The application pod watch precedes its migration hooks and init containers. Cluster
shutdown, actual pod-task drain and credential cleanup remain outer-owned.

## Crash cleanup and diagnostics

The supervisor writes/fsyncs a private non-secret cluster ownership journal before
starting its API. Namespace fixtures likewise persist owner/name before ambiguous
creation, then record the returned UID. Normal deletion checks owner label/UID
and uses a UID precondition. The maintenance account has only GET/PATCH on one
precreated Lease, plus a finite token and verified CA.

Independent cleanup has one 35-second absolute budget within the 40-second
systemd stop bound. It reaps exact-owned controllers/backends/dependencies before
the cluster and confirms the immutable cluster ID is absent. The root supervisor
then kills remaining tasks only in its previously recorded pod subtree and checks
that every sibling cgroup is unpopulated. A root-owned drain attestation precedes
the final unprivileged cleanup of scoped credentials/journals. That local-only alternative requires the journal's
exact destroyed cluster ID; API unavailability, a name or a replacement cluster
is not evidence of destruction. A retained journal supports retry after an
interrupted cleanup. Foreign or relabelled resources are not silently removed.

Stopped-image inventory has a separate exact-ownership journal, replayed by the
supervisor. Final label discovery also handles ambiguous daemon-side creation.
Failures retain journals/reports; success is published only after work and
cleanup both finish. A timeout or killed controller is not cleanup acknowledgement.

Root-only collection exports explicit synthetic reports and bounded log tails,
at most 16 MiB per file. It never exports kubeconfig, service-account tokens,
environment files, TLS/SSH private keys or arbitrary fixture directories. Keep
those private diagnostics out of source control. Preserve failure outcomes rather
than hiding them with a later successful run.

## Qualification scope

`make cache-matrix` uses the versioned synthetic `tests/fixtures/cache-matrix.json`
through the same observer and immutable image handoff. Its twelve cells rotate
disabled, all-PostgreSQL and PostgreSQL/S3 placement across derivation and complete
body profiles. A ready body cannot satisfy the mapping-work profile. The fixture
records owned/pending build claims and actual duplicate computations; it does not
assume that a shared cache is a distributed CPU scheduler. Native full porcelain
is prepared outside the measured cells, and each response must match it exactly.
Mixed source work uses separate repositories and an explicitly held maintenance
GET. Reported primary WAL includes probes/source work; measured copies do not
cover allocator/kernel/TLS copies. Cold means fresh application state, without
flushing OS or PostgreSQL buffers. A physical replay fence is not an HA result.
The smoke does not replace a separately configured full-corpus/GitLab campaign.

These files define checks. They do not attest that a changed checkout has passed.
Bind every result to its source/dependency revision, lockfile, executable/image
identities, discovered inventory, resource envelope, actual outcomes and cleanup.
Filesystem absence and zero subprocess metrics are not alone process-lifecycle
proof. Native parity on synthetic repositories is not a general performance,
production HA, storage-failure or coordinated-recovery guarantee.
