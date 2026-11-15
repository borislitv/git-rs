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

The preparatory `build-inputs` target audits and fetches the locked sources before
the offline release/fixture builds. It is not a fifth runtime role and is not part
of the exported four-image inventory. Each preparation/build invocation retains
its independent deadline and worker limits; splitting preparation does not attest
that a cold hosted job fits its overall timeout. Cleanup tolerates only exact
owned-worker removal races and confirms an empty slice within its original budget.

## Independent bounds and ownership

`tests/runtime_exec.py run` requires a real systemd invocation and verifies its
unit, dedicated slice, current process cgroup, `RuntimeMaxSec<=900`,
`TimeoutStopSec<=40`, `KillMode=control-group` and exact independent
`ExecStopPost=.../tests/runtime_exec.py cleanup --unit ... --slice ...`. It rejects an unbounded/remote
Docker context and requires the local systemd/cgroup-v2 Docker driver.

The default qualification envelope is `legacy-2cpu-4g-v1`: at most 4 GiB memory,
2 CPU equivalents, 256 tasks and zero extra swap. CPU affinity selects at most
two available CPUs. The explicit `candidate-4cpu-8g-v1` envelope permits at most
8 GiB memory, 4 CPU equivalents, 256 tasks and zero extra swap, with at most four
available affinity CPUs. Both profiles retain the finite 900-second outer and
40-second stop ceilings. `tests/fixtures/runtime-profiles.json` versions their
resource contracts; a candidate name is not a qualification verdict.

Select the envelope with `tests/runtime_exec.py run --profile NAME` or
`RUNTIME_PROFILE=NAME make <runtime-phase>`. Omission retains the legacy profile.
Unknown names fail argument parsing. For the candidate, the independently installed
systemd `ExecStopPost` must also select
`tests/runtime_exec.py cleanup --unit UNIT --slice SLICE --profile candidate-4cpu-8g-v1`.
Legacy cleanup may omit `--profile`; explicit legacy selection is also accepted.
The root verifier reads the lossless D-Bus `ExecStopPost` property using
`busctl --system --json=short get-property` (`a(sasbttttuii)`). The human
`systemctl show` rendering joins arguments with spaces and is not parsed as a
shell command or used as an argv fallback. A missing/unsupported typed response
fails attestation. Exactly one non-ignore-errors command is permitted: the same
resolved Python interpreter, `-B`, the matching runtime observer script, and the
`cleanup` operation. Only complete `--unit`, `--slice` and optional `--profile`
options are accepted, in separate-value or `--name=value` form, once each.
Owner and profile values must match completely. Extra commands, wrappers,
abbreviations, duplicate options and profile suffixes fail attestation. All three
owned Python entrypoints disable argparse option abbreviation; runtime owner and
profile options also reject repetition at the CLI boundary.
The launcher owns the actual slice limits and CPU affinity IDs. No portable profile
contains host CPU numbers, and the selector does not change systemd/container caps.

The root observer binds the selected profile's canonical SHA-256 identity and
normalized live cgroup limits into its private scope record and read-only unit
attestation. The image-user validator recomputes the profile identity and live
limits before accepting that proof. Reports include `envelope.profile`,
`profile_sha256`, `actual_limits` (memory, tasks, CPU quota/period, CPU IDs and swap),
and `exact_profile`. Smaller envelopes remain bounded, but `exact_profile` is false
unless memory, tasks, CPU quota and affinity count all equal the selected profile.
The profile name alone must never be reported as an exact measured allocation.
Changed limits, CPU IDs, profile identities or invocation owners invalidate the
bound proof. Root cleanup rechecks the saved scope before removing containers or
draining tasks; a profile mismatch does not authorize removal.

The existing deadline arithmetic is unchanged: the observer starts an outer
deadline of start + 900 seconds; the work helper gives the phase subprocess
outer - 40 seconds, and the phase's work deadline is outer - 40 - 35 seconds.
Thus the work cutoff is start + 825 seconds before startup/attestation overhead,
not an 840-second work guarantee. The 40-second phase reserve and 35-second
inner cleanup reserve are separate deductions. A tighter independent systemd
runtime can stop the invocation earlier; selecting a profile does not extend it.

The controller,
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

Repeated backend-state polling uses read-only container inspection over the same
local Unix Docker socket, with a three-second I/O timeout and one-MiB response
bound. Every observation revalidates the immutable container ID and fixture owner;
it never caches running state. Signals still require this current owner check.
This avoids a new external Docker CLI process for each readiness/resource sample.

## Successful-exec lifecycle evidence

The external `bpftrace` observer attaches before work. It seeds the already-created
service/slice and records new descendant cgroups in the kernel before successful
`sched_process_exec` events, including short-lived helpers. A successful-exec probe
must be observed before the work child is released. Tracer errors, lost-event
diagnostics, missing cgroup joins, truncated/oversized records and premature event
streams fail the gate. Requested executable paths, the kernel-installed executable
dentry basename and cgroup IDs are recorded; argv, environment variables and
credential values are not. The kernel basename identifies descriptor-based reexecs
without racing a later `/proc/<pid>/exe` lookup. Both requested and installed names
are checked for native Git; backend coverage and the program inventory use the
installed name. Missing or malformed installed identities fail the gate.
Descriptor-based `runc` bootstrap executions in bound backend cgroups are counted
separately; they cannot supply backend program coverage. An ordinary requested
`runc` path is not accepted as this observed container-runtime bootstrap.

Exact-owner Docker create events bind container IDs to the revision-matched image
roles. A resource-version Kubernetes pod watch starts before Helm hooks and records
actual image/container identities, including terminated init/migration containers.
Finite watch responses resume from the last version; an expired version or failed
stream is not repaired by an unobserved relist. Native Git/helpers may execute only
in the bound external client/oracle containers. Bound backend executions use an
explicit program inventory, and client-side `git-rs ssh` does not count as backend
coverage. Names in a checkout directory are not executable identities.

Raw observer inputs are root-owned/private, separate from the work child's files.
After verifying the actual unit, the root observer publishes a bounded root-owned
read-only attestation bound to its invocation, unit, slice, current cgroup and
selected resource envelope with its actual live limits.
The image UID validates that attestation and live aggregate limits without needing
a host passwd entry or system-bus access. Image-owned replacements and proofs from
a different invocation are rejected.
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

For Kubernetes and Istio, the supervisor copies the pinned node image's `kubectl`,
checks its ELF and content digest, and mounts it read-only into the existing
external controller. API commands and the resource-version watch use that client
with a private kubeconfig bound to the exact owned cluster and published TLS
endpoint. The controller and node keep their existing task limits; client threads
no longer consume the node's container-runtime allowance. The kubeconfig is removed
after its controller stops, including independent failure cleanup.

The Istio bootstrap executes the actual migration hook with one serving replica,
verifies schema v2 and waits for the hook pod to disappear, then enables both mesh
replicas without a redundant migration. Cold-restart checks explicitly stop both
old replicas before starting two fresh ones; they do not claim rolling-upgrade
availability. Authentication, image identity, mesh and lifecycle checks still
exercise both serving replicas within the unchanged aggregate bounds.

The existing non-mesh external controller also performs the plaintext rejection
probe through the fixture backend NodePort. It creates the fixture S3 bucket through
a separate NodePort with verified CA/hostname and signed requests. No additional
idle probe pod or container-runtime shim is needed. These NodePorts belong only
to the disposable fixture; the positive application path still uses HTTPS Gateway
and destination-verified mutual TLS.

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

HTTP, SSH and health accept loops use fallible request-thread creation. If the OS
rejects a new worker, its socket and admission owner are dropped, a closed-label
rejection is recorded, and the accept loop remains available after capacity
returns. Startup thread failures propagate as startup errors. The real-image
failure contract temporarily lowers one exact-owned backend's task cap, observes
connection rejection, restores its original cap, and verifies listener recovery,
released admission and native-client interoperability.

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
GET. Concurrent native receive and upload use distinct recorded production
containers, each retaining its one-worker/zero-waiter admission profile. They do
not compete for a single process's only CPU permit. The additional replica counts
against the same aggregate bounds in every placement and repetition. Reported
primary WAL includes probes/source work; measured copies do not
cover allocator/kernel/TLS copies. Cold means fresh application state, without
flushing OS or PostgreSQL buffers. A physical replay fence is not an HA result.
The smoke does not replace a separately configured full-corpus/GitLab campaign.

Cell preparation executes the genuine Rust migration/repository CLI in its owned
source-publisher container, sharing that immutable image, environment and resource
limits. The development listener is idle until source/cache migrations and all
repository creation finish; no native traffic or measured work precedes them.
This avoids a separate container for each CLI command. Listener startup is not a
readiness/schema-availability assertion or a change to Kubernetes migration hooks.

Each matrix response still records the daemon's kernel RSS high-water mark. The
test-only HTTP fixture reads bounded `/proc/self/status` and exports `VmHWM` in
its existing metrics response, avoiding two external execs for every sample.
Before the first measured cell, the controller verifies this observation between
two original external namespace readings of that exact container. Missing or
invalid observations fail; there is no zero-value fallback or RSS limit increase.

The physical-replica probe reuses one owned external `psql` session per cell for
its fixed read-only lag query, rather than starting a client for every sample.
Each observation requires its complete response marker, a fresh nonnegative
value, the same two-second SQL/four-second client bounds and at most one KiB of
output. The probe retains its existing sampling interval and resource inventory;
the client is registered for independent cleanup and joined before database
teardown. This does not use a serving SQL subprocess or authorize source work.

These files define checks. They do not attest that a changed checkout has passed.
Existing results retain their original resource envelope; adding a candidate
profile does not relabel historical runs. This selector changes qualification
plumbing only: application worker/job-memory/decoder limits, individual fixture
container caps and the persistent VM launcher remain independently configured.
In particular, selecting the larger envelope does not increase those inner caps
or establish that a larger application/VM profile is supported or qualified.
Bind every result to its source/dependency revision, lockfile, executable/image
identities, discovered inventory, resource envelope, actual outcomes and cleanup.
Filesystem absence and zero subprocess metrics are not alone process-lifecycle
proof. Native parity on synthetic repositories is not a general performance,
production HA, storage-failure or coordinated-recovery guarantee.
