# Persistent VM playground

Try ordinary Git push, clone, fetch, pull and the server's complete blame API on
synthetic data. PostgreSQL and S3 data live in two named Docker volumes; stopping
and starting the playground preserves the repository. The server uses bounded
Rust-owned buffers and immutable object-store ranges. Its read-only filesystem has
a private 1 MiB Unix-socket mount and a separate 64 MiB `/tmp`; no local bare
repository or object-pack scratch is required. No Kubernetes or Compose is required.

## Prepare the VM

Use a **Linux amd64** VM with Docker Engine, Python 3, Git and curl (with
`--aws-sigv4` support). A practical starting point is **2–4 vCPUs, 8 GiB RAM and
40 GiB free disk**; this is a recommendation for experimentation, not a certified
minimum. Your user must be able to run Docker.

The launcher uses the local Docker Engine socket at `/var/run/docker.sock`, even
when the caller inherited `DOCKER_HOST` or `DOCKER_CONTEXT`. The persistent
volumes, published loopback ports and client commands belong to this same VM.

Use the supplied prebuilt VM package. From its root, verify the package, load the
image and select its immutable identity:

```sh
sha256sum --check SHA256SUMS
docker load --input git-rs-runtime.tar
runtime_image=$(python3 -c 'import json; print(json.load(open("release.json"))["image"]["id"])')
python3 examples/playground/playground.py up --image "$runtime_image"
```

The launcher never compiles or pulls the application. It accepts a locally loaded
release runtime, checks its image role and saves the immutable image ID. Build and
client/oracle images are rejected. A distribution manifest identifies inputs;
qualification evidence accompanies the actual delivered release separately.

`init` is an alias of `up`. Both create/migrate the source database, create the S3
bucket if absent, and create `demo/repo` owned by `alice` if absent. Repeated calls
check actual SQL repository/owner state; creation failures are not ignored.
The dependency images are pinned to the same PostgreSQL 18.6 and Versity digests
as the integration fixture. Initial pulls require network access.

### Local-only development access

This intentionally uses **unauthenticated development HTTP**, with all requests
acting as `alice`. The application binds Git to `127.0.0.1:18080` and health/metrics
to `127.0.0.1:18081`. PostgreSQL (`15432`) and S3 (`17070`) are published only on
`127.0.0.1`. The Linux app container uses host networking so its development-mode
database and S3 endpoints are also loopback addresses.

Do not change these bindings to `0.0.0.0` or expose the ports publicly. Other users
and processes on this VM can access the development listener. Use a dedicated VM
and disposable data. Production authentication/TLS setup is documented in
[OPERATIONS.md](../../OPERATIONS.md).

To run Git and curl on your laptop instead, open a tunnel there and keep it open:

```sh
ssh -N -L 18080:127.0.0.1:18080 YOUR_VM
```

The following client commands work either in a separate VM terminal or on the
laptop with that tunnel. The laptop's port 18080 must be free.

## Push your first commits

Run in a fresh client directory, outside this source checkout:

```sh
mkdir git-rs-first-try
cd git-rs-first-try
git init -b main
git config user.name 'Playground Alice'
git config user.email 'alice@example.invalid'
printf 'hello from git-rs\nsecond line\n' > hello.txt
git add hello.txt
git -c commit.gpgsign=false commit -m 'Add greeting'
git remote add origin http://127.0.0.1:18080/demo/repo.git
git push -u origin main
```

Use a second client checkout to make another change:

```sh
cd ..
git clone http://127.0.0.1:18080/demo/repo.git git-rs-second-try
cd git-rs-second-try
git config user.name 'Playground Bob'
git config user.email 'bob@example.invalid'
printf 'third line from another checkout\n' >> hello.txt
git add hello.txt
git -c commit.gpgsign=false commit -m 'Extend greeting'
git push origin main
cd ../git-rs-first-try
git fetch origin
git log --oneline --all --graph
git pull --ff-only
git fsck --full
```

`Playground Bob` is commit metadata; HTTP access still uses the development
principal `alice`. These examples assume a fresh `demo/repo`. On subsequent visits,
clone the existing repository and continue committing instead of trying to replace
its history. `main` rejects deletion and non-fast-forward pushes, including owner
pushes.

## Ask the server for blame

Ordinary `git blame` computes blame locally. To exercise **server-side Rust blame**,
request the immutable full commit ID through HTTP:

```sh
commit=$(git rev-parse HEAD)
curl --fail-with-body --get \
  --data-urlencode 'path=hello.txt' \
  --output ../server-blame.porcelain \
  --write-out 'HTTP %{http_code}; total %{time_total}s\n' \
  "http://127.0.0.1:18080/demo/repo.git/blame/$commit"
git --no-pager blame --root --line-porcelain --diff-algorithm=myers \
  --no-textconv "$commit" -- hello.txt > ../native-blame.porcelain
cmp ../native-blame.porcelain ../server-blame.porcelain
git --no-pager blame --root "$commit" -- hello.txt
```

`cmp` prints nothing and exits zero when the complete porcelain bytes agree.
For an exact qualified oracle, use the external Git **2.56.0** client;
different client versions can have formatting/semantic differences. The native
Git command is an independent local comparison, not a server fallback.

The API needs a full reachable 40-digit SHA-1 commit and a literal repository-relative
path. Use `--data-urlencode` for paths containing spaces. Repeat the curl command to
experiment with timings, but this playground **does not configure persisted derived
blame caches or checkpoints**. Process-local RAM reuse is not cross-restart reuse.
The source Git history remains durable in PostgreSQL/S3 across app restarts.

## Stop, restart and inspect

Back at this source checkout's root:

```sh
python3 examples/playground/playground.py status
python3 examples/playground/playground.py logs
python3 examples/playground/playground.py stop
python3 examples/playground/playground.py up
git ls-remote http://127.0.0.1:18080/demo/repo.git
```

After restarting, repeat clone/pull and the blame request for the same commit.
`stop` stops the three containers without removing containers, volumes or generated
credentials. There is no automatic reset or data deletion command. After a VM reboot,
start Docker and run `up` again.

Names start with `git-rs-playground-`. The script refuses containers or volumes with
those names unless their owner label matches the saved random local owner ID, and
checks occupied ports before provisioning. Only one instance per Docker host is
supported. Failures leave owned resources available for `status`, `logs` and `stop`;
readiness checks have finite deadlines.

Generated demo-only credentials/configuration are in `examples/playground/.state/`
(directory 0700, files 0600, gitignored). Keep that directory with this checkout:
losing it does not authorize adoption of existing volumes, and generating new
passwords would not change existing PostgreSQL credentials. Do not commit it or
reuse these credentials for any other service. Docker administrators can inspect
container environments. This demo uses the PostgreSQL administrator and the S3
root identity for convenience; it is not a production least-privilege deployment.

Runtime container caps total 3840 MiB RAM, 2 CPU equivalents and 224 tasks; the app's
1 MiB socket mount and 64 MiB `/tmp` count toward its container memory cap. Allow host headroom
for Docker, clients, image builds and filesystem cache. Named-volume persistence
is not a backup, HA or disaster-recovery guarantee.

### Change the selected prebuilt runtime

Load the new release archive first. Upgrade only after stopping the existing
playground; the explicit upgrade preserves volume identities and credentials,
replaces the stopped application container and removes the obsolete pack-thread
environment setting from older playground snapshots:

```sh
python3 examples/playground/playground.py stop
python3 examples/playground/playground.py upgrade --image "$new_runtime_image"
python3 examples/playground/playground.py up
```

The next `up` applies the supported forward-only metadata migration before serving.
Downgrading a schema-v2 database to an older schema-v1 runtime is unsupported.
This VM demo has no Kubernetes worker: `cleanup` jobs remain queued until an elected
maintenance worker is configured separately.

## Optional automated client smoke

On a freshly initialized, empty `demo/repo`, this exercises the same two-client
flow and retains native/server porcelain in the ignored `.state/` directory:

```sh
python3 examples/playground/smoke.py
python3 examples/playground/playground.py stop
python3 examples/playground/playground.py up
python3 examples/playground/smoke.py --verify-existing
```

The first command intentionally refuses nonempty history. The restart check uses
the exact saved commit and repository URL, makes a fresh clone and checks full
blame byte parity again. Use Git 2.56.0 for the qualified native comparison.
