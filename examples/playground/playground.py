#!/usr/bin/env python3
"""Persistent, loopback-only Linux VM demo. Requires Docker, Python 3 and curl."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import subprocess
import sys
import time


STATE = Path(__file__).resolve().parent / ".state"
PREFIX = "git-rs-playground"
LABEL = "git-rs.playground-owner"
PG_IMAGE = "postgres:18.6-bookworm@sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5"
S3_IMAGE = "versity/versitygw@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"
PORTS = {"pg": [15432], "s3": [17070], "app": [18080, 18081]}


def run(*args, capture=False, data=None, check=True):
    environment = dict(os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
    environment.pop("DOCKER_CONTEXT", None)
    return subprocess.run(args, input=data, text=True, check=check,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None, timeout=180, env=environment)


def inspect(kind, name):
    result = run("docker", kind, "inspect", name, capture=True, check=False)
    if result.returncode:
        # A broken daemon is not evidence that a named object is absent.
        run("docker", "info", capture=True)
        if "no such" not in result.stderr.lower():
            raise RuntimeError(result.stderr.strip())
        return None
    return json.loads(result.stdout)[0]


def owned(kind, name, owner):
    item = inspect(kind, name)
    if item is not None:
        labels = item["Config"].get("Labels") if kind == "container" else item.get("Labels")
        if (labels or {}).get(LABEL) != owner:
            raise RuntimeError(f"Refusing foreign {kind}: {name}")
    return item


def private_write(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write(content)


def private_read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "r", encoding="utf-8") as source:
        metadata = os.fstat(source.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > 64 * 1024):
            raise RuntimeError(f"Expected a bounded private mode 0600 file: {path}")
        content = source.read(64 * 1024 + 1)
        if len(content.encode("utf-8")) > 64 * 1024:
            raise RuntimeError(f"Private playground file exceeded its byte bound: {path}")
        return content


def private_replace(path, content):
    temporary = path.with_name("." + path.name + "." + secrets.token_hex(16))
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def configuration(create, image):
    if not STATE.exists():
        if not create:
            raise RuntimeError("No playground state. Run up first.")
        STATE.mkdir(mode=0o700)
    if STATE.is_symlink() or STATE.stat().st_uid != os.getuid() or STATE.stat().st_mode & 0o077:
        raise RuntimeError(f"State directory must be owned by this user and mode 0700: {STATE}")
    config_path = STATE / "config.json"
    if not config_path.exists():
        if not create:
            raise RuntimeError("Missing config.json; do not replace existing volume credentials.")
        if not image:
            raise RuntimeError("Select a loaded prebuilt runtime: up --image IMAGE_OR_SHA256")
        config = {"format": 2, "owner": secrets.token_hex(16), "image": runtime_image(image)}
        private_write(config_path, json.dumps(config) + "\n")
    config = json.loads(private_read(config_path))
    if not re.fullmatch(r"[0-9a-f]{32}", config.get("owner", "")):
        raise RuntimeError("Invalid playground resource owner")
    if image and runtime_image(image) != config["image"]:
        raise RuntimeError("Image differs from saved configuration; stop and review .state/config.json first.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*", config["image"]):
        raise RuntimeError("Invalid image reference")
    if create:
        if config.get("format") != 2 or not re.fullmatch(r"sha256:[0-9a-f]{64}", config["image"]):
            raise RuntimeError("Legacy playground state: run stop, then upgrade --image PREBUILT_RUNTIME, then up.")
        runtime_image(config["image"])
    return config


def runtime_image(reference):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*", reference):
        raise RuntimeError("Invalid image reference")
    image = inspect("image", reference)
    if image is None:
        raise RuntimeError("Prebuilt runtime is not loaded; use docker load or pull the published digest first.")
    if (image["Config"].get("Labels") or {}).get("io.git-rs.role") != "runtime":
        raise RuntimeError("Expected a Git-free release runtime, not a client/oracle or build image")
    identity = image["Id"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity):
        raise RuntimeError("Runtime has no immutable image identity")
    return identity


def upgrade(config, image):
    if not image:
        raise RuntimeError("upgrade requires --image PREBUILT_RUNTIME")
    identity = runtime_image(image)
    for service in PORTS:
        item = owned("container", PREFIX + "-" + service, config["owner"])
        if item is not None and item["State"]["Running"]:
            raise RuntimeError("Stop all playground services before upgrading the runtime selection")
    environment = STATE / "app.env"
    lines = [line for line in private_read(environment).splitlines()
             if not line.startswith("GIT_RS_PACK_THREADS=")]
    for key, value in [("GIT_RS_BLAME_CONCURRENCY", "1"), ("GIT_RS_BLAME_WAITERS", "0")]:
        if not any(line.startswith(key + "=") for line in lines):
            lines.append(key + "=" + value)
    # Only the stopped, exact-owned application container is replaced. Persistent
    # dependency volumes and their existing credentials remain the same identities.
    existing = owned("container", PREFIX + "-app", config["owner"])
    if existing is not None:
        run("docker", "rm", existing["Id"])
    private_replace(environment, "\n".join(lines) + "\n")
    private_replace(STATE / "config.json", json.dumps({"format": 2, "owner": config["owner"], "image": identity}) + "\n")
    print("Runtime selection updated. Run up to migrate and start the selected prebuilt image.")


def wait_for(description, probe):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {description}; run status and logs.")


def sql(query):
    return run("docker", "exec", PREFIX + "-pg", "psql", "-X", "-qAt",
               "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "git_rs", "-c",
               "SET search_path = git_rs, pg_catalog; " + query,
               capture=True)


def bucket(method):
    # Credentials go through curl's stdin configuration, never its argument list.
    config = private_read(STATE / "s3.curl")
    return run("curl", "--disable", "--noproxy", "*", "--silent", "--show-error", "--fail",
               "--max-time", "3", "--config", "-", "--request", method,
               "--output", "/dev/null", capture=True, data=config, check=False)


def resources(memory, cpus, pids):
    args = ["--memory", memory, "--memory-swap", memory, "--cpus", cpus,
            "--pids-limit", str(pids)]
    # Optional CPU affinity is useful on a shared verification host.
    if os.environ.get("PLAYGROUND_CPUSET"):
        args += ["--cpuset-cpus", os.environ["PLAYGROUND_CPUSET"]]
    return args


def start(config, service, args):
    name = PREFIX + "-" + service
    existing = owned("container", name, config["owner"])
    if existing:
        if not existing["State"]["Running"]:
            run("docker", "start", existing["Id"])
        return
    run("docker", "run", "-d", "--name", name, "--label",
        LABEL + "=" + config["owner"], *args)


def app_options(config):
    return ["--pull=never", "--network", "host", "--user", "10001:10001", "--read-only",
            "--env-file", str(STATE / "app.env"),
            "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,mode=1777",
            "--tmpfs", "/run/git-rs:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=10001,gid=10001",
            *resources("2304m", "1.25", 112), config["image"]]


def cli(config, *args):
    run("docker", "run", "--rm", "--label", LABEL + "=" + config["owner"],
        *app_options(config), *args)


def up(config):
    # Check every name/port before starting any service. Never attach foreign data.
    for service, ports in PORTS.items():
        item = owned("container", PREFIX + "-" + service, config["owner"])
        if item and item["State"]["Running"]:
            continue
        for port in ports:
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
    volumes = {s: PREFIX + "-" + s + "-data" for s in ["pg", "s3"]}
    for volume in volumes.values():
        owned("volume", volume, config["owner"])
    run("docker", "image", "inspect", config["image"], capture=True)
    # Dependency pulls are explicit and pinned; the application is never pulled.
    for image in [PG_IMAGE, S3_IMAGE]:
        if run("docker", "image", "inspect", image, capture=True, check=False).returncode:
            run("docker", "pull", image)
    if not (STATE / "app.env").exists():
        if any(inspect("volume", v) for v in volumes.values()):
            raise RuntimeError("Existing volumes without credentials; restore .state rather than regenerate it.")
        password, secret = secrets.token_hex(24), secrets.token_hex(24)
        private_write(STATE / "pg.env", f"POSTGRES_DB=git_rs\nPOSTGRES_PASSWORD={password}\n")
        private_write(STATE / "s3.env", f"ROOT_ACCESS_KEY_ID=playground\nROOT_SECRET_ACCESS_KEY={secret}\n")
        private_write(STATE / "s3.curl", 'url = "http://127.0.0.1:17070/git-playground"\n'
                      'aws-sigv4 = "aws:amz:us-east-1:s3"\n'
                      f'user = "playground:{secret}"\n')
        private_write(STATE / "app.env", f"PGHOST=127.0.0.1\nPGPORT=15432\n"
                      f"PGUSER=postgres\nPGDATABASE=git_rs\nPGPASSWORD={password}\n"
                      "PGSSLMODE=disable\nPGGSSENCMODE=disable\nPGCONNECT_TIMEOUT=2\n"
                      "GIT_RS_DEV_HTTP=1\nGIT_RS_S3_ENDPOINT=http://127.0.0.1:17070\n"
                      f"GIT_RS_S3_BUCKET=git-playground\nGIT_RS_S3_ACCESS_KEY=playground\n"
                      f"GIT_RS_S3_SECRET_KEY={secret}\nGIT_RS_BLAME_CONCURRENCY=1\nGIT_RS_BLAME_WAITERS=0\n")
    for filename in ["pg.env", "s3.env", "s3.curl", "app.env"]:
        private_read(STATE / filename)
    for volume in volumes.values():
        if not inspect("volume", volume):
            run("docker", "volume", "create", "--label", LABEL + "=" + config["owner"], volume)
    start(config, "pg", ["--pull=never", *resources("1024m", ".5", 64),
                          "--env-file", str(STATE / "pg.env"),
                          "-p", "127.0.0.1:15432:5432", "-v",
                          volumes["pg"] + ":/var/lib/postgresql", PG_IMAGE])
    start(config, "s3", ["--pull=never", *resources("512m", ".25", 48),
                          "--env-file", str(STATE / "s3.env"),
                          "-p", "127.0.0.1:17070:7070", "-v",
                          volumes["s3"] + ":/data", S3_IMAGE, "--quiet", "posix", "/data"])
    def postgres_ready():
        result = run("docker", "exec", PREFIX + "-pg", "pg_isready", "-h", "127.0.0.1",
                     "-U", "postgres", "-d", "git_rs", capture=True, check=False)
        return result.returncode == 0
    wait_for("PostgreSQL", postgres_ready)
    wait_for("S3", lambda: bucket("HEAD").returncode == 0 or bucket("PUT").returncode == 0)
    # Init runs while serving is stopped; repeated up checks the actual SQL state.
    app = owned("container", PREFIX + "-app", config["owner"])
    if app is not None and app["Image"] != config["image"]:
        raise RuntimeError("Saved application container differs from the selected immutable runtime")
    running = app is not None and app["State"]["Running"]
    if not running:
        cli(config, "migrate")
    count = sql("SELECT count(*) FROM repositories WHERE slug='demo/repo';").stdout.strip()
    if count == "0":
        if running:
            raise RuntimeError("Running playground lost demo/repo; stop it before reinitializing")
        cli(config, "create", "demo/repo", "alice")
    elif count != "1":
        raise RuntimeError("Unexpected demo repository count")
    owner = sql("SELECT count(*) FROM permissions p JOIN repositories r ON r.id=p.repo_id "
                "WHERE r.slug='demo/repo' AND p.principal='alice' AND p.role='owner';").stdout.strip()
    if owner != "1":
        raise RuntimeError("demo/repo exists without expected alice owner permission")
    start(config, "app", [*app_options(config), "serve",
                           "--socket", "/run/git-rs/backend.sock", "--dev-http", "127.0.0.1:18080",
                           "--dev-principal", "alice", "--health", "127.0.0.1:18081"])
    wait_for("application readiness", lambda: run(
        "curl", "--disable", "--noproxy", "*", "--silent", "--fail", "--max-time", "2",
        "http://127.0.0.1:18081/readyz", capture=True, check=False).returncode == 0)
    print("Ready: http://127.0.0.1:18080/demo/repo.git (development principal alice)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["up", "init", "upgrade", "status", "logs", "stop"])
    parser.add_argument("--image", help="loaded prebuilt runtime image or digest; immutable ID saved on first up")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("This host-network playground requires Linux")
    os.umask(0o077)
    # Serialize state creation and lifecycle operations across terminal sessions.
    import fcntl
    if args.command in ["up", "init"]:
        STATE.mkdir(mode=0o700, exist_ok=True)
    if not STATE.exists():
        raise RuntimeError("No playground state. Run up first.")
    if STATE.is_symlink() or STATE.stat().st_uid != os.getuid() or STATE.stat().st_mode & 0o077:
        raise RuntimeError(f"State directory must be owned by this user and mode 0700: {STATE}")
    descriptor = os.open(STATE / "lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(descriptor, "a") as lock:
        metadata = os.fstat(lock.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise RuntimeError("Expected a private owned playground lock file")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = configuration(args.command in ["up", "init"], None if args.command == "upgrade" else args.image)
        if args.command == "upgrade":
            upgrade(config, args.image)
        elif args.command in ["up", "init"]:
            up(config)
        else:
            for service in ["app", "s3", "pg"]:
                name = PREFIX + "-" + service
                item = owned("container", name, config["owner"])
                if item is None:
                    print(f"{name}: absent")
                elif args.command == "stop":
                    run("docker", "stop", "--timeout", "15", item["Id"])
                elif args.command == "logs":
                    print(f"\n{name}", flush=True)
                    run("docker", "logs", "--tail", "80", name)
                else:
                    print(f"{name}: {item['State']['Status']} (image {item['Config']['Image']})")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"playground: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr.strip(), file=sys.stderr)
        sys.exit(1)
