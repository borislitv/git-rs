"""Run prebuilt servers in their Git-free image, with external client/oracle I/O.

Only the private server directory is mounted. Client repositories, the Docker
socket and the oracle installation never enter a backend container.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid


LABEL = "git-rs.integration-fixture"
IMAGE_ID = r"sha256:[0-9a-f]{64}"


def docker_environment(environment=None):
    value = dict(os.environ if environment is None else environment, DOCKER_HOST="unix:///var/run/docker.sock")
    value.pop("DOCKER_CONTEXT", None)
    return value


def run(arguments, *, timeout=20, ok=True, **kwargs):
    if arguments[0] == "docker":
        kwargs["env"] = docker_environment(kwargs.get("env"))
    result = subprocess.run(list(map(str, arguments)), capture_output=True,
                            timeout=timeout, **kwargs)
    if ok and result.returncode:
        raise AssertionError(f"runtime fixture command failed: {arguments[:3]}: "
                             f"{result.stderr[-4000:]!r}")
    return result


def inspect_image(reference, revision, role):
    assert re.fullmatch(r"[0-9a-f]{40}", revision), "exact source revision required"
    rows = json.loads(run(["docker", "image", "inspect", reference]).stdout)
    assert len(rows) == 1
    image = rows[0]
    assert re.fullmatch(IMAGE_ID, image["Id"]), "immutable local image ID required"
    labels = image["Config"].get("Labels") or {}
    assert labels.get("io.git-rs.role") == role, "wrong server image role"
    assert labels.get("org.opencontainers.image.revision") == revision, "server image revision differs"
    assert image["Os"] == "linux" and image["Architecture"] == "amd64"
    return image["Id"]


def cluster_cgroups(cluster, root):
    """Attest actual CRI task placement, not only the outer k3s container limits."""
    assert root.startswith("/") and ".." not in Path(root).parts
    observations = []
    for listing, inspecting, key, state in (("ps", "inspect", "containers", "CONTAINER_RUNNING"),
                                          ("pods", "inspectp", "items", "SANDBOX_READY")):
        rows = json.loads(run(["docker", "exec", cluster, "crictl", listing, "-o", "json"]).stdout)[key]
        assert len(rows) <= 128, "fixture CRI inventory exceeded its bound"
        for row in rows:
            if row["state"] != state:
                continue
            assert re.fullmatch(r"[0-9a-f]{64}", row["id"])
            info = json.loads(run(["docker", "exec", cluster, "crictl", inspecting, row["id"]]).stdout)["info"]
            pid = int(info["pid"])
            assert 0 < pid < 2**31
            group = run(["docker", "exec", cluster, "cat", f"/proc/{pid}/cgroup"]).stdout.decode().strip()
            assert group.startswith("0::" + root + "/"), "CRI task escaped the owned aggregate cgroup"
            observations.append({"id": row["id"], "cgroup": group[3:]})
    return observations


class Runtime:
    def __init__(self, directory, owner, image, fixture_image, revision, register):
        self.directory = Path(directory).resolve(strict=True)
        assert self.directory.is_dir() and self.directory.stat().st_mode & 0o777 == 0o700
        assert self.directory.stat().st_uid == os.getuid()
        assert re.fullmatch(r"[0-9a-f]{32}", owner)
        self.owner, self.register = owner, register
        self.image = inspect_image(image, revision, "runtime")
        self.fixture_image = (inspect_image(fixture_image, revision, "runtime-fixture")
                              if fixture_image else None)

    def arguments(self, env, *, fixture=False, service_account=None):
        image = self.fixture_image if fixture else self.image
        assert image, "the prebuilt Git-free runtime-fixture image is required"
        host_root = os.environ.get("GIT_RS_TEST_HOST_ROOT")
        controller_root = os.environ.get("GIT_RS_TEST_CONTROLLER_ROOT")
        assert bool(host_root) == bool(controller_root), "both fixture root mappings are required"

        def host_path(path):
            if not host_root:
                return path
            root = Path(host_root)
            assert root.is_absolute() and ".." not in root.parts, "invalid Docker-host fixture root"
            return root / path.relative_to(Path(controller_root).resolve(strict=True))

        host_directory = host_path(self.directory)
        assert "," not in str(host_directory) and "," not in str(self.directory), "fixture paths cannot contain commas"
        args = ["--pull=never", "--network", "host", "--read-only", "--cap-drop=ALL",
                "--security-opt", "no-new-privileges", "--user", f"{os.getuid()}:{os.getgid()}",
                "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777",
                "--memory", "1024m", "--memory-swap", "1024m", "--cpus", "1",
                "--pids-limit", "64", "--label", f"{LABEL}={self.owner}",
                "--mount", f"type=bind,src={host_directory},dst={self.directory}"]
        if service_account is not None:
            account = Path(service_account).resolve(strict=True)
            assert account.is_dir() and account.is_relative_to(self.directory.parent)
            assert not account.is_relative_to(self.directory), "serving must not see maintenance credentials"
            assert account.stat().st_uid == os.getuid() and account.stat().st_mode & 0o777 == 0o700
            source = host_path(account)
            assert "," not in str(source)
            args += ["--mount", f"type=bind,src={source},dst=/var/run/secrets/kubernetes.io/serviceaccount,readonly"]
        # --mount refuses a missing daemon-side source instead of creating an
        # unrelated root-owned directory. Nested controllers must bind this exact
        # fixture path from their Docker host before starting the controller.
        if parent := os.environ.get("GIT_RS_TEST_CGROUP_PARENT"):
            args += ["--cgroup-parent", parent]
        if label := os.environ.get("GIT_RS_TEST_OWNER_LABEL"):
            args += ["--label", label]
        if cpus := os.environ.get("GIT_RS_TEST_SERVICE_CPUS"):
            args += ["--cpuset-cpus", cpus]
        if env is not None:
            descriptor, name = tempfile.mkstemp(prefix=".runtime-env-", dir=self.directory)
            try:
                with os.fdopen(descriptor, "w") as stream:
                    stream.write("HOME=/var/empty/git-rs\nLC_ALL=C\n")
                    for key, value in sorted(env.items()):
                        if not (key.startswith("PG") or key.startswith("GIT_RS_")
                                or key in {"SSL_CERT_FILE", "SSL_CERT_DIR", "SSH_ORIGINAL_COMMAND", "GIT_PROTOCOL",
                                           "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT"}):
                            continue
                        assert re.fullmatch(r"[A-Z][A-Z0-9_]*", key)
                        assert not any(byte in str(value) for byte in ("\n", "\r", "\0")), "invalid environment value"
                        stream.write(f"{key}={value}\n")
                args += ["--env-file", name]
            except BaseException:
                Path(name).unlink()
                raise
        else:
            name = None
        return args, image, name

    def start(self, arguments, env, log, *, program=None, service_account=None):
        args, image, environment = self.arguments(env, fixture=program is not None, service_account=service_account)
        name = "git-rs-runtime-" + uuid.uuid4().hex
        if program is not None:
            args += ["--entrypoint", "/usr/bin/tini"]
            arguments = ["--", program, *arguments]
        try:
            identifier = run(["docker", "create", "--name", name, *args, image, *arguments]).stdout.decode().strip()
            assert re.fullmatch(r"[0-9a-f]{64}", identifier)
            self.register(identifier)
            attached = subprocess.Popen(["docker", "start", "--attach", identifier],
                                        stdout=log, stderr=log, env=docker_environment())
            return RuntimeProcess(identifier, self.owner, attached)
        finally:
            if environment:
                Path(environment).unlink()

    def execute(self, arguments, env, *, data=None, ok=True, timeout=180, service_account=None):
        args, image, environment = self.arguments(env, service_account=service_account)
        try:
            return run(["docker", "run", "--rm", "--interactive", *args, image, *arguments],
                       timeout=timeout, ok=ok, input=data)
        finally:
            if environment:
                Path(environment).unlink()

class RuntimeProcess:
    def __init__(self, identifier, owner, attached):
        self.identifier, self.owner, self.attached = identifier, owner, attached

    def state(self):
        rows = json.loads(run(["docker", "inspect", self.identifier]).stdout)
        assert len(rows) == 1 and rows[0]["Id"] == self.identifier
        assert (rows[0]["Config"].get("Labels") or {}).get(LABEL) == self.owner, "foreign runtime container"
        return rows[0]["State"]

    def poll(self):
        state = self.state()
        if state["Running"] or (state["Status"] == "created" and self.attached.poll() is None):
            return None
        return state["ExitCode"]

    def wait(self, timeout=180):
        deadline = time.monotonic() + timeout
        while (code := self.poll()) is None:
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(["docker", "wait", self.identifier], timeout)
            time.sleep(0.025)
        self.attached.wait(timeout=max(0.1, deadline - time.monotonic()))
        return code

    def signal(self, value):
        if self.state()["Running"]:
            result = run(["docker", "kill", "--signal", value, self.identifier], ok=False)
            assert not result.returncode or not self.state()["Running"], result.stderr

    def terminate(self):
        self.signal("TERM")

    def kill(self):
        self.signal("KILL")

    def process_status(self):
        assert self.state()["Running"]
        # The production ENTRYPOINT is tini with exactly one Rust child. Inspect
        # that child's namespace, not the unrelated docker-attach client PID or
        # a similarly numbered process in the external oracle's PID namespace.
        children = run(["docker", "exec", self.identifier, "/bin/cat", "/proc/1/task/1/children"]).stdout.split()
        assert len(children) == 1 and children[0].isdigit(), "expected one production-entrypoint child"
        pid = children[0].decode()
        return run(["docker", "exec", self.identifier, "/bin/cat", f"/proc/{pid}/status"]).stdout.decode()
