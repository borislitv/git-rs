#!/usr/bin/env python3
"""Inventory the actual release filesystem without executing a container.

Absence is checked across exported paths, symlink/hardlink targets and the runtime
package inventory, not only PATH or metrics. Lifecycle behavior is a separate gate.
The private journal lets an independent supervisor repeat exact-owned cleanup.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import select
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid


LABEL = "git-rs.runtime-inventory-owner"
MAX_BYTES = 4 * 1024 * 1024 * 1024
MAX_FILES = 100_000
MAX_TAR_METADATA = 64 * 1024
EXPORT_SECONDS = 120


class ExportStream:
    """Poll the export pipe against one absolute deadline, including tar headers."""

    def __init__(self, pipe, seconds=EXPORT_SECONDS):
        self.pipe = pipe
        self.deadline = time.monotonic() + seconds
        self.received = 0

    def read(self, size):
        if not 0 <= size <= 1024 * 1024:
            raise ValueError("runtime export read exceeds chunk bound")
        if size == 0:
            return b""
        while True:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("runtime export absolute deadline exceeded")
            ready, _, _ = select.select([self.pipe], [], [], remaining)
            if ready:
                chunk = os.read(self.pipe.fileno(), size)
                self.received += len(chunk)
                if self.received > MAX_BYTES + MAX_FILES * 1024:
                    raise ValueError("runtime export wire byte bound exceeded")
                return chunk


def docker_environment():
    environment = dict(os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
    environment.pop("DOCKER_CONTEXT", None)
    return environment


def command(arguments, *, check=True):
    result = subprocess.run(list(map(str, arguments)), capture_output=True, timeout=20,
                            env=docker_environment())
    if check and result.returncode:
        raise RuntimeError(f"runtime inventory command failed ({result.returncode})")
    return result


def inspect(name):
    result = command(["docker", "container", "inspect", name], check=False)
    if result.returncode == 0:
        return json.loads(result.stdout)[0]
    if b"No such container" not in result.stderr:
        raise RuntimeError("runtime inventory container state unavailable")
    return None


def cleanup(journal):
    parent = journal.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700:
        raise ValueError("runtime cleanup journal parent is not privately owned")
    fd = os.open(journal, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(fd, "rb") as source:
        metadata = os.fstat(source.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > 16384):
            raise ValueError("runtime cleanup journal is not a private regular file")
        raw = source.read(16385)
    if len(raw) > 16384:
        raise ValueError("runtime cleanup journal exceeds byte bound")
    owned = json.loads(raw)
    if not re.fullmatch(r"[0-9a-f]{32}", owned["owner"]) or owned["name"] != "git-rs-inventory-" + owned["owner"]:
        raise ValueError("invalid runtime inventory ownership journal")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", owned["image"]):
        raise ValueError("invalid runtime inventory image identity")
    current = inspect(owned["name"])
    if current is None:
        return
    if (current["Config"].get("Labels") or {}).get(LABEL) != owned["owner"] or current["Image"] != owned["image"]:
        raise ValueError("refusing cleanup of foreign runtime inventory container")
    if current["State"]["Running"]:
        raise ValueError("inventory container was started unexpectedly; automatic removal refused")
    command(["docker", "rm", current["Id"]])
    if inspect(owned["name"]) is not None:
        raise RuntimeError("runtime inventory cleanup not confirmed")


def forbidden(value):
    for component in PurePosixPath(value).parts:
        if component in {".git", ".gitconfig", "git-core", "git", "gitk", "scalar", "gitweb.cgi"}:
            return True
        if component.startswith("git-") and component != "git-rs":
            return True
    return False


def scan(stream):
    files, total, executables, packages = 0, 0, [], None
    headers, metadata_depth = 0, 0

    class BoundedMember(tarfile.TarInfo):
        def _proc_member(self, archive):
            nonlocal headers, metadata_depth
            headers += 1
            if headers > MAX_FILES or not 0 <= self.size <= MAX_BYTES:
                raise ValueError("runtime filesystem inventory bound exceeded")
            if (len(archive.pax_headers) > 1024
                    or sum(len(key) + len(value) for key, value in archive.pax_headers.items()) > MAX_TAR_METADATA):
                raise ValueError("runtime global tar metadata exceeded its bound")
            if self.type == tarfile.GNUTYPE_SPARSE:
                raise ValueError("unsupported sparse runtime archive member")
            if self.type not in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE,
                                 tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK):
                return super()._proc_member(archive)
            if self.size > MAX_TAR_METADATA:
                raise ValueError("runtime tar metadata exceeded its byte bound")
            metadata_depth += 1
            try:
                if metadata_depth > 16:
                    raise ValueError("runtime tar metadata exceeded its nesting bound")
                return super()._proc_member(archive)
            finally:
                metadata_depth -= 1

        def _proc_gnusparse_00(self, *args):
            raise ValueError("unsupported sparse runtime archive member")

        _proc_gnusparse_01 = _proc_gnusparse_00
        _proc_gnusparse_10 = _proc_gnusparse_00

    with tarfile.open(fileobj=stream, mode="r|", stream=True, tarinfo=BoundedMember) as archive:
        for member in archive:
            files += 1
            total += member.size
            path = PurePosixPath(member.name)
            if (files > MAX_FILES or not 0 <= member.size <= MAX_BYTES or total > MAX_BYTES
                    or len(member.name) > 4096 or len(member.linkname) > 4096):
                raise ValueError("runtime filesystem inventory bound exceeded")
            if path.is_absolute() or ".." in path.parts or forbidden(member.name) or forbidden(member.linkname):
                raise ValueError(f"forbidden runtime path or link: {member.name}")
            if not member.isfile():
                continue
            source = archive.extractfile(member)
            first = source.read(4)
            if path.as_posix() == "usr/share/git-rs/packages.tsv":
                if member.size > 1024 * 1024:
                    raise ValueError("runtime package inventory exceeds byte bound")
                packages = (first + source.read()).decode("utf-8")
            elif first == b"\x7fELF" or member.mode & 0o111:
                checksum = hashlib.sha256(first)
                while chunk := source.read(1024 * 1024):
                    checksum.update(chunk)
                executables.append({"path": path.as_posix(), "bytes": member.size,
                                     "mode": member.mode, "elf": first == b"\x7fELF", "sha256": checksum.hexdigest()})
    # A tar end marker does not attest that the export process finished or that
    # trailing wire bytes fit. The actual ExportStream enforces both bounds.
    while stream.read(1024 * 1024):
        pass
    if packages is None:
        raise ValueError("runtime package inventory missing")
    for row in packages.splitlines():
        package = row.split("\t", 1)[0].split(":", 1)[0]
        if package in {"git", "gitk", "gitweb", "scalar"} or package.startswith(("git-", "libgit2")):
            raise ValueError("native Git package present in runtime: " + package)
    if not any(row["path"] == "usr/local/bin/git-rs" and row["elf"] for row in executables):
        raise ValueError("release server ELF missing")
    return {"files": files, "headers": headers, "logical_bytes": total, "executables": sorted(executables, key=lambda row: row["path"]),
            "packages_sha256": hashlib.sha256(packages.encode()).hexdigest()}


def finish(directory, report, journal, exporter):
    original, failures = sys.exception(), []
    if exporter is not None:
        try:
            exporter.stdout.close()
        except BaseException as error:
            failures.append(error)
        try:
            if exporter.poll() is None:
                exporter.terminate()
                try:
                    exporter.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    exporter.kill()
                    exporter.wait(timeout=5)
        except BaseException as error:
            failures.append(error)
    try:
        cleanup(journal)
    except BaseException as error:
        failures.append(error)
    report["cleanup"] = not failures
    if failures:
        report.update(status="failed", cleanup_errors=[type(error).__name__ for error in failures])
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".report-", dir=directory)
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(json.dumps(report, indent=2) + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, directory / "report.json")
        parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
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
            original.add_note("Runtime inventory cleanup/report also failed: " + ", ".join(type(error).__name__ for error in failures))
        elif len(failures) == 1:
            raise failures[0]
        else:
            raise BaseExceptionGroup("Runtime inventory cleanup/report failed", failures)


def inventory(image, revision, directory, role="runtime"):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("immutable runtime image and full expected revision required")
    metadata = json.loads(command(["docker", "image", "inspect", image]).stdout)[0]
    labels = metadata["Config"].get("Labels") or {}
    if (metadata["Id"] != image or labels.get("io.git-rs.role") != role
            or labels.get("org.opencontainers.image.revision") != revision
            or metadata["Os"] != "linux" or metadata["Architecture"] != "amd64"):
        raise ValueError("expected Git-free runtime role/revision unavailable")
    directory.mkdir(mode=0o700)
    owner = uuid.uuid4().hex
    name = "git-rs-inventory-" + owner
    journal = directory / "ownership.json"
    fd = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump({"owner": owner, "name": name, "image": image}, output)
        output.flush()
        os.fsync(output.fileno())
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    report = {"format": 1, "image": image, "role": role, "revision": revision, "status": "running", "cleanup": False}
    exporter = None
    try:
        command(["docker", "create", "--pull=never", "--name", name, "--label", LABEL + "=" + owner, image])
        exporter = subprocess.Popen(["docker", "export", name], stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, env=docker_environment())
        report.update(scan(ExportStream(exporter.stdout)))
        if exporter.wait(timeout=10):
            raise RuntimeError("runtime filesystem export failed")
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["failure"] = type(error).__name__
        raise
    finally:
        finish(directory, report, journal, exporter)
    print(json.dumps({"status": report["status"], "image": image, "revision": revision, "files": report["files"]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-image")
    parser.add_argument("--revision")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--role", choices=("runtime", "runtime-fixture"), default="runtime")
    parser.add_argument("--cleanup", type=Path, help="repeat exact-owned cleanup from ownership.json")
    args = parser.parse_args()
    if args.cleanup:
        if args.runtime_image or args.revision or args.directory:
            parser.error("--cleanup cannot be combined with inventory arguments")
        cleanup(args.cleanup)
    else:
        if not args.runtime_image or not args.revision or not args.directory:
            parser.error("--runtime-image, --revision and --directory are required")
        inventory(args.runtime_image, args.revision, args.directory, args.role)


if __name__ == "__main__":
    main()
