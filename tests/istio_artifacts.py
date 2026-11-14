"""Bounded public inputs for the separately supervised Istio phase."""
import gzip
import hashlib
import os
from pathlib import Path, PurePosixPath
import stat
import tarfile
import tempfile
import urllib.request


ISTIO = "1.31.3"
GATEWAY_API = "v1.7.0"
ARTIFACTS = {
    "istio-1.31.3-linux-amd64.tar.gz": (
        "https://github.com/istio/istio/releases/download/1.31.3/istio-1.31.3-linux-amd64.tar.gz",
        "b32bba09bc6138d738fd19a09ef6d0fcdd5c5c013efeb72969663c24a2bddd47"),
    "gateway-api-v1.7.0.yaml": (
        "https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml",
        "4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1"),
}
IMAGES = ("docker.io/istio/pilot:" + ISTIO, "docker.io/istio/proxyv2:" + ISTIO)
MAX_BYTES = 128 * 1024 * 1024
MAX_EXPANDED_BYTES = 512 * 1024 * 1024
MAX_MEMBERS = 10_000
MAX_METADATA_BYTES = 64 * 1024
MAX_METADATA_DEPTH = 16


class BoundedInput:
    def __init__(self, source, limit, message, checksum=None):
        self.source, self.remaining, self.message = source, limit, message
        self.checksum = checksum

    def read(self, size):
        assert size >= 0, "bounded Istio input reads required"
        chunk = self.source.read(min(size, 64 * 1024, self.remaining + 1))
        self.remaining -= len(chunk)
        assert self.remaining >= 0, self.message
        if self.checksum is not None:
            self.checksum.update(chunk)
        return chunk


def digest(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())
        assert stat.S_ISREG(metadata.st_mode) and metadata.st_size <= MAX_BYTES, "bounded regular Istio input required"
        value, total = hashlib.sha256(), 0
        while chunk := source.read(64 * 1024):
            total += len(chunk)
            assert total <= MAX_BYTES, "Istio artifact exceeded its byte bound"
            value.update(chunk)
    return value.hexdigest()


def verify_artifacts(cache):
    assert cache.is_dir() and not cache.is_symlink(), "prepare public Istio artifacts before measurement"
    for name, (_, expected) in ARTIFACTS.items():
        assert digest(cache / name) == expected, "checksum: " + name
    return {name: expected for name, (_, expected) in ARTIFACTS.items()}


def read_artifact(cache, name):
    expected = ARTIFACTS[name][1]
    descriptor = os.open(cache / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())
        assert stat.S_ISREG(metadata.st_mode) and metadata.st_size <= MAX_BYTES, "bounded regular Istio input required"
        body = source.read(metadata.st_size + 1)
        assert len(body) == metadata.st_size, "Istio artifact changed size during read"
    assert hashlib.sha256(body).hexdigest() == expected, "checksum: " + name
    return body


def prepare_artifacts(cache):
    cache.mkdir(parents=True, exist_ok=True)
    assert not cache.is_symlink(), "linked Istio artifact directory"
    for name, (url, expected) in ARTIFACTS.items():
        path = cache / name
        if not path.exists() and not path.is_symlink():
            descriptor, temporary = tempfile.mkstemp(prefix=".download-", dir=cache)
            try:
                with os.fdopen(descriptor, "wb") as destination, urllib.request.urlopen(url, timeout=90) as response:
                    total = 0
                    while chunk := response.read(64 * 1024):
                        total += len(chunk)
                        assert total <= MAX_BYTES, "public Istio artifact exceeded its download bound"
                        destination.write(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
                assert digest(temporary) == expected, "checksum: " + name
                os.chmod(temporary, 0o644)
                try:
                    os.link(temporary, path)
                except FileExistsError:
                    pass  # A concurrent preparer must have published the same pinned bytes.
                parent = os.open(cache, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            finally:
                Path(temporary).unlink(missing_ok=True)
        assert digest(path) == expected, "checksum: " + name


def artifacts(cache, destination):
    verify_artifacts(cache)
    destination.mkdir(mode=0o700)
    prefix = PurePosixPath("istio-" + ISTIO) / "manifests/charts"
    count, metadata_depth = 0, 0
    selected = set()

    class BoundedMember(tarfile.TarInfo):
        # tarfile's documented member-processing extension point runs before
        # hidden PAX/GNU records allocate their payload or recurse into a header.
        def _proc_member(self, archive):
            nonlocal count, metadata_depth
            count += 1
            assert count <= MAX_MEMBERS and 0 <= self.size <= MAX_EXPANDED_BYTES, "Istio archive exceeded its expansion bound"
            assert (len(archive.pax_headers) <= 1024
                    and sum(len(key) + len(value) for key, value in archive.pax_headers.items()) <= MAX_METADATA_BYTES), "Istio global archive metadata exceeded its bound"
            extended = self.type in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE, tarfile.GNUTYPE_LONGNAME)
            assert extended or self.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE), "linked or special Istio archive member"
            if not extended:
                return super()._proc_member(archive)
            assert self.size <= MAX_METADATA_BYTES, "Istio archive metadata exceeded its byte bound"
            metadata_depth += 1
            try:
                assert metadata_depth <= MAX_METADATA_DEPTH, "Istio archive metadata exceeded its nesting bound"
                return super()._proc_member(archive)
            finally:
                metadata_depth -= 1

        def _proc_gnusparse_00(self, *args):
            raise AssertionError("linked or special Istio archive member")

        _proc_gnusparse_01 = _proc_gnusparse_00
        _proc_gnusparse_10 = _proc_gnusparse_00

    name = "istio-" + ISTIO + "-linux-amd64.tar.gz"
    descriptor = os.open(cache / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        metadata = os.fstat(source.fileno())
        assert stat.S_ISREG(metadata.st_mode) and metadata.st_size <= MAX_BYTES, "bounded regular Istio input required"
        compressed = BoundedInput(source, MAX_BYTES, "Istio artifact exceeded its byte bound", hashlib.sha256())
        # Bound gzip output before tarfile can buffer metadata or skip bodies.
        # Extract charts only; never publish or execute the bundled istioctl.
        with gzip.GzipFile(fileobj=compressed, mode="rb") as decoded:
            expanded = BoundedInput(decoded, MAX_EXPANDED_BYTES, "Istio archive exceeded its expansion bound")
            with tarfile.open(fileobj=expanded, mode="r|", stream=True, tarinfo=BoundedMember) as archive:
                for member in archive:
                    path = PurePosixPath(member.name)
                    assert not path.is_absolute() and ".." not in path.parts and len(member.name) <= 1024, "unsafe Istio archive path"
                    assert not member.issparse() and 0 <= member.size <= MAX_EXPANDED_BYTES, "sparse or oversized Istio archive member"
                    if path.is_relative_to(prefix):
                        if member.isfile():
                            assert path not in selected, "duplicate Istio chart file"
                            selected.add(path)
                        archive.extract(member, destination, filter="data")
            # Tar's end marker can precede gzip's end. Include padding, skipped
            # data and concatenated gzip members in the bound and pinned digest.
            while expanded.read(64 * 1024):
                pass
        assert compressed.checksum.hexdigest() == ARTIFACTS[name][1], "checksum: " + name
    distribution = destination / ("istio-" + ISTIO)
    for chart in ("base", "istio-control/istio-discovery"):
        assert (distribution / "manifests/charts" / chart / "Chart.yaml").is_file(), "official Istio chart missing"
    return distribution
