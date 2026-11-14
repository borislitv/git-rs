#!/usr/bin/env python3
"""Export a commit-bound, prebuilt runtime and the persistent VM launcher.

This packages existing inputs. It does not build, qualify, publish or run images.
Only a clean public checkout and a matching immutable runtime can be exported.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def command(arguments, timeout=20):
    environment = None
    if arguments[0] == "docker":
        environment = dict(os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
        environment.pop("DOCKER_CONTEXT", None)
    return subprocess.run(list(map(str, arguments)), check=True, capture_output=True,
                          text=True, timeout=timeout, env=environment).stdout.strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def export(image, directory):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image):
        raise ValueError("--image must be an immutable, locally loaded image ID")
    revision = command(["git", "--no-optional-locks", "-C", ROOT, "rev-parse", "HEAD"])
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("full source revision required")
    if command(["git", "--no-optional-locks", "-C", ROOT, "status", "--porcelain"]):
        raise ValueError("package from the clean, qualified public source checkout")
    inspection = json.loads(command(["docker", "image", "inspect", image]))[0]
    labels = inspection["Config"].get("Labels") or {}
    if inspection["Id"] != image or labels.get("io.git-rs.role") != "runtime" or labels.get("org.opencontainers.image.revision") != revision:
        raise ValueError("runtime image role/revision differs from the selected checkout")
    if inspection["Os"] != "linux" or inspection["Architecture"] != "amd64":
        raise ValueError("VM package requires the supported Linux amd64 runtime")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    if not directory.parent.is_dir():
        raise ValueError("package parent directory must already exist")
    directory.mkdir(mode=0o755)
    # Retain a partial directory on error; never replace an existing package.
    archive = directory / "git-rs-runtime.tar"
    command(["docker", "image", "save", "--output", archive, image], timeout=180)
    paths = ["examples/playground/playground.py", "examples/playground/smoke.py",
             "examples/playground/README.md", "OPERATIONS.md"]
    for relative in paths:
        target = directory / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    instructions = """# Prebuilt Linux VM package

Verify and load the supplied runtime, then start the persistent loopback demo:

```sh
sha256sum --check SHA256SUMS
docker load --input git-rs-runtime.tar
runtime_image=$(python3 -c 'import json; print(json.load(open("release.json"))["image"]["id"])')
python3 examples/playground/playground.py up --image "$runtime_image"
```

Docker Engine, Python 3 and curl are required on the VM; Git is an external client.
The launcher does not compile the server or pull a mutable application tag.
See examples/playground/README.md for two-client push/clone/blame and restart use.
release.json binds distribution inputs; qualification evidence is delivered separately.
"""
    (directory / "README.md").write_text(instructions)
    paths.extend(["README.md", "git-rs-runtime.tar"])
    manifest = {"format": 1, "version": version, "source": {"repository": "https://github.com/borislitv/git-rs", "revision": revision},
                "image": {"id": image, "os": "linux", "architecture": "amd64"},
                "files": {relative: {"sha256": digest(directory / relative), "bytes": (directory / relative).stat().st_size}
                          for relative in sorted(paths)}}
    (directory / "release.json").write_text(json.dumps(manifest, indent=2) + "\n")
    paths.append("release.json")
    (directory / "SHA256SUMS").write_text("".join(f"{digest(directory / relative)}  {relative}\n" for relative in sorted(paths)))
    print(json.dumps({"directory": str(directory), "image": image, "revision": revision, "version": version}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    arguments = parser.parse_args()
    export(arguments.image, arguments.directory.resolve())


if __name__ == "__main__":
    main()
