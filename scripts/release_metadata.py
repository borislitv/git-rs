#!/usr/bin/env python3
"""Write public release checksums and immutable OCI references after publication."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--chart-digest", required=True)
    args = parser.parse_args()
    for digest in [args.image_digest, args.chart_digest]:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            parser.error("expected a SHA-256 OCI manifest digest")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository):
        parser.error("invalid GitHub owner/repository")
    if not re.fullmatch(r"[a-f0-9]{40}", args.commit):
        parser.error("expected a complete Git commit ID")
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    package = args.directory / f"git-rs-{version}.tgz"
    image = "ghcr.io/" + args.repository.lower()
    chart = "oci://ghcr.io/" + args.repository.split("/")[0].lower() + "/charts/git-rs"
    metadata = {
        "version": version,
        "git_commit": args.commit,
        "image": {"tag": image + ":" + version, "reference": image + "@" + args.image_digest,
                  "platform": "linux/amd64", "runtime_git": "2.53.0"},
        "chart": {"reference": chart, "version": version, "digest": args.chart_digest,
                  "package": package.name, "sha256": hashlib.sha256(package.read_bytes()).hexdigest()},
        "verification": "Rust, native Git/SSH, PostgreSQL/S3 failure contracts, operator CRD schemas, Helm install/upgrade/restart",
        "limitations": "Experimental prerelease; local tests are not production CNPG/Ceph HA certification.",
    }
    report = args.directory / "release.json"
    report.write_text(json.dumps(metadata, indent=2) + "\n")
    (args.directory / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in [package, report]))
    (args.directory / "NOTES.md").write_text(
        f"## git-rs {version}\n\n"
        "Experimental stateless Git backend in safe Rust. PostgreSQL owns refs/catalog; "
        "HTTPS S3 owns immutable native packs. The independent Helm chart has no dependencies "
        "and directly renders optional CNPG/Rook/Barman resources.\n\n"
        f"- Image: `{image}@{args.image_digest}` (Linux amd64; tag `{version}`).\n"
        f"- Chart: `{chart}`, version `{version}`, manifest `{args.chart_digest}`.\n"
        f"- Source commit: `{args.commit}`.\n"
        "- Attached chart archive, SHA256SUMS and release.json record exact published references.\n\n"
        "```sh\n"
        f"helm pull {chart} --version {version}\n"
        f"docker pull {image}@{args.image_digest}\n"
        "```\n\n"
        "Verification covers native Git/SSH, generated TLS dependencies, cold restart, concurrent "
        "publication, transport faults and Helm install/upgrade. Production CNPG failover, Ceph "
        "durability and coordinated PITR remain unverified. Read OPERATIONS.md before storing "
        "important data.\n")


if __name__ == "__main__":
    main()
