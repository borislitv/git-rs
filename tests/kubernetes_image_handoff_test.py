#!/usr/bin/env python3
"""Image handoff contracts with stateful Docker/containerd boundary fakes.

No daemon is contacted. The fake models Docker's active-image last-tag conflict
and containerd's named/digest import records, not tar encoding or CRI integration.
Pinned semantics: k3s-io/containerd v2.3.7-k3s1, images/import.go,
client/import.go and core/images/archive/reference.go. Real owned integration
is still required to qualify archive/platform/CRI behavior.
"""
import hashlib
import io
import json
import re
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import kubernetes
import runtime_process


REVISION = "a" * 40
ROLE = "external-client-oracle"
IMAGE = "sha256:" + "1" * 64
MANIFEST_BYTES = (
    b'{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",'
    b'"config":{"mediaType":"application/vnd.oci.image.config.v1+json",'
    b'"digest":"sha256:' + b"1" * 64 + b'","size":123},"layers":[]}'
)
MANIFEST = "sha256:" + hashlib.sha256(MANIFEST_BYTES).hexdigest()
FOREIGN = "sha256:" + "4" * 64
REPOSITORY = "git-rs.fixture.invalid/" + ROLE
OLD_ALIAS = REPOSITORY + ":" + REVISION + "-" + "1" * 64


class ExportPipe(io.BytesIO):
    def __init__(self, contents, close_error):
        super().__init__(contents)
        self.close_error = close_error

    def close(self):
        super().close()
        error, self.close_error = self.close_error, None
        if error is not None:
            raise error


class SavedImage:
    def __init__(self, names, exit_code, close_error=None, terminate_stalls=False):
        # Metadata at the subprocess boundary; deliberately not a fake tar parser.
        self.stdout = ExportPipe(json.dumps({"names": names}).encode(), close_error)
        self.returncode = None
        self.exit_code = exit_code
        self.terminate_stalls = terminate_stalls
        self.terminated = False

    def wait(self, timeout):
        if self.returncode is None and self.terminated and self.terminate_stalls:
            raise subprocess.TimeoutExpired("docker save", timeout)
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        if not self.terminate_stalls:
            self.returncode = -15

    def kill(self):
        self.returncode = -9


class ImageDaemons:
    """Small state model; unsupported external operations fail closed."""

    def __init__(self, existing_alias=None, manifest_bytes=MANIFEST_BYTES, docker_id=IMAGE):
        # Source identity is independent of returned archive content. Tampering
        # must never silently repin a manifest-backed Docker source image.
        self.docker_id = docker_id
        self.tags = {"example.invalid/unrelated:keep": FOREIGN}
        if existing_alias is not None:
            self.tags[OLD_ALIAS] = existing_alias
        self.containers = {"active-controller": docker_id, "unrelated-container": FOREIGN}
        self.labels = {"io.git-rs.role": ROLE,
                       "org.opencontainers.image.revision": REVISION}
        self.records = {}
        self.host_mutations = []
        self.exports = []
        self.import_error = False
        self.import_exception = None
        self.export_exit = 0
        self.close_error = None
        self.terminate_stalls = False
        self.cri_id = IMAGE
        self.cri_labels = dict(self.labels)
        self.manifest_bytes = manifest_bytes
        self.manifest_digest = "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()
        self.import_digests = [self.manifest_digest]
        self.manifest_type = "application/vnd.oci.image.manifest.v1+json"
        self.listed_digest = None
        self.named_imports = []

    def identity(self, reference):
        if reference in {self.docker_id, FOREIGN}:
            return reference
        return self.tags.get(reference)

    def result(self, arguments, payload=b"", code=0, ok=True):
        result = SimpleNamespace(returncode=code, stdout=payload, stderr=b"fake daemon failure")
        if code and ok:
            raise AssertionError(f"{arguments[:3]} failed ({code}): {result.stderr!r}")
        return result

    def command(self, arguments, *, ok=True, **_):
        if arguments[:3] == ["docker", "image", "inspect"]:
            identity = self.identity(arguments[3])
            payload = [{"Id": identity, "Config": {"Labels": self.labels},
                        "Os": "linux", "Architecture": "amd64"}] if identity else []
            return self.result(arguments, json.dumps(payload).encode(), 0 if identity else 1, ok)
        if arguments[:2] == ["docker", "tag"]:
            identity = self.identity(arguments[2])
            assert identity is not None, "cannot tag an absent image"
            self.host_mutations.append(tuple(arguments))
            self.tags[arguments[3]] = identity
            return self.result(arguments)
        if arguments[:3] == ["docker", "image", "rm"]:
            assert len(arguments) == 4, "forced image deletion is not an allowed handoff"
            alias = arguments[3]
            identity = self.tags[alias]
            self.host_mutations.append(tuple(arguments))
            last_tag = list(self.tags.values()).count(identity) == 1
            if last_tag and identity in self.containers.values():
                raise AssertionError(
                    "conflict: container active-controller is using its referenced image")
            del self.tags[alias]
            return self.result(arguments)
        assert arguments[:3] == ["docker", "exec", "owned-cluster"], arguments
        operation = arguments[3:]
        if operation[:3] == ["ctr", "images", "list"]:
            rows = ["REF TYPE DIGEST SIZE PLATFORMS LABELS"]
            for name, digest in self.records.items():
                selected = True
                for expression in operation[3:]:
                    if expression.startswith("name=="):
                        selected &= name == expression[6:]
                    elif expression.startswith("name~="):
                        selected &= re.search(expression[6:].strip('"'), name) is not None
                    else:
                        raise AssertionError(f"unsupported image filter: {expression}")
                if selected:
                    rows.append(f"{name} {self.manifest_type} {self.listed_digest or digest} 1B linux/amd64 -")
            return self.result(arguments, "\n".join(rows).encode())
        if operation[:3] == ["ctr", "images", "tag"]:
            self.records[operation[4]] = self.records[operation[3]]
            return self.result(arguments)
        if operation[:3] == ["ctr", "content", "get"]:
            assert len(operation) == 4 and operation[3] == self.manifest_digest, "manifest content missing"
            return self.result(arguments, self.manifest_bytes)
        if operation[:2] == ["crictl", "inspecti"]:
            assert operation[2] in self.records, "CRI reference was not imported"
            payload = {"status": {"id": self.cri_id},
                       "info": {"imageSpec": {"config": {"Labels": self.cri_labels}}}}
            return self.result(arguments, json.dumps(payload).encode())
        raise AssertionError(f"unsupported daemon command: {arguments}")

    def save(self, arguments, **_):
        assert arguments[:2] == ["docker", "save"] and len(arguments) == 3, arguments
        assert self.identity(arguments[2]) == self.docker_id, "export must resolve the selected image"
        names = [arguments[2]] if arguments[2] in self.tags else []
        process = SavedImage(names, self.export_exit, self.close_error, self.terminate_stalls)
        self.exports.append(process)
        return process

    def import_stream(self, arguments, *, stdin, **_):
        assert arguments[:7] == ["docker", "exec", "-i", "owned-cluster",
                                 "ctr", "images", "import"], arguments
        assert arguments[-1] == "-", "import must consume the owned export stream"
        options = arguments[7:-1]
        prefix, digests = None, False
        while options:
            option, *options = options
            if option == "--base-name":
                prefix, *options = options
            elif option == "--digests":
                digests = True
            elif option != "--local":
                raise AssertionError(f"unsupported import option: {option}")
        archive = json.loads(stdin.read())
        if self.import_exception is not None:
            raise self.import_exception
        if self.import_error:
            return SimpleNamespace(returncode=1, stderr=b"simulated import failure")
        for name in archive["names"]:
            self.records[name] = self.manifest_digest
            self.named_imports.append(name)
        if digests:
            assert prefix, "digest imports must use an explicit owned repository"
            for digest in self.import_digests:
                self.records[prefix + "@" + digest] = digest
        return SimpleNamespace(returncode=0, stderr=b"")


class ImageHandoffContracts(unittest.TestCase):
    def bind(self, daemons):
        with patch.object(kubernetes, "command", side_effect=daemons.command), \
                patch.object(runtime_process, "run", side_effect=daemons.command), \
                patch.object(kubernetes.subprocess, "Popen", side_effect=daemons.save), \
                patch.object(kubernetes.subprocess, "run", side_effect=daemons.import_stream):
            return kubernetes.bound_image("owned-cluster", daemons.docker_id, REVISION, ROLE)

    def assert_host_preserved(self, daemons, tags, containers):
        self.assertEqual(daemons.tags, tags)
        self.assertEqual(daemons.containers, containers)
        self.assertEqual(daemons.host_mutations, [], "handoff must not mutate host image aliases")
        self.assertTrue(daemons.exports, "the selected image must actually be exported")
        for process in daemons.exports:
            self.assertTrue(process.stdout.closed, "export pipe must close on every exit path")
            self.assertIsNotNone(process.returncode, "export process must be reaped")

    def test_untagged_image_with_active_controller_imports_without_host_mutation(self):
        daemons = ImageDaemons()
        tags, containers = dict(daemons.tags), dict(daemons.containers)
        self.assertNotIn(IMAGE, tags.values(), "regression fixture must initially be untagged")
        result = self.bind(daemons)
        self.assertEqual(result["docker_id"], IMAGE)
        self.assertEqual(result["cri_id"], IMAGE)
        self.assertTrue(result["reference"].startswith(REPOSITORY))
        self.assertTrue(result["reference"].endswith("@" + MANIFEST))
        self.assertEqual(daemons.records[result["reference"]], MANIFEST)
        self.assert_host_preserved(daemons, tags, containers)

    def test_preexisting_matching_or_foreign_host_alias_is_preserved(self):
        for owner in (IMAGE, FOREIGN):
            with self.subTest(alias_identity=owner):
                daemons = ImageDaemons(existing_alias=owner)
                tags, containers = dict(daemons.tags), dict(daemons.containers)
                result = self.bind(daemons)
                self.assertEqual(result["cri_id"], IMAGE)
                self.assert_host_preserved(daemons, tags, containers)

    def test_import_failure_preserves_original_error_and_reaps_export(self):
        daemons = ImageDaemons()
        daemons.import_error = True
        tags, containers = dict(daemons.tags), dict(daemons.containers)
        with self.assertRaisesRegex(AssertionError, "simulated import failure"):
            self.bind(daemons)
        self.assertEqual(daemons.records, {})
        self.assert_host_preserved(daemons, tags, containers)

    def test_export_failure_is_not_accepted_and_preserves_host_state(self):
        daemons = ImageDaemons()
        daemons.export_exit = 1
        tags, containers = dict(daemons.tags), dict(daemons.containers)
        with self.assertRaisesRegex(AssertionError, "Docker image export failed"):
            self.bind(daemons)
        self.assert_host_preserved(daemons, tags, containers)

    def test_import_timeout_or_cancellation_reaps_export_without_replacing_cause(self):
        for cause in (subprocess.TimeoutExpired("ctr images import", 120),
                      KeyboardInterrupt("cancelled import")):
            with self.subTest(cause=type(cause).__name__):
                daemons = ImageDaemons()
                daemons.import_exception = cause
                daemons.terminate_stalls = True
                tags, containers = dict(daemons.tags), dict(daemons.containers)
                with self.assertRaises(type(cause)) as raised:
                    self.bind(daemons)
                self.assertIs(raised.exception, cause)
                self.assertEqual(daemons.exports[0].returncode, -9)
                self.assert_host_preserved(daemons, tags, containers)

    def test_pipe_close_failure_does_not_mask_import_failure_or_skip_reaping(self):
        daemons = ImageDaemons()
        daemons.import_error = True
        daemons.close_error = OSError("simulated pipe close failure")
        tags, containers = dict(daemons.tags), dict(daemons.containers)
        with self.assertRaisesRegex(AssertionError, "simulated import failure") as raised:
            self.bind(daemons)
        self.assertTrue(any("simulated pipe close failure" in note
                            for note in raised.exception.__notes__))
        self.assert_host_preserved(daemons, tags, containers)

    def test_cleanup_failure_after_successful_import_is_not_accepted(self):
        daemons = ImageDaemons()
        daemons.close_error = OSError("simulated pipe close failure")
        tags, containers = dict(daemons.tags), dict(daemons.containers)
        with self.assertRaisesRegex(OSError, "simulated pipe close failure"):
            self.bind(daemons)
        self.assert_host_preserved(daemons, tags, containers)

    def test_legacy_named_import_caller_still_imports_its_original_reference(self):
        daemons = ImageDaemons(existing_alias=IMAGE)
        with patch.object(kubernetes.subprocess, "Popen", side_effect=daemons.save), \
                patch.object(kubernetes.subprocess, "run", side_effect=daemons.import_stream):
            kubernetes.import_image("owned-cluster", OLD_ALIAS)
        self.assertEqual(daemons.records, {OLD_ALIAS: MANIFEST})
        self.assertEqual(daemons.named_imports, [OLD_ALIAS])
        self.assertTrue(daemons.exports[0].stdout.closed)
        self.assertEqual(daemons.exports[0].returncode, 0)

    def test_missing_or_multiple_digest_candidates_are_rejected(self):
        for candidates in ([], [MANIFEST, "sha256:" + "5" * 64]):
            with self.subTest(candidates=candidates):
                daemons = ImageDaemons()
                daemons.import_digests = candidates
                with self.assertRaisesRegex(AssertionError, "exactly one imported image manifest"):
                    self.bind(daemons)

    def test_archive_index_is_not_guessed_to_be_an_image_manifest(self):
        daemons = ImageDaemons()
        daemons.manifest_type = "application/vnd.oci.image.index.v1+json"
        with self.assertRaisesRegex(AssertionError, "unsupported imported manifest type"):
            self.bind(daemons)

    def test_digest_reference_must_match_the_stored_manifest_target(self):
        daemons = ImageDaemons()
        daemons.listed_digest = "sha256:" + "5" * 64
        with self.assertRaisesRegex(AssertionError, "reference differs from its manifest digest"):
            self.bind(daemons)

    def test_config_or_manifest_cri_id_requires_the_same_proven_config(self):
        for docker_id, cri_id in ((IMAGE, IMAGE), (IMAGE, MANIFEST),
                                  (MANIFEST, IMAGE), (MANIFEST, MANIFEST)):
            with self.subTest(docker_id=docker_id, cri_id=cri_id):
                daemons = ImageDaemons(docker_id=docker_id)
                daemons.cri_id = cri_id
                tags, containers = dict(daemons.tags), dict(daemons.containers)
                self.assertNotIn(docker_id, tags.values(), "selected source must be untagged")
                result = self.bind(daemons)
                self.assertEqual(result["docker_id"], docker_id)
                self.assertEqual(result["cri_id"], cri_id)
                self.assertTrue(result["reference"].endswith("@" + MANIFEST))
                self.assert_host_preserved(daemons, tags, containers)

    def test_same_labels_and_valid_manifest_hash_do_not_authorize_a_different_config(self):
        manifest = json.loads(MANIFEST_BYTES)
        manifest["config"]["digest"] = FOREIGN
        for docker_id in (IMAGE, MANIFEST):
            for id_form in ("config", "manifest"):
                with self.subTest(docker_id=docker_id, id_form=id_form):
                    daemons = ImageDaemons(manifest_bytes=json.dumps(manifest).encode(),
                                           docker_id=docker_id)
                    # In the modern case Docker remains pinned to ORIGINAL
                    # MANIFEST, not the hash of the changed manifest below.
                    self.assertNotEqual(daemons.manifest_digest, MANIFEST)
                    daemons.cri_id = FOREIGN if id_form == "config" else daemons.manifest_digest
                    with self.assertRaisesRegex(AssertionError, "not linked to Docker identity"):
                        self.bind(daemons)

    def test_unrelated_cri_identity_is_rejected_with_observed_ids(self):
        for docker_id in (IMAGE, MANIFEST):
            for unsupported in (FOREIGN, "containerd://" + IMAGE, REPOSITORY + "@" + MANIFEST):
                with self.subTest(docker_id=docker_id, cri_id=unsupported):
                    daemons = ImageDaemons(docker_id=docker_id)
                    daemons.cri_id = unsupported
                    with self.assertRaisesRegex(AssertionError, "unsupported imported CRI identity") as raised:
                        self.bind(daemons)
                    message = str(raised.exception)
                    self.assertIn("cri_id=" + unsupported, message)
                    self.assertIn("docker_id=" + docker_id, message)
                    self.assertIn("config_digest=" + IMAGE, message)
                    self.assertIn("manifest_digest=" + MANIFEST, message)

    def test_matching_labels_do_not_authorize_an_unlinked_docker_identity(self):
        daemons = ImageDaemons(docker_id="sha256:" + "6" * 64)
        daemons.cri_id = MANIFEST
        with self.assertRaisesRegex(AssertionError, "not linked to Docker identity"):
            self.bind(daemons)

    def test_manifest_config_digest_must_be_a_valid_sha256_identity(self):
        for invalid in (None, "sha256:short", [IMAGE], "sha512:" + "1" * 128):
            with self.subTest(config_digest=invalid):
                manifest = json.loads(MANIFEST_BYTES)
                manifest["config"]["digest"] = invalid
                daemons = ImageDaemons(manifest_bytes=json.dumps(manifest).encode())
                with self.assertRaisesRegex(AssertionError, "invalid imported config digest"):
                    self.bind(daemons)

    def test_raw_manifest_bytes_must_match_the_selected_digest(self):
        daemons = ImageDaemons()
        # The name/target still identify the original bytes; the content read
        # is altered. Labels and both reported identities otherwise agree.
        daemons.manifest_bytes = MANIFEST_BYTES + b" "
        with self.assertRaisesRegex(AssertionError, "manifest content digest differs"):
            self.bind(daemons)

    def test_hash_matching_malformed_json_or_schema_is_rejected(self):
        fixtures = [
            (b"{", "invalid imported image manifest JSON"),
            (b"[]", "invalid imported schema-2 image manifest"),
            (b'{"schemaVersion":1,"config":{},"layers":[]}', "invalid imported schema-2 image manifest"),
            (b'{"schemaVersion":2,"config":[],"layers":[]}', "invalid imported schema-2 image manifest"),
            (b'{"schemaVersion":2,"config":{}}', "invalid imported schema-2 image manifest"),
            (b'{"schemaVersion":2,"manifests":[],"config":{},"layers":[]}',
             "invalid imported schema-2 image manifest"),
        ]
        for contents, diagnostic in fixtures:
            with self.subTest(contents=contents):
                daemons = ImageDaemons(manifest_bytes=contents)
                daemons.cri_id = daemons.manifest_digest
                with self.assertRaisesRegex(AssertionError, diagnostic):
                    self.bind(daemons)

    def test_manifest_body_media_type_must_match_its_selected_descriptor(self):
        manifest = json.loads(MANIFEST_BYTES)
        manifest["mediaType"] = "application/vnd.oci.image.index.v1+json"
        daemons = ImageDaemons(manifest_bytes=json.dumps(manifest).encode())
        with self.assertRaisesRegex(AssertionError, "manifest media type differs"):
            self.bind(daemons)

    def test_imported_role_and_revision_must_still_match(self):
        for cri_id in (IMAGE, MANIFEST):
            for key, value in (("io.git-rs.role", "runtime"),
                               ("org.opencontainers.image.revision", "b" * 40)):
                with self.subTest(cri_id=cri_id, label=key):
                    daemons = ImageDaemons(existing_alias=IMAGE)
                    daemons.cri_id = cri_id
                    daemons.cri_labels[key] = value
                    with self.assertRaises(AssertionError):
                        self.bind(daemons)


if __name__ == "__main__":
    unittest.main()
