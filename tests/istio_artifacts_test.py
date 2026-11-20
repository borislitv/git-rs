#!/usr/bin/env python3
"""Prepared-input integrity and bounded chart extraction, without network or Docker."""
import gzip
import hashlib
import io
import os
from contextlib import nullcontext
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import istio_artifacts as inputs


class ArtifactContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cache = self.root / "inputs"
        self.cache.mkdir(mode=0o700)
        self.prefix = "istio-" + inputs.ISTIO + "/manifests/charts/"
        self.charts = [(self.prefix + "base/Chart.yaml", b"name: base\n"),
                       (self.prefix + "istio-control/istio-discovery/Chart.yaml", b"name: istiod\n")]

    def seed(self, entries):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w:gz") as output:
            for name, body in entries:
                if isinstance(body, tarfile.TarInfo):
                    output.addfile(body)
                else:
                    member = name if isinstance(name, tarfile.TarInfo) else tarfile.TarInfo(name)
                    member.size = len(body)
                    output.addfile(member, io.BytesIO(body))
        files = {"istio-" + inputs.ISTIO + "-linux-amd64.tar.gz": archive.getvalue(),
                 "gateway-api-" + inputs.GATEWAY_API + ".yaml": b"kind: CustomResourceDefinition\n"}
        for name, body in files.items():
            (self.cache / name).write_bytes(body)
        return {name: ("https://artifacts.example.invalid/" + name, hashlib.sha256(body).hexdigest())
                for name, body in files.items()}

    def test_verified_inputs_never_download_or_extract_the_bundled_executable(self):
        records = self.seed([*self.charts, ("istio-" + inputs.ISTIO + "/bin/istioctl", b"not-a-chart")])
        with patch.object(inputs, "ARTIFACTS", records), \
                patch.object(inputs.urllib.request, "urlopen", side_effect=AssertionError("unexpected network")):
            inputs.prepare_artifacts(self.cache)
            distribution = inputs.artifacts(self.cache, self.root / "selected")
            self.assertEqual(inputs.verify_artifacts(self.cache), {name: record[1] for name, record in records.items()})
        self.assertEqual((distribution / "manifests/charts/base/Chart.yaml").read_bytes(), b"name: base\n")
        self.assertEqual((distribution / "manifests/charts/istio-control/istio-discovery/Chart.yaml").read_bytes(), b"name: istiod\n")
        self.assertFalse((distribution / "bin").exists())
        self.assertEqual((self.root / "selected").stat().st_mode & 0o777, 0o700)

    def test_changed_input_fails_before_extraction_or_network_access(self):
        records = self.seed(self.charts)
        (self.cache / ("gateway-api-" + inputs.GATEWAY_API + ".yaml")).write_bytes(b"changed")
        destination = self.root / "selected"
        with patch.object(inputs, "ARTIFACTS", records), \
                patch.object(inputs.urllib.request, "urlopen", side_effect=AssertionError("unexpected network")):
            with self.assertRaisesRegex(AssertionError, "checksum"):
                inputs.artifacts(self.cache, destination)
        self.assertFalse(destination.exists())

    def test_links_special_files_and_oversized_inputs_are_not_read_as_artifacts(self):
        target = self.root / "target"
        target.write_bytes(b"bytes")
        link = self.root / "link"
        link.symlink_to(target)
        with self.assertRaises(OSError):
            inputs.digest(link)
        pipe = self.root / "pipe"
        os.mkfifo(pipe)
        with self.assertRaisesRegex(AssertionError, "bounded regular"):
            inputs.digest(pipe)
        with patch.object(inputs, "MAX_BYTES", 4):
            with self.assertRaisesRegex(AssertionError, "bounded regular"):
                inputs.digest(target)

    def test_consumed_manifest_bytes_are_pinned_bounded_regular_inputs(self):
        records = self.seed(self.charts)
        name = "gateway-api-" + inputs.GATEWAY_API + ".yaml"
        with patch.object(inputs, "ARTIFACTS", records):
            self.assertEqual(inputs.read_artifact(self.cache, name), b"kind: CustomResourceDefinition\n")
            (self.cache / name).write_bytes(b"changed after preflight\n")
            with self.assertRaisesRegex(AssertionError, "checksum"):
                inputs.read_artifact(self.cache, name)
            with patch.object(inputs, "MAX_BYTES", 4):
                with self.assertRaisesRegex(AssertionError, "bounded regular"):
                    inputs.read_artifact(self.cache, name)
            (self.cache / name).unlink()
            os.mkfifo(self.cache / name)
            with self.assertRaisesRegex(AssertionError, "bounded regular"):
                inputs.read_artifact(self.cache, name)

    def test_archive_escape_links_and_expansion_limits_fail_closed(self):
        link = tarfile.TarInfo(self.prefix + "link")
        link.type, link.linkname = tarfile.SYMTYPE, "../../../../outside"
        scenarios = [([*self.charts, (self.prefix + "../../outside", b"escape")], {}, "unsafe"),
                     ([*self.charts, (link.name, link)], {}, "linked or special"),
                     (self.charts, {"MAX_EXPANDED_BYTES": 1}, "expansion bound"),
                     (self.charts, {"MAX_MEMBERS": 1}, "expansion bound")]
        for ordinal, (entries, bounds, message) in enumerate(scenarios):
            with self.subTest(ordinal=ordinal):
                records = self.seed(entries)
                with patch.object(inputs, "ARTIFACTS", records), (patch.multiple(inputs, **bounds) if bounds else nullcontext()):
                    with self.assertRaisesRegex(AssertionError, message):
                        inputs.artifacts(self.cache, self.root / ("selected-" + str(ordinal)))
                self.assertFalse((self.root / "outside").exists())

    def test_hidden_pax_headers_are_counted_before_their_payload_is_parsed(self):
        member = tarfile.TarInfo(self.charts[0][0])
        member.pax_headers = {"comment": "x" * 4096}
        records = self.seed([(member.name, member), self.charts[1]])
        scenarios = [({"MAX_METADATA_BYTES": 1024}, "metadata exceeded its byte bound"),
                     ({"MAX_MEMBERS": 2}, "expansion bound")]
        for ordinal, (bounds, message) in enumerate(scenarios):
            with self.subTest(bounds=bounds), patch.object(inputs, "ARTIFACTS", records), patch.multiple(inputs, **bounds):
                with self.assertRaisesRegex(AssertionError, message):
                    inputs.artifacts(self.cache, self.root / ("pax-" + str(ordinal)))

    def test_extended_header_recursion_has_a_separate_bound(self):
        headers = []
        for ordinal in range(3):
            member = tarfile.TarInfo("metadata-" + str(ordinal))
            member.type = tarfile.XHDTYPE
            headers.append((member.name, member))
        records = self.seed([*headers, *self.charts])
        with patch.object(inputs, "ARTIFACTS", records), patch.object(inputs, "MAX_METADATA_DEPTH", 2):
            with self.assertRaisesRegex(AssertionError, "metadata exceeded its nesting bound"):
                inputs.artifacts(self.cache, self.root / "nested")

    def test_sparse_metadata_is_rejected_even_for_unselected_members(self):
        member = tarfile.TarInfo("outside-charts-sparse")
        member.pax_headers = {"GNU.sparse.major": "1", "GNU.sparse.minor": "0", "GNU.sparse.realsize": "0"}
        records = self.seed([*self.charts, (member, b"0\n".ljust(512, b"\0"))])
        with patch.object(inputs, "ARTIFACTS", records):
            with self.assertRaisesRegex(AssertionError, "linked or special"):
                inputs.artifacts(self.cache, self.root / "sparse")

    def test_global_pax_metadata_cannot_accumulate_across_bounded_headers(self):
        records = self.seed(self.charts)
        name = "istio-" + inputs.ISTIO + "-linux-amd64.tar.gz"
        archive = self.cache / name
        headers = b"".join(tarfile.TarInfo.create_pax_global_header({key: "x" * 600}) for key in ("one", "two"))
        body = gzip.compress(headers + gzip.decompress(archive.read_bytes()))
        archive.write_bytes(body)
        records[name] = (records[name][0], hashlib.sha256(body).hexdigest())
        with patch.object(inputs, "ARTIFACTS", records), patch.object(inputs, "MAX_METADATA_BYTES", 1024):
            with self.assertRaisesRegex(AssertionError, "global archive metadata exceeded"):
                inputs.artifacts(self.cache, self.root / "global-pax")

    def test_expansion_bound_includes_tar_headers_and_end_padding(self):
        records = self.seed(self.charts)
        with patch.object(inputs, "ARTIFACTS", records), patch.object(inputs, "MAX_EXPANDED_BYTES", 4096):
            with self.assertRaisesRegex(AssertionError, "expansion bound"):
                inputs.artifacts(self.cache, self.root / "padding")

    def test_duplicate_chart_files_cannot_replace_an_earlier_member(self):
        records = self.seed([*self.charts, (self.charts[0][0], b"replacement\n")])
        with patch.object(inputs, "ARTIFACTS", records):
            with self.assertRaisesRegex(AssertionError, "duplicate Istio chart file"):
                inputs.artifacts(self.cache, self.root / "duplicates")

    def test_extraction_verifies_the_bytes_read_after_input_preflight(self):
        records = self.seed(self.charts)
        archive = self.cache / ("istio-" + inputs.ISTIO + "-linux-amd64.tar.gz")
        original = archive.read_bytes()
        self.seed([(self.charts[0][0], b"changed after verification\n"), self.charts[1]])
        changed = archive.read_bytes()
        archive.write_bytes(original)
        verify = inputs.verify_artifacts

        def replace_after_verification(cache):
            result = verify(cache)
            archive.write_bytes(changed)
            return result

        with patch.object(inputs, "ARTIFACTS", records), \
                patch.object(inputs, "verify_artifacts", side_effect=replace_after_verification):
            with self.assertRaisesRegex(AssertionError, "checksum"):
                inputs.artifacts(self.cache, self.root / "replaced")

    def test_a_failed_download_never_publishes_partial_or_unpinned_bytes(self):
        records = {"input": ("https://artifacts.example.invalid/input", hashlib.sha256(b"expected").hexdigest())}
        with patch.object(inputs, "ARTIFACTS", records), \
                patch.object(inputs.urllib.request, "urlopen", return_value=io.BytesIO(b"truncated")):
            with self.assertRaisesRegex(AssertionError, "checksum"):
                inputs.prepare_artifacts(self.cache)
        self.assertEqual(list(self.cache.iterdir()), [])

    def test_preparation_publishes_only_verified_readable_bytes(self):
        body = b"pinned public input\n"
        records = {"input": ("https://artifacts.example.invalid/input", hashlib.sha256(body).hexdigest())}
        with patch.object(inputs, "ARTIFACTS", records), \
                patch.object(inputs.urllib.request, "urlopen", return_value=io.BytesIO(body)):
            inputs.prepare_artifacts(self.cache)
        self.assertEqual([path.name for path in self.cache.iterdir()], ["input"])
        self.assertEqual((self.cache / "input").read_bytes(), body)
        self.assertEqual((self.cache / "input").stat().st_mode & 0o777, 0o644)


if __name__ == "__main__":
    unittest.main()
