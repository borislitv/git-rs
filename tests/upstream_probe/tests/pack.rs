#![forbid(unsafe_code)]

use git_rs_upstream_probe::pack_decoder::{Limits, Pack, PackEntryDecoder};
use gix_hash::Kind::Sha1;
use gix_pack::data::entry::Header;

#[path = "support/pack.rs"]
mod support;
use support::{BASE, Fixture, MID, NativeGit, Source, TOP};

const LIMITS: Limits = Limits {
    max_packs: 4,
    max_catalog_entries: 16,
    max_entry_bytes: 256,
    max_source_bytes: 1024,
    max_depth: 4,
    max_object_bytes: 64,
    max_expanded_bytes: 128,
    max_live_buffer_bytes: 512,
};

#[test]
fn native_full_ofs_and_forward_ref_chain() {
    let git = NativeGit::new();
    let (base, mid, top, empty) = (git.oid(BASE), git.oid(MID), git.oid(TOP), git.oid(b""));
    let mut fixture = Fixture::new(4);
    fixture.append_byte(Header::RefDelta { base_id: mid }, MID, TOP);
    let base_offset = fixture.entry(Header::Blob, BASE);
    fixture.append_byte(
        Header::OfsDelta {
            base_distance: fixture.offset() - base_offset,
        },
        BASE,
        MID,
    );
    fixture.entry(Header::Blob, b"");
    let stored = git.install(&fixture.finish(), false);
    assert_eq!(stored.entries[0].oid, top);
    assert_eq!(stored.entries[1].oid, base);
    assert_eq!(stored.entries[2].oid, mid);

    let catalog = [stored.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    for (id, depth) in [(base, 0), (mid, 1), (top, 2), (empty, 0)] {
        let mut source = Source::open(&[&stored]);
        let result = decoder.decode(id, &mut source).unwrap().unwrap();
        assert_eq!(result.kind, git.kind(id));
        assert_eq!(result.bytes, git.blob(id));
        assert_eq!(result.stats.depth, depth);
        assert_eq!(result.stats.source_bytes, source.requested_bytes());
        assert_eq!(result.stats.source_reads, source.reads.len());
        assert!(result.stats.peak_buffer_capacity <= LIMITS.max_live_buffer_bytes);
        if id == top {
            // Git's physical index identifies the unrelated empty entry. No
            // request for the mixed chain may include it or the pack trailer.
            assert!(
                source
                    .reads
                    .iter()
                    .all(|(p, r)| *p == 0 && r.end <= stored.entries[3].offset)
            );
            assert_eq!(
                result.stats.expanded_bytes,
                BASE.len() + MID.len() + TOP.len() + 12
            );
        }
    }
}

#[test]
fn native_ref_base_in_another_original_pack() {
    let git = NativeGit::new();
    let mut base_pack = Fixture::new(1);
    base_pack.entry(Header::Blob, BASE);
    let base_pack = git.install(&base_pack.finish(), false);
    let mid = git.oid(MID);
    let mut thin = Fixture::new(1);
    thin.append_byte(
        Header::RefDelta {
            base_id: git.oid(BASE),
        },
        BASE,
        MID,
    );
    let thin = thin.finish();
    let completed = git.install(&thin, true);
    let original = git.original_thin(&thin, &completed);
    assert_eq!(original.entries[0].oid, mid);

    let catalog = [original.catalog(), base_pack.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let mut source = Source::open(&[&original, &base_pack]);
    let result = decoder.decode(mid, &mut source).unwrap().unwrap();
    assert_eq!(result.kind, git.kind(mid));
    assert_eq!(result.bytes, git.blob(mid));
    assert_eq!(result.stats.depth, 1);
    assert!(source.reads.iter().any(|(p, _)| *p == 0));
    assert!(source.reads.iter().any(|(p, _)| *p == 1));

    let missing_base = [original.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &missing_base, LIMITS).unwrap();
    let mut source = Source::open(&[&original]);
    let error = decoder.decode(mid, &mut source).unwrap_err();
    assert!(error.is_corrupted() && error.to_string().contains("REF base missing"));
    assert_eq!(source.reads.len(), 1, "only the original delta probe");

    let mut base_entries = base_pack.entries.clone();
    base_entries[0].crc32 ^= 1;
    let unverified_base = [
        original.catalog(),
        Pack {
            entries: &base_entries,
            ..base_pack.catalog()
        },
    ];
    let decoder = PackEntryDecoder::new(Sha1, &unverified_base, LIMITS).unwrap();
    let mut source = Source::open(&[&original, &base_pack]);
    let error = decoder.decode(mid, &mut source).unwrap_err();
    assert!(error.is_corrupted() && error.to_string().contains("CRC mismatch"));
    assert_eq!(source.reads.iter().filter(|(p, _)| *p == 0).count(), 1);
}

#[test]
fn native_canonical_header_boundaries_and_original_ofs_location() {
    let git = NativeGit::new();
    let base = b"123456789abcdef";
    let result = b"123456789abcdef!";
    let mut fixture = Fixture::new(5);
    fixture.entry(Header::Blob, b"");
    let base_offset = fixture.entry(Header::Blob, base);
    let full = fixture.entry(Header::Blob, b"123456789abcdef?");
    fixture.entry(Header::Blob, &(0..=255).collect::<Vec<u8>>());
    let delta = fixture.offset();
    let distance = delta - base_offset;
    assert!((128..=16_511).contains(&distance), "two-byte OFS distance");
    fixture.append_byte(
        Header::OfsDelta {
            base_distance: distance,
        },
        base,
        result,
    );
    let stored = git.install(&fixture.finish(), false);
    let raw = std::fs::read(&stored.path).unwrap();
    let original_ofs = &raw[delta as usize..delta as usize + 3];
    assert_eq!(raw[base_offset as usize] & 0x80, 0); // size 15
    assert_ne!(raw[full as usize] & 0x80, 0); // size 16
    assert_ne!(original_ofs[1] & 0x80, 0);
    assert_eq!(original_ofs[2] & 0x80, 0);

    let catalog = [stored.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    for id in [git.oid(base), stored.entries[2].oid, git.oid(result)] {
        let decoded = decoder
            .decode(id, &mut Source::open(&[&stored]))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.bytes, git.blob(id));
    }

    // Only the first read sees the alternate header. The full read returns the
    // native-indexed bytes and passes the original CRC, but disagrees with the probe.
    let mut source = Source::open(&[&stored]);
    source.first_prefix_override = Some((
        0,
        delta,
        vec![original_ofs[0] | 0x80, 0, original_ofs[1], original_ofs[2]],
    ));
    let id = git.oid(result);
    let error = decoder.decode(id, &mut source).unwrap_err();
    assert!(error.is_corrupted() && error.to_string().contains("header changed after probe"));
    assert_eq!(
        source
            .reads
            .iter()
            .filter(|(_, r)| r.start == delta)
            .count(),
        2
    );

    // The OFS distance still names the original offset. An otherwise ordered
    // catalog that lost that exact boundary cannot substitute the next byte.
    let mut entries = stored.entries.clone();
    assert_eq!(entries[1].offset, base_offset);
    entries[1].offset += 1;
    let catalog = [Pack {
        entries: &entries,
        ..stored.catalog()
    }];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let mut source = Source::open(&[&stored]);
    let error = decoder.decode(id, &mut source).unwrap_err();
    assert!(
        error.is_corrupted()
            && error
                .to_string()
                .contains("OFS base is not an indexed entry")
    );
    assert_eq!(source.reads.len(), 1);
    assert_eq!(source.reads[0].1.start, delta);
}

#[test]
fn native_redundant_size_ofs_header_preserves_original_span() {
    let git = NativeGit::new();
    let mut fixture = Fixture::new(2);
    let base = fixture.entry(Header::Blob, BASE);
    let delta = fixture.offset();
    fixture.append_byte(
        Header::OfsDelta {
            base_distance: delta - base,
        },
        BASE,
        MID,
    );
    fixture.extend_size_encoding_at(delta);
    // Native `index-pack --strict` is the oracle for fixture acceptance and the
    // physical index (including its CRC); `cat-file` supplies the object bytes.
    let stored = git.install(&fixture.finish(), false);
    let id = git.oid(MID);
    assert_eq!(stored.entries[1].offset, delta);
    assert_eq!(stored.entries[1].oid, id);
    let catalog = [stored.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let result = decoder
        .decode(id, &mut Source::open(&[&stored]))
        .unwrap()
        .unwrap();
    assert_eq!(result.bytes, git.blob(id));
    assert_eq!(result.stats.depth, 1);
}

#[test]
fn ordinary_pack_detects_catalog_integrity_mismatches() {
    let git = NativeGit::new();
    let mut fixture = Fixture::new(2);
    fixture.entry(Header::Blob, BASE);
    fixture.entry(Header::Blob, MID);
    let stored = git.install(&fixture.finish(), false);

    let mut entries = stored.entries.clone();
    entries[0].offset += 1;
    let catalog = [Pack {
        entries: &entries,
        ..stored.catalog()
    }];
    assert!(matches!(PackEntryDecoder::new(Sha1, &catalog, LIMITS), Err(e) if e.is_corrupted()));

    for offset in [stored.entries[0].offset, stored.catalog().entries_end] {
        let mut entries = stored.entries.clone();
        entries[1].offset = offset;
        let catalog = [Pack {
            entries: &entries,
            ..stored.catalog()
        }];
        assert!(
            matches!(PackEntryDecoder::new(Sha1, &catalog, LIMITS), Err(e) if e.is_corrupted())
        );
    }

    for (change_oid, expected) in [(false, "CRC mismatch"), (true, "object ID mismatch")] {
        let mut entries = stored.entries.clone();
        if change_oid {
            entries[0].oid = git.oid(TOP);
        } else {
            entries[0].crc32 ^= 1;
        }
        let catalog = [Pack {
            entries: &entries,
            ..stored.catalog()
        }];
        let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
        let error = decoder
            .decode(entries[0].oid, &mut Source::open(&[&stored]))
            .unwrap_err();
        assert!(
            error.is_corrupted() && error.to_string().contains(expected),
            "{error:?}"
        );
    }

    // Keep the pack unchanged. A catalog boundary off by one, even with the
    // matching CRC for that range, cannot turn two spans into one entry.
    let mut entries = stored.entries.clone();
    entries[1].offset += 1;
    let raw = std::fs::read(&stored.path).unwrap();
    entries[0].crc32 =
        crc32fast::hash(&raw[entries[0].offset as usize..entries[1].offset as usize]);
    let catalog = [Pack {
        entries: &entries,
        ..stored.catalog()
    }];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let error = decoder
        .decode(entries[0].oid, &mut Source::open(&[&stored]))
        .unwrap_err();
    assert!(
        error.is_corrupted() && error.to_string().contains("exact original compressed span"),
        "{error:?}"
    );
}

#[test]
fn inconsistent_catalog_cycle_stops_at_original_header_probes() {
    let git = NativeGit::new();
    let (base, mid, top) = (git.oid(BASE), git.oid(MID), git.oid(TOP));
    let mut fixture = Fixture::new(3);
    fixture.append_byte(Header::RefDelta { base_id: mid }, MID, TOP);
    fixture.append_byte(Header::RefDelta { base_id: base }, BASE, MID);
    fixture.entry(Header::Blob, BASE);
    let stored = git.install(&fixture.finish(), false);
    assert_eq!(git.blob(top), TOP);
    let mut entries = stored.entries.clone();
    assert_eq!(
        [entries[0].oid, entries[1].oid, entries[2].oid],
        [top, mid, base]
    );

    // The physical bytes and CRCs are unchanged. Only the held identity map
    // lies: the second REF now resolves back to the first original location.
    entries[0].oid = base;
    entries[2].oid = top;
    let catalog = [Pack {
        entries: &entries,
        ..stored.catalog()
    }];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let mut source = Source::open(&[&stored]);
    let error = decoder.decode(base, &mut source).unwrap_err();
    assert!(
        error.is_corrupted()
            && error
                .to_string()
                .contains("original pack delta chain contains a cycle"),
        "{error:?}"
    );
    // Each tiny entry fits in a probe. Exactly two reads: no repeat of the
    // first location, no physical base read, and no full-read decode phase.
    assert_eq!(
        source.reads,
        [
            (0, entries[0].offset..entries[1].offset),
            (0, entries[1].offset..entries[2].offset),
        ]
    );
}

#[test]
fn stored_blob_rejects_short_read_and_framing_errors_with_matching_entry_crc() {
    use std::io::{self, Read};

    use git_rs_upstream_probe::pack_decoder::RangeSource;
    use gix_error::ResultExt;

    struct LocalBytes<'a> {
        bytes: &'a [u8],
        expected: std::ops::Range<usize>,
        reads: usize,
    }

    impl RangeSource for LocalBytes<'_> {
        fn read_exact(&mut self, pack: usize, offset: u64, out: &mut [u8]) -> gix_error::Result {
            let start = usize::try_from(offset).unwrap();
            assert_eq!(pack, 0);
            assert_eq!(start..start + out.len(), self.expected);
            self.reads += 1;
            assert!(self.reads <= 2);
            (&self.bytes[start..]).read_exact(out).or_error()
        }
    }

    let git = NativeGit::new();
    let mut fixture = Fixture::new(1);
    let start = fixture.entry(Header::Blob, BASE) as usize;
    let stored = git.install(&fixture.finish(), false);
    let end = stored.catalog().entries_end as usize;
    let raw = std::fs::read(&stored.path).unwrap();
    let catalog = [stored.catalog()];
    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let id = stored.entries[0].oid;

    // Storage ended one byte before the indexed entry end. Keep the original
    // catalog and require the source's short-read error, not a missing object.
    let mut source = LocalBytes {
        bytes: &raw[..end - 1],
        expected: start..end,
        reads: 0,
    };
    let error = decoder.decode(id, &mut source).unwrap_err();
    assert_eq!(
        error.downcast_any_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(source.reads, 1);

    let mut bad_checksum = raw[..end].to_vec();
    bad_checksum[end - 1] ^= 1; // One bit of the zlib Adler-32, not the compressed data.
    let mut wrong_size = raw[..end].to_vec();
    let written = Header::Blob
        .write_to(
            (BASE.len() + 1) as u64,
            &mut &mut wrong_size[start..start + 1],
        )
        .unwrap();
    assert_eq!(written, 1); // Same header width; zlib stream stays intact.

    for (bytes, expected_error) in [
        (bad_checksum, "Failed to decompress pack entry"),
        (wrong_size, "decompressed size does not match entry header"),
    ] {
        // Synthetic catalog: use the CRC of the received span to exercise the
        // independent zlib/declared-size checks, not the CRC check again.
        let mut entry = stored.entries[0];
        entry.crc32 = crc32fast::hash(&bytes[start..end]);
        let entries = [entry];
        let catalog = [Pack {
            entries: &entries,
            ..stored.catalog()
        }];
        let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
        let mut source = LocalBytes {
            bytes: &bytes,
            expected: start..end,
            reads: 0,
        };
        let error = decoder.decode(id, &mut source).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error:?}");
        assert_eq!(source.reads, 2);
    }
}

#[test]
fn ordinary_chain_respects_resource_limits_and_source_errors() {
    let git = NativeGit::new();
    let mid = git.oid(MID);
    let mut fixture = Fixture::new(3);
    let base = fixture.entry(Header::Blob, BASE);
    fixture.append_byte(
        Header::OfsDelta {
            base_distance: fixture.offset() - base,
        },
        BASE,
        MID,
    );
    fixture.append_byte(Header::RefDelta { base_id: mid }, MID, TOP);
    let stored = git.install(&fixture.finish(), false);
    let catalog = [stored.catalog()];
    let top = git.oid(TOP);
    for (limits, message) in [
        (
            Limits {
                max_depth: 1,
                ..LIMITS
            },
            "delta depth",
        ),
        (
            Limits {
                max_entry_bytes: 1,
                ..LIMITS
            },
            "entry range",
        ),
        (
            Limits {
                max_source_bytes: 1,
                ..LIMITS
            },
            "source read budget",
        ),
        (
            Limits {
                max_live_buffer_bytes: 1,
                ..LIMITS
            },
            "live buffer budget",
        ),
        (
            Limits {
                max_expanded_bytes: 30,
                ..LIMITS
            },
            "chain expansion",
        ),
        (
            Limits {
                max_object_bytes: MID.len(),
                ..LIMITS
            },
            "object size",
        ),
    ] {
        let decoder = PackEntryDecoder::new(Sha1, &catalog, limits).unwrap();
        let mut source = Source::open(&[&stored]);
        let error = decoder.decode(top, &mut source).unwrap_err();
        assert!(
            error.is_resource_exhausted() && error.to_string().contains(message),
            "{error:?}"
        );
        assert!(source.requested_bytes() <= limits.max_source_bytes);
    }

    let decoder = PackEntryDecoder::new(Sha1, &catalog, LIMITS).unwrap();
    let mut source = Source::open(&[&stored]);
    assert!(
        decoder
            .decode(git.oid(b"absent"), &mut source)
            .unwrap()
            .is_none()
    );
    assert!(source.reads.is_empty());
    source.fail_at = Some((0, base));
    let error = decoder.decode(top, &mut source).unwrap_err();
    assert_eq!(
        error.downcast_any_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::TimedOut
    );
}
