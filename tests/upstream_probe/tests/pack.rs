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
