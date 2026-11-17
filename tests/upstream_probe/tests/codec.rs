#![forbid(unsafe_code)]

use git_rs_upstream_probe::artifact_codec::{
    Encoding, Error, Limits, MAX_BYTES, Metadata, decode, encode,
};
use sha2::{Digest, Sha256};
use zstd::zstd_safe::{self, CCtx, CParameter};

const LIMITS: Limits = Limits {
    stored: MAX_BYTES,
    logical: MAX_BYTES,
};
const FIXTURE_LEN: usize = 192 * 1024;
const FIXTURE_LOGICAL_SHA: &str =
    "ac7f5c23a86716e8e242f4417bf4329654e94796b373687927b090f272f0ba92";
// CPython 3.14.4 compression.zstd (libzstd 1.5.7), one-shot compression of
// b"1" * FIXTURE_LEN with level 3, window log 20, checksum and content size,
// and zero workers. This is a neutral fixture, NOT output of the old app writer.
const FIXTURE: &str = "28b52ffda4000003005400001031310100fbff39c0020300083102c718ca";
const FIXTURE_STORED_SHA: &str = "458e57a428089ca9b190546fac22dacbcc56115e9c670539579cd8a8751bc20c";
const EMPTY_FRAME: &str = "28b52ffd240001000099e9d851";

fn hex(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn fixed_metadata(stored: &[u8], stored_sha: &str) -> Metadata {
    Metadata {
        encoding: Encoding::ZstdV1,
        stored_size: stored.len(),
        logical_size: FIXTURE_LEN,
        stored_digest: hex(stored_sha).try_into().unwrap(),
        logical_digest: hex(FIXTURE_LOGICAL_SHA).try_into().unwrap(),
    }
}

fn metadata(stored: &[u8], logical: &[u8]) -> Metadata {
    Metadata {
        encoding: Encoding::ZstdV1,
        stored_size: stored.len(),
        logical_size: logical.len(),
        stored_digest: Sha256::digest(stored).into(),
        logical_digest: Sha256::digest(logical).into(),
    }
}

fn one_shot(logical: &[u8], checksum: bool, content_size: bool, window: u32) -> Vec<u8> {
    let mut ctx = CCtx::try_create().unwrap();
    for param in [
        CParameter::CompressionLevel(3),
        CParameter::WindowLog(window),
        CParameter::ChecksumFlag(checksum),
        CParameter::ContentSizeFlag(content_size),
    ] {
        ctx.set_parameter(param).unwrap();
    }
    let mut bytes = vec![0; zstd_safe::compress_bound(logical.len())];
    let size = ctx.compress2(&mut bytes[..], logical).unwrap();
    bytes.truncate(size);
    bytes
}

#[test]
fn reads_frozen_neutral_frame_at_exact_limits() {
    let stored = hex(FIXTURE);
    let meta = fixed_metadata(&stored, FIXTURE_STORED_SHA);
    let limits = Limits {
        stored: stored.len(),
        logical: FIXTURE_LEN,
    };
    assert_eq!(
        decode(&stored, &meta, limits).unwrap(),
        vec![b'1'; FIXTURE_LEN]
    );
}

#[test]
fn raw_dispatch_uses_metadata_and_borrows_on_write() {
    let frame = hex(FIXTURE);
    for logical in [
        b"".as_slice(),
        b"\x28\xb5\x2f\xfd\0\xff",
        b"\x50\x2a\x4d\x18raw",
        frame.as_slice(),
    ] {
        let encoded = encode(logical, LIMITS).unwrap();
        assert_eq!(encoded.metadata.encoding, Encoding::RawV1);
        assert!(matches!(&encoded.stored, std::borrow::Cow::Borrowed(_)));
        assert_eq!(encoded.stored.as_ptr(), logical.as_ptr());
        assert_eq!(decode(logical, &encoded.metadata, LIMITS).unwrap(), logical);
        let mut bad = encoded.metadata;
        bad.logical_digest[0] ^= 1;
        assert_eq!(decode(logical, &bad, LIMITS), Err(Error::RawMetadata));
    }
}

#[test]
fn writer_preserves_threshold_savings_and_checksum() {
    let below = vec![7; 4095];
    assert_eq!(
        encode(&below, LIMITS).unwrap().metadata.encoding,
        Encoding::RawV1
    );
    let exact = vec![7; 4096];
    assert_eq!(
        encode(&exact, LIMITS).unwrap().metadata.encoding,
        Encoding::ZstdV1
    );

    let logical = vec![0x55; MAX_BYTES];
    let encoded = encode(&logical, LIMITS).unwrap();
    assert_eq!(encoded.metadata.encoding, Encoding::ZstdV1);
    assert!(encoded.stored.len() <= logical.len() - logical.len().div_ceil(16));
    let mut independent = vec![0; logical.len()];
    let size = zstd_safe::decompress(&mut independent[..], &encoded.stored).unwrap();
    assert_eq!(size, logical.len());
    assert_eq!(independent, logical);
    assert_eq!(
        decode(&encoded.stored, &encoded.metadata, LIMITS).unwrap(),
        logical
    );

    let mut corrupt = encoded.stored.into_owned();
    *corrupt.last_mut().unwrap() ^= 1;
    let repaired = metadata(&corrupt, &logical);
    assert!(matches!(
        decode(&corrupt, &repaired, LIMITS),
        Err(Error::Zstd(_))
    ));
}

#[test]
fn incompressible_input_falls_back_to_raw() {
    let mut state = 0x93af_701e_826b_c52du64;
    let bytes: Vec<u8> = (0..128 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let encoded = encode(&bytes, LIMITS).unwrap();
    assert_eq!(encoded.metadata.encoding, Encoding::RawV1);
    assert_eq!(encoded.stored.as_ptr(), bytes.as_ptr());
}

#[test]
fn stored_and_logical_caps_and_digests_are_independent() {
    let stored = hex(FIXTURE);
    let mut meta = fixed_metadata(&stored, FIXTURE_STORED_SHA);
    let stored_tight = Limits {
        stored: stored.len() - 1,
        ..LIMITS
    };
    let logical_tight = Limits {
        logical: FIXTURE_LEN - 1,
        ..LIMITS
    };
    assert_eq!(
        decode(&stored, &meta, stored_tight),
        Err(Error::StoredLimit)
    );
    assert_eq!(
        decode(&stored, &meta, logical_tight),
        Err(Error::LogicalLimit)
    );
    let logical = vec![b'1'; FIXTURE_LEN];
    assert_eq!(
        encode(&logical, logical_tight).unwrap_err(),
        Error::LogicalLimit
    );
    assert_eq!(
        encode(
            &logical,
            Limits {
                stored: 1,
                ..LIMITS
            }
        )
        .unwrap_err(),
        Error::StoredLimit
    );
    assert_eq!(
        decode(&stored[..stored.len() - 1], &meta, LIMITS),
        Err(Error::StoredIntegrity)
    );
    meta.stored_digest[0] ^= 1;
    assert_eq!(decode(&stored, &meta, LIMITS), Err(Error::StoredIntegrity));
    meta.stored_digest[0] ^= 1;
    meta.logical_digest[0] ^= 1;
    assert_eq!(decode(&stored, &meta, LIMITS), Err(Error::LogicalIntegrity));
}

#[test]
fn only_one_complete_frame_and_no_extra_output() {
    let stored = hex(FIXTURE);
    let logical = vec![b'1'; FIXTURE_LEN];
    let bad = &stored[..stored.len() - 1];
    assert!(matches!(
        decode(bad, &metadata(bad, &logical), LIMITS),
        Err(Error::Truncated | Error::Zstd(_))
    ));
    for len in 0..9 {
        let prefix = &stored[..len];
        assert_eq!(
            decode(prefix, &metadata(prefix, &logical), LIMITS),
            Err(Error::FrameHeader),
            "truncated header at {len} bytes"
        );
    }
    for trailing in [b"x".as_slice(), stored.as_slice()] {
        let joined = [stored.as_slice(), trailing].concat();
        assert_eq!(
            decode(&joined, &metadata(&joined, &logical), LIMITS),
            Err(Error::TrailingInput)
        );
    }
    let skip = hex("502a4d1800000000");
    assert_eq!(
        decode(&skip, &metadata(&skip, b""), LIMITS),
        Err(Error::FrameHeader)
    );
    let prefixed = [skip.as_slice(), &stored].concat();
    assert_eq!(
        decode(&prefixed, &metadata(&prefixed, &logical), LIMITS),
        Err(Error::FrameHeader)
    );

    // Corrupt just the declared content size, not the compressed payload.
    // zstd may reject the mismatch itself or expose the byte to the cap check.
    let mut extra = stored;
    extra[5..9].copy_from_slice(&((FIXTURE_LEN - 1) as u32).to_le_bytes());
    let meta = metadata(&extra, &logical[..FIXTURE_LEN - 1]);
    assert!(matches!(
        decode(&extra, &meta, LIMITS),
        Err(Error::ExtraOutput | Error::Zstd(_))
    ));
}

#[test]
fn strict_header_requires_checksum_and_matching_declared_size() {
    let logical = vec![7; 8192];
    let without_checksum = one_shot(&logical, false, true, 20);
    let meta = metadata(&without_checksum, &logical);
    assert_eq!(
        decode(&without_checksum, &meta, LIMITS),
        Err(Error::FrameHeader)
    );
    let frame = one_shot(&logical, true, true, 20);
    assert_eq!(
        decode(&frame, &metadata(&frame, &logical[..8191]), LIMITS),
        Err(Error::FrameHeader)
    );
    let without_size = one_shot(&logical, true, false, 20);
    assert_eq!(
        decode(&without_size, &metadata(&without_size, &logical), LIMITS),
        Err(Error::FrameHeader)
    );

    let mut bad_digest = meta;
    bad_digest.stored_digest[0] ^= 1;
    assert_eq!(
        decode(&without_checksum, &bad_digest, LIMITS),
        Err(Error::StoredIntegrity)
    );
}

#[test]
fn accepts_declared_zero_but_not_unknown_size_or_nonzero_dictionary_id() {
    // Each frame has an empty last block and the checksum of empty content.
    for (wire, expected) in [
        (EMPTY_FRAME, Ok(vec![])),
        ("28b52ffd040001000099e9d851", Err(Error::FrameHeader)), // no content size
        ("28b52ffd25000001000099e9d851", Ok(vec![])),            // explicit ID zero
        ("28b52ffd25010001000099e9d851", Err(Error::FrameHeader)), // ID one
    ] {
        let stored = hex(wire);
        assert_eq!(
            decode(&stored, &metadata(&stored, b""), LIMITS),
            expected,
            "frame: {wire}"
        );
    }
}

#[test]
fn declared_window_is_checked_at_the_exact_boundary() {
    // Empty frames with a four-byte content size of zero and an explicit
    // window. Only the window descriptor differs: 1 MiB vs. 1 MiB + 128 KiB.
    let at_cap = hex("28b52ffd84500000000001000099e9d851");
    let above_cap = hex("28b52ffd84510000000001000099e9d851");
    assert_eq!(decode(&at_cap, &metadata(&at_cap, b""), LIMITS), Ok(vec![]));
    assert_eq!(
        decode(&above_cap, &metadata(&above_cap, b""), LIMITS),
        Err(Error::FrameHeader)
    );
}

#[test]
fn reads_frozen_legacy_writer_frame_at_exact_limits() {
    // Captured from APP 3eda4085eea672c8b72d8ec89293dbae719f0424 via
    // derived_cache::codec::encode (vendored libzstd 1.5.7). This capture
    // independently matches the neutral one-shot fixture byte for byte.
    const STORED_HEX: &str = "28b52ffda4000003005400001031310100fbff39c0020300083102c718ca";
    const STORED_SHA: &str = "458e57a428089ca9b190546fac22dacbcc56115e9c670539579cd8a8751bc20c";
    const LOGICAL_SHA: &str = "ac7f5c23a86716e8e242f4417bf4329654e94796b373687927b090f272f0ba92";
    let stored = hex(STORED_HEX);
    let meta = Metadata {
        encoding: Encoding::ZstdV1,
        stored_size: 30,
        logical_size: FIXTURE_LEN,
        stored_digest: hex(STORED_SHA).try_into().unwrap(),
        logical_digest: hex(LOGICAL_SHA).try_into().unwrap(),
    };
    let limits = Limits {
        stored: meta.stored_size,
        logical: meta.logical_size,
    };
    assert_eq!(
        decode(&stored, &meta, limits).unwrap(),
        vec![b'1'; FIXTURE_LEN]
    );
}

#[test]
fn concurrent_bounded_decodes_keep_strict_outcomes() {
    use std::{sync::Barrier, thread};

    let stored = hex(FIXTURE);
    let valid = fixed_metadata(&stored, FIXTURE_STORED_SHA);
    let wide = hex("28b52ffd84510000000001000099e9d851");
    let invalid = metadata(&wide, b"");
    let rendezvous = Barrier::new(2);
    let run = |bytes: &[u8], meta: &Metadata| {
        let limits = Limits {
            stored: bytes.len(),
            logical: meta.logical_size,
        };
        // Both jobs are live before either decodes, and neither exits early.
        rendezvous.wait();
        let result = decode(bytes, meta, limits);
        rendezvous.wait();
        result
    };

    let (decoded, rejected) = thread::scope(|scope| {
        let good = scope.spawn(|| run(&stored, &valid));
        let bad = scope.spawn(|| run(&wide, &invalid));
        (good.join(), bad.join())
    });
    assert_eq!(decoded.unwrap().unwrap(), vec![b'1'; FIXTURE_LEN]);
    assert_eq!(rejected.unwrap(), Err(Error::FrameHeader));
}
