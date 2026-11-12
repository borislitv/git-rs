use super::{Encoding, MAX_BYTES, Metadata, decode, encode};
use crate::{derived_cache::payload::Budget, process::Operation};

#[test]
fn raw_roundtrip_preserves_magic_bytes_and_borrows_publication_input() {
    Operation::new(5).enter(|| {
        for bytes in [
            b"".as_slice(),
            b"\x28\xb5\x2f\xfdnot-a-frame".as_slice(),
            b"\x50\x2a\x4d\x18not-a-skippable-frame".as_slice(),
            b"\0binary\xff\r\nwithout-final-newline".as_slice(),
        ] {
            let budget = Budget::new(128 * 1024);
            let encoded = encode(bytes, &budget, MAX_BYTES).unwrap();
            assert_eq!(encoded.metadata.encoding, Encoding::Raw);
            assert_eq!(encoded.bytes(), bytes);
            assert_eq!(encoded.bytes().as_ptr(), bytes.as_ptr());
            assert_eq!(
                budget.used(),
                0,
                "raw publication must not allocate a second payload"
            );
            let decoded = decode(encoded.bytes(), &encoded.metadata, &budget).unwrap();
            assert_eq!(decoded.bytes, bytes);
            assert_eq!(budget.used(), decoded.bytes.capacity());
            drop(decoded);
            assert_eq!(budget.used(), 0, "last decoded owner releases its capacity");
        }
    });
}

#[test]
fn metadata_rejects_unknown_encoding_invalid_hashes_and_independent_size_limits() {
    let digest = [7; 32];
    let valid = Metadata::parse("raw-v1", 3, &digest, 3, &digest, MAX_BYTES).unwrap();
    assert_eq!(valid.logical_size, 3);
    assert_eq!(valid.stored_size, 3);
    for (encoding, logical_size, logical_digest, stored_size, stored_digest, limit) in [
        (
            "unknown",
            3,
            digest.as_slice(),
            3,
            digest.as_slice(),
            MAX_BYTES,
        ),
        (
            "raw-v1",
            -1,
            digest.as_slice(),
            3,
            digest.as_slice(),
            MAX_BYTES,
        ),
        ("raw-v1", 3, &digest[..31], 3, digest.as_slice(), MAX_BYTES),
        ("raw-v1", 3, digest.as_slice(), 3, &digest[..31], MAX_BYTES),
        (
            "raw-v1",
            3,
            digest.as_slice(),
            2,
            digest.as_slice(),
            MAX_BYTES,
        ),
        (
            "zstd-v1",
            3,
            digest.as_slice(),
            -1,
            digest.as_slice(),
            MAX_BYTES,
        ),
        (
            "zstd-v1",
            3,
            digest.as_slice(),
            0,
            digest.as_slice(),
            MAX_BYTES,
        ),
        ("zstd-v1", 4, digest.as_slice(), 2, digest.as_slice(), 3),
        ("zstd-v1", 2, digest.as_slice(), 4, digest.as_slice(), 3),
        (
            "zstd-v1",
            MAX_BYTES as i64 + 1,
            digest.as_slice(),
            2,
            digest.as_slice(),
            usize::MAX,
        ),
        (
            "zstd-v1",
            2,
            digest.as_slice(),
            MAX_BYTES as i64 + 1,
            digest.as_slice(),
            usize::MAX,
        ),
    ] {
        assert!(
            Metadata::parse(
                encoding,
                logical_size,
                logical_digest,
                stored_size,
                stored_digest,
                limit
            )
            .is_err(),
            "encoding={encoding}, logical={logical_size}, stored={stored_size}, limit={limit}"
        );
    }
    assert!(Metadata::parse("raw-v1", 3, &digest, 3, &[8; 32], MAX_BYTES).is_err());
}

#[test]
fn corrupt_raw_bytes_never_allocate_or_escape_as_decoded_output() {
    Operation::new(5).enter(|| {
        let budget = Budget::new(128 * 1024);
        let encoded = encode(b"valid", &budget, MAX_BYTES).unwrap();
        assert!(decode(b"wrong", &encoded.metadata, &budget).is_err());
        assert!(decode(b"valid-extra", &encoded.metadata, &budget).is_err());
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn decoded_capacity_is_admitted_before_output_and_failure_releases_reservations() {
    Operation::new(5).enter(|| {
        let budget = Budget::new(5);
        let encoded = encode(b"raw", &budget, MAX_BYTES).unwrap();
        assert!(decode(encoded.bytes(), &encoded.metadata, &budget).is_err());
        assert_eq!(
            budget.used(),
            0,
            "rejected allocation must not retain admission"
        );
        assert!(encode(b"raw", &budget, 2).is_err());
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn cancelled_operation_does_not_allocate_decoded_output() {
    let budget = Budget::new(128 * 1024);
    let encoded = Operation::new(5)
        .enter(|| encode(b"raw", &budget, MAX_BYTES))
        .unwrap();
    let cancelled = Operation::new(5);
    cancelled.cancel();
    assert!(
        cancelled
            .enter(|| decode(encoded.bytes(), &encoded.metadata, &budget))
            .is_err()
    );
    assert_eq!(budget.used(), 0);
}

// A separate one-shot producer supplies valid/adversarial frames to the streaming
// decoder. Expected logical bytes come from the fixture input, not its decoder.
fn frame(bytes: &[u8], checksum: bool, content_size: bool, window_log: u32) -> Vec<u8> {
    use zstd::zstd_safe::{self, CCtx, CParameter};
    let mut context = CCtx::try_create().unwrap();
    for parameter in [
        CParameter::CompressionLevel(3),
        CParameter::WindowLog(window_log),
        CParameter::ChecksumFlag(checksum),
        CParameter::ContentSizeFlag(content_size),
    ] {
        context.set_parameter(parameter).unwrap();
    }
    let mut encoded = vec![0; zstd_safe::compress_bound(bytes.len())];
    let size = context.compress2(&mut encoded[..], bytes).unwrap();
    encoded.truncate(size);
    encoded
}

fn compressed_metadata(stored: &[u8], logical: &[u8]) -> Metadata {
    Metadata::parse(
        "zstd-v1",
        i64::try_from(logical.len()).unwrap(),
        &openssl::sha::sha256(logical),
        i64::try_from(stored.len()).unwrap(),
        &openssl::sha::sha256(stored),
        MAX_BYTES,
    )
    .unwrap()
}

#[test]
fn beneficial_compression_roundtrip_retains_only_payload_capacity_not_native_workspace() {
    Operation::new(15).enter(|| {
        let bytes = vec![0xa7; 256 * 1024];
        let budget = Budget::new(64 * 1024 * 1024);
        let encoded = encode(&bytes, &budget, MAX_BYTES).unwrap();
        assert_eq!(encoded.metadata.encoding, Encoding::Zstd);
        assert_eq!(encoded.metadata.logical_size, bytes.len());
        assert_eq!(
            encoded.metadata.logical_digest,
            openssl::sha::sha256(&bytes)
        );
        assert_eq!(encoded.metadata.stored_size, encoded.bytes().len());
        assert_eq!(
            encoded.metadata.stored_digest,
            openssl::sha::sha256(encoded.bytes())
        );
        assert!(matches!(
            zstd::zstd_safe::get_frame_header(encoded.bytes()).unwrap(),
            zstd::zstd_safe::FrameHeaderStatus::Complete(header)
                if header.frame_type == zstd::zstd_safe::FrameType::Zstandard
                    && header.content_size == Some(256 * 1024)
                    && header.window_size <= 1 << 20
                    && header.checksum
                    && header.dict_id.is_none()
        ));
        assert!(encoded.bytes().len() <= bytes.len() - bytes.len().div_ceil(16));
        assert!(
            budget.used() <= bytes.len(),
            "native workspace must not accompany the encoded owner"
        );
        let decoded = decode(encoded.bytes(), &encoded.metadata, &budget).unwrap();
        assert_eq!(decoded.bytes, bytes);
        drop(encoded);
        assert_eq!(
            budget.used(),
            decoded.bytes.capacity(),
            "decoded owner retains only its payload capacity"
        );
        drop(decoded);
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn incompressible_payload_falls_back_to_borrowed_raw_input() {
    Operation::new(15).enter(|| {
        // Deterministic pseudo-random data, not network/OS randomness or a copy
        // of the production compression algorithm.
        let mut seed = 0x93af_701e_826b_c52du64;
        let bytes: Vec<u8> = (0..128 * 1024)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed.to_le_bytes()[0]
            })
            .collect();
        let budget = Budget::new(64 * 1024 * 1024);
        let encoded = encode(&bytes, &budget, MAX_BYTES).unwrap();
        assert_eq!(encoded.metadata.encoding, Encoding::Raw);
        assert_eq!(encoded.bytes(), bytes);
        assert_eq!(encoded.bytes().as_ptr(), bytes.as_ptr());
        assert_eq!(
            budget.used(),
            0,
            "rejected candidate and context are released"
        );
    });
}

#[test]
fn policy_keeps_below_threshold_raw_and_compresses_exact_threshold() {
    Operation::new(5).enter(|| {
        let budget = Budget::new(64 * 1024 * 1024);
        let below = vec![7; 4095];
        let exact = vec![7; 4096];
        assert_eq!(
            encode(&below, &budget, MAX_BYTES)
                .unwrap()
                .metadata
                .encoding,
            Encoding::Raw
        );
        assert_eq!(
            encode(&exact, &budget, MAX_BYTES)
                .unwrap()
                .metadata
                .encoding,
            Encoding::Zstd
        );
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn strict_header_rejection_precedes_native_or_output_admission() {
    Operation::new(15).enter(|| {
        let bytes = vec![9; 8192];
        let normal = frame(&bytes, true, true, 20);
        let mut dictionary = normal.clone();
        let dict_offset = if dictionary[4] & 0x20 != 0 { 5 } else { 6 };
        dictionary[4] = (dictionary[4] & !3) | 1;
        dictionary.insert(dict_offset, 1);
        let header = zstd::zstd_safe::get_frame_header(&dictionary).unwrap();
        assert!(
            matches!(header, zstd::zstd_safe::FrameHeaderStatus::Complete(header)
            if header.dict_id.is_some() && header.content_size == Some(8192))
        );
        let oversized = vec![9; 2 * 1024 * 1024];
        let wide = frame(&oversized, true, true, 21);
        let mut invalid_magic = normal.clone();
        invalid_magic[0] ^= 1;
        let mut skippable = vec![0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0];
        skippable.extend_from_slice(&normal);
        for (name, stored, logical) in [
            (
                "missing checksum",
                frame(&bytes, false, true, 20),
                bytes.as_slice(),
            ),
            (
                "missing content size",
                frame(&bytes, true, false, 20),
                bytes.as_slice(),
            ),
            ("advertised dictionary", dictionary, bytes.as_slice()),
            ("oversized window", wide, oversized.as_slice()),
            ("invalid magic", invalid_magic, bytes.as_slice()),
            ("skippable prefix", skippable, bytes.as_slice()),
            ("truncated header", normal[..3].to_vec(), bytes.as_slice()),
        ] {
            let metadata = compressed_metadata(&stored, logical);
            let budget = Budget::new(0);
            let error = decode(&stored, &metadata, &budget).err().expect(name);
            assert!(error.to_string().contains("cache zstd"), "{name}: {error}");
            assert!(
                !error.to_string().contains("budget exhausted"),
                "{name}: header must reject before admission"
            );
            assert_eq!(budget.used(), 0, "{name}");
        }
    });
}

#[test]
fn frame_completion_checksum_trailing_and_logical_digest_are_all_required() {
    Operation::new(15).enter(|| {
        let bytes = vec![0x31; 192 * 1024];
        let valid = frame(&bytes, true, true, 20);
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut concatenated = valid.clone();
        concatenated.extend_from_slice(&valid);
        let mut bad_checksum = valid.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        for (name, stored) in [
            ("truncated footer", valid[..valid.len() - 1].to_vec()),
            ("trailing byte", trailing),
            ("concatenated frame", concatenated),
            ("invalid checksum", bad_checksum),
        ] {
            // Repair storage integrity so these failures reach actual framing.
            let metadata = compressed_metadata(&stored, &bytes);
            let budget = Budget::new(64 * 1024 * 1024);
            assert!(decode(&stored, &metadata, &budget).is_err(), "{name}");
            assert_eq!(
                budget.used(),
                0,
                "{name} must release output and native admission"
            );
        }
        let budget = Budget::new(64 * 1024 * 1024);
        let mut metadata = compressed_metadata(&valid, &bytes);
        metadata.logical_digest = [0xff; 32];
        let error = decode(&valid, &metadata, &budget).err().unwrap();
        assert!(error.to_string().contains("logical payload integrity"));
        assert_eq!(budget.used(), 0);
        metadata.logical_size += 1;
        assert!(
            decode(&valid, &metadata, &budget).is_err(),
            "declared content size must match metadata"
        );
        assert_eq!(budget.used(), 0);
    });
}

#[test]
fn compressed_admission_failure_is_explicit_and_does_not_silently_disable_compression() {
    Operation::new(5).enter(|| {
        let bytes = vec![4; 8192];
        let stored = frame(&bytes, true, true, 20);
        let metadata = compressed_metadata(&stored, &bytes);
        let budget = Budget::new(1);
        let encode_error = encode(&bytes, &budget, MAX_BYTES).err().unwrap();
        assert!(encode_error.to_string().contains("budget exhausted"));
        assert_eq!(budget.used(), 0);
        let decode_error = decode(&stored, &metadata, &budget).err().unwrap();
        assert!(decode_error.to_string().contains("budget exhausted"));
        assert_eq!(budget.used(), 0);

        // A completed encoded owner can outlive its native context while a
        // second job starts against the same finite budget. Reject that second
        // candidate without refunding the still-live first owner's capacity.
        let bytes = vec![4; MAX_BYTES];
        let shared = Budget::new(16 * 1024 * 1024);
        let first = encode(&bytes, &shared, MAX_BYTES).unwrap();
        let retained = shared.used();
        assert!(retained > 0);
        assert!(encode(&bytes, &shared, MAX_BYTES).is_err());
        assert_eq!(shared.used(), retained);
        drop(first);
        assert_eq!(shared.used(), 0);
        let fresh = encode(&bytes, &shared, MAX_BYTES).unwrap();
        assert_eq!(fresh.metadata.encoding, Encoding::Zstd);
        drop(fresh);
        assert_eq!(shared.used(), 0);
    });
}

#[test]
fn exact_decoded_limits_allow_footer_completion_but_never_extra_output() {
    Operation::new(30).enter(|| {
        let budget = Budget::new(64 * 1024 * 1024);
        let empty_frame = frame(b"", true, true, 20);
        let empty = decode(
            &empty_frame,
            &compressed_metadata(&empty_frame, b""),
            &budget,
        )
        .unwrap();
        assert!(empty.bytes.is_empty());
        drop(empty);
        let bytes = vec![0x55; MAX_BYTES];
        let encoded = encode(&bytes, &budget, MAX_BYTES).unwrap();
        assert_eq!(encoded.metadata.encoding, Encoding::Zstd);
        let decoded = decode(encoded.bytes(), &encoded.metadata, &budget).unwrap();
        assert_eq!(decoded.bytes, bytes);
        drop(decoded);
        drop(encoded);
        assert_eq!(budget.used(), 0);

        // A valid frame encoded from 8192 bytes, with its declared size reduced
        // by one. This reaches the native output/content-size boundary rather
        // than failing the metadata/header equality check.
        let bytes = vec![0x55; 8192];
        let mut stored = frame(&bytes, true, true, 20);
        assert_ne!(stored[4] & 0x20, 0, "fixture is a single-segment frame");
        assert_eq!(stored[4] >> 6, 1, "fixture declares two-byte content size");
        assert_eq!(stored[4] & 3, 0, "fixture has no dictionary field");
        stored[5..7].copy_from_slice(&(8191u16 - 256).to_le_bytes());
        let metadata = compressed_metadata(&stored, &bytes[..8191]);
        assert!(
            decode(&stored, &metadata, &budget).is_err(),
            "output beyond the declared boundary must never escape"
        );
        assert_eq!(budget.used(), 0);
    });
}
