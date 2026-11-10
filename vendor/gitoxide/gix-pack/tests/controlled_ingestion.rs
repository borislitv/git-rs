#![cfg(all(feature = "generate", feature = "streaming-input"))]

use std::{
    cell::Cell,
    collections::BTreeMap,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

use gix_error::Result;
use gix_pack::{
    Bundle,
    bundle::write::{ControlledOptions, ControlledOutcome},
    cache,
    data::{self, entry::Header},
    index,
};

fn options() -> ControlledOptions {
    ControlledOptions {
        input_pack_id: 123,
        max_input_bytes: 8 * 1024 * 1024,
        max_output_bytes: 8 * 1024 * 1024,
        max_index_bytes: 1024 * 1024,
        max_objects: 4096,
        max_decode_bytes: 16 * 1024 * 1024,
        max_resolve_attempts: 100_000,
        decode: data::decode::Limits {
            max_delta_depth: 128,
            max_delta_bytes: 4 * 1024 * 1024,
        },
        compression: gix_zlib::Compression::DEFAULT,
    }
}

fn id(hash: gix_hash::Kind, bytes: &[u8]) -> gix_hash::ObjectId {
    gix_object::compute_hash(hash, gix_object::Kind::Blob, bytes).expect("small synthetic object can be hashed")
}

fn append(pack: &mut Vec<u8>, header: Header, bytes: &[u8]) -> u64 {
    let offset = pack.len() as u64;
    header
        .write_to(bytes.len() as u64, pack)
        .expect("in-memory header write succeeds");
    let mut deflate = gix_zlib::stream::deflate::Write::new(Vec::new(), gix_zlib::Compression::DEFAULT);
    deflate.write_all(bytes).expect("synthetic object compresses");
    deflate.flush().expect("synthetic zlib stream finishes");
    pack.extend_from_slice(&deflate.into_inner());
    offset
}

fn seal(mut bytes: Vec<u8>, hash: gix_hash::Kind) -> Vec<u8> {
    let mut digest = gix_hash::hasher(hash);
    digest.update(&bytes);
    bytes.extend_from_slice(
        digest
            .try_finalize()
            .expect("synthetic pack has no hash collision")
            .as_slice(),
    );
    bytes
}

fn forward(hash: gix_hash::Kind) -> Vec<u8> {
    let mut pack = data::header::encode(data::Version::V2, 3).to_vec();
    append(
        &mut pack,
        Header::RefDelta {
            base_id: id(hash, b"abcd"),
        },
        b"\x04\x05\x90\x04\x01e",
    );
    append(
        &mut pack,
        Header::RefDelta {
            base_id: id(hash, b"abc"),
        },
        b"\x03\x04\x90\x03\x01d",
    );
    append(&mut pack, Header::Blob, b"abc");
    seal(pack, hash)
}

fn objects(outcome: &ControlledOutcome, hash: gix_hash::Kind) -> Result<BTreeMap<gix_hash::ObjectId, Vec<u8>>> {
    let index = index::File::from_data(outcome.index.as_slice(), PathBuf::new(), hash)?;
    let pack = data::File::from_data(outcome.pack.as_slice(), PathBuf::new(), hash)?
        .with_alloc_limit_bytes(Some(options().max_decode_bytes));
    let mut result = BTreeMap::new();
    for entry in index.iter() {
        let packed = pack.entry(entry.pack_offset)?;
        assert!(
            !packed.header.is_delta(),
            "normalization removes external and forward dependencies"
        );
        let mut output = Vec::new();
        pack.decode_entry_controlled(
            packed,
            &mut output,
            &mut gix_zlib::Inflate::default(),
            &|_, _| Ok(None),
            &mut cache::Never,
            options().decode,
            &|| Ok(()),
        )?;
        result.insert(entry.oid, output);
    }
    Ok(result)
}

#[test]
fn forward_ref_delta_chains_produce_complete_native_compatible_pack_and_index() -> gix_error::TestResult {
    for hash in [gix_hash::Kind::Sha1, gix_hash::Kind::Sha256] {
        for version in [data::Version::V2, data::Version::V3] {
            let mut input = forward(hash);
            input.truncate(input.len() - hash.len_in_bytes());
            input[..data::header::SIZE].copy_from_slice(&data::header::encode(version, 3));
            let input = seal(input, hash);
            let outcome = Bundle::prepare_stream_controlled(
                &mut input.as_slice(),
                hash,
                options(),
                &|_, _| Ok(None),
                &mut cache::Never,
                &|| Ok(()),
            )?;
            assert_eq!(outcome.outcome.num_objects, 3);
            assert_eq!(
                objects(&outcome, hash)?,
                BTreeMap::from([
                    (id(hash, b"abc"), b"abc".to_vec()),
                    (id(hash, b"abcd"), b"abcd".to_vec()),
                    (id(hash, b"abcde"), b"abcde".to_vec()),
                ])
            );
            // Native Git is an isolated test oracle, never an implementation fallback.
            let temp = gix_testtools::tempfile::tempdir()?;
            gix_testtools::git(
                temp.path(),
                if hash == gix_hash::Kind::Sha1 {
                    "init --bare"
                } else {
                    "init --bare --object-format=sha256"
                },
            )?;
            std::fs::write(temp.path().join("input.pack"), &outcome.pack)?;
            let output = gix_testtools::git_command(temp.path())
                .args(["index-pack", "--strict", "--index-version=2", "input.pack"])
                .output()?;
            assert!(
                output.status.success(),
                "native strict pack oracle: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read(temp.path().join("input.idx"))?,
                outcome.index,
                "maintained encoder's index agrees byte-for-byte with native Git"
            );
        }
    }
    Ok(())
}

#[test]
fn thin_ref_delta_checks_external_base_identity_and_propagates_provider_failure() -> Result {
    let hash = gix_hash::Kind::Sha1;
    let base = id(hash, b"abc");
    let mut bytes = data::header::encode(data::Version::V2, 1).to_vec();
    append(&mut bytes, Header::RefDelta { base_id: base }, b"\x03\x04\x90\x03\x01d");
    let bytes = seal(bytes, hash);
    let calls = Cell::new(0);
    let provider = |requested: &gix_hash::oid, out: &mut Vec<u8>| {
        assert_eq!(requested, base.as_ref());
        calls.set(calls.get() + 1);
        out.clear();
        out.extend_from_slice(b"abc");
        Ok(Some(gix_object::Kind::Blob))
    };
    let outcome = Bundle::prepare_stream_controlled(
        &mut bytes.as_slice(),
        hash,
        options(),
        &provider,
        &mut cache::Never,
        &|| Ok(()),
    )?;
    assert_eq!(
        objects(&outcome, hash)?,
        BTreeMap::from([(id(hash, b"abcd"), b"abcd".to_vec())])
    );
    assert_eq!(calls.get(), 1);
    let wrong = |_: &gix_hash::oid, out: &mut Vec<u8>| {
        out.clear();
        out.extend_from_slice(b"zzz");
        Ok(Some(gix_object::Kind::Blob))
    };
    let err = Bundle::prepare_stream_controlled(
        &mut bytes.as_slice(),
        hash,
        options(),
        &wrong,
        &mut cache::Never,
        &|| Ok(()),
    )
    .err()
    .expect("wrong base is rejected");
    assert!(err.is_corrupted());
    let err = Bundle::prepare_stream_controlled(
        &mut bytes.as_slice(),
        hash,
        options(),
        &|_, _| Err(gix_error::cancelled("base provider cancelled").into()),
        &mut cache::Never,
        &|| Ok(()),
    )
    .err()
    .expect("provider failure is not a missing base");
    assert!(err.is_cancelled());
    assert!(err.probable_cause().to_string().contains("base provider cancelled"));
    Ok(())
}

#[test]
fn ofs_delta_and_every_unreachable_incoming_entry_are_validated() -> Result {
    let hash = gix_hash::Kind::Sha1;
    for malformed in [false, true] {
        let mut bytes = data::header::encode(data::Version::V2, 2).to_vec();
        let base = append(&mut bytes, Header::Blob, b"abc");
        let distance = bytes.len() as u64 - base;
        append(
            &mut bytes,
            Header::OfsDelta {
                base_distance: distance,
            },
            if malformed {
                b"\x03\x04\x00"
            } else {
                b"\x03\x04\x90\x03\x01d"
            },
        );
        let bytes = seal(bytes, hash);
        let result = Bundle::prepare_stream_controlled(
            &mut bytes.as_slice(),
            hash,
            options(),
            &|_, _| Ok(None),
            &mut cache::Never,
            &|| Ok(()),
        );
        if malformed {
            assert!(
                result
                    .err()
                    .expect("the second object has invalid delta instructions")
                    .is_corrupted()
            );
        } else {
            assert_eq!(
                objects(&result?, hash)?,
                BTreeMap::from([
                    (id(hash, b"abc"), b"abc".to_vec()),
                    (id(hash, b"abcd"), b"abcd".to_vec())
                ])
            );
        }
    }
    Ok(())
}

#[test]
fn missing_or_cyclic_ref_bases_checksum_damage_and_trailing_bytes_fail() {
    let hash = gix_hash::Kind::Sha1;
    let mut cyclic = data::header::encode(data::Version::V2, 1).to_vec();
    append(
        &mut cyclic,
        Header::RefDelta {
            base_id: id(hash, b"abcd"),
        },
        b"\x03\x04\x90\x03\x01d",
    );
    let cyclic = seal(cyclic, hash);
    let valid = forward(hash);
    let mut bad_checksum = valid.clone();
    *bad_checksum.last_mut().expect("checksum exists") ^= 1;
    let mut bad_adler = valid[..valid.len() - 20].to_vec();
    *bad_adler.last_mut().expect("last entry has a zlib checksum") ^= 1;
    let mut trailing = valid[..valid.len() - 20].to_vec();
    trailing.push(0);
    let mut incomplete = valid[..valid.len() - 20].to_vec();
    incomplete.pop();
    for bytes in [
        cyclic,
        bad_checksum,
        seal(bad_adler, hash),
        seal(trailing, hash),
        seal(incomplete, hash),
    ] {
        let result = Bundle::prepare_stream_controlled(
            &mut bytes.as_slice(),
            hash,
            options(),
            &|_, _| Ok(None),
            &mut cache::Never,
            &|| Ok(()),
        );
        assert!(result.is_err(), "no incomplete or invalid pack can yield an outcome");
    }
}

#[test]
fn request_byte_count_decode_and_attempt_limits_are_enforced() {
    let bytes = forward(gix_hash::Kind::Sha1);
    let default = options();
    for limits in [
        ControlledOptions {
            max_input_bytes: bytes.len() - 1,
            ..default
        },
        ControlledOptions {
            max_output_bytes: 32,
            ..default
        },
        ControlledOptions {
            max_index_bytes: 100,
            ..default
        },
        ControlledOptions {
            max_objects: 2,
            ..default
        },
        ControlledOptions {
            max_decode_bytes: 2,
            ..default
        },
        ControlledOptions {
            max_resolve_attempts: 0,
            ..default
        },
        ControlledOptions {
            decode: data::decode::Limits {
                max_delta_depth: 0,
                max_delta_bytes: 0,
            },
            ..default
        },
    ] {
        assert!(
            Bundle::prepare_stream_controlled(
                &mut bytes.as_slice(),
                gix_hash::Kind::Sha1,
                limits,
                &|_, _| Ok(None),
                &mut cache::Never,
                &|| Ok(())
            )
            .is_err()
        );
    }
}

#[test]
fn empty_packs_and_supported_pack_versions_have_complete_indexes() -> Result {
    for version in [data::Version::V2, data::Version::V3] {
        let bytes = seal(data::header::encode(version, 0).to_vec(), gix_hash::Kind::Sha1);
        let outcome = Bundle::prepare_stream_controlled(
            &mut bytes.as_slice(),
            gix_hash::Kind::Sha1,
            options(),
            &|_, _| Ok(None),
            &mut cache::Never,
            &|| Ok(()),
        )?;
        assert_eq!(outcome.outcome.num_objects, 0);
        assert!(objects(&outcome, gix_hash::Kind::Sha1)?.is_empty());
        assert_eq!(&outcome.pack[..12], data::header::encode(version, 0).as_slice());
    }
    Ok(())
}

#[test]
fn ingestion_cancellation_inside_one_large_inflate_never_returns_an_outcome() -> Result {
    let mut bytes = data::header::encode(data::Version::V2, 1).to_vec();
    append(&mut bytes, Header::Blob, &vec![b'A'; 4 * 1024 * 1024]);
    let bytes = seal(bytes, gix_hash::Kind::Sha1);
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    // After input/header checks and buffer initialization, this budget can only
    // be reached by the inner inflate polls, before compression/index work.
    let check = || {
        polls.set(polls.get() + 1);
        if polls.get() == 160 {
            flag.store(true, Ordering::Release);
        }
        if flag.load(Ordering::Acquire) {
            Err(gix_error::cancelled("request cancelled").into())
        } else {
            Ok(())
        }
    };
    let err = Bundle::prepare_stream_controlled(
        &mut bytes.as_slice(),
        gix_hash::Kind::Sha1,
        options(),
        &|_, _| Ok(None),
        &mut cache::Never,
        &check,
    )
    .err()
    .expect("cancellation does not return any output");
    assert!(err.is_cancelled());
    let outcome = Bundle::prepare_stream_controlled(
        &mut bytes.as_slice(),
        gix_hash::Kind::Sha1,
        options(),
        &|_, _| Ok(None),
        &mut cache::Never,
        &|| Ok(()),
    )?;
    assert_eq!(
        objects(&outcome, gix_hash::Kind::Sha1)?
            .values()
            .next()
            .expect("one object")
            .len(),
        4 * 1024 * 1024
    );
    Ok(())
}

#[test]
fn cancellation_during_hashing_of_one_large_untrusted_pack_is_not_deferred_to_parsing() {
    use std::io::{self, Read};
    let mut bytes = data::header::encode(data::Version::V2, 0).to_vec();
    bytes.resize(4 * 1024 * 1024, 0);
    // An attacker can send a checksum-valid pack with an inconsistent count.
    // Hashing its bytes must still be interruptible before the parser rejects it.
    let bytes = seal(bytes, gix_hash::Kind::Sha1);
    let at_eof = Cell::new(false);
    struct Input<'a> {
        bytes: &'a [u8],
        at_eof: &'a Cell<bool>,
    }
    impl Read for Input<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.bytes.read(out)?;
            if n == 0 {
                self.at_eof.set(true);
            }
            Ok(n)
        }
    }
    let mut input = Input {
        bytes: &bytes,
        at_eof: &at_eof,
    };
    let hash_polls = Cell::new(0);
    let check = || {
        if at_eof.get() {
            hash_polls.set(hash_polls.get() + 1);
        }
        if hash_polls.get() >= 4 {
            Err(gix_error::cancelled("hash cancelled").into())
        } else {
            Ok(())
        }
    };
    let err = Bundle::prepare_stream_controlled(
        &mut input,
        gix_hash::Kind::Sha1,
        options(),
        &|_, _| Ok(None),
        &mut cache::Never,
        &check,
    )
    .err()
    .expect("the request cannot succeed");
    assert!(
        err.is_cancelled(),
        "cancellation must win during the hash, not a later trailing-byte parse error: {err:?}"
    );
}
