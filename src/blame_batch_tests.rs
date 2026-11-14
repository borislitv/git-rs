//! Frame-owner contracts; real mapping parity is covered by the source gate.
use super::*;
use crate::derived_cache::KeyParts;
use std::time::{Duration, Instant};

fn scope(key: &[u8; 32], principal: &[u8]) -> Scope {
    let config = CacheConfig::new(
        "11111111-1111-1111-1111-111111111111".into(),
        "22222222-2222-2222-2222-222222222222".into(),
    );
    Scope {
        key: ArtifactKey::new(KeyParts {
            repository: b"repository",
            security: principal,
            object_format: "sha1",
            kind: "complete-blame-workset",
            inputs: &[b"generation-1", b"epoch-1", b"commit", b"path"],
            options: b"native",
            representation: "authenticated-complete-units-v1",
            producer: "fixture-producer",
        })
        .unwrap(),
        producer: [3; 32],
        options: [4; 32],
        writer: WriterProof::from_key(&config, key).unwrap(),
    }
}
fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(5))
}
fn hello() -> UnitKey {
    UnitKey::Object {
        oid: ObjectId::from_hex(b"ce013625030ba8dba906f756967f9e9ca394464a").unwrap(),
        kind: Kind::Blob,
    }
}

#[test]
fn complete_body_authentication_binds_full_key_writer_and_rotated_secret() {
    let control = control();
    let owner = scope(&[7; 32], b"alice");
    // An independently framed one-chunk descriptor, not a cache-derived tag.
    let mut manifest = b"git-rs-complete-artifact\0\x01\x01\0\0\0\x01\0\0\0\x06".to_vec();
    manifest.extend_from_slice(&openssl::sha::sha256(b"hello\n"));
    let tag = owner
        .writer
        .body_tag(&owner.key, &manifest, &control)
        .unwrap();
    owner
        .writer
        .verify_body(&owner.key, &manifest, &tag, &control)
        .unwrap();
    assert!(
        scope(&[8; 32], b"alice")
            .writer
            .verify_body(&owner.key, &manifest, &tag, &control)
            .is_err()
    );
    // Change each canonical field independently, including producer/options,
    // source generation/epoch and exact kind/representation. Payload is identical.
    for field in 0..11 {
        let inputs: [&[u8]; 4] = [
            if field == 7 {
                b"generation-2"
            } else {
                b"generation-1"
            },
            if field == 8 { b"epoch-2" } else { b"epoch-1" },
            if field == 9 {
                b"other-commit"
            } else {
                b"commit"
            },
            if field == 10 { b"other-path" } else { b"path" },
        ];
        let key = ArtifactKey::new(KeyParts {
            repository: if field == 0 {
                b"other-repository"
            } else {
                b"repository"
            },
            security: if field == 1 { b"bob" } else { b"alice" },
            object_format: if field == 2 { "sha256" } else { "sha1" },
            kind: if field == 3 {
                "complete-blame"
            } else {
                "complete-blame-workset"
            },
            representation: if field == 4 {
                "other-format"
            } else {
                "authenticated-complete-units-v1"
            },
            producer: if field == 5 {
                "other-producer"
            } else {
                "fixture-producer"
            },
            options: if field == 6 {
                b"other-options"
            } else {
                b"native"
            },
            inputs: &inputs,
        })
        .unwrap();
        assert!(
            owner
                .writer
                .verify_body(&key, &manifest, &tag, &control)
                .is_err(),
            "foreign full-key field {field}"
        );
    }
    for config in [
        CacheConfig::new(
            "33333333-3333-3333-3333-333333333333".into(),
            "22222222-2222-2222-2222-222222222222".into(),
        ),
        CacheConfig::new(
            "11111111-1111-1111-1111-111111111111".into(),
            "33333333-3333-3333-3333-333333333333".into(),
        ),
    ] {
        let foreign = WriterProof::from_key(&config, &[7; 32]).unwrap();
        assert!(
            foreign
                .verify_body(&owner.key, &manifest, &tag, &control)
                .is_err()
        );
    }
}

#[test]
fn complete_body_authentication_rejects_repaired_digest_length_kind_and_format() {
    let owner = scope(&[7; 32], b"alice");
    let control = control();
    let mut manifest = b"git-rs-complete-artifact\0\x01\x01\0\0\0\x01\0\0\0\x06".to_vec();
    manifest.extend_from_slice(&openssl::sha::sha256(b"hello\n"));
    let tag = owner
        .writer
        .body_tag(&owner.key, &manifest, &control)
        .unwrap();
    for offset in [0, 25, 26, 30, 34] {
        let mut changed = manifest.clone();
        changed[offset] ^= 1;
        assert!(
            owner
                .writer
                .verify_body(&owner.key, &changed, &tag, &control)
                .is_err(),
            "changed descriptor byte {offset}"
        );
    }
    let mut repaired = manifest.clone();
    let start = repaired.len() - 32;
    repaired[start..].copy_from_slice(&openssl::sha::sha256(b"jello\n"));
    assert!(
        owner
            .writer
            .verify_body(&owner.key, &repaired, &tag, &control)
            .is_err()
    );
    let mut corrupt_tag = tag;
    corrupt_tag[31] ^= 1;
    assert!(
        owner
            .writer
            .verify_body(&owner.key, &manifest, &corrupt_tag, &control)
            .is_err()
    );
}

#[test]
fn authenticated_round_trip_accepts_empty_and_split_chunks_and_releases_all_backing() {
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let control = control();
    let scope = scope(&[7; 32], b"alice");
    {
        let mut original = BatchSnapshot::new(budget.clone()).unwrap();
        assert!(original.insert(hello(), b"hello\n", &control).unwrap());
        let frame = original.encode(&scope, &control).unwrap();
        let bytes = frame.as_slice();
        let mut restored = BatchSnapshot::decode(
            &[&[], &bytes[..13], &[], &bytes[13..], &[]],
            &scope,
            budget.clone(),
            &control,
        )
        .unwrap();
        let mut output = [0; 6];
        assert_eq!(
            restored.load(hello(), &mut output, &control).unwrap(),
            Some(6)
        );
        assert_eq!(&output, b"hello\n");
        assert!(
            budget.used() > bytes.len() + 12,
            "metadata/backings remain owned"
        );
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn repaired_digest_without_authorized_signature_and_foreign_scope_are_rejected() {
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let control = control();
    let owner = scope(&[7; 32], b"alice");
    let mut original = BatchSnapshot::new(budget.clone()).unwrap();
    original.insert(hello(), b"hello\n", &control).unwrap();
    let mut frame = original.encode(&owner, &control).unwrap();
    let retained = budget.used();
    assert!(
        BatchSnapshot::decode(
            &[frame.as_slice()],
            &scope(&[8; 32], b"alice"),
            budget.clone(),
            &control
        )
        .is_err()
    );
    assert!(
        BatchSnapshot::decode(
            &[frame.as_slice()],
            &scope(&[7; 32], b"bob"),
            budget.clone(),
            &control
        )
        .is_err()
    );
    // Rewrite complete object data AND its structural digest. HMAC stays intact.
    let bytes = frame.as_mut_slice();
    bytes[HEADER + RECORD_HEADER] = b'j';
    let repaired = openssl::sha::sha256(&bytes[HEADER + RECORD_HEADER..HEADER + RECORD_HEADER + 6]);
    bytes[HEADER + KEY_BYTES + 4..HEADER + RECORD_HEADER].copy_from_slice(&repaired);
    let error = BatchSnapshot::decode(&[frame.as_slice()], &owner, budget.clone(), &control)
        .err()
        .expect("repaired structural digest cannot authorize changed bytes");
    assert_eq!(
        error.to_string(),
        "unauthorized checkpoint writer or repaired data",
        "reject at authentication, not a later physical OID check"
    );
    assert_eq!(
        budget.used(),
        retained,
        "all failed-import temporary charges returned"
    );
    drop(frame);
    drop(original);
    assert_eq!(budget.used(), 0);
}

#[test]
fn temporary_import_can_be_discarded_after_outer_fence_without_installing_units() {
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let control = control();
    let scope = scope(&[7; 32], b"alice");
    let mut producer = BatchSnapshot::new(budget.clone()).unwrap();
    producer.insert(hello(), b"hello\n", &control).unwrap();
    let frame = producer.encode(&scope, &control).unwrap();
    let live = BatchSnapshot::new(budget.clone()).unwrap();
    let retained = budget.used();
    let candidate =
        BatchSnapshot::decode(&[frame.as_slice()], &scope, budget.clone(), &control).unwrap();
    assert!(budget.used() > retained);
    // Models owner rollback only, not the real SQL/post-consumption fence gate.
    drop(candidate);
    assert_eq!(live.len(), 0);
    assert_eq!(budget.used(), retained);
    drop(live);
    drop(frame);
    drop(producer);
    assert_eq!(budget.used(), 0);
}

#[test]
fn optional_copy_exhaustion_skips_and_wrong_physical_oid_is_never_retained() {
    let budget = CapacityBudget::new(2 * 1024 * 1024);
    let control = control();
    let mut batch = BatchSnapshot::new(budget.clone()).unwrap();
    assert!(batch.insert(hello(), b"jello\n", &control).is_err());
    assert_eq!(batch.len(), 0);
    let held = budget.reservation(2 * 1024 * 1024 - budget.used()).unwrap();
    assert!(!batch.insert(hello(), b"hello\n", &control).unwrap());
    assert_eq!(batch.len(), 0);
    drop(held);
    assert!(batch.insert(hello(), b"hello\n", &control).unwrap());
    let retained = budget.used();
    control.cancel();
    assert!(batch.encode(&scope(&[7; 32], b"alice"), &control).is_err());
    assert_eq!(budget.used(), retained);
    drop(batch);
    assert_eq!(budget.used(), 0);
}

#[test]
fn row_unit_and_actual_backing_caps_evict_without_unbounded_growth() {
    let budget = CapacityBudget::new(40 * 1024 * 1024);
    let control = control();
    let mut batch = BatchSnapshot::new(budget.clone()).unwrap();
    let key = |n: u64| {
        let mut id = [0; 20];
        id[..8].copy_from_slice(&n.to_be_bytes());
        UnitKey::Mapping {
            before: ObjectId::from(id),
            after: ObjectId::from([2; 20]),
            tuning: 0,
            producer: [3; 32],
            options: [4; 32],
        }
    };
    for n in 0..MAX_MAPPINGS as u64 + 1 {
        assert!(
            batch
                .insert(key(n), b"complete callback fixture", &control)
                .unwrap()
        );
    }
    assert_eq!(batch.mapping_count(), MAX_MAPPINGS);
    assert_eq!(batch.load(key(0), &mut [0; 32], &control).unwrap(), None);
    let unit = vec![0; UNIT_BYTES];
    for n in MAX_MAPPINGS as u64 + 1..MAX_MAPPINGS as u64 + 6 {
        assert!(batch.insert(key(n), &unit, &control).unwrap());
    }
    assert!(
        batch.mapping_count() <= 3,
        "wire overhead also consumes the 16 MiB record cap"
    );
    assert!(
        budget.used() < WORK_BYTES + 1024 * 1024,
        "actual backings plus bounded metadata"
    );
    assert!(
        !batch
            .insert(key(999), &vec![0; UNIT_BYTES + 1], &control)
            .unwrap()
    );
    drop(batch);
    assert_eq!(budget.used(), 0);
}

#[test]
fn source_metadata_stream_cannot_evict_imported_mappings_before_the_seam() {
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let control = control();
    let mut batch = BatchSnapshot::new(budget.clone()).unwrap();
    let mapping = UnitKey::Mapping {
        before: ObjectId::from([1; 20]),
        after: ObjectId::from([2; 20]),
        tuning: 0,
        producer: [3; 32],
        options: [4; 32],
    };
    batch
        .insert(mapping, b"complete mapping store fixture", &control)
        .unwrap();
    for n in 0..8192 {
        let bytes = format!("physical metadata fixture {n}");
        // Generate independent valid physical fixture identities with OpenSSL.
        let mut loose = format!("blob {}\0", bytes.len()).into_bytes();
        loose.extend_from_slice(bytes.as_bytes());
        let oid = ObjectId::from(openssl::sha::sha1(&loose));
        batch
            .insert(
                UnitKey::Object {
                    oid,
                    kind: Kind::Blob,
                },
                bytes.as_bytes(),
                &control,
            )
            .unwrap();
    }
    let mut out = [0; 64];
    let size = batch
        .load(mapping, &mut out, &control)
        .unwrap()
        .expect("metadata erased a not-yet-consumed mapping");
    assert_eq!(&out[..size], b"complete mapping store fixture");
    assert!(batch.len() <= RECORDS);
    drop(batch);
    assert_eq!(budget.used(), 0);
}

#[test]
fn imported_physical_bytes_survive_metadata_pressure_until_first_owned_copy() {
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let control = control();
    let scope = scope(&[7; 32], b"alice");
    let mut original = BatchSnapshot::new(budget.clone()).unwrap();
    original.insert(hello(), b"hello\n", &control).unwrap();
    let frame = original.encode(&scope, &control).unwrap();
    let mut restored =
        BatchSnapshot::decode(&[frame.as_slice()], &scope, budget.clone(), &control).unwrap();
    for n in 0..8192 {
        let bytes = format!("other physical object {n}");
        let mut loose = format!("blob {}\0", bytes.len()).into_bytes();
        loose.extend_from_slice(bytes.as_bytes());
        let oid = ObjectId::from(openssl::sha::sha1(&loose));
        restored
            .insert(
                UnitKey::Object {
                    oid,
                    kind: Kind::Blob,
                },
                bytes.as_bytes(),
                &control,
            )
            .unwrap();
    }
    let UnitKey::Object { oid, kind } = hello() else {
        unreachable!()
    };
    let object = restored
        .object(oid, kind, &control)
        .unwrap()
        .expect("imported physical object was displaced before first use");
    assert_eq!(object.data.as_slice(), b"hello\n");
    assert_eq!(restored.imported_objects_reused, 1);
    control.cancel();
    let held = budget.used();
    assert!(restored.object(oid, kind, &control).is_err());
    assert_eq!(budget.used(), held);
    drop(restored);
    drop(frame);
    drop(original);
    assert_eq!(budget.used(), object.data.capacity());
    drop(object);
    assert_eq!(budget.used(), 0);
}

#[test]
fn scope_producer_options_and_authorized_writer_are_not_interchangeable() {
    let budget = CapacityBudget::new(2 * 1024 * 1024);
    let control = control();
    let original = scope(&[7; 32], b"alice");
    let mut batch = BatchSnapshot::new(budget.clone()).unwrap();
    batch.insert(hello(), b"hello\n", &control).unwrap();
    let frame = batch.encode(&original, &control).unwrap();
    let mut foreign = original.clone();
    foreign.producer[0] ^= 1;
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &foreign, budget.clone(), &control).is_err()
    );
    foreign = original.clone();
    foreign.options[0] ^= 1;
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &foreign, budget.clone(), &control).is_err()
    );
    let mut config = CacheConfig::new(
        "11111111-1111-1111-1111-111111111111".into(),
        "33333333-3333-3333-3333-333333333333".into(),
    );
    foreign = original.clone();
    foreign.writer = WriterProof::from_key(&config, &[7; 32]).unwrap();
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &foreign, budget.clone(), &control).is_err()
    );
    config.namespace = "44444444-4444-4444-4444-444444444444".into();
    foreign.writer = WriterProof::from_key(&config, &[7; 32]).unwrap();
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &foreign, budget.clone(), &control).is_err()
    );
    drop(frame);
    drop(batch);
    assert_eq!(budget.used(), 0);
}

#[test]
#[ignore = "requires a protected writable key-test root"]
fn key_file_enforces_bounds_permissions_ancestors_and_rotation() {
    use std::os::unix::fs::PermissionsExt;
    let root = std::env::var_os("GIT_RS_KEY_TEST_ROOT")
        .map(std::path::PathBuf::from)
        .expect("protected qualified key-test root required");
    let directory = root.join(format!(
        "key-case-{}",
        crate::scratch::identifier().unwrap()
    ));
    fs::create_dir(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let path = directory.join("key");
    let config = CacheConfig::new(
        "11111111-1111-1111-1111-111111111111".into(),
        "22222222-2222-2222-2222-222222222222".into(),
    );
    fs::write(&path, [7; 32]).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let old = WriterProof::load(&config, &path).unwrap();
    assert!(WriterProof::load(&config, Path::new("relative-key")).is_err());
    for length in [0, 31, 33] {
        fs::write(&path, vec![7; length]).unwrap();
        assert!(WriterProof::load(&config, &path).is_err());
    }
    fs::write(&path, [7; 32]).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    assert!(WriterProof::load(&config, &path).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o770)).unwrap();
    assert!(WriterProof::load(&config, &path).is_err());
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let link = directory.join("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(WriterProof::load(&config, &link).is_err());
    let budget = CapacityBudget::new(2 * 1024 * 1024);
    let control = control();
    let mut original = scope(&[7; 32], b"alice");
    original.writer = old;
    let batch = BatchSnapshot::new(budget.clone()).unwrap();
    let frame = batch.encode(&original, &control).unwrap();
    fs::write(&path, [8; 32]).unwrap();
    let mut rotated = original.clone();
    rotated.writer = WriterProof::load(&config, &path).unwrap();
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &rotated, budget.clone(), &control).is_err()
    );
    assert!(
        BatchSnapshot::decode(&[frame.as_slice()], &original, budget.clone(), &control).is_ok(),
        "old live signer is trusted until process reload"
    );
}
