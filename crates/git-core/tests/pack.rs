use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::pack::PackSet;
use git_rs_core::{CapacityBudget, Control, ReadLimits};

// Tiny synthetic pack fixtures. These writers intentionally use fixed Git v2
// entry headers and v1 indexes; product decoding remains entirely in gix-pack.
// Stored DEFLATE blocks keep corruption/offset fixtures independently readable.
fn compressed(bytes: &[u8]) -> Vec<u8> {
    let length = u16::try_from(bytes.len()).unwrap();
    let mut out = vec![0x78, 0x01, 0x01];
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&(!length).to_le_bytes());
    out.extend_from_slice(bytes);
    let (mut a, mut b) = (1u32, 0u32);
    for byte in bytes {
        a = (a + u32::from(*byte)) % 65521;
        b = (b + a) % 65521;
    }
    out.extend_from_slice(&((b << 16) | a).to_be_bytes());
    out
}

fn checksum(bytes: &[u8]) -> gix_hash::ObjectId {
    let mut hash = gix_hash::hasher(gix_hash::Kind::Sha1);
    hash.update(bytes);
    hash.try_finalize().unwrap()
}

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "git-core-pack-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, entries: &[(gix_hash::ObjectId, Vec<u8>)]) -> std::path::PathBuf {
        let mut pack =
            gix_pack::data::header::encode(gix_pack::data::Version::V2, entries.len() as u32)
                .to_vec();
        let mut offsets = Vec::new();
        for (id, entry) in entries {
            offsets.push((*id, pack.len() as u32));
            pack.extend_from_slice(entry);
        }
        let pack_hash = checksum(&pack);
        pack.extend_from_slice(pack_hash.as_slice());
        offsets.sort_by_key(|(id, _)| *id);
        let mut index = Vec::new();
        for byte in 0..256 {
            let count = offsets
                .iter()
                .filter(|(id, _)| usize::from(id.as_slice()[0]) <= byte)
                .count() as u32;
            index.extend_from_slice(&count.to_be_bytes());
        }
        for (id, offset) in offsets {
            index.extend_from_slice(&offset.to_be_bytes());
            index.extend_from_slice(id.as_slice());
        }
        index.extend_from_slice(pack_hash.as_slice());
        let index_hash = checksum(&index);
        index.extend_from_slice(index_hash.as_slice());
        let path = self.0.join(name).with_extension("idx");
        std::fs::write(&path, index).unwrap();
        std::fs::write(path.with_extension("pack"), pack).unwrap();
        path
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn blob(bytes: &[u8]) -> gix_hash::ObjectId {
    gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, bytes).unwrap()
}

fn full_hello() -> Vec<u8> {
    let mut entry = vec![0x35]; // blob, five bytes
    entry.extend_from_slice(&compressed(b"hello"));
    entry
}

fn ref_delta(base: gix_hash::ObjectId) -> Vec<u8> {
    let mut entry = vec![0x76]; // REF_DELTA, six instruction bytes
    entry.extend_from_slice(base.as_slice());
    entry.extend_from_slice(&compressed(&[5, 6, 0x90, 5, 1, b'!']));
    entry
}

#[test]
fn cross_pack_ref_delta_and_symlinked_indexes_are_found_explicitly() {
    let root = Directory::new();
    let base = blob(b"hello");
    let result = blob(b"hello!");
    let base_index = root.write("base", &[(base, full_hello())]);
    let result_index = root.write("delta", &[(result, ref_delta(base))]);
    let link = root.0.join("linked.idx");
    std::os::unix::fs::symlink(&result_index, &link).unwrap();
    std::os::unix::fs::symlink(
        result_index.with_extension("pack"),
        link.with_extension("pack"),
    )
    .unwrap();
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let packs = PackSet::open(
        [link, base_index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(packs.locate(&result, &control).unwrap().unwrap().pack, 0);
    let object = packs
        .decode(
            &result,
            &control,
            &CapacityBudget::new(512 * 1024 * 1024),
            ReadLimits::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(object.kind, gix_object::Kind::Blob);
    assert_eq!(object.data.as_slice(), b"hello!");
    let shallow = ReadLimits {
        max_delta_depth: 0,
        ..ReadLimits::default()
    };
    assert!(
        packs
            .decode(
                &result,
                &control,
                &CapacityBudget::new(512 * 1024 * 1024),
                shallow
            )
            .is_err()
    );
    let small = ReadLimits {
        max_decode_bytes: 4,
        ..ReadLimits::default()
    };
    assert!(
        packs
            .decode(
                &result,
                &control,
                &CapacityBudget::new(512 * 1024 * 1024),
                small
            )
            .is_err()
    );
}

#[test]
fn local_ofs_delta_uses_maintained_decoder_and_missing_bases_fail() {
    let root = Directory::new();
    let base_entry = full_hello();
    let mut delta = vec![0x66, u8::try_from(base_entry.len()).unwrap()];
    delta.extend_from_slice(&compressed(&[5, 6, 0x90, 5, 1, b'!']));
    let result = blob(b"hello!");
    let index = root.write("ofs", &[(blob(b"hello"), base_entry), (result, delta)]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let packs = PackSet::open(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(
        packs
            .decode(
                &result,
                &control,
                &CapacityBudget::new(512 * 1024 * 1024),
                ReadLimits::default()
            )
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!"
    );
    let root = Directory::new();
    let index = root.write("missing", &[(result, ref_delta(blob(b"absent")))]);
    let packs = PackSet::open(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
    )
    .unwrap();
    assert!(
        packs
            .decode(
                &result,
                &control,
                &CapacityBudget::new(512 * 1024 * 1024),
                ReadLimits::default()
            )
            .is_err()
    );
}

#[test]
fn corrupt_cross_pack_cycle_is_an_error_not_unbounded_recursion() {
    let root = Directory::new();
    let a = blob(b"claimed-a");
    let b = blob(b"claimed-b");
    let one = root.write("one", &[(a, ref_delta(b))]);
    let two = root.write("two", &[(b, ref_delta(a))]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let packs = PackSet::open(
        [one, two],
        root,
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
    )
    .unwrap();
    assert!(
        packs
            .decode(
                &a,
                &control,
                &CapacityBudget::new(512 * 1024 * 1024),
                ReadLimits::default()
            )
            .unwrap_err()
            .to_string()
            .contains("cycle")
    );
}

struct Pin(Arc<AtomicBool>);
impl Drop for Pin {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[test]
fn explicit_empty_catalog_has_no_virtual_objects_and_retains_its_guard() {
    let alive = Arc::new(AtomicBool::new(true));
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let packs = PackSet::open(
        [],
        Pin(alive.clone()),
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
    )
    .unwrap();
    let empty = gix_hash::ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap();
    assert!(packs.locate(&empty, &control).unwrap().is_none());
    assert!(
        packs
            .decode(
                &empty,
                &control,
                &CapacityBudget::new(1024),
                ReadLimits::default()
            )
            .unwrap()
            .is_none()
    );
    assert!(alive.load(Ordering::Acquire));
    drop(packs);
    assert!(!alive.load(Ordering::Acquire));
}

#[test]
fn real_sealed_memfd_full_reads_hold_source_pins_through_cancellation() {
    use git_rs_core::pack::PackProvider;
    use git_rs_core::{SnapshotIdentity, SnapshotSession};
    use memfd::{FileSeal, MemfdOptions};
    use std::io::{Seek, Write};
    use std::os::fd::AsRawFd;

    struct SealedSource {
        _pins: Vec<memfd::Memfd>,
        _directory: Directory,
    }
    let root = Directory::new();
    let id = blob(b"hello");
    let ordinary = root.write("ordinary", &[(id, full_hello())]);
    let sealed_index = root.0.join("sealed.idx");
    let mut pins = Vec::new();
    for extension in ["idx", "pack"] {
        let bytes = std::fs::read(ordinary.with_extension(extension)).unwrap();
        let memfd = MemfdOptions::default()
            .allow_sealing(true)
            .create("synthetic-pack")
            .unwrap();
        memfd.as_file().set_len(bytes.len() as u64).unwrap();
        let mut writer = memfd.as_file();
        writer.write_all(&bytes).unwrap();
        memfd
            .add_seals(&[
                FileSeal::SealGrow,
                FileSeal::SealShrink,
                FileSeal::SealWrite,
                FileSeal::SealSeal,
            ])
            .unwrap();
        writer.rewind().unwrap();
        assert!(
            writer.write_all(b"X").is_err(),
            "the fixture must actually be sealed against writes"
        );
        let backing = format!("/proc/{}/fd/{}", std::process::id(), memfd.as_raw_fd());
        std::os::unix::fs::symlink(backing, sealed_index.with_extension(extension)).unwrap();
        pins.push(memfd);
    }
    // Remove the ordinary files so a discovery/fallback mutation cannot pass.
    std::fs::remove_file(ordinary.with_extension("idx")).unwrap();
    std::fs::remove_file(ordinary.with_extension("pack")).unwrap();
    let source = Arc::new(SealedSource {
        _pins: pins,
        _directory: root,
    });
    let weak = Arc::downgrade(&source);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let limits = ReadLimits {
        max_decode_bytes: 4096,
        ..ReadLimits::default()
    };
    let capacity = CapacityBudget::new(64 * 1024);
    let packs = PackSet::open(
        [sealed_index],
        source.clone(),
        gix_hash::Kind::Sha1,
        &control,
        limits,
    )
    .unwrap();
    let identity = SnapshotIdentity {
        source: b"sealed-synthetic".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix_hash::Kind::Sha1,
    };
    let provider = PackProvider::new(identity.clone(), packs, capacity.clone(), limits);
    let session = SnapshotSession::open(
        provider,
        &identity,
        control.clone(),
        capacity.clone(),
        limits,
    )
    .unwrap();
    drop(source);
    assert!(weak.upgrade().is_some());
    let body = session.object(&id).unwrap().unwrap();
    assert_eq!(body.kind, gix_object::Kind::Blob);
    assert_eq!(body.data.as_slice(), b"hello");
    let mut buffer = session.find_buffer(5).unwrap();
    assert_eq!(
        session.find_in(&id, &mut buffer).unwrap().unwrap().data,
        b"hello"
    );
    drop(buffer);
    let executor = git_rs_core::CpuExecutor::new(git_rs_core::ExecutorConfig {
        workers: 1,
        queue_capacity: 1,
    })
    .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let (stopped_tx, stopped_rx) = std::sync::mpsc::sync_channel(1);
    let ticket = executor
        .submit(
            control.clone(),
            git_rs_core::JobOptions::default(),
            move |_, _| {
                let failed = |error: &dyn std::fmt::Display| {
                    git_rs_core::JobError::Failed(error.to_string())
                };
                entered_tx.send(()).map_err(|error| failed(&error))?;
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| failed(&error))?;
                let rejected = session.object(&id).is_err();
                stopped_tx.send(rejected).map_err(|error| failed(&error))?;
                Ok(())
            },
        )
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    ticket.cancel(); // Cancellation is explicit; dropping a waiter alone is not cancellation.
    drop(ticket); // Neither action may drop captured source pins before CPU cleanup.
    assert!(
        weak.upgrade().is_some(),
        "a cancelled ticket must not release sealed-RAM source pins before CPU acknowledgment"
    );
    release_tx.send(()).unwrap();
    assert!(
        stopped_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "cancelled full read must fail, not return bytes"
    );
    drop(executor); // joins actual worker completion, not only waiter cancellation.
    assert!(weak.upgrade().is_none());
    assert!(
        capacity.used() >= 5,
        "returned owned bytes retain actual capacity independently of source mappings"
    );
    drop(body);
    assert_eq!(capacity.used(), 0);
}

#[test]
fn mixed_cross_pack_ref_and_local_ofs_share_depth_and_expansion_budgets() {
    let root = Directory::new();
    let base = blob(b"hello");
    let middle = blob(b"hello!");
    let target = blob(b"hello!!");
    let full = full_hello();
    let mut ofs = vec![0x66, full.len() as u8];
    ofs.extend_from_slice(&compressed(&[5, 6, 0x90, 5, 1, b'!']));
    let one = root.write("local", &[(base, full), (middle, ofs)]);
    let mut external = vec![0x76];
    external.extend_from_slice(middle.as_slice());
    external.extend_from_slice(&compressed(&[6, 7, 0x90, 6, 1, b'!']));
    let two = root.write("external", &[(target, external)]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let limits = ReadLimits {
        max_decode_bytes: 4096,
        max_delta_depth: 2,
        ..ReadLimits::default()
    };
    let budget = CapacityBudget::new(64 * 1024);
    let packs = PackSet::open([two, one], root, gix_hash::Kind::Sha1, &control, limits).unwrap();
    let bytes = packs
        .decode(&target, &control, &budget, limits)
        .unwrap()
        .unwrap();
    assert_eq!(bytes.data.as_slice(), b"hello!!");
    drop(bytes);
    assert_eq!(budget.used(), 0);
    let shallow = ReadLimits {
        max_delta_depth: 1,
        ..limits
    };
    assert!(
        packs
            .decode(&target, &control, &budget, shallow)
            .unwrap_err()
            .to_string()
            .contains("depth")
    );
    assert_eq!(budget.used(), 0);
    // 5 full-base bytes + 12 inflated instruction bytes + 6 + 7 result bytes.
    let exact = ReadLimits {
        max_decode_bytes: 30,
        ..limits
    };
    assert_eq!(
        packs
            .decode(&target, &control, &budget, exact)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!!"
    );
    let short = ReadLimits {
        max_decode_bytes: 29,
        ..limits
    };
    assert!(
        packs
            .decode(&target, &control, &budget, short)
            .unwrap_err()
            .to_string()
            .contains("byte budget")
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn corrupt_index_identity_never_returns_decoded_bytes_or_leaks_admission() {
    let root = Directory::new();
    let claimed = blob(b"different");
    let index = root.write("wrong-identity", &[(claimed, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let limits = ReadLimits {
        max_decode_bytes: 4096,
        ..ReadLimits::default()
    };
    let packs = PackSet::open([index], root, gix_hash::Kind::Sha1, &control, limits).unwrap();
    let budget = CapacityBudget::new(64 * 1024);
    assert!(
        packs
            .decode(&claimed, &control, &budget, limits)
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    assert_eq!(budget.used(), 0);
}
