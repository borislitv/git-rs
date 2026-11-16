use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::pack::{DecoderOptions, PackSet};
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
fn invalid_delta_identity_is_never_published_even_with_a_verified_warm_base() {
    let root = Directory::new();
    let base = blob(b"hello");
    let wrong = blob(b"hello?");
    let one = root.write("verified-base", &[(base, full_hello())]);
    // The maintained decoder produces hello!, not the identity claimed here.
    let two = root.write("wrong-delta-id", &[(wrong, ref_delta(base))]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [one, two],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    drop(packs.decode(&base, &control, &budget, limits).unwrap());
    let pin = packs
        .cached_object(&base, &control, limits)
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        assert!(
            packs
                .header(&wrong, &control, &budget, limits)
                .unwrap_err()
                .to_string()
                .contains("hash mismatch")
        );
        assert!(
            packs
                .decode(&wrong, &control, &budget, limits)
                .unwrap_err()
                .to_string()
                .contains("hash mismatch")
        );
        assert!(
            packs
                .cached_object(&wrong, &control, limits)
                .unwrap()
                .is_none()
        );
        assert_eq!(pin.as_slice(), b"hello");
        let stats = packs.cache_stats(&control).unwrap();
        assert_eq!(
            stats.entries, 1,
            "failed identity must not publish a cache entry"
        );
        assert_eq!(
            stats.header_entries, 1,
            "failed identity must not publish a header memo"
        );
    }
    control.cancel();
    assert!(packs.cached_object(&base, &control, limits).is_err());
    assert_eq!(pin.as_slice(), b"hello");
    drop(packs);
    assert!(budget.used() >= pin.capacity());
    drop(pin);
    assert_eq!(budget.used(), 0);
}

fn full_blob(bytes: &[u8]) -> Vec<u8> {
    let mut size = bytes.len();
    let mut first = 0x30 | (size & 15) as u8;
    size >>= 4;
    if size != 0 {
        first |= 0x80;
    }
    let mut entry = vec![first];
    while size != 0 {
        let mut byte = (size & 127) as u8;
        size >>= 7;
        if size != 0 {
            byte |= 0x80;
        }
        entry.push(byte);
    }
    entry.extend_from_slice(&compressed(bytes));
    entry
}

#[test]
fn complete_blame_moves_owned_and_pins_cached_physical_blobs_with_identical_porcelain() {
    use git_rs_core::objects::{Result, VerifiedObject};
    use git_rs_core::{
        BlameOptions, BlameRequest, Porcelain, SnapshotIdentity, SnapshotSession,
        VerifiedObjectProvider,
    };
    use gix_hash::{ObjectId, oid};
    use gix_object::{Header, Kind};

    struct Source {
        identity: SnapshotIdentity,
        packs: Arc<PackSet<()>>,
        budget: CapacityBudget,
        base: ObjectId,
        panic_base: Arc<AtomicBool>,
    }
    impl VerifiedObjectProvider for Source {
        fn identity(&self) -> &SnapshotIdentity {
            &self.identity
        }
        fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
            // The session's limit/budget identities are fixed by this fixture.
            self.packs.header(
                id,
                control,
                &self.budget,
                BlameOptions::default().read_limits(),
            )
        }
        fn object(
            &self,
            _: &oid,
            _: &Control,
            _: &CapacityBudget,
            _: ReadLimits,
        ) -> Result<Option<git_rs_core::Object>> {
            panic!("overridden verified_object must not fall back to mutable provider reads")
        }
        fn verified_object(
            &self,
            id: &oid,
            control: &Control,
            budget: &CapacityBudget,
            limits: ReadLimits,
        ) -> Result<Option<VerifiedObject>> {
            assert!(
                !(id == self.base.as_ref() && self.panic_base.load(Ordering::Relaxed)),
                "physical source panic while a cached opposite-side raw owner is live"
            );
            self.packs.decode_verified(id, control, budget, limits)
        }
    }
    struct MissMemo {
        stores: usize,
        packs: Option<Arc<PackSet<()>>>,
        control: Control,
    }
    impl git_rs_core::blame::memo::MappingMemo for MissMemo {
        fn load(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &mut [u8],
        ) -> std::result::Result<Option<usize>, git_rs_core::BlameError> {
            if let Some(packs) = &self.packs {
                assert_eq!(
                    packs.cache_stats(&self.control).unwrap().pinned_entries,
                    2,
                    "both physical raw blobs must remain pinned during read-only mapping, not copied into mutable objects"
                );
            }
            Ok(None)
        }
        fn completed(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &[u8],
        ) -> std::result::Result<(), git_rs_core::BlameError> {
            self.stores += 1;
            Ok(())
        }
    }
    struct CancelPinnedMemo {
        packs: Arc<PackSet<()>>,
        control: Control,
        observed_two_pins: bool,
    }
    impl git_rs_core::blame::memo::MappingMemo for CancelPinnedMemo {
        fn load(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &mut [u8],
        ) -> std::result::Result<Option<usize>, git_rs_core::BlameError> {
            assert_eq!(
                self.packs
                    .cache_stats(&self.control)
                    .unwrap()
                    .pinned_entries,
                2,
                "cancel only after both actual warmed raw owners are live"
            );
            self.observed_two_pins = true;
            self.control.cancel();
            // Exercise the public callback bridge's strict post-call check,
            // rather than manufacturing a stop error in the fixture itself.
            Ok(None)
        }
        fn completed(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &[u8],
        ) -> std::result::Result<(), git_rs_core::BlameError> {
            panic!("cancelled raw mapping cannot publish a completed unit")
        }
    }
    let root = Directory::new();
    let mut entries = Vec::new();
    let mut insert = |kind: Kind, bytes: Vec<u8>| -> ObjectId {
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap();
        let mut entry = full_blob(&bytes);
        let code = match kind {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
            Kind::Tag => 4,
        };
        entry[0] = (entry[0] & !0x70) | (code << 4);
        entries.push((id, entry));
        id
    };
    let base_blob = insert(Kind::Blob, b"one\ntwo\n".to_vec());
    let tip_blob = insert(Kind::Blob, b"one\nnew\n".to_vec());
    let mut commit = |blob: ObjectId, parent: Option<ObjectId>, time| {
        let mut tree = b"100644 file\0".to_vec();
        tree.extend_from_slice(blob.as_bytes());
        let tree = insert(Kind::Tree, tree);
        let parent = parent.map_or(String::new(), |id| format!("parent {id}\n"));
        insert(Kind::Commit, format!("tree {tree}\n{parent}author Example <example@example.test> {time} +0000\ncommitter Example <example@example.test> {time} +0000\n\nPhysical history\n").into_bytes())
    };
    let first = commit(base_blob, None, 1);
    let tip = commit(tip_blob, Some(first), 2);
    let index = root.write("raw-blame", &entries);
    let mut expected = None;
    for cached in [false, true] {
        for memoized in [false, true] {
            let budget = CapacityBudget::new(8 * 1024 * 1024);
            let control = Control::new(Instant::now() + Duration::from_secs(60));
            let limits = BlameOptions::default().read_limits();
            let packs = if cached {
                PackSet::open_with_options(
                    [index.clone()],
                    (),
                    gix_hash::Kind::Sha1,
                    &control,
                    limits,
                    DecoderOptions {
                        cache_slots: 16,
                        cache_bytes: 64 * 1024,
                        max_cache_entry_bytes: 64 * 1024,
                        header_slots: 16,
                        retained_workspace_bytes: 1024,
                    },
                    budget.clone(),
                )
            } else {
                PackSet::open([index.clone()], (), gix_hash::Kind::Sha1, &control, limits)
            }
            .unwrap();
            if cached {
                for (id, _) in &entries {
                    drop(
                        packs
                            .decode_verified(id, &control, &budget, limits)
                            .unwrap()
                            .expect("physical warm object"),
                    );
                }
            }
            let pin = if cached {
                Some(
                    packs
                        .cached_object(&base_blob, &control, limits)
                        .unwrap()
                        .expect("warm verified base pin"),
                )
            } else {
                None
            };
            if let Some(pin) = &pin {
                assert_eq!(pin.as_slice(), b"one\ntwo\n");
            }
            drop(pin);
            let before = packs.cache_stats(&control).unwrap();
            let packs = Arc::new(packs);
            let identity = SnapshotIdentity {
                source: b"physical-raw-blame".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            };
            let panic_base = Arc::new(AtomicBool::new(false));
            let source = Source {
                identity: identity.clone(),
                packs: packs.clone(),
                budget: budget.clone(),
                base: base_blob,
                panic_base: panic_base.clone(),
            };
            let session =
                SnapshotSession::open(source, &identity, control.clone(), budget.clone(), limits)
                    .unwrap();
            let mut memo = MissMemo {
                stores: 0,
                packs: cached.then(|| packs.clone()),
                control: control.clone(),
            };
            let request = BlameRequest {
                commit: &tip,
                path: b"file",
            };
            let blame = if memoized {
                session.blame_complete_with_memo(
                    request,
                    &BlameOptions::default(),
                    git_rs_core::blame::memo::MemoryMappingRequest {
                        reuse: true,
                        memo: &mut memo,
                        boundary: None,
                    },
                )
            } else {
                session.blame_complete(request, &BlameOptions::default())
            }
            .unwrap();
            assert_eq!(blame.statistics().mapping.computations, 1);
            assert_eq!(memo.stores, usize::from(memoized));
            let after = packs.cache_stats(&control).unwrap();
            if cached {
                assert_eq!(
                    after.decodes, before.decodes,
                    "all physical bodies were served from verified immutable pins"
                );
                assert!(after.hits > before.hits);
                assert_eq!(
                    after.pinned_entries, 0,
                    "raw pins cannot escape through complete output"
                );
            }
            let body = Porcelain::encode_complete(
                &blame,
                &control,
                &budget,
                git_rs_core::blame::MAX_BODY_BYTES,
            )
            .unwrap();
            let bytes: Vec<_> = body.chunks().flatten().copied().collect();
            let text = std::str::from_utf8(&bytes).unwrap();
            let origins: Vec<_> = text
                .lines()
                .filter(|line| {
                    line.split(' ').next().is_some_and(|word| {
                        word.len() == 40 && word.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
                })
                .collect();
            assert_eq!(origins, [format!("{first} 1 1 1"), format!("{tip} 2 2 1")]);
            if let Some(expected) = &expected {
                assert_eq!(
                    &bytes, expected,
                    "owned/pinned and memo/no-memo must preserve complete porcelain"
                );
            }
            expected = Some(bytes);
            drop(body);
            drop(blame);
            drop(memo);
            if cached {
                let baseline = budget.used();
                panic_base.store(true, Ordering::Relaxed);
                let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    session.blame_complete(
                        BlameRequest {
                            commit: &tip,
                            path: b"file",
                        },
                        &BlameOptions::default(),
                    )
                }));
                assert!(
                    failed.is_err(),
                    "reach a provider panic on old-side acquisition"
                );
                assert_eq!(
                    packs.cache_stats(&control).unwrap().pinned_entries,
                    0,
                    "unwind releases the opposite raw cache pin"
                );
                assert_eq!(
                    budget.used(),
                    baseline,
                    "unwind releases operation metadata and scratch, not source cache admission"
                );
                panic_base.store(false, Ordering::Relaxed);
                let mut cancel_memo = CancelPinnedMemo {
                    packs: packs.clone(),
                    control: control.clone(),
                    observed_two_pins: false,
                };
                let cancelled = session.blame_complete_with_memo(
                    BlameRequest {
                        commit: &tip,
                        path: b"file",
                    },
                    &BlameOptions::default(),
                    git_rs_core::blame::memo::MemoryMappingRequest {
                        reuse: true,
                        memo: &mut cancel_memo,
                        boundary: None,
                    },
                );
                assert!(
                    cancel_memo.observed_two_pins,
                    "reach the public load callback with two live physical cache pins"
                );
                assert_eq!(
                    cancelled.err(),
                    Some(git_rs_core::BlameError::Stopped(
                        git_rs_core::StopReason::Cancelled
                    )),
                    "request cancellation returns no complete output and preserves its typed cause"
                );
                assert_eq!(control.check(), Err(git_rs_core::StopReason::Cancelled));
                // The request's control is now stopped; cache inspection uses an
                // independent finite control, not a reset of request authority.
                let inspection = Control::new(Instant::now() + Duration::from_secs(60));
                assert_eq!(
                    packs.cache_stats(&inspection).unwrap().pinned_entries,
                    0,
                    "cancellation releases both raw owners"
                );
                assert_eq!(
                    budget.used(),
                    baseline,
                    "cancelled mapping releases its scratch and metadata ledger while source cache admission stays live"
                );
                drop(cancel_memo);
            }
            drop(session);
            drop(packs);
            assert_eq!(
                budget.used(),
                0,
                "all source, result and raw metadata admission is released"
            );
        }
    }
}

fn ofs_delta(base_distance: usize, base_size: u8, result_size: u8) -> Vec<u8> {
    ofs_delta_with_suffix(base_distance, base_size, result_size, b'!')
}

fn ofs_delta_with_suffix(
    base_distance: usize,
    base_size: u8,
    result_size: u8,
    suffix: u8,
) -> Vec<u8> {
    // All OFS fixture distances fit Git's single-byte distance encoding.
    let mut entry = vec![0x66, u8::try_from(base_distance).unwrap()];
    entry.extend_from_slice(&compressed(&[
        base_size,
        result_size,
        0x90,
        base_size,
        1,
        suffix,
    ]));
    entry
}

#[test]
fn cold_ofs_target_publishes_verified_intermediate_and_reuses_it_for_a_sibling() {
    let root = Directory::new();
    let base_entry = full_hello();
    let middle_entry = ofs_delta(base_entry.len(), 5, 6);
    let target_entry = ofs_delta(middle_entry.len(), 6, 7);
    let sibling_entry = ofs_delta_with_suffix(middle_entry.len() + target_entry.len(), 6, 7, b'?');
    let wrong_entry = ofs_delta(
        middle_entry.len() + target_entry.len() + sibling_entry.len(),
        6,
        7,
    );
    let middle = blob(b"hello!");
    let target = blob(b"hello!!");
    let sibling = blob(b"hello!?");
    let wrong = blob(b"hello??");
    let index = root.write(
        "cold-intermediate",
        &[
            (blob(b"hello"), base_entry),
            (middle, middle_entry),
            (target, target_entry),
            (sibling, sibling_entry),
            (wrong, wrong_entry),
        ],
    );
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        packs
            .decode(&target, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!!",
    );
    let pin = packs
        .cached_object(&middle, &control, limits)
        .unwrap()
        .expect("a cold target must expose its independently verified OFS intermediate");
    assert_eq!(pin.id(), middle);
    assert_eq!(pin.as_slice(), b"hello!");
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 2);
    assert_eq!(packs.cache_stats(&control).unwrap().decodes, 1);
    assert_eq!(
        packs
            .decode(&middle, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!",
    );
    let before = packs.cache_stats(&control).unwrap();
    assert_eq!(
        before.decodes, 1,
        "reading the intermediate must not decode again"
    );
    assert_eq!(
        packs
            .decode(&sibling, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!?",
    );
    let after = packs.cache_stats(&control).unwrap();
    assert_eq!(after.decodes, 2);
    assert_eq!(after.entries, 3);
    assert_eq!(
        after.hits - before.hits,
        2,
        "one internal base fetch plus the completed sibling's final pin"
    );
    assert!(
        packs
            .decode(&wrong, &control, &budget, limits)
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    assert!(
        packs
            .cached_object(&wrong, &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 3);
    control.cancel();
    assert!(packs.decode(&middle, &control, &budget, limits).is_err());
    assert_eq!(pin.as_slice(), b"hello!");
    drop(packs);
    assert!(budget.used() >= pin.capacity());
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn ofs_intermediate_publication_requires_its_exact_indexed_physical_location() {
    // Exercise both a missing digest and a digest indexed at another location.
    for duplicate_elsewhere in [false, true] {
        let root = Directory::new();
        let base_entry = full_hello();
        let middle_entry = ofs_delta(base_entry.len(), 5, 6);
        let target_entry = ofs_delta(middle_entry.len(), 6, 7);
        let middle = blob(b"hello!");
        let wrong = blob(b"hello?");
        let target = blob(b"hello!!");
        let mut entries = Vec::new();
        if duplicate_elsewhere {
            entries.push((middle, full_blob(b"hello!")));
        }
        entries.extend([
            (blob(b"hello"), base_entry),
            (wrong, middle_entry),
            (target, target_entry),
        ]);
        let index = root.write("unproven-intermediate", &entries);
        let control = Control::new(Instant::now() + Duration::from_secs(10));
        let budget = CapacityBudget::new(128 * 1024);
        let limits = ReadLimits::default();
        let packs = PackSet::open_with_options(
            [index],
            root,
            gix_hash::Kind::Sha1,
            &control,
            limits,
            acceleration(),
            budget.clone(),
        )
        .unwrap();
        assert_eq!(
            packs
                .decode(&target, &control, &budget, limits)
                .unwrap()
                .unwrap()
                .data
                .as_slice(),
            b"hello!!",
            "unprovable optional intermediate caching must not reject a valid target",
        );
        assert!(
            packs
                .cached_object(&wrong, &control, limits)
                .unwrap()
                .is_none()
        );
        assert!(
            packs
                .cached_object(&middle, &control, limits)
                .unwrap()
                .is_none()
        );
        assert_eq!(packs.cache_stats(&control).unwrap().entries, 1);
        assert!(
            packs
                .decode(&wrong, &control, &budget, limits)
                .unwrap_err()
                .to_string()
                .contains("hash mismatch")
        );
        assert_eq!(packs.cache_stats(&control).unwrap().entries, 1);
        drop(packs);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn discovered_ofs_intermediate_retains_original_chain_limits() {
    let root = Directory::new();
    let base_entry = full_hello();
    // Shrink a five-byte base to one byte, so checking only cached output size
    // would hide the original maximum object size and reconstruction work.
    let mut middle_entry = vec![0x64, u8::try_from(base_entry.len()).unwrap()];
    middle_entry.extend_from_slice(&compressed(&[5, 1, 1, b'x']));
    let target_entry = ofs_delta(middle_entry.len(), 1, 2);
    let middle = blob(b"x");
    let target = blob(b"x!");
    let index = root.write(
        "intermediate-limits",
        &[
            (blob(b"hello"), base_entry),
            (middle, middle_entry),
            (target, target_entry),
        ],
    );
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        packs
            .decode(&target, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"x!",
    );
    assert_eq!(
        packs
            .cached_object(&middle, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"x"
    );
    // Original middle: maximum object 5, expanded work 5+4+1=10,
    // depth 1, workspace 2*5+4=14; its cached payload itself is only one byte.
    for tighter in [
        ReadLimits {
            max_object_bytes: 4,
            ..limits
        },
        ReadLimits {
            max_decode_bytes: 9,
            ..limits
        },
        ReadLimits {
            max_decode_bytes: 13,
            ..limits
        },
        ReadLimits {
            max_delta_depth: 0,
            ..limits
        },
    ] {
        assert!(packs.cached_object(&middle, &control, tighter).is_err());
        assert!(packs.decode(&middle, &control, &budget, tighter).is_err());
    }
    assert_eq!(packs.cache_stats(&control).unwrap().decodes, 1);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn oversized_optional_ofs_intermediate_does_not_prevent_a_verified_target() {
    let root = Directory::new();
    let base_entry = full_hello();
    let middle_entry = ofs_delta(base_entry.len(), 5, 6);
    let target_entry = ofs_delta(middle_entry.len(), 6, 7);
    let middle = blob(b"hello!");
    let target = blob(b"hello!!");
    let index = root.write(
        "oversized-intermediate",
        &[
            (blob(b"hello"), base_entry),
            (middle, middle_entry),
            (target, target_entry),
        ],
    );
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            max_cache_entry_bytes: 5,
            retained_workspace_bytes: 0,
            ..acceleration()
        },
        budget.clone(),
    )
    .unwrap();
    let baseline = budget.used();
    assert_eq!(
        packs
            .decode(&target, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!!",
    );
    assert!(
        packs
            .cached_object(&middle, &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 0);
    assert_eq!(budget.used(), baseline);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn pinned_quota_denial_skips_optional_ofs_intermediate_without_losing_the_target() {
    let root = Directory::new();
    let mut indexes = Vec::new();
    for (name, base, middle, target) in [
        ("first", b"hello", b"hello!", b"hello!!"),
        ("second", b"world", b"world!", b"world!!"),
    ] {
        let base_entry = full_blob(base);
        let middle_entry = ofs_delta(base_entry.len(), 5, 6);
        let target_entry = ofs_delta(middle_entry.len(), 6, 7);
        indexes.push(root.write(
            name,
            &[
                (blob(base), base_entry),
                (blob(middle), middle_entry),
                (blob(target), target_entry),
            ],
        ));
    }
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        indexes,
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            cache_slots: 1,
            cache_bytes: 6,
            max_cache_entry_bytes: 6,
            retained_workspace_bytes: 0,
            ..acceleration()
        },
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        packs
            .decode(&blob(b"hello!!"), &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!!",
    );
    let pin = packs
        .cached_object(&blob(b"hello!"), &control, limits)
        .unwrap()
        .expect("the first intermediate owns the single cache slot");
    let pinned_usage = budget.used();
    assert_eq!(
        packs
            .decode(&blob(b"world!!"), &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"world!!",
    );
    assert!(
        packs
            .cached_object(&blob(b"world!"), &control, limits)
            .unwrap()
            .is_none()
    );
    let stats = packs.cache_stats(&control).unwrap();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.pinned_entries, 1);
    assert_eq!(pin.as_slice(), b"hello!");
    assert_eq!(budget.used(), pinned_usage);
    drop(packs);
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn optional_ofs_intermediate_cannot_spend_the_oversized_targets_return_buffer() {
    let root = Directory::new();
    let base_entry = full_hello();
    let middle_entry = ofs_delta(base_entry.len(), 5, 6);
    let expected = vec![b'x'; 32 * 1024];
    // Delta header: base length 6, result length 32768. Literal instructions
    // intentionally do not reuse the base; the physical dependency still exists.
    let mut instructions = vec![6, 0x80, 0x80, 0x02];
    for literal in expected.chunks(127) {
        instructions.push(u8::try_from(literal.len()).unwrap());
        instructions.extend_from_slice(literal);
    }
    // Reuse only the synthetic entry size/zlib writer, replacing the object kind
    // and inserting the one-byte OFS distance before the compressed stream.
    let mut target_entry = full_blob(&instructions);
    let header_end = target_entry
        .iter()
        .position(|byte| byte & 0x80 == 0)
        .unwrap()
        + 1;
    target_entry[0] = 0x60 | (target_entry[0] & 0x8f);
    target_entry.insert(header_end, u8::try_from(middle_entry.len()).unwrap());
    let target = blob(&expected);
    let index = root.write(
        "protected-return-buffer",
        &[
            (blob(b"hello"), base_entry),
            (blob(b"hello!"), middle_entry),
            (target, target_entry),
        ],
    );
    let limits = ReadLimits {
        max_delta_depth: 2,
        ..ReadLimits::default()
    };
    let attempt = |capacity, max_entry| {
        let budget = CapacityBudget::new(capacity);
        let control = Control::new(Instant::now() + Duration::from_secs(10));
        let result = (|| {
            let packs = PackSet::open_with_options(
                [index.clone()],
                (),
                gix_hash::Kind::Sha1,
                &control,
                limits,
                DecoderOptions {
                    cache_slots: 2,
                    cache_bytes: 64 * 1024,
                    max_cache_entry_bytes: max_entry,
                    header_slots: 0,
                    retained_workspace_bytes: 0,
                },
                budget.clone(),
            )
            .ok()?;
            let object = packs.decode(&target, &control, &budget, limits).ok()??;
            assert_eq!(object.data.as_slice(), expected.as_slice());
            let stats = packs.cache_stats(&control).unwrap();
            drop(object);
            drop(packs);
            Some(stats)
        })();
        assert_eq!(
            budget.used(),
            0,
            "all successful and denied attempts clean up"
        );
        result
    };
    // Find the actual baseline boundary, independently of private metadata sizes
    // and allocator capacity. Entry limit 5 excludes the six-byte intermediate;
    // entry limit 6 permits it, while the 32768-byte target is never eligible.
    let (mut low, mut high) = (0, 256 * 1024);
    assert!(attempt(high, 5).is_some());
    while low < high {
        let middle = low + (high - low) / 2;
        if attempt(middle, 5).is_some() {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    assert_eq!(attempt(low, 5).unwrap().entries, 0);
    let tight = attempt(low, 6)
        .expect("optional intermediate admission must not steal mandatory target backing");
    assert_eq!(
        tight.entries, 0,
        "deny the intermediate, not the target result"
    );
    let room = attempt(low + 4096, 6).expect("spare capacity permits optional publication");
    assert_eq!(
        room.entries, 1,
        "only the six-byte intermediate is cacheable"
    );
    let cacheable_target = attempt(low + 4096, expected.len())
        .expect("denied optional target copying must preserve mandatory result backing");
    assert_eq!(cacheable_target.entries, 1);
    let cached_target = attempt(low + 64 * 1024, expected.len())
        .expect("sufficient headroom preserves optional target caching");
    assert_eq!(cached_target.entries, 2);
}

#[test]
fn protected_ofs_result_stays_owned_through_mutable_conversion_with_retained_workspace() {
    fn literal_delta(distance: usize, base_size: usize, result: &[u8]) -> Vec<u8> {
        let mut instructions = Vec::new();
        for mut size in [base_size, result.len()] {
            loop {
                let byte = u8::try_from(size & 127).unwrap();
                size >>= 7;
                instructions.push(byte | if size == 0 { 0 } else { 0x80 });
                if size == 0 {
                    break;
                }
            }
        }
        for literal in result.chunks(127) {
            instructions.push(u8::try_from(literal.len()).unwrap());
            instructions.extend_from_slice(literal);
        }
        let mut entry = Vec::new();
        gix_pack::data::entry::Header::OfsDelta {
            base_distance: u64::try_from(distance).unwrap(),
        }
        .write_to(u64::try_from(instructions.len()).unwrap(), &mut entry)
        .unwrap();
        entry.extend_from_slice(&compressed(&instructions));
        entry
    }

    let root = Directory::new();
    let base_entry = full_hello();
    let middle_bytes = vec![b'm'; 16 * 1024];
    let target_bytes = vec![b't'; 32 * 1024];
    let middle_entry = literal_delta(base_entry.len(), 5, &middle_bytes);
    let target_entry = literal_delta(middle_entry.len(), middle_bytes.len(), &target_bytes);
    let middle = blob(&middle_bytes);
    let target = blob(&target_bytes);
    let index = root.write(
        "owned-mutable-result",
        &[
            (blob(b"hello"), base_entry),
            (middle, middle_entry),
            (target, target_entry),
        ],
    );
    let limits = ReadLimits {
        max_delta_depth: 2,
        ..ReadLimits::default()
    };
    let attempt = |capacity, cache_enabled, explicit_conversion| {
        let budget = CapacityBudget::new(capacity);
        let control = Control::new(Instant::now() + Duration::from_secs(10));
        let result = (|| {
            let packs = PackSet::open_with_options(
                [index.clone()],
                (),
                gix_hash::Kind::Sha1,
                &control,
                limits,
                DecoderOptions {
                    cache_slots: if cache_enabled { 2 } else { 0 },
                    cache_bytes: if cache_enabled { 64 * 1024 } else { 0 },
                    max_cache_entry_bytes: if cache_enabled { 32 * 1024 } else { 0 },
                    header_slots: 0,
                    retained_workspace_bytes: 256 * 1024,
                },
                budget.clone(),
            )
            .ok()?;
            let object = if explicit_conversion {
                // Exercise the witness-to-mutable seam used by sessions as well
                // as PackSet::decode. The fix must not depend on a decode flag.
                let verified = packs
                    .decode_verified(&target, &control, &budget, limits)
                    .ok()??;
                verified.into_object(&control, &budget).ok()?
            } else {
                packs.decode(&target, &control, &budget, limits).ok()??
            };
            assert_eq!(object.data.as_slice(), target_bytes.as_slice());
            let stats = packs.cache_stats(&control).unwrap();
            assert!(stats.workspace_capacity >= 2 * target_bytes.len());
            if cache_enabled {
                let pin = packs
                    .cached_object(&middle, &control, limits)
                    .unwrap()
                    .expect("the tight run must really retain the 16 KiB intermediate");
                assert_eq!(pin.as_slice(), middle_bytes.as_slice());
            }
            drop(object);
            drop(packs);
            Some(stats)
        })();
        assert_eq!(
            budget.used(),
            0,
            "retained workspace and all entries are released"
        );
        result
    };
    // Locate the cache-disabled boundary without copying internal admission
    // arithmetic. Add enough for the 16 KiB intermediate and fixed metadata,
    // but less than another mandatory 32 KiB mutable target allocation.
    let (mut low, mut high) = (0, 512 * 1024);
    assert!(attempt(high, false, false).is_some());
    while low < high {
        let middle = low + (high - low) / 2;
        if attempt(middle, false, false).is_some() {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let capacity = low + 24 * 1024;
    for explicit_conversion in [false, true] {
        assert_eq!(
            attempt(capacity, false, explicit_conversion)
                .unwrap()
                .entries,
            0
        );
        let enabled = attempt(capacity, true, explicit_conversion)
            .expect("optional caching cannot force a second mandatory mutable target copy");
        assert_eq!(
            enabled.entries, 1,
            "only the intermediate fits optional cache admission"
        );
    }
}

#[test]
fn maintained_controlled_cache_fetch_resolves_a_local_ofs_base() {
    let root = Directory::new();
    let base_entry = full_hello();
    let middle_entry = ofs_delta(base_entry.len(), 5, 6);
    let target_entry = ofs_delta(middle_entry.len(), 6, 7);
    let middle = blob(b"hello!");
    let target = blob(b"hello!!");
    let index = root.write(
        "local-cache",
        &[
            (blob(b"hello"), base_entry),
            (middle, middle_entry),
            (target, target_entry),
        ],
    );
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        packs
            .decode(&middle, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!"
    );
    let before = packs.cache_stats(&control).unwrap();
    assert_eq!(
        packs
            .decode(&target, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello!!"
    );
    let after = packs.cache_stats(&control).unwrap();
    assert_eq!(after.decodes, 2);
    assert!(after.hits > before.hits);
    // This observes the cache's public pin contract independently of vendor
    // outcome counters: neither object may alias the other's instruction offset.
    assert_eq!(
        packs
            .cached_object(&middle, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"hello!"
    );
    assert_eq!(
        packs
            .cached_object(&target, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"hello!!"
    );
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn optional_global_admission_denial_preserves_the_successful_uncached_result() {
    let root = Directory::new();
    let bytes = vec![b'x'; 4096];
    let id = blob(&bytes);
    let index = root.write("tight", &[(id, full_blob(&bytes))]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(64 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let options = DecoderOptions {
        retained_workspace_bytes: 0,
        ..acceleration()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    let baseline = budget.used();
    // Room for actual preflight, workspace, and returned bytes, but not an
    // additional cache owner and its copy at the insertion point.
    let blocker = budget
        .buffer(64 * 1024 - baseline - 2 * 4096 - 512)
        .unwrap();
    let charged = budget.used();
    let result = packs
        .decode(&id, &control, &budget, limits)
        .unwrap()
        .unwrap();
    assert_eq!(result.data.as_slice(), bytes);
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 0);
    drop(result);
    assert_eq!(budget.used(), charged);
    drop(blocker);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn concurrent_cross_pack_decodes_share_cache_without_recursive_lock_ownership() {
    let root = Directory::new();
    let base = blob(b"hello");
    let target = blob(b"hello!");
    let one = root.write("concurrent-base", &[(base, full_hello())]);
    let two = root.write("concurrent-target", &[(target, ref_delta(base))]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = Arc::new(
        PackSet::open_with_options(
            [one, two],
            root,
            gix_hash::Kind::Sha1,
            &control,
            limits,
            acceleration(),
            budget.clone(),
        )
        .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (packs, control, budget, barrier, sender) = (
            packs.clone(),
            control.clone(),
            budget.clone(),
            barrier.clone(),
            sender.clone(),
        );
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..16 {
                assert_eq!(
                    packs
                        .decode(&target, &control, &budget, limits)
                        .unwrap()
                        .unwrap()
                        .data
                        .as_slice(),
                    b"hello!"
                );
            }
            sender.send(()).unwrap();
        }));
    }
    for _ in 0..2 {
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 2);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

fn acceleration() -> DecoderOptions {
    DecoderOptions {
        cache_slots: 4,
        cache_bytes: 64 * 1024,
        max_cache_entry_bytes: 32 * 1024,
        header_slots: 4,
        retained_workspace_bytes: 64 * 1024,
    }
}

#[test]
fn cold_header_needs_workspace_and_cache_but_not_a_third_mutable_body() {
    const BODY_BYTES: usize = 32 * 1024;
    // Two 32 KiB bodies plus 16 KiB for bounded metadata; a third body alone
    // would require 96 KiB. These bounds do not depend on decoder statistics.
    const BUDGET_BYTES: usize = 80 * 1024;
    let root = Directory::new();
    let bytes = vec![b'x'; BODY_BYTES];
    let id = blob(&bytes);
    // Git v2 blob header for exactly 32768 bytes, followed by stored DEFLATE.
    let mut entry = vec![0xb0, 0x80, 0x10];
    entry.extend_from_slice(&compressed(&bytes));
    let index = root.write("header-two-bodies", &[(id, entry)]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(BUDGET_BYTES);
    let limits = ReadLimits {
        max_object_bytes: BODY_BYTES,
        max_decode_bytes: BODY_BYTES,
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            cache_slots: 1,
            cache_bytes: BODY_BYTES,
            max_cache_entry_bytes: BODY_BYTES,
            header_slots: 1,
            retained_workspace_bytes: BODY_BYTES,
        },
        budget.clone(),
    )
    .unwrap();
    let expected = gix_object::Header {
        kind: gix_object::Kind::Blob,
        size: BODY_BYTES as u64,
    };
    assert_eq!(
        packs.header(&id, &control, &budget, limits).unwrap(),
        Some(expected)
    );
    let stats = packs.cache_stats(&control).unwrap();
    assert_eq!(stats.workspace_capacity, BODY_BYTES);
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.decodes, 1);
    assert!(budget.used() >= 2 * BODY_BYTES);
    assert!(budget.used() <= BUDGET_BYTES);
    assert_eq!(
        packs.header(&id, &control, &budget, limits).unwrap(),
        Some(expected)
    );
    assert!(
        packs
            .header(&blob(b"absent"), &control, &budget, limits)
            .unwrap()
            .is_none()
    );
    for strict in [
        ReadLimits {
            max_object_bytes: BODY_BYTES - 1,
            ..limits
        },
        ReadLimits {
            max_decode_bytes: BODY_BYTES - 1,
            ..limits
        },
    ] {
        assert!(packs.header(&id, &control, &budget, strict).is_err());
    }
    let unrelated = CapacityBudget::new(BUDGET_BYTES);
    assert!(packs.header(&id, &control, &unrelated, limits).is_err());
    let pin = packs.cached_object(&id, &control, limits).unwrap().unwrap();
    assert_eq!(pin.as_slice(), bytes);
    assert!(
        packs.decode(&id, &control, &budget, limits).is_err(),
        "mutable output still needs its own admitted body"
    );
    control.cancel();
    assert!(packs.header(&id, &control, &budget, limits).is_err());
    drop(packs);
    assert_eq!(pin.as_slice(), bytes);
    assert!(budget.used() >= BODY_BYTES);
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn verified_pack_find_borrows_pinned_bytes_without_an_intermediate_object_allocation() {
    use git_rs_core::pack::PackProvider;
    use git_rs_core::{SnapshotIdentity, SnapshotSession};
    let root = Directory::new();
    let id = blob(b"hello");
    let index = root.write("immutable-find", &[(id, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    let identity = SnapshotIdentity {
        source: b"immutable-find".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix_hash::Kind::Sha1,
    };
    let provider = PackProvider::new(identity.clone(), packs, budget.clone(), limits);
    let session =
        SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)
            .unwrap();
    let witness = session.verified_object(&id).unwrap().unwrap();
    assert_eq!(witness.as_slice(), b"hello");
    let mut output = session.find_buffer(5).unwrap();
    let blocker = budget.buffer(128 * 1024 - budget.used()).unwrap();
    assert_eq!(budget.used(), 128 * 1024);
    assert_eq!(
        session.find_in(&id, &mut output).unwrap().unwrap().data,
        b"hello"
    );
    assert_eq!(
        budget.used(),
        128 * 1024,
        "the immutable Find path must not allocate a second body"
    );
    assert!(
        session.object(&id).is_err(),
        "a mutable Object must own an independent admitted copy"
    );
    drop(blocker);
    drop(output);
    drop(session);
    assert_eq!(witness.as_slice(), b"hello");
    assert!(budget.used() >= witness.capacity());
    let mutable = witness.into_object(&control, &budget).unwrap();
    assert_eq!(mutable.data.as_slice(), b"hello");
    assert_eq!(
        budget.used(),
        mutable.data.capacity(),
        "converting the last cache witness drops cache owners, not the new body"
    );
    drop(mutable);
    assert_eq!(budget.used(), 0);
}

#[test]
fn capacity_identity_is_shared_state_not_equal_limits_or_usage() {
    let budget = CapacityBudget::new(100);
    let cloned = budget.clone();
    let independent = CapacityBudget::new(100);
    assert!(budget.shares_with(&budget));
    assert!(budget.shares_with(&cloned));
    assert!(!budget.shares_with(&independent));
    let storage = cloned.buffer(19).unwrap();
    assert_eq!(budget.used(), storage.capacity());
    assert_eq!(independent.used(), 0);
    assert!(!cloned.shares_with(&independent));
    drop(storage);
    assert_eq!(budget.used(), 0);
}

#[test]
fn snapshot_pack_ids_are_exact_unique_decoder_identities_at_equal_offsets() {
    let root = Directory::new();
    let a = blob(b"hello");
    let b = blob(b"world");
    let one = root.write("one", &[(a, full_hello())]);
    let two = root.write("two", &[(b, full_blob(b"world"))]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [one, two],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    let first = packs.locate(&a, &control).unwrap().unwrap();
    let second = packs.locate(&b, &control).unwrap().unwrap();
    assert_eq!(first.offset, second.offset);
    assert_eq!(packs.source_pack_id(first), Some(0));
    assert_eq!(packs.source_pack_id(second), Some(1));
    for _ in 0..2 {
        for (id, expected) in [(a, b"hello"), (b, b"world")] {
            let object = packs
                .decode(&id, &control, &budget, limits)
                .unwrap()
                .unwrap();
            assert_eq!(object.data.as_slice(), expected);
        }
    }
    assert_eq!(packs.cache_stats(&control).unwrap().decodes, 2);
    assert_eq!(
        packs
            .cached_object(&a, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"hello"
    );
    assert_eq!(
        packs
            .cached_object(&b, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"world"
    );
    let other = CapacityBudget::new(128 * 1024);
    assert!(
        packs
            .decode(&a, &control, &other, limits)
            .unwrap_err()
            .to_string()
            .contains("shared capacity budget")
    );
    assert_eq!(other.used(), 0);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn tiny_pin_keeps_full_backing_and_metadata_after_snapshot_drop() {
    let root = Directory::new();
    let body = vec![b'a'; 4096];
    let id = blob(&body);
    let other = blob(b"hello");
    let index = root.write("pins", &[(id, full_blob(&body)), (other, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let options = DecoderOptions {
        cache_slots: 1,
        cache_bytes: 4096,
        max_cache_entry_bytes: 4096,
        retained_workspace_bytes: 0,
        ..acceleration()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    drop(
        packs
            .decode(&id, &control, &budget, limits)
            .unwrap()
            .unwrap(),
    );
    let pin = packs.cached_object(&id, &control, limits).unwrap().unwrap();
    let tiny = &pin.as_slice()[..1];
    assert_eq!(tiny, b"a");
    assert!(pin.capacity() >= body.len());
    let before = budget.used();
    drop(
        packs
            .decode(&other, &control, &budget, limits)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(packs.cache_stats(&control).unwrap().pinned_entries, 1);
    assert!(
        packs
            .cached_object(&other, &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        budget.used(),
        before,
        "rejected candidate storage/metadata must be released"
    );
    drop(packs);
    assert_eq!(tiny, b"a");
    assert!(budget.used() >= pin.capacity());
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn unpinned_slot_eviction_releases_old_capacity_and_publishes_new_object() {
    let root = Directory::new();
    let body = vec![b'a'; 4096];
    let old = blob(&body);
    let new = blob(b"hello");
    let index = root.write("evict", &[(old, full_blob(&body)), (new, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let options = DecoderOptions {
        cache_slots: 1,
        retained_workspace_bytes: 0,
        ..acceleration()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    drop(packs.decode(&old, &control, &budget, limits).unwrap());
    let before = budget.used();
    drop(packs.decode(&new, &control, &budget, limits).unwrap());
    assert!(
        packs
            .cached_object(&old, &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        packs
            .cached_object(&new, &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        b"hello"
    );
    assert!(budget.used() < before - 4000);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn controlled_headers_use_actual_workspace_and_only_verified_memos() {
    let root = Directory::new();
    let id = blob(b"hello");
    let index = root.write("tiny", &[(id, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(32 * 1024);
    let limits = ReadLimits::default(); // 256 MiB ceiling, deliberately much larger than this budget.
    let options = DecoderOptions {
        cache_slots: 0,
        cache_bytes: 0,
        max_cache_entry_bytes: 0,
        header_slots: 2,
        retained_workspace_bytes: 16,
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        packs
            .header(&id, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .size,
        5
    );
    let first = packs.cache_stats(&control).unwrap();
    assert_eq!(first.workspace_capacity, 5);
    assert_eq!(first.decodes, 1);
    // Leave no room for another preflight or full decode. A verified memo needs
    // neither output admission nor inflate and still observes per-call limits.
    let blocker = budget.buffer(32 * 1024 - budget.used()).unwrap();
    assert_eq!(
        packs
            .header(&id, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .kind,
        gix_object::Kind::Blob
    );
    let stricter = ReadLimits {
        max_object_bytes: 4,
        ..limits
    };
    assert!(packs.header(&id, &control, &budget, stricter).is_err());
    let stats = packs.cache_stats(&control).unwrap();
    assert_eq!(stats.decodes, 1);
    assert_eq!(stats.header_hits, 2);
    drop(blocker);
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn workspace_growth_requires_overlap_admission_then_reuses_large_small_large() {
    let root = Directory::new();
    let small = vec![b's'; 4096];
    let large = vec![b'l'; 32768];
    let a = blob(&small);
    let b = blob(&large);
    let index = root.write(
        "relocation",
        &[(a, full_blob(&small)), (b, full_blob(&large))],
    );
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let options = DecoderOptions {
        retained_workspace_bytes: 32768,
        ..DecoderOptions::default()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    drop(packs.decode(&a, &control, &budget, limits).unwrap());
    let steady = budget.used();
    assert_eq!(
        packs.cache_stats(&control).unwrap().workspace_capacity,
        4096
    );
    // Admission must include both old and replacement buffers during relocation.
    // Enough for the replacement only if the old 4 KiB charge is released
    // prematurely; the metadata allowance is deliberately just 1 KiB.
    let blocker = budget
        .buffer(128 * 1024 - steady - (32768 - 4096 + 1024))
        .unwrap();
    let denial = packs.decode(&b, &control, &budget, limits).unwrap_err();
    assert!(denial.to_string().contains("capacity"));
    assert_eq!(
        packs.cache_stats(&control).unwrap().workspace_capacity,
        4096
    );
    drop(blocker);
    assert_eq!(budget.used(), steady);
    for id in [b, a, b] {
        let object = packs
            .decode(&id, &control, &budget, limits)
            .unwrap()
            .unwrap();
        assert_eq!(
            object.data.as_slice(),
            if id == a { &small } else { &large }
        );
        drop(object);
        assert_eq!(
            packs.cache_stats(&control).unwrap().workspace_capacity,
            32768
        );
    }
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn cross_pack_cache_recursion_preserves_full_chain_limits_on_warm_hits() {
    use git_rs_core::objects::{Result, VerifiedObject};
    use git_rs_core::{Object, SnapshotIdentity, SnapshotSession, VerifiedObjectProvider};

    // A provider can move a witness into a different session, but cannot remove
    // the physical chain's expansion and depth requirements by doing so.
    struct ForwardedWitness {
        identity: SnapshotIdentity,
        value: std::sync::Mutex<Option<VerifiedObject>>,
    }
    impl VerifiedObjectProvider for ForwardedWitness {
        fn identity(&self) -> &SnapshotIdentity {
            &self.identity
        }
        fn header(&self, _: &gix_hash::oid, _: &Control) -> Result<Option<gix_object::Header>> {
            unreachable!()
        }
        fn object(
            &self,
            _: &gix_hash::oid,
            _: &Control,
            _: &CapacityBudget,
            _: ReadLimits,
        ) -> Result<Option<Object>> {
            unreachable!("verified read must not fall back to mutable bytes")
        }
        fn verified_object(
            &self,
            _: &gix_hash::oid,
            _: &Control,
            _: &CapacityBudget,
            _: ReadLimits,
        ) -> Result<Option<VerifiedObject>> {
            Ok(self.value.lock().unwrap().take())
        }
    }

    let root = Directory::new();
    let base = blob(b"hello");
    let middle = blob(b"hello!");
    let target = blob(b"hello!!");
    let one = root.write("base", &[(base, full_hello())]);
    let two = root.write("middle", &[(middle, ref_delta(base))]);
    let mut delta = vec![0x76];
    delta.extend_from_slice(middle.as_slice());
    delta.extend_from_slice(&compressed(&[6, 7, 0x90, 6, 1, b'!']));
    let three = root.write("target", &[(target, delta)]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 2,
        max_decode_bytes: 4096,
        ..ReadLimits::default()
    };
    let packs = PackSet::open_with_options(
        [one, two, three],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        acceleration(),
        budget.clone(),
    )
    .unwrap();
    drop(packs.decode(&base, &control, &budget, limits).unwrap());
    for _ in 0..2 {
        assert_eq!(
            packs
                .decode(&target, &control, &budget, limits)
                .unwrap()
                .unwrap()
                .data
                .as_slice(),
            b"hello!!"
        );
    }
    let stats = packs.cache_stats(&control).unwrap();
    assert_eq!(
        stats.decodes, 3,
        "one physical decode per verified object despite recursive warm bases"
    );
    assert_eq!(stats.entries, 3);
    for strict in [
        ReadLimits {
            max_delta_depth: 1,
            ..limits
        },
        ReadLimits {
            max_decode_bytes: 29,
            ..limits
        },
        ReadLimits {
            max_object_bytes: 6,
            ..limits
        },
    ] {
        assert!(packs.decode(&target, &control, &budget, strict).is_err());
        assert!(packs.header(&target, &control, &budget, strict).is_err());
        assert!(packs.cached_object(&target, &control, strict).is_err());
        assert!(
            packs
                .decode_verified(&target, &control, &budget, strict)
                .is_err()
        );
        let identity = SnapshotIdentity {
            source: b"forwarded-pack".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        };
        let provider = ForwardedWitness {
            identity: identity.clone(),
            value: std::sync::Mutex::new(
                packs
                    .decode_verified(&target, &control, &budget, limits)
                    .unwrap(),
            ),
        };
        let session =
            SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), strict)
                .unwrap();
        assert!(
            session.verified_object(&target).is_err(),
            "a forwarded witness must enforce the receiving session's {strict:?}"
        );
    }
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
    control.cancel();
    assert!(packs.decode(&target, &control, &budget, limits).is_err());
    assert!(packs.header(&target, &control, &budget, limits).is_err());
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn optional_quota_denial_never_turns_success_into_failure_or_memoizes_corruption() {
    let root = Directory::new();
    let id = blob(b"hello");
    let wrong = blob(b"wrong");
    let one = root.write("valid", &[(id, full_hello())]);
    let two = root.write("corrupt", &[(wrong, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(64 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let options = DecoderOptions {
        cache_slots: 1,
        cache_bytes: 4,
        max_cache_entry_bytes: 4,
        header_slots: 1,
        retained_workspace_bytes: 0,
    };
    let packs = PackSet::open_with_options(
        [one, two],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        options,
        budget.clone(),
    )
    .unwrap();
    let baseline = budget.used();
    assert_eq!(
        packs
            .decode(&id, &control, &budget, limits)
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello"
    );
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 0);
    assert_eq!(budget.used(), baseline);
    for _ in 0..2 {
        assert!(
            packs
                .header(&wrong, &control, &budget, limits)
                .unwrap_err()
                .to_string()
                .contains("hash mismatch")
        );
        assert_eq!(budget.used(), baseline);
    }
    assert_eq!(
        packs.cache_stats(&control).unwrap().header_entries,
        1,
        "only the valid identity has a memo"
    );
    drop(packs);
    assert_eq!(budget.used(), 0);
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

#[test]
fn header_ring_overwrites_individual_duplicates_without_stale_slot_aliases() {
    let root = Directory::new();
    let bodies: [&[u8]; 4] = [b"a", b"bb", b"ccc", b"dddd"];
    let ids = bodies.map(blob);
    let entries: Vec<_> = ids
        .iter()
        .zip(bodies)
        .map(|(&id, bytes)| (id, full_blob(bytes)))
        .collect();
    let index = root.write("header-ring", &entries);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(64 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            header_slots: 3,
            ..DecoderOptions::default()
        },
        budget.clone(),
    )
    .unwrap();
    // Decode, rather than header(), deliberately publishes A in two ring slots.
    for index in [0, 0, 1] {
        drop(
            packs
                .decode(&ids[index], &control, &budget, limits)
                .unwrap(),
        );
    }
    assert_eq!(packs.cache_stats(&control).unwrap().header_entries, 3);
    drop(packs.decode(&ids[2], &control, &budget, limits).unwrap()); // replace first A
    let check_headers_without_decode = |expected: &[(usize, u64)]| {
        let before = packs.cache_stats(&control).unwrap().decodes;
        let blocker = budget.buffer(64 * 1024 - budget.used()).unwrap();
        for &(index, size) in expected {
            assert_eq!(
                packs
                    .header(&ids[index], &control, &budget, limits)
                    .unwrap()
                    .unwrap()
                    .size,
                size
            );
        }
        assert_eq!(packs.cache_stats(&control).unwrap().decodes, before);
        drop(blocker);
    };
    check_headers_without_decode(&[(0, 1), (1, 2), (2, 3)]); // second A survives
    drop(packs.decode(&ids[3], &control, &budget, limits).unwrap()); // replace last A
    {
        let blocker = budget.buffer(64 * 1024 - budget.used()).unwrap();
        assert!(
            packs.header(&ids[0], &control, &budget, limits).is_err(),
            "an overwritten memo must miss, not return the new occupant's header"
        );
        assert!(
            packs
                .header(&blob(b"absent"), &control, &budget, limits)
                .unwrap()
                .is_none()
        );
        drop(blocker);
    }
    check_headers_without_decode(&[(1, 2), (2, 3), (3, 4)]);
    // More than one full wrap, in a non-key-sorted order.
    for index in [0, 3, 2, 1, 0, 2, 3, 1] {
        drop(
            packs
                .decode(&ids[index], &control, &budget, limits)
                .unwrap(),
        );
        check_headers_without_decode(&[(index, bodies[index].len() as u64)]);
    }
    // After these two writes the cursor wraps to zero. Repeated A fills slots
    // 1, 2, then 0. Replacing slot 1 must not remove the still-live A at slot 0.
    for index in [3, 3, 1, 0, 0, 0, 2] {
        drop(
            packs
                .decode(&ids[index], &control, &budget, limits)
                .unwrap(),
        );
    }
    check_headers_without_decode(&[(0, 1), (2, 3)]);
    control.cancel();
    assert!(packs.header(&ids[1], &control, &budget, limits).is_err());
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn indexed_slot_reuse_preserves_retained_pins_and_never_returns_evicted_keys() {
    let root = Directory::new();
    let bodies: [&[u8]; 4] = [b"alpha", b"bravo", b"charlie", b"delta"];
    let ids = bodies.map(blob);
    // All four physical objects have the same offset, in different packs.
    let indexes: Vec<_> = bodies
        .iter()
        .enumerate()
        .map(|(n, bytes)| root.write(&format!("indexed-{n}"), &[(ids[n], full_blob(bytes))]))
        .collect();
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(128 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        indexes,
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            cache_slots: 2,
            retained_workspace_bytes: 0,
            ..acceleration()
        },
        budget.clone(),
    )
    .unwrap();
    let offset = packs.locate(&ids[0], &control).unwrap().unwrap().offset;
    for id in &ids[1..] {
        assert_eq!(packs.locate(id, &control).unwrap().unwrap().offset, offset);
    }
    drop(packs.decode(&ids[0], &control, &budget, limits).unwrap());
    let pin = packs
        .cached_object(&ids[0], &control, limits)
        .unwrap()
        .unwrap();
    drop(packs.decode(&ids[1], &control, &budget, limits).unwrap());
    drop(packs.decode(&ids[2], &control, &budget, limits).unwrap());
    assert!(
        packs
            .cached_object(&ids[1], &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        packs
            .cached_object(&ids[2], &control, limits)
            .unwrap()
            .unwrap()
            .as_slice(),
        bodies[2]
    );
    assert_eq!(pin.as_slice(), bodies[0]);
    assert_eq!(packs.cache_stats(&control).unwrap().pinned_entries, 1);
    drop(pin);
    drop(packs.decode(&ids[3], &control, &budget, limits).unwrap());
    assert!(
        packs
            .cached_object(&ids[0], &control, limits)
            .unwrap()
            .is_none()
    );
    for index in [2, 3] {
        assert_eq!(
            packs
                .cached_object(&ids[index], &control, limits)
                .unwrap()
                .unwrap()
                .as_slice(),
            bodies[index]
        );
    }
    // Reinsert an evicted key in a reused slot, then check the old occupant.
    drop(packs.decode(&ids[1], &control, &budget, limits).unwrap());
    assert!(
        packs
            .cached_object(&ids[2], &control, limits)
            .unwrap()
            .is_none()
    );
    let pin = packs
        .cached_object(&ids[1], &control, limits)
        .unwrap()
        .unwrap();
    assert_eq!(pin.id(), ids[1]);
    control.cancel();
    assert!(packs.cached_object(&ids[1], &control, limits).is_err());
    drop(packs);
    assert_eq!(pin.as_slice(), bodies[1]);
    assert!(budget.used() >= pin.capacity());
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn fixed_slot_indexes_require_metadata_admission_before_any_object_is_read() {
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let options = DecoderOptions {
        cache_slots: 4096,
        cache_bytes: 4096,
        max_cache_entry_bytes: 1,
        ..DecoderOptions::default()
    };
    // Each body slot needs at least an owner pointer, plus its full pack/offset
    // key and slot number in the index. This lower bound ignores padding and all
    // runtime/header metadata; it is not a copy of the accounting implementation.
    let minimum = 4096 * (2 * std::mem::size_of::<usize>() + 4 + 8);
    let tight = CapacityBudget::new(minimum - 1);
    assert!(
        PackSet::open_with_options(
            [],
            (),
            gix_hash::Kind::Sha1,
            &control,
            ReadLimits::default(),
            options,
            tight.clone(),
        )
        .is_err(),
        "an index cannot be allocated outside admission"
    );
    assert_eq!(tight.used(), 0, "failed construction returns every charge");
    let budget = CapacityBudget::new(1024 * 1024);
    let packs = PackSet::open_with_options(
        [],
        (),
        gix_hash::Kind::Sha1,
        &control,
        ReadLimits::default(),
        options,
        budget.clone(),
    )
    .unwrap();
    assert!(budget.used() >= minimum);
    assert_eq!(packs.cache_stats(&control).unwrap().entries, 0);
    drop(packs);
    assert_eq!(budget.used(), 0);
    control.cancel();
    assert!(
        PackSet::open_with_options(
            [],
            (),
            gix_hash::Kind::Sha1,
            &control,
            ReadLimits::default(),
            options,
            budget.clone(),
        )
        .is_err()
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn zero_slot_indexes_keep_uncached_reads_missing_objects_and_stop_semantics() {
    let root = Directory::new();
    let id = blob(b"hello");
    let index = root.write("no-indexes", &[(id, full_hello())]);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(32 * 1024);
    let limits = ReadLimits::default();
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions::default(),
        budget.clone(),
    )
    .unwrap();
    let steady = budget.used();
    for _ in 0..2 {
        let value = packs
            .decode(&id, &control, &budget, limits)
            .unwrap()
            .unwrap();
        assert_eq!(value.data.as_slice(), b"hello");
        assert!(
            packs
                .cached_object(&id, &control, limits)
                .unwrap()
                .is_none()
        );
        drop(value);
        assert_eq!(budget.used(), steady);
    }
    assert_eq!(packs.cache_stats(&control).unwrap().header_entries, 0);
    assert!(
        packs
            .decode(&blob(b"missing"), &control, &budget, limits)
            .unwrap()
            .is_none()
    );
    control.cancel();
    assert!(packs.header(&id, &control, &budget, limits).is_err());
    drop(packs);
    assert_eq!(budget.used(), 0);
}

#[test]
fn full_4096_slot_tables_keep_first_middle_last_and_missing_lookups_exact() {
    let root = Directory::new();
    let bodies: Vec<_> = (0u32..4096).map(|n| n.to_le_bytes()).collect();
    let entries: Vec<_> = bodies
        .iter()
        .map(|bytes| (blob(bytes), full_blob(bytes)))
        .collect();
    let index = root.write("full-indexes", &entries);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let limits = ReadLimits {
        max_delta_depth: 1,
        ..ReadLimits::default()
    };
    let packs = PackSet::open_with_options(
        [index],
        root,
        gix_hash::Kind::Sha1,
        &control,
        limits,
        DecoderOptions {
            cache_slots: 4096,
            header_slots: 4096,
            cache_bytes: 1024 * 1024,
            max_cache_entry_bytes: 4,
            retained_workspace_bytes: 0,
        },
        budget.clone(),
    )
    .unwrap();
    // Descending physical offsets exercise insertions before every existing key.
    for (id, _) in entries.iter().rev() {
        drop(packs.decode(id, &control, &budget, limits).unwrap());
    }
    let stats = packs.cache_stats(&control).unwrap();
    assert_eq!(stats.entries, 4096);
    assert_eq!(stats.header_entries, 4096);
    assert_eq!(stats.decodes, 4096);
    let blocker = budget.buffer(4 * 1024 * 1024 - budget.used()).unwrap();
    for n in [0, 1, 2047, 2048, 4094, 4095] {
        let id = &entries[n].0;
        let pin = packs.cached_object(id, &control, limits).unwrap().unwrap();
        assert_eq!(pin.id(), *id);
        assert_eq!(pin.as_slice(), bodies[n]);
        assert_eq!(
            packs
                .header(id, &control, &budget, limits)
                .unwrap()
                .unwrap()
                .size,
            4
        );
    }
    assert!(
        packs
            .cached_object(&blob(b"missing"), &control, limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(packs.cache_stats(&control).unwrap().decodes, 4096);
    drop(blocker);
    drop(packs);
    assert_eq!(budget.used(), 0);
}
