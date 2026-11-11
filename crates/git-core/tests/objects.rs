use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::objects::Result;
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

struct Provider {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
    alive: Arc<AtomicBool>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
    }
}

impl VerifiedObjectProvider for Provider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
        Ok(self.objects.get(id).map(|(kind, data)| Header {
            kind: *kind,
            size: data.len() as u64,
        }))
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        let Some((kind, bytes)) = self.objects.get(id) else {
            return Ok(None);
        };
        if bytes.len() > limits.max_object_bytes {
            return Err("fixture object limit".into());
        }
        let mut data = budget.buffer(bytes.len())?;
        data.resize(bytes.len(), 0)?;
        data.as_mut_slice().copy_from_slice(bytes);
        Ok(Some(Object { kind: *kind, data }))
    }
}

fn provider() -> Provider {
    Provider {
        identity: SnapshotIdentity {
            source: b"source".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        },
        objects: BTreeMap::new(),
        alive: Arc::new(AtomicBool::new(true)),
    }
}

fn insert(provider: &mut Provider, kind: Kind, bytes: &[u8]) -> ObjectId {
    let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, bytes).unwrap();
    provider.objects.insert(id, (kind, bytes.to_vec()));
    id
}

fn open(
    provider: Provider,
    capacity: CapacityBudget,
    limits: ReadLimits,
) -> SnapshotSession<Provider> {
    let identity = provider.identity.clone();
    SnapshotSession::open(
        provider,
        &identity,
        Control::new(Instant::now() + Duration::from_secs(10)),
        capacity,
        limits,
    )
    .unwrap()
}

#[test]
fn physical_empty_tree_is_not_a_virtual_hit() {
    let empty = ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap();
    let session = open(provider(), CapacityBudget::new(1024), ReadLimits::default());
    assert!(session.header(&empty).unwrap().is_none());
    assert!(session.object(&empty).unwrap().is_none());
    let mut stored = provider();
    assert_eq!(insert(&mut stored, Kind::Tree, b""), empty);
    let session = open(stored, CapacityBudget::new(1024), ReadLimits::default());
    assert_eq!(session.header(&empty).unwrap().unwrap().kind, Kind::Tree);
    assert!(
        session
            .object(&empty)
            .unwrap()
            .unwrap()
            .data
            .as_slice()
            .is_empty()
    );
}

#[test]
fn identity_pins_and_actual_output_capacity_have_distinct_lifetimes() {
    let mut source = provider();
    let id = insert(&mut source, Kind::Blob, b"hello");
    let alive = source.alive.clone();
    let budget = CapacityBudget::new(10);
    let session = open(source, budget.clone(), ReadLimits::default());
    let object = session.object(&id).unwrap().unwrap();
    assert_eq!(object.data.as_slice(), b"hello");
    assert_eq!(budget.used(), object.data.capacity());
    assert!(alive.load(Ordering::Acquire));
    drop(session);
    assert!(!alive.load(Ordering::Acquire));
    assert!(
        budget.used() >= 5,
        "owned result still pins backing allocation"
    );
    drop(object);
    assert_eq!(budget.used(), 0);

    let source = provider();
    let mut wrong = source.identity.clone();
    wrong.generation = b"other".to_vec();
    assert!(
        SnapshotSession::open(
            source,
            &wrong,
            Control::new(Instant::now() + Duration::from_secs(10)),
            budget,
            ReadLimits::default()
        )
        .is_err()
    );
}

#[test]
fn corrupt_content_and_capacity_exhaustion_never_return_partial_objects() {
    let mut source = provider();
    let id = insert(&mut source, Kind::Blob, b"good");
    source.objects.get_mut(&id).unwrap().1 = b"evil".to_vec();
    let budget = CapacityBudget::new(32);
    let session = open(source, budget.clone(), ReadLimits::default());
    assert!(
        session
            .object(&id)
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    assert_eq!(budget.used(), 0);
    let mut source = provider();
    let id = insert(&mut source, Kind::Blob, b"larger than budget");
    let session = open(source, CapacityBudget::new(2), ReadLimits::default());
    assert!(session.object(&id).is_err());
}

#[test]
fn nested_byte_paths_and_symlinks_are_read_without_filesystem_interpretation() {
    let mut source = provider();
    let blob = insert(&mut source, Kind::Blob, b"../not-followed");
    let mut nested = b"120000 \xff\0".to_vec();
    nested.extend_from_slice(blob.as_slice());
    let subtree = insert(&mut source, Kind::Tree, &nested);
    let mut root = b"40000 dir\0".to_vec();
    root.extend_from_slice(subtree.as_slice());
    let tree = insert(&mut source, Kind::Tree, &root);
    let session = open(source, CapacityBudget::new(1024), ReadLimits::default());
    let (id, object) = session.lookup_blob(&tree, b"dir/\xff").unwrap().unwrap();
    assert_eq!(id, blob);
    assert_eq!(object.data.as_slice(), b"../not-followed");
    assert!(
        session
            .lookup_blob(&tree, b"dir/missing")
            .unwrap()
            .is_none()
    );
    for path in [
        b"/dir".as_slice(),
        b"dir//x",
        b"dir/../x",
        b"dir/",
        b"dir/\0",
    ] {
        assert!(session.lookup_blob(&tree, path).is_err());
    }
}

#[test]
fn woof_tree_lookup_uses_the_object_bound_not_the_commit_metadata_bound() {
    for (extra_bytes, succeeds) in [(0, true), (1, false)] {
        let mut source = provider();
        let blob = insert(&mut source, Kind::Blob, b"small\n");
        let mut bytes = Vec::new();
        for n in 0..2000 {
            bytes.extend_from_slice(format!("100644 file{n:04}\0").as_bytes());
            bytes.extend_from_slice(blob.as_bytes());
        }
        assert_eq!(bytes.len(), 72_000);
        let tree = insert(&mut source, Kind::Tree, &bytes);
        let commit = insert(&mut source, Kind::Commit, format!("tree {tree}\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\nSmall\n").as_bytes());
        let budget = CapacityBudget::new(256 * 1024);
        let session = open(
            source,
            budget.clone(),
            ReadLimits {
                max_object_bytes: 72_000 - extra_bytes,
                max_metadata_bytes: 64 * 1024,
                ..ReadLimits::default()
            },
        );
        for start in [tree, commit] {
            let result = session.lookup_blob(&start, b"file1999");
            if succeeds {
                let (id, object) = result
                    .expect("wide tree lookup must not use the commit metadata cap")
                    .unwrap();
                assert_eq!(id, blob);
                assert_eq!(object.data.as_slice(), b"small\n");
            } else {
                assert!(result.is_err(), "trees still obey the finite object bound");
            }
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn cancelled_session_cannot_read_headers_or_bodies() {
    let source = provider();
    let identity = source.identity.clone();
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let session = SnapshotSession::open(
        source,
        &identity,
        control.clone(),
        CapacityBudget::new(1024),
        ReadLimits::default(),
    )
    .unwrap();
    control.cancel();
    let id = ObjectId::null(gix_hash::Kind::Sha1);
    assert!(session.header(&id).is_err());
    assert!(session.object(&id).is_err());
}

#[test]
fn absent_physical_tree_and_malformed_tree_are_errors_not_path_misses() {
    let mut source = provider();
    let commit = insert(&mut source, Kind::Commit, b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\ninitial\n");
    let malformed = insert(&mut source, Kind::Tree, b"100644 truncated");
    let session = open(source, CapacityBudget::new(4096), ReadLimits::default());
    assert!(
        session.lookup_blob(&commit, b"file").is_err(),
        "the empty-tree ID is not a physical tree"
    );
    assert!(
        session.lookup_blob(&malformed, b"missing").is_err(),
        "do not suppress maintained tree parser errors"
    );
}

#[test]
fn maintained_find_traits_use_admitted_output_and_never_grow_arbitrary_vecs() {
    let mut source = provider();
    let id = insert(&mut source, Kind::Blob, b"hello");
    let budget = CapacityBudget::new(64);
    let session = open(source, budget.clone(), ReadLimits::default());
    let header = gix_object::FindHeader::try_header(&session, &id)
        .unwrap()
        .unwrap();
    assert_eq!(
        header,
        Header {
            kind: Kind::Blob,
            size: 5
        }
    );
    let mut output = session.find_buffer(5).unwrap();
    let found = session.find_in(&id, &mut output).unwrap().unwrap();
    assert_eq!(found.kind, Kind::Blob);
    assert_eq!(found.data, b"hello");
    assert_eq!(
        budget.used(),
        output.capacity(),
        "temporary provider decode storage has already been released"
    );
    let mut unadmitted = Vec::new();
    assert!(gix_object::Find::try_find(&session, &id, &mut unadmitted).is_err());
    assert_eq!(unadmitted.capacity(), 0);
    assert!(unadmitted.is_empty());
    let missing = ObjectId::null(gix_hash::Kind::Sha1);
    assert!(session.find_in(&missing, &mut output).unwrap().is_none());
    assert!(
        output.as_slice().is_empty(),
        "missing reads do not expose bytes from an earlier object"
    );
    drop(output);
    assert_eq!(budget.used(), 0);
}
