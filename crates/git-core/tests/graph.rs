use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use git_rs_core::objects::Result;
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

struct Source {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
}
impl VerifiedObjectProvider for Source {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, _: &Control) -> Result<Option<Header>> {
        Ok(self.objects.get(id).map(|(kind, bytes)| Header {
            kind: *kind,
            size: bytes.len() as u64,
        }))
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        let Some((kind, bytes)) = self.objects.get(id) else {
            return Ok(None);
        };
        let mut data = budget.buffer(bytes.len())?;
        data.resize(bytes.len(), 0)?;
        data.as_mut_slice().copy_from_slice(bytes);
        Ok(Some(Object { kind: *kind, data }))
    }
}
fn source() -> Source {
    Source {
        identity: SnapshotIdentity {
            source: b"synthetic".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        },
        objects: BTreeMap::new(),
    }
}
fn commit(source: &mut Source, parents: &[ObjectId], timestamp: u64, message: &str) -> ObjectId {
    let mut data = "tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n".to_owned();
    for parent in parents {
        data.push_str(&format!("parent {parent}\n"));
    }
    data.push_str(&format!("author A <a@example.invalid> {timestamp} +0000\ncommitter A <a@example.invalid> {timestamp} +0000\n\n{message}\n"));
    let id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Commit, data.as_bytes()).unwrap();
    source.objects.insert(id, (Kind::Commit, data.into_bytes()));
    id
}
fn open(source: Source, limits: ReadLimits) -> SnapshotSession<Source> {
    let identity = source.identity.clone();
    SnapshotSession::open(
        source,
        &identity,
        Control::new(Instant::now() + Duration::from_secs(10)),
        CapacityBudget::new(64 * 1024),
        limits,
    )
    .unwrap()
}

#[test]
fn all_merge_parents_are_walked_despite_nonmonotonic_timestamps() {
    let mut source = source();
    let root = commit(&mut source, &[], 900, "root");
    let first = commit(&mut source, &[root], 1, "first");
    let second = commit(&mut source, &[root], 1000, "second");
    let side = commit(&mut source, &[], 2, "side");
    let merge = commit(&mut source, &[first, second, side], 3, "octopus");
    let session = open(source, ReadLimits::default());
    assert!(session.is_ancestor(&second, &merge).unwrap());
    assert!(session.is_ancestor(&root, &merge).unwrap());
    assert!(!session.is_ancestor(&side, &first).unwrap());
    let reachable = session
        .reachable_from_refs([(b"refs/heads/\xff".as_slice(), merge)])
        .unwrap();
    assert_eq!(
        reachable.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, first, second, side, merge])
    );
    assert_eq!(
        reachable.iter().collect::<Vec<_>>(),
        vec![merge, side, second, root, first],
        "fixed parent order gives deterministic exhaustive traversal, not timestamp order"
    );
}

#[test]
fn missing_equal_ids_and_missing_merge_parents_are_errors_not_positive_hits() {
    let missing = ObjectId::null(gix_hash::Kind::Sha1);
    let mut source = source();
    let parent = commit(&mut source, &[], 1, "parent");
    let merge = commit(&mut source, &[parent, missing], 2, "broken merge");
    let session = open(source, ReadLimits::default());
    assert!(session.is_ancestor(&missing, &missing).is_err());
    assert!(
        session.is_ancestor(&parent, &merge).is_err(),
        "early positive must not hide corrupt parents"
    );
}

#[test]
fn graph_fanout_limits_fail_without_returning_partial_reachability() {
    let mut source = source();
    let root = commit(&mut source, &[], 1, "root");
    let child = commit(&mut source, &[root], 1, "child");
    let limits = ReadLimits {
        max_graph_objects: 1,
        ..ReadLimits::default()
    };
    let session = open(source, limits);
    assert!(
        session
            .reachable_from_refs([(b"refs/heads/main".as_slice(), child)])
            .is_err()
    );
}

#[test]
fn annotated_tags_are_peeled_and_direct_blob_refs_do_not_become_commits() {
    let mut source = source();
    let root = commit(&mut source, &[], 1, "root");
    let tag = format!(
        "object {root}\ntype commit\ntag release\ntagger A <a@example.invalid> 1 +0000\n\nrelease\n"
    );
    let tag_id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Tag, tag.as_bytes()).unwrap();
    source.objects.insert(tag_id, (Kind::Tag, tag.into_bytes()));
    let blob_id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, b"data").unwrap();
    source
        .objects
        .insert(blob_id, (Kind::Blob, b"data".to_vec()));
    let session = open(
        source,
        ReadLimits {
            max_delta_depth: 1,
            ..ReadLimits::default()
        },
    );
    let reachable = session
        .reachable_from_refs([
            (b"refs/tags/release".as_slice(), tag_id),
            (b"refs/tags/bytes".as_slice(), blob_id),
        ])
        .unwrap();
    assert_eq!(reachable.iter().collect::<Vec<_>>(), vec![root]);
}

#[test]
fn tag_peeling_obeys_graph_limits_without_charging_the_terminal_object() {
    for (graph_limit, tag_count, pack_depth) in
        [(1, 1, 1), (1, 2, 1), (2, 2, 1), (2, 3, 1), (100, 64, 64)]
    {
        let mut source = source();
        let root = commit(&mut source, &[], 1, "root");
        let mut tip = root;
        let mut target_kind = Kind::Commit;
        for n in 0..tag_count {
            let data = format!(
                "object {tip}\ntype {target_kind}\ntag release-{n}\ntagger A <a@example.invalid> 1 +0000\n\nrelease\n"
            );
            let id =
                gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Tag, data.as_bytes()).unwrap();
            source.objects.insert(id, (Kind::Tag, data.into_bytes()));
            tip = id;
            target_kind = Kind::Tag;
        }
        let session = open(
            source,
            ReadLimits {
                max_graph_objects: graph_limit,
                max_delta_depth: pack_depth,
                ..ReadLimits::default()
            },
        );
        let result = session.reachable_from_refs([(b"refs/tags/release".as_slice(), tip)]);
        if tag_count <= graph_limit {
            let reachable = result.unwrap_or_else(|error| panic!("{tag_count} physical tags within graph limit {graph_limit}, pack depth {pack_depth}: {error}"));
            assert_eq!(
                reachable.iter().collect::<Vec<_>>(),
                vec![root],
                "only commits are returned, not tag objects"
            );
        } else {
            let error = result
                .err()
                .expect("an over-limit tag chain must not return partial reachability");
            assert!(
                error.to_string().contains("graph object limit"),
                "tag chain must fail its graph bound, not a pack delta bound: {error}"
            );
        }
    }
}

#[test]
fn malformed_parent_is_not_silently_ignored_by_graph_walk() {
    let mut source = source();
    let data = b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nparent broken\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\ninvalid\n";
    let id = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Commit, data).unwrap();
    source.objects.insert(id, (Kind::Commit, data.to_vec()));
    let session = open(source, ReadLimits::default());
    assert!(
        session
            .reachable_from_refs([(b"refs/heads/main".as_slice(), id)])
            .is_err()
    );
}

#[test]
fn reachability_result_keeps_its_backing_capacity_admitted() {
    let mut source = source();
    let root = commit(&mut source, &[], 1, "root");
    let identity = source.identity.clone();
    let budget = CapacityBudget::new(64 * 1024);
    let session = SnapshotSession::open(
        source,
        &identity,
        Control::new(Instant::now() + Duration::from_secs(10)),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    let reachable = session
        .reachable_from_refs([(b"refs/heads/main".as_slice(), root)])
        .unwrap();
    assert_eq!(reachable.len(), 1);
    assert_eq!(reachable.iter().collect::<Vec<_>>(), vec![root]);
    drop(session);
    assert!(
        budget.used() >= 20,
        "a tiny logical result must retain its real backing-capacity charge"
    );
    drop(reachable);
    assert_eq!(budget.used(), 0);
}
