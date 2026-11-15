use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider, objects::Result,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

struct Source {
    identity: SnapshotIdentity,
    commits: BTreeMap<ObjectId, Vec<u8>>,
    reads: Arc<AtomicUsize>,
}

impl Source {
    fn new() -> Self {
        Self {
            identity: SnapshotIdentity {
                source: b"history-contract".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            },
            commits: BTreeMap::new(),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn commit(&mut self, parents: &[ObjectId], timestamp: u64) -> ObjectId {
        let mut bytes = "tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n".to_owned();
        for parent in parents {
            bytes.push_str(&format!("parent {parent}\n"));
        }
        bytes.push_str(&format!("author A <a@example.invalid> {timestamp} +0000\ncommitter A <a@example.invalid> {timestamp} +0000\n\nsynthetic\n"));
        let id =
            gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Commit, bytes.as_bytes()).unwrap();
        self.commits.insert(id, bytes.into_bytes());
        id
    }

    fn open(self, budget: CapacityBudget) -> SnapshotSession<Self> {
        let identity = self.identity.clone();
        SnapshotSession::open(
            self,
            &identity,
            Control::new(Instant::now() + Duration::from_secs(10)),
            budget,
            ReadLimits::default(),
        )
        .unwrap()
    }
}

impl VerifiedObjectProvider for Source {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
        Ok(self.commits.get(id).map(|bytes| Header {
            kind: Kind::Commit,
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
        self.reads.fetch_add(1, Ordering::Relaxed);
        let Some(bytes) = self.commits.get(id) else {
            return Ok(None);
        };
        let mut data = budget.buffer(bytes.len())?;
        data.resize(bytes.len(), 0)?;
        data.as_mut_slice().copy_from_slice(bytes);
        Ok(Some(Object {
            kind: Kind::Commit,
            data,
        }))
    }
}

#[test]
fn history_reads_each_physical_commit_once_and_retains_only_completed_ids() -> Result<()> {
    let mut source = Source::new();
    let root = source.commit(&[], 900);
    let left = source.commit(&[root], 1);
    let right = source.commit(&[root], 1000);
    let tip = source.commit(&[left, right], 2);
    let reads = source.reads.clone();
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let session = source.open(budget.clone());
    let history = session.history(tip, 100)?;
    assert_eq!(
        history.iter().collect::<Vec<_>>(),
        vec![tip, right, left, root]
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        4,
        "topology must reuse this invocation's already verified commit headers"
    );
    let prefix = session.history(tip, 1)?;
    assert_eq!(prefix.iter().collect::<Vec<_>>(), vec![tip]);
    assert_eq!(
        reads.load(Ordering::Relaxed),
        8,
        "a new invocation revalidates its complete physical inventory even for a prefix"
    );
    drop(prefix);
    drop(session);
    assert!(
        budget.used() > 0,
        "completed history keeps its output admitted"
    );
    drop(history);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn short_history_still_rejects_a_missing_later_merge_parent_and_releases_scratch() {
    let mut source = Source::new();
    let root = source.commit(&[], 900);
    let absent = ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
    let tip = source.commit(&[root, absent], 1);
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let session = source.open(budget.clone());
    assert!(
        session.history(tip, 1).is_err(),
        "output truncation cannot hide an unverified parent"
    );
    drop(session);
    assert_eq!(budget.used(), 0);
}
