use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use git_rs_core::{
    BlameOptions, BlameRequest, CapacityBudget, Control, Object, Porcelain, ReadLimits,
    SnapshotIdentity, SnapshotSession, VerifiedObjectProvider, objects::Result,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

struct Source {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
}

impl Source {
    fn insert(&mut self, kind: Kind, bytes: Vec<u8>) -> ObjectId {
        let id = gix_object::compute_hash(self.identity.object_hash, kind, &bytes).unwrap();
        self.objects.insert(id, (kind, bytes));
        id
    }

    fn commit(&mut self, blob: Vec<u8>, parent: Option<ObjectId>, time: u64) -> ObjectId {
        let blob = self.insert(Kind::Blob, blob);
        let mut tree = b"100644 file.txt\0".to_vec();
        tree.extend_from_slice(blob.as_bytes());
        let tree = self.insert(Kind::Tree, tree);
        let mut commit = format!("tree {tree}\n");
        if let Some(parent) = parent {
            commit.push_str(&format!("parent {parent}\n"));
        }
        commit.push_str(&format!("author A <a@example.invalid> {time} +0000\ncommitter A <a@example.invalid> {time} +0000\n\nsynthetic\n"));
        self.insert(Kind::Commit, commit.into_bytes())
    }
}

impl VerifiedObjectProvider for Source {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
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
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        let Some((kind, bytes)) = self.objects.get(id) else {
            return Ok(None);
        };
        let mut data = budget.buffer(bytes.len())?;
        data.extend_controlled(bytes, limits.max_object_bytes, control)?;
        Ok(Some(Object { kind: *kind, data }))
    }
}

#[test]
fn large_distinct_line_dictionary_preserves_complete_attribution_and_owned_output() -> Result<()> {
    const ORIGINAL_LINES: usize = 140_000;
    let mut source = Source {
        identity: SnapshotIdentity {
            source: b"large-dictionary-contract".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        },
        objects: BTreeMap::new(),
    };
    let mut bytes = Vec::new();
    for number in 0..ORIGINAL_LINES {
        bytes.extend_from_slice(format!("line-{number:06}\n").as_bytes());
    }
    let root = source.commit(bytes.clone(), None, 1);
    bytes.extend_from_slice(b"new-tail\n");
    let tip = source.commit(bytes, Some(root), 2);
    let identity = source.identity.clone();
    let options = BlameOptions::default();
    let control = Control::new(Instant::now() + Duration::from_secs(30));
    let budget = CapacityBudget::new(512 * 1024 * 1024);
    let snapshot = SnapshotSession::open(
        source,
        &identity,
        control.clone(),
        budget.clone(),
        options.read_limits(),
    )?;
    let blame = snapshot.blame_complete(
        BlameRequest {
            commit: &tip,
            path: b"file.txt",
        },
        &options,
    )?;
    let body = Porcelain::encode_complete(&blame, &control, &budget, 64 * 1024 * 1024)?;
    let encoded: Vec<u8> = body.chunks().flatten().copied().collect();
    let mut records = encoded.split_inclusive(|byte| *byte == b'\n');
    for number in 0..=ORIGINAL_LINES {
        let header = std::str::from_utf8(records.next().expect("one record per output line"))?;
        let fields = header.split_whitespace().collect::<Vec<_>>();
        assert!((3..=4).contains(&fields.len()), "invalid porcelain header");
        assert_eq!(
            fields[0],
            if number < ORIGINAL_LINES { root } else { tip }.to_string()
        );
        assert_eq!(fields[1].parse::<usize>()?, number + 1);
        assert_eq!(fields[2].parse::<usize>()?, number + 1);
        let content = records
            .find(|line| line.starts_with(b"\t"))
            .expect("complete line content");
        let expected = if number < ORIGINAL_LINES {
            format!("\tline-{number:06}\n")
        } else {
            "\tnew-tail\n".to_owned()
        };
        assert_eq!(content, expected.as_bytes());
    }
    assert!(
        records.next().is_none(),
        "no duplicate or trailing output records"
    );
    drop(snapshot);
    assert!(
        budget.used() > 0,
        "completed consumers retain their admission"
    );
    drop((body, blame));
    assert_eq!(
        budget.used(),
        0,
        "all admitted backing is released after consumption"
    );
    Ok(())
}
