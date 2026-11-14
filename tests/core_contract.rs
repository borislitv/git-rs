#![forbid(unsafe_code)]

#[path = "support/range_source.rs"]
mod ranges;

use git_rs::Result;
use git_rs_core::{
    CapacityBudget, Control, CpuExecutor, ExecutorConfig, JobError, JobOptions, ReadLimits,
    StopReason,
};
use ranges::{Pack, Source, compressed, full, oid};
use std::{
    io::Write,
    sync::{Arc, atomic::Ordering, mpsc},
    time::{Duration, Instant},
};

struct Fixture {
    packs: Vec<Pack>,
    objects: Vec<(gix::ObjectId, Vec<u8>)>,
    blob: gix::ObjectId,
}

impl Fixture {
    fn packed() -> Result<Self> {
        let base = oid(gix::objs::Kind::Blob, b"hello")?;
        let middle = oid(gix::objs::Kind::Blob, b"hello!")?;
        let blob = oid(gix::objs::Kind::Blob, b"hello!!")?;
        let base_entry = full(gix::objs::Kind::Blob, b"hello");
        let mut ofs = vec![0x66, u8::try_from(base_entry.len())?];
        ofs.extend_from_slice(&compressed(&[5, 6, 0x90, 5, 1, b'!']));
        let mut reference = vec![0x76];
        reference.extend_from_slice(middle.as_slice());
        reference.extend_from_slice(&compressed(&[6, 7, 0x90, 6, 1, b'!']));
        Ok(Self {
            packs: vec![
                Pack::new(&[(base, base_entry), (middle, ofs)])?,
                Pack::new(&[(blob, reference)])?,
            ],
            objects: Vec::new(),
            blob,
        })
    }

    fn object(&mut self, kind: gix::objs::Kind, bytes: &[u8]) -> Result<gix::ObjectId> {
        let id = oid(kind, bytes)?;
        self.objects.push((id, full(kind, bytes)));
        Ok(id)
    }

    fn commit(
        &mut self,
        tree: gix::ObjectId,
        parents: &[gix::ObjectId],
        time: u64,
        message: &str,
    ) -> Result<gix::ObjectId> {
        let mut data = format!("tree {tree}\n");
        for parent in parents {
            data.push_str(&format!("parent {parent}\n"));
        }
        data.push_str(&format!("author A <a@example.invalid> {time} +0000\ncommitter A <a@example.invalid> {time} +0000\n\n{message}\n"));
        self.object(gix::objs::Kind::Commit, data.as_bytes())
    }

    fn source(mut self) -> Result<Source> {
        if !self.objects.is_empty() {
            self.packs.push(Pack::new(&self.objects)?);
        }
        Ok(Source::new(self.packs))
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        max_decode_bytes: 4096,
        max_graph_objects: 128,
        ..ReadLimits::default()
    }
}
fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(10))
}
fn failed(error: impl std::fmt::Display) -> JobError {
    JobError::Failed(error.to_string())
}

#[test]
fn root_snapshot_resolves_cross_pack_ranges_and_all_merge_parents() -> Result<()> {
    let mut fixture = Fixture::packed()?;
    let blob = fixture.blob;
    let mut tree_data = b"100644 byte-\xff\0".to_vec();
    tree_data.extend_from_slice(blob.as_slice());
    let tree = fixture.object(gix::objs::Kind::Tree, &tree_data)?;
    let root = fixture.commit(tree, &[], 900, "root")?;
    let first = fixture.commit(tree, &[root], 1, "first")?;
    let second = fixture.commit(tree, &[root], 1000, "second")?;
    let merge = fixture.commit(tree, &[first, second], 2, "merge")?;
    let source = fixture.source()?;
    let weak = source.witness();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let odb = source.open(control(), budget.clone(), limits())?;
    assert!(weak.upgrade().is_some());
    let (id, object) = odb
        .lookup_blob(&merge, b"byte-\xff")?
        .expect("byte path to cross-pack delta");
    assert_eq!(id, blob);
    assert_eq!(object.data.as_slice(), b"hello!!");
    assert!(odb.pack_location(&id)?.is_some());
    let before_destination = budget.used();
    let mut destination = odb.find_buffer(7)?;
    assert_eq!(budget.used(), before_destination + destination.capacity());
    assert_eq!(
        odb.find_in(&id, &mut destination)?.unwrap().data,
        b"hello!!"
    );
    assert!(
        odb.is_ancestor(&second, &merge)?,
        "all parents remain reachable despite clock skew"
    );
    assert_eq!(
        odb.reachable_from_refs([(b"refs/heads/\xff".as_slice(), merge)])?
            .iter()
            .collect::<Vec<_>>(),
        vec![merge, second, root, first]
    );
    let absent = gix::ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904")?;
    assert!(
        odb.object(&absent)?.is_none(),
        "an absent empty tree is not virtual source membership"
    );
    assert!(gix::objs::FindHeader::try_header(&odb, &absent)?.is_none());
    assert!(odb.find_in(&absent, &mut destination)?.is_none());
    assert!(destination.as_slice().is_empty());
    drop(destination);
    let last = Arc::new(object);
    drop(odb);
    assert!(
        weak.upgrade().is_none(),
        "all range metadata drops with the source session"
    );
    assert_eq!(
        budget.used(),
        last.data.capacity(),
        "the surviving output alone owns its backing charge"
    );
    assert_eq!(last.data.as_slice(), b"hello!!");
    drop(last);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn root_ranges_reject_corrupt_physical_identity_and_unadmitted_find_growth() -> Result<()> {
    let good = oid(gix::objs::Kind::Blob, b"good")?;
    let source = Source::new(vec![Pack::new(&[(
        good,
        full(gix::objs::Kind::Blob, b"evil"),
    )])?]);
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let odb = source.open(control(), budget.clone(), limits())?;
    assert!(
        odb.object(&good).is_err(),
        "a valid index and zlib frame cannot attest a false object ID"
    );
    drop(odb);
    assert_eq!(budget.used(), 0);
    let fixture = Fixture::packed()?;
    let blob = fixture.blob;
    let odb = fixture
        .source()?
        .open(control(), budget.clone(), limits())?;
    let mut raw = Vec::new();
    assert!(gix::objs::Find::try_find(&odb, &blob, &mut raw).is_err());
    assert_eq!(
        raw.capacity(),
        0,
        "Find cannot silently grow unadmitted caller backing"
    );
    assert!(raw.is_empty());
    drop(odb);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn range_blame_rejects_oversized_full_and_delta_blobs_before_decode() -> Result<()> {
    use git_rs_core::blame::{BlameError, BlameOptions, BlameRequest, MAX_BLOB_BYTES};

    let size = MAX_BLOB_BYTES + 1;
    let mut blob_hash = openssl::sha::Sha1::new();
    blob_hash.update(format!("blob {size}\0").as_bytes());
    let mut delta_hash = openssl::sha::Sha1::new();
    delta_hash.update(format!("blob {}\0", size + 1).as_bytes());
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    let block = vec![0; 1024 * 1024];
    for _ in 0..128 {
        encoder.write_all(&block)?;
        blob_hash.update(&block);
        delta_hash.update(&block);
    }
    encoder.write_all(&[0])?;
    blob_hash.update(&[0]);
    delta_hash.update(&[0, b'x']);
    let blob = gix::ObjectId::from(blob_hash.finish());
    let delta = gix::ObjectId::from(delta_hash.finish());
    // Git pack entry: full blob, declared size 128 MiB + 1.
    let mut blob_entry = vec![0xb1, 0x80, 0x80, 0x80, 0x04];
    blob_entry.extend_from_slice(&encoder.finish()?);
    // Delta size integers are LEB128. Repeated default 64-KiB copies read
    // zeros from the base, followed by the final zero and distinct suffix.
    let mut instructions = Vec::new();
    for length in [size, size + 1] {
        let mut remaining = length;
        while remaining >= 128 {
            instructions.push((remaining as u8 & 127) | 128);
            remaining >>= 7;
        }
        instructions.push(remaining as u8);
    }
    instructions.extend(std::iter::repeat_n(0x80, 2048));
    instructions.extend_from_slice(&[2, 0, b'x']);
    assert_eq!(instructions.len(), 2059);
    // Git pack entry: reference delta with 2059 instruction bytes.
    let mut delta_entry = vec![0xfb, 0x80, 0x01];
    delta_entry.extend_from_slice(blob.as_slice());
    delta_entry.extend_from_slice(&compressed(&instructions));
    let mut entries = vec![(blob, blob_entry), (delta, delta_entry)];
    let mut commits = Vec::new();
    for selected in [blob, delta] {
        let mut tree = b"100644 file\0".to_vec();
        tree.extend_from_slice(selected.as_slice());
        let tree_id = oid(gix::objs::Kind::Tree, &tree)?;
        let commit = format!(
            "tree {tree_id}\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\nsynthetic\n"
        );
        let commit_id = oid(gix::objs::Kind::Commit, commit.as_bytes())?;
        entries.push((tree_id, full(gix::objs::Kind::Tree, &tree)));
        entries.push((commit_id, full(gix::objs::Kind::Commit, commit.as_bytes())));
        commits.push(commit_id);
    }
    let source = Source::new(vec![Pack::new(&entries)?]);
    let options = BlameOptions::default();
    for commit in commits {
        // This budget cannot hold either blob: rejection must precede decode.
        let budget = CapacityBudget::new(32 * 1024 * 1024);
        let session = source
            .clone()
            .open(control(), budget.clone(), options.read_limits())?;
        let error = session
            .blame_complete(
                BlameRequest {
                    commit: &commit,
                    path: b"file",
                },
                &options,
            )
            .err()
            .expect("oversized historical blobs must not reach attribution");
        assert_eq!(error, BlameError::BlobTooLarge, "selected commit {commit}");
        drop(session);
        assert_eq!(
            budget.used(),
            0,
            "failed preflight must release every admitted buffer"
        );
    }
    Ok(())
}

#[test]
fn root_cpu_cancellation_retains_snapshot_until_stop_and_isolates_healthy_work() -> Result<()> {
    let fixture = Fixture::packed()?;
    let blob = fixture.blob;
    let source = fixture.source()?;
    let weak = source.witness();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let job_budget = budget.clone();
    let executor = CpuExecutor::new(ExecutorConfig {
        workers: 2,
        queue_capacity: 2,
    })?;
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (rejected_tx, rejected_rx) = mpsc::sync_channel(1);
    let ticket = executor.submit(control(), JobOptions::default(), move |control, _| {
        let odb = source
            .open(control.clone(), job_budget, limits())
            .map_err(failed)?;
        let mut destination = odb.find_buffer(7).map_err(failed)?;
        assert_eq!(
            odb.find_in(&blob, &mut destination)
                .map_err(failed)?
                .unwrap()
                .data,
            b"hello!!"
        );
        entered_tx.send(()).map_err(failed)?;
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(failed)?;
        rejected_tx
            .send(odb.find_in(&blob, &mut destination).is_err())
            .map_err(failed)?;
        Ok(())
    })?;
    entered_rx.recv_timeout(Duration::from_secs(5))?;
    ticket.cancel();
    assert!(
        weak.upgrade().is_some(),
        "cancellation is not source-release acknowledgment"
    );
    assert!(budget.used() >= 7);
    let healthy = executor.submit(control(), JobOptions::default(), |control, _| {
        control.check()?;
        Ok(42)
    })?;
    assert_eq!(healthy.wait_timeout(Duration::from_secs(5))??, 42);
    assert!(weak.upgrade().is_some());
    release_tx.send(())?;
    assert!(
        rejected_rx.recv_timeout(Duration::from_secs(5))?,
        "range reads observe the executor's exact job control"
    );
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn root_cpu_deadline_watcher_stops_the_same_pinned_snapshot_control() -> Result<()> {
    let fixture = Fixture::packed()?;
    let blob = fixture.blob;
    let source = fixture.source()?;
    let weak = source.witness();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let job_budget = budget.clone();
    let executor = CpuExecutor::new(ExecutorConfig {
        workers: 1,
        queue_capacity: 1,
    })?;
    let (observed_tx, observed_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let ticket = executor.submit(
        Control::new(Instant::now() + Duration::from_millis(300)),
        JobOptions::default(),
        move |control, _| {
            let odb = source
                .open(control.clone(), job_budget, limits())
                .map_err(failed)?;
            let safety = Instant::now() + Duration::from_secs(3);
            while !control.cancellation_flag().load(Ordering::Acquire) && Instant::now() < safety {
                std::thread::yield_now();
            }
            observed_tx
                .send((
                    control.cancellation_flag().load(Ordering::Acquire),
                    odb.object(&blob).is_err(),
                ))
                .map_err(failed)?;
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(failed)?;
            Ok(())
        },
    )?;
    let result = observed_rx.recv_timeout(Duration::from_secs(5));
    let retained = weak.upgrade().is_some();
    release_tx.send(())?;
    assert_eq!(
        result?,
        (true, true),
        "the deadline flag and physical reader share one stop state"
    );
    assert!(
        retained,
        "deadline notification cannot discard a still-running source owner"
    );
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Deadline)));
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    Ok(())
}
