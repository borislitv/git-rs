#![forbid(unsafe_code)]

use std::fs;
use std::io::{Seek, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::{Arc, atomic::Ordering, mpsc};
use std::time::{Duration, Instant};

use git_rs::{Result, scratch::Scratch};
use git_rs_core::{
    CapacityBudget, Control, CpuExecutor, ExecutorConfig, JobError, JobOptions, ReadLimits,
    SnapshotIdentity, StopReason,
};
use memfd::{FileSeal, Memfd, MemfdOptions};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "git-core-contract-{}",
            git_rs::scratch::identifier()?
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove owned contract fixture");
    }
}

// Synthetic stored DEFLATE blocks and v1 indexes make REF/OFS fixtures explicit.
// This is a fixture writer, not another product parser or decoder.
fn compressed(bytes: &[u8]) -> Vec<u8> {
    let length = u16::try_from(bytes.len()).expect("small fixture");
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
fn hash(bytes: &[u8]) -> Result<gix::ObjectId> {
    let mut hash = gix::hash::hasher(gix::hash::Kind::Sha1);
    hash.update(bytes);
    Ok(hash.try_finalize()?)
}
fn oid(kind: gix::objs::Kind, bytes: &[u8]) -> Result<gix::ObjectId> {
    Ok(gix::objs::compute_hash(gix::hash::Kind::Sha1, kind, bytes)?)
}
fn full(kind: gix::objs::Kind, bytes: &[u8]) -> Vec<u8> {
    let code = match kind {
        gix::objs::Kind::Commit => 1,
        gix::objs::Kind::Tree => 2,
        gix::objs::Kind::Blob => 3,
        gix::objs::Kind::Tag => 4,
    };
    let mut size = bytes.len();
    let mut first = (code << 4) | (size as u8 & 15);
    size >>= 4;
    if size != 0 {
        first |= 128;
    }
    let mut out = vec![first];
    while size != 0 {
        let mut byte = size as u8 & 127;
        size >>= 7;
        if size != 0 {
            byte |= 128;
        }
        out.push(byte);
    }
    out.extend_from_slice(&compressed(bytes));
    out
}
struct Pins {
    _files: Vec<Memfd>,
}
struct Fixture {
    scratch: Scratch,
    _directory: Directory,
    hold: Option<Arc<Pins>>,
    blob: gix::ObjectId,
}
impl Fixture {
    fn packed() -> Result<Self> {
        let directory = Directory::new()?;
        let scratch = Scratch::new(&directory.0, 1)?;
        let base = oid(gix::objs::Kind::Blob, b"hello")?;
        let middle = oid(gix::objs::Kind::Blob, b"hello!")?;
        let blob = oid(gix::objs::Kind::Blob, b"hello!!")?;
        let base_entry = full(gix::objs::Kind::Blob, b"hello");
        let mut ofs = vec![0x66, u8::try_from(base_entry.len())?];
        ofs.extend_from_slice(&compressed(&[5, 6, 0x90, 5, 1, b'!']));
        let mut reference = vec![0x76];
        reference.extend_from_slice(middle.as_slice());
        reference.extend_from_slice(&compressed(&[6, 7, 0x90, 6, 1, b'!']));
        let mut files = Vec::new();
        Self::pack(
            &scratch,
            "local",
            &[(base, base_entry), (middle, ofs)],
            &mut files,
        )?;
        Self::pack(&scratch, "external", &[(blob, reference)], &mut files)?;
        Ok(Self {
            scratch,
            _directory: directory,
            hold: Some(Arc::new(Pins { _files: files })),
            blob,
        })
    }
    fn pack(
        scratch: &Scratch,
        name: &str,
        entries: &[(gix::ObjectId, Vec<u8>)],
        files: &mut Vec<Memfd>,
    ) -> Result<()> {
        let mut pack = gix::odb::pack::data::header::encode(
            gix::odb::pack::data::Version::V2,
            entries.len() as u32,
        )
        .to_vec();
        let mut offsets = Vec::new();
        for (id, bytes) in entries {
            offsets.push((*id, pack.len() as u32));
            pack.extend_from_slice(bytes);
        }
        let checksum = hash(&pack)?;
        pack.extend_from_slice(checksum.as_slice());
        offsets.sort_by_key(|(id, _)| *id);
        let mut index = Vec::new();
        for first in 0..256 {
            let count = offsets
                .iter()
                .filter(|(id, _)| usize::from(id.as_slice()[0]) <= first)
                .count() as u32;
            index.extend_from_slice(&count.to_be_bytes());
        }
        for (id, offset) in offsets {
            index.extend_from_slice(&offset.to_be_bytes());
            index.extend_from_slice(id.as_slice());
        }
        index.extend_from_slice(checksum.as_slice());
        let checksum = hash(&index)?;
        index.extend_from_slice(checksum.as_slice());
        for (extension, bytes) in [("pack", pack), ("idx", index)] {
            let file = MemfdOptions::default()
                .allow_sealing(true)
                .create("synthetic-core-contract")?;
            let mut writer = file.as_file();
            writer.write_all(&bytes)?;
            file.add_seals(&[
                FileSeal::SealGrow,
                FileSeal::SealShrink,
                FileSeal::SealWrite,
                FileSeal::SealSeal,
            ])?;
            writer.rewind()?;
            assert!(
                writer.write_all(b"X").is_err(),
                "fixture must prohibit writes within existing backing length"
            );
            let path = scratch
                .path
                .join("objects/pack")
                .join(format!("{name}.{extension}"));
            std::os::unix::fs::symlink(
                format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd()),
                &path,
            )?;
            assert!(fs::symlink_metadata(path)?.file_type().is_symlink());
            files.push(file);
        }
        Ok(())
    }
    fn loose(&self, kind: gix::objs::Kind, bytes: &[u8]) -> Result<gix::ObjectId> {
        let id = oid(kind, bytes)?;
        let hex = id.to_string();
        let directory = self.scratch.path.join("objects").join(&hex[..2]);
        fs::create_dir_all(&directory)?;
        let mut raw = gix::objs::encode::loose_header(kind, bytes.len() as u64).to_vec();
        raw.extend_from_slice(bytes);
        fs::write(directory.join(&hex[2..]), compressed(&raw))?;
        Ok(id)
    }
    fn commit(
        &self,
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
        self.loose(gix::objs::Kind::Commit, data.as_bytes())
    }
}
fn identity() -> SnapshotIdentity {
    SnapshotIdentity {
        source: b"synthetic-root-contract".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix::hash::Kind::Sha1,
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

#[test]
fn root_snapshot_resolves_sealed_cross_pack_bytes_and_all_merge_parents() -> Result<()> {
    let mut fixture = Fixture::packed()?;
    let mut tree_data = b"100644 byte-\xff\0".to_vec();
    tree_data.extend_from_slice(fixture.blob.as_slice());
    let tree = fixture.loose(gix::objs::Kind::Tree, &tree_data)?;
    let root = fixture.commit(tree, &[], 900, "root")?;
    let first = fixture.commit(tree, &[root], 1, "first")?;
    let second = fixture.commit(tree, &[root], 1000, "second")?;
    let merge = fixture.commit(tree, &[first, second], 2, "merge")?;
    let hold = fixture.hold.take().unwrap();
    let weak = Arc::downgrade(&hold);
    let budget = CapacityBudget::new(128 * 1024);
    let odb =
        fixture
            .scratch
            .snapshot_odb(identity(), hold, control(), budget.clone(), limits())?;
    assert!(
        weak.upgrade().is_some(),
        "root snapshot must own the opaque source hold"
    );
    let (id, object) = odb
        .lookup_blob(&merge, b"byte-\xff")?
        .expect("byte path to a cross-pack delta");
    assert_eq!(id, fixture.blob);
    assert_eq!(object.data.as_slice(), b"hello!!");
    assert!(odb.pack_location(&id)?.is_some());
    let mut destination = odb.find_buffer(7)?;
    assert_eq!(
        budget.used(),
        object.data.capacity() + destination.capacity()
    );
    assert_eq!(
        odb.find_in(&id, &mut destination)?.unwrap().data,
        b"hello!!"
    );
    assert!(
        odb.is_ancestor(&second, &merge)?,
        "a non-first parent with a newer clock must remain reachable"
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
        "an absent physical empty tree must not become a virtual hit"
    );
    assert!(gix::objs::FindHeader::try_header(&odb, &absent)?.is_none());
    assert!(odb.find_in(&absent, &mut destination)?.is_none());
    assert!(destination.as_slice().is_empty());
    drop(destination);
    let object = Arc::new(object);
    let last = object.clone();
    drop(odb);
    assert!(weak.upgrade().is_none());
    drop(object);
    assert!(
        budget.used() >= 7,
        "the last result reference retains backing admission after source release"
    );
    assert_eq!(last.data.as_slice(), b"hello!!");
    drop(last);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn root_bridge_rejects_corrupt_physical_identity_and_unadmitted_find_growth() -> Result<()> {
    let mut fixture = Fixture::packed()?;
    let good = fixture.loose(gix::objs::Kind::Blob, b"good")?;
    let hex = good.to_string();
    fs::write(
        fixture
            .scratch
            .path
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..]),
        compressed(b"blob 4\0evil"),
    )?;
    let budget = CapacityBudget::new(128 * 1024);
    let odb = fixture.scratch.snapshot_odb(
        identity(),
        fixture.hold.take().unwrap(),
        control(),
        budget.clone(),
        limits(),
    )?;
    assert!(
        odb.object(&good).is_err(),
        "root physical reader must reject content not matching its requested OID"
    );
    assert_eq!(budget.used(), 0);
    let mut raw = Vec::new();
    assert!(gix::objs::Find::try_find(&odb, &fixture.blob, &mut raw).is_err());
    assert_eq!(
        raw.capacity(),
        0,
        "the maintained Find bridge must not allocate behind caller admission"
    );
    assert!(raw.is_empty());
    assert_eq!(budget.used(), 0);
    Ok(())
}

fn failed(error: impl std::fmt::Display) -> JobError {
    JobError::Failed(error.to_string())
}

#[test]
fn root_cpu_cancellation_retains_snapshot_until_stop_and_isolates_healthy_work() -> Result<()> {
    let mut fixture = Fixture::packed()?;
    let path = fixture.scratch.path.clone();
    let hold = fixture.hold.take().unwrap();
    let weak = Arc::downgrade(&hold);
    let budget = CapacityBudget::new(128 * 1024);
    let job_budget = budget.clone();
    let executor = CpuExecutor::new(ExecutorConfig {
        workers: 2,
        queue_capacity: 2,
    })?;
    let request = control();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (rejected_tx, rejected_rx) = mpsc::sync_channel(1);
    let ticket = executor.submit(request, JobOptions::default(), move |control, _| {
        let odb = fixture
            .scratch
            .snapshot_odb(identity(), hold, control.clone(), job_budget, limits())
            .map_err(failed)?;
        let mut destination = odb.find_buffer(7).map_err(failed)?;
        assert_eq!(
            odb.find_in(&fixture.blob, &mut destination)
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
            .send(odb.find_in(&fixture.blob, &mut destination).is_err())
            .map_err(failed)?;
        Ok(())
    })?;
    entered_rx.recv_timeout(Duration::from_secs(5))?;
    ticket.cancel();
    assert!(
        weak.upgrade().is_some(),
        "cancellation is not source-release acknowledgment"
    );
    assert!(path.exists());
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
        "root Scratch forwarding must propagate the cancelled job Control into Find"
    );
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
    assert!(weak.upgrade().is_none());
    assert!(
        !path.exists(),
        "scratch cleanup precedes CPU stop acknowledgment"
    );
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn root_cpu_deadline_watcher_stops_the_same_pinned_snapshot_control() -> Result<()> {
    let mut fixture = Fixture::packed()?;
    let hold = fixture.hold.take().unwrap();
    let weak = Arc::downgrade(&hold);
    let budget = CapacityBudget::new(128 * 1024);
    let job_budget = budget.clone();
    let executor = CpuExecutor::new(ExecutorConfig {
        workers: 1,
        queue_capacity: 1,
    })?;
    let (observed_tx, observed_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let request = Control::new(Instant::now() + Duration::from_millis(300));
    let ticket = executor.submit(request, JobOptions::default(), move |control, _| {
        let odb = fixture
            .scratch
            .snapshot_odb(identity(), hold, control.clone(), job_budget, limits())
            .map_err(failed)?;
        let safety = Instant::now() + Duration::from_secs(3);
        while !control.cancellation_flag().load(Ordering::Acquire) && Instant::now() < safety {
            std::thread::yield_now();
        }
        observed_tx
            .send((
                control.cancellation_flag().load(Ordering::Acquire),
                odb.object(&fixture.blob).is_err(),
            ))
            .map_err(failed)?;
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(failed)?;
        Ok(())
    })?;
    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(5))?,
        (true, true),
        "deadline flag and root reader must observe the same job stop state"
    );
    assert!(weak.upgrade().is_some());
    release_tx.send(())?;
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Deadline)));
    assert!(weak.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    Ok(())
}
