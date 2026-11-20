use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::objects::{Error, Result, VerifiedObject};
use git_rs_core::{
    BlameError, BlameOptions, BlameRequest, CapacityBudget, CapacityError, Control, Object,
    ReadLimits, SnapshotIdentity, SnapshotSession, StopReason, VerifiedObjectProvider,
};
use gix_error::ErrorExt;
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

#[derive(Clone, Copy, Debug)]
enum Failure {
    Cancelled,
    Deadline,
    Capacity,
    Allocation,
    CapacityStopped,
    Corrupt,
}

// Keep the existing physical fixture, adding one changed edge. The parent blob
// is not an initial target read: its first body acquisition is the raw old side,
// after the new side has already installed an admitted owner.
fn changed_fixture() -> (Provider, ObjectId, ObjectId) {
    let mut provider = Provider::new(None, false, false);
    let root = provider.commit;
    let base = gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, b"one\ntwo\n").unwrap();
    let mut insert = |kind, bytes: Vec<u8>| {
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap();
        provider.objects.insert(id, (kind, bytes));
        id
    };
    let blob = insert(Kind::Blob, b"one\nnew\n".to_vec());
    let mut tree = b"100644 file\0".to_vec();
    tree.extend_from_slice(blob.as_bytes());
    let tree = insert(Kind::Tree, tree);
    provider.commit = insert(Kind::Commit, format!(
        "tree {tree}\nparent {root}\nauthor Example <example@example.test> 2 +0000\ncommitter Example <example@example.test> 2 +0000\n\nChanged\n"
    ).into_bytes());
    (provider, base, root)
}

#[derive(Clone, Copy, Debug)]
enum RawFailure {
    Source(Failure),
    Missing,
    WrongIdentity,
    WrongBudget,
    CorruptBytes,
    MetadataPressure,
    CancelAfterVerified,
    Panic,
}

struct RawProvider {
    inner: Provider,
    base: ObjectId,
    failure: Option<RawFailure>,
    reads: Arc<AtomicUsize>,
    pressure: Arc<Mutex<Option<git_rs_core::CapacityBuffer>>>,
}

const RAW_TEST_CAPACITY: usize = 8 * 1024 * 1024;

impl VerifiedObjectProvider for RawProvider {
    fn identity(&self) -> &SnapshotIdentity {
        self.inner.identity()
    }
    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        self.inner.header(id, control)
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.inner.object(id, control, budget, limits)
    }
    fn verified_object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        control.check()?;
        let failure = if id == self.base.as_ref() {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.failure
        } else {
            None
        };
        match failure {
            Some(RawFailure::Source(failure)) => {
                return Err(Box::new(
                    gix_error::Error::from_boxed(failure.error())
                        .and_raise(gix_error::message("physical raw blob acquisition")),
                ));
            }
            Some(RawFailure::Missing) => return Ok(None),
            Some(RawFailure::Panic) => {
                panic!("physical source panics after the opposite raw owner was installed")
            }
            Some(RawFailure::WrongIdentity) => {
                let (other_id, _) = self
                    .inner
                    .objects
                    .iter()
                    .find(|(other, (kind, _))| **other != self.base && *kind == Kind::Blob)
                    .expect("different real blob");
                let object = self
                    .inner
                    .object(other_id, control, budget, limits)?
                    .expect("physical alternate blob");
                return VerifiedObject::verify(other_id, object, control, budget, limits).map(Some);
            }
            Some(RawFailure::WrongBudget) => {
                let other = CapacityBudget::new(RAW_TEST_CAPACITY);
                let object = self
                    .inner
                    .object(id, control, &other, limits)?
                    .expect("physical parent blob");
                return VerifiedObject::verify(id, object, control, &other, limits).map(Some);
            }
            _ => {}
        }
        let Some(mut object) = self.inner.object(id, control, budget, limits)? else {
            return Ok(None);
        };
        if matches!(failure, Some(RawFailure::CorruptBytes)) {
            object.data.as_mut_slice()[0] ^= 1;
        }
        let verified = VerifiedObject::verify(id, object, control, budget, limits)?;
        if matches!(failure, Some(RawFailure::MetadataPressure)) {
            // Model another live admitted allocation exhausting the same job
            // budget after bytes are ready, before the raw Arc can be created.
            *self.pressure.lock().expect("pressure mutex") =
                Some(budget.buffer(RAW_TEST_CAPACITY - budget.used())?);
        }
        if matches!(failure, Some(RawFailure::CancelAfterVerified)) {
            control.cancel();
        }
        Ok(Some(verified))
    }
}

#[derive(Default)]
struct MissMemo(usize);
impl git_rs_core::blame::memo::MappingMemo for MissMemo {
    fn load(
        &mut self,
        _: &git_rs_core::blame::memo::MappingKey,
        _: &mut [u8],
    ) -> std::result::Result<Option<usize>, BlameError> {
        Ok(None)
    }
    fn completed(
        &mut self,
        _: &git_rs_core::blame::memo::MappingKey,
        _: &[u8],
    ) -> std::result::Result<(), BlameError> {
        self.0 += 1;
        Ok(())
    }
}

#[test]
fn raw_owned_blobs_keep_complete_porcelain_with_and_without_memo() {
    let mut previous = None;
    for memoized in [false, true] {
        let (inner, base, root) = changed_fixture();
        let commit = inner.commit;
        let identity = inner.identity.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let provider = RawProvider {
            inner,
            base,
            failure: None,
            reads: reads.clone(),
            pressure: Arc::new(Mutex::new(None)),
        };
        let budget = CapacityBudget::new(RAW_TEST_CAPACITY);
        let control = Control::new(Instant::now() + Duration::from_secs(60));
        let session = SnapshotSession::open(
            provider,
            &identity,
            control.clone(),
            budget.clone(),
            BlameOptions::default().read_limits(),
        )
        .unwrap();
        let mut memo = MissMemo::default();
        let request = BlameRequest {
            commit: &commit,
            path: b"file",
        };
        let complete = if memoized {
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
        assert!(
            reads.load(Ordering::Relaxed) > 0,
            "read the physical old-side blob through verified_object"
        );
        assert_eq!(complete.statistics().mapping.computations, 1);
        assert_eq!(memo.0, usize::from(memoized));
        let body = git_rs_core::Porcelain::encode_complete(
            &complete,
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
                    word.len() == 40 && word.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
            .collect();
        assert_eq!(
            origins,
            [format!("{root} 1 1 1"), format!("{commit} 2 2 1")]
        );
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with('\t'))
                .collect::<Vec<_>>(),
            ["\tone", "\tnew"]
        );
        if let Some(expected) = &previous {
            assert_eq!(
                &bytes, expected,
                "memo selection cannot change complete porcelain"
            );
        }
        previous = Some(bytes);
        drop(body);
        drop(complete);
        assert_eq!(
            budget.used(),
            0,
            "no raw owner or metadata ledger escapes in the result"
        );
        drop(session);
    }
}

#[test]
fn raw_owned_long_history_preserves_every_append_and_releases_grown_ledger() {
    // Twenty-five distinct blob revisions exceed the ledger's initial sixteen
    // slots. Every append survives at the tip, so independent per-line origins
    // require the complete history, not a shortened traversal or private counts.
    const APPENDS: usize = 24;
    let mut provider = Provider::new(None, false, false);
    let root = provider.commit;
    let mut tip = root;
    let mut contents = b"one\ntwo\n".to_vec();
    let mut expected_contents = vec!["one".to_owned(), "two".to_owned()];
    let mut expected_origins = vec![(root, 1usize, 1usize), (root, 2, 2)];
    let mut insert = |kind, bytes: Vec<u8>| {
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap();
        provider.objects.insert(id, (kind, bytes));
        id
    };
    for revision in 1..=APPENDS {
        let line = format!("append-{revision:02}");
        contents.extend_from_slice(line.as_bytes());
        contents.push(b'\n');
        let blob = insert(Kind::Blob, contents.clone());
        let mut tree = b"100644 file\0".to_vec();
        tree.extend_from_slice(blob.as_bytes());
        let tree = insert(Kind::Tree, tree);
        let time = revision + 1;
        tip = insert(Kind::Commit, format!(
            "tree {tree}\nparent {tip}\nauthor Example <example@example.test> {time} +0000\ncommitter Example <example@example.test> {time} +0000\n\nAppend {revision}\n"
        ).into_bytes());
        let line_number = revision + 2;
        expected_origins.push((tip, line_number, line_number));
        expected_contents.push(line);
    }
    provider.commit = tip;
    let identity = provider.identity.clone();
    let budget = CapacityBudget::new(RAW_TEST_CAPACITY);
    // An independent real allocation must remain charged after request cleanup.
    let retained = budget.buffer(1024).unwrap();
    let baseline = budget.used();
    let control = Control::new(Instant::now() + Duration::from_secs(60));
    let session = SnapshotSession::open(
        provider,
        &identity,
        control.clone(),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap();
    let mut previous = None;
    for memoized in [false, true] {
        let request = BlameRequest {
            commit: &tip,
            path: b"file",
        };
        let mut memo = MissMemo::default();
        let complete = if memoized {
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
        let body = git_rs_core::Porcelain::encode_complete(
            &complete,
            &control,
            &budget,
            git_rs_core::blame::MAX_BODY_BYTES,
        )
        .unwrap();
        let bytes: Vec<_> = body.chunks().flatten().copied().collect();
        let text = std::str::from_utf8(&bytes).unwrap();
        let mut origins = Vec::new();
        for line in text.lines() {
            let mut fields = line.split(' ');
            let id = fields.next().unwrap();
            if id.len() == 40 && id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                origins.push((
                    ObjectId::from_hex(id.as_bytes()).unwrap(),
                    fields.next().unwrap().parse::<usize>().unwrap(),
                    fields.next().unwrap().parse::<usize>().unwrap(),
                ));
            }
        }
        assert_eq!(
            origins, expected_origins,
            "every final line retains its independently known introducing commit and line number"
        );
        assert_eq!(
            text.lines()
                .filter_map(|line| line.strip_prefix('\t'))
                .collect::<Vec<_>>(),
            expected_contents
        );
        if let Some(expected) = &previous {
            assert_eq!(
                &bytes, expected,
                "memo selection preserves the entire long-history porcelain body"
            );
        }
        previous = Some(bytes);
        drop(body);
        drop(complete);
        assert_eq!(
            budget.used(),
            baseline,
            "grown ledger, raw owners and output release admission without releasing independent storage"
        );
    }
    drop(session);
    assert_eq!(budget.used(), baseline);
    drop(retained);
    assert_eq!(budget.used(), 0);
}

#[test]
fn raw_blob_failures_preserve_causes_and_release_owners_and_metadata() {
    for memoized in [false, true] {
        for failure in [
            RawFailure::Source(Failure::Cancelled),
            RawFailure::Source(Failure::Deadline),
            RawFailure::Source(Failure::Capacity),
            RawFailure::Source(Failure::Allocation),
            RawFailure::Source(Failure::CapacityStopped),
            RawFailure::Source(Failure::Corrupt),
            RawFailure::Missing,
            RawFailure::WrongIdentity,
            RawFailure::WrongBudget,
            RawFailure::CorruptBytes,
            RawFailure::MetadataPressure,
            RawFailure::CancelAfterVerified,
            RawFailure::Panic,
        ] {
            let (inner, base, _) = changed_fixture();
            let commit = inner.commit;
            let identity = inner.identity.clone();
            let reads = Arc::new(AtomicUsize::new(0));
            let pressure = Arc::new(Mutex::new(None));
            let provider = RawProvider {
                inner,
                base,
                failure: Some(failure),
                reads: reads.clone(),
                pressure: pressure.clone(),
            };
            let budget = CapacityBudget::new(RAW_TEST_CAPACITY);
            let session = SnapshotSession::open(
                provider,
                &identity,
                Control::new(Instant::now() + Duration::from_secs(60)),
                budget.clone(),
                BlameOptions::default().read_limits(),
            )
            .unwrap();
            let mut memo = MissMemo::default();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let request = BlameRequest {
                    commit: &commit,
                    path: b"file",
                };
                if memoized {
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
            }));
            assert_eq!(
                reads.load(Ordering::Relaxed),
                1,
                "reach old-side acquisition once, without fallback: {failure:?}"
            );
            if matches!(failure, RawFailure::Panic) {
                assert!(result.is_err(), "source panic must unwind");
            } else {
                let error = result
                    .expect("ordinary source errors do not panic")
                    .err()
                    .expect("no completed output on failure");
                match failure {
                    RawFailure::Source(source) => assert_eq!(error, source.expected()),
                    RawFailure::MetadataPressure => {
                        assert!(
                            pressure.lock().expect("pressure mutex").is_some(),
                            "reach actual shared-budget exhaustion after verification"
                        );
                        assert_eq!(error, BlameError::Capacity(CapacityError::Exhausted));
                    }
                    RawFailure::CancelAfterVerified => {
                        assert_eq!(error, BlameError::Stopped(StopReason::Cancelled))
                    }
                    _ => assert!(
                        matches!(error, BlameError::Corrupt(_)),
                        "invalid/missing physical witness is corruption, not a stop: {error:?}"
                    ),
                }
            }
            drop(pressure.lock().expect("pressure mutex").take());
            assert_eq!(
                budget.used(),
                0,
                "release new-side owner, failed acquisition and metadata on {failure:?}"
            );
            drop(session);
        }
    }
}

impl Failure {
    fn error(self) -> Error {
        match self {
            Self::Cancelled => {
                let control = Control::new(Instant::now() + Duration::from_secs(60));
                control.cancel();
                Box::new(control.check().unwrap_err())
            }
            Self::Deadline => {
                let control = Control::new(Instant::now() - Duration::from_secs(1));
                Box::new(control.check().unwrap_err())
            }
            Self::Capacity => Box::new(CapacityBudget::new(0).reservation(1).err().unwrap()),
            Self::Allocation => Box::new(CapacityError::Allocation),
            Self::CapacityStopped => Box::new(CapacityError::Stopped(StopReason::Deadline)),
            Self::Corrupt => Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "physical object header is malformed",
            )),
        }
    }

    fn expected(self) -> BlameError {
        match self {
            Self::Cancelled => BlameError::Stopped(StopReason::Cancelled),
            Self::Deadline | Self::CapacityStopped => BlameError::Stopped(StopReason::Deadline),
            Self::Capacity => BlameError::Capacity(CapacityError::Exhausted),
            Self::Allocation => BlameError::Capacity(CapacityError::Allocation),
            Self::Corrupt => BlameError::Corrupt("object provider failed"),
        }
    }
}

// Entirely in-memory, hash-verified physical objects; no Git or native fixture.
struct Provider {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
    commit: ObjectId,
    commit_headers: Cell<usize>,
    failures: Arc<AtomicUsize>,
    failure: Option<Failure>,
    wrapped: bool,
    cancel_after_failure: bool,
}

impl Provider {
    fn new(failure: Option<Failure>, wrapped: bool, cancel_after_failure: bool) -> Self {
        let mut objects = BTreeMap::new();
        let mut insert = |kind, bytes: Vec<u8>| {
            let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap();
            objects.insert(id, (kind, bytes));
            id
        };
        let blob = insert(Kind::Blob, b"one\ntwo\n".to_vec());
        let mut tree = b"100644 file\0".to_vec();
        tree.extend_from_slice(blob.as_bytes());
        let tree = insert(Kind::Tree, tree);
        let commit = insert(
            Kind::Commit,
            format!(
                "tree {tree}\nauthor Example <example@example.test> 1 +0000\n\
                 committer Example <example@example.test> 1 +0000\n\nRoot\n"
            )
            .into_bytes(),
        );
        Self {
            identity: SnapshotIdentity {
                source: b"typed-header-fixture".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            },
            objects,
            commit,
            commit_headers: Cell::new(0),
            failures: Arc::new(AtomicUsize::new(0)),
            failure,
            wrapped,
            cancel_after_failure,
        }
    }
}

impl VerifiedObjectProvider for Provider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
        if id == self.commit.as_ref() {
            let calls = self.commit_headers.get() + 1;
            self.commit_headers.set(calls);
            // First: public target preflight. Second: vendor's controlled
            // object read through FindBridge::try_header, before try_find.
            if calls == 2
                && let Some(failure) = self.failure
            {
                self.failures.fetch_add(1, Ordering::Relaxed);
                let error = failure.error();
                let error: Error = if self.wrapped {
                    Box::new(
                        gix_error::Error::from_boxed(error)
                            .and_raise(gix_error::message("controlled provider header"))
                            .and_raise(gix_error::message("physical source lookup")),
                    )
                } else {
                    error
                };
                if self.cancel_after_failure {
                    // Cleanup may observe a later stop, but must not replace
                    // the first recorded capacity/corruption/deadline cause.
                    control.cancel();
                }
                return Err(error);
            }
        }
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
        control.check()?;
        Ok(Some(Object { kind: *kind, data }))
    }
}

fn check_failure(failure: Failure, wrapped: bool, cancel_after_failure: bool) {
    let provider = Provider::new(Some(failure), wrapped, cancel_after_failure);
    let failures = provider.failures.clone();
    let commit = provider.commit;
    let identity = provider.identity.clone();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let control = Control::new(Instant::now() + Duration::from_secs(60));
    let session = SnapshotSession::open(
        provider,
        &identity,
        control.clone(),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap();
    let result = session.blame_complete(
        BlameRequest {
            commit: &commit,
            path: b"file",
        },
        &BlameOptions::default(),
    );
    assert_eq!(
        failures.load(Ordering::Relaxed),
        1,
        "must reach vendor header read"
    );
    assert_eq!(
        result.err(),
        Some(failure.expected()),
        "{failure:?}, wrapped={wrapped}, later cancellation={cancel_after_failure}",
    );
    if cancel_after_failure {
        assert_eq!(control.check(), Err(StopReason::Cancelled));
    } else {
        assert_eq!(control.check(), Ok(()));
    }
    drop(session);
    assert_eq!(budget.used(), 0, "failed blame must release all admission");
}

#[test]
fn wrapped_header_stop_reasons_remain_typed() {
    for failure in [
        Failure::Cancelled,
        Failure::Deadline,
        Failure::CapacityStopped,
    ] {
        check_failure(failure, true, false);
    }
}

#[test]
fn wrapped_header_capacity_errors_remain_typed() {
    for failure in [Failure::Capacity, Failure::Allocation] {
        check_failure(failure, true, false);
    }
}

#[test]
fn direct_header_errors_remain_compatible() {
    for failure in [
        Failure::Cancelled,
        Failure::Deadline,
        Failure::Capacity,
        Failure::Allocation,
        Failure::CapacityStopped,
        Failure::Corrupt,
    ] {
        check_failure(failure, false, false);
    }
}

#[test]
fn genuine_header_corruption_is_not_a_stop() {
    check_failure(Failure::Corrupt, true, false);
    check_failure(Failure::Corrupt, true, true);
}

#[test]
fn first_header_cause_survives_later_cancellation() {
    check_failure(Failure::Capacity, true, true);
    check_failure(Failure::Deadline, true, true);
}

#[test]
fn physical_fixture_completes_without_injected_failure() {
    let provider = Provider::new(None, false, false);
    let commit = provider.commit;
    let identity = provider.identity.clone();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let session = SnapshotSession::open(
        provider,
        &identity,
        Control::new(Instant::now() + Duration::from_secs(60)),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap();
    let complete = session
        .blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .unwrap();
    assert_eq!(complete.statistics().commits_traversed, 1);
    drop(complete);
    drop(session);
    assert_eq!(budget.used(), 0);
}
