use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::objects::{Error, Result};
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
