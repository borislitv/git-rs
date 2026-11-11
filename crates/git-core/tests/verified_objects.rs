use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::objects::{Result, VerifiedObject};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(10))
}
fn identity() -> SnapshotIdentity {
    SnapshotIdentity {
        source: b"verified-fixture".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix_hash::Kind::Sha1,
    }
}
fn id(bytes: &[u8]) -> ObjectId {
    gix_object::compute_hash(gix_hash::Kind::Sha1, Kind::Blob, bytes).unwrap()
}
fn object(budget: &CapacityBudget, bytes: &[u8], kind: Kind) -> Object {
    let mut data = budget.buffer(bytes.len()).unwrap();
    data.resize(bytes.len(), 0).unwrap();
    data.as_mut_slice().copy_from_slice(bytes);
    Object { kind, data }
}

type MutableValue = Arc<Mutex<Option<(Kind, Vec<u8>)>>>;

struct MutableProvider {
    identity: SnapshotIdentity,
    value: MutableValue,
    reads: Arc<AtomicUsize>,
}
impl VerifiedObjectProvider for MutableProvider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, _: &oid, _: &Control) -> Result<Option<Header>> {
        unreachable!("body read must not add a header probe")
    }
    fn object(
        &self,
        _: &oid,
        _: &Control,
        budget: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<Object>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .value
            .lock()
            .unwrap()
            .as_ref()
            .map(|(kind, bytes)| object(budget, bytes, *kind)))
    }
}

#[test]
fn generic_predecoded_objects_keep_the_existing_object_limit_contract() {
    let budget = CapacityBudget::new(4096);
    let provider = MutableProvider {
        identity: identity(),
        value: Arc::new(Mutex::new(Some((Kind::Blob, b"hello".to_vec())))),
        reads: Arc::new(AtomicUsize::new(0)),
    };
    // This limit governs pack-chain inflation/workspace, not an already decoded
    // generic provider value. Its object limit still applies independently.
    let limits = ReadLimits {
        max_decode_bytes: 1,
        ..ReadLimits::default()
    };
    let session =
        SnapshotSession::open(provider, &identity(), control(), budget.clone(), limits).unwrap();
    assert_eq!(
        session
            .verified_object(&id(b"hello"))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"hello"
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn generic_mutable_results_are_verified_after_every_read_not_only_the_first() {
    let value = Arc::new(Mutex::new(Some((Kind::Blob, b"hello".to_vec()))));
    let reads = Arc::new(AtomicUsize::new(0));
    let budget = CapacityBudget::new(4096);
    let provider = MutableProvider {
        identity: identity(),
        value: value.clone(),
        reads: reads.clone(),
    };
    let session = SnapshotSession::open(
        provider,
        &identity(),
        control(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(
        session
            .object(&id(b"hello"))
            .unwrap()
            .unwrap()
            .data
            .as_slice(),
        b"hello"
    );
    *value.lock().unwrap() = Some((Kind::Blob, b"world".to_vec()));
    assert!(
        session
            .object(&id(b"hello"))
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    *value.lock().unwrap() = Some((Kind::Tree, b"hello".to_vec()));
    assert!(
        session
            .verified_object(&id(b"hello"))
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    assert_eq!(reads.load(Ordering::SeqCst), 3);
    assert_eq!(budget.used(), 0);
}

#[test]
fn default_missing_and_corrupt_reads_have_no_fallback_or_second_read() {
    let value = Arc::new(Mutex::new(None));
    let reads = Arc::new(AtomicUsize::new(0));
    let budget = CapacityBudget::new(4096);
    let provider = MutableProvider {
        identity: identity(),
        value: value.clone(),
        reads: reads.clone(),
    };
    let session = SnapshotSession::open(
        provider,
        &identity(),
        control(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    assert!(session.object(&id(b"hello")).unwrap().is_none());
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    *value.lock().unwrap() = Some((Kind::Blob, b"world".to_vec()));
    assert!(session.verified_object(&id(b"hello")).is_err());
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    assert_eq!(budget.used(), 0);
}

struct WitnessProvider {
    identity: SnapshotIdentity,
    witness: Mutex<Option<VerifiedObject>>,
    allowed: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
}
impl VerifiedObjectProvider for WitnessProvider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, _: &oid, _: &Control) -> Result<Option<Header>> {
        unreachable!()
    }
    fn object(
        &self,
        _: &oid,
        _: &Control,
        _: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<Object>> {
        panic!("an overridden verified read must not fall back to mutable reads")
    }
    fn verified_object(
        &self,
        _: &oid,
        _: &Control,
        _: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if !self.allowed.load(Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(self.witness.lock().unwrap().take())
    }
}
fn witness_provider(budget: &CapacityBudget, bytes: &[u8]) -> WitnessProvider {
    WitnessProvider {
        identity: identity(),
        witness: Mutex::new(Some(
            VerifiedObject::verify(
                &id(bytes),
                object(budget, bytes, Kind::Blob),
                &control(),
                budget,
                ReadLimits::default(),
            )
            .unwrap(),
        )),
        allowed: Arc::new(AtomicBool::new(true)),
        reads: Arc::new(AtomicUsize::new(0)),
    }
}

#[test]
fn valid_witness_for_a_different_oid_or_budget_is_not_a_valid_requested_read() {
    let budget = CapacityBudget::new(4096);
    let provider = witness_provider(&budget, b"world");
    let session = SnapshotSession::open(
        provider,
        &identity(),
        control(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    assert!(
        session
            .verified_object(&id(b"hello"))
            .unwrap_err()
            .to_string()
            .contains("identity mismatch")
    );
    assert_eq!(budget.used(), 0);
    let separate = CapacityBudget::new(4096);
    let provider = witness_provider(&separate, b"hello");
    let session = SnapshotSession::open(
        provider,
        &identity(),
        control(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    assert!(
        session
            .verified_object(&id(b"hello"))
            .unwrap_err()
            .to_string()
            .contains("budget identity")
    );
    assert_eq!(separate.used(), 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn session_checks_current_limits_stop_and_membership_independently_of_witness() {
    let budget = CapacityBudget::new(4096);
    let provider = witness_provider(&budget, b"hello");
    let limits = ReadLimits {
        max_object_bytes: 4,
        ..ReadLimits::default()
    };
    let session =
        SnapshotSession::open(provider, &identity(), control(), budget.clone(), limits).unwrap();
    assert!(
        session
            .verified_object(&id(b"hello"))
            .unwrap_err()
            .to_string()
            .contains("object byte limit")
    );
    assert_eq!(budget.used(), 0);
    let provider = witness_provider(&budget, b"hello");
    let reads = provider.reads.clone();
    let stopped = control();
    let session = SnapshotSession::open(
        provider,
        &identity(),
        stopped.clone(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    stopped.cancel();
    assert!(session.verified_object(&id(b"hello")).is_err());
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    drop(session);
    assert_eq!(budget.used(), 0);
    let provider = witness_provider(&budget, b"hello");
    let allowed = provider.allowed.clone();
    let reads = provider.reads.clone();
    let session = SnapshotSession::open(
        provider,
        &identity(),
        control(),
        budget.clone(),
        ReadLimits::default(),
    )
    .unwrap();
    let pin = session.verified_object(&id(b"hello")).unwrap().unwrap();
    allowed.store(false, Ordering::SeqCst);
    assert!(session.verified_object(&id(b"hello")).unwrap().is_none());
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "integrity must not cache source membership"
    );
    assert_eq!(pin.as_slice(), b"hello");
    drop(session);
    assert!(budget.used() >= pin.capacity());
    drop(pin);
    assert_eq!(budget.used(), 0);
}

#[test]
fn conversion_discards_witness_and_reverification_rejects_mutated_bytes_or_kind() {
    let budget = CapacityBudget::new(4096);
    let verified = VerifiedObject::verify(
        &id(b"hello"),
        object(&budget, b"hello", Kind::Blob),
        &control(),
        &budget,
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(verified.id(), id(b"hello").as_ref());
    assert_eq!(
        verified.header(),
        Header {
            kind: Kind::Blob,
            size: 5
        }
    );
    let mut mutable = verified.into_object(&control(), &budget).unwrap();
    mutable.data.as_mut_slice()[0] = b'j';
    assert!(
        VerifiedObject::verify(
            &id(b"hello"),
            mutable,
            &control(),
            &budget,
            ReadLimits::default()
        )
        .is_err()
    );
    assert!(
        VerifiedObject::verify(
            &id(b"hello"),
            object(&budget, b"hello", Kind::Tree),
            &control(),
            &budget,
            ReadLimits::default()
        )
        .is_err()
    );
    assert_eq!(budget.used(), 0);
}
