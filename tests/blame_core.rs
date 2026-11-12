#![forbid(unsafe_code)]

use git_rs::{
    Result,
    blame::{Completed, JobEvent, JobObserver, Jobs, Request},
    process::Operation,
};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
    blame::{BlameOptions, BlameRequest},
    porcelain::{CompletePorcelain, Porcelain},
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

#[path = "support/blame_source_support.rs"]
mod source_support;

#[path = "support/blame_runtime_support.rs"]
mod runtime_support;

#[path = "support/blame_body_auth_support.rs"]
mod body_auth_support;

fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "observable job state did not converge"
        );
        std::thread::yield_now();
    }
}

#[derive(Default)]
struct Gate(Mutex<bool>, Condvar);
impl Gate {
    fn open(&self) {
        *self.0.lock().unwrap() = true;
        self.1.notify_all();
    }
    fn wait(&self, control: &Control) -> Result<()> {
        let mut open = self.0.lock().unwrap();
        while !*open {
            control.check()?;
            open = self
                .1
                .wait_timeout(open, control.remaining().min(Duration::from_millis(10)))
                .unwrap()
                .0;
        }
        Ok(())
    }
}

type JobRequest = std::thread::JoinHandle<Result<Option<Arc<Completed<usize>>>>>;

fn occupy_worker(jobs: Arc<Jobs<usize>>) -> (JobRequest, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    let worker_gate = gate.clone();
    let (entered, started) = mpsc::channel();
    let request = std::thread::spawn(move || {
        Operation::new(6).enter(|| {
            jobs.run(b"other".to_vec(), 6, move |control, _| {
                entered.send(()).unwrap();
                worker_gate.wait(control)?;
                Ok(1)
            })
        })
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    (request, gate)
}

struct LifecycleGate {
    event: JobEvent,
    entered: Mutex<Option<mpsc::Sender<()>>>,
    gate: Arc<Gate>,
}

struct QueuedCleanup {
    entered: mpsc::Sender<()>,
    gate: Arc<Gate>,
}
impl Drop for QueuedCleanup {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        self.gate
            .wait(&Control::new(Instant::now() + Duration::from_secs(6)))
            .unwrap();
    }
}
impl JobObserver for LifecycleGate {
    fn observe(&self, key: &[u8], event: JobEvent, deadline: Instant) {
        if key == b"K" && event == self.event {
            let entered = self.entered.lock().unwrap().take();
            if let Some(entered) = entered {
                entered.send(()).unwrap();
                // QueuedAbandoned is already cancelled. This notification gate
                // has the same finite deadline but is not a new job/work budget.
                self.gate.wait(&Control::new(deadline)).unwrap();
            }
        }
    }
}

#[test]
fn healthy_same_key_after_queued_abandonment_is_fresh_or_busy_never_failed_flight() {
    // With no spare admission the retry is Busy until discard. With one spare
    // admission it must create distinct healthy work, never revive the old K.
    for queued in [1, 2] {
        let jobs = Arc::new(Jobs::<usize>::new(1, queued).unwrap());
        let (running, gate) = occupy_worker(jobs.clone());
        let operation = Operation::new(6);
        let old_calls = Arc::new(AtomicUsize::new(0));
        let cleanup_gate = Arc::new(Gate::default());
        let (cleanup_started, cleanup_rx) = mpsc::channel();
        let cleanup = QueuedCleanup {
            entered: cleanup_started,
            gate: cleanup_gate.clone(),
        };
        let (queued_jobs, queued_operation, calls) =
            (jobs.clone(), operation.clone(), old_calls.clone());
        let abandoned = std::thread::spawn(move || {
            queued_operation.enter(|| {
                queued_jobs.run(b"K".to_vec(), 6, move |_, _| {
                    let _cleanup = cleanup;
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok(999)
                })
            })
        });
        until(|| jobs.waiters() == 2);
        operation.cancel();
        assert!(abandoned.join().unwrap().is_err());
        cleanup_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(jobs.waiters(), 1);
        assert_eq!(
            jobs.admitted(),
            2,
            "queued cancellation released admission before executor discard"
        );
        let (retry_jobs, fresh_calls) = (jobs.clone(), Arc::new(AtomicUsize::new(0)));
        let calls = fresh_calls.clone();
        let (done, finished) = mpsc::channel();
        let retry = std::thread::spawn(move || {
            let result = Operation::new(6).enter(|| {
                retry_jobs.run(b"K".to_vec(), 6, move |_, _| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok(2)
                })
            });
            done.send(result).unwrap();
        });
        let mut early = None;
        until(|| {
            if let Ok(result) = finished.try_recv() {
                early = Some(result);
                true
            } else {
                jobs.waiters() == 2
            }
        });
        // The healthy retry was attempted while the sole worker was occupied.
        // Release that worker only after Busy or actual waiter enrollment.
        cleanup_gate.open();
        gate.open();
        drop(running.join().unwrap().unwrap().unwrap());
        let result =
            early.unwrap_or_else(|| finished.recv_timeout(Duration::from_secs(2)).unwrap());
        retry.join().unwrap();
        match result {
            Ok(Some(body)) => {
                assert_eq!(**body, 2);
                assert_eq!(fresh_calls.load(Ordering::Acquire), 1);
                drop(body);
            }
            Ok(None) => assert_eq!(fresh_calls.load(Ordering::Acquire), 0),
            Err(error) => panic!("healthy retry joined an abandoned flight: {error}"),
        }
        assert_eq!(
            old_calls.load(Ordering::Acquire),
            0,
            "abandoned queued source was prepared"
        );
        until(|| jobs.admitted() == 0);
        assert_eq!(jobs.waiters(), 0);
        assert_eq!(
            **Operation::new(2)
                .enter(|| jobs.run(b"K".to_vec(), 2, |_, _| Ok(3)))
                .unwrap()
                .unwrap(),
            3
        );
    }
}

#[test]
fn late_abandon_notification_cannot_cancel_a_replacement_that_has_started() {
    let detached_gate = Arc::new(Gate::default());
    let (detached, observed) = mpsc::channel();
    let observer = Arc::new(LifecycleGate {
        event: JobEvent::QueuedAbandoned,
        entered: Mutex::new(Some(detached)),
        gate: detached_gate.clone(),
    });
    let jobs = Arc::new(Jobs::<usize>::new(1, 2).unwrap().with_observer(observer));
    let (running, gate) = occupy_worker(jobs.clone());
    let operation = Operation::new(6);
    let (queued_jobs, queued_operation) = (jobs.clone(), operation.clone());
    let abandoned = std::thread::spawn(move || {
        queued_operation.enter(|| {
            queued_jobs.run(b"K".to_vec(), 6, |_, _| {
                panic!("abandoned work was prepared")
            })
        })
    });
    until(|| jobs.waiters() == 2);
    operation.cancel();
    // Detachment/cancellation is committed but the departing request has not
    // returned yet. The observer runs without the flight/map locks.
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(jobs.waiters(), 1);
    assert_eq!(jobs.admitted(), 2);
    let fresh_gate = Arc::new(Gate::default());
    let (fresh_started, started) = mpsc::channel();
    let (retry_jobs, prepared_gate) = (jobs.clone(), fresh_gate.clone());
    let retry = std::thread::spawn(move || {
        Operation::new(6).enter(|| {
            retry_jobs.run(b"K".to_vec(), 6, move |control, _| {
                fresh_started.send(()).unwrap();
                prepared_gate.wait(control)?;
                control.check()?;
                Ok(7)
            })
        })
    });
    until(|| jobs.waiters() == 2);
    assert_eq!(
        jobs.admitted(),
        3,
        "replacement reused the abandoned work owner"
    );
    gate.open();
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    // Finish the old departure only after the replacement really started. There
    // must be no deferred cancellation after the abandonment linearization.
    detached_gate.open();
    assert!(abandoned.join().unwrap().is_err());
    fresh_gate.open();
    let complete = retry.join().unwrap().unwrap().unwrap();
    assert_eq!(**complete, 7);
    drop(complete);
    drop(running.join().unwrap().unwrap().unwrap());
    until(|| jobs.admitted() == 0);
    assert_eq!(jobs.waiters(), 0);
}

#[test]
fn queued_discard_reuses_root_admission_before_unrelated_running_work_finishes() {
    let jobs = Arc::new(Jobs::<usize>::new(1, 1).unwrap());
    let (active, gate) = occupy_worker(jobs.clone());
    let operation = Operation::new(6);
    let (queued_jobs, queued_operation) = (jobs.clone(), operation.clone());
    let queued = std::thread::spawn(move || {
        queued_operation.enter(|| {
            queued_jobs.run(b"cancelled".to_vec(), 6, |_, _| {
                panic!("cancelled queue acquired a source")
            })
        })
    });
    until(|| jobs.waiters() == 2);
    operation.cancel();
    assert!(queued.join().unwrap().is_err());
    until(|| jobs.admitted() == 1);
    assert_eq!(jobs.waiters(), 1);
    let replacement_jobs = jobs.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let prepared = calls.clone();
    let replacement = std::thread::spawn(move || {
        Operation::new(6).enter(|| {
            replacement_jobs.run(b"replacement".to_vec(), 6, move |_, _| {
                prepared.fetch_add(1, Ordering::AcqRel);
                Ok(8)
            })
        })
    });
    until(|| jobs.waiters() == 2);
    assert_eq!(jobs.admitted(), 2);
    assert_eq!(
        calls.load(Ordering::Acquire),
        0,
        "replacement prepared before a worker was free"
    );
    gate.open();
    drop(active.join().unwrap().unwrap().unwrap());
    let complete = replacement.join().unwrap().unwrap().unwrap();
    assert_eq!(**complete, 8);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(complete);
    until(|| jobs.admitted() == 0);
    assert_eq!(jobs.waiters(), 0);
}

#[test]
fn started_observer_panic_keeps_root_admission_until_unused_capture_cleanup() {
    struct Panics;
    impl JobObserver for Panics {
        fn observe(&self, _: &[u8], _: JobEvent, _: Instant) {
            panic!("synthetic pre-prepare observer panic");
        }
    }
    let jobs = Arc::new(
        Jobs::<usize>::new(1, 0)
            .unwrap()
            .with_observer(Arc::new(Panics)),
    );
    let cleanup_gate = Arc::new(Gate::default());
    let (started, started_rx) = mpsc::channel();
    let cleanup = QueuedCleanup {
        entered: started,
        gate: cleanup_gate.clone(),
    };
    let requests = jobs.clone();
    let request = std::thread::spawn(move || {
        Operation::new(6).enter(|| {
            requests.run(b"panic".to_vec(), 6, move |_, _| {
                let _cleanup = cleanup;
                panic!("source acquired after observer failed");
            })
        })
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        jobs.admitted(),
        1,
        "panic released admission while captured cleanup was still gated"
    );
    cleanup_gate.open();
    assert!(request.join().unwrap().is_err());
    until(|| jobs.admitted() == 0);
    assert_eq!(jobs.waiters(), 0);
}

#[test]
fn worker_start_before_last_detach_keeps_running_flight_joinable() {
    let start_gate = Arc::new(Gate::default());
    let (started, observed) = mpsc::channel();
    let observer = Arc::new(LifecycleGate {
        event: JobEvent::Started,
        entered: Mutex::new(Some(started)),
        gate: start_gate.clone(),
    });
    let jobs = Arc::new(Jobs::<usize>::new(1, 1).unwrap().with_observer(observer));
    let (running, gate) = occupy_worker(jobs.clone());
    let operation = Operation::new(6);
    let calls = Arc::new(AtomicUsize::new(0));
    let (queued_jobs, queued_operation, prepared_calls) =
        (jobs.clone(), operation.clone(), calls.clone());
    let first = std::thread::spawn(move || {
        queued_operation.enter(|| {
            queued_jobs.run(b"K".to_vec(), 6, move |control, _| {
                control.check()?;
                prepared_calls.fetch_add(1, Ordering::AcqRel);
                Ok(11)
            })
        })
    });
    until(|| jobs.waiters() == 2);
    gate.open();
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(running.join().unwrap().unwrap().unwrap());
    operation.cancel();
    assert!(first.join().unwrap().is_err());
    assert_eq!(jobs.waiters(), 0);
    assert_eq!(jobs.admitted(), 1);
    let retry_jobs = jobs.clone();
    let retry = std::thread::spawn(move || {
        Operation::new(6).enter(|| {
            retry_jobs.run(b"K".to_vec(), 6, |_, _| {
                panic!("running shared work was replaced")
            })
        })
    });
    until(|| jobs.waiters() == 1);
    start_gate.open();
    let complete = retry.join().unwrap().unwrap().unwrap();
    assert_eq!(**complete, 11);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(complete);
    until(|| jobs.admitted() == 0);
    assert_eq!(jobs.waiters(), 0);
}

#[test]
fn two_jobs_overlap_one_queues_without_preparation_and_overflow_is_rejected() {
    let jobs = Arc::new(Jobs::<usize>::new(2, 1).unwrap());
    let (started, observed) = mpsc::channel();
    let gates = [
        Arc::new(Gate::default()),
        Arc::new(Gate::default()),
        Arc::new(Gate::default()),
    ];
    let mut requests = Vec::new();
    for (index, gate) in gates.iter().take(2).enumerate() {
        let (jobs, gate, started) = (jobs.clone(), gate.clone(), started.clone());
        requests.push(std::thread::spawn(move || {
            Operation::new(5).enter(|| {
                jobs.run(vec![index as u8 + 1], 4, move |control, _| {
                    started.send(index).unwrap();
                    gate.wait(control)?;
                    Ok(index)
                })
            })
        }));
    }
    let a = observed.recv_timeout(Duration::from_secs(2)).unwrap();
    let b = observed.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_ne!(a, b, "both real execution slots must overlap");
    let (queued_jobs, gate, queued_started) = (jobs.clone(), gates[2].clone(), started.clone());
    requests.push(std::thread::spawn(move || {
        Operation::new(5).enter(|| {
            queued_jobs.run(vec![3], 4, move |control, _| {
                queued_started.send(2).unwrap();
                gate.wait(control)?;
                Ok(2)
            })
        })
    }));
    until(|| jobs.waiters() == 3);
    assert!(
        matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "queued source preparation ran early"
    );
    assert!(
        Operation::new(2)
            .enter(|| jobs.run(vec![4], 2, |_, _| panic!("overflow prepared a source")))
            .unwrap()
            .is_none()
    );
    gates[0].open();
    assert_eq!(observed.recv_timeout(Duration::from_secs(2)).unwrap(), 2);
    gates[1].open();
    gates[2].open();
    let results = requests
        .into_iter()
        .map(|request| request.join().unwrap().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results.iter().map(|result| ***result).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        jobs.admitted(),
        3,
        "completed consumers must retain actual admission"
    );
    assert!(
        Operation::new(2)
            .enter(|| jobs.run(vec![5], 2, |_, _| Ok(5)))
            .unwrap()
            .is_none()
    );
    drop(results);
    until(|| jobs.admitted() == 0);
    assert_eq!(
        **Operation::new(2)
            .enter(|| jobs.run(vec![6], 2, |_, _| Ok(6)))
            .unwrap()
            .unwrap(),
        6
    );
}

#[test]
fn cancelling_first_waiter_does_not_cancel_shared_running_derivation() {
    let jobs = Arc::new(Jobs::<usize>::new(1, 0).unwrap());
    let first = Operation::new(4);
    let gate = Arc::new(Gate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let (started, observed) = mpsc::channel();
    let (work_jobs, work_gate, work_calls, operation) =
        (jobs.clone(), gate.clone(), calls.clone(), first.clone());
    let creator = std::thread::spawn(move || {
        operation.enter(|| {
            work_jobs.run(vec![1], 3, move |control, _| {
                work_calls.fetch_add(1, Ordering::AcqRel);
                started.send(()).unwrap();
                work_gate.wait(control)?;
                Ok(17)
            })
        })
    });
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    let joined_jobs = jobs.clone();
    let joined = std::thread::spawn(move || {
        Operation::new(4).enter(|| {
            joined_jobs.run(vec![1], 3, |_, _| {
                panic!("same-key waiter executed a second derivation")
            })
        })
    });
    until(|| jobs.waiters() == 2);
    first.cancel();
    assert!(creator.join().unwrap().is_err());
    assert_eq!(
        jobs.admitted(),
        1,
        "cancelled creator released a still-running job"
    );
    gate.open();
    let complete = joined.join().unwrap().unwrap().unwrap();
    assert_eq!(**complete, 17);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(complete);
    until(|| jobs.admitted() == 0);
}

#[test]
fn abandoned_queued_waiter_never_acquires_source_and_admission_recovers() {
    queued_waiter_stops(false);
}

#[test]
fn queued_waiter_uses_original_deadline_without_resetting_it() {
    queued_waiter_stops(true);
}

fn queued_waiter_stops(expire: bool) {
    let jobs = Arc::new(Jobs::<usize>::new(1, 1).unwrap());
    let gate = Arc::new(Gate::default());
    let (started, observed) = mpsc::channel();
    let (running_jobs, running_gate) = (jobs.clone(), gate.clone());
    let running = std::thread::spawn(move || {
        Operation::new(5).enter(|| {
            running_jobs.run(vec![1], 4, move |control, _| {
                started.send(()).unwrap();
                running_gate.wait(control)?;
                Ok(1)
            })
        })
    });
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    let queued_operation = if expire {
        Operation::until(Instant::now() + Duration::from_secs(1))
    } else {
        Operation::new(4)
    };
    let (queued_jobs, operation) = (jobs.clone(), queued_operation.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let source_calls = calls.clone();
    let queued = std::thread::spawn(move || {
        operation.enter(|| {
            queued_jobs.run(vec![2], 3, move |_, _| {
                source_calls.fetch_add(1, Ordering::AcqRel);
                Ok(2)
            })
        })
    });
    until(|| jobs.waiters() == 2);
    if !expire {
        queued_operation.cancel();
    }
    assert!(queued.join().unwrap().is_err());
    if expire {
        assert!(
            Instant::now() < queued_operation.deadline() + Duration::from_secs(1),
            "queued waiting reset the original request deadline"
        );
    }
    assert_eq!(calls.load(Ordering::Acquire), 0);
    gate.open();
    drop(running.join().unwrap().unwrap().unwrap());
    until(|| jobs.admitted() == 0);
    assert_eq!(
        **Operation::new(2)
            .enter(|| jobs.run(vec![3], 2, |_, _| Ok(3)))
            .unwrap()
            .unwrap(),
        3
    );
    assert_eq!(
        calls.load(Ordering::Acquire),
        0,
        "abandoned queued source was acquired after its waiter left"
    );
}

#[test]
fn failed_derivation_has_no_complete_result_and_releases_ownership() {
    let jobs = Jobs::<usize>::new(1, 0).unwrap();
    assert!(
        Operation::new(2)
            .enter(|| jobs.run(vec![1], 2, |_, _| Err(git_rs::invalid(
                "synthetic dependency unavailable"
            ))))
            .is_err()
    );
    until(|| jobs.admitted() == 0);
    assert_eq!(
        **Operation::new(2)
            .enter(|| jobs.run(vec![2], 2, |_, _| Ok(2)))
            .unwrap()
            .unwrap(),
        2
    );
}

#[test]
fn final_running_waiter_can_leave_without_releasing_actual_work_or_its_source() {
    let jobs = Arc::new(Jobs::<Hold>::new(1, 0).unwrap());
    let operation = Operation::new(4);
    let gate = Arc::new(Gate::default());
    let released = Arc::new(AtomicUsize::new(0));
    let (started, observed) = mpsc::channel();
    let (running_jobs, running_operation, running_gate, probe) = (
        jobs.clone(),
        operation.clone(),
        gate.clone(),
        released.clone(),
    );
    let request = std::thread::spawn(move || {
        running_operation.enter(|| {
            running_jobs.run(vec![1], 3, move |control, _| {
                let hold = Hold(probe);
                started.send(()).unwrap();
                running_gate.wait(control)?;
                Ok(hold)
            })
        })
    });
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
    operation.cancel();
    assert!(request.join().unwrap().is_err());
    assert_eq!(jobs.admitted(), 1);
    assert_eq!(released.load(Ordering::Acquire), 0);
    gate.open();
    until(|| jobs.admitted() == 0);
    assert_eq!(
        released.load(Ordering::Acquire),
        1,
        "abandoned completed result was not cleaned up"
    );
}

struct Hold(Arc<AtomicUsize>);
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct MemorySource {
    identity: SnapshotIdentity,
    objects: BTreeMap<[u8; 20], (gix::objs::Kind, Vec<u8>)>,
    hold: Arc<Hold>,
}
impl MemorySource {
    fn insert(&mut self, kind: gix::objs::Kind, bytes: &[u8]) -> gix::ObjectId {
        // Independent Git loose-object hash, not the provider/core hash helper.
        let mut physical = format!("{kind} {}\0", bytes.len()).into_bytes();
        physical.extend_from_slice(bytes);
        let id = openssl::sha::sha1(&physical);
        self.objects.insert(id, (kind, bytes.to_vec()));
        gix::ObjectId::from(id)
    }
    fn get(&self, id: &gix::hash::oid) -> Option<&(gix::objs::Kind, Vec<u8>)> {
        let key: &[u8; 20] = id.as_bytes().try_into().ok()?;
        self.objects.get(key)
    }
}
impl VerifiedObjectProvider for MemorySource {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        control.check()?;
        Ok(self.get(id).map(|(kind, data)| gix::objs::Header {
            kind: *kind,
            size: data.len() as u64,
        }))
    }
    fn object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        let Some((kind, bytes)) = self.get(id) else {
            return Ok(None);
        };
        if bytes.len() > limits.max_object_bytes {
            return Err(git_rs::invalid("fixture object limit exceeded"));
        }
        let mut data = budget.buffer(bytes.len())?;
        for part in bytes.chunks(32 * 1024) {
            control.check()?;
            let start = data.as_slice().len();
            data.resize(start + part.len(), 0)?;
            data.as_mut_slice()[start..].copy_from_slice(part);
        }
        Ok(Some(Object { kind: *kind, data }))
    }
}
struct Output {
    body: CompletePorcelain,
    _hold: Arc<Hold>,
    budget: CapacityBudget,
}

#[test]
fn real_core_porcelain_survives_job_scope_and_holds_capacity_and_source_until_consumed() {
    let released = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Hold(released.clone()));
    let identity = SnapshotIdentity {
        source: b"synthetic".to_vec(),
        generation: vec![1],
        object_hash: gix::hash::Kind::Sha1,
    };
    let mut source = MemorySource {
        identity: identity.clone(),
        objects: BTreeMap::new(),
        hold,
    };
    let blob = source.insert(gix::objs::Kind::Blob, b"hello\n");
    let mut tree = b"100644 file\0".to_vec();
    tree.extend_from_slice(blob.as_bytes());
    let tree = source.insert(gix::objs::Kind::Tree, &tree);
    let commit = source.insert(gix::objs::Kind::Commit, format!(
        "tree {tree}\nauthor A <a@example.invalid> 1 +0000\ncommitter B <b@example.invalid> 2 +0000\n\nroot\n").as_bytes());
    let expected = format!(
        "{commit} 1 1 1\nauthor A\nauthor-mail <a@example.invalid>\nauthor-time 1\nauthor-tz +0000\ncommitter B\ncommitter-mail <b@example.invalid>\ncommitter-time 2\ncommitter-tz +0000\nsummary root\nfilename file\n\thello\n"
    );
    let jobs = Jobs::new(1, 0).unwrap();
    let complete = Operation::new(5)
        .enter(|| {
            jobs.run(vec![1], 4, move |control, budget| {
                let hold = source.hold.clone();
                let options = BlameOptions::default();
                let session = SnapshotSession::open(
                    source,
                    &identity,
                    control.clone(),
                    budget.clone(),
                    options.read_limits(),
                )?;
                let blame = session.blame_complete(
                    BlameRequest {
                        commit: &commit,
                        path: b"file",
                    },
                    &options,
                )?;
                let body = Porcelain::encode_complete(&blame, control, budget, 256 * 1024 * 1024)?;
                Ok(Output {
                    body,
                    _hold: hold,
                    budget: budget.clone(),
                })
            })
        })
        .unwrap()
        .unwrap();
    let actual = complete
        .body
        .chunks()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        expected.as_bytes(),
        "full porcelain, including root/group/metadata/framing, differs"
    );
    assert_eq!(complete.body.len(), actual.len() as u64);
    assert_eq!(
        released.load(Ordering::Acquire),
        0,
        "source released before consumption"
    );
    let budget = complete.budget.clone();
    assert!(budget.used() > 0, "sealed output lost its capacity owner");
    assert_eq!(jobs.admitted(), 1);
    drop(complete);
    until(|| jobs.admitted() == 0);
    assert_eq!(released.load(Ordering::Acquire), 1);
    assert_eq!(budget.used(), 0);
}

#[test]
fn request_paths_are_literal_bytes_and_plus_is_not_a_form_space() {
    let commit = "1".repeat(40);
    let request = Request::parse(&commit, "path=%FF%09literal+%25%2A").unwrap();
    assert_eq!(request.path(), b"\xff\tliteral+%*");
    assert_eq!(request.commit().hex(), commit);
    assert!(Request::parse(&commit, &format!("path={}", "x".repeat(4096))).is_ok());
    assert!(Request::parse(&commit, &format!("path={}", "x".repeat(4097))).is_err());
    for query in [
        "path=",
        "path=/file",
        "path=a//b",
        "path=a/../b",
        "path=%00",
        "path=%",
        "path=%GG",
        "path=x&path=y",
    ] {
        assert!(
            Request::parse(&commit, query).is_err(),
            "accepted {query:?}"
        );
    }
}

struct SourceGate {
    gate: Arc<Gate>,
    ready: Mutex<Option<mpsc::Sender<()>>>,
}
impl git_rs::blame::Observer for SourceGate {
    fn observe(&self, phase: git_rs::blame::Phase, control: &Control) -> Result<()> {
        if phase == git_rs::blame::Phase::SourceAcquired {
            let ready = self.ready.lock().unwrap().take();
            if let Some(ready) = ready {
                ready.send(()).unwrap();
                self.gate.wait(control)?;
            }
        }
        Ok(())
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_native_parity_lazy_selected_packs_and_batched_sql() {
    use git_rs::blame::{Blame, Response};
    let fixture = source_support::Fixture::new(Blame::default(), 1000);
    let sql = source_support::metric("sql");
    let body = fixture.complete();
    assert_eq!(fixture.leases(), 1);
    let files = fixture.source_files();
    assert!(files.contains(&format!("pack-{}.pack", fixture.selected)));
    assert!(
        !files.contains(&format!("pack-{}.pack", fixture.unrelated)),
        "unrelated pack was eagerly downloaded"
    );
    assert!(
        fixture.engine.cache.allocated() > 0,
        "verified immutable cache pins were not admitted"
    );
    struct Consuming<'a> {
        fixture: &'a source_support::Fixture,
        bytes: Vec<u8>,
        acknowledged: bool,
    }
    impl std::io::Write for Consuming<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            assert_eq!(
                self.fixture.leases(),
                1,
                "source released before output acknowledgment"
            );
            assert!(!self.fixture.source_files().is_empty());
            self.acknowledged = true;
            Ok(())
        }
    }
    let mut output = Consuming {
        fixture: &fixture,
        bytes: Vec::new(),
        acknowledged: false,
    };
    Operation::new(10)
        .enter(|| body.write_to(&mut output))
        .unwrap();
    assert!(output.acknowledged);
    assert_eq!(
        output.bytes, fixture.expected,
        "complete native Git 2.56 bytes differ"
    );
    assert_eq!(body.len(), fixture.expected.len() as u64);
    let sql_calls = source_support::metric("sql") - sql;
    eprintln!("owned source: lines=1000 sql_calls={sql_calls} acquired_files={files:?}");
    assert!(
        sql_calls < 40,
        "{sql_calls} SQL calls for 1000 lines; authority must be batch-boundary only"
    );
    drop(body);
    assert_eq!(fixture.leases(), 0);
    assert!(fixture.source_files().is_empty());
    let dangling = Request::parse(&fixture.dangling, "path=file").unwrap();
    assert!(matches!(
        Operation::new(10)
            .enter(|| fixture.engine.blame(&fixture.slug, "bob", &dangling))
            .unwrap(),
        Response::NotFound
    ));
    assert!(
        Operation::new(10)
            .enter(|| fixture
                .engine
                .blame(&fixture.slug, "carol", &fixture.request()))
            .is_err()
    );
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_actual_grants_generation_storage_epoch_and_lease_fence_responses() {
    let fixture = source_support::Fixture::new(git_rs::blame::Blame::default(), 1);
    for fence in ["generation=generation+1", "storage_epoch=storage_epoch+1"] {
        let body = fixture.complete();
        fixture.fence(fence);
        let mut output = Vec::new();
        assert!(
            Operation::new(5)
                .enter(|| body.write_to(&mut output))
                .is_err(),
            "accepted {fence}"
        );
        assert!(output.is_empty());
        drop(body);
        assert_eq!(fixture.leases(), 0);
    }
    let body = fixture.complete();
    fixture.revoke();
    assert!(Operation::new(5).enter(|| body.check()).is_err());
    drop(body);
    fixture.restored_reader();
    let body = fixture.complete();
    fixture.sql(&format!("UPDATE repository_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE repo_id={}::uuid", git_rs::metadata::text(&fixture.repo)));
    assert!(Operation::new(5).enter(|| body.check()).is_err());
    drop(body);
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_default_is_one_running_no_queue_and_overflow_has_no_source_sql() {
    let gate = Arc::new(Gate::default());
    let (ready, observed) = mpsc::channel();
    let observer = Arc::new(SourceGate {
        gate: gate.clone(),
        ready: Mutex::new(Some(ready)),
    });
    let fixture =
        source_support::Fixture::new(git_rs::blame::Blame::default().with_observer(observer), 1);
    let (engine, slug, query) = (
        fixture.engine.clone(),
        fixture.slug.clone(),
        fixture.request(),
    );
    let running =
        std::thread::spawn(move || Operation::new(15).enter(|| engine.blame(&slug, "bob", &query)));
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    let sql = source_support::metric("sql");
    let distinct = Request::parse(&fixture.commit, "path=distinct").unwrap();
    assert!(matches!(
        Operation::new(5)
            .enter(|| fixture.engine.blame(&fixture.slug, "bob", &distinct))
            .unwrap(),
        git_rs::blame::Response::Busy
    ));
    assert_eq!(
        source_support::metric("sql"),
        sql,
        "overflow acquired source authority before admission"
    );
    assert_eq!(fixture.leases(), 1);
    gate.open();
    drop(running.join().unwrap().unwrap());
    until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_last_cancelled_waiter_retains_real_barrier_until_worker_stops() {
    let gate = Arc::new(Gate::default());
    let (ready, observed) = mpsc::channel();
    let observer = Arc::new(SourceGate {
        gate: gate.clone(),
        ready: Mutex::new(Some(ready)),
    });
    let fixture = source_support::Fixture::new(
        git_rs::blame::Blame::new(2, 1)
            .unwrap()
            .with_observer(observer),
        1,
    );
    let operation = Operation::new(15);
    let (engine, slug, query, worker_operation) = (
        fixture.engine.clone(),
        fixture.slug.clone(),
        fixture.request(),
        operation.clone(),
    );
    let request =
        std::thread::spawn(move || worker_operation.enter(|| engine.blame(&slug, "bob", &query)));
    observed.recv_timeout(Duration::from_secs(3)).unwrap();
    operation.cancel();
    assert!(request.join().unwrap().is_err());
    assert_eq!(fixture.engine.blame.admitted(), 1);
    assert_eq!(
        fixture.leases(),
        1,
        "request cancellation released actual running source ownership"
    );
    assert_eq!(fs_count(&fixture.engine.scratch_root), 1);
    gate.open();
    until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.leases(), 0);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 0);
    drop(fixture.complete());
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_queued_discard_reuses_two_plus_one_capacity_without_source_acquisition() {
    struct Sources {
        preparing: AtomicUsize,
        acquired: AtomicUsize,
        gate: Arc<Gate>,
    }
    impl git_rs::blame::Observer for Sources {
        fn observe(&self, phase: git_rs::blame::Phase, control: &Control) -> Result<()> {
            if phase == git_rs::blame::Phase::Preparing {
                self.preparing.fetch_add(1, Ordering::AcqRel);
            }
            if phase == git_rs::blame::Phase::SourceAcquired {
                self.acquired.fetch_add(1, Ordering::AcqRel);
                self.gate.wait(control)?;
            }
            Ok(())
        }
    }
    let gate = Arc::new(Gate::default());
    let sources = Arc::new(Sources {
        preparing: AtomicUsize::new(0),
        acquired: AtomicUsize::new(0),
        gate: gate.clone(),
    });
    let fixture = source_support::Fixture::new(
        git_rs::blame::Blame::new(2, 1)
            .unwrap()
            .with_observer(sources.clone()),
        2,
    );
    let request = |path: &str, operation: Arc<Operation>| {
        let (engine, slug, query) = (
            fixture.engine.clone(),
            fixture.slug.clone(),
            Request::parse(&fixture.commit, path).unwrap(),
        );
        std::thread::spawn(move || operation.enter(|| engine.blame(&slug, "bob", &query)))
    };
    let first = request("path=file", Operation::new(10));
    let second = request("path=missing-one", Operation::new(10));
    until(|| sources.acquired.load(Ordering::Acquire) == 2);
    assert_eq!(fixture.leases(), 2);
    let cancelled = Operation::new(10);
    let queued = request("path=cancelled", cancelled.clone());
    until(|| fixture.engine.blame.waiters() == 3);
    cancelled.cancel();
    assert!(queued.join().unwrap().is_err());
    until(|| fixture.engine.blame.admitted() == 2);
    assert_eq!(fixture.engine.blame.waiters(), 2);
    assert_eq!(sources.preparing.load(Ordering::Acquire), 2);
    assert_eq!(fixture.leases(), 2);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 2);
    let replacement = request("path=replacement", Operation::new(10));
    until(|| fixture.engine.blame.waiters() == 3);
    assert_eq!(fixture.engine.blame.admitted(), 3);
    assert_eq!(
        sources.preparing.load(Ordering::Acquire),
        2,
        "replacement prepared while both workers were gated"
    );
    gate.open();
    assert!(matches!(
        first.join().unwrap().unwrap(),
        git_rs::blame::Response::Complete(_)
    ));
    assert!(matches!(
        second.join().unwrap().unwrap(),
        git_rs::blame::Response::NotFound
    ));
    assert!(matches!(
        replacement.join().unwrap().unwrap(),
        git_rs::blame::Response::NotFound
    ));
    assert_eq!(
        sources.preparing.load(Ordering::Acquire),
        3,
        "cancelled queue acquired a source"
    );
    until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.engine.blame.waiters(), 0);
    assert_eq!(fixture.leases(), 0);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_http_disconnect_reuses_queue_while_both_cpu_workers_remain_gated() {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
    };
    let gate = Arc::new(Gate::default());
    let (ready, acquired) = mpsc::channel();
    struct HttpSources {
        gate: Arc<Gate>,
        ready: mpsc::Sender<()>,
    }
    impl git_rs::blame::Observer for HttpSources {
        fn observe(&self, phase: git_rs::blame::Phase, control: &Control) -> Result<()> {
            if phase == git_rs::blame::Phase::SourceAcquired {
                self.ready.send(()).unwrap();
                self.gate.wait(control)?;
            }
            Ok(())
        }
    }
    let observer = Arc::new(HttpSources {
        gate: gate.clone(),
        ready,
    });
    let fixture = source_support::Fixture::new(
        git_rs::blame::Blame::new(2, 1)
            .unwrap()
            .with_observer(observer),
        2,
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let (engine, handlers) = (fixture.engine.clone(), active.clone());
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for _ in 0..4 {
            let (stream, _) = listener.accept().unwrap();
            let (engine, handlers) = (engine.clone(), handlers.clone());
            handlers.fetch_add(1, Ordering::AcqRel);
            requests.push(std::thread::spawn(move || {
                let result = git_rs::http::handle(stream, &engine, "bob");
                handlers.fetch_sub(1, Ordering::AcqRel);
                result
            }));
        }
        requests
            .into_iter()
            .map(|request| request.join().unwrap())
            .collect::<Vec<_>>()
    });
    let request = |path: &str| {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(8)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(
            stream,
            "GET /{}.git/blame/{}?path={} HTTP/1.1\r\nHost: localhost\r\n\r\n",
            fixture.slug, fixture.commit, path
        )
        .unwrap();
        stream
    };
    let mut first = request("file");
    let mut second = request("missing");
    acquired.recv_timeout(Duration::from_secs(3)).unwrap();
    acquired.recv_timeout(Duration::from_secs(3)).unwrap();
    until(|| fixture.leases() == 2 && fs_count(&fixture.engine.scratch_root) == 2);
    let cancelled = request("cancelled");
    until(|| fixture.engine.blame.waiters() == 3);
    assert_eq!(fixture.engine.blame.admitted(), 3);
    assert_eq!(active.load(Ordering::Acquire), 3);
    drop(cancelled); // exercise the real HTTP peer monitor, not Operation::cancel
    until(|| fixture.engine.blame.waiters() == 2 && active.load(Ordering::Acquire) == 2);
    eprintln!(
        "queued HTTP detached: active={} admitted={} waiters={}",
        active.load(Ordering::Acquire),
        fixture.engine.blame.admitted(),
        fixture.engine.blame.waiters()
    );
    until(|| fixture.engine.blame.admitted() == 2);
    eprintln!(
        "queued discard acknowledged: active={} admitted={} waiters={}",
        active.load(Ordering::Acquire),
        fixture.engine.blame.admitted(),
        fixture.engine.blame.waiters()
    );
    assert_eq!(fixture.leases(), 2);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 2);
    let mut replacement = request("replacement");
    until(|| fixture.engine.blame.waiters() == 3);
    assert_eq!(fixture.engine.blame.admitted(), 3);
    assert_eq!(
        fixture.leases(),
        2,
        "replacement/source started while CPU workers remained gated"
    );
    gate.open();
    let mut complete = Vec::new();
    first.read_to_end(&mut complete).unwrap();
    assert!(complete.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(complete.ends_with(&fixture.expected));
    for stream in [&mut second, &mut replacement] {
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        assert!(
            response.starts_with(b"HTTP/1.1 404 Not Found\r\n"),
            "{}",
            String::from_utf8_lossy(&response)
        );
    }
    let results = server.join().unwrap();
    assert!(results[0].is_ok() && results[1].is_ok() && results[3].is_ok());
    assert!(results[2].is_err());
    until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.engine.blame.waiters(), 0);
    assert_eq!(active.load(Ordering::Acquire), 0);
    assert_eq!(fixture.leases(), 0);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 0);
}

fn fs_count(path: &std::path::Path) -> usize {
    std::fs::read_dir(path).unwrap().count()
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_mid_write_revocation_cannot_finish_content_length_body() {
    let fixture = source_support::Fixture::new(git_rs::blame::Blame::default(), 1000);
    let body = fixture.complete();
    struct Revoking<'a> {
        fixture: &'a source_support::Fixture,
        bytes: Vec<u8>,
        revoked: bool,
    }
    impl std::io::Write for Revoking<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.revoked {
                self.fixture.revoke();
                self.revoked = true;
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Revoking {
        fixture: &fixture,
        bytes: Vec::new(),
        revoked: false,
    };
    assert!(
        Operation::new(10)
            .enter(|| body.write_to(&mut output))
            .is_err()
    );
    assert!(output.revoked && !output.bytes.is_empty());
    assert!(
        output.bytes.len() < body.len() as usize,
        "revoked read emitted a complete successful body"
    );
    assert_eq!(fixture.leases(), 1);
    drop(body);
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_http_blocked_output_keeps_hold_until_deadline_and_write_ack() {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
    };
    let mut fixture = source_support::Fixture::new(git_rs::blame::Blame::default(), 100_000);
    Arc::get_mut(&mut fixture.engine).unwrap().operation_seconds = 4;
    assert!(fixture.expected.len() > 8 * 1024 * 1024);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let engine = fixture.engine.clone();
    let (done, finished) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let failed = git_rs::http::handle(stream, &engine, "bob").is_err();
        let _ = done.send(failed);
    });
    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    write!(
        client,
        "GET /{}.git/blame/{}?path=file HTTP/1.1\r\nHost: localhost\r\n\r\n",
        fixture.slug, fixture.commit
    )
    .unwrap();
    let mut reader = BufReader::new(client);
    let mut header = String::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        header.push_str(&line);
        if line == "\r\n" {
            break;
        }
    }
    assert!(header.starts_with("HTTP/1.1 200 OK"), "{header}");
    assert!(
        matches!(
            finished.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "large write did not actually block"
    );
    assert_eq!(fixture.leases(), 1);
    assert_eq!(fixture.engine.blame.admitted(), 1);
    assert!(
        finished.recv_timeout(Duration::from_secs(6)).unwrap(),
        "blocked writer did not observe its original deadline"
    );
    server.join().unwrap();
    let mut partial = Vec::new();
    reader.read_to_end(&mut partial).unwrap();
    assert!(partial.len() < fixture.expected.len());
    until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.leases(), 0);
    assert_eq!(fs_count(&fixture.engine.scratch_root), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_s3_integrity_failure_has_no_complete_result_and_recovers() {
    let mut fixture = source_support::Fixture::new(git_rs::blame::Blame::default(), 10);
    // Force verified cold acquisition for each fault; a previously verified RAM
    // hit is allowed to survive corruption of its remote immutable backing.
    Arc::get_mut(&mut fixture.engine).unwrap().cache = Arc::new(git_rs::cache::Cache::new(0, 0));
    for (field, extension) in [("idx_key", "idx"), ("pack_key", "pack")] {
        let path = fixture
            .root
            .join("native/objects/pack")
            .join(format!("pack-{}.{}", fixture.selected, extension));
        let key = fixture
            .sql(&format!(
                "SELECT {field} FROM packs WHERE repo_id={}::uuid AND native_hash={}",
                git_rs::metadata::text(&fixture.repo),
                git_rs::metadata::text(&fixture.selected)
            ))
            .trim()
            .to_owned();
        let corrupt = fixture.root.join("corrupt-source");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(&corrupt, bytes).unwrap();
        fixture.put(&key, &corrupt);
        let result = Operation::new(10).enter(|| {
            fixture
                .engine
                .blame(&fixture.slug, "bob", &fixture.request())
        });
        let error = match result {
            Err(error) => error,
            _ => panic!("unverified S3 {extension} produced a serving outcome"),
        };
        assert!(
            error.to_string().contains("integrity"),
            "wrong failure: {error}"
        );
        until(|| fixture.engine.blame.admitted() == 0);
        assert_eq!(fixture.leases(), 0);
        assert!(fixture.source_files().is_empty());
        fixture.put(&key, &path);
        let body = fixture.complete();
        let mut actual = Vec::new();
        Operation::new(10)
            .enter(|| body.write_to(&mut actual))
            .unwrap();
        assert_eq!(actual, fixture.expected);
        drop(body);
        assert_eq!(fixture.leases(), 0);
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 fixture"]
fn owned_source_unavailable_optional_cache_falls_back_to_real_local_derivation() {
    use git_rs::derived_cache::{CacheConfig, DerivedCache};
    // This owned database intentionally has no cache migration/namespace. Source
    // authorization and S3 are real and available; only optional cache calls fail.
    let config = CacheConfig::new(
        "11111111-1111-1111-1111-111111111111".into(),
        "22222222-2222-2222-2222-222222222222".into(),
    );
    let cache = DerivedCache::open(config).unwrap();
    let fixture =
        source_support::Fixture::new(git_rs::blame::Blame::default().with_cache(cache), 1000);
    let body = fixture.complete();
    let mut actual = Vec::new();
    Operation::new(10)
        .enter(|| body.write_to(&mut actual))
        .unwrap();
    assert_eq!(actual, fixture.expected);
    assert_eq!(fixture.leases(), 1);
    drop(body);
    assert_eq!(fixture.leases(), 0);
}
