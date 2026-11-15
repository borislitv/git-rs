#![forbid(unsafe_code)]
//! Background ownership on the shared foreground executor.
use git_rs::{
    Result,
    blame::{Blame, Jobs, ResourceProfile},
    cpu_jobs::{CpuJob, CpuJobs},
    process::Operation,
};
use git_rs_core::{CapacityBudget, CapacityBuffer, CapacityReservation, Control};
use std::{
    sync::{Arc, Condvar, Mutex, mpsc},
    time::{Duration, Instant},
};

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
fn until(mut test: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(3);
    while !test() {
        assert!(Instant::now() < end, "job ownership did not converge");
        std::thread::yield_now();
    }
}

#[test]
fn independent_background_uses_same_default_work_slot_without_waiter() {
    let jobs = Jobs::<usize>::new(1, 0).unwrap();
    let gate = Arc::new(Gate::default());
    let running = gate.clone();
    let (entered, started) = mpsc::channel();
    assert!(
        jobs.try_background(5, move |control, _| {
            entered.send(()).unwrap();
            running.wait(control)
        })
        .unwrap()
    );
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(jobs.admitted(), 1);
    assert_eq!(jobs.waiters(), 0);
    assert!(
        !jobs
            .try_background(5, |_, _| panic!("full background executed"))
            .unwrap()
    );
    assert!(
        Operation::new(2)
            .enter(|| jobs.run(b"query".to_vec(), 2, |_, _| Ok(1)))
            .unwrap()
            .is_none()
    );
    gate.open();
    until(|| jobs.admitted() == 0);
    let result = Operation::new(2)
        .enter(|| jobs.run(b"query".to_vec(), 2, |_, _| Ok(42)))
        .unwrap()
        .unwrap();
    assert_eq!(**result, 42);
}

#[test]
fn selected_blame_budget_is_exact_and_generic_preparation_keeps_one_gib() {
    for (profile, bytes) in [
        (ResourceProfile::BaselineV1, 1usize << 30),
        (ResourceProfile::LargeV1, 4usize << 30),
    ] {
        let jobs =
            Jobs::<(CapacityBudget, CapacityReservation)>::new_with_resource_profile(1, 0, profile)
                .unwrap();
        let output = Operation::new(5)
            .enter(|| {
                jobs.run(b"budget".to_vec(), 5, move |_, budget| {
                    let reservation = budget.reservation(bytes)?;
                    assert!(budget.reservation(1).is_err());
                    Ok((budget.clone(), reservation))
                })
            })
            .unwrap()
            .unwrap();
        let budget = output.0.clone();
        assert_eq!(budget.used(), bytes);
        let cpu = jobs.cpu();
        assert!(cpu.try_admit(Operation::new(5)).unwrap().is_none());
        drop(output);
        until(|| cpu.admitted() == 0);
        assert_eq!(budget.used(), 0);
        let admitted = cpu.try_admit(Operation::new(5)).unwrap().unwrap();
        let reservation = admitted.budget().reservation(1usize << 30).unwrap();
        assert!(admitted.budget().reservation(1).is_err());
        drop(reservation);
        drop(admitted);
        assert_eq!(cpu.admitted(), 0);
    }
}

#[test]
fn large_background_owns_four_gib_policy_and_the_same_actual_slot() {
    let jobs = Jobs::<usize>::new_with_resource_profile(1, 0, ResourceProfile::LargeV1).unwrap();
    let gate = Arc::new(Gate::default());
    let running = gate.clone();
    let (entered, started) = mpsc::channel();
    assert!(
        jobs.try_background(5, move |control, budget| {
            let _reservation = budget.reservation(4usize << 30)?;
            assert!(budget.reservation(1).is_err());
            entered.send(budget.clone()).unwrap();
            running.wait(control)
        })
        .unwrap()
    );
    let budget = started.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(budget.used(), 4usize << 30);
    assert!(jobs.cpu().try_admit(Operation::new(5)).unwrap().is_none());
    assert!(
        Operation::new(5)
            .enter(|| jobs.run(b"blocked".to_vec(), 5, |_, _| panic!("shared slot bypass")))
            .unwrap()
            .is_none()
    );
    gate.open();
    until(|| jobs.admitted() == 0);
    assert_eq!(budget.used(), 0);
}

#[derive(Debug)]
struct OwnedFailure {
    _bytes: CapacityBuffer,
    _capacity: CapacityReservation,
}
impl std::fmt::Display for OwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owned blame provider failure")
    }
}
impl std::error::Error for OwnedFailure {}

#[test]
fn returned_blame_failure_retains_backing_and_actual_admission_until_error_drop() {
    let jobs = Jobs::<usize>::new_with_resource_profile(1, 0, ResourceProfile::LargeV1).unwrap();
    let (sent, received) = mpsc::channel();
    let error = match Operation::new(5).enter(|| {
        jobs.run(b"failure".to_vec(), 5, move |_, budget| {
            let bytes = budget.buffer(4096)?;
            let capacity = budget.reservation((4usize << 30) - bytes.capacity())?;
            sent.send(budget.clone()).unwrap();
            Err(Box::new(OwnedFailure {
                _bytes: bytes,
                _capacity: capacity,
            }))
        })
    }) {
        Err(error) => error,
        Ok(_) => panic!("provider error accepted"),
    };
    let budget = received.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(error.to_string(), "owned blame provider failure");
    let mut cause: &(dyn std::error::Error + 'static) = error.as_ref();
    while !cause.is::<OwnedFailure>() {
        cause = cause.source().expect("typed provider cause lost");
    }
    assert_eq!(budget.used(), 4usize << 30);
    assert_eq!(jobs.admitted(), 1);
    assert!(jobs.cpu().try_admit(Operation::new(5)).unwrap().is_none());
    drop(error);
    until(|| jobs.admitted() == 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn resource_selector_and_capacity_reconfiguration_preserve_explicit_policy() {
    use std::os::unix::ffi::OsStringExt;
    assert_eq!(
        ResourceProfile::from_os_value(None).unwrap(),
        ResourceProfile::BaselineV1
    );
    let invalid = std::ffi::OsString::from_vec(vec![0xff]);
    assert!(ResourceProfile::from_os_value(Some(&invalid)).is_err());
    assert_eq!(
        ResourceProfile::parse("baseline-v1").unwrap(),
        ResourceProfile::BaselineV1
    );
    assert_eq!(
        ResourceProfile::parse("large-v1").unwrap(),
        ResourceProfile::LargeV1
    );
    for invalid in ["", "large", "large-v1.invalid", "baseline-v2"] {
        assert!(ResourceProfile::parse(invalid).is_err());
    }
    for (workers, queued) in [(0, 0), (2, 0), (1, 1), (usize::MAX, 0)] {
        assert!(
            Jobs::<usize>::new_with_resource_profile(workers, queued, ResourceProfile::LargeV1)
                .is_err()
        );
    }
    let service = Blame::new_with_resource_profile(1, 0, ResourceProfile::LargeV1)
        .unwrap()
        .with_capacity(1, 0)
        .unwrap();
    assert_eq!(service.resource_profile(), ResourceProfile::LargeV1);
    assert!(service.with_capacity(2, 0).is_err());
    assert_eq!(
        Blame::new(1, 0).unwrap().resource_profile(),
        ResourceProfile::BaselineV1
    );
}

#[test]
fn resource_profiles_preserve_object_body_metadata_depth_and_graph_contracts() {
    let options = git_rs_core::blame::BlameOptions::default();
    let baseline = ResourceProfile::BaselineV1.read_limits(&options);
    for (profile, decode, cache, entry, retained) in [
        (
            ResourceProfile::BaselineV1,
            256usize << 20,
            32usize << 20,
            1usize << 20,
            0,
        ),
        (
            ResourceProfile::LargeV1,
            8usize << 30,
            512usize << 20,
            128usize << 20,
            64usize << 20,
        ),
    ] {
        let limits = profile.read_limits(&options);
        assert_eq!(limits.max_decode_bytes, decode);
        assert_eq!(limits.max_object_bytes, 128usize << 20);
        assert_eq!(git_rs::blame::MAX_OUTPUT, 256u64 << 20);
        assert_eq!(limits.max_metadata_bytes, 64 * 1024);
        assert_eq!(limits.max_delta_depth, 64);
        assert_eq!(limits.max_graph_objects, baseline.max_graph_objects);
        assert_eq!(limits.max_graph_edges, baseline.max_graph_edges);
        let decoder = profile.decoder_options();
        assert_eq!(decoder.cache_bytes, cache);
        assert_eq!(decoder.max_cache_entry_bytes, entry);
        assert_eq!(decoder.cache_slots, 4096);
        assert_eq!(decoder.header_slots, 4096);
        assert_eq!(decoder.retained_workspace_bytes, retained);
    }
}

struct Cleanup {
    started: mpsc::Sender<()>,
    gate: Arc<Gate>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.started.send(()).unwrap();
        self.gate
            .wait(&Control::new(Instant::now() + Duration::from_secs(5)))
            .unwrap();
    }
}

struct BackgroundFailure {
    _cleanup: Cleanup,
    _bytes: CapacityBuffer,
}
impl std::fmt::Debug for BackgroundFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("background failure with retained backing")
    }
}
impl std::fmt::Display for BackgroundFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}
impl std::error::Error for BackgroundFailure {}

#[test]
fn background_failure_keeps_actual_admission_through_backing_cleanup() {
    let jobs = Jobs::<usize>::new_with_resource_profile(1, 0, ResourceProfile::LargeV1).unwrap();
    let gate = Arc::new(Gate::default());
    let (started, cleanup_started) = mpsc::channel();
    let cleanup = Cleanup {
        started,
        gate: gate.clone(),
    };
    let (sent, received) = mpsc::channel();
    assert!(
        jobs.try_background(5, move |_, budget| {
            let bytes = budget.buffer(4096)?;
            sent.send(budget.clone()).unwrap();
            Err(Box::new(BackgroundFailure {
                _cleanup: cleanup,
                _bytes: bytes,
            }))
        })
        .unwrap()
    );
    cleanup_started
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    let budget = received.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(budget.used() >= 4096);
    assert_eq!(jobs.admitted(), 1);
    assert!(jobs.cpu().try_admit(Operation::new(5)).unwrap().is_none());
    gate.open();
    until(|| jobs.admitted() == 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn queued_background_deadline_discards_capture_before_refunding_actual_slot() {
    let jobs = Jobs::<usize>::new(1, 1).unwrap();
    let worker = Arc::new(Gate::default());
    let worker_run = worker.clone();
    let (entered, started) = mpsc::channel();
    jobs.try_background(6, move |control, _| {
        entered.send(()).unwrap();
        worker_run.wait(control)
    })
    .unwrap();
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    let cleanup_gate = Arc::new(Gate::default());
    let (cleanup_entered, cleanup_started) = mpsc::channel();
    let cleanup = Cleanup {
        started: cleanup_entered,
        gate: cleanup_gate.clone(),
    };
    assert!(
        jobs.try_background(1, move |_, _| {
            let _cleanup = cleanup;
            panic!("expired queued background executed")
        })
        .unwrap()
    );
    cleanup_started
        .recv_timeout(Duration::from_secs(3))
        .expect("queued capture must be discarded without freeing worker");
    assert_eq!(
        jobs.admitted(),
        2,
        "destruction still owns actual admission"
    );
    assert_eq!(jobs.waiters(), 0);
    assert!(!jobs.try_background(2, |_, _| Ok(())).unwrap());
    cleanup_gate.open();
    until(|| jobs.admitted() == 1);
    let (replacement, replacement_started) = mpsc::channel();
    assert!(
        jobs.try_background(3, move |_, _| {
            replacement.send(()).unwrap();
            Ok(())
        })
        .unwrap()
    );
    worker.open();
    replacement_started
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    until(|| jobs.admitted() == 0);
}

struct ChargedCapture {
    bytes: Option<git_rs_core::CapacityBuffer>,
    freed: Arc<std::sync::atomic::AtomicBool>,
}
impl Drop for ChargedCapture {
    fn drop(&mut self) {
        drop(self.bytes.take());
        self.freed.store(true, std::sync::atomic::Ordering::Release);
    }
}
#[test]
fn background_panic_frees_charged_capture_and_shared_pool_remains_usable() {
    let jobs = Jobs::<usize>::new(1, 0).unwrap();
    let budget = git_rs_core::CapacityBudget::new(8192);
    let freed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let capture = ChargedCapture {
        bytes: Some(budget.buffer(8192).unwrap()),
        freed: freed.clone(),
    };
    assert!(
        jobs.try_background(3, move |_, _| {
            let _capture = capture;
            panic!("fixture background callback panic")
        })
        .unwrap()
    );
    until(|| jobs.admitted() == 0);
    assert!(freed.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(budget.used(), 0);
    let (sent, received) = mpsc::channel();
    assert!(
        jobs.try_background(3, move |_, _| {
            sent.send(()).unwrap();
            Ok(())
        })
        .unwrap()
    );
    received.recv_timeout(Duration::from_secs(2)).unwrap();
    until(|| jobs.admitted() == 0);
}

// Ownership deliberately moves INTO the panic payload, rather than remaining a
// callback local that ordinary unwinding would already destroy before refund.
struct OwningPanic {
    bytes: Option<CapacityBuffer>,
    source: Option<Arc<()>>,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl Drop for OwningPanic {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        // Finite and non-panicking even if a test assertion fails and disconnects
        // the sender. No double-panic or infinite-destructor contract is tested.
        let _ = self.release.recv_timeout(Duration::from_secs(5));
        drop(self.bytes.take());
        drop(self.source.take());
    }
}

struct PanicJob(OwningPanic);
impl CpuJob for PanicJob {
    type Output = usize;
    fn run(self, _: &Control, _: &CapacityBudget) -> Result<usize> {
        std::panic::panic_any(self.0)
    }
}

struct Answer;
impl CpuJob for Answer {
    type Output = usize;
    fn run(self, control: &Control, _: &CapacityBudget) -> Result<usize> {
        control.check()?;
        Ok(42)
    }
}

#[test]
fn generic_owning_panic_payload_keeps_admission_until_cleanup_before_failure_ack() {
    let cpu = CpuJobs::new(1, 0).unwrap();
    let admitted = cpu.try_admit(Operation::new(10)).unwrap().unwrap();
    let budget = admitted.budget().clone();
    let source = Arc::new(());
    let witness = Arc::downgrade(&source);
    let (entered, cleanup_started) = mpsc::channel();
    let (release, cleanup_release) = mpsc::channel();
    let job = PanicJob(OwningPanic {
        bytes: Some(budget.buffer(4096).unwrap()),
        source: Some(source),
        entered,
        release: cleanup_release,
    });
    let (returned, finished) = mpsc::channel();
    std::thread::scope(|scope| {
        let waiter = scope.spawn(move || {
            let error = match admitted.run(job) {
                Err(error) => error,
                Ok(_) => panic!("panicked CPU job returned success"),
            };
            returned.send(error.to_string()).unwrap();
        });
        cleanup_started
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert_eq!(budget.used(), 4096);
        assert!(witness.upgrade().is_some());
        assert_eq!(
            cpu.admitted(),
            1,
            "owning panic payload is still being destroyed"
        );
        assert!(cpu.try_admit(Operation::new(2)).unwrap().is_none());
        assert_eq!(finished.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.send(()).unwrap();
        assert_eq!(
            finished.recv_timeout(Duration::from_secs(2)).unwrap(),
            "CPU job panicked"
        );
        waiter.join().unwrap();
    });
    assert_eq!(budget.used(), 0);
    assert!(witness.upgrade().is_none());
    assert_eq!(cpu.admitted(), 0);
    let result = cpu.try_run(Operation::new(5), Answer).unwrap().unwrap();
    assert_eq!(*result, 42);
    drop(result);
    assert_eq!(cpu.admitted(), 0);
}

fn foreground_owning_panic_case(cancel_waiters: bool) {
    let jobs = Jobs::<usize>::new(1, 0).unwrap();
    let first_operation = Operation::new(10);
    let second_operation = Operation::new(10);
    let start_panic = Arc::new(Gate::default());
    let run_panic = start_panic.clone();
    let source = Arc::new(());
    let witness = Arc::downgrade(&source);
    let (started, running) = mpsc::channel();
    let (entered, cleanup_started) = mpsc::channel();
    let (release, cleanup_release) = mpsc::channel();
    let (returned, finished) = mpsc::channel();
    let (sent_budget, received_budget) = mpsc::channel();
    std::thread::scope(|scope| {
        let first_jobs = &jobs;
        let operation = first_operation.clone();
        let first_returned = returned.clone();
        let first = scope.spawn(move || {
            let error = match operation.enter(|| {
                first_jobs.run(b"owning-panic".to_vec(), 10, move |control, budget| {
                    let payload = OwningPanic {
                        bytes: Some(budget.buffer(4096)?),
                        source: Some(source),
                        entered,
                        release: cleanup_release,
                    };
                    sent_budget.send(budget.clone()).unwrap();
                    started.send(()).unwrap();
                    run_panic.wait(control)?;
                    std::panic::panic_any(payload)
                })
            }) {
                Err(error) => error,
                Ok(_) => panic!("panicked flight returned success"),
            };
            first_returned.send(error).unwrap();
        });
        running.recv_timeout(Duration::from_secs(2)).unwrap();
        let second_jobs = &jobs;
        let operation = second_operation.clone();
        let second = scope.spawn(move || {
            let error = match operation.enter(|| {
                second_jobs.run(b"owning-panic".to_vec(), 10, |_, _| {
                    panic!("single-flight join ran a duplicate callback")
                })
            }) {
                Err(error) => error,
                Ok(_) => panic!("panicked flight returned success"),
            };
            returned.send(error).unwrap();
        });
        let mut waiters = vec![first, second];
        until(|| jobs.waiters() == 2);
        start_panic.open();
        cleanup_started
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let budget = received_budget
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let mut errors = Vec::new();
        if cancel_waiters {
            // Each waiter may independently cancel. Join both so none can mask
            // an early refund by retaining its flight Arc while waking.
            first_operation.cancel();
            second_operation.cancel();
            for _ in 0..2 {
                errors.push(finished.recv_timeout(Duration::from_secs(2)).unwrap());
            }
            for waiter in waiters.drain(..) {
                waiter.join().unwrap();
            }
            assert_eq!(jobs.waiters(), 0);
        } else {
            assert!(matches!(
                finished.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
        }
        assert_eq!(budget.used(), 4096);
        assert!(witness.upgrade().is_some());
        assert_eq!(
            jobs.admitted(),
            1,
            "payload cleanup, not waiters, owns the slot"
        );
        assert!(jobs.cpu().try_admit(Operation::new(2)).unwrap().is_none());
        release.send(()).unwrap();
        if !cancel_waiters {
            for _ in 0..2 {
                let error = finished.recv_timeout(Duration::from_secs(2)).unwrap();
                assert_eq!(error.to_string(), "CPU job panicked");
                assert!(
                    witness.upgrade().is_none(),
                    "failure acknowledged live source cleanup"
                );
                assert_eq!(budget.used(), 0);
                errors.push(error);
            }
            for waiter in waiters.drain(..) {
                waiter.join().unwrap();
            }
            assert_eq!(jobs.waiters(), 0);
            assert_eq!(
                jobs.admitted(),
                1,
                "shared returned errors retain actual admission"
            );
            drop(errors.pop());
            assert_eq!(
                jobs.admitted(),
                1,
                "the other shared error still owns admission"
            );
        }
        drop(errors);
        until(|| jobs.admitted() == 0);
        assert_eq!(budget.used(), 0);
        assert!(witness.upgrade().is_none());
    });
    let result = Operation::new(5)
        .enter(|| jobs.run(b"owning-panic".to_vec(), 5, |_, _| Ok(42)))
        .unwrap()
        .unwrap();
    assert_eq!(**result, 42);
    assert_eq!(
        jobs.admitted(),
        1,
        "delivered success still owns actual admission"
    );
    drop(result);
    until(|| jobs.admitted() == 0);
}

#[test]
fn foreground_owning_panic_keeps_shared_flight_admitted_after_all_waiters_cancel() {
    foreground_owning_panic_case(true);
}

#[test]
fn foreground_owning_panic_shared_failure_is_delivered_only_after_payload_cleanup() {
    foreground_owning_panic_case(false);
}

#[test]
fn background_owning_panic_payload_keeps_admission_through_storage_destruction() {
    let jobs = Jobs::<usize>::new(1, 0).unwrap();
    let source = Arc::new(());
    let witness = Arc::downgrade(&source);
    let (entered, cleanup_started) = mpsc::channel();
    let (release, cleanup_release) = mpsc::channel();
    let (sent_budget, received_budget) = mpsc::channel();
    assert!(
        jobs.try_background(10, move |_, budget| {
            let payload = OwningPanic {
                bytes: Some(budget.buffer(4096)?),
                source: Some(source),
                entered,
                release: cleanup_release,
            };
            sent_budget.send(budget.clone()).unwrap();
            std::panic::panic_any(payload)
        })
        .unwrap()
    );
    cleanup_started
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    let budget = received_budget
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(budget.used(), 4096);
    assert!(witness.upgrade().is_some());
    assert_eq!(
        jobs.admitted(),
        1,
        "background payload cleanup owns the actual slot"
    );
    assert!(jobs.cpu().try_admit(Operation::new(2)).unwrap().is_none());
    release.send(()).unwrap();
    until(|| jobs.admitted() == 0);
    assert_eq!(budget.used(), 0);
    assert!(witness.upgrade().is_none());
    let result = Operation::new(5)
        .enter(|| jobs.run(b"after-background-panic".to_vec(), 5, |_, _| Ok(42)))
        .unwrap()
        .unwrap();
    assert_eq!(**result, 42);
    drop(result);
    until(|| jobs.admitted() == 0);
}
