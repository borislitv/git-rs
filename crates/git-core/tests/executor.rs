use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use git_rs_core::{
    CapacityBudget, CapacityError, CapacityReservation, Control, CpuExecutor, ExecutorConfig,
    JobError, JobOptions, StopReason, SubmitError, WaitError,
};

const WAIT: Duration = Duration::from_secs(3);

fn pool(workers: usize, queue_capacity: usize) -> CpuExecutor {
    CpuExecutor::new(ExecutorConfig {
        workers,
        queue_capacity,
    })
    .unwrap()
}

fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(10))
}

struct Hold(mpsc::Sender<()>);
impl Drop for Hold {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn ticket_drop_does_not_drop_running_sources_or_capacity() {
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(1024);
    let buffer = budget.buffer(1024).unwrap();
    let (dropped, dropped_rx) = mpsc::channel();
    let hold = Hold(dropped);
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let ticket = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            let (_hold, _buffer) = (hold, buffer);
            started.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
            Ok(())
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    drop(ticket);
    assert_eq!(budget.used(), 1024);
    assert!(matches!(
        dropped_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    dropped_rx.recv_timeout(WAIT).unwrap();
    // Joining establishes complete cleanup independent of destructor order.
    drop(executor);
    assert_eq!(budget.used(), 0);
}

#[test]
fn cancelled_running_job_keeps_ownership_until_callback_stops() {
    let executor = pool(1, 1);
    let (dropped, dropped_rx) = mpsc::channel();
    let hold = Hold(dropped);
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let ticket = executor
        .submit(control(), JobOptions::default(), move |control, _| {
            let _hold = hold;
            started.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
            control.check()?;
            Ok(())
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    ticket.cancel();
    assert_eq!(ticket.wait_timeout(Duration::ZERO), Err(WaitError::Timeout));
    assert!(matches!(
        dropped_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
    dropped_rx.recv_timeout(WAIT).unwrap();
}

#[test]
fn bounded_queue_cancels_without_executing_queued_work() {
    let executor = pool(1, 1);
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let first = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            started.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
            Ok(())
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let ran_job = ran.clone();
    let (dropped, dropped_rx) = mpsc::channel();
    let hold = Hold(dropped);
    let queued = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            let _hold = hold;
            ran_job.store(true, Ordering::Release);
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        executor.submit(control(), JobOptions::default(), |_, _| Ok(())),
        Err(SubmitError::Full)
    ));
    queued.cancel();
    assert!(matches!(
        dropped_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    assert_eq!(first.wait(), Ok(()));
    assert_eq!(queued.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
    dropped_rx.recv_timeout(WAIT).unwrap();
    assert!(!ran.load(Ordering::Acquire));
}

#[test]
fn watcher_sets_inner_atomic_flag_without_outer_poll() {
    let executor = pool(1, 1);
    let (started, started_rx) = mpsc::channel();
    let (observed, observed_rx) = mpsc::channel();
    let ticket = executor
        .submit(
            Control::new(Instant::now() + Duration::from_millis(500)),
            JobOptions::default(),
            move |control, _| {
                started.send(()).unwrap();
                let safety = Instant::now() + Duration::from_secs(2);
                // Represents an upstream loop polling only its AtomicBool argument.
                while !control.cancellation_flag().load(Ordering::Acquire) {
                    if Instant::now() >= safety {
                        observed.send(false).unwrap();
                        return Err(JobError::Failed("watcher did not interrupt".into()));
                    }
                    std::thread::yield_now();
                }
                observed.send(true).unwrap();
                Ok(42)
            },
        )
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    // Final outer check alone would also report Deadline after the safety cap;
    // assert the inner loop actually saw the flag, independently of that result.
    assert!(observed_rx.recv_timeout(WAIT).unwrap());
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Deadline)));
}

#[test]
fn panic_cleans_up_and_worker_accepts_independent_job() {
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(64);
    let buffer = budget.buffer(64).unwrap();
    let (dropped, dropped_rx) = mpsc::channel();
    let hold = Hold(dropped);
    let ticket = executor
        .submit::<(), _>(control(), JobOptions::default(), move |_, _| {
            let (_buffer, _hold) = (buffer, hold);
            panic!("synthetic job panic");
        })
        .unwrap();
    assert_eq!(ticket.wait(), Err(JobError::Panicked));
    dropped_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(budget.used(), 0);
    let healthy = executor
        .submit(control(), JobOptions::default(), |_, _| Ok(7))
        .unwrap();
    assert_eq!(healthy.wait(), Ok(7));
}

#[test]
fn nested_submission_is_rejected_instead_of_deadlocking_or_spawning() {
    let executor = Arc::new(pool(1, 1));
    let nested = executor.clone();
    let ticket = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            assert!(matches!(
                nested.submit(control(), JobOptions::default(), |_, _| Ok(())),
                Err(SubmitError::Nested)
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(ticket.wait(), Ok(()));
}

#[test]
fn options_and_cancellation_are_isolated_between_workers() {
    let executor = pool(2, 2);
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let first_options = JobOptions {
        max_object_bytes: 12,
        max_output_bytes: 34,
    };
    let first = executor
        .submit(control(), first_options, move |control, options| {
            assert_eq!(*options, first_options);
            started.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
            control.check()?;
            Ok(())
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    first.cancel();
    let second_options = JobOptions {
        max_object_bytes: 56,
        max_output_bytes: 78,
    };
    let second = executor
        .submit(control(), second_options, |control, options| {
            control.check()?;
            Ok(*options)
        })
        .unwrap();
    assert_eq!(second.wait(), Ok(second_options));
    release.send(()).unwrap();
    assert_eq!(first.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
}

#[test]
fn live_capacity_not_logical_length_or_shared_slice_weight() {
    let budget = CapacityBudget::new(1024);
    let mut large = budget.buffer(1024).unwrap();
    large.resize(1024, 3).unwrap();
    large.resize(1, 0).unwrap();
    assert_eq!(large.as_slice(), &[3]);
    assert_eq!(budget.used(), large.capacity());
    assert!(matches!(budget.buffer(1), Err(CapacityError::Exhausted)));
    let pinned = Arc::new(large);
    let survivor = pinned.clone();
    drop(pinned);
    assert_eq!(survivor.as_slice(), &[3]);
    assert!(matches!(budget.buffer(1024), Err(CapacityError::Exhausted)));
    drop(survivor);
    assert_eq!(budget.used(), 0);
    let mut again = budget.buffer(1024).unwrap();
    again.resize(1025, 0).unwrap_err();
    assert_eq!(again.capacity(), 1024);
    drop(again);
    assert_eq!(budget.used(), 0);
}

#[test]
fn result_keeps_capacity_reserved_until_consumer_releases_it() {
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(512);
    let buffer = budget.buffer(512).unwrap();
    let ticket = executor
        .submit(control(), JobOptions::default(), move |_, _| Ok(buffer))
        .unwrap();
    let output = ticket.wait().unwrap();
    assert_eq!(budget.used(), output.capacity());
    assert!(matches!(budget.buffer(1), Err(CapacityError::Exhausted)));
    drop(output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn invalid_admission_never_executes_callback() {
    let executor = pool(1, 1);
    let expired = Control::new(Instant::now());
    assert!(matches!(
        executor.submit(expired, JobOptions::default(), |_, _| {
            panic!("expired work must not execute");
            #[allow(unreachable_code)]
            Ok(())
        }),
        Err(SubmitError::Control(StopReason::Deadline))
    ));
    let options = JobOptions {
        max_object_bytes: 0,
        max_output_bytes: 1,
    };
    assert!(matches!(
        executor.submit(control(), options, |_, _| Ok(())),
        Err(SubmitError::InvalidOptions)
    ));
    assert!(
        CpuExecutor::new(ExecutorConfig {
            workers: 0,
            queue_capacity: 1
        })
        .is_err()
    );
}

#[test]
fn fixed_workers_do_not_start_a_third_job_until_a_worker_finishes() {
    let executor = pool(2, 1);
    let (started, started_rx) = mpsc::channel();
    let mut releases = Vec::new();
    let mut tickets = Vec::new();
    for _ in 0..2 {
        let started = started.clone();
        let (release, release_rx) = mpsc::channel();
        releases.push(release);
        tickets.push(
            executor
                .submit(control(), JobOptions::default(), move |_, _| {
                    started.send(()).unwrap();
                    release_rx.recv_timeout(WAIT).unwrap();
                    Ok(())
                })
                .unwrap(),
        );
        // Wait before the next admission so queue capacity is deterministic.
        started_rx.recv_timeout(WAIT).unwrap();
    }
    let (third_started, third_started_rx) = mpsc::channel();
    let third = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            third_started.send(()).unwrap();
            Ok(3)
        })
        .unwrap();
    assert_eq!(third.wait_timeout(Duration::ZERO), Err(WaitError::Timeout));
    assert!(matches!(
        third_started_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    releases[0].send(()).unwrap();
    third_started_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(third.wait(), Ok(3));
    releases[1].send(()).unwrap();
    for ticket in tickets {
        assert_eq!(ticket.wait(), Ok(()));
    }
}

#[test]
fn failure_drops_captured_resources_before_completion() {
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(128);
    let buffer = budget.buffer(128).unwrap();
    let ticket = executor
        .submit::<(), _>(control(), JobOptions::default(), move |_, _| {
            let _buffer = buffer;
            Err(JobError::Failed("synthetic dependency failure".into()))
        })
        .unwrap();
    assert_eq!(
        ticket.wait(),
        Err(JobError::Failed("synthetic dependency failure".into()))
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn reservation_growth_shrink_and_failed_growth_are_atomic() {
    let budget = CapacityBudget::new(16);
    let mut first = budget.reservation(8).unwrap();
    let second = budget.reservation(4).unwrap();
    assert_eq!(budget.used(), 12);
    first.resize(12).unwrap();
    assert_eq!(budget.used(), 16);
    assert_eq!(first.resize(13), Err(CapacityError::Exhausted));
    assert_eq!(budget.used(), 16);
    assert_eq!(first.resize(usize::MAX), Err(CapacityError::Exhausted));
    assert_eq!(budget.used(), 16);
    first.resize(5).unwrap();
    assert_eq!(budget.used(), 9);
    drop(second);
    assert_eq!(budget.used(), 5);
    first.resize(16).unwrap();
    assert_eq!(budget.used(), 16);
    drop(first);
    assert_eq!(budget.used(), 0);
}

#[test]
fn reservations_allocate_no_backing_storage_and_handle_zero_and_maximum() {
    let budget = CapacityBudget::new(usize::MAX);
    // A real backing allocation of this size is impossible. Admission itself
    // must only account for the requested capacity, without allocating bytes.
    let mut reservation = budget.reservation(usize::MAX).unwrap();
    assert_eq!(budget.used(), usize::MAX);
    assert!(matches!(
        budget.reservation(1),
        Err(CapacityError::Exhausted)
    ));
    reservation.resize(usize::MAX).unwrap();
    reservation.resize(0).unwrap();
    assert_eq!(budget.used(), 0);
    drop(reservation);
    assert_eq!(budget.used(), 0);
    let empty = CapacityBudget::new(0);
    let mut zero = empty.reservation(0).unwrap();
    assert_eq!(zero.resize(1), Err(CapacityError::Exhausted));
    assert_eq!(empty.used(), 0);
    assert!(matches!(
        empty.reservation(1),
        Err(CapacityError::Exhausted)
    ));
}

#[test]
fn impossible_buffer_allocation_rolls_back_its_preallocation_charge() {
    let budget = CapacityBudget::new(usize::MAX);
    // Vec rejects a byte capacity above isize::MAX before calling the allocator.
    assert!(matches!(
        budget.buffer(usize::MAX),
        Err(CapacityError::Allocation)
    ));
    assert_eq!(budget.used(), 0);
    let reservation = budget.reservation(16).unwrap();
    assert_eq!(budget.used(), 16);
    drop(reservation);
    assert_eq!(budget.used(), 0);
}

struct PinnedBacking {
    // Rust drops fields in declaration order: storage before its admission.
    bytes: Vec<u8>,
    _reservation: CapacityReservation,
}

#[test]
fn shared_external_backing_and_output_retain_one_actual_capacity_charge() {
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(16);
    let mut reservation = budget.reservation(16).unwrap();
    let bytes = vec![11u8; 8];
    reservation.resize(bytes.capacity()).unwrap();
    let backing = Arc::new(PinnedBacking {
        bytes,
        _reservation: reservation,
    });
    let capacity = backing.bytes.capacity();
    assert_eq!(budget.used(), capacity);
    let captured = backing.clone();
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let ticket = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            started.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
            Ok(captured)
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    drop(backing);
    assert_eq!(budget.used(), capacity);
    release.send(()).unwrap();
    let output = ticket.wait().unwrap();
    let survivor = output.clone();
    drop(output);
    assert_eq!(&survivor.bytes[..1], &[11]);
    assert_eq!(budget.used(), capacity);
    drop(survivor);
    assert_eq!(budget.used(), 0);
}

#[test]
fn controlled_fill_preserves_prefix_and_capacity_across_large_small_large() {
    let budget = CapacityBudget::new(65_539);
    let mut buffer = budget.buffer(65_539).unwrap();
    buffer.resize(3, 7).unwrap();
    buffer.resize_controlled(65_539, 5, &control()).unwrap();
    assert_eq!(&buffer.as_slice()[..3], &[7, 7, 7]);
    assert!(buffer.as_slice()[3..].iter().all(|byte| *byte == 5));
    buffer.resize_controlled(1, 0, &control()).unwrap();
    assert_eq!(buffer.as_slice(), &[7]);
    assert_eq!(budget.used(), 65_539);
    buffer.resize_controlled(65_539, 9, &control()).unwrap();
    assert_eq!(buffer.as_slice()[0], 7);
    assert!(buffer.as_slice()[1..].iter().all(|byte| *byte == 9));
    assert_eq!(budget.used(), 65_539);
    assert_eq!(
        buffer.resize_controlled(65_540, 0, &control()),
        Err(CapacityError::Exhausted)
    );
    assert_eq!(buffer.as_slice().len(), 65_539);
    drop(buffer);
    assert_eq!(budget.used(), 0);
}

#[test]
fn stopped_controlled_fill_does_not_publish_new_length_or_release_capacity() {
    let budget = CapacityBudget::new(65_539);
    let mut buffer = budget.buffer(65_539).unwrap();
    buffer.resize(3, 7).unwrap();
    let cancelled = control();
    cancelled.cancel();
    assert_eq!(
        buffer.resize_controlled(65_539, 5, &cancelled),
        Err(CapacityError::Stopped(StopReason::Cancelled))
    );
    assert_eq!(buffer.as_slice(), &[7, 7, 7]);
    let expired = Control::new(Instant::now());
    assert_eq!(
        buffer.resize_controlled(0, 0, &expired),
        Err(CapacityError::Stopped(StopReason::Deadline))
    );
    assert_eq!(buffer.as_slice(), &[7, 7, 7]);
    assert_eq!(budget.used(), 65_539);
}

#[test]
fn completion_waits_for_source_cleanup_not_just_callback_return() {
    struct Cleanup {
        started: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        _reservation: CapacityReservation,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.started.send(()).unwrap();
            self.release.recv_timeout(WAIT).unwrap();
        }
    }
    let executor = pool(1, 1);
    let budget = CapacityBudget::new(128);
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let cleanup = Cleanup {
        started,
        release: release_rx,
        _reservation: budget.reservation(128).unwrap(),
    };
    let ticket = executor
        .submit(control(), JobOptions::default(), move |_, _| {
            let _cleanup = cleanup;
            Ok(4)
        })
        .unwrap();
    started_rx.recv_timeout(WAIT).unwrap();
    ticket.cancel();
    assert_eq!(ticket.wait_timeout(Duration::ZERO), Err(WaitError::Timeout));
    assert_eq!(budget.used(), 128);
    release.send(()).unwrap();
    assert_eq!(ticket.wait(), Err(JobError::Stopped(StopReason::Cancelled)));
    assert_eq!(budget.used(), 0);
}
