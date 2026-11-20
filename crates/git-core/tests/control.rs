use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use git_rs_core::{Control, StopReason};

#[test]
fn cancellation_is_shared_but_request_local() {
    let deadline = Instant::now() + Duration::from_secs(10);
    let first = Control::new(deadline);
    let clone = first.clone();
    let independent = Control::new(deadline);
    assert_eq!(first.check(), Ok(()));
    clone.cancel();
    assert_eq!(first.check(), Err(StopReason::Cancelled));
    assert!(first.cancellation_flag().load(Ordering::Acquire));
    assert_eq!(independent.check(), Ok(()));
    assert_eq!(first.deadline(), deadline);
    assert!(first.remaining() <= Duration::from_secs(10));
}

#[test]
fn expired_deadline_sets_the_library_flag() {
    let control = Control::new(Instant::now());
    assert_eq!(control.remaining(), Duration::ZERO);
    assert_eq!(control.check(), Err(StopReason::Deadline));
    assert!(control.cancellation_flag().load(Ordering::Acquire));
}

// Wait on the monotonic condition itself, not an assumed sleep duration. These
// controls are standalone: no executor/watcher is allowed to set their flags.
fn await_deadline(deadline: Instant) {
    while Instant::now() < deadline {
        std::thread::yield_now();
    }
}

#[test]
fn cpu_poller_checks_expired_initial_deadline_without_a_watcher() {
    let control = Control::new(Instant::now());
    assert!(!control.cancellation_flag().load(Ordering::Acquire));
    assert_eq!(control.cpu_poller().checkpoint(), Err(StopReason::Deadline));
    assert!(control.cancellation_flag().load(Ordering::Acquire));
}

#[test]
fn cpu_poller_finds_unflagged_deadline_within_32_bounded_work_points() {
    let deadline = Instant::now() + Duration::from_millis(100);
    let control = Control::new(deadline);
    let mut poller = control.cpu_poller();
    assert_eq!(poller.checkpoint(), Ok(()));
    await_deadline(deadline);
    assert!(!control.cancellation_flag().load(Ordering::Acquire));
    let observed = (1..=32).find_map(|point| poller.checkpoint().err().map(|stop| (point, stop)));
    assert!(matches!(observed, Some((1..=32, StopReason::Deadline))));
    assert!(control.cancellation_flag().load(Ordering::Acquire));
}

#[test]
fn cpu_poller_explicit_cancellation_is_immediate_even_between_clock_samples() {
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let mut poller = control.cpu_poller();
    assert_eq!(poller.checkpoint(), Ok(()));
    control.cancel();
    assert_eq!(poller.checkpoint(), Err(StopReason::Cancelled));
}

#[test]
fn cpu_poller_boundaries_and_control_checks_remain_strict_between_samples() {
    let deadline = Instant::now() + Duration::from_millis(100);
    let control = Control::new(deadline);
    let mut poller = control.cpu_poller();
    assert_eq!(poller.checkpoint(), Ok(()));
    await_deadline(deadline);
    assert!(!control.cancellation_flag().load(Ordering::Acquire));
    assert_eq!(poller.boundary(), Err(StopReason::Deadline));

    let deadline = Instant::now() + Duration::from_millis(100);
    let control = Control::new(deadline);
    let mut poller = control.cpu_poller();
    assert_eq!(poller.checkpoint(), Ok(()));
    await_deadline(deadline);
    assert!(!control.cancellation_flag().load(Ordering::Acquire));
    assert_eq!(control.check(), Err(StopReason::Deadline));
}

#[test]
fn interleaved_cpu_pollers_are_independent_and_new_invocations_check_time_first() {
    let deadline = Instant::now() + Duration::from_millis(100);
    let first = Control::new(deadline);
    let second = Control::new(Instant::now() + Duration::from_secs(10));
    let mut a = first.cpu_poller();
    let mut b = second.cpu_poller();
    assert_eq!(a.checkpoint(), Ok(()));
    assert_eq!(b.checkpoint(), Ok(()));
    await_deadline(deadline);
    assert!(!first.cancellation_flag().load(Ordering::Acquire));
    let mut stopped = false;
    for _ in 0..32 {
        assert_eq!(b.checkpoint(), Ok(()));
        if a.checkpoint() == Err(StopReason::Deadline) {
            stopped = true;
            break;
        }
    }
    assert!(stopped);
    assert!(!second.cancellation_flag().load(Ordering::Acquire));
    second.cancel();
    assert_eq!(b.checkpoint(), Err(StopReason::Cancelled));

    let third = Control::new(Instant::now() + Duration::from_millis(100));
    assert_eq!(third.cpu_poller().checkpoint(), Ok(()));
    await_deadline(third.deadline());
    assert!(!third.cancellation_flag().load(Ordering::Acquire));
    assert_eq!(third.cpu_poller().checkpoint(), Err(StopReason::Deadline));
}
