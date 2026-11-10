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
