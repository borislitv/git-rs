use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Request-local stop state. Clones control the same request, never global gix state.
#[derive(Clone, Debug)]
pub struct Control {
    flag: Arc<AtomicBool>,
    deadline: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    Cancelled,
    Deadline,
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Cancelled => "request cancelled",
                Self::Deadline => "request deadline exceeded",
            }
        )
    }
}

impl std::error::Error for StopReason {}

impl Control {
    pub fn new(deadline: Instant) -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            deadline,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }

    pub fn check(&self) -> Result<(), StopReason> {
        if Instant::now() >= self.deadline {
            self.cancel();
            Err(StopReason::Deadline)
        } else if self.flag.load(Ordering::Acquire) {
            Err(StopReason::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Bridge for library entrypoints accepting `&AtomicBool`. Treat this as
    /// read-only (never clear it). While admitted, the pool's deadline watcher
    /// sets it even when inner library code does not call `check()`.
    pub fn cancellation_flag(&self) -> &AtomicBool {
        &self.flag
    }
}
