use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Request-local stop state. Clones control the same request, never global gix state.
#[derive(Clone, Debug)]
pub struct Control {
    flag: Arc<AtomicBool>,
    deadline: Instant,
}

/// Opt-in clock amortization for an invocation's **bounded CPU work only**.
///
/// Every checkpoint observes explicit cancellation. The first checkpoint reads
/// the absolute deadline, then at most 32 further checkpoints may elapse before
/// the next deadline read, even without an executor/deadline watcher. This is a
/// work-point bound, not a wall-clock/scheduler/opaque-call latency guarantee.
///
/// The caller must establish a finite maximum work quantum between checkpoints
/// and call `boundary()` before/after I/O, allocations, external callbacks, phase
/// transitions and completion/publication. Never replace those strict checks or
/// `Control::check()` with a sampled checkpoint. Do not reuse across invocations.
/// Borrowing and the lack of Clone keep the counter local; no timer/TLS is used.
pub struct CpuPoller<'a> {
    control: &'a Control,
    remaining: u8,
}

impl CpuPoller<'_> {
    /// Observe cancellation now; sample time initially and every <=32 points.
    #[inline]
    pub fn checkpoint(&mut self) -> Result<(), StopReason> {
        if self.control.flag.load(Ordering::Acquire) || self.remaining == 0 {
            self.boundary()
        } else {
            self.remaining -= 1;
            Ok(())
        }
    }

    /// Strict absolute check, including on a partially consumed CPU quantum.
    #[inline]
    pub fn boundary(&mut self) -> Result<(), StopReason> {
        self.control.check()?;
        self.remaining = 31;
        Ok(())
    }
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

    /// Start a non-cloneable, borrowed bounded-CPU invocation. This does not
    /// change the strict semantics of `check()` or rely on a watcher thread.
    pub fn cpu_poller(&self) -> CpuPoller<'_> {
        CpuPoller {
            control: self,
            remaining: 0,
        }
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
