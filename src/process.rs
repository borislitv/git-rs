//! Process-local admission, deadlines and cancellation. No subprocess execution.

use crate::{Result, invalid};
use std::cell::RefCell;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

pub const DEADLINE: u64 = 120;
pub const MAX_OPERATION_SECONDS: u64 = 3600;

pub struct Admission {
    active: AtomicUsize,
    limit: usize,
}

pub struct Permit(Arc<Admission>);

impl Admission {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            limit,
        })
    }

    pub fn try_admit(self: &Arc<Self>) -> Option<Permit> {
        self.active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limit).then_some(active + 1)
            })
            .ok()
            .map(|_| Permit(self.clone()))
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct Operation {
    deadline: Instant,
    cancelled: AtomicBool,
    timing: (u64, Instant),
}
thread_local! { static OPERATION: RefCell<Option<Arc<Operation>>> = const { RefCell::new(None) }; }

impl Operation {
    /// Preserve an existing absolute deadline when bridging independently owned
    /// CPU work and source I/O. This never resets a queue or response budget.
    pub fn until(deadline: Instant) -> Arc<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let now = Instant::now();
        let timing = Self::current().map_or_else(
            || (NEXT_ID.fetch_add(1, Ordering::Relaxed), now),
            |parent| parent.timing,
        );
        Arc::new(Self {
            deadline,
            cancelled: AtomicBool::new(false),
            timing,
        })
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            Err(invalid("operation cancelled or deadline exceeded"))
        } else {
            Ok(())
        }
    }

    pub fn new(seconds: u64) -> Arc<Self> {
        // Independent cleanup budgets retain their parent's diagnostic context,
        // without inheriting its deadline or cancellation state.
        Self::until(Instant::now() + Duration::from_secs(seconds))
    }
    pub(crate) fn timing(&self) -> (u64, Instant) {
        self.timing
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    pub fn current() -> Option<Arc<Self>> {
        OPERATION.with(|slot| slot.borrow().clone())
    }

    /// Interrupt blocking transport I/O at the absolute operation deadline.
    /// The caller owns admission before entering; at most one watcher is needed
    /// per admitted connection. Completion wakes and joins it, including on panic.
    pub fn with_interrupt<T>(
        self: &Arc<Self>,
        interrupt: impl FnOnce() + Send,
        run: impl FnOnce() -> T,
    ) -> T {
        struct Completion<'a>(&'a (Mutex<bool>, Condvar));
        impl Drop for Completion<'_> {
            fn drop(&mut self) {
                let (finished, changed) = self.0;
                *finished.lock().expect("operation completion poisoned") = true;
                changed.notify_one();
            }
        }
        let completion = (Mutex::new(false), Condvar::new());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let (finished, changed) = &completion;
                let finished = finished.lock().expect("operation completion poisoned");
                let (finished, _) = changed
                    .wait_timeout_while(
                        finished,
                        self.deadline.saturating_duration_since(Instant::now()),
                        |finished| !*finished,
                    )
                    .expect("operation completion poisoned");
                if !*finished {
                    self.cancel();
                    interrupt();
                }
            });
            let _completion = Completion(&completion);
            self.enter(run)
        })
    }
    pub fn enter<T>(self: &Arc<Self>, run: impl FnOnce() -> T) -> T {
        struct Restore(Option<Arc<Operation>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                OPERATION.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _restore = Restore(OPERATION.with(|slot| slot.replace(Some(self.clone()))));
        run()
    }
}

pub fn check_operation() -> Result<()> {
    OPERATION.with(|slot| {
        if slot.borrow().as_ref().is_some_and(|operation| {
            operation.cancelled.load(Ordering::Acquire) || Instant::now() >= operation.deadline
        }) {
            Err(invalid("operation cancelled or deadline exceeded"))
        } else {
            Ok(())
        }
    })
}

pub(crate) fn remaining() -> Result<Duration> {
    check_operation()?;
    OPERATION.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|op| op.deadline.saturating_duration_since(Instant::now()))
            .ok_or_else(|| invalid("repository lease requires a bounded operation"))
    })
}
