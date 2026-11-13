//! Bounded single-flight ownership around the common CPU executor. The job, not
//! its first waiter, owns its independent finite deadline and admission.

use crate::{Result, cpu_jobs::CpuJobs, invalid, process};
use git_rs_core::{CapacityBudget, Control, JobOptions, SubmitError};
use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub use crate::cpu_jobs::{Completed, JOB_BYTES};
type Error = Arc<dyn std::error::Error + Send + Sync>;
type JobResult<T> = Result<Option<Arc<Completed<T>>>>;

// Captures must be destroyed before actual admission on rejection/discard.
struct Background<F> {
    prepare: F,
    operation: Arc<process::Operation>,
    permit: process::Permit,
}
impl<F> Background<F>
where
    F: FnOnce(&Control, &CapacityBudget) -> Result<()>,
{
    fn run(self, control: &Control) -> Result<()> {
        let _permit = self.permit;
        let operation = self.operation;
        let prepare = self.prepare;
        let budget = CapacityBudget::new(JOB_BYTES);
        operation.enter(|| {
            control.check()?;
            let result = prepare(control, &budget);
            control.check()?;
            operation.check()?;
            result
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Queued,
    Running,
    Abandoned,
    Complete,
}

struct FlightState<T> {
    phase: Phase,
    waiters: usize,
    result: Option<std::result::Result<Arc<Completed<T>>, Error>>,
}

struct Flight<T> {
    key: Arc<[u8]>,
    state: Mutex<FlightState<T>>,
    changed: Condvar,
    control: Control,
    permit: Arc<process::Permit>,
}

type Flights<T> = HashMap<Arc<[u8]>, Weak<Flight<T>>>;

struct Completion<T>(Arc<Flight<T>>);

// Keep queued captures and their actual admission in one owner. Rust drops fields
// in declaration order; a rejected/discarded prepare must finish destruction
// before Completion can release the last flight/permit. Consuming this owner via
// a method also prevents disjoint closure capture from undoing that ordering.
struct Prepared<F, T> {
    prepare: F,
    done: Completion<T>,
}
impl<F, T> Prepared<F, T> {
    fn start(self) -> (F, Completion<T>) {
        (self.prepare, self.done)
    }
}

impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        // Also runs when the executor discards a cancelled queued closure, when
        // submission fails, and during unwinding. Never leave abandoned waiters.
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.result = Some(Err(Arc::from(invalid(
                "blame job stopped before completion",
            ))));
        }
        // Abandonment is terminal even after the queued closure is discarded.
        if state.phase != Phase::Abandoned {
            state.phase = Phase::Complete;
        }
        self.0.changed.notify_all();
    }
}
struct Waiting<T> {
    flight: Option<Arc<Flight<T>>>,
    attached: Arc<AtomicUsize>,
}
impl<T> Waiting<T> {
    fn detach(&mut self) -> Option<Arc<Flight<T>>> {
        let flight = self.flight.take()?;
        let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
        state.waiters -= 1;
        self.attached.fetch_sub(1, Ordering::AcqRel);
        if state.waiters == 0 && state.phase == Phase::Queued {
            // Join, abandonment, cancellation and start linearize on this lock.
            // No delayed cancellation may poison a newly joined/running flight.
            state.phase = Phase::Abandoned;
            flight.control.cancel();
            drop(state);
            // Keep the actual permit in Flight/Completion until queued discard;
            // removing discoverability does not acknowledge executor cleanup.
            return Some(flight);
        }
        None
    }
}
impl<T> Drop for Waiting<T> {
    fn drop(&mut self) {
        // Unwinding must detach too, but never call instrumentation from Drop.
        self.detach();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobEvent {
    /// The worker has atomically started; waiter departure cannot cancel it.
    Started,
    /// The last queued waiter detached and cancellation is already committed.
    QueuedAbandoned,
}

/// Optional in-process lifecycle observation. Called outside all scheduler locks
/// and never from Drop. Implementations must finish by `deadline`; abandonment
/// notifications describe an already-cancelled flight, not a work callback.
pub trait JobObserver: Send + Sync {
    fn observe(&self, key: &[u8], event: JobEvent, deadline: Instant);
}

/// Public in-process contract used by serving and deterministic integration
/// tests. `prepare` runs only after dequeue. Joining a flight never invokes it.
/// Dropping/cancelling a running waiter does not cancel the independently owned
/// derivation. Retain Completed through consumption to retain actual admission.
pub struct Jobs<T> {
    cpu: Arc<CpuJobs>,
    flights: Mutex<Flights<T>>,
    waiter_admission: Arc<process::Admission>,
    attached: Arc<AtomicUsize>,
    observer: Option<Arc<dyn JobObserver>>,
}

impl<T: Send + Sync + 'static> Jobs<T> {
    /// Shared CPU work/result ownership and local attached request waiters. These
    /// counters expose admission, not an implementation-specific scheduling gate.
    pub fn admitted(&self) -> usize {
        self.cpu.admitted()
    }
    pub fn waiters(&self) -> usize {
        self.attached.load(Ordering::Acquire)
    }
    pub fn with_observer(mut self, observer: Arc<dyn JobObserver>) -> Self {
        self.observer = Some(observer);
        self
    }
    pub fn new(workers: usize, queued: usize) -> Result<Self> {
        if !(1..=4).contains(&workers) || queued > 3 || workers + queued > 4 {
            return Err(invalid(
                "blame capacity requires concurrency in 1..=4, waiters in 0..=3 and total <=4",
            ));
        }
        Ok(Self::with_cpu(Arc::new(CpuJobs::new(workers, queued)?)))
    }

    /// Inject the application's existing runtime; never create a second pool.
    /// Flights and waiter admission remain local, actual work/result admission
    /// is shared with receive and independent warming.
    pub fn with_cpu(cpu: Arc<CpuJobs>) -> Self {
        Self {
            cpu,
            flights: Mutex::new(HashMap::new()),
            waiter_admission: process::Admission::new(16),
            attached: Arc::new(AtomicUsize::new(0)),
            observer: None,
        }
    }

    /// Cheap handle for constructor owners wiring another user of this pool.
    pub fn cpu(&self) -> Arc<CpuJobs> {
        self.cpu.clone()
    }

    /// Independent no-waiter work on the same pool and actual work admission.
    /// Queue time consumes the original deadline; source acquisition is deferred.
    pub fn try_background(
        &self,
        seconds: u64,
        prepare: impl FnOnce(&Control, &CapacityBudget) -> Result<()> + Send + 'static,
    ) -> Result<bool> {
        if !(1..=3600).contains(&seconds) {
            return Err(invalid("invalid background deadline"));
        }
        let Some(permit) = self.cpu.admission.try_admit() else {
            return Ok(false);
        };
        let operation = process::Operation::new(seconds);
        let control = Control::new(operation.deadline());
        let background = Background {
            prepare,
            operation,
            permit,
        };
        match self
            .cpu
            .executor
            .submit(control, JobOptions::default(), move |control, _| {
                let _ = background.run(control);
                Ok(())
            }) {
            Ok(ticket) => {
                drop(ticket);
                Ok(true)
            }
            Err(SubmitError::Full) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub fn run(
        &self,
        key: Vec<u8>,
        seconds: u64,
        prepare: impl FnOnce(&Control, &CapacityBudget) -> Result<T> + Send + 'static,
    ) -> JobResult<T> {
        if key.is_empty() || key.len() > 8192 || !(1..=3600).contains(&seconds) {
            return Err(invalid("invalid bounded blame job"));
        }
        let waiter = process::Operation::current()
            .ok_or_else(|| invalid("blame requires a bounded operation"))?;
        waiter.check()?;
        let Some(_waiting_permit) = self.waiter_admission.try_admit() else {
            return Ok(None);
        };
        let key: Arc<[u8]> = key.into();
        let (flight, created, mut waiting) = {
            let mut flights = self
                .flights
                .lock()
                .map_err(|_| invalid("blame flights poisoned"))?;
            flights.retain(|_, flight| flight.strong_count() != 0);
            let existing = flights.get(&key).and_then(Weak::upgrade).filter(|flight| {
                let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.phase == Phase::Abandoned || flight.control.check().is_err() {
                    return false;
                }
                state.waiters += 1;
                self.attached.fetch_add(1, Ordering::AcqRel);
                true
            });
            let (flight, created) = if let Some(flight) = existing {
                (flight, false)
            } else {
                let Some(permit) = self.cpu.admission.try_admit() else {
                    return Ok(None);
                };
                let operation = process::Operation::new(seconds);
                let flight = Arc::new(Flight {
                    key: key.clone(),
                    state: Mutex::new(FlightState {
                        phase: Phase::Queued,
                        waiters: 1,
                        result: None,
                    }),
                    changed: Condvar::new(),
                    control: Control::new(operation.deadline()),
                    permit: Arc::new(permit),
                });
                self.attached.fetch_add(1, Ordering::AcqRel);
                flights.insert(key, Arc::downgrade(&flight));
                (flight, true)
            };
            let waiting = Waiting {
                flight: Some(flight.clone()),
                attached: self.attached.clone(),
            };
            (flight, created, waiting)
        };
        if created {
            let prepared = Prepared {
                prepare,
                done: Completion(flight.clone()),
            };
            let running = flight.clone();
            let operation = process::Operation::until(flight.control.deadline());
            let observer = self.observer.clone();
            let submitted = self.cpu.executor.submit(
                flight.control.clone(),
                JobOptions::default(),
                move |control, _| {
                    {
                        let mut state = running.state.lock().unwrap_or_else(|e| e.into_inner());
                        if state.phase != Phase::Queued
                            || state.waiters == 0
                            || control.check().is_err()
                        {
                            return Ok(());
                        }
                        state.phase = Phase::Running;
                    }
                    if let Some(observer) = observer {
                        observer.observe(&running.key, JobEvent::Started, control.deadline());
                    }
                    let (prepare, _done) = prepared.start();
                    let budget = CapacityBudget::new(JOB_BYTES);
                    let result: std::result::Result<Arc<Completed<T>>, Error> = operation
                        .enter(|| -> Result<T> {
                            control.check()?;
                            let value = prepare(control, &budget)?;
                            control.check()?;
                            operation.check()?;
                            Ok(value)
                        })
                        .map(|value| {
                            Arc::new(Completed {
                                value,
                                _permit: running.permit.clone(),
                            })
                        })
                        .map_err(Arc::from);
                    let mut state = running.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.result = Some(result);
                    state.phase = Phase::Complete;
                    running.changed.notify_all();
                    Ok(())
                },
            );
            match submitted {
                Ok(ticket) => drop(ticket), // Work owns all resources, not this receiver.
                Err(SubmitError::Full) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        let result: JobResult<T> = (|| {
            let mut state = flight
                .state
                .lock()
                .map_err(|_| invalid("blame result poisoned"))?;
            loop {
                waiter.check()?;
                if let Some(result) = state.result.as_ref() {
                    return result
                        .clone()
                        .map(Some)
                        .map_err(|error| Box::new(SharedError(error)) as _);
                }
                state = flight
                    .changed
                    .wait_timeout(
                        state,
                        waiter
                            .deadline()
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(10)),
                    )
                    .map_err(|_| invalid("blame result poisoned"))?
                    .0;
            }
        })();
        if let Some(abandoned) = waiting.detach()
            && let Some(observer) = &self.observer
        {
            observer.observe(
                &abandoned.key,
                JobEvent::QueuedAbandoned,
                abandoned.control.deadline(),
            );
        }
        result
    }
}

#[derive(Debug)]
struct SharedError(Error);
impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
