use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::{Control, StopReason};

thread_local! {
    static IN_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// All jobs are single-threaded internally. Adapters must not create nested pools.
#[derive(Clone, Copy, Debug)]
pub struct ExecutorConfig {
    pub workers: usize,
    pub queue_capacity: usize,
}

/// Immutable per-job limits, not process environment flags. Accelerated paths are
/// deliberately not exposed until their adapters and qualification exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobOptions {
    pub max_object_bytes: usize,
    pub max_output_bytes: usize,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            max_object_bytes: 128 * 1024 * 1024,
            max_output_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitError {
    Full,
    Stopped,
    Nested,
    InvalidOptions,
    Control(StopReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobError {
    Stopped(StopReason),
    Panicked,
    Failed(String),
}

impl From<StopReason> for JobError {
    fn from(reason: StopReason) -> Self {
        Self::Stopped(reason)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitError {
    Timeout,
    Disconnected,
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CPU job admission: {self:?}")
    }
}
impl std::error::Error for SubmitError {}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped(reason) => reason.fmt(f),
            Self::Panicked => f.write_str("CPU job panicked"),
            Self::Failed(message) => write!(f, "CPU job failed: {message}"),
        }
    }
}
impl std::error::Error for JobError {}

impl std::fmt::Display for WaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CPU completion wait: {self:?}")
    }
}
impl std::error::Error for WaitError {}

type Work = Box<dyn FnOnce() + Send + 'static>;
struct Job {
    id: u64,
    work: Work,
}
struct State {
    queue: VecDeque<Job>,
    controls: Vec<(u64, Control)>,
    next_id: u64,
    stopped: bool,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    queue_capacity: usize,
}

/// Fixed CPU workers plus one non-CPU deadline watcher, with fail-fast admission.
///
/// Cancellation is cooperative: arbitrary callbacks/destructors can block forever.
/// Drop cancels and joins, rather than pretending an abort is a stop acknowledgment.
/// The application MUST install an appropriate bounded panic hook before starting
/// workers: Rust's default hook may block writing stderr *before* catch_unwind.
/// Hooks and allocation failure with panic=abort are outside this pool's control.
/// Unwinding also requires cooperative panic payloads and destructors; a second
/// panic during unwinding can abort the process. This library installs no hook.
/// Keep the owning executor outside its own jobs: its joining shutdown must not
/// run in a callback whose completion another callback is waiting for.
pub struct CpuExecutor {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

impl CpuExecutor {
    pub fn new(config: ExecutorConfig) -> std::io::Result<Self> {
        if config.workers == 0 || config.queue_capacity == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "pool sizes must be nonzero",
            ));
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                controls: Vec::new(),
                next_id: 0,
                stopped: false,
            }),
            wake: Condvar::new(),
            queue_capacity: config.queue_capacity,
        });
        // Construct first so partial thread-start failure cancels and joins.
        let mut pool = Self {
            shared,
            threads: Vec::new(),
        };
        for index in 0..config.workers {
            let shared = pool.shared.clone();
            pool.threads.push(
                thread::Builder::new()
                    .name(format!("git-core-{index}"))
                    .spawn(move || worker(shared))?,
            );
        }
        let shared = pool.shared.clone();
        pool.threads.push(
            thread::Builder::new()
                .name("git-core-deadlines".into())
                .spawn(move || watch_deadlines(shared))?,
        );
        Ok(pool)
    }

    /// Captured source holds, scratch and capacity owners move into `work`.
    /// They must not be tied to the waiting request's lifetime. Admission failure
    /// drops the rejected closure without executing it. No blocking nested admission.
    pub fn submit<T, F>(
        &self,
        control: Control,
        options: JobOptions,
        work: F,
    ) -> Result<Ticket<T>, SubmitError>
    where
        T: Send + 'static,
        F: FnOnce(&Control, &JobOptions) -> Result<T, JobError> + Send + 'static,
    {
        if IN_WORKER.get() {
            return Err(SubmitError::Nested);
        }
        if options.max_object_bytes == 0 || options.max_output_bytes == 0 {
            return Err(SubmitError::InvalidOptions);
        }
        control.check().map_err(SubmitError::Control)?;
        let (send, receive) = mpsc::sync_channel(1);
        let mut state = self.shared.state.lock().unwrap();
        if state.stopped {
            return Err(SubmitError::Stopped);
        }
        if state.queue.len() >= self.shared.queue_capacity {
            return Err(SubmitError::Full);
        }
        let id = state.next_id;
        state.next_id = state.next_id.checked_add(1).ok_or(SubmitError::Stopped)?;
        state.controls.push((id, control.clone()));
        let job_control = control.clone();
        state.queue.push_back(Job {
            id,
            work: Box::new(move || {
                // The callable is consumed/dropped inside the unwind boundary even
                // when cancelled before starting; completion is sent only afterwards.
                let result = catch_unwind(AssertUnwindSafe(move || {
                    job_control.check()?;
                    let result = work(&job_control, &options);
                    job_control.check()?;
                    result
                }))
                .unwrap_or(Err(JobError::Panicked));
                // A dropped waiter cannot keep a worker blocked on output.
                let _ = send.send(result);
            }),
        });
        self.shared.wake.notify_all();
        Ok(Ticket { control, receive })
    }
}

impl Drop for CpuExecutor {
    fn drop(&mut self) {
        {
            let mut state = self.shared.state.lock().unwrap();
            state.stopped = true;
            for (_, control) in &state.controls {
                control.cancel();
            }
            self.shared.wake.notify_all();
        }
        for handle in self.threads.drain(..) {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

fn worker(shared: Arc<Shared>) {
    IN_WORKER.set(true);
    loop {
        let job = {
            let mut state = shared.state.lock().unwrap();
            loop {
                if let Some(job) = state.queue.pop_front() {
                    break job;
                }
                if state.stopped {
                    return;
                }
                state = shared.wake.wait(state).unwrap();
            }
        };
        // Also isolate result destruction if the receiver disappeared. A user
        // output's Drop can panic after the inner computation unwind boundary.
        let _ = catch_unwind(AssertUnwindSafe(job.work));
        let mut state = shared.state.lock().unwrap();
        state.controls.retain(|(id, _)| *id != job.id);
    }
}

fn watch_deadlines(shared: Arc<Shared>) {
    let mut state = shared.state.lock().unwrap();
    loop {
        if state.stopped {
            return;
        }
        let now = Instant::now();
        let mut delay = Duration::from_secs(60);
        for (_, control) in &state.controls {
            if control.deadline() <= now {
                control.cancel();
            } else {
                delay = delay.min(control.deadline() - now);
            }
        }
        state = shared.wake.wait_timeout(state, delay).unwrap().0;
    }
}

/// A completion acknowledgment, not ownership of running resources. Dropping a
/// ticket neither cancels nor releases work. Use `cancel` explicitly, then wait
/// for completion when stop acknowledgment is required. A wait timeout is NOT stop.
pub struct Ticket<T> {
    control: Control,
    receive: mpsc::Receiver<Result<T, JobError>>,
}

impl<T> Ticket<T> {
    pub fn cancel(&self) {
        self.control.cancel();
    }
    pub fn control(&self) -> &Control {
        &self.control
    }
    pub fn wait(self) -> Result<T, JobError> {
        self.receive.recv().unwrap_or(Err(JobError::Panicked))
    }
    /// A timeout leaves the ticket usable. Success consumes its single completion;
    /// subsequent timed waits return Disconnected, not a second copy of output.
    pub fn wait_timeout(&self, timeout: Duration) -> Result<Result<T, JobError>, WaitError> {
        self.receive
            .recv_timeout(timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => WaitError::Timeout,
                mpsc::RecvTimeoutError::Disconnected => WaitError::Disconnected,
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityError {
    Exhausted,
    Allocation,
    Stopped(StopReason),
}

impl From<StopReason> for CapacityError {
    fn from(reason: StopReason) -> Self {
        Self::Stopped(reason)
    }
}

impl std::fmt::Display for CapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "capacity admission: {self:?}")
    }
}
impl std::error::Error for CapacityError {}

/// Live backing-allocation capacity, NOT process RSS, allocator metadata, stack,
/// library allocations or logical LRU weight. All participating allocations must
/// use this budget; unwrapped external allocations need separate admission.
#[derive(Clone, Debug)]
pub struct CapacityBudget {
    inner: Arc<Mutex<CapacityState>>,
}
#[derive(Debug)]
struct CapacityState {
    limit: usize,
    used: usize,
}

impl CapacityBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CapacityState { limit, used: 0 })),
        }
    }
    pub fn used(&self) -> usize {
        self.inner.lock().unwrap().used
    }
    fn reserve(&self, bytes: usize) -> Result<(), CapacityError> {
        let mut state = self.inner.lock().unwrap();
        if bytes > state.limit - state.used {
            return Err(CapacityError::Exhausted);
        }
        state.used += bytes;
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.inner.lock().unwrap().used -= bytes;
    }

    /// Admit external backing storage without allocating that storage. Reserve a
    /// conservative upper bound before allocation, then reconcile to the actual
    /// reported capacity. The owner must free storage before dropping its charge;
    /// logical lengths and cache weights are not valid reconciliation inputs.
    pub fn reservation(&self, bytes: usize) -> Result<CapacityReservation, CapacityError> {
        self.reserve(bytes)?;
        Ok(CapacityReservation {
            budget: self.clone(),
            charged: bytes,
        })
    }

    /// Reserve before allocation, reconcile any allocator-reported excess before
    /// exposing the buffer. No growth API; shrinking length does not free budget.
    pub fn buffer(&self, capacity: usize) -> Result<CapacityBuffer, CapacityError> {
        let mut owner = CapacityBuffer {
            data: Vec::new(),
            reservation: self.reservation(capacity)?,
        };
        owner
            .data
            .try_reserve_exact(capacity)
            .map_err(|_| CapacityError::Allocation)?;
        let actual = owner.data.capacity();
        owner.reservation.resize(actual)?;
        Ok(owner)
    }
}

/// Move-only RAII admission for an externally owned allocation. Share the storage
/// and its reservation in the same `Arc` owner, not as independently cloned
/// charges. Shrink only after backing capacity really decreased. Failed growth
/// leaves the old charge intact; it does not authorize retaining excess storage.
#[derive(Debug)]
pub struct CapacityReservation {
    budget: CapacityBudget,
    charged: usize,
}

impl CapacityReservation {
    pub fn resize(&mut self, bytes: usize) -> Result<(), CapacityError> {
        if bytes > self.charged {
            self.budget.reserve(bytes - self.charged)?;
        } else {
            self.budget.release(self.charged - bytes);
        }
        self.charged = bytes;
        Ok(())
    }
}

impl Drop for CapacityReservation {
    fn drop(&mut self) {
        self.budget.release(self.charged);
    }
}

/// Share with `Arc<CapacityBuffer>` to pin the full backing allocation even for
/// tiny borrowed slices. Returning a small logical length never discounts capacity.
#[derive(Debug)]
pub struct CapacityBuffer {
    data: Vec<u8>,
    reservation: CapacityReservation,
}

impl CapacityBuffer {
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    /// Unpolled initialization for already bounded small chunks. Large fills
    /// must use `resize_controlled`; neither method grows backing capacity.
    pub fn resize(&mut self, len: usize, value: u8) -> Result<(), CapacityError> {
        if len > self.data.capacity() {
            return Err(CapacityError::Exhausted);
        }
        self.data.resize(len, value);
        Ok(())
    }

    /// Initialize at most 32 KiB between request-local checks, including a final
    /// check. On interruption, retain the original visible length and capacity
    /// charge. This bounds fill chunks, not allocator or scheduler latency.
    pub fn resize_controlled(
        &mut self,
        len: usize,
        value: u8,
        control: &Control,
    ) -> Result<(), CapacityError> {
        if len > self.data.capacity() {
            return Err(CapacityError::Exhausted);
        }
        let original_len = self.data.len();
        loop {
            if let Err(reason) = control.check() {
                self.data.truncate(original_len);
                return Err(reason.into());
            }
            // The same inner poll protects the first chunk, every subsequent
            // chunk, and publication of the final visible length (or shrink).
            if self.data.len() >= len {
                self.data.truncate(len);
                return Ok(());
            }
            self.data
                .resize(len.min(self.data.len().saturating_add(32 * 1024)), value);
        }
    }
}

impl Drop for CapacityBuffer {
    fn drop(&mut self) {
        // Free backing memory before returning capacity to other jobs.
        drop(std::mem::take(&mut self.data));
        // The reservation field drops after this destructor returns.
    }
}
