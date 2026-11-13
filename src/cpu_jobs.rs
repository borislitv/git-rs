//! One bounded application CPU runtime and admission shared by work types.
//!
//! Keep this owner outside its own jobs. Clone the `Arc<CpuJobs>`, not a pool;
//! callbacks must not own the last runtime/Engine handle whose Drop joins them.

use crate::{
    Result, invalid, process,
    receive_io::{IoControl, ReceiveInput},
};
use git_rs_core::{
    CapacityBudget, Control, CpuExecutor, ExecutorConfig, JobOptions, SubmitError, WaitError,
};
use std::{ops::Deref, sync::Arc, time::Duration};

/// Per-job live capacity shared by provider, decoder, vendor and serializer.
/// Source mappings retain their separate catalog/file bounds. Admission counts
/// completed outputs too; a configured 2+1 admits at most three such owners.
pub const JOB_BYTES: usize = 1024 * 1024 * 1024;

/// Retain through output consumption/publication, not only computation. Drop
/// backing output/source/scratch before refunding its actual admission.
pub struct Completed<T> {
    pub(crate) value: T,
    pub(crate) _permit: Arc<process::Permit>,
}

impl<T> Deref for Completed<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

// A provider error can itself own backing storage or source pins. Preserve its
// cause, but do not refund admission while that returned owner is still live.
struct Failed {
    error: Box<dyn std::error::Error + Send + Sync>,
    _permit: Arc<process::Permit>,
}
impl std::fmt::Debug for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CpuJobFailure").field(&self.error).finish()
    }
}
impl std::fmt::Display for Failed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}
impl std::error::Error for Failed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

/// Trusted, finite CPU work with owned captures, not a transport callback.
///
/// Implementations must poll `control` in bounded quanta and cannot block on
/// socket/SQL/source acquisition or create nested workers. Acquire/materialize
/// bounded input outside the callback, then move source pins, scratch and input
/// into this owner. Return any resources needed by publication in `Output`.
/// Captured storage must already be admitted/bounded at its input boundary; the
/// supplied CPU-phase budget does not retrospectively charge opaque captures.
/// Destructors/cleanup must also be bounded: cancellation cannot preempt opaque
/// I/O or arbitrary Drop. Interactive jobs use the explicit `CpuIoJob` seam,
/// never lifetime erasure or an unbounded blocking Read in this callback.
pub trait CpuJob: Send + 'static {
    type Output: Send + 'static;

    fn run(self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output>;
}

/// Trusted interactive work on the SAME pool. The only transport read is the
/// supplied owned, poll-bounded ReceiveInput. Do not perform SQL/source/network
/// acquisition, use arbitrary stdio, or start nested workers here. Stop after
/// the core command/single-pack boundary, not after client/socket EOF. Neither
/// this trait nor a successful read grants EOF/publication authority.
pub trait CpuIoJob: Send + 'static {
    type Output: Send + 'static;

    fn run(
        self,
        input: &mut ReceiveInput,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<Self::Output>;
}

struct WithInput<J> {
    input: ReceiveInput,
    job: J,
}
impl<J: CpuIoJob> CpuJob for WithInput<J> {
    type Output = J::Output;
    fn run(mut self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output> {
        self.job.run(&mut self.input, control, budget)
    }
}

// Field order also covers submission rejection and cancelled queue discard:
// owned captures must be destroyed before their admission is returned.
struct Work<J> {
    job: J,
    operation: Arc<process::Operation>,
    budget: CapacityBudget,
    permit: Arc<process::Permit>,
}

impl<J: CpuJob> Work<J> {
    fn run(self, control: &Control) -> Result<Completed<J::Output>> {
        // Reverse local-drop order preserves the same capture-before-permit rule
        // on a check failure or unwind after the executor invokes this owner.
        let permit = self.permit;
        let operation = self.operation;
        let job = self.job;
        let budget = self.budget;
        operation
            .enter(|| {
                control.check()?;
                operation.check()?;
                let value = job.run(control, &budget)?;
                control.check()?;
                operation.check()?;
                Ok(Completed {
                    value,
                    _permit: permit.clone(),
                })
            })
            .map_err(|error| {
                Box::new(Failed {
                    error,
                    _permit: permit.clone(),
                }) as _
            })
    }
}

pub struct CpuJobs {
    // Only the existing Blame flight owner uses the raw seam: it already keeps
    // its independent operation, admission and result ownership through cleanup.
    pub(crate) executor: CpuExecutor,
    pub(crate) admission: Arc<process::Admission>,
}

impl CpuJobs {
    pub fn new(workers: usize, queued: usize) -> Result<Self> {
        if !(1..=4).contains(&workers) || queued > 3 || workers + queued > 4 {
            return Err(invalid(
                "CPU capacity requires concurrency in 1..=4, waiters in 0..=3 and total <=4",
            ));
        }
        let total = workers + queued;
        Ok(Self {
            // Initial submissions must fit even before workers pop the queue.
            // Work/result admission enforces the configured running+queued cap.
            executor: CpuExecutor::new(ExecutorConfig {
                workers,
                queue_capacity: total,
            })?,
            admission: process::Admission::new(total),
        })
    }

    /// All admitted preparation, CPU work and retained outputs across callers.
    pub fn admitted(&self) -> usize {
        self.admission.active()
    }

    /// Fail fast with `None` if full; otherwise synchronously await completion.
    /// Queue time consumes the original operation deadline. Cancellation of the
    /// supplied operation (including disconnect signalled by its transport owner)
    /// propagates to this job's Control within the bounded wait poll; no extra
    /// watcher or process-global interrupt state is created.
    ///
    /// A cancelled caller does NOT return while its captures are still live:
    /// wait through executor stop/cleanup acknowledgment. A timeout is not that
    /// acknowledgment. Output and job errors retain admission through consumption
    /// (a provider error may itself retain backing storage or source pins).
    pub fn try_run<J: CpuJob>(
        &self,
        operation: Arc<process::Operation>,
        job: J,
    ) -> Result<Option<Completed<J::Output>>> {
        let Some(admitted) = self.try_admit(operation)? else {
            return Ok(None);
        };
        admitted.run(job)
    }

    /// Reserve actual admission BEFORE framing, body spooling, source/scratch
    /// acquisition or capacity allocation. The caller builds owned resources
    /// under this token, then consumes it into run/run_io. No input is read here.
    /// On preparation errors destroy those resources before dropping the token.
    pub fn try_admit(
        &self,
        operation: Arc<process::Operation>,
    ) -> Result<Option<CpuAdmission<'_>>> {
        let io = IoControl::new(operation)?;
        let Some(permit) = self.admission.try_admit() else {
            return Ok(None);
        };
        Ok(Some(CpuAdmission {
            cpu: self,
            io,
            budget: CapacityBudget::new(JOB_BYTES),
            permit: Arc::new(permit),
        }))
    }
}

/// Borrow the runtime outside its jobs; only owned input/work/budget/permit move
/// into its static callback. This is not a scoped-thread lifetime workaround.
pub struct CpuAdmission<'a> {
    cpu: &'a CpuJobs,
    io: IoControl,
    budget: CapacityBudget,
    permit: Arc<process::Permit>,
}

impl CpuAdmission<'_> {
    pub fn budget(&self) -> &CapacityBudget {
        &self.budget
    }

    pub fn io_control(&self) -> IoControl {
        self.io.clone()
    }

    pub fn run_io<J: CpuIoJob>(
        self,
        input: ReceiveInput,
        job: J,
    ) -> Result<Option<Completed<J::Output>>> {
        if !input.belongs_to(&self.io) {
            return Err(invalid("receive input belongs to another CPU admission"));
        }
        self.run(WithInput { input, job })
    }

    /// Consume the reserved slot without re-admitting or resetting its budget.
    /// Rare executor backpressure still returns None, never publication success.
    pub fn run<J: CpuJob>(self, job: J) -> Result<Option<Completed<J::Output>>> {
        let operation = self.io.operation().clone();
        operation.check()?;
        let work = Work {
            job,
            operation: operation.clone(),
            budget: self.budget,
            permit: self.permit,
        };
        let ticket = match self.cpu.executor.submit(
            self.io.control().clone(),
            JobOptions::default(),
            move |control, _| Ok(work.run(control)),
        ) {
            Ok(ticket) => ticket,
            Err(SubmitError::Full) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        loop {
            if operation.check().is_err() {
                ticket.cancel();
            }
            match ticket.wait_timeout(Duration::from_millis(10)) {
                Ok(result) => {
                    operation.check()?;
                    return result?.map(Some);
                }
                Err(WaitError::Timeout) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CpuIoJob, CpuJob, CpuJobs};
    use crate::{
        Result,
        blame::Jobs,
        process::Operation,
        receive_io::{IoControl, ReceiveInput},
    };
    use git_rs_core::{CapacityBudget, CapacityBuffer, Control};
    use std::{
        io::{Read, Write},
        os::unix::net::UnixStream,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    struct Value;
    impl CpuJob for Value {
        type Output = usize;
        fn run(self, control: &Control, _: &CapacityBudget) -> Result<usize> {
            assert_eq!(Operation::current().unwrap().deadline(), control.deadline());
            Ok(42)
        }
    }

    fn until(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !ready() {
            assert!(Instant::now() < deadline, "CPU ownership did not converge");
            std::thread::yield_now();
        }
    }

    #[test]
    fn blame_and_generic_outputs_share_the_default_single_actual_slot() {
        let jobs = Jobs::<usize>::new(1, 0).unwrap();
        let cpu = jobs.cpu();
        let first = Operation::new(5)
            .enter(|| jobs.run(b"first".to_vec(), 5, |_, _| Ok(7)))
            .unwrap()
            .unwrap();
        assert_eq!(**first, 7);
        assert_eq!(cpu.admitted(), 1);
        assert!(cpu.try_run(Operation::new(5), Value).unwrap().is_none());
        drop(first);
        until(|| cpu.admitted() == 0);
        let second = cpu.try_run(Operation::new(5), Value).unwrap().unwrap();
        assert_eq!(*second, 42);
        assert_eq!(jobs.admitted(), 1);
        assert!(
            Operation::new(5)
                .enter(|| jobs.run(b"blocked".to_vec(), 5, |_, _| panic!("full work ran")))
                .unwrap()
                .is_none()
        );
        drop(second);
        assert_eq!(cpu.admitted(), 0);
        assert_eq!(jobs.waiters(), 0);
        assert!(Operation::current().is_none());
    }

    #[test]
    fn expired_or_overlong_operations_are_rejected_without_retaining_admission() {
        let cpu = CpuJobs::new(1, 0).unwrap();
        let expired = Operation::until(Instant::now());
        assert!(cpu.try_run(expired, Value).is_err());
        let overlong = Operation::until(
            Instant::now() + Duration::from_secs(crate::process::MAX_OPERATION_SECONDS + 60),
        );
        assert!(cpu.try_run(overlong, Value).is_err());
        assert_eq!(cpu.admitted(), 0);
        assert_eq!(*cpu.try_run(Operation::new(5), Value).unwrap().unwrap(), 42);
    }

    #[derive(Debug)]
    struct FailedBytes(CapacityBuffer);
    impl std::fmt::Display for FailedBytes {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "fixture CPU failure retaining {} bytes",
                self.0.capacity()
            )
        }
    }
    impl std::error::Error for FailedBytes {}
    impl CpuJob for FailedBytes {
        type Output = ();
        fn run(self, _: &Control, _: &CapacityBudget) -> Result<()> {
            Err(Box::new(self))
        }
    }

    #[test]
    fn a_returned_provider_error_keeps_its_backing_owner_admitted_until_drop() {
        let cpu = CpuJobs::new(1, 0).unwrap();
        let budget = CapacityBudget::new(8192);
        let job = FailedBytes(budget.buffer(4096).unwrap());
        let error = match cpu.try_run(Operation::new(5), job) {
            Err(error) => error,
            Ok(_) => panic!("provider failure was accepted"),
        };
        assert_eq!(
            error.to_string(),
            "fixture CPU failure retaining 4096 bytes"
        );
        assert!(error.source().unwrap().is::<FailedBytes>());
        assert_eq!(cpu.admitted(), 1);
        assert_eq!(budget.used(), 4096);
        assert!(cpu.try_run(Operation::new(5), Value).unwrap().is_none());
        drop(error);
        assert_eq!(budget.used(), 0);
        assert_eq!(cpu.admitted(), 0);
    }

    // Deliberately hold destruction after cancellation to observe the real
    // cleanup boundary, rather than mistaking a cancelled waiter for stop ack.
    struct Cleanup {
        bytes: Option<CapacityBuffer>,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(self.bytes.take());
        }
    }

    struct UntilCancelled {
        _capture: Cleanup,
        entered: mpsc::Sender<()>,
        operation: Arc<Operation>,
    }
    impl CpuJob for UntilCancelled {
        type Output = ();
        fn run(self, control: &Control, _: &CapacityBudget) -> Result<()> {
            assert!(Arc::ptr_eq(&Operation::current().unwrap(), &self.operation));
            self.entered.send(()).unwrap();
            loop {
                // Only Control stops the CPU loop: Operation cancellation must
                // actually bridge, not just make its waiting caller return.
                control.check()?;
                std::thread::yield_now();
            }
        }
    }

    #[test]
    fn request_cancel_waits_for_running_capture_cleanup_and_retains_shared_admission() {
        let cpu = Arc::new(CpuJobs::new(1, 0).unwrap());
        let jobs = Jobs::<usize>::with_cpu(cpu.clone());
        let operation = Operation::new(10);
        let budget = CapacityBudget::new(8192);
        let (cleanup_entered, cleanup_started) = mpsc::channel();
        let (release, cleanup_release) = mpsc::channel();
        let (entered, started) = mpsc::channel();
        let job = UntilCancelled {
            _capture: Cleanup {
                bytes: Some(budget.buffer(4096).unwrap()),
                entered: cleanup_entered,
                release: cleanup_release,
            },
            entered,
            operation: operation.clone(),
        };
        let (returned, finished) = mpsc::channel();
        std::thread::scope(|scope| {
            let running_cpu = cpu.clone();
            let running_operation = operation.clone();
            let waiter = scope.spawn(move || {
                let result = running_cpu.try_run(running_operation, job);
                returned.send(result.is_err()).unwrap();
            });
            started.recv_timeout(Duration::from_secs(2)).unwrap();
            operation.cancel();
            cleanup_started
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            assert_eq!(cpu.admitted(), 1);
            assert_eq!(budget.used(), 4096);
            assert_eq!(finished.try_recv(), Err(mpsc::TryRecvError::Empty));
            assert!(
                Operation::new(2)
                    .enter(|| jobs.run(b"during-cleanup".to_vec(), 2, |_, _| {
                        panic!("cleanup admission was released early")
                    }))
                    .unwrap()
                    .is_none()
            );
            release.send(()).unwrap();
            assert!(finished.recv_timeout(Duration::from_secs(2)).unwrap());
            waiter.join().unwrap();
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(cpu.admitted(), 0);
        assert_eq!(*cpu.try_run(Operation::new(5), Value).unwrap().unwrap(), 42);
    }

    struct NeverStart {
        _capture: Cleanup,
        executed: Arc<AtomicBool>,
    }
    impl CpuJob for NeverStart {
        type Output = ();
        fn run(self, _: &Control, _: &CapacityBudget) -> Result<()> {
            self.executed.store(true, Ordering::Release);
            panic!("cancelled queued CPU job executed")
        }
    }

    #[test]
    fn queued_request_cancel_discards_owned_capture_without_a_free_worker_or_early_refund() {
        let cpu = Arc::new(CpuJobs::new(1, 1).unwrap());
        let jobs = Jobs::<usize>::with_cpu(cpu.clone());
        let (running, started) = mpsc::channel();
        let (release_worker, worker_release) = mpsc::channel();
        assert!(
            jobs.try_background(10, move |control, _| {
                running.send(()).unwrap();
                loop {
                    control.check()?;
                    match worker_release.recv_timeout(Duration::from_millis(10)) {
                        Ok(()) => return Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            })
            .unwrap()
        );
        started.recv_timeout(Duration::from_secs(2)).unwrap();
        let operation = Operation::new(10);
        let budget = CapacityBudget::new(8192);
        let (cleanup_entered, cleanup_started) = mpsc::channel();
        let (release, cleanup_release) = mpsc::channel();
        let executed = Arc::new(AtomicBool::new(false));
        let job = NeverStart {
            _capture: Cleanup {
                bytes: Some(budget.buffer(4096).unwrap()),
                entered: cleanup_entered,
                release: cleanup_release,
            },
            executed: executed.clone(),
        };
        let (returned, finished) = mpsc::channel();
        std::thread::scope(|scope| {
            let queued_cpu = cpu.clone();
            let queued_operation = operation.clone();
            let waiter = scope.spawn(move || {
                returned
                    .send(queued_cpu.try_run(queued_operation, job).is_err())
                    .unwrap();
            });
            until(|| cpu.admitted() == 2);
            operation.cancel();
            cleanup_started
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            assert!(!executed.load(Ordering::Acquire));
            assert_eq!(cpu.admitted(), 2);
            assert_eq!(budget.used(), 4096);
            assert_eq!(finished.try_recv(), Err(mpsc::TryRecvError::Empty));
            assert!(cpu.try_run(Operation::new(2), Value).unwrap().is_none());
            release.send(()).unwrap();
            assert!(finished.recv_timeout(Duration::from_secs(2)).unwrap());
            waiter.join().unwrap();
        });
        assert_eq!(budget.used(), 0);
        assert!(!executed.load(Ordering::Acquire));
        assert_eq!(cpu.admitted(), 1, "the Blame worker still owns its slot");
        release_worker.send(()).unwrap();
        until(|| cpu.admitted() == 0);
    }

    struct Budgeted {
        expected: CapacityBudget,
        bytes: CapacityBuffer,
    }
    impl CpuJob for Budgeted {
        type Output = CapacityBuffer;
        fn run(self, _: &Control, budget: &CapacityBudget) -> Result<CapacityBuffer> {
            assert!(
                budget.shares_with(&self.expected),
                "preparation budget was reset"
            );
            Ok(self.bytes)
        }
    }

    #[test]
    fn admission_precedes_body_preparation_and_reuses_its_budget_without_a_second_slot() {
        let jobs = Jobs::<usize>::new(1, 0).unwrap();
        let cpu = jobs.cpu();
        let admitted = cpu.try_admit(Operation::new(5)).unwrap().unwrap();
        assert_eq!(
            cpu.admitted(),
            1,
            "preparation already owns actual admission"
        );
        assert!(cpu.try_admit(Operation::new(5)).unwrap().is_none());
        assert!(
            !jobs
                .try_background(5, |_, _| panic!("preparation slot was bypassed"))
                .unwrap()
        );
        let budget = admitted.budget().clone();
        let job = Budgeted {
            bytes: budget.buffer(4096).unwrap(),
            expected: budget.clone(),
        };
        let complete = admitted.run(job).unwrap().unwrap();
        assert_eq!(budget.used(), 4096);
        assert_eq!(cpu.admitted(), 1);
        drop(complete);
        assert_eq!(budget.used(), 0);
        assert_eq!(cpu.admitted(), 0);
    }

    struct FourBytes;
    impl CpuIoJob for FourBytes {
        type Output = [u8; 4];
        fn run(self, input: &mut ReceiveInput, _: &Control, _: &CapacityBudget) -> Result<[u8; 4]> {
            let mut bytes = [0; 4];
            input.read_exact(&mut bytes)?;
            Ok(bytes)
        }
    }

    #[test]
    fn owned_io_finishes_its_boundary_while_client_input_stays_open_and_preserves_other_fd() {
        let cpu = CpuJobs::new(1, 0).unwrap();
        let admitted = cpu.try_admit(Operation::new(3)).unwrap().unwrap();
        let (server, mut peer) = UnixStream::pair().unwrap();
        let mut protected = server.try_clone().unwrap();
        protected
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let flags = rustix::fs::fcntl_getfl(&protected).unwrap();
        peer.write_all(b"PING").unwrap();
        let input = ReceiveInput::new(server.into(), admitted.io_control()).unwrap();
        let complete = admitted.run_io(input, FourBytes).unwrap().unwrap();
        assert_eq!(*complete, *b"PING");
        assert_eq!(cpu.admitted(), 1);
        peer.write_all(b"NEXT").unwrap();
        let mut next = [0; 4];
        protected.read_exact(&mut next).unwrap();
        assert_eq!(&next, b"NEXT", "job must not shutdown the shared socket");
        assert_eq!(rustix::fs::fcntl_getfl(&protected).unwrap(), flags);
        drop(complete);
        assert_eq!(cpu.admitted(), 0);
    }

    #[test]
    fn an_unrelated_io_context_is_rejected_without_consuming_input_or_releasing_foreign_state() {
        let cpu = CpuJobs::new(1, 0).unwrap();
        let operation = Operation::new(3);
        let admitted = cpu.try_admit(operation.clone()).unwrap().unwrap();
        let (server, mut peer) = UnixStream::pair().unwrap();
        let mut protected = server.try_clone().unwrap();
        protected
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        peer.write_all(b"KEEP").unwrap();
        // Same operation/deadline is insufficient: reader must use the actual
        // admission's Control, not an unrelated cancellation flag.
        let foreign = IoControl::new(operation).unwrap();
        let input = ReceiveInput::new(server.into(), foreign).unwrap();
        assert!(admitted.run_io(input, FourBytes).is_err());
        assert_eq!(cpu.admitted(), 0);
        let mut bytes = [0; 4];
        protected.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"KEEP");
    }

    struct BlockedRead {
        _capture: Cleanup,
        entered: mpsc::Sender<()>,
    }
    impl CpuIoJob for BlockedRead {
        type Output = ();
        fn run(self, input: &mut ReceiveInput, _: &Control, _: &CapacityBudget) -> Result<()> {
            self.entered.send(()).unwrap();
            let mut byte = [0];
            input.read_exact(&mut byte)?;
            panic!("idle client unexpectedly sent data")
        }
    }

    #[test]
    fn cancelled_blocked_read_stops_before_source_cleanup_and_admission_refund() {
        let cpu = Arc::new(CpuJobs::new(1, 0).unwrap());
        let jobs = Jobs::<usize>::with_cpu(cpu.clone());
        let operation = Operation::new(10);
        let (server, _open_peer) = UnixStream::pair().unwrap();
        let (entered, started) = mpsc::channel();
        let (cleanup_entered, cleanup_started) = mpsc::channel();
        let (release, cleanup_release) = mpsc::channel();
        let (returned, finished) = mpsc::channel();
        let admitted = cpu.try_admit(operation.clone()).unwrap().unwrap();
        let budget = admitted.budget().clone();
        let job = BlockedRead {
            _capture: Cleanup {
                bytes: Some(budget.buffer(4096).unwrap()),
                entered: cleanup_entered,
                release: cleanup_release,
            },
            entered,
        };
        let input = ReceiveInput::new(server.into(), admitted.io_control()).unwrap();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(move || {
                returned.send(admitted.run_io(input, job).is_err()).unwrap();
            });
            started.recv_timeout(Duration::from_secs(2)).unwrap();
            operation.cancel();
            cleanup_started
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            assert_eq!(budget.used(), 4096);
            assert_eq!(cpu.admitted(), 1);
            assert_eq!(finished.try_recv(), Err(mpsc::TryRecvError::Empty));
            assert!(
                !jobs
                    .try_background(2, |_, _| panic!("source cleanup is still live"))
                    .unwrap()
            );
            release.send(()).unwrap();
            assert!(finished.recv_timeout(Duration::from_secs(2)).unwrap());
            waiter.join().unwrap();
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(cpu.admitted(), 0);
    }
}
