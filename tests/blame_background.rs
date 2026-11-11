#![forbid(unsafe_code)]
//! Background ownership on the shared foreground executor.
use git_rs::{Result, blame::Jobs, process::Operation};
use git_rs_core::Control;
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
