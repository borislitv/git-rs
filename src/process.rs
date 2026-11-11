use crate::{
    Result, invalid,
    metrics::{self, Metric, Program, Stage},
};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus, Stdio};
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

pub fn command(program: &str) -> Command {
    // Ubuntu's uutils timeout rejects non-UTF-8 ref arguments before Git sees
    // them. Use the explicit GNU entrypoint without changing system providers.
    let mut command = Command::new("/usr/bin/gnutimeout");
    let remaining = OPERATION.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|op| op.deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(DEADLINE))
    });
    command.args([
        "--kill-after=3s",
        &format!("{:.3}s", remaining.as_secs_f64().max(0.001)),
        program,
    ]);
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C");
    command.process_group(0);
    command
}

pub struct Guard(pub Child, bool, Option<metrics::Timer>);

impl Guard {
    pub fn new(child: Child) -> Self {
        Self(child, false, None)
    }
    pub fn spawn(command: &mut Command) -> std::io::Result<Self> {
        let program = match command.get_args().nth(2).and_then(|arg| arg.to_str()) {
            Some("/usr/bin/git") => Program::Git,
            Some("/usr/bin/curl") => Program::Curl,
            Some("/usr/bin/sha256sum") => Program::Sha256sum,
            _ => Program::Other,
        };
        let timer = metrics::Timer::start(Metric::Subprocess(program));
        let spawn = metrics::Timer::start(Metric::Stage(Stage::ProcessSpawn));
        let child = command.spawn()?;
        spawn.finish(true);
        Ok(Self(child, false, Some(timer)))
    }
    pub fn terminate(&self) {
        if !self.1 {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &format!("-{}", self.0.id())])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
    pub fn wait(&mut self) -> std::io::Result<ExitStatus> {
        let status = self.0.wait()?;
        self.1 = true;
        if let Some(timer) = self.2.take() {
            timer.finish(status.success());
        }
        Ok(status)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Kill descendants as well as the direct CGI/timeout child, using an ordinary
        // process rather than an unsafe signal handler or pre_exec callback.
        self.terminate();
        let _ = self.wait();
    }
}

pub fn capture(command: &mut Command, input: &[u8], limit: u64) -> Result<Vec<u8>> {
    check_operation()?;
    let mut child = Guard::spawn(
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null()),
    )?;
    let mut stdin = child
        .0
        .stdin
        .take()
        .ok_or_else(|| invalid("missing child stdin"))?;
    let mut stdout = child
        .0
        .stdout
        .take()
        .ok_or_else(|| invalid("missing child stdout"))?;
    std::thread::scope(|scope| -> Result<Vec<u8>> {
        let sender = scope.spawn(move || -> std::io::Result<()> { stdin.write_all(input) });
        let mut bytes = Vec::new();
        let read = stdout.by_ref().take(limit + 1).read_to_end(&mut bytes);
        if read.is_err() || bytes.len() as u64 > limit {
            child.terminate();
        }
        let sent = sender
            .join()
            .map_err(|_| invalid("subprocess writer panicked"))?;
        read?;
        if bytes.len() as u64 > limit {
            return Err(invalid("subprocess output limit exceeded"));
        }
        sent?;
        if !child.wait()?.success() {
            return Err(invalid("subprocess failed (details suppressed)"));
        }
        Ok(bytes)
    })
}

/// Stream directly into a caller-owned, size-sealed RAM file. The child shares
/// its write offset, so the caller can reject a short response before hashing.
pub fn capture_to_file(command: &mut Command, input: &[u8], file: &File) -> Result<()> {
    check_operation()?;
    let mut child = Guard::spawn(
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::from(file.try_clone()?))
            .stderr(Stdio::null()),
    )?;
    let mut stdin = child
        .0
        .stdin
        .take()
        .ok_or_else(|| invalid("missing child stdin"))?;
    stdin.write_all(input)?;
    drop(stdin);
    if !child.wait()?.success() {
        return Err(invalid("subprocess failed (details suppressed)"));
    }
    check_operation()
}

pub fn git() -> Command {
    let mut cmd = command("/usr/bin/git");
    cmd.env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_REPLACE_OBJECTS", "1");
    cmd
}

pub fn forward_input(input: &mut impl Read, output: &mut impl Write) -> std::io::Result<u64> {
    // Socket-to-pipe splice can block the pipe reader while waiting for more
    // input. Git must be able to answer a request before the client sends EOF.
    let mut buffer = [0; 8192];
    let mut copied = 0;
    loop {
        let count = match input.read(&mut buffer) {
            Ok(0) => return Ok(copied),
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        output.write_all(&buffer[..count])?;
        copied += count as u64;
    }
}
