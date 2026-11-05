use crate::{Result, invalid};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub const DEADLINE: u64 = 120;

pub struct Operation {
    deadline: Instant,
    cancelled: AtomicBool,
}
thread_local! { static OPERATION: RefCell<Option<Arc<Operation>>> = const { RefCell::new(None) }; }

impl Operation {
    pub fn new(seconds: u64) -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + Duration::from_secs(seconds),
            cancelled: AtomicBool::new(false),
        })
    }
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
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
    let mut command = Command::new("/usr/bin/timeout");
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

pub struct Guard(pub Child, bool);

impl Guard {
    pub fn new(child: Child) -> Self {
        Self(child, false)
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
    let mut child = Guard::new(
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?,
    );
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
