#![forbid(unsafe_code)]

use std::{
    fs,
    io::{Read, Write},
    os::{fd::OwnedFd, unix::net::UnixListener},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

// Fill the last partial PIPE_BUF too: an EAGAIN for a large atomic write alone
// does not prove that the short terminal diagnostic would actually block.
fn full_pipe() -> (OwnedFd, OwnedFd) {
    let (read, write) = rustix::pipe::pipe_with(
        rustix::pipe::PipeFlags::CLOEXEC | rustix::pipe::PipeFlags::NONBLOCK,
    )
    .unwrap();
    for bytes in [&[b'x'; 4096][..], b"x"] {
        loop {
            match rustix::io::write(&write, bytes) {
                Ok(count) => assert!(count > 0),
                Err(rustix::io::Errno::AGAIN) => break,
                Err(error) => panic!("fill stderr pipe: {error}"),
            }
        }
    }
    let flags = rustix::fs::fcntl_getfl(&write).unwrap();
    rustix::fs::fcntl_setfl(&write, flags & !rustix::fs::OFlags::NONBLOCK).unwrap();
    (read, write)
}

struct Running(Child);
impl Running {
    fn exit(&mut self) -> ExitStatus {
        // End-to-end process watchdog, including input-pump and logging-guard
        // cleanup. This is not a measurement of the reporter's 200-ms wait.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "blocked terminal diagnostic prevented exit"
            );
            std::thread::yield_now();
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn backend_framing_failure(timings: &str, blocked_stderr: bool) -> Option<String> {
    let root = Root(std::env::temp_dir().join(format!(
        "git-rs-terminal-{}",
        git_rs::scratch::identifier().unwrap()
    )));
    fs::create_dir(&root.0).unwrap();
    let path = root.0.join("backend.sock");
    let listener = UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let (_undrained, stderr) = if blocked_stderr {
        let (read, write) = full_pipe();
        (Some(read), Stdio::from(write))
    } else {
        (None, Stdio::piped())
    };
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("GIT_RS_DEBUG_TIMINGS", timings)
            // The real SSH grammar requires .git; the handshake sends the
            // validated suffix-stripped slug, not the original path.
            .env("SSH_ORIGINAL_COMMAND", "git-upload-pack 'team/project.git'")
            .args(["ssh", "--principal", "alice", "--socket"])
            .arg(&path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut backend = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "CLI exited before connecting to the backend fixture"
                );
                assert!(Instant::now() < deadline, "CLI did not connect to fixture");
                std::thread::yield_now();
            }
            Err(error) => panic!("accept CLI handshake: {error}"),
        }
    };
    backend
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    backend
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    for expected in [
        "git-rs-ssh-1",
        "alice",
        "team/project",
        "git-upload-pack",
        "",
    ] {
        assert_eq!(
            git_rs::protocol::read_packet(&mut backend)
                .unwrap()
                .unwrap(),
            expected.as_bytes()
        );
    }
    // The real CLI has reached its backend boundary. Keep stdin open: the
    // invalid backend response must stop its input pump as well as exit.
    backend.write_all(b"zzzz").unwrap();
    assert_eq!(child.exit().code(), Some(1));
    child.0.stderr.take().map(|mut pipe| {
        let mut text = String::new();
        pipe.read_to_string(&mut text).unwrap();
        text
    })
}

#[test]
fn terminal_cli_error_exits_with_undrained_stderr_after_backend_handshake() {
    for timings in ["0", "1"] {
        assert!(backend_framing_failure(timings, true).is_none());
    }
}

#[test]
fn backend_handshake_reports_the_intended_framing_error_when_stderr_is_writable() {
    for timings in ["0", "1"] {
        let stderr = backend_framing_failure(timings, false).unwrap();
        assert!(
            stderr
                .lines()
                .any(|line| line == "git-rs: invalid SSH response frame"),
            "expected the post-handshake framing diagnostic, got: {stderr}"
        );
    }
}

#[test]
fn service_initialization_error_exits_with_undrained_stderr() {
    for timings in ["0", "1"] {
        let (_undrained, stderr) = full_pipe();
        let mut child = Running(
            Command::new(env!("CARGO_BIN_EXE_git-rs"))
                .env_clear()
                .env("GIT_RS_DEBUG_TIMINGS", timings)
                // Conflict is checked after service diagnostics initialize but
                // before sockets, database, credentials or object stores open.
                .args([
                    "serve",
                    "--http",
                    "127.0.0.1:0",
                    "--dev-http",
                    "127.0.0.1:0",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::from(stderr))
                .spawn()
                .unwrap(),
        );
        assert_eq!(child.exit().code(), Some(1));
    }
}

#[test]
fn terminal_cli_error_keeps_useful_diagnostics_when_stderr_is_writable() {
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .arg("not-a-command")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    assert_eq!(child.exit().code(), Some(1));
    let mut stderr = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(stderr.starts_with("git-rs: usage: git-rs "), "{stderr}");
}
