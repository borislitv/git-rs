#![forbid(unsafe_code)]

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

struct Backend {
    child: Child,
    root: PathBuf,
    health: String,
    http: String,
}

impl Backend {
    fn start() -> Self {
        Self::with_timeout(1)
    }

    fn with_timeout(seconds: u64) -> Self {
        let root = std::env::temp_dir().join(format!(
            "git-rs-transport-{}",
            git_rs::scratch::identifier().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("PGHOST", root.join("no-postgres"))
            .env("PGSSLMODE", "verify-full")
            .env("GIT_RS_S3_ENDPOINT", "https://unused.example.invalid")
            .env("GIT_RS_S3_BUCKET", "synthetic")
            .env("GIT_RS_S3_ACCESS_KEY", "synthetic")
            .env("GIT_RS_S3_SECRET_KEY", "synthetic")
            .env("GIT_RS_OPERATION_SECONDS", seconds.to_string())
            .args(["serve", "--scratch"])
            .arg(&root)
            .arg("--socket")
            .arg(root.join("ssh.sock"))
            .args([
                "--health",
                "127.0.0.1:0",
                "--dev-http",
                "127.0.0.1:0",
                "--dev-principal",
                "alice",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut backend = Self {
            child,
            root,
            health: String::new(),
            http: String::new(),
        };
        let stderr = backend.child.stderr.take().unwrap();
        let (sent, received) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                if sent.send(line).is_err() {
                    break;
                }
            }
        });
        let mut startup = Vec::new();
        loop {
            let line = received
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|error| panic!("backend startup {error}: {startup:?}"));
            startup.push(line.clone());
            if let Some(address) = line.strip_prefix("health ") {
                backend.health = address.into();
            }
            if let Some(address) = line.strip_prefix("development HTTP ") {
                backend.http = address.into();
            }
            if line == "SSH bridge ready" {
                break;
            }
        }
        assert!(!backend.health.is_empty() && !backend.http.is_empty());
        backend
    }

    fn health(&self, path: &str) -> String {
        let mut stream = TcpStream::connect(&self.health).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("health must answer within probe timeout");
        response
    }
}

#[test]
fn git_worker_configuration_rejects_unbounded_or_invalid_parallelism() {
    let root = std::env::temp_dir().join(format!(
        "git-rs-worker-options-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    for threads in ["0", "5", "255", "256", "auto", "-1"] {
        let scratch = root.join("must-not-be-created");
        let output = Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("GIT_RS_PACK_THREADS", threads)
            .args(["serve", "--scratch"])
            .arg(&scratch)
            .arg("--socket")
            .arg(root.join("absent/ssh.sock"))
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("GIT_RS_PACK_THREADS must be in 1..=4"),
            "invalid worker budget must be rejected before creating serving state: {:?}",
            output.stderr
        );
        assert!(!scratch.exists());
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn blame_capacity_rejects_invalid_settings_before_serving() {
    let root = std::env::temp_dir().join(format!(
        "git-rs-blame-capacity-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    for (concurrency, waiters) in [
        ("0", "0"),
        ("5", "0"),
        ("2", "3"),
        ("1", "4"),
        ("auto", "0"),
        ("2", "-1"),
        ("", "0"),
        ("18446744073709551615", "0"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("GIT_RS_BLAME_CONCURRENCY", concurrency)
            .env("GIT_RS_BLAME_WAITERS", waiters)
            .args(["serve", "--scratch"])
            .arg(&root)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("blame capacity") || error.contains("GIT_RS_BLAME_"),
            "{error}"
        );
        assert!(
            !root.exists(),
            "invalid capacity must not create serving state"
        );
    }
    for (concurrency, waiters) in [(1, 0), (1, 3), (2, 1), (4, 0)] {
        assert!(
            git_rs::blame::Blame::default()
                .with_capacity(concurrency, waiters)
                .is_ok()
        );
    }
}

#[test]
fn obsolete_blame_worker_configuration_is_rejected_before_serving() {
    use std::os::unix::fs::PermissionsExt;

    let root = std::env::temp_dir().join(format!(
        "git-rs-blame-worker-options-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let not_executable = root.join("not-executable");
    fs::write(&not_executable, b"synthetic, not an executable\n").unwrap();
    fs::set_permissions(&not_executable, fs::Permissions::from_mode(0o600)).unwrap();
    for path in [
        PathBuf::from("relative-worker"),
        root.join("missing"),
        root.clone(),
        not_executable,
        PathBuf::from("/usr/bin/git"),
        PathBuf::from(""),
    ] {
        let scratch = root.join("must-not-be-created");
        let output = Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("GIT_RS_BLAME_WORKER", path)
            .args(["serve", "--scratch"])
            .arg(&scratch)
            .arg("--socket")
            .arg(root.join("absent/ssh.sock"))
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("GIT_RS_BLAME_WORKER is obsolete"),
            "obsolete worker configuration must fail before serving state is created: {:?}",
            output.stderr
        );
        assert!(!scratch.exists());
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn operation_configuration_rejects_budgets_outside_the_hard_bound() {
    let root = std::env::temp_dir().join(format!(
        "git-rs-budget-options-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    for seconds in ["3601", "0", "18446744073709551615"] {
        let output = Command::new(env!("CARGO_BIN_EXE_git-rs"))
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("PGHOST", root.join("no-postgres"))
            .env("PGSSLMODE", "verify-full")
            .env("GIT_RS_S3_ENDPOINT", "https://unused.example.invalid")
            .env("GIT_RS_S3_BUCKET", "synthetic")
            .env("GIT_RS_S3_ACCESS_KEY", "synthetic")
            .env("GIT_RS_S3_SECRET_KEY", "synthetic")
            .env("GIT_RS_OPERATION_SECONDS", seconds)
            .args(["backup", "team/project", "alice", "--scratch"])
            .arg(&root)
            .arg("--output")
            .arg(root.join("backup.bundle"))
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("GIT_RS_OPERATION_SECONDS must be in 1..=3600"),
            "invalid budget must be rejected before dependency access: {:?}",
            output.stderr
        );
        assert!(!root.join("backup.bundle").exists());
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn long_operation_budgets_preserve_independent_health_deadlines() {
    for seconds in [121, 3600] {
        let backend = Backend::with_timeout(seconds);
        assert!(backend.health("/livez").starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(
            backend
                .health("/readyz")
                .starts_with("HTTP/1.1 503 Service Unavailable\r\n")
        );
        let mut partial = TcpStream::connect(&backend.health).unwrap();
        partial
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        partial.write_all(b"GET /").unwrap();
        let started = Instant::now();
        let mut byte = [0];
        assert_eq!(
            partial.read(&mut byte).unwrap(),
            0,
            "health deadline must close stalled input"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(backend.health("/livez").starts_with("HTTP/1.1 200 OK\r\n"));
    }
}

#[test]
fn private_hook_broker_stops_for_expired_or_cancelled_operation_budgets() {
    use git_rs::{hooks, process::Operation};
    use std::sync::atomic::{AtomicBool, Ordering};

    let cancelled = Operation::new(3600);
    cancelled.cancel();
    for operation in [Operation::new(0), cancelled] {
        let socket = hooks::PrivateSocket::new().unwrap();
        let stop = AtomicBool::new(false);
        let (finished, received) = mpsc::channel();
        let started = Instant::now();
        let result = std::thread::scope(|scope| {
            // Bound the regression even if the broker ignores its operation.
            let stop = &stop;
            scope.spawn(move || {
                let _ = received.recv_timeout(Duration::from_secs(2));
                stop.store(true, Ordering::Release);
            });
            let result = operation.enter(|| {
                hooks::broker(
                    socket.listener.try_clone().unwrap(),
                    "synthetic-nonce",
                    stop,
                    |_, _| panic!("an expired operation cannot publish"),
                )
            });
            let _ = finished.send(());
            result
        });
        result.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the broker must stop with its operation"
        );
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn blocked_stderr_backend(timings: bool) -> (Backend, UnixStream) {
    use std::os::fd::OwnedFd;
    let root = std::env::temp_dir().join(format!(
        "git-rs-diagnostics-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let (reader, mut writer) = UnixStream::pair().unwrap();
    reader
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let stderr: OwnedFd = writer.try_clone().unwrap().into();
    let child = Command::new(env!("CARGO_BIN_EXE_git-rs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("PGHOST", root.join("no-postgres"))
        .env("PGUSER", "postgres")
        .env("PGDATABASE", "synthetic")
        .env("PGSSLMODE", "verify-full")
        .env("GIT_RS_S3_ENDPOINT", "https://unused.example.invalid")
        .env("GIT_RS_S3_BUCKET", "synthetic")
        .env("GIT_RS_S3_ACCESS_KEY", "synthetic")
        .env("GIT_RS_S3_SECRET_KEY", "synthetic")
        .env("GIT_RS_OPERATION_SECONDS", "1")
        .env("GIT_RS_DEBUG_TIMINGS", if timings { "1" } else { "0" })
        .args(["serve", "--scratch"])
        .arg(&root)
        .arg("--socket")
        .arg(root.join("ssh.sock"))
        .args([
            "--health",
            "127.0.0.1:0",
            "--dev-http",
            "127.0.0.1:0",
            "--dev-principal",
            "alice",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    let mut backend = Backend {
        child,
        root,
        health: String::new(),
        http: String::new(),
    };
    let mut startup = BufReader::new(reader);
    loop {
        let mut line = String::new();
        assert!(
            startup.read_line(&mut line).unwrap() > 0,
            "missing startup status"
        );
        if let Some(address) = line.trim().strip_prefix("health ") {
            backend.health = address.into();
        }
        if let Some(address) = line.trim().strip_prefix("development HTTP ") {
            backend.http = address.into();
        }
        if line.trim() == "SSH bridge ready" {
            break;
        }
    }
    writer.set_nonblocking(true).unwrap();
    loop {
        match writer.write(&[b'x'; 8192]) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("fill service diagnostic sink: {error}"),
        }
    }
    writer.set_nonblocking(false).unwrap();
    // Keep the reader alive but never consume it until the backend has exited.
    (backend, startup.into_inner())
}

fn scrape_value(backend: &Backend, name: &str) -> u64 {
    backend
        .health("/metrics")
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("missing metric {name}"))
        .parse()
        .unwrap()
}

fn failing_request(backend: &Backend, ssh: bool) {
    if ssh {
        let mut stream = UnixStream::connect(backend.root.join("ssh.sock")).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        for field in [
            "git-rs-ssh-1",
            "alice",
            "team/synthetic",
            "git-upload-pack",
            "",
        ] {
            git_rs::protocol::write_packet(&mut stream, field.as_bytes()).unwrap();
        }
        assert!(git_rs::protocol::receive_framed(&mut stream, &mut Vec::new()).is_err());
    } else {
        let mut stream = TcpStream::connect(&backend.http).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream.write_all(b"GET /team/synthetic.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: example.invalid\r\n\r\n").unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        assert!(
            response.is_empty(),
            "dependency failure cannot return Git success"
        );
    }
}

fn blocked_stderr_releases_failed_requests(ssh: bool) {
    for timings in [false, true] {
        let (backend, _unread) = blocked_stderr_backend(timings);
        for _ in 0..4 {
            failing_request(&backend, ssh);
        }
        let until = Instant::now() + Duration::from_secs(3);
        while scrape_value(&backend, "git_rs_active_requests") != 0 {
            assert!(
                Instant::now() < until,
                "blocked stderr retained failed request admission (ssh={ssh}, timings={timings})"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let transport = if ssh { "ssh" } else { "http" };
        assert_eq!(
            scrape_value(
                &backend,
                &format!("git_rs_requests_total{{transport=\"{transport}\",outcome=\"error\"}}")
            ),
            4
        );
        // Reuse real admissions while the diagnostic sink remains full. More
        // records than the fixed queue can retain must be dropped, not spawned.
        let baseline_threads = fs::read_dir(format!("/proc/{}/task", backend.child.id()))
            .unwrap()
            .count();
        for _ in 0..1100 {
            failing_request(&backend, ssh);
            // A terminal SSH failure frame can arrive before the serving
            // closure drops its permit. Wait for actual ownership release,
            // rather than accidentally testing legitimate transient overload.
            let until = Instant::now() + Duration::from_secs(3);
            while scrape_value(&backend, "git_rs_active_requests") != 0 {
                assert!(
                    Instant::now() < until,
                    "diagnostic overflow retained admission"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        assert_eq!(
            scrape_value(
                &backend,
                &format!("git_rs_requests_total{{transport=\"{transport}\",outcome=\"error\"}}")
            ),
            1104
        );
        assert!(scrape_value(&backend, "git_rs_service_logs_dropped_total") > 0);
        assert_eq!(scrape_value(&backend, "git_rs_cache_allocated_bytes"), 0);
        assert_eq!(
            scrape_value(
                &backend,
                &format!("git_rs_admission_rejections_total{{listener=\"{transport}\"}}")
            ),
            0
        );
        let threads = fs::read_dir(format!("/proc/{}/task", backend.child.id()))
            .unwrap()
            .count();
        assert!(
            threads <= baseline_threads + 4,
            "diagnostics must not accumulate blocked request threads: {baseline_threads} -> {threads}"
        );
    }
}

#[test]
fn blocked_stderr_http_dependency_errors_release_admission() {
    blocked_stderr_releases_failed_requests(false);
}

#[test]
fn blocked_stderr_ssh_dependency_errors_release_admission() {
    blocked_stderr_releases_failed_requests(true);
}

#[test]
fn cli_and_startup_errors_remain_visible_with_or_without_timing_logs() {
    for timings in ["0", "1"] {
        for (args, message) in [
            (
                vec!["create", "team/synthetic", "alice"],
                "PostgreSQL configuration invalid",
            ),
            (vec!["serve"], "missing --scratch"),
        ] {
            let output = Command::new(env!("CARGO_BIN_EXE_git-rs"))
                .env_clear()
                .env("GIT_RS_DEBUG_TIMINGS", timings)
                .args(args)
                .output()
                .unwrap();
            assert!(!output.status.success());
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(stderr.contains(&format!("git-rs: {message}")), "{stderr}");
            assert!(
                output.stdout.is_empty(),
                "CLI diagnostics must not become stdout data"
            );
        }
    }
}

#[test]
fn partial_health_request_does_not_starve_liveness() {
    let backend = Backend::start();
    let mut slow = TcpStream::connect(&backend.health).unwrap();
    slow.write_all(b"GET /").unwrap();
    assert!(backend.health("/livez").starts_with("HTTP/1.1 200 OK\r\n"));
}

#[test]
fn prometheus_scrape_reports_real_transport_and_dependency_failures_without_identity_labels() {
    let backend = Backend::start();
    for request in [
        "GET /not-a-git-route HTTP/1.1\r\nHost: example.invalid\r\n\r\n",
        "GET /private-project.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: example.invalid\r\n\r\n",
    ] {
        let mut stream = TcpStream::connect(&backend.http).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        stream.read_to_end(&mut Vec::new()).unwrap();
    }
    let metrics = backend.health("/metrics");
    assert!(metrics.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(metrics.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
    for expected in [
        "git_rs_requests_total{transport=\"http\",outcome=\"bad_request\"} 1\n",
        "git_rs_requests_total{transport=\"http\",outcome=\"error\"} 1\n",
        "git_rs_request_duration_seconds_count{transport=\"http\"} 2\n",
        "git_rs_phase_errors_total{phase=\"snapshot\"} 1\n",
    ] {
        assert!(metrics.contains(expected), "missing metric: {expected}");
    }
    let errors: u64 = metrics
        .lines()
        .find_map(|line| line.strip_prefix("git_rs_dependency_errors_total{operation=\"sql\"} "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(errors >= 1);
    assert!(!metrics.contains("private-project"));
    assert!(!metrics.contains("alice"));
}

#[test]
fn trickled_health_request_has_an_absolute_lifetime() {
    let backend = Backend::start();
    let mut stream = TcpStream::connect(&backend.health).unwrap();
    stream.write_all(b"GET /").unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let (stop, stopped) = mpsc::channel();
    let trickle = std::thread::spawn(move || {
        while stopped.recv_timeout(Duration::from_millis(50))
            == Err(mpsc::RecvTimeoutError::Timeout)
        {
            if writer.write_all(b"a").is_err() {
                break;
            }
        }
    });
    let result = stream.read_to_end(&mut Vec::new());
    let _ = stop.send(());
    trickle.join().unwrap();
    assert!(
        result.is_ok()
            || result.is_err_and(|error| error.kind() == std::io::ErrorKind::ConnectionReset),
        "trickling must not extend the absolute health deadline"
    );
    assert!(backend.health("/livez").starts_with("HTTP/1.1 200 OK\r\n"));
}

#[test]
fn health_admission_rejects_excess_partial_requests() {
    let backend = Backend::start();
    let mut streams = Vec::new();
    for _ in 0..32 {
        let mut stream = TcpStream::connect(&backend.health).unwrap();
        let _ = stream.write_all(b"GET /");
        stream.set_nonblocking(true).unwrap();
        streams.push(stream);
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut rejected = 0;
    while Instant::now() < deadline && rejected < 24 {
        streams.retain_mut(|stream| match stream.read(&mut [0]) {
            Ok(0) => {
                rejected += 1;
                false
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(_) => {
                rejected += 1;
                false
            }
            Ok(_) => panic!("partial request cannot have a response"),
        });
        std::thread::yield_now();
    }
    assert!(
        rejected >= 24,
        "at most eight health handlers may be admitted; rejected={rejected}"
    );
    drop(streams);
}

#[test]
fn trickled_http_headers_release_request_admission_at_absolute_deadline() {
    let backend = Backend::start();
    let mut streams = Vec::new();
    for _ in 0..4 {
        let mut stream = TcpStream::connect(&backend.http).unwrap();
        stream.write_all(b"GET /").unwrap();
        streams.push(stream);
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    while !backend
        .health("/metrics")
        .contains("git_rs_active_requests 4\n")
    {
        assert!(
            Instant::now() < deadline,
            "four partial requests must hold admission"
        );
        std::thread::yield_now();
    }
    let mut writers: Vec<_> = streams.iter().map(|s| s.try_clone().unwrap()).collect();
    let (stop, stopped) = mpsc::channel();
    let trickle = std::thread::spawn(move || {
        while stopped
            .recv_timeout(Duration::from_millis(50))
            .is_err_and(|error| error == mpsc::RecvTimeoutError::Timeout)
        {
            for stream in &mut writers {
                let _ = stream.write_all(b"a");
            }
        }
    });
    // Stop the heartbeat even if the assertion fails, so test failures do not leak it.
    let mut result = Ok(());
    for stream in &mut streams {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut response = Vec::new();
        if let Err(error) = stream.read_to_end(&mut response) {
            if error.kind() == std::io::ErrorKind::ConnectionReset {
                continue;
            }
            result = Err(error);
            break;
        }
    }
    let _ = stop.send(());
    trickle.join().unwrap();
    result.expect("absolute one-second header budget must close trickling connections");
    let deadline = Instant::now() + Duration::from_millis(500);
    while !backend
        .health("/metrics")
        .contains("git_rs_active_requests 0\n")
    {
        assert!(
            Instant::now() < deadline,
            "expired requests must release admission"
        );
        std::thread::yield_now();
    }
}

#[test]
fn unread_output_deadline_reaps_child_and_restores_admission() {
    use git_rs::process::{Admission, Guard, Operation};

    let admission = Admission::new(1);
    let permit = admission.try_admit().unwrap();
    assert!(admission.try_admit().is_none());
    let (mut output, _unread_peer) = UnixStream::pair().unwrap();
    // A fallback bounds even the deliberately broken implementation in the red
    // check. The absolute operation budget, not this idle timeout, must win.
    output
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let interrupt = output.try_clone().unwrap();
    let operation = Operation::new(1);
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _permit = permit;
        operation.with_interrupt(
            || {
                let _ = interrupt.shutdown(std::net::Shutdown::Both);
            },
            || {
                let mut child = Guard::new(
                    git_rs::process::command("/usr/bin/dd")
                        .args(["if=/dev/zero", "bs=8192", "count=64", "status=none"])
                        .stdout(Stdio::piped())
                        .stderr(Stdio::null())
                        .spawn()
                        .unwrap(),
                );
                let pid = child.0.id();
                let mut stdout = child.0.stdout.take().unwrap();
                let mut framed = git_rs::protocol::Framed(&mut output);
                let copied = std::io::copy(&mut stdout, &mut framed);
                child.terminate();
                let status = child.wait().unwrap();
                let terminal = framed.finish(copied.is_ok());
                (copied, pid, status, terminal)
            },
        )
    });
    // Observe completion without blocking the test forever on a failed watchdog.
    let collector = std::thread::spawn(move || {
        let _ = sent.send(worker.join().unwrap());
    });
    let outcome = received.recv_timeout(Duration::from_secs(2));
    collector.join().unwrap();
    let (copied, pid, status, terminal) =
        outcome.expect("blocked output must end around the one-second deadline");
    assert!(
        copied.is_err(),
        "a truncated SSH payload must never be reported as success"
    );
    assert!(
        !status.success(),
        "deadline must not leave a successful live producer"
    );
    assert!(
        terminal.is_err(),
        "expired transport cannot send terminal success"
    );
    assert!(
        !PathBuf::from(format!("/proc/{pid}")).exists(),
        "child must be reaped"
    );
    assert_eq!(admission.active(), 0);
    assert!(
        admission.try_admit().is_some(),
        "the next request must be admitted"
    );
}

#[test]
fn completed_and_panicking_requests_join_watchers_without_interrupting() {
    use git_rs::process::Operation;
    use std::sync::atomic::{AtomicBool, Ordering};

    let interrupted = AtomicBool::new(false);
    let began = Instant::now();
    assert_eq!(
        Operation::new(1).with_interrupt(
            || {
                interrupted.store(true, Ordering::Release);
            },
            || 42,
        ),
        42
    );
    let panic = std::panic::catch_unwind(|| {
        Operation::new(1).with_interrupt(
            || {
                interrupted.store(true, Ordering::Release);
            },
            || panic!("request failed"),
        );
    });
    assert!(panic.is_err());
    assert!(!interrupted.load(Ordering::Acquire));
    assert!(
        began.elapsed() < Duration::from_millis(500),
        "watcher must be joined promptly on return and unwind"
    );
    assert!(
        Operation::current().is_none(),
        "request context must be restored on unwind"
    );
}

#[test]
fn completed_transport_preserves_explicit_terminal_success() {
    let (mut output, mut input) = UnixStream::pair().unwrap();
    input
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let interrupt = output.try_clone().unwrap();
    git_rs::process::Operation::new(1).with_interrupt(
        || {
            let _ = interrupt.shutdown(std::net::Shutdown::Both);
        },
        || {
            let mut framed = git_rs::protocol::Framed(&mut output);
            framed.write_all(b"complete Git response").unwrap();
            framed.finish(true).unwrap();
        },
    );
    let mut response = Vec::new();
    git_rs::protocol::receive_framed(&mut input, &mut response).unwrap();
    assert_eq!(response, b"complete Git response");
}
