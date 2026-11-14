//! Real runtime failures, isolated by exec; filesystem mutation is container-only.
use super::source_support;
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase},
    process::Operation,
};
use git_rs_core::{BlameError, CodecRuntimeError, Control};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[path = "blame_cache_support.rs"]
mod cache_support;

fn assert_runtime(mut error: &(dyn std::error::Error + 'static)) {
    loop {
        if matches!(
            error.downcast_ref::<BlameError>(),
            Some(BlameError::CodecRuntime(_))
        ) {
            return;
        }
        error = error
            .source()
            .expect("typed codec runtime rejection, not a cache miss or timeout");
    }
}

fn barrier(control: &Control) -> Result<()> {
    let ready = PathBuf::from(std::env::var_os("GIT_RS_ROOT_RUNTIME_READY").unwrap());
    let resume = PathBuf::from(std::env::var_os("GIT_RS_ROOT_RUNTIME_RESUME").unwrap());
    fs::write(ready, b"ready")?;
    while !resume.exists() {
        control.check()?;
        std::thread::sleep(Duration::from_millis(5));
    }
    control.check()?;
    Ok(())
}

struct RuntimeGate {
    phase: Phase,
    armed: AtomicBool,
    hits: AtomicUsize,
    completed: AtomicUsize,
}
impl Observer for RuntimeGate {
    fn observe(&self, phase: Phase, control: &Control) -> Result<()> {
        if phase == Phase::CacheHit {
            self.hits.fetch_add(1, Ordering::AcqRel);
        }
        if phase == Phase::Complete {
            self.completed.fetch_add(1, Ordering::AcqRel);
        }
        if phase == self.phase && self.armed.swap(false, Ordering::AcqRel) {
            barrier(control)?;
        }
        Ok(())
    }
}

fn failed_http(fixture: &source_support::Fixture) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let engine = fixture.engine.clone();
    let server = std::thread::spawn(move || {
        // The immediately connected client below owns the only accepted request.
        let (stream, _) = listener.accept().unwrap();
        git_rs::http::handle(stream, &engine, "bob")
    });
    let mut client = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(65)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        client,
        "GET /{}.git/blame/{}?path=file HTTP/1.1\r\nHost: localhost\r\n\r\n",
        fixture.slug, fixture.commit
    )
    .unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).unwrap();
    assert!(
        bytes.starts_with(b"HTTP/1.1 500 Internal Server Error\r\n"),
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(!bytes.windows(15).any(|part| part == b"HTTP/1.1 200 OK"));
    let error = server
        .join()
        .unwrap()
        .expect_err("runtime rejection must reach HTTP caller");
    assert_runtime(error.as_ref());
}

fn roots(admin: &cache_support::Admin, namespace: &str) -> String {
    admin.scalar(&format!(
        "SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'"
    ))
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL and source/cache fixture"]
fn owned_source_ready_cache_rechecks_authority_before_and_during_consumption() {
    let (cache, namespace) = cache_support::cache();
    let gate = Arc::new(RuntimeGate {
        phase: Phase::Complete,
        armed: AtomicBool::new(false),
        hits: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
    });
    let fixture = source_support::Fixture::new(
        Blame::new(1, 0)
            .unwrap()
            .with_cache(cache)
            .with_observer(gate.clone()),
        2,
    );
    let admin = cache_support::Admin::new();
    drop(fixture.complete());
    super::until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(roots(&admin, &namespace), "1");
    let body = fixture.complete();
    assert_eq!(gate.hits.load(Ordering::Acquire), 1);
    fixture.revoke();
    git_rs_core::blame::ProducerVersion::verified().unwrap();
    assert!(Operation::new(5).enter(|| body.check()).is_err());
    let mut output = Vec::new();
    assert!(
        Operation::new(5)
            .enter(|| body.write_to(&mut output))
            .is_err()
    );
    assert!(output.is_empty(), "revoked ready body emitted bytes");
    drop(body);
    super::until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.leases(), 0);
    fixture.restored_reader();
    // Grant changes advance source generation, hence require a new durable key.
    drop(fixture.complete());
    super::until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(roots(&admin, &namespace), "2");
    let body = fixture.complete();
    assert_eq!(gate.hits.load(Ordering::Acquire), 2);
    struct Revoking<'a> {
        fixture: &'a source_support::Fixture,
        bytes: Vec<u8>,
        revoked: bool,
    }
    impl Write for Revoking<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.revoked {
                self.fixture.revoke();
                self.revoked = true;
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Revoking {
        fixture: &fixture,
        bytes: Vec::new(),
        revoked: false,
    };
    assert!(
        Operation::new(5)
            .enter(|| body.write_to(&mut output))
            .is_err()
    );
    assert!(output.revoked && !output.bytes.is_empty());
    assert!(
        output.bytes.len() < body.len() as usize,
        "revoked cached response finished framing"
    );
    assert_eq!(
        fixture.leases(),
        1,
        "source lease released before cached response consumption ended"
    );
    drop(body);
    super::until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.leases(), 0);
    assert_eq!(fixture.engine.blame.waiters(), 0);
}

#[test]
#[ignore = "fresh-exec helper for the root runtime tests"]
fn runtime_child() {
    let scenario = std::env::var("GIT_RS_ROOT_RUNTIME_SCENARIO").unwrap();
    if scenario == "override" {
        let error = match Blame::new(1, 0) {
            Err(error) => error,
            Ok(_) => panic!("startup admitted a rejected codec runtime"),
        };
        assert_eq!(
            error.downcast_ref::<BlameError>(),
            Some(&BlameError::CodecRuntime(CodecRuntimeError::Override))
        );
        return;
    }
    let (cache, namespace) = cache_support::cache();
    let gate = Arc::new(RuntimeGate {
        phase: match scenario.as_str() {
            "stage" => Phase::Staging,
            "publish" => Phase::Publishing,
            _ => Phase::Complete,
        },
        armed: AtomicBool::new(false),
        hits: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
    });
    let mut fixture = source_support::Fixture::new(
        Blame::new(1, 0)
            .unwrap()
            .with_cache(cache)
            .with_observer(gate.clone()),
        2,
    );
    Arc::get_mut(&mut fixture.engine).unwrap().operation_seconds = 60;
    let admin = cache_support::Admin::new();
    assert_eq!(roots(&admin, &namespace), "0");
    if matches!(scenario.as_str(), "response" | "cache-hit") {
        let body = fixture.complete();
        let mut bytes = Vec::new();
        Operation::new(10)
            .enter(|| body.write_to(&mut bytes))
            .unwrap();
        assert_eq!(bytes, fixture.expected);
        drop(body);
        super::until(|| fixture.engine.blame.admitted() == 0);
        assert_eq!(
            roots(&admin, &namespace),
            "1",
            "seed a real durable complete body"
        );
    }
    if scenario == "response" {
        let body = fixture.complete();
        assert_eq!(
            gate.hits.load(Ordering::Acquire),
            1,
            "consume a genuine ready-cache hit"
        );
        let sql_before = source_support::metric("sql");
        barrier(&Control::new(Instant::now() + Duration::from_secs(60))).unwrap();
        let error = Operation::new(5).enter(|| body.check()).unwrap_err();
        assert_runtime(error.as_ref());
        let mut bytes = Vec::new();
        let error = Operation::new(5)
            .enter(|| body.write_to(&mut bytes))
            .unwrap_err();
        assert_runtime(error.as_ref());
        assert!(bytes.is_empty(), "rejected sealed body emitted bytes");
        failed_http(&fixture);
        assert_eq!(
            source_support::metric("sql"),
            sql_before,
            "runtime rejection must precede source authority access and admission"
        );
        drop(body);
    } else {
        gate.armed.store(true, Ordering::Release);
        failed_http(&fixture);
        if scenario == "cache-hit" {
            assert_eq!(
                gate.hits.load(Ordering::Acquire),
                1,
                "late mutation must occur after durable hit, before HTTP 200"
            );
        }
    }
    assert_eq!(
        roots(&admin, &namespace),
        if matches!(scenario.as_str(), "stage" | "publish") {
            "0"
        } else {
            "1"
        },
        "runtime rejection must never publish a ready root"
    );
    if scenario == "stage" {
        assert_eq!(
            admin.scalar(&format!(
                "SELECT count(*)::text FROM git_rs_cache.payloads WHERE namespace='{namespace}'"
            )),
            "0",
            "pre-staging runtime fence must not write any payload"
        );
    }
    if matches!(scenario.as_str(), "stage" | "publish") {
        assert_eq!(
            gate.completed.load(Ordering::Acquire),
            0,
            "optional cache swallowed a fatal runtime error and completed local success"
        );
    }
    if scenario == "publish" {
        assert_eq!(
            admin.scalar(&format!(
                "SELECT (count(*)>0)::text FROM git_rs_cache.payloads WHERE namespace='{namespace}'"
            )),
            "true",
            "ready-publication fence must be reached after real staging"
        );
    }
    super::until(|| fixture.engine.blame.admitted() == 0);
    assert_eq!(fixture.engine.blame.waiters(), 0);
    assert_eq!(fixture.leases(), 0);
}

struct Probe(Child);
impl Probe {
    fn spawn(scenario: &str, paths: Option<(&Path, &Path)>) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "runtime_support::runtime_child",
                "--ignored",
                "--nocapture",
            ])
            .env("GIT_RS_ROOT_RUNTIME_SCENARIO", scenario)
            .stdin(Stdio::null());
        if let Some((ready, resume)) = paths {
            command
                .env("GIT_RS_ROOT_RUNTIME_READY", ready)
                .env("GIT_RS_ROOT_RUNTIME_RESUME", resume);
        } else {
            command.env("GCONV_PATH", "/nonexistent-synthetic-codec-path");
        }
        Self(command.spawn().unwrap())
    }
    fn ready(&mut self, ready: &Path) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !ready.exists() {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "root runtime child exited before barrier"
            );
            assert!(Instant::now() < deadline, "root runtime barrier timeout");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn success(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(70);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "root runtime child {status}");
                return;
            }
            assert!(Instant::now() < deadline, "root runtime child timeout");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn startup_rejects_codec_override_before_executor_admission() {
    Probe::spawn("override", None).success();
}

#[test]
#[ignore = "run ALONE in owned disposable root container with real PostgreSQL/source+cache S3; changes/restores TSCII"]
fn owned_source_runtime_fences_ready_cache_http_response_staging_and_publication() {
    assert_eq!(
        std::env::var("GIT_RS_OWNED_CODEC_CONTAINER").as_deref(),
        Ok("1")
    );
    assert!(
        Path::new("/.dockerenv").is_file(),
        "never mutate host runtime"
    );
    struct Restore {
        file: fs::File,
        length: u64,
        ready: PathBuf,
        resume: PathBuf,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            self.file
                .set_len(self.length)
                .expect("restore owned codec module length");
            self.file.sync_all().expect("flush restored codec module");
            let _ = fs::remove_file(&self.ready);
            let _ = fs::remove_file(&self.resume);
        }
    }
    for scenario in ["response", "cache-hit", "stage", "publish"] {
        let prefix =
            std::env::temp_dir().join(format!("root-runtime-{}-{scenario}", std::process::id()));
        let ready = prefix.with_extension("ready");
        let resume = prefix.with_extension("resume");
        assert!(!ready.exists() && !resume.exists());
        let mut child = Probe::spawn(scenario, Some((&ready, &resume)));
        child.ready(&ready);
        let file = OpenOptions::new()
            .append(true)
            .open("/usr/lib/x86_64-linux-gnu/gconv/TSCII.so")
            .unwrap();
        let mut restore = Restore {
            length: file.metadata().unwrap().len(),
            file,
            ready,
            resume,
        };
        // Preserve valid mapped ELF contents and inode: this exercises per-gate
        // freshness, not merely cold dlopen failure or a replacement inode.
        restore
            .file
            .write_all(b"root-runtime-negative-control")
            .unwrap();
        restore.file.sync_all().unwrap();
        fs::write(&restore.resume, b"continue").unwrap();
        child.success();
        drop(child);
        drop(restore);
    }
}
