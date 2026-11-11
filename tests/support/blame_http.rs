#![forbid(unsafe_code)]
//! Test-only HTTP process: the real root Engine/core, with cooperative observer gates.
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase},
    blob::Store,
    cache::Cache,
    engine::Engine,
    hooks::ServingSocket,
    http, invalid, metrics,
    process::Admission,
};
use git_rs_core::Control;
use std::{
    cell::Cell,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

thread_local! { static JOB: Cell<u64> = const { Cell::new(0) }; }
#[path = "blame_prepared.rs"]
mod prepared;
#[path = "blame_work_observer.rs"]
mod work_observer;
struct Gate {
    path: PathBuf,
    phase: Phase,
    sequence: AtomicU64,
}
impl Observer for Gate {
    fn observe(&self, phase: Phase, control: &Control) -> Result<()> {
        if phase == Phase::Preparing {
            JOB.set(self.sequence.fetch_add(1, Ordering::AcqRel) + 1);
        }
        if phase != self.phase {
            return Ok(());
        }
        let mut peer = UnixStream::connect(&self.path)?;
        peer.set_read_timeout(Some(Duration::from_millis(10)))?;
        peer.set_write_timeout(Some(Duration::from_secs(1)))?;
        writeln!(peer, "{{\"job\":{},\"phase\":\"{:?}\"}}", JOB.get(), phase)?;
        let mut stopping = None;
        loop {
            if stopping.is_none() && control.check().is_err() {
                peer.write_all(b"stop-requested\n")?;
                stopping = Some(Instant::now() + Duration::from_secs(2));
            }
            if stopping.is_some_and(|end| Instant::now() >= end) {
                return Err(invalid("fixture stop acknowledgment deadline"));
            }
            let mut byte = [0];
            match peer.read(&mut byte) {
                Ok(1) => match byte[0] {
                    b'G' => {
                        control.check()?;
                        return Ok(());
                    }
                    b'E' => return Err(invalid("synthetic observer failure")),
                    b'C' => {
                        control.cancel();
                        control.check()?;
                    }
                    b'P' => panic!("synthetic observer panic"),
                    b'A' if stopping.is_some() => {
                        control.check()?;
                        return Err(invalid("fixture stopped"));
                    }
                    _ => return Err(invalid("invalid fixture gate command")),
                },
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                    ) => {}
                _ => return Err(invalid("fixture control disconnected")),
            }
        }
    }
}

fn option(args: &[String], name: &str) -> Result<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| invalid(&format!("missing fixture {name}")))
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--prepared") {
        return prepared::run(&args);
    }
    let seconds: u64 = option(&args, "--seconds")?.parse()?;
    if !(1..=3600).contains(&seconds) {
        return Err(invalid("invalid fixture operation seconds"));
    }
    let mut blame = Blame::new(
        option(&args, "--concurrency")?.parse()?,
        option(&args, "--waiters")?.parse()?,
    )?;
    let options = fixture_options(&args)?;
    if args
        .iter()
        .any(|arg| matches!(arg.as_str(), "--tuning" | "--reuse-off" | "--measure-work"))
    {
        blame = blame.with_test_options(options, !args.iter().any(|arg| arg == "--reuse-off"));
    }
    if args.iter().any(|arg| arg == "--cache") {
        if args.iter().any(|arg| arg == "--checkpoint-fixture-key") {
            blame = blame.with_authenticated_cache_key(
                git_rs::derived_cache::CacheConfig::from_env()?,
                &[7; 32],
            )?;
        } else if let Ok(key) = option(&args, "--checkpoint-key-file") {
            blame = blame.with_authenticated_cache(
                git_rs::derived_cache::CacheConfig::from_env()?,
                std::path::Path::new(&key),
            )?;
        } else {
            blame = blame.with_cache(git_rs::derived_cache::DerivedCache::from_env()?);
        }
    }
    if let Ok(path) = option(&args, "--warm-path") {
        blame = blame.with_warming(git_rs::blame::WarmPolicy::one_path(&path)?)?;
    }
    let mut gate_observer: Option<Arc<dyn Observer>> = None;
    if let Ok(path) = option(&args, "--control") {
        let phase = match option(&args, "--phase")?.as_str() {
            "preparing" => Phase::Preparing,
            "source" => Phase::SourceAcquired,
            "complete" => Phase::Complete,
            "checkpoint-imported" => Phase::CheckpointImported,
            "checkpoint-decoded" => Phase::CheckpointDecoded,
            "checkpoint-installed" => Phase::CheckpointInstalled,
            "checkpoint-published" => Phase::CheckpointPublished,
            _ => return Err(invalid("invalid fixture phase")),
        };
        gate_observer = Some(Arc::new(Gate {
            path: path.into(),
            phase,
            sequence: AtomicU64::new(0),
        }));
    }
    let work = Arc::new(work_observer::WorkObserver::new(gate_observer));
    blame = blame.with_observer(work.clone());
    let root = PathBuf::from(option(&args, "--scratch")?);
    let serving = ServingSocket::bind(&PathBuf::from(option(&args, "--socket")?))?;
    let engine = Arc::new(Engine {
        blame,
        store: Store::from_env()?,
        cache: Arc::new(Cache::new(1024 * 1024, 8 * 1024 * 1024)),
        scratch_root: serving.scratch_root(&root)?,
        operation_seconds: seconds,
        pack_threads: 1,
    });
    let active = Admission::new(16);
    let health = TcpListener::bind("127.0.0.1:0")?;
    eprintln!("health {}", health.local_addr()?);
    let (monitor, requests) = (engine.clone(), active.clone());
    std::thread::spawn(move || {
        for mut stream in health.incoming().flatten() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
            let mut line = String::new();
            let mut reader = BufReader::new((&mut stream).take(8193));
            if reader.read_line(&mut line).is_err() || line.len() > 8192 {
                continue;
            }
            let body = if line.starts_with("GET /metrics ") {
                let mut body =
                    metrics::global().render(requests.active(), monitor.cache.allocated());
                body.push_str(&format!(
                    "git_rs_fixture_blame_admitted {}\n",
                    monitor.blame.admitted()
                ));
                body.push_str(&format!(
                    "git_rs_fixture_blame_waiters {}\n",
                    monitor.blame.waiters()
                ));
                body.push_str(&work.render());
                if let Some(stats) = monitor.blame.warming_statistics() {
                    body.push_str(&format!("git_rs_fixture_warm_admitted {}\ngit_rs_fixture_warm_completed {}\ngit_rs_fixture_warm_failed {}\n", stats.admitted, stats.completed, stats.failed));
                }
                body
            } else {
                "live\n".into()
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0")?;
    eprintln!("development HTTP {}", listener.local_addr()?);
    let principal = option(&args, "--principal")?;
    for stream in listener.incoming() {
        let stream = stream?;
        let Some(permit) = active.try_admit() else {
            continue;
        };
        let (engine, principal) = (engine.clone(), principal.clone());
        std::thread::spawn(move || {
            let _permit = permit;
            if let Err(error) = http::handle(stream, &engine, &principal) {
                eprintln!("fixture request: {error}");
            }
        });
    }
    Ok(())
}
fn fixture_options(args: &[String]) -> Result<git_rs_core::blame::BlameOptions> {
    let options = git_rs_core::blame::BlameOptions::default();
    if !args.iter().any(|arg| arg == "--tuning") {
        return Ok(options);
    }
    let profile = option(args, "--tuning")?;
    let enabled = match profile.as_str() {
        "off" => false,
        "all" | "tokens-off" => true,
        _ => return Err(invalid("fixture tuning must be off, all or tokens-off")),
    };
    let mut tuning = options.tuning();
    tuning.reuse_line_tokens = enabled && profile != "tokens-off";
    tuning.retain_current_input_tokens = enabled;
    tuning.reuse_unchanged_runs = enabled;
    tuning.bounded_resource_cache = enabled;
    tuning.index_changes = enabled;
    let selected = git_rs_core::blame::BlameOptions::native_root(tuning);
    let expected = if !enabled {
        0
    } else if profile == "tokens-off" {
        30
    } else {
        31
    };
    if selected.canonical_bytes()[16] != expected {
        return Err(invalid("fixture tuning canonical options mismatch"));
    }
    Ok(selected)
}
fn main() {
    let invocation = std::env::args().next().unwrap_or_default();
    let basename = std::path::Path::new(&invocation)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    if matches!(basename, "pre-receive" | "proc-receive") {
        if let Err(error) = git_rs::hooks::helper(basename) {
            eprintln!("fixture hook: {error}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(error) = run() {
        eprintln!("blame fixture: {error}");
        std::process::exit(1);
    }
}
