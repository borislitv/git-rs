#![forbid(unsafe_code)]

use git_rs::{
    Result,
    auth::{self, Service},
    blob::Store,
    cache::Cache,
    engine::Engine,
    history, http, invalid, metadata,
    metrics::{self, Listener, Outcome, RequestTimer, ServiceEvent, Transport},
    process::Admission,
    protocol, serving_socket,
};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

fn option(args: &[String], key: &str) -> Result<String> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| invalid(&format!("missing {key}")))
}

fn operation_seconds() -> Result<u64> {
    let seconds = std::env::var("GIT_RS_OPERATION_SECONDS")
        .unwrap_or_else(|_| git_rs::process::DEADLINE.to_string())
        .parse()?;
    if !(1..=git_rs::process::MAX_OPERATION_SECONDS).contains(&seconds) {
        return Err(invalid("GIT_RS_OPERATION_SECONDS must be in 1..=3600"));
    }
    Ok(seconds)
}

fn blame_capacity(name: &str, default: usize) -> Result<usize> {
    std::env::var_os(name).map_or(Ok(default), |value| {
        value
            .to_str()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| invalid(&format!("{name} must be an unsigned integer")))
    })
}

fn bridge(args: &[String]) -> Result<()> {
    let principal = option(args, "--principal")?;
    auth::principal(&principal)?;
    let original = std::env::var("SSH_ORIGINAL_COMMAND")?;
    let (service, slug) = auth::ssh_command(&original)?;
    let mut socket = UnixStream::connect(option(args, "--socket")?)?;
    // The backend enforces its configured absolute budget. The independent
    // bridge must admit any supported backend budget, even without its config.
    let maximum = Duration::from_secs(git_rs::process::MAX_OPERATION_SECONDS);
    socket.set_read_timeout(Some(maximum))?;
    socket.set_write_timeout(Some(maximum))?;
    for field in [
        "git-rs-ssh-1",
        &principal,
        slug,
        service.name(),
        &std::env::var("GIT_PROTOCOL").unwrap_or_default(),
    ] {
        protocol::write_packet(&mut socket, field.as_bytes())?;
    }
    socket.flush()?;
    let operation = git_rs::process::Operation::new(git_rs::process::MAX_OPERATION_SECONDS);
    let control = git_rs::receive_io::IoControl::new(operation.clone())?;
    let input = git_rs::receive_io::ReceiveInput::stdin(control.clone())?;
    let writer = socket.try_clone()?;
    let interrupt = socket.try_clone()?;
    let mut output = BridgeOutput::new(operation.clone())?;
    std::thread::scope(|scope| {
        let sender = scope.spawn(move || {
            let result = input.forward_to(&writer);
            if result.is_ok() {
                let _ = writer.shutdown(std::net::Shutdown::Write);
            }
            result
        });
        let result = operation.with_interrupt(
            || {
                let _ = interrupt.shutdown(std::net::Shutdown::Both);
            },
            || protocol::receive_framed(&mut socket, &mut output),
        );
        // A framed terminal outcome, not client stdin EOF, ends the input pump.
        // Cancel before joining, including failures and full backend send buffers.
        control.cancel();
        let joined = sender
            .join()
            .map_err(|_| invalid("SSH input pump panicked"));
        let _ = joined?;
        result
    })
}

/// Exclusive bridge stdout. No global stdio lock survives a blocked client.
/// Pipe writes are <= PIPE_BUF after writable readiness; sockets use per-call
/// nonblocking sends. Arbitrary devices/files lack this stop contract.
struct BridgeOutput {
    fd: std::os::fd::OwnedFd,
    socket: bool,
    operation: Arc<git_rs::process::Operation>,
}

impl BridgeOutput {
    fn new(operation: Arc<git_rs::process::Operation>) -> Result<Self> {
        use std::os::fd::AsFd;
        let fd = std::io::stdout().as_fd().try_clone_to_owned()?;
        let kind = rustix::fs::FileType::from_raw_mode(rustix::fs::fstat(&fd)?.st_mode);
        let socket = match kind {
            rustix::fs::FileType::Fifo => false,
            rustix::fs::FileType::Socket
                if rustix::net::sockopt::socket_type(&fd)? == rustix::net::SocketType::STREAM =>
            {
                true
            }
            _ => {
                return Err(invalid(
                    "SSH bridge stdout must be an exclusive pipe or stream socket",
                ));
            }
        };
        Ok(Self {
            fd,
            socket,
            operation,
        })
    }
}

impl Write for BridgeOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            self.operation.check().map_err(std::io::Error::other)?;
            let mut poll = [rustix::event::PollFd::new(
                &self.fd,
                rustix::event::PollFlags::OUT,
            )];
            let timeout = rustix::event::Timespec::try_from(Duration::from_millis(10))
                .map_err(std::io::Error::other)?;
            rustix::event::poll(&mut poll, Some(&timeout))?;
            if poll[0].revents().is_empty() {
                continue;
            }
            let bytes = &bytes[..bytes.len().min(4096)];
            let result = if self.socket {
                rustix::net::send(
                    &self.fd,
                    bytes,
                    rustix::net::SendFlags::DONTWAIT | rustix::net::SendFlags::NOSIGNAL,
                )
            } else {
                rustix::io::write(&self.fd, bytes)
            };
            match result {
                Err(rustix::io::Errno::INTR | rustix::io::Errno::AGAIN) => continue,
                result => return result.map_err(Into::into),
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn authorized_keys(args: &[String]) -> Result<()> {
    let mut mapping = String::new();
    fs::File::open(option(args, "--file")?)?
        .take(65537)
        .read_to_string(&mut mapping)?;
    if let Some(line) = auth::authorized_key(
        &mapping,
        &option(args, "--type")?,
        &option(args, "--key")?,
        std::env::current_exe()?
            .to_str()
            .ok_or_else(|| invalid("non UTF-8 executable path"))?,
        &option(args, "--socket")?,
    )? {
        std::io::stdout().lock().write_all(line.as_bytes())?;
    }
    Ok(())
}

fn field(socket: &mut UnixStream) -> Result<String> {
    let packet = protocol::read_packet(socket)?.ok_or_else(|| invalid("missing SSH field"))?;
    if packet.len() > 1024 {
        return Err(invalid("oversized SSH field"));
    }
    Ok(String::from_utf8(packet)?)
}

fn session(mut socket: UnixStream, engine: &Engine) -> Result<()> {
    let operation = git_rs::process::Operation::new(engine.operation_seconds);
    let mut request = operation.enter(|| RequestTimer::start(Transport::Ssh));
    let interrupt = socket.try_clone()?;
    let result = operation.with_interrupt(
        || {
            let _ = interrupt.shutdown(std::net::Shutdown::Both);
        },
        || session_bounded(&mut socket, engine),
    );
    request.outcome(if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Error
    });
    result
}

fn session_bounded(socket: &mut UnixStream, engine: &Engine) -> Result<()> {
    let timeout = Duration::from_secs(engine.operation_seconds);
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;
    if field(socket)? != "git-rs-ssh-1" {
        return Err(invalid("unsupported SSH bridge version"));
    }
    let principal = field(socket)?;
    let slug = field(socket)?;
    let service = Service::parse(&field(socket)?)?;
    let protocol = field(socket)?;
    let input = socket.try_clone()?;
    if service == Service::Receive {
        // The typed receive entrypoint owns its private terminal frame. It must
        // keep the source/admission receipt through that output and cleanup.
        return engine.receive_ssh(
            &slug,
            &principal,
            &protocol,
            input.into(),
            &mut protocol::Framed(socket),
        );
    }
    git_rs::upload::ssh(
        engine,
        &slug,
        &principal,
        &protocol,
        input.into(),
        socket.try_clone()?.into(),
    )
}

enum HttpIdentity {
    Development(String),
    Basic(auth::HttpCredentials),
}

fn serve(args: &[String]) -> Result<()> {
    // Rust retains the initial GNU C/ASCII thread locale. Blame::new verifies
    // that producer-bound codeset before workers/listeners; never setlocale or
    // uselocale while serving.
    if std::env::var_os("GIT_RS_PACK_THREADS").is_some() {
        return Err(invalid(
            "GIT_RS_PACK_THREADS is obsolete; use the shared CPU admission configuration",
        ));
    }
    if std::env::var_os("GIT_RS_BLAME_WORKER").is_some() {
        return Err(invalid(
            "GIT_RS_BLAME_WORKER is obsolete; remove it: blame now runs directly in the bounded Rust core executor",
        ));
    }
    let mut blame = git_rs::blame::Blame::new(
        blame_capacity("GIT_RS_BLAME_CONCURRENCY", 1)?,
        blame_capacity("GIT_RS_BLAME_WAITERS", 0)?,
    )?;
    if let Some(key_file) = std::env::var_os("GIT_RS_BLAME_CHECKPOINT_KEY_FILE") {
        let config = git_rs::derived_cache::CacheConfig::from_env()?;
        blame = blame.with_authenticated_cache(config, Path::new(&key_file))?;
    } else if std::env::var_os("GIT_RS_CACHE_NAMESPACE").is_some() {
        // The cache is optional. Invalid/unavailable cache configuration must not
        // remove the independently bounded local derivation path.
        if let Ok(cache) = git_rs::derived_cache::DerivedCache::from_env() {
            blame = blame.with_cache(cache);
        }
    }
    if let Some(path) = std::env::var_os("GIT_RS_BLAME_WARM_PATH") {
        let path = path
            .to_str()
            .ok_or_else(|| invalid("warm path must be percent-encoded UTF-8"))?;
        blame = blame.with_warming(git_rs::blame::WarmPolicy::one_path(path)?)?;
    }
    metrics::global();
    let _diagnostics = metrics::init_service_logs()?;
    // Validate credentials before opening any serving listener. Production and
    // fixed-principal development HTTP can never be enabled in the same process.
    let production_http = args
        .iter()
        .any(|arg| arg == "--http" || arg == "--http-credentials");
    let development_http = args
        .iter()
        .any(|arg| arg == "--dev-http" || arg == "--dev-principal");
    if production_http && development_http {
        return Err(invalid(
            "production and development HTTP are mutually exclusive",
        ));
    }
    let http = if production_http {
        for flag in ["--http", "--http-credentials"] {
            if args.iter().filter(|arg| arg.as_str() == flag).count() != 1 {
                return Err(invalid(
                    "production HTTP requires one address and one credential file",
                ));
            }
        }
        Some((
            option(args, "--http")?,
            HttpIdentity::Basic(auth::HttpCredentials::load(Path::new(&option(
                args,
                "--http-credentials",
            )?))?),
        ))
    } else if development_http {
        let address = http::loopback(&option(args, "--dev-http")?)?;
        let principal = option(args, "--dev-principal")?;
        auth::principal(&principal)?;
        Some((address.to_string(), HttpIdentity::Development(principal)))
    } else {
        None
    };
    let socket = PathBuf::from(option(args, "--socket")?);
    let listener = serving_socket::ServingSocket::bind(&socket)?;
    let cache_bytes = std::env::var("GIT_RS_CACHE_BYTES")
        .unwrap_or_else(|_| "67108864".into())
        .parse()?;
    let engine = Arc::new(Engine {
        blame,
        store: Store::from_env()?,
        cache: Arc::new(Cache::new(cache_bytes, 8 * 1024 * 1024)),
        operation_seconds: operation_seconds()?,
    });
    let active = Admission::new(4);
    if let Ok(address) = option(args, "--health") {
        health(&address, engine.clone(), active.clone())?;
    }
    if let Some((address, identity)) = http {
        let http = TcpListener::bind(address)?;
        let label = match &identity {
            HttpIdentity::Development(_) => "development HTTP",
            HttpIdentity::Basic(_) => "HTTP",
        };
        eprintln!("{label} {}", http.local_addr()?);
        let identity = Arc::new(identity);
        let engine = engine.clone();
        let active = active.clone();
        std::thread::spawn(move || {
            for stream in http.incoming() {
                match stream {
                    Ok(stream) => {
                        if let Some(slot) = active.try_admit() {
                            let engine = engine.clone();
                            let identity = identity.clone();
                            std::thread::spawn(move || {
                                let _slot = slot;
                                let result = match identity.as_ref() {
                                    HttpIdentity::Development(principal) => {
                                        http::handle(stream, &engine, principal)
                                    }
                                    HttpIdentity::Basic(credentials) => {
                                        http::handle_authenticated(stream, &engine, credentials)
                                    }
                                };
                                if result.is_err() {
                                    metrics::service_event(ServiceEvent::HttpOperationFailed);
                                }
                            });
                        } else {
                            metrics::global().reject(Listener::Http);
                        }
                    }
                    Err(_) => {
                        metrics::service_event(ServiceEvent::HttpAcceptFailed);
                        break;
                    }
                }
            }
        });
    }
    eprintln!("SSH bridge ready");
    for socket in listener.listener.incoming() {
        let socket = socket?;
        if let Some(slot) = active.try_admit() {
            let engine = engine.clone();
            std::thread::spawn(move || {
                let _slot = slot;
                if session(socket, &engine).is_err() {
                    metrics::service_event(ServiceEvent::SshOperationFailed);
                }
            });
        } else {
            metrics::global().reject(Listener::Ssh);
        }
    }
    Ok(())
}

fn health(address: &str, engine: Arc<Engine>, active: Arc<Admission>) -> Result<()> {
    let listener = TcpListener::bind(address)?;
    eprintln!("health {}", listener.local_addr()?);
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let updated = ready.clone();
    std::thread::spawn(move || {
        loop {
            let check = git_rs::process::Operation::new(5)
                .enter(|| metadata::sql("SELECT NOT pg_is_in_recovery();"));
            updated.store(
                check.is_ok_and(|value| value.trim() == "t"),
                Ordering::Release,
            );
            std::thread::sleep(Duration::from_secs(5));
        }
    });
    std::thread::spawn(move || {
        let health_active = Admission::new(8);
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                break;
            };
            // Health requests cannot queue behind one slow peer. Keep a separate
            // bounded budget, so Git admission cannot starve probes either.
            let Some(slot) = health_active.try_admit() else {
                metrics::global().reject(Listener::Health);
                continue;
            };
            let engine = engine.clone();
            let active = active.clone();
            let ready = ready.clone();
            std::thread::spawn(move || {
                let _slot = slot;
                let _ = health_request(stream, &engine, &active, &ready);
            });
        }
    });
    Ok(())
}

fn health_request(
    mut stream: std::net::TcpStream,
    engine: &Engine,
    active: &Admission,
    ready: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    let interrupt = stream.try_clone()?;
    git_rs::process::Operation::new(1).with_interrupt(
        || {
            let _ = interrupt.shutdown(std::net::Shutdown::Both);
        },
        || {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
            let path = health_path(&mut BufReader::new(&mut stream))?;
            let (status, body) = match path.as_str() {
                "/livez" => ("200 OK", "live\n".to_string()),
                "/readyz" if ready.load(Ordering::Acquire) => {
                    ("200 OK", "primary metadata available\n".to_string())
                }
                "/readyz" => (
                    "503 Service Unavailable",
                    "primary metadata unavailable\n".to_string(),
                ),
                "/metrics" => (
                    "200 OK",
                    metrics::global().render(active.active(), engine.cache.allocated()),
                ),
                _ => ("404 Not Found", String::new()),
            };
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )?;
            stream.shutdown(std::net::Shutdown::Write)?;
            Ok(())
        },
    )
}

// Probes share the transport's original one-second absolute interrupt. Consume
// the entire bounded header section before replying; closing with unread TCP
// headers can reset an otherwise successful response. Health never acquires SQL.
fn health_path(input: &mut impl BufRead) -> Result<String> {
    git_rs::health::path(input)
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("--version") => {
            println!("git-rs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("serve") => serve(&args),
        Some("maintenance") => git_rs::maintenance::run(git_rs::maintenance::Options {
            scratch: PathBuf::from(option(&args, "--scratch")?),
            health: option(&args, "--health")?.parse()?,
            once: args.iter().any(|arg| arg == "--once"),
            wait_seconds: if args.iter().any(|arg| arg == "--wait") {
                option(&args, "--wait")?.parse()?
            } else {
                120
            },
        }),
        Some("ssh") => bridge(&args),
        Some("authorized-keys") => authorized_keys(&args),
        Some("migrate") => metadata::migrate(),
        Some("cache-migrate") if args.len() == 2 => git_rs::process::Operation::new(30)
            .enter(|| git_rs::derived_cache::DerivedCache::from_env()?.migrate()),
        Some("create") if args.len() == 4 => metadata::create(&args[2], &args[3]),
        Some("grant") if args.len() == 7 => {
            metadata::grant(&args[2], &args[3], &args[4], &args[5], args[6].as_bytes())
        }
        Some("cleanup") if args.len() >= 4 => git_rs::process::Operation::new(operation_seconds()?)
            .enter(|| {
                let queued = history::cleanup(&args[2], &args[3])?;
                let state = if args.iter().any(|arg| arg == "--wait") {
                    git_rs::maintenance::queue::wait(&queued.id, option(&args, "--wait")?.parse()?)?
                } else {
                    queued
                };
                println!(
                    "{}\t{}\t{}",
                    state.id,
                    state.state,
                    state.error_code.as_deref().unwrap_or("")
                );
                Ok(())
            }),
        Some(action @ ("history" | "backup" | "truncate")) if args.len() >= 4 => {
            let engine = Engine {
                blame: git_rs::blame::Blame::new(1, 0)?,
                store: Store::from_env()?,
                cache: Arc::new(Cache::new(0, 0)),
                operation_seconds: operation_seconds()?,
            };
            git_rs::process::Operation::new(engine.operation_seconds).enter(|| match action {
                "history" => {
                    let branch = if args.iter().any(|arg| arg == "--ref") {
                        Some(option(&args, "--ref")?)
                    } else {
                        None
                    };
                    let max = if args.iter().any(|arg| arg == "--max-commits") {
                        option(&args, "--max-commits")?.parse()?
                    } else {
                        1_000_000
                    };
                    let history = history::list(
                        &engine,
                        &args[2],
                        &args[3],
                        branch.as_deref().map(str::as_bytes),
                        max,
                    )?;
                    let mut output = std::io::stdout().lock();
                    for id in history.iter() {
                        git_rs::process::check_operation()?;
                        writeln!(output, "{id}")?;
                    }
                    output.flush()?;
                    history.check()
                }
                "backup" => history::backup(
                    &engine,
                    &args[2],
                    &args[3],
                    Path::new(&option(&args, "--output")?),
                ),
                "truncate" => history::truncate(
                    &engine,
                    &args[2],
                    &args[3],
                    option(&args, "--keep")?.parse()?,
                    Path::new(&option(&args, "--backup")?),
                    args.iter().any(|a| a == "--drop-other-refs"),
                ),
                _ => Err(invalid("unsupported administration action")),
            })
        }
        _ => Err(invalid(
            "usage: git-rs serve --socket PATH [--http ADDRESS --http-credentials FILE | --dev-http 127.0.0.1:PORT --dev-principal NAME] | maintenance --scratch PATH --health ADDRESS [--once --wait SECONDS] | ssh --socket PATH --principal NAME | migrate | cache-migrate | create REPO OWNER | grant REPO ACTOR SUBJECT ROLE PREFIX | history REPO PRINCIPAL [--ref refs/heads/NAME --max-commits COUNT] | backup REPO PRINCIPAL --output PATH | truncate REPO PRINCIPAL --keep COUNT --backup PATH [--drop-other-refs] | cleanup REPO PRINCIPAL [--wait SECONDS]",
        )),
    }
}

fn main() {
    // The default hook may block indefinitely on stderr before an executor can
    // catch a panic. Do not format untrusted payloads or perform I/O here. The
    // executor turns unwinding into a bounded failed-job acknowledgment.
    std::panic::set_hook(Box::new(|_| {}));
    let timings = metrics::init_timing_logs();
    let result = run();
    drop(timings);
    if let Err(error) = result {
        eprintln!("git-rs: {error}");
        std::process::exit(1);
    }
}
