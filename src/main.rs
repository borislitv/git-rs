#![forbid(unsafe_code)]

use git_rs::{
    Result,
    auth::{self, Service},
    blob::Store,
    cache::Cache,
    engine::Engine,
    history, hooks, http, invalid, metadata,
    metrics::{self, Listener, Outcome, RequestTimer, Transport},
    process::Admission,
    protocol,
};
use std::fs::{self, DirBuilder};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
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

fn pack_threads() -> Result<u8> {
    std::env::var_os("GIT_RS_PACK_THREADS").map_or(Ok(1), |value| {
        value
            .to_str()
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|threads| (1..=4).contains(threads))
            .ok_or_else(|| invalid("GIT_RS_PACK_THREADS must be in 1..=4"))
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
    let mut writer = socket.try_clone()?;
    // The server closes its half once Git exits; the client-side input pipe then
    // closes as ordinary Git terminates. Do not join a blocked terminal reader.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut writer);
        let _ = writer.shutdown(std::net::Shutdown::Write);
    });
    protocol::receive_framed(&mut socket, &mut std::io::stdout().lock())
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
    let mut input = socket.try_clone()?;
    let interrupt = socket.try_clone()?;
    let mut output = protocol::Framed(socket);
    let result = engine.run(
        &slug,
        &principal,
        service,
        &protocol,
        None,
        &mut input,
        &mut output,
        &|| {
            let _ = interrupt.shutdown(std::net::Shutdown::Read);
        },
    );
    output.finish(result.is_ok())?;
    result
}

enum HttpIdentity {
    Development(String),
    Basic(auth::HttpCredentials),
}

fn serve(args: &[String]) -> Result<()> {
    let pack_threads = pack_threads()?;
    let blame = std::env::var_os("GIT_RS_BLAME_WORKER").map_or_else(
        || Ok(git_rs::blame::Blame::default()),
        |path| git_rs::blame::Blame::with_worker(PathBuf::from(path)),
    )?;
    metrics::global();
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
    let root = PathBuf::from(option(args, "--scratch")?);
    if !root.exists() {
        DirBuilder::new().mode(0o700).create(&root)?;
    }
    if !fs::metadata(&root)?.is_dir() || fs::metadata(&root)?.permissions().mode() & 0o077 != 0 {
        return Err(invalid("scratch root must be a private directory (0700)"));
    }
    let socket = PathBuf::from(option(args, "--socket")?);
    let listener = hooks::ServingSocket::bind(&socket)?;
    // The socket lock proves the previous owner has exited; only its recorded
    // scratch root is swept. Other serving processes can share the parent.
    let own_root = listener.scratch_root(&root)?;
    let cache_bytes = std::env::var("GIT_RS_CACHE_BYTES")
        .unwrap_or_else(|_| "67108864".into())
        .parse()?;
    let engine = Arc::new(Engine {
        blame,
        store: Store::from_env()?,
        cache: Cache::new(cache_bytes, 8 * 1024 * 1024),
        scratch_root: own_root,
        operation_seconds: operation_seconds()?,
        pack_threads,
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
                                if let Err(error) = result {
                                    eprintln!("HTTP operation failed: {error}");
                                }
                            });
                        } else {
                            metrics::global().reject(Listener::Http);
                        }
                    }
                    Err(error) => {
                        eprintln!("HTTP accept failed: {error}");
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
                if let Err(error) = session(socket, &engine) {
                    eprintln!("SSH operation failed: {error}");
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
            let mut line = String::new();
            if BufReader::new(&mut stream)
                .take(4096)
                .read_line(&mut line)
                .is_err()
            {
                return Ok(());
            }
            let (status, body) = match line
                .split_whitespace()
                .take(2)
                .collect::<Vec<_>>()
                .as_slice()
            {
                ["GET", "/livez"] => ("200 OK", "live\n".to_string()),
                ["GET", "/readyz"] if ready.load(Ordering::Acquire) => {
                    ("200 OK", "primary metadata available\n".to_string())
                }
                ["GET", "/readyz"] => (
                    "503 Service Unavailable",
                    "primary metadata unavailable\n".to_string(),
                ),
                ["GET", "/metrics"] => (
                    "200 OK",
                    metrics::global().render(active.active(), engine.cache.allocated()),
                ),
                _ => ("404 Not Found", String::new()),
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            Ok(())
        },
    )
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let basename = Path::new(&args[0])
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    if ["pre-receive", "proc-receive"].contains(&basename) {
        return hooks::helper(basename);
    }
    match args.get(1).map(String::as_str) {
        Some("--version") => {
            println!("git-rs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("serve") => serve(&args),
        Some("ssh") => bridge(&args),
        Some("authorized-keys") => authorized_keys(&args),
        Some("migrate") => metadata::migrate(),
        Some("create") if args.len() == 4 => metadata::create(&args[2], &args[3]),
        Some("grant") if args.len() == 7 => {
            metadata::grant(&args[2], &args[3], &args[4], &args[5], args[6].as_bytes())
        }
        Some(action @ ("backup" | "truncate" | "cleanup")) if args.len() >= 4 => {
            let root = PathBuf::from(option(&args, "--scratch")?);
            let engine = Engine {
                blame: git_rs::blame::Blame::default(),
                store: Store::from_env()?,
                cache: Cache::new(0, 0),
                scratch_root: root,
                operation_seconds: operation_seconds()?,
                pack_threads: pack_threads()?,
            };
            git_rs::process::Operation::new(engine.operation_seconds).enter(|| match action {
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
                _ => history::cleanup(&engine, &args[2], &args[3]),
            })
        }
        _ => Err(invalid(
            "usage: git-rs serve --scratch DIR --socket PATH [--http ADDRESS --http-credentials FILE | --dev-http 127.0.0.1:PORT --dev-principal NAME] | ssh --socket PATH --principal NAME | migrate | create REPO OWNER | grant REPO ACTOR SUBJECT ROLE PREFIX",
        )),
    }
}

fn main() {
    let timings = metrics::init_timing_logs();
    let result = run();
    drop(timings);
    if let Err(error) = result {
        eprintln!("git-rs: {error}");
        std::process::exit(1);
    }
}
