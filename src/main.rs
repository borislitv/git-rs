#![forbid(unsafe_code)]

use git_rs::{
    Result,
    auth::{self, Service},
    blob::Store,
    cache::Cache,
    engine::Engine,
    history, hooks, http, invalid, metadata, protocol,
};
use std::fs::{self, DirBuilder};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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
    if !(1..=git_rs::process::DEADLINE).contains(&seconds) {
        return Err(invalid("GIT_RS_OPERATION_SECONDS must be in 1..=120"));
    }
    Ok(seconds)
}

fn bridge(args: &[String]) -> Result<()> {
    let principal = option(args, "--principal")?;
    auth::principal(&principal)?;
    let original = std::env::var("SSH_ORIGINAL_COMMAND")?;
    let (service, slug) = auth::ssh_command(&original)?;
    let mut socket = UnixStream::connect(option(args, "--socket")?)?;
    socket.set_read_timeout(Some(Duration::from_secs(git_rs::process::DEADLINE)))?;
    socket.set_write_timeout(Some(Duration::from_secs(git_rs::process::DEADLINE)))?;
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
    socket.set_read_timeout(Some(Duration::from_secs(git_rs::process::DEADLINE)))?;
    socket.set_write_timeout(Some(Duration::from_secs(git_rs::process::DEADLINE)))?;
    if field(&mut socket)? != "git-rs-ssh-1" {
        return Err(invalid("unsupported SSH bridge version"));
    }
    let principal = field(&mut socket)?;
    let slug = field(&mut socket)?;
    let service = Service::parse(&field(&mut socket)?)?;
    let protocol = field(&mut socket)?;
    let mut input = socket.try_clone()?;
    let interrupt = socket.try_clone()?;
    let mut output = protocol::Framed(&mut socket);
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

struct Slot(Arc<AtomicUsize>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn admit(active: &Arc<AtomicUsize>) -> Option<Slot> {
    active
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            (value < 4).then_some(value + 1)
        })
        .ok()
        .map(|_| Slot(active.clone()))
}

fn serve(args: &[String]) -> Result<()> {
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
        store: Store::from_env()?,
        cache: Cache::new(cache_bytes, 8 * 1024 * 1024),
        scratch_root: own_root,
        operation_seconds: operation_seconds()?,
    });
    let active = Arc::new(AtomicUsize::new(0));
    if let Ok(address) = option(args, "--health") {
        health(&address, engine.clone(), active.clone())?;
    }
    if let Ok(address) = option(args, "--dev-http") {
        let principal = option(args, "--dev-principal")?;
        auth::principal(&principal)?;
        let http = TcpListener::bind(http::loopback(&address)?)?;
        eprintln!("development HTTP {}", http.local_addr()?);
        let engine = engine.clone();
        let active = active.clone();
        std::thread::spawn(move || {
            for stream in http.incoming() {
                match stream {
                    Ok(stream) => {
                        if let Some(slot) = admit(&active) {
                            let engine = engine.clone();
                            let principal = principal.clone();
                            std::thread::spawn(move || {
                                let _slot = slot;
                                if let Err(error) = http::handle(stream, &engine, &principal) {
                                    eprintln!("HTTP operation failed: {error}");
                                }
                            });
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
        if let Some(slot) = admit(&active) {
            let engine = engine.clone();
            std::thread::spawn(move || {
                let _slot = slot;
                if let Err(error) = session(socket, &engine) {
                    eprintln!("SSH operation failed: {error}");
                }
            });
        }
    }
    Ok(())
}

fn health(address: &str, engine: Arc<Engine>, active: Arc<AtomicUsize>) -> Result<()> {
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
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                break;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
            let mut line = String::new();
            if BufReader::new(&mut stream)
                .take(4096)
                .read_line(&mut line)
                .is_err()
            {
                continue;
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
                    format!(
                        "git_rs_active_requests {}\ngit_rs_cache_allocated_bytes {}\n",
                        active.load(Ordering::Acquire),
                        engine.cache.allocated()
                    ),
                ),
                _ => ("404 Not Found", String::new()),
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    Ok(())
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
                store: Store::from_env()?,
                cache: Cache::new(0, 0),
                scratch_root: root,
                operation_seconds: operation_seconds()?,
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
            "usage: git-rs serve --scratch DIR --socket PATH [--dev-http 127.0.0.1:PORT --dev-principal NAME] | ssh --socket PATH --principal NAME | migrate | create REPO OWNER | grant REPO ACTOR SUBJECT ROLE PREFIX",
        )),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("git-rs: {error}");
        std::process::exit(1);
    }
}
