//! Bounded process fixture using the normal HTTP handler and typed SSH entrypoint.
use git_rs::{
    Result, blob::Store, cache::Cache, engine::Engine, http, invalid, metrics, process, protocol,
};
use std::{io::Write, net::TcpListener, os::unix::net::UnixListener, path::PathBuf, sync::Arc};

#[test]
#[ignore = "process helper; launched by receive_integration.py in an owned fixture"]
fn receive_fixture_process() -> Result<()> {
    let base = PathBuf::from(std::env::var("GIT_RS_RECEIVE_FIXTURE")?);
    let _logs = metrics::init_service_logs()?;
    let mut blame = git_rs::blame::Blame::new(2, 0)?;
    if std::env::var_os("GIT_RS_CACHE_NAMESPACE").is_some() {
        let cache = git_rs::derived_cache::DerivedCache::from_env()?;
        assert!(
            cache.migrate().is_err(),
            "fixture cache dependency must actually be unavailable"
        );
        eprintln!("optional-cache-outage confirmed");
        blame = blame.with_cache(cache);
    }
    let engine = Arc::new(Engine {
        blame,
        store: Store::from_env()?,
        cache: Arc::new(Cache::new(1024 * 1024, 8 * 1024 * 1024)),
        operation_seconds: 15,
    });
    let ssh = UnixListener::bind(base.join("receive.sock"))?;
    let ssh_engine = engine.clone();
    let active = process::Admission::new(4);
    let health = TcpListener::bind("127.0.0.1:0")?;
    eprintln!("health {}", health.local_addr()?);
    let monitor = engine.clone();
    let requests = active.clone();
    std::thread::spawn(move || {
        for mut stream in health.incoming().flatten() {
            let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(2)));
            let mut body = metrics::global().render(requests.active(), monitor.cache.allocated());
            body.push_str(&format!(
                "git_rs_fixture_receive_admitted {}\n",
                monitor.blame.admitted()
            ));
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let ssh_active = active.clone();
    std::thread::spawn(move || {
        for socket in ssh.incoming() {
            let Ok(mut socket) = socket else { break };
            let Some(slot) = ssh_active.try_admit() else {
                continue;
            };
            let engine = ssh_engine.clone();
            std::thread::spawn(move || {
                let _slot = slot;
                let operation = process::Operation::new(engine.operation_seconds);
                let interrupt = socket.try_clone().unwrap();
                let result = operation.with_interrupt(
                    || {
                        let _ = interrupt.shutdown(std::net::Shutdown::Both);
                    },
                    || -> Result<()> {
                        socket.set_read_timeout(Some(std::time::Duration::from_secs(
                            engine.operation_seconds,
                        )))?;
                        socket.set_write_timeout(Some(std::time::Duration::from_secs(
                            engine.operation_seconds,
                        )))?;
                        let mut field = || -> Result<String> {
                            let value = protocol::read_packet(&mut socket)?
                                .ok_or_else(|| invalid("missing bridge field"))?;
                            if value.len() > 1024 {
                                return Err(invalid("oversized bridge field"));
                            }
                            Ok(String::from_utf8(value)?)
                        };
                        if field()? != "git-rs-ssh-1" {
                            return Err(invalid("bridge version"));
                        }
                        let principal = field()?;
                        let slug = field()?;
                        if field()? != "git-receive-pack" {
                            return Err(invalid("fixture accepts receive only"));
                        }
                        let version = field()?;
                        let input = socket.try_clone()?.into();
                        let mut output = protocol::Framed(&mut socket);
                        engine.receive_ssh(&slug, &principal, &version, input, &mut output)
                    },
                );
                eprintln!("rust-receive-ssh result={result:?}");
            });
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0")?;
    eprintln!("development HTTP {}", listener.local_addr()?);
    for stream in listener.incoming() {
        let stream = stream?;
        let Some(slot) = active.try_admit() else {
            continue;
        };
        let engine = engine.clone();
        std::thread::spawn(move || {
            let _slot = slot;
            let result = http::handle(stream, &engine, "alice");
            eprintln!("rust-receive-http result={result:?}");
        });
    }
    Ok(())
}
