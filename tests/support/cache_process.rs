#![forbid(unsafe_code)]
//! Test-only IPC fixture. No production endpoint or external computation ABI.
use git_rs::{
    Result,
    derived_cache::{
        Access, ArtifactKey, CacheMaintenance, Coordinator, DerivedCache, KeyParts, Lookup, Payload,
    },
    invalid,
    process::Operation,
};
use std::{
    collections::HashMap,
    io::{BufRead, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Authority(AtomicBool);
impl Access for Authority {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        if self.0.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(invalid("fixture authorization/source hold expired"))
        }
    }
}
fn key(name: &str) -> Result<ArtifactKey> {
    ArtifactKey::new(KeyParts {
        repository: b"synthetic/process",
        security: b"fixture-readers-v1",
        object_format: "opaque",
        kind: "fixture",
        inputs: &[name.as_bytes()],
        options: b"",
        representation: "raw-v1",
        producer: "fixture-v1",
    })
}
fn report(payload: &Payload) -> Result<String> {
    payload.with_bytes(|bytes| {
        Ok(format!(
            "hit {} {} {}",
            bytes.len(),
            git_rs::protocol::hex(&openssl::sha::sha256(bytes)),
            payload.identity().unwrap_or("local")
        ))
    })
}
fn response(value: &str) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}

// Only synthetic cost-fixture input: bounded even if the file grows after metadata().
fn file_bytes(path: &str) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("fixture input must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(8 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(invalid("fixture payload limit"));
    }
    Ok(bytes)
}

fn run() -> Result<()> {
    let cache = DerivedCache::from_env()?;
    let maintenance = CacheMaintenance::new(&cache);
    let coordinator = Coordinator::new(cache.clone(), 2)?;
    let access = Arc::new(Authority(AtomicBool::new(true)));
    let mut claims = HashMap::new();
    let mut held = HashMap::new();
    let mut lease = None;
    let mut file_loads = 0u64;
    let mut file_loaded_bytes = 0u64;
    let seconds: u64 = std::env::var("GIT_RS_TEST_OPERATION_SECONDS")
        .unwrap_or_else(|_| "15".into())
        .parse()?;
    response("ready");
    let mut input = std::io::stdin().lock();
    loop {
        let mut line = String::new();
        if input.by_ref().take(8193).read_line(&mut line)? == 0 {
            break;
        }
        if line.len() > 8192 {
            return Err(invalid("fixture command too long"));
        }
        let args: Vec<_> = line.split_whitespace().collect();
        if args.as_slice() == ["quit"] {
            break;
        }
        let result = Operation::new(seconds).enter(|| -> Result<String> {
            match args.as_slice() {
                ["migrate"] => {
                    cache.migrate()?;
                    Ok("ok".into())
                }
                ["allow"] => {
                    access.0.store(true, Ordering::Release);
                    Ok("ok".into())
                }
                ["deny"] => {
                    access.0.store(false, Ordering::Release);
                    Ok("ok".into())
                }
                ["claim", name] if claims.len() < 64 => {
                    match cache.get_or_claim(&key(name)?, access.clone())? {
                        Lookup::Hit(payload) => report(&payload),
                        Lookup::Pending => Ok("pending".into()),
                        Lookup::Claimed(claim) => {
                            claims.insert(name.to_string(), claim);
                            Ok("claimed".into())
                        }
                    }
                }
                ["renew", name] => Ok(format!(
                    "{}",
                    cache.renew(
                        claims
                            .get(*name)
                            .ok_or_else(|| invalid("fixture claim absent"))?
                    )?
                )),
                ["publish", name, length, byte] => {
                    let length: usize = length.parse()?;
                    let byte: u8 = byte.parse()?;
                    if length > 8 * 1024 * 1024 {
                        return Err(invalid("fixture payload limit"));
                    }
                    Ok(format!(
                        "{}",
                        cache.publish(
                            claims
                                .get(*name)
                                .ok_or_else(|| invalid("fixture claim absent"))?,
                            access.as_ref(),
                            &vec![byte; length]
                        )?
                    ))
                }
                ["publish-file", name, path] => {
                    let bytes = file_bytes(path)?;
                    file_loads += 1;
                    file_loaded_bytes += bytes.len() as u64;
                    Ok(cache
                        .publish(
                            claims
                                .get(*name)
                                .ok_or_else(|| invalid("fixture claim absent"))?,
                            access.as_ref(),
                            &bytes,
                        )?
                        .to_string())
                }
                ["compute-file", name, path] => {
                    let path = path.to_string();
                    let loaded = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let computed = Arc::new(AtomicBool::new(false));
                    let worker_loaded = loaded.clone();
                    let worker_computed = computed.clone();
                    let payload =
                        coordinator.get_or_compute(key(name)?, access.clone(), move |context| {
                            context.check()?;
                            let bytes = file_bytes(&path)?;
                            if bytes.len() > context.max_bytes() {
                                return Err(invalid("fixture payload limit"));
                            }
                            worker_loaded.store(bytes.len(), Ordering::Release);
                            worker_computed.store(true, Ordering::Release);
                            context.check()?;
                            Ok(bytes)
                        })?;
                    if computed.load(Ordering::Acquire) {
                        file_loads += 1;
                        file_loaded_bytes += loaded.load(Ordering::Acquire) as u64;
                    }
                    report(&payload)
                }
                ["file-stats"] => Ok(format!("files {file_loads} {file_loaded_bytes}")),
                ["get", name] => cache
                    .get_verified(&key(name)?, access.clone())?
                    .as_ref()
                    .map_or(Ok("miss".into()), report),
                ["hold", name] if held.len() < 16 => {
                    let payload = cache
                        .get_verified(&key(name)?, access.clone())?
                        .ok_or_else(|| invalid("fixture cache miss"))?;
                    let value = report(&payload)?;
                    held.insert(name.to_string(), payload);
                    Ok(value)
                }
                ["consume", name] => report(
                    held.get(*name)
                        .ok_or_else(|| invalid("fixture read absent"))?,
                ),
                ["release", name] => {
                    held.remove(*name);
                    Ok("ok".into())
                }
                ["invalidate", name] => {
                    let payload = held
                        .get(*name)
                        .ok_or_else(|| invalid("fixture read absent"))?;
                    Ok(format!(
                        "{}",
                        cache.invalidate(
                            &key(name)?,
                            payload
                                .identity()
                                .ok_or_else(|| invalid("fixture local result"))?
                        )?
                    ))
                }
                ["maintenance"] => {
                    lease = maintenance.claim()?;
                    Ok(if lease.is_some() { "owned" } else { "pending" }.into())
                }
                [action @ ("expire" | "retire" | "reconcile" | "delete")] => {
                    let lease = lease
                        .as_ref()
                        .ok_or_else(|| invalid("fixture maintenance authority absent"))?;
                    Ok(match *action {
                        "expire" => maintenance.expire_batch(lease)?.to_string(),
                        "retire" => maintenance.retire_payloads(lease)?.to_string(),
                        "reconcile" => maintenance.reconcile_uploads(lease)?.to_string(),
                        _ => maintenance.delete_retired(lease)?.to_string(),
                    })
                }
                ["compute", name, length, byte] => {
                    let length: usize = length.parse()?;
                    let byte: u8 = byte.parse()?;
                    let gate = std::env::var("GIT_RS_TEST_COMPUTE_GATE").ok();
                    let payload =
                        coordinator.get_or_compute(key(name)?, access.clone(), move |context| {
                            if length > context.max_bytes() {
                                return Err(invalid("fixture payload limit"));
                            }
                            response("computing");
                            if let Some(address) = gate {
                                let mut socket = std::net::TcpStream::connect(address)?;
                                socket.set_read_timeout(Some(Duration::from_millis(20)))?;
                                let mut signal = [0];
                                loop {
                                    context.check()?;
                                    match socket.read(&mut signal) {
                                        Ok(1) if signal[0] == b'G' => break,
                                        Err(e)
                                            if matches!(
                                                e.kind(),
                                                std::io::ErrorKind::WouldBlock
                                                    | std::io::ErrorKind::TimedOut
                                            ) => {}
                                        _ => return Err(invalid("fixture gate failed")),
                                    }
                                }
                            }
                            let mut bytes = Vec::with_capacity(length);
                            while bytes.len() < length {
                                context.check()?;
                                bytes.resize((bytes.len() + 4096).min(length), byte);
                            }
                            Ok(bytes)
                        })?;
                    report(&payload)
                }
                ["source"] => {
                    let snapshot =
                        git_rs::metadata::snapshot("synthetic/cache-source", "alice", false)?;
                    Ok(format!(
                        "source {} {}",
                        snapshot.generation,
                        snapshot.refs.len()
                    ))
                }
                _ => Err(invalid("invalid fixture command")),
            }
        });
        match result {
            Ok(value) => response(&value),
            Err(error) => response(&format!("error {error}")),
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cache fixture: {error}");
        std::process::exit(1);
    }
}
