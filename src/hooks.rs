use crate::protocol::{self, Update};
use crate::{Result, invalid};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::symlink;
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub fn install(
    scratch: &Path,
    socket: &Path,
    nonce: &str,
    command: &mut std::process::Command,
) -> Result<()> {
    let executable = std::env::current_exe()?;
    fs::create_dir(scratch.join("hooks"))?;
    for hook in ["pre-receive", "proc-receive"] {
        symlink(&executable, scratch.join("hooks").join(hook))?;
    }
    command
        .env("GIT_RS_HOOK_SOCKET", socket)
        .env("GIT_RS_HOOK_NONCE", nonce);
    Ok(())
}

pub fn helper(kind: &str) -> Result<()> {
    let mut socket = UnixStream::connect(
        std::env::var_os("GIT_RS_HOOK_SOCKET").ok_or_else(|| invalid("missing hook context"))?,
    )?;
    socket.set_read_timeout(Some(Duration::from_secs(crate::process::DEADLINE)))?;
    socket.set_write_timeout(Some(Duration::from_secs(crate::process::DEADLINE)))?;
    protocol::write_packet(&mut socket, std::env::var("GIT_RS_HOOK_NONCE")?.as_bytes())?;
    protocol::write_packet(&mut socket, kind.as_bytes())?;
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    let commands = if kind == "pre-receive" {
        let mut commands = Vec::new();
        let mut reader = BufReader::new(input);
        loop {
            let mut line = Vec::new();
            let read = reader
                .by_ref()
                .take((protocol::MAX_REF + 2 * protocol::OID_HEX + 4) as u64)
                .read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            if !line.ends_with(b"\n") || commands.len() >= protocol::MAX_COMMANDS {
                return Err(invalid("hook command limit exceeded"));
            }
            commands.push(Update::parse(&line)?);
        }
        commands
    } else {
        let version =
            protocol::read_packet(&mut input)?.ok_or_else(|| invalid("missing hook version"))?;
        let version = version.strip_suffix(b"\n").unwrap_or(&version);
        let mut parts = version.splitn(2, |b| *b == 0);
        if parts.next() != Some(b"version=1".as_slice()) {
            return Err(invalid("unsupported hook version"));
        }
        let mut atomic = false;
        if let Some(features) = parts.next() {
            for feature in features.split(|b| *b == b' ') {
                if feature == b"atomic" {
                    atomic = true;
                } else if !feature.is_empty() {
                    return Err(invalid("unsupported hook capability"));
                }
            }
        }
        if protocol::read_packet(&mut input)?.is_some() {
            return Err(invalid("invalid capability boundary"));
        }
        protocol::write_packet(
            &mut output,
            if atomic {
                b"version=1\0atomic"
            } else {
                b"version=1"
            },
        )?;
        protocol::flush(&mut output)?;
        protocol::read_updates(&mut input)?
    };
    for command in &commands {
        protocol::write_packet(&mut socket, &command.line())?;
    }
    protocol::flush(&mut socket)?;
    let response = protocol::read_packet(&mut socket)?
        .ok_or_else(|| invalid("missing publication outcome"))?;
    if kind == "pre-receive" {
        if response != b"ok" {
            return Err(invalid("push rejected"));
        }
    } else {
        for command in &commands {
            let mut line = if response == b"ok" {
                b"ok ".to_vec()
            } else {
                b"ng ".to_vec()
            };
            line.extend_from_slice(&command.name);
            if response != b"ok" {
                line.extend_from_slice(b" publication rejected");
            }
            protocol::write_packet(&mut output, &line)?;
        }
        protocol::flush(&mut output)?;
    }
    Ok(())
}

pub fn broker(
    listener: UnixListener,
    nonce: &str,
    stop: &AtomicBool,
    mut validate: impl FnMut(bool, &[Update]) -> Result<()>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + Duration::from_secs(crate::process::DEADLINE);
    let mut original = None;
    let mut published = false;
    while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
        let (mut socket, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        socket.set_read_timeout(Some(Duration::from_secs(10)))?;
        socket.set_write_timeout(Some(Duration::from_secs(10)))?;
        let token = protocol::read_packet(&mut socket)?;
        if token.as_deref() != Some(nonce.as_bytes()) {
            return Err(invalid("invalid private hook context"));
        }
        let kind = protocol::read_packet(&mut socket)?;
        let commands = protocol::read_updates(&mut socket)?;
        let result = match kind.as_deref() {
            Some(b"pre-receive") if original.is_none() && !published => {
                let result = validate(false, &commands);
                if result.is_ok() {
                    original = Some(commands);
                }
                result
            }
            Some(b"proc-receive") if original.as_ref() == Some(&commands) && !published => {
                let result = validate(true, &commands);
                if result.is_ok() {
                    published = true;
                }
                result
            }
            _ => Err(invalid("incomplete or repeated push command list")),
        };
        protocol::write_packet(
            &mut socket,
            if result.is_ok() { b"ok" } else { b"rejected" },
        )?;
        socket.flush()?;
        if let Err(error) = result {
            eprintln!("hook rejected: {error}");
        }
    }
    Ok(())
}

pub fn bind(path: &Path) -> Result<UnixListener> {
    if fs::symlink_metadata(path).is_ok() {
        return Err(invalid("private socket already exists"));
    }
    Ok(UnixListener::bind(path)?)
}

pub struct PrivateSocket {
    pub listener: UnixListener,
    pub path: std::path::PathBuf,
    directory: std::path::PathBuf,
}
impl PrivateSocket {
    pub fn new() -> Result<Self> {
        let directory =
            std::path::PathBuf::from(format!("/tmp/grs-hook-{}", crate::scratch::identifier()?));
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let path = directory.join("hook");
        match bind(&path) {
            Ok(listener) => Ok(Self {
                listener,
                path,
                directory,
            }),
            Err(error) => {
                let _ = fs::remove_dir_all(directory);
                Err(error)
            }
        }
    }
}
impl Drop for PrivateSocket {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            eprintln!("hook socket cleanup failed: {error}");
        }
    }
}

pub struct ServingSocket {
    pub listener: UnixListener,
    path: std::path::PathBuf,
    _lock: std::fs::File,
}
impl ServingSocket {
    pub fn bind(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("socket parent required"))?;
        if fs::metadata(parent)?.permissions().mode() & 0o077 != 0 {
            return Err(invalid("serving socket parent must be private (0700)"));
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path.with_extension("lock"))?;
        lock.try_lock()
            .map_err(|_| invalid("serving socket is owned by another process"))?;
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if !metadata.file_type().is_socket() {
                return Err(invalid("refusing to replace non-socket path"));
            }
            if UnixStream::connect(path).is_ok() {
                return Err(invalid("serving socket is live"));
            }
            fs::remove_file(path)?;
        }
        let listener = bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            listener,
            path: path.into(),
            _lock: lock,
        })
    }

    pub fn scratch_root(&self, root: &Path) -> Result<std::path::PathBuf> {
        let record = self.path.with_extension("instance");
        if let Ok(previous) = fs::read_to_string(&record) {
            let id = previous
                .strip_prefix("instance-")
                .ok_or_else(|| invalid("invalid prior scratch ownership record"))?;
            if id.len() != 32 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err(invalid("invalid prior scratch ownership record"));
            }
            let previous = root.join(previous);
            if previous.exists() {
                fs::remove_dir_all(previous)?;
            }
        }
        let basename = format!("instance-{}", crate::scratch::identifier()?);
        let directory = root.join(&basename);
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        fs::write(record, basename)?;
        Ok(directory)
    }
}
impl Drop for ServingSocket {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            eprintln!("serving socket cleanup failed: {error}");
        }
    }
}
