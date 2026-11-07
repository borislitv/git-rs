//! Authenticated, snapshot-bound complete native line-porcelain annotations.

use crate::{Result, engine::Engine, invalid, metadata, process, protocol::Oid};
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub const MAX_BLOB: u64 = 128 * 1024 * 1024;
pub const MAX_OUTPUT: u64 = 256 * 1024 * 1024;

pub struct Request {
    commit: Oid,
    path: Vec<u8>,
}

impl Request {
    pub fn parse(commit: &str, query: &str) -> Result<Self> {
        let commit = Oid::parse(commit.as_bytes())?;
        let encoded = query
            .strip_prefix("path=")
            .filter(|value| !value.contains('&'))
            .ok_or_else(|| invalid("blame requires exactly one path query"))?;
        let mut cursor = encoded.as_bytes().iter();
        while let Some(byte) = cursor.next() {
            if *byte == b'%'
                && !(cursor.next().is_some_and(u8::is_ascii_hexdigit)
                    && cursor.next().is_some_and(u8::is_ascii_hexdigit))
            {
                return Err(invalid("invalid blame path encoding"));
            }
        }
        let path = percent_encoding::percent_decode_str(encoded).collect::<Vec<_>>();
        if commit == Oid::ZERO
            || path.is_empty()
            || path.len() > 4096
            || path.contains(&0)
            || path
                .split(|byte| *byte == b'/')
                .any(|part| part.is_empty() || part == b"." || part == b"..")
        {
            return Err(invalid("invalid immutable blame target"));
        }
        Ok(Self { commit, path })
    }
}

pub struct Blame {
    worker: Option<PathBuf>,
    admission: Arc<process::Admission>,
}

impl Default for Blame {
    fn default() -> Self {
        Self {
            worker: None,
            admission: process::Admission::new(1),
        }
    }
}

impl Blame {
    /// Select a server-owned executable, never a client-supplied command line.
    pub fn with_worker(path: PathBuf) -> Result<Self> {
        if !path.is_absolute()
            || path.to_str().is_none()
            || !fs::metadata(&path)
                .is_ok_and(|info| info.is_file() && info.permissions().mode() & 0o111 != 0)
        {
            return Err(invalid(
                "GIT_RS_BLAME_WORKER must name an absolute executable file",
            ));
        }
        Ok(Self {
            worker: Some(path),
            ..Self::default()
        })
    }
}

pub enum Response {
    Busy,
    NotFound,
    TooLarge,
    Complete(Vec<u8>),
}

impl Engine {
    pub fn blame(&self, slug: &str, principal: &str, query: &Request) -> Result<Response> {
        let Some(_permit) = self.blame.admission.try_admit() else {
            return Ok(Response::Busy);
        };
        crate::auth::repository(slug)?;
        crate::auth::principal(principal)?;
        let (snapshot, barrier) = metadata::leased_snapshot(slug, principal, false)?;
        let scratch = self.materialize(&snapshot)?;
        let commit = query.commit.hex();
        let kind = capture(
            process::git().arg("-C").arg(&scratch.path).args([
                "cat-file",
                "--batch-check=%(objecttype)",
                "-Z",
            ]),
            &[commit.as_bytes(), b"\0"].concat(),
            128,
        )?;
        if kind != b"commit\0" {
            return Ok(Response::NotFound);
        }
        let reachable = capture(
            process::git().arg("-C").arg(&scratch.path).args([
                "for-each-ref",
                &format!("--contains={commit}"),
                "--count=1",
                "--format=%(objectname)",
            ]),
            b"",
            128,
        )?;
        if reachable.is_empty() {
            return Ok(Response::NotFound);
        }
        let entry = capture(
            process::git()
                .arg("--literal-pathspecs")
                .arg("-C")
                .arg(&scratch.path)
                .args(["ls-tree", "--full-tree", "-z", &commit, "--"])
                .arg(OsStr::from_bytes(&query.path)),
            b"",
            8192,
        )?;
        let Some(tab) = entry.iter().position(|byte| *byte == b'\t') else {
            return Ok(Response::NotFound);
        };
        let fields = std::str::from_utf8(&entry[..tab])?
            .split(' ')
            .collect::<Vec<_>>();
        let [_, "blob", blob] = fields.as_slice() else {
            return Ok(Response::NotFound);
        };
        if entry[tab + 1..].strip_suffix(b"\0") != Some(query.path.as_slice()) {
            return Ok(Response::NotFound);
        }
        let size = capture(
            process::git()
                .arg("-C")
                .arg(&scratch.path)
                .args(["cat-file", "-s", blob]),
            b"",
            64,
        )?;
        if std::str::from_utf8(&size)?.trim().parse::<u64>()? > MAX_BLOB {
            return Ok(Response::TooLarge);
        }
        let mut command = if let Some(worker) = &self.blame.worker {
            let mut command = process::command(
                worker
                    .to_str()
                    .ok_or_else(|| invalid("invalid configured blame worker"))?,
            );
            command
                .arg(&scratch.path)
                .arg(&commit)
                .arg(OsStr::from_bytes(&query.path))
                .env(
                    "GIT_RS_BLAME_REMAINING_MILLIS",
                    process::remaining()?.as_millis().to_string(),
                );
            command
        } else {
            let mut command = process::git();
            command
                .arg("--literal-pathspecs")
                .arg("-C")
                .arg(&scratch.path)
                .args([
                    "blame",
                    "--root",
                    "--line-porcelain",
                    "--no-textconv",
                    "--no-progress",
                    &commit,
                    "--",
                ])
                .arg(OsStr::from_bytes(&query.path));
            command
        };
        let body = capture(&mut command, b"", MAX_OUTPUT)?;
        process::check_operation()?;
        barrier.check()?;
        Ok(Response::Complete(body))
    }
}

/// Reap the owned group on cancellation while its leader is still unreaped, so
/// a reused PID can never identify another request. No child may escape the group.
fn capture(command: &mut Command, input: &[u8], limit: u64) -> Result<Vec<u8>> {
    use std::io::Write;

    process::check_operation()?;
    let operation = process::Operation::current()
        .ok_or_else(|| invalid("blame requires a bounded operation"))?;
    let mut child = process::Guard::spawn(
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null()),
    )?;
    let pid = child.0.id();
    let mut stdin = child
        .0
        .stdin
        .take()
        .ok_or_else(|| invalid("missing blame worker input"))?;
    let mut stdout = child
        .0
        .stdout
        .take()
        .ok_or_else(|| invalid("missing blame worker output"))?;
    let completion = (Mutex::new(false), Condvar::new());
    struct Complete<'a>(&'a (Mutex<bool>, Condvar));
    impl Drop for Complete<'_> {
        fn drop(&mut self) {
            let (finished, changed) = self.0;
            *finished.lock().expect("blame completion poisoned") = true;
            changed.notify_all();
        }
    }
    std::thread::scope(|scope| -> Result<Vec<u8>> {
        let completed = Complete(&completion);
        let monitor = scope.spawn(|| {
            let (finished, changed) = &completion;
            let mut finished = finished.lock().expect("blame completion poisoned");
            while !*finished {
                if operation.enter(process::check_operation).is_err() {
                    let _ = Command::new("/bin/kill")
                        .args(["-KILL", "--", &format!("-{pid}")])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                    break;
                }
                finished = changed
                    .wait_timeout(finished, Duration::from_millis(25))
                    .expect("blame completion poisoned")
                    .0;
            }
        });
        let writer = scope.spawn(move || stdin.write_all(input));
        let mut bytes = Vec::new();
        let read = stdout.by_ref().take(limit + 1).read_to_end(&mut bytes);
        if read.is_err() || bytes.len() as u64 > limit {
            child.terminate();
        }
        drop(completed);
        monitor
            .join()
            .map_err(|_| invalid("blame cancellation monitor panicked"))?;
        let sent = writer
            .join()
            .map_err(|_| invalid("blame input worker panicked"))?;
        read?;
        sent?;
        if bytes.len() as u64 > limit {
            return Err(invalid("blame output limit exceeded"));
        }
        let status = child.wait()?;
        process::check_operation()?;
        if !status.success() {
            return Err(invalid("blame worker failed"));
        }
        Ok(bytes)
    })
}
