use crate::{
    Result,
    auth::{self, Service},
    blob::Store,
    cache::Cache,
    hooks, invalid,
    metadata::{self, Snapshot},
    metrics::{self, Metric, Phase, Stage},
    process,
    protocol::{Oid, Update},
    scratch::Scratch,
};
use std::ffi::OsStr;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};

pub struct Engine {
    pub blame: crate::blame::Blame,
    pub store: Store,
    pub cache: Cache,
    pub scratch_root: PathBuf,
    pub operation_seconds: u64,
    pub pack_threads: u8,
}

impl Engine {
    pub fn materialize(&self, snapshot: &Snapshot) -> Result<Scratch> {
        metrics::measure(Metric::Phase(Phase::Materialize), || {
            self.materialize_inner(snapshot)
        })
    }

    fn materialize_inner(&self, snapshot: &Snapshot) -> Result<Scratch> {
        let mut scratch = Scratch::new(&self.scratch_root, self.pack_threads)?;
        let total: u64 = snapshot
            .packs
            .iter()
            .map(|p| p.pack_size.saturating_add(p.idx_size))
            .sum();
        if total > 12 * 1024 * 1024 * 1024 {
            return Err(invalid("snapshot exceeds scratch admission budget"));
        }
        let directory = scratch.path.join("objects/pack");
        for pack in &snapshot.packs {
            scratch.retain(self.store.materialize(
                &snapshot.id,
                &pack.pack_key,
                &pack.pack_digest,
                pack.pack_size,
                &directory.join(format!("pack-{}.pack", pack.hash)),
                &self.cache,
            )?);
            scratch.retain(self.store.materialize(
                &snapshot.id,
                &pack.idx_key,
                &pack.idx_digest,
                pack.idx_size,
                &directory.join(format!("pack-{}.idx", pack.hash)),
                &self.cache,
            )?);
        }
        let restore = metrics::Timer::start(Metric::Stage(Stage::RestoreRefs));
        let mut commands = Vec::new();
        for (name, oid) in &snapshot.refs {
            commands.extend_from_slice(b"create ");
            commands.extend_from_slice(name);
            commands.push(0);
            commands.extend_from_slice(oid.hex().as_bytes());
            commands.push(0);
        }
        if !commands.is_empty() {
            process::capture(
                process::git()
                    .arg("-C")
                    .arg(&scratch.path)
                    .args(["update-ref", "--stdin", "-z"]),
                &commands,
                4096,
            )?;
        }
        restore.finish(true);
        // The native template already points HEAD at main. Other defaults still
        // go through Git; combining HEAD with its referent in update-ref's
        // transaction is rejected by upstream Git as a duplicate update.
        if snapshot.default_ref != b"refs/heads/main" {
            let head = metrics::Timer::start(Metric::Stage(Stage::RestoreHead));
            process::capture(
                process::git()
                    .arg("-C")
                    .arg(&scratch.path)
                    .args(["symbolic-ref", "HEAD"])
                    .arg(OsStr::from_bytes(&snapshot.default_ref)),
                b"",
                4096,
            )?;
            head.finish(true);
        }
        Ok(scratch)
    }

    #[allow(clippy::too_many_arguments)]
    fn validate(
        &self,
        path: &Path,
        snapshot: &Snapshot,
        lease: &metadata::Barrier,
        principal: &str,
        operation: &str,
        commands: &[Update],
        publish: bool,
    ) -> Result<()> {
        let refs = metrics::Timer::start(Metric::Stage(Stage::RefValidate));
        for command in commands {
            process::capture(
                process::git()
                    .args(["check-ref-format"])
                    .arg(OsStr::from_bytes(&command.name)),
                b"",
                4096,
            )?;
            if snapshot
                .refs
                .get(&command.name)
                .copied()
                .unwrap_or(Oid::ZERO)
                != command.old
            {
                return Err(invalid("stale expected ref"));
            }
        }
        refs.finish(true);
        if !publish {
            return Ok(());
        }
        if path.join("shallow").exists() {
            return Err(invalid("shallow repository publication unsupported"));
        }
        let mut fast_forward = Vec::new();
        let mut tips = Vec::new();
        let tip_validation = metrics::Timer::start(Metric::Stage(Stage::TipValidate));
        for command in commands {
            if command.new != Oid::ZERO {
                if command.name.starts_with(b"refs/heads/") {
                    let kind = process::capture(
                        process::git().arg("-C").arg(path).args([
                            "cat-file",
                            "-t",
                            &command.new.hex(),
                        ]),
                        b"",
                        4096,
                    )?;
                    if kind != b"commit\n" {
                        return Err(invalid("branch must point directly to a commit"));
                    }
                }
                tips.push(command.new.hex());
            }
            let ff = if command.old == Oid::ZERO {
                true
            } else if command.new == Oid::ZERO {
                false
            } else if command.name.starts_with(b"refs/heads/") {
                let ancestry = metrics::Timer::start(Metric::Stage(Stage::FastForward));
                let result = process::capture(
                    process::git().arg("-C").arg(path).args([
                        "merge-base",
                        "--is-ancestor",
                        &command.old.hex(),
                        &command.new.hex(),
                    ]),
                    b"",
                    4096,
                );
                // Non-ancestor checks count as unsuccessful stages, like their
                // native Git nonzero exit; authorization still owns the decision.
                ancestry.finish(result.is_ok());
                result.is_ok()
            } else {
                command.old == command.new
            };
            fast_forward.push(ff);
        }
        tip_validation.finish(true);
        if !tips.is_empty() {
            // receive.fsckObjects makes native index-pack strictly validate every
            // incoming object before either hook. Existing packs are immutable,
            // authenticated catalog entries and were validated on ingestion.
            // Check graph connectivity here without re-inflating all old blobs.
            metrics::measure(Metric::Phase(Phase::Graph), || {
                process::capture(
                    process::git()
                        .arg("-C")
                        .arg(path)
                        .args([
                            "fsck",
                            "--connectivity-only",
                            "--strict",
                            "--no-reflogs",
                            "--no-dangling",
                        ])
                        .args(&tips),
                    b"",
                    64 * 1024,
                )
            })?;
        }
        let directory = path.join("objects/pack");
        let baseline: std::collections::BTreeSet<_> =
            snapshot.packs.iter().map(|p| p.hash.as_str()).collect();
        let mut hashes = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let name = entry?.file_name();
            if let Some(hash) = name
                .to_str()
                .and_then(|n| n.strip_prefix("pack-"))
                .and_then(|n| n.strip_suffix(".pack"))
            {
                if hash.len() != crate::protocol::OID_HEX
                    || !hash.bytes().all(|c| c.is_ascii_hexdigit())
                {
                    return Err(invalid("invalid native pack filename"));
                }
                if !baseline.contains(hash) {
                    let mut header = [0; 12];
                    fs::File::open(directory.join(format!("pack-{hash}.pack")))?
                        .read_exact(&mut header)?;
                    if &header[..4] != b"PACK" {
                        return Err(invalid("invalid native pack header"));
                    }
                    // receive-pack can leave an empty pack for a ref-only push.
                    // No new objects means there is nothing to add to S3/catalog.
                    if header[8..12] == [0; 4] {
                        continue;
                    }
                    hashes.push(hash.to_owned());
                }
            }
        }
        hashes.sort();
        let mut packs = Vec::new();
        for hash in hashes {
            packs.push(metrics::measure(Metric::Phase(Phase::Upload), || {
                self.store
                    .upload(&snapshot.id, operation, &directory, &hash)
            })?);
        }
        metrics::measure(Metric::Phase(Phase::Publish), || {
            metadata::publish(
                snapshot,
                lease,
                operation,
                principal,
                commands,
                &fast_forward,
                &packs,
            )
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &self,
        slug: &str,
        principal: &str,
        service: Service,
        protocol: &str,
        cgi: Option<&Cgi>,
        input: &mut (impl Read + Send),
        output: &mut (impl Write + Send),
        cancel_input: &(dyn Fn() + Sync),
    ) -> Result<()> {
        // Transport parsing and Git execution share one absolute request budget.
        let operation = process::Operation::current()
            .unwrap_or_else(|| process::Operation::new(self.operation_seconds));
        operation.enter(|| {
            self.run_operation(
                slug,
                principal,
                service,
                protocol,
                cgi,
                input,
                output,
                cancel_input,
                &operation,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn run_operation(
        &self,
        slug: &str,
        principal: &str,
        service: Service,
        protocol: &str,
        cgi: Option<&Cgi>,
        input: &mut (impl Read + Send),
        output: &mut (impl Write + Send),
        cancel_input: &(dyn Fn() + Sync),
        budget: &std::sync::Arc<process::Operation>,
    ) -> Result<()> {
        auth::repository(slug)?;
        auth::principal(principal)?;
        if !["", "version=0", "version=1", "version=2"].contains(&protocol) {
            return Err(invalid("unsupported Git protocol header"));
        }
        let (snapshot, barrier) = metrics::measure(Metric::Phase(Phase::Snapshot), || {
            metadata::leased_snapshot(slug, principal, service == Service::Receive)
        })?;
        let scratch = self.materialize(&snapshot)?;
        let operation = crate::scratch::identifier()?;
        let setup = metrics::Timer::start(Metric::Stage(Stage::HookSetup));
        let socket = hooks::PrivateSocket::new()?;
        let listener = socket.listener.try_clone()?;
        let nonce = crate::scratch::identifier()?;
        let mut command = process::git();
        if let Some(cgi) = cgi {
            let root = scratch
                .path
                .parent()
                .ok_or_else(|| invalid("invalid scratch root"))?;
            let basename = scratch
                .path
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or_else(|| invalid("invalid scratch basename"))?;
            command
                .arg("http-backend")
                .env("GIT_PROJECT_ROOT", root)
                .env("GIT_HTTP_EXPORT_ALL", "1")
                .env("PATH_INFO", format!("/{basename}{}", cgi.path))
                .env("REQUEST_METHOD", &cgi.method)
                .env("QUERY_STRING", &cgi.query)
                .env("REMOTE_USER", principal)
                .env("CONTENT_TYPE", &cgi.content_type);
            // Only this generated repository path reaches CGI path resolution.
            if let Some(length) = cgi.length {
                command.env("CONTENT_LENGTH", length.to_string());
            }
            if !cgi.encoding.is_empty() {
                command.env("HTTP_CONTENT_ENCODING", &cgi.encoding);
            }
        } else {
            command
                .arg(if service == Service::Upload {
                    "upload-pack"
                } else {
                    "receive-pack"
                })
                .arg(&scratch.path);
        }
        if !protocol.is_empty() {
            command.env("GIT_PROTOCOL", protocol);
        }
        hooks::install(&scratch.path, &socket.path, &nonce, &mut command)?;
        setup.finish(true);
        metrics::measure(Metric::Phase(Phase::Git), || {
            self.execute(
                &scratch.path,
                &snapshot,
                &barrier,
                principal,
                &operation,
                listener,
                &nonce,
                command,
                cgi.is_some(),
                input,
                output,
                cancel_input,
                budget,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute(
        &self,
        path: &Path,
        snapshot: &Snapshot,
        lease: &metadata::Barrier,
        principal: &str,
        operation: &str,
        listener: std::os::unix::net::UnixListener,
        nonce: &str,
        mut command: Command,
        cgi: bool,
        input: &mut (impl Read + Send),
        output: &mut (impl Write + Send),
        cancel_input: &(dyn Fn() + Sync),
        budget: &std::sync::Arc<process::Operation>,
    ) -> Result<()> {
        let mut child = process::Guard::spawn(
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null()),
        )?;
        let mut stdin = child
            .0
            .stdin
            .take()
            .ok_or_else(|| invalid("missing Git input"))?;
        let stdout = child
            .0
            .stdout
            .take()
            .ok_or_else(|| invalid("missing Git output"))?;
        let stop = AtomicBool::new(false);
        let input_finished = (Mutex::new(None), Condvar::new());
        std::thread::scope(|scope| -> Result<()> {
            let broker = scope.spawn(|| {
                budget.enter(|| {
                    hooks::broker(listener, nonce, &stop, |publish, commands| {
                        if cgi && publish {
                            let (state, ready) = &input_finished;
                            let completed = state
                                .lock()
                                .map_err(|_| invalid("HTTP body state poisoned"))?;
                            let (completed, _) = ready
                                .wait_timeout_while(completed, process::remaining()?, |value| {
                                    value.is_none()
                                })
                                .map_err(|_| invalid("HTTP body state poisoned"))?;
                            if *completed != Some(true) {
                                return Err(invalid("HTTP body incomplete"));
                            }
                        }
                        metrics::measure(
                            Metric::Phase(if publish {
                                Phase::Validate
                            } else {
                                Phase::PreReceive
                            }),
                            || {
                                self.validate(
                                    path, snapshot, lease, principal, operation, commands, publish,
                                )
                            },
                        )
                    })
                })
            });
            let finished = &input_finished;
            let sender = scope.spawn(move || {
                let result = budget.enter(|| {
                    let timer = metrics::Timer::start(Metric::Stage(Stage::GitInput));
                    let result = process::forward_input(input, &mut stdin);
                    timer.finish(result.is_ok());
                    result
                });
                let (state, ready) = finished;
                *state.lock().expect("HTTP body state poisoned") = Some(result.is_ok());
                ready.notify_all();
                result
            });
            let mut stdout = BufReader::new(stdout);
            let result = metrics::measure(Metric::Stage(Stage::GitOutput), || -> Result<()> {
                if cgi {
                    forward_cgi_headers(&mut stdout, output)?;
                }
                std::io::copy(&mut stdout, output)?;
                output.flush()?;
                Ok(())
            });
            if result.is_err() {
                child.terminate();
            }
            cancel_input();
            budget.cancel();
            let wait = metrics::Timer::start(Metric::Stage(Stage::GitWait));
            let status = child.wait()?;
            wait.finish(status.success());
            stop.store(true, Ordering::Release);
            let sent = sender
                .join()
                .map_err(|_| invalid("Git input worker panicked"))?;
            let hook_result = broker.join().map_err(|_| invalid("hook broker panicked"))?;
            result?;
            sent?;
            hook_result?;
            if !status.success() {
                return Err(invalid("Git subprocess failed"));
            }
            Ok(())
        })
    }
}

pub struct Cgi {
    pub path: String,
    pub method: String,
    pub query: String,
    pub content_type: String,
    pub encoding: String,
    pub length: Option<u64>,
}

fn forward_cgi_headers(input: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    let mut headers = Vec::new();
    let mut total = 0;
    let mut status = "200 OK".to_string();
    loop {
        let mut line = Vec::new();
        let count = input.take(8193).read_until(b'\n', &mut line)?;
        total += count;
        if count == 0 || count > 8192 || total > 16384 {
            return Err(invalid("invalid CGI headers"));
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        let line = std::str::from_utf8(&line)?.trim_end_matches(['\r', '\n']);
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| invalid("invalid CGI header"))?;
        if value.bytes().any(|c| c < 32 && c != b'\t') {
            return Err(invalid("invalid CGI header value"));
        }
        if name.eq_ignore_ascii_case("status") {
            status = value.trim().to_owned();
        } else if ["content-type", "cache-control", "expires", "pragma"]
            .iter()
            .any(|h| name.eq_ignore_ascii_case(h))
        {
            headers.push(line.to_owned());
        }
    }
    write!(output, "HTTP/1.1 {status}\r\nConnection: close\r\n")?;
    for header in headers {
        write!(output, "{header}\r\n")?;
    }
    output.write_all(b"\r\n")?;
    Ok(())
}
