//! Direct Rust upload on the existing shared CPU runtime. Source authority is
//! acquired before work; the provider performs only controlled immutable ranges.

use crate::{
    Result, auth,
    blob::Store,
    cache::Cache,
    cpu_jobs::{CpuAdmission, CpuIoJob, CpuJob},
    engine::Engine,
    http, invalid,
    object_source::{RangeStore, Source},
    process,
    protocol::Framed,
    receive::transport::{self, Encoding, MemoryOutput},
    receive_io::{ReceiveInput, StreamOutput},
};
use git_rs_core::{
    CapacityBudget, CapacityBuffer, Control, ReadLimits, SnapshotSession,
    upload::{UploadSession, Version},
};
use std::{
    io::{BufRead, Cursor, Read, Write},
    net::TcpStream,
    os::fd::OwnedFd,
    sync::Arc,
};

const MAX_REQUEST: usize = 16 * 1024 * 1024;

fn admit(engine: &Engine) -> Result<(Arc<crate::cpu_jobs::CpuJobs>, Arc<process::Operation>)> {
    let operation = process::Operation::current()
        .ok_or_else(|| invalid("upload requires a bounded transport operation"))?;
    Ok((engine.blame.cpu(), operation))
}

pub fn ssh(
    engine: &Engine,
    slug: &str,
    principal: &str,
    protocol: &str,
    input: OwnedFd,
    output: OwnedFd,
) -> Result<()> {
    let version = Version::parse(protocol)?;
    let (cpu, operation) = admit(engine)?;
    let admitted = cpu
        .try_admit(operation)?
        .ok_or_else(|| invalid("upload CPU admission busy"))?;
    let io = admitted.io_control();
    let source = acquire(slug, principal, &admitted)?;
    let job = Upload {
        source,
        store: engine.store.clone(),
        cache: engine.cache.clone(),
        version,
        output: StreamOutput::new(output, io.clone())?,
    };
    let input = ReceiveInput::new(input, io)?;
    let _completed = admitted
        .run_io(input, job)?
        .ok_or_else(|| invalid("upload interrupted before completion"))?;
    Ok(())
}

pub(crate) fn http(
    engine: &Engine,
    request: &http::Request,
    principal: &str,
    input: &mut impl BufRead,
    output: &TcpStream,
) -> Result<()> {
    let version = Version::parse(&request.protocol)?;
    let http::Action::Git(cgi) = &request.action else {
        return Err(invalid("upload requires a Git HTTP action"));
    };
    if request.service != auth::Service::Upload {
        return Err(invalid("upload service required"));
    }
    let discovery = match (cgi.method.as_str(), cgi.path.as_str(), cgi.query.as_str()) {
        ("GET", "/info/refs", "service=git-upload-pack") => true,
        ("POST", "/git-upload-pack", "") => false,
        _ => return Err(invalid("unsupported direct upload HTTP request")),
    };
    let encoding = Encoding::parse(&cgi.encoding)?;
    let (cpu, operation) = admit(engine)?;
    let admitted = cpu
        .try_admit(operation)?
        .ok_or_else(|| invalid("upload CPU admission busy"))?;
    let io = admitted.io_control();
    let source = acquire(&request.slug, principal, &admitted)?;
    let mut body = admitted.budget().buffer(0)?;
    if !discovery {
        let mut destination = MemoryOutput {
            bytes: &mut body,
            control: io.control(),
            limit: MAX_REQUEST,
        };
        transport::copy_bounded(
            &mut request.body(input),
            &mut destination,
            MAX_REQUEST as u64,
            &|| {
                io.operation().check()?;
                io.control().check()?;
                Ok(())
            },
        )?;
    }
    let job = Http {
        upload: Upload {
            source,
            store: engine.store.clone(),
            cache: engine.cache.clone(),
            version,
            output: StreamOutput::new(output.try_clone()?.into(), io)?,
        },
        body,
        encoding,
        discovery,
    };
    let _completed = admitted
        .run(job)?
        .ok_or_else(|| invalid("upload interrupted before completion"))?;
    Ok(())
}

fn acquire(slug: &str, principal: &str, admitted: &CpuAdmission<'_>) -> Result<Arc<Source>> {
    auth::repository(slug)?;
    auth::principal(principal)?;
    let io = admitted.io_control();
    io.control().check()?;
    let source = Source::acquire(
        slug,
        principal,
        false,
        io.operation().clone(),
        admitted.budget(),
    )?;
    source.check()?;
    Ok(source)
}

struct Upload {
    source: Arc<Source>,
    store: Store,
    cache: Arc<Cache>,
    version: Version,
    output: StreamOutput,
}

#[derive(Clone, Copy)]
enum Mode {
    Ssh,
    Http,
    Discovery,
}

impl Upload {
    fn serve(
        self,
        input: &mut impl Read,
        mode: Mode,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<Arc<Source>> {
        let Self {
            source,
            store,
            cache,
            version,
            mut output,
        } = self;
        let prepare = || -> Result<_> {
            control.check()?;
            let limits = ReadLimits {
                max_graph_objects: 1_000_000,
                max_graph_edges: 10_000_000,
                ..ReadLimits::default()
            };
            let provider = RangeStore::open(
                source.clone(),
                store,
                cache,
                budget.clone(),
                limits,
                control,
            )?;
            let snapshot = SnapshotSession::open(
                provider,
                &source.identity(),
                control.clone(),
                budget.clone(),
                limits,
            )?;
            UploadSession::new(
                snapshot,
                source
                    .snapshot
                    .refs
                    .iter()
                    .map(|(name, id)| (name.as_slice(), gix::ObjectId::from(id.0))),
                &source.snapshot.default_ref,
                version,
            )
        };
        match mode {
            Mode::Ssh => {
                let mut framed = Framed(&mut output);
                let result = (|| {
                    let mut upload = prepare()?;
                    upload.advertise(false, &mut framed)?;
                    upload.run(input, &mut framed, false)
                })();
                framed.finish(result.is_ok())?;
                result?;
            }
            Mode::Http | Mode::Discovery => {
                let mut upload = prepare()?;
                let discovery = matches!(mode, Mode::Discovery);
                let kind = if discovery { "advertisement" } else { "result" };
                write!(
                    output,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/x-git-upload-pack-{kind}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"
                )?;
                if discovery {
                    upload.advertise(true, &mut output)?;
                } else {
                    upload.run(input, &mut output, true)?;
                }
                output.flush()?;
            }
        }
        control.check()?;
        Ok(source)
    }
}

impl CpuIoJob for Upload {
    type Output = Arc<Source>;
    fn run(
        self,
        input: &mut ReceiveInput,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<Self::Output> {
        self.serve(input, Mode::Ssh, control, budget)
    }
}

struct Http {
    upload: Upload,
    body: CapacityBuffer,
    encoding: Encoding,
    discovery: bool,
}
impl CpuJob for Http {
    type Output = Arc<Source>;
    fn run(self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output> {
        let body = if self.encoding == Encoding::Gzip && !self.discovery {
            let mut decoded = budget.buffer(0)?;
            transport::decode_gzip(
                &mut Cursor::new(self.body),
                &mut MemoryOutput {
                    bytes: &mut decoded,
                    control,
                    limit: MAX_REQUEST,
                },
                control,
                budget,
            )?;
            decoded
        } else {
            self.body
        };
        self.upload.serve(
            &mut Cursor::new(body),
            if self.discovery {
                Mode::Discovery
            } else {
                Mode::Http
            },
            control,
            budget,
        )
    }
}
