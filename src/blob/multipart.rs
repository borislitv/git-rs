//! Attempt-specific multipart output. Only a bounded part is resident in RAM;
//! durable intent precedes network I/O and survives unknown completion outcomes.

use super::{Store, validate_key};
use crate::{
    Result, invalid,
    metadata::{self, Pack},
    metrics::{self, Dependency, Direction, Metric},
    process,
};
use git_rs_core::{
    CapacityBudget, CapacityBuffer, CapacityReservation, Control, SnapshotSession,
    VerifiedObjectProvider, selection::ObjectSelection, write_pack::PackWriteLimits,
};
use openssl::sha::Sha256;
use quick_xml::{Reader, events::Event};
use std::io::Write;

const PART_BYTES: usize = 8 * 1024 * 1024;
const MAX_BYTES: u64 = 12 * 1024 * 1024 * 1024;
const MAX_PARTS: usize = (MAX_BYTES / PART_BYTES as u64) as usize;
const METADATA_BYTES: usize = 2 * 1024 * 1024;

pub(crate) struct UploadPlan {
    store: Store,
    pub repo: String,
    pub operation: String,
    pub pack_key: String,
    pub index_key: String,
    guard: String,
}

impl UploadPlan {
    pub fn begin(store: Store, repo: &str, operation: &str) -> Result<Self> {
        Self::begin_guarded(store, repo, operation, String::new())
    }

    pub(crate) fn begin_for_job(
        store: Store,
        job: &crate::maintenance::queue::Job,
    ) -> Result<Self> {
        job.check()?;
        Self::begin_guarded(
            store,
            &job.repo,
            &job.token,
            format!(
                "DO $job$ BEGIN PERFORM maintenance_assert_job({}); END $job$;",
                job.args()
            ),
        )
    }

    fn begin_guarded(store: Store, repo: &str, operation: &str, guard: String) -> Result<Self> {
        let plan = Self {
            store,
            repo: repo.into(),
            operation: operation.into(),
            pack_key: format!("repos/{repo}/operations/{operation}/output.pack"),
            index_key: format!("repos/{repo}/operations/{operation}/output.idx"),
            guard,
        };
        validate_key(&plan.pack_key)?;
        validate_key(&plan.index_key)?;
        // No retry can overwrite a prior attempt: operation is a fresh identity,
        // and a duplicate/retired intent is rejected before any multipart call.
        metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; {} INSERT INTO staged_objects(repo_id,operation_id,object_key,state) VALUES({}::uuid,{}::uuid,{},'planned'),({}::uuid,{}::uuid,{},'planned'); COMMIT;",
            plan.guard,
            metadata::text(repo),
            metadata::text(operation),
            metadata::text(&plan.pack_key),
            metadata::text(repo),
            metadata::text(operation),
            metadata::text(&plan.index_key)
        ))?;
        Ok(plan)
    }

    pub fn write<P: VerifiedObjectProvider>(
        &self,
        source: &SnapshotSession<P>,
        selection: &ObjectSelection,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<Pack> {
        let mut output =
            Multipart::new(self.store.clone(), &self.pack_key, control.clone(), budget)?;
        let mut manifest = source.write_pack(
            selection,
            &mut output,
            PackWriteLimits {
                max_output_bytes: MAX_BYTES,
            },
        )?;
        let (pack_size, pack_digest) = output.finish()?;
        if manifest.pack_bytes != pack_size {
            return Err(invalid("streamed pack length mismatch"));
        }
        let mut output =
            Multipart::new(self.store.clone(), &self.index_key, control.clone(), budget)?;
        manifest.write_index(&mut output)?;
        let (idx_size, idx_digest) = output.finish()?;
        let pack = Pack {
            hash: manifest.pack_hash.to_string(),
            pack_key: self.pack_key.clone(),
            idx_key: self.index_key.clone(),
            pack_size,
            idx_size,
            pack_digest,
            idx_digest,
        };
        // Re-read every uploaded byte before accepting it as a publication input.
        // A multipart ETag is opaque and is never treated as a content digest.
        self.verify(
            &pack.pack_key,
            pack.pack_size,
            &pack.pack_digest,
            control,
            budget,
        )?;
        self.verify(
            &pack.idx_key,
            pack.idx_size,
            &pack.idx_digest,
            control,
            budget,
        )?;
        Ok(pack)
    }

    fn verify(
        &self,
        key: &str,
        size: u64,
        expected: &str,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<()> {
        let mut buffer = budget.buffer(PART_BYTES)?;
        let mut hash = Sha256::new();
        let mut position = 0;
        while position < size {
            control.check()?;
            let length = (size - position).min(PART_BYTES as u64) as usize;
            buffer.resize_controlled(length, 0, control)?;
            self.store
                .source_range(key, size, position, buffer.as_mut_slice(), control)?;
            for chunk in buffer.as_slice().chunks(32 * 1024) {
                control.check()?;
                hash.update(chunk);
            }
            position += length as u64;
        }
        if crate::protocol::hex(&hash.finish()) != expected {
            return Err(invalid("multipart readback digest mismatch"));
        }
        control.check()?;
        Ok(())
    }

    /// Called by the non-CPU publication owner. Planned evidence is deliberately
    /// retained on failure, including successful PUTs whose response was lost.
    pub fn uploaded(&self) -> Result<()> {
        let result = metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; {} UPDATE staged_objects s SET state='uploaded' WHERE s.repo_id={}::uuid AND s.operation_id={}::uuid AND s.object_key IN ({},{}) AND s.retired_at IS NULL AND NOT EXISTS(SELECT 1 FROM retired_objects t WHERE t.object_key=s.object_key) RETURNING 1; COMMIT;",
            self.guard,
            metadata::text(&self.repo),
            metadata::text(&self.operation),
            metadata::text(&self.pack_key),
            metadata::text(&self.index_key)
        ))?;
        if result.lines().count() != 2 {
            return Err(invalid("multipart intent was retired or replaced"));
        }
        Ok(())
    }
}

struct Multipart {
    buffer: CapacityBuffer,
    parts: Vec<String>,
    store: Store,
    key: String,
    upload: String,
    control: Control,
    hash: Sha256,
    bytes: u64,
    complete: bool,
    _metadata: CapacityReservation,
}

impl Multipart {
    fn new(store: Store, key: &str, control: Control, budget: &CapacityBudget) -> Result<Self> {
        control.check()?;
        let metadata = budget.reservation(METADATA_BYTES)?;
        let buffer = budget.buffer(PART_BYTES)?;
        let mut parts = Vec::new();
        parts.try_reserve_exact(MAX_PARTS)?;
        let mut response = Vec::with_capacity(16384);
        store.request_metadata(
            reqwest::Method::POST,
            &format!("{}/{}/{key}?uploads", store.endpoint, store.bucket),
            None,
            None,
            &mut response,
            &control,
        )?;
        let upload = xml_field(
            &response,
            b"InitiateMultipartUploadResult",
            Some(b"UploadId"),
        )?
        .ok_or_else(|| invalid("multipart response omitted UploadId"))?;
        if upload.is_empty() || upload.len() > 1024 || upload.chars().any(char::is_control) {
            return Err(invalid("multipart upload identity bound exceeded"));
        }
        Ok(Self {
            buffer,
            parts,
            store,
            key: key.into(),
            upload,
            control,
            hash: Sha256::new(),
            bytes: 0,
            complete: false,
            _metadata: metadata,
        })
    }

    fn url(&self, part: Option<usize>) -> Result<String> {
        let mut url = reqwest::Url::parse(&format!(
            "{}/{}/{}",
            self.store.endpoint, self.store.bucket, self.key
        ))?;
        let mut query = url.query_pairs_mut();
        query.append_pair("uploadId", &self.upload);
        if let Some(part) = part {
            query.append_pair("partNumber", &part.to_string());
        }
        drop(query);
        Ok(url.into())
    }

    fn part(&mut self) -> Result<()> {
        if self.buffer.as_slice().is_empty() {
            return Ok(());
        }
        self.control.check()?;
        if self.parts.len() >= MAX_PARTS {
            return Err(invalid("multipart part count limit exceeded"));
        }
        let mut hash = Sha256::new();
        for chunk in self.buffer.as_slice().chunks(32 * 1024) {
            self.control.check()?;
            hash.update(chunk);
        }
        let digest = crate::protocol::hex(&hash.finish());
        let etag = metrics::measure(Metric::Dependency(Dependency::S3Put), || {
            self.store.request_metadata(
                reqwest::Method::PUT,
                &self.url(Some(self.parts.len() + 1))?,
                Some(&digest),
                Some(self.buffer.as_slice()),
                &mut std::io::sink(),
                &self.control,
            )
        })?
        .ok_or_else(|| invalid("multipart part omitted ETag"))?;
        self.parts.push(etag);
        metrics::global().transferred(Direction::Upload, self.buffer.as_slice().len() as u64);
        self.buffer.resize(0, 0)?;
        Ok(())
    }

    fn finish(mut self) -> Result<(u64, String)> {
        self.part()?;
        if self.parts.is_empty() {
            return Err(invalid("empty multipart output"));
        }
        let mut body = String::new();
        body.try_reserve_exact(self.parts.len() * 1024 + 128)?;
        body.push_str("<CompleteMultipartUpload>");
        for (ordinal, etag) in self.parts.iter().enumerate() {
            self.control.check()?;
            use std::fmt::Write;
            write!(
                body,
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                ordinal + 1,
                quick_xml::escape::escape(etag)
            )?;
        }
        body.push_str("</CompleteMultipartUpload>");
        let mut hash = Sha256::new();
        for chunk in body.as_bytes().chunks(32 * 1024) {
            self.control.check()?;
            hash.update(chunk);
        }
        let digest = crate::protocol::hex(&hash.finish());
        let mut response = Vec::with_capacity(16384);
        self.store.request_metadata(
            reqwest::Method::POST,
            &self.url(None)?,
            Some(&digest),
            Some(body.as_bytes()),
            &mut response,
            &self.control,
        )?;
        // S3 can return an Error document after sending HTTP 200.
        xml_field(&response, b"CompleteMultipartUploadResult", None)?;
        self.complete = true;
        self.control.check()?;
        let digest =
            crate::protocol::hex(&std::mem::replace(&mut self.hash, Sha256::new()).finish());
        Ok((self.bytes, digest))
    }
}

impl Write for Multipart {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut run = || -> Result<usize> {
            self.control.check()?;
            let count = bytes
                .len()
                .min(32 * 1024)
                .min(PART_BYTES - self.buffer.as_slice().len());
            if self
                .bytes
                .checked_add(count as u64)
                .is_none_or(|size| size > MAX_BYTES)
            {
                return Err(invalid("multipart output byte limit exceeded"));
            }
            self.buffer
                .extend_controlled(&bytes[..count], PART_BYTES, &self.control)?;
            self.hash.update(&bytes[..count]);
            self.bytes += count as u64;
            if self.buffer.as_slice().len() == PART_BYTES {
                self.part()?;
            }
            Ok(count)
        };
        run().map_err(std::io::Error::other)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.control.check().map_err(std::io::Error::other)
    }
}

impl Drop for Multipart {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        // Abort only this upload identity. A raced/unknown Complete may have
        // created the object, so its durable planned intent remains for fenced GC.
        let cleanup = process::Operation::new(5);
        let control = Control::new(cleanup.deadline());
        let result = cleanup.enter(|| {
            self.store.request_metadata(
                reqwest::Method::DELETE,
                &self.url(None)?,
                None,
                None,
                &mut std::io::sink(),
                &control,
            )
        });
        if result.is_err() {
            // Cleanup must not wait for a diagnostic sink, including library/CLI
            // callers that never initialized service logging. The durable intent
            // is the recovery record; this best-effort event is not that record.
            metrics::service_event(metrics::ServiceEvent::MultipartAbortUnconfirmed);
        }
    }
}

fn xml_field(input: &[u8], root: &[u8], target: Option<&[u8]>) -> Result<Option<String>> {
    let input = super::xml::document(input, root, true)?;
    let mut reader = Reader::from_str(input);
    reader.config_mut().trim_text(true);
    let (mut opened, mut closed, mut found) = (false, false, None);
    loop {
        match reader.read_event()? {
            Event::Decl(_) if !opened => {}
            Event::Start(tag) if !opened && tag.name().as_ref().as_bytes() == root => opened = true,
            Event::Empty(tag) if !opened && tag.name().as_ref().as_bytes() == root => {
                opened = true;
                closed = true;
            }
            Event::Start(tag) if opened && !closed => {
                let name = tag.name();
                let value = reader.read_text(name)?;
                if value.contains('<') {
                    return Err(invalid("unexpected nested S3 XML value"));
                }
                if target.is_some_and(|target| name.as_ref().as_bytes() == target) {
                    if found.is_some() {
                        return Err(invalid("duplicate S3 XML field"));
                    }
                    found = Some(quick_xml::escape::unescape(value.as_ref())?.into_owned());
                }
            }
            Event::Empty(tag)
                if opened
                    && !closed
                    && target.is_none_or(|target| tag.name().as_ref().as_bytes() != target) => {}
            Event::End(tag) if opened && !closed && tag.name().as_ref().as_bytes() == root => {
                closed = true
            }
            Event::Text(value)
                if value
                    .as_ref()
                    .as_bytes()
                    .iter()
                    .all(u8::is_ascii_whitespace) => {}
            Event::Eof if closed => return Ok(found),
            _ => return Err(invalid("invalid or failed S3 multipart XML response")),
        }
    }
}

#[cfg(test)]
mod diagnostics_tests {
    use super::{Store, UploadPlan};
    use crate::{
        Result,
        cpu_jobs::{CpuJob, CpuJobs},
        process::Operation,
    };
    use git_rs_core::{
        CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
        VerifiedObjectProvider,
    };
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        os::{fd::OwnedFd, unix::net::UnixStream},
        process::{Child, Command, Stdio},
        sync::Arc,
        time::{Duration, Instant},
    };

    const CONTENTS: &[u8] = b"synthetic pack body";

    // Faithful immutable in-memory source: real object hashing, selection and
    // pack encoding remain production code. The weak witness observes its hold
    // without keeping the provider alive on behalf of the job.
    struct BlobSource {
        identity: SnapshotIdentity,
        id: gix::ObjectId,
        _hold: Arc<()>,
    }
    impl VerifiedObjectProvider for BlobSource {
        fn identity(&self) -> &SnapshotIdentity {
            &self.identity
        }

        fn header(
            &self,
            id: &gix::hash::oid,
            control: &Control,
        ) -> Result<Option<gix::objs::Header>> {
            control.check()?;
            Ok((id == self.id.as_ref()).then_some(gix::objs::Header {
                kind: gix::objs::Kind::Blob,
                size: CONTENTS.len() as u64,
            }))
        }

        fn object(
            &self,
            id: &gix::hash::oid,
            control: &Control,
            budget: &CapacityBudget,
            limits: ReadLimits,
        ) -> Result<Option<Object>> {
            if self.header(id, control)?.is_none() {
                return Ok(None);
            }
            if CONTENTS.len() > limits.max_object_bytes {
                return Err(crate::invalid("fixture object exceeds read limit"));
            }
            let mut data = budget.buffer(CONTENTS.len())?;
            data.extend_controlled(CONTENTS, CONTENTS.len(), control)?;
            Ok(Some(Object {
                kind: gix::objs::Kind::Blob,
                data,
            }))
        }
    }

    struct FailedPlan {
        plan: UploadPlan,
        source: BlobSource,
    }
    impl CpuJob for FailedPlan {
        type Output = ();
        fn run(self, control: &Control, budget: &CapacityBudget) -> Result<()> {
            let identity = self.source.identity().clone();
            let id = self.source.id;
            let source = SnapshotSession::open(
                self.source,
                &identity,
                control.clone(),
                budget.clone(),
                ReadLimits::default(),
            )?;
            let selection = source.select_objects([id], None, &[])?;
            // Exercise the component's public operation, including real pack
            // writing and its failure cleanup. Never construct Multipart here.
            self.plan.write(&source, &selection, control, budget)?;
            Ok(())
        }
    }

    struct NextJob;
    impl CpuJob for NextJob {
        type Output = usize;
        fn run(self, control: &Control, _: &CapacityBudget) -> Result<usize> {
            control.check()?;
            Ok(42)
        }
    }

    // Start at a faithfully pre-established plan, i.e. the state returned by
    // UploadPlan::begin after its SQL transaction. This fixture does NOT execute
    // or prove that transaction, durable intent retention, SQL publication or GC.
    // It tests UploadPlan::write's public contract without a new production seam.
    #[test]
    fn multipart_diagnostic_fixture() {
        if std::env::var("GIT_RS_MULTIPART_DIAGNOSTIC_FIXTURE").as_deref() != Ok("1") {
            return;
        }
        let logs = if std::env::var("GIT_RS_MULTIPART_SERVICE_LOGS").as_deref() == Ok("1") {
            crate::metrics::init_service_logs().unwrap()
        } else {
            None
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let store = Store {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            bucket: "synthetic".into(),
            region: "us-east-1".into(),
            access: "synthetic".into(),
            secret: "synthetic".into(),
        };
        let repo = "00000000-0000-0000-0000-000000000001";
        let operation = "00000000-0000-0000-0000-000000000002";
        let plan = UploadPlan {
            store,
            repo: repo.into(),
            operation: operation.into(),
            pack_key: format!("repos/{repo}/operations/{operation}/output.pack"),
            index_key: format!("repos/{repo}/operations/{operation}/output.idx"),
            guard: String::new(),
        };
        let hold = Arc::new(());
        let witness = Arc::downgrade(&hold);
        let source = BlobSource {
            identity: SnapshotIdentity {
                source: b"synthetic-multipart-source".to_vec(),
                generation: b"one".to_vec(),
                object_hash: gix::hash::Kind::Sha1,
            },
            id: gix::objs::compute_hash(gix::hash::Kind::Sha1, gix::objs::Kind::Blob, CONTENTS)
                .unwrap(),
            _hold: hold,
        };
        let cpu = Arc::new(CpuJobs::new(1, 0).unwrap());
        let admitted = cpu.try_admit(Operation::new(20)).unwrap().unwrap();
        let budget = admitted.budget().clone();
        std::thread::scope(|scope| {
            let observed_cpu = cpu.clone();
            let observed_budget = budget.clone();
            let observed_source = witness.clone();
            let peer = scope.spawn(move || {
                let mut requests = Vec::new();
                for ordinal in 0..3 {
                    let (stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut stream = BufReader::new(stream);
                    let mut line = String::new();
                    stream.read_line(&mut line).unwrap();
                    requests.push(line.trim().to_owned());
                    assert!(requests[ordinal].contains("/output.pack?"));
                    let mut length = 0;
                    loop {
                        line.clear();
                        assert!(stream.read_line(&mut line).unwrap() > 0);
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':')
                            && name.eq_ignore_ascii_case("content-length")
                        {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                    }
                    assert!(length <= 1024);
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    let (status, response) = if ordinal == 0 {
                        assert!(requests[0].starts_with("POST "));
                        assert!(requests[0].contains("?uploads "));
                        (
                            "200 OK",
                            concat!(
                                "<InitiateMultipartUploadResult>",
                                "<UploadId>owned-upload</UploadId>",
                                "</InitiateMultipartUploadResult>",
                            ),
                        )
                    } else {
                        assert!(requests[ordinal].contains("uploadId=owned-upload"));
                        if ordinal == 1 {
                            assert!(requests[1].starts_with("PUT "));
                            assert!(requests[1].contains("partNumber=1"));
                            assert!(body.len() > 32);
                            assert_eq!(&body[..12], b"PACK\0\0\0\x02\0\0\0\x01");
                        } else {
                            assert!(requests[2].starts_with("DELETE "));
                            assert_eq!(observed_cpu.admitted(), 1);
                            assert!(observed_budget.used() > 0);
                            assert!(
                                observed_source.upgrade().is_some(),
                                "source hold released during abort"
                            );
                            println!("\nabort-ready");
                            std::io::stdout().flush().unwrap();
                            // The parent releases this real fault boundary only
                            // after observing admission held and stderr full.
                            std::io::stdin().read_exact(&mut [0]).unwrap();
                        }
                        ("503 Service Unavailable", "")
                    };
                    write!(
                        stream.get_mut(),
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                        response.len()
                    )
                    .unwrap();
                }
                requests
            });
            let error = match admitted.run(FailedPlan { plan, source }) {
                Err(error) => error,
                Ok(_) => panic!("failed S3 part must not produce a publication input"),
            };
            assert!(error.to_string().contains("HTTP 503"));
            drop(error);
            assert_eq!(cpu.admitted(), 0, "abort diagnostic retained CPU admission");
            assert_eq!(budget.used(), 0, "multipart backing survived cleanup");
            assert!(
                witness.upgrade().is_none(),
                "source hold survived completed cleanup"
            );
            let next = cpu.try_run(Operation::new(5), NextJob).unwrap().unwrap();
            assert_eq!(*next, 42, "the actual CPU slot must accept subsequent work");
            drop(next);
            let requests = peer.join().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(
                requests
                    .iter()
                    .filter(|line| line.starts_with("POST "))
                    .count(),
                1,
                "failed output must never send CompleteMultipartUpload"
            );
        });
        drop(logs);
        // Avoid unrelated libtest final output/diagnostics after our receipt.
        println!("cleanup-complete");
        std::io::stdout().flush().unwrap();
        std::process::exit(0);
    }

    struct Running(Child);
    impl Drop for Running {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn upload_plan_write_failed_abort_diagnostic_releases_source_and_cpu() {
        for service_logs in ["0", "1"] {
            let (undrained, stderr) = rustix::pipe::pipe_with(
                rustix::pipe::PipeFlags::CLOEXEC | rustix::pipe::PipeFlags::NONBLOCK,
            )
            .unwrap();
            for bytes in [&[b'x'; 4096][..], b"x"] {
                loop {
                    match rustix::io::write(&stderr, bytes) {
                        Ok(count) => assert!(count > 0),
                        Err(rustix::io::Errno::AGAIN) => break,
                        Err(error) => panic!("fill multipart diagnostic pipe: {error}"),
                    }
                }
            }
            let flags = rustix::fs::fcntl_getfl(&stderr).unwrap();
            rustix::fs::fcntl_setfl(&stderr, flags & !rustix::fs::OFlags::NONBLOCK).unwrap();
            let (stdout, writer) = UnixStream::pair().unwrap();
            stdout
                .set_read_timeout(Some(Duration::from_secs(8)))
                .unwrap();
            let stdout_fd: OwnedFd = writer.into();
            let mut child = Running(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "blob::multipart::diagnostics_tests::multipart_diagnostic_fixture",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env_clear()
                    .env("GIT_RS_MULTIPART_DIAGNOSTIC_FIXTURE", "1")
                    .env("GIT_RS_MULTIPART_SERVICE_LOGS", service_logs)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::from(stdout_fd))
                    .stderr(Stdio::from(stderr))
                    .spawn()
                    .unwrap(),
            );
            let mut output = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                assert!(output.read_line(&mut line).unwrap() > 0);
                if line.trim() == "abort-ready" {
                    break;
                }
            }
            child.0.stdin.take().unwrap().write_all(b"x").unwrap();
            let mut line = String::new();
            output.read_line(&mut line).unwrap();
            assert_eq!(line.trim(), "cleanup-complete");
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                assert!(Instant::now() < deadline, "multipart fixture did not exit");
                std::thread::yield_now();
            }
            drop(undrained);
        }
    }
}
