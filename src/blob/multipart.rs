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
            eprintln!("multipart abort unconfirmed; durable intent retained");
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
