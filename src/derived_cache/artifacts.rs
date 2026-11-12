//! Complete, bounded artifacts. A checkpoint is reusable completed work, not a
//! serialized suspended blame traversal or a claim that the core can resume it.
use super::{
    Access, ArtifactKey, BuildLease, Claim, Coordinator, DerivedCache, Payload, config, hash,
};
use crate::{Result, invalid, process};
use std::{sync::Arc, time::Instant};

pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHUNKS: usize = 64;
const HEADER: &[u8] = b"git-rs-complete-artifact\0\x01";
const BODY_AUTH: &[u8] = b"git-rs-body-auth\0\x01";

type BodyVerifier<'a> = &'a dyn Fn(&[u8], &[u8; 32]) -> Result<()>;
type BodySigner<'a> = &'a mut dyn FnMut(&ArtifactKey, &[u8]) -> Result<[u8; 32]>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    CompleteBody,
    CompleteMappingBatch,
    CompleteCheckpoint,
}
impl ArtifactKind {
    fn code(self) -> i32 {
        match self {
            Self::CompleteBody => 1,
            Self::CompleteMappingBatch => 2,
            Self::CompleteCheckpoint => 3,
        }
    }
}

struct ScopedAccess {
    key: ArtifactKey,
    source: Arc<dyn Access>,
}
impl Access for ScopedAccess {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        self.source.check(&self.key)
    }
}

/// Constructed only after complete content verification and read admission.
/// Retaining it retains its charged RAM and cache/source read ownership.
pub struct StagedChunk {
    payload: Payload,
    id: String,
    root: ArtifactKey,
    digest: [u8; 32],
}
impl StagedChunk {
    pub fn len(&self) -> usize {
        self.payload.len()
    }
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

pub struct BlameArtifacts {
    cache: DerivedCache,
}

/// Exact immutable root identity, not a progress counter or source authority.
/// Constructed only from a verified checkpoint or its confirmed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointVersion {
    namespace: String,
    key: ArtifactKey,
    payload: String,
    digest: [u8; 32],
    generation: i64,
}
impl CheckpointVersion {
    pub fn payload_id(&self) -> &str {
        &self.payload
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
    /// Short payload-job generation; not the parent build's attempt generation.
    pub fn generation(&self) -> i64 {
        self.generation
    }
}

/// A complete reusable-unit batch plus its CAS version. This is NOT a serialized
/// pending DAG. Root decodes/copies this batch into its admitted in-memory
/// BatchSnapshot once, outside vendor CPU callbacks. with_chunks fences that
/// import before/after the whole batch; it must not be called per line/unit.
pub struct CompleteCheckpoint {
    artifact: CompleteArtifact,
    version: CheckpointVersion,
}
impl CompleteCheckpoint {
    pub fn version(&self) -> &CheckpointVersion {
        &self.version
    }
    pub fn len(&self) -> usize {
        self.artifact.len()
    }
    pub fn is_empty(&self) -> bool {
        self.artifact.is_empty()
    }
    pub fn with_chunks<T>(&self, consume: impl FnOnce(&[&[u8]]) -> Result<T>) -> Result<T> {
        self.artifact.with_chunks(consume)
    }
}

/// All children are verified before this owner exists. This does not allocate a
/// second concatenated Vec; backing capacity remains charged until the last owner.
/// The cache's RAM limit may reject a large body: callers use their bounded local
/// result instead. Increasing the 256 MiB format limit does not increase RAM budget.
pub struct CompleteArtifact {
    root: Payload,
    chunks: Vec<Payload>,
    size: usize,
    kind: ArtifactKind,
}
impl CompleteArtifact {
    pub fn len(&self) -> usize {
        self.size
    }
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }
    /// Batch-level fencing, never SQL per output line. As for opaque Payload,
    /// the consumer must not expose partial success if its final fence fails.
    pub fn with_chunks<T>(&self, consume: impl FnOnce(&[&[u8]]) -> Result<T>) -> Result<T> {
        self.root.with_bytes(|_| {
            let slices: Vec<_> = self
                .chunks
                .iter()
                .map(|chunk| chunk.data.bytes.as_slice())
                .collect();
            consume(&slices)
        })
    }
}

impl BlameArtifacts {
    pub fn new(cache: &DerivedCache) -> Self {
        Self {
            cache: cache.clone(),
        }
    }

    /// Cache errors/pending claims are optional; callers retain their own complete
    /// locally admitted output and may skip publication instead of waiting forever.
    /// Requires an entered bounded Operation (normally run_with_renewal). Rejects
    /// its absence before any cache I/O; never widens a caller's narrower deadline.
    pub fn stage_chunk(&self, build: &BuildLease, bytes: &[u8]) -> Result<Option<StagedChunk>> {
        process::remaining()?;
        build.check_cache(&self.cache)?;
        build.check_authority()?;
        if bytes.is_empty() || bytes.len() > MAX_CHUNK_BYTES {
            return Err(invalid("invalid complete-artifact chunk size"));
        }
        let digest = hash(bytes)?;
        let key = build.key.body_chunk(&digest);
        let access: Arc<dyn Access> = Arc::new(ScopedAccess {
            key: build.key.clone(),
            source: build.access.clone(),
        });
        let mut payload = if let Some(payload) = self.cache.get_verified(&key, access.clone())? {
            payload
        } else {
            let Some(claim) = build.claim_payload(&self.cache, &key)? else {
                return Ok(None);
            };
            let published = Coordinator::with_renewal(&self.cache, &claim, || {
                build.check_authority()?;
                self.cache.publish(&claim, access.as_ref(), bytes)
            })?;
            if !published {
                return Ok(None);
            }
            let Some(payload) = self.cache.get_verified(&key, access)? else {
                return Ok(None);
            };
            payload
        };
        payload.with_bytes(|found| {
            if found.len() != bytes.len() || hash(found)? != digest {
                return Err(invalid("content-addressed chunk mismatch"));
            }
            Ok(())
        })?;
        let id = payload.identity().expect("verified chunk hold").to_owned();
        let c = &self.cache.inner.config;
        let rows = self.cache.inner.pg.query(
            "SELECT git_rs_cache.pin_build_chunk($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6::text::uuid,$7,$8)",
            &[&c.namespace, &c.owner, &&build.key.hash[..], &build.token, &build.generation,
                &id, &&digest[..], &(bytes.len() as i64)], 1, 64)?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Ok(None);
        }
        // The durable build pin replaces this transient per-chunk read hold.
        // Batched owners must not accumulate 64 synchronous cleanup round trips
        // when their final consumer is cancelled or the cache is unavailable.
        drop(payload.hold.take());
        Ok(Some(StagedChunk {
            payload,
            id,
            root: build.key.clone(),
            digest,
        }))
    }

    /// One atomic ready descriptor, after every complete child is verified. Flat
    /// manifests have at most 64 children; at 4 MiB each this covers 256 MiB without
    /// unbounded graph traversal or recursively trusted manifest structures.
    /// Requires the caller's bounded Operation, as stage_chunk does.
    pub fn publish(
        &self,
        build: &BuildLease,
        kind: ArtifactKind,
        chunks: &[StagedChunk],
    ) -> Result<bool> {
        self.publish_complete(build, kind, chunks, None)
    }

    /// Root-only signing boundary: sign a descriptor made from the caller's
    /// locally derived complete chunks. No cache-loaded descriptor is re-signed.
    /// The signer must bind the complete ArtifactKey and independent writer key.
    pub(crate) fn publish_body_authenticated(
        &self,
        build: &BuildLease,
        chunks: &[StagedChunk],
        mut sign: impl FnMut(&ArtifactKey, &[u8]) -> Result<[u8; 32]>,
    ) -> Result<bool> {
        self.publish_complete(build, ArtifactKind::CompleteBody, chunks, Some(&mut sign))
    }

    fn publish_complete(
        &self,
        build: &BuildLease,
        kind: ArtifactKind,
        chunks: &[StagedChunk],
        sign: Option<BodySigner<'_>>,
    ) -> Result<bool> {
        process::remaining()?;
        build.check_cache(&self.cache)?;
        build.check_authority()?;
        let mut bytes = encode(build, kind, chunks)?;
        if let Some(sign) = sign {
            let tag = sign(&build.key, &bytes)?;
            bytes.extend_from_slice(BODY_AUTH);
            bytes.extend_from_slice(&tag);
        }
        let Some(claim) = build.claim_payload(&self.cache, &build.key)? else {
            return Ok(false);
        };
        let manifest = ManifestCommit {
            build,
            kind,
            chunks,
            checkpoint_payload: None,
        };
        Coordinator::with_renewal(&self.cache, &claim, || {
            self.cache
                .publish_inner(&claim, build.access.as_ref(), &bytes, Some(&manifest))
        })
    }

    /// Explicit copy-on-write checkpoint publication. expected=None means no
    /// live checkpoint; a replacement requires the exact verified version.
    /// CompleteBody and normal publish remain immutable. Only complete serialized
    /// units belong in chunks; root owns their typed codec/producer/full-input key.
    /// Success consumes the committed children's pins AFTER new root/edges protect
    /// them; omitted pending units stay pinned. Re-stage committed units before
    /// using them in a subsequent snapshot. Retain the
    /// same finite BuildLease across all rolls; never claim fresh CPU time.
    pub fn replace_checkpoint(
        &self,
        build: &BuildLease,
        expected: Option<&CheckpointVersion>,
        chunks: &[StagedChunk],
    ) -> Result<Option<CheckpointVersion>> {
        process::remaining()?;
        build.check_cache(&self.cache)?;
        build.check_authority()?;
        if expected.is_some_and(|version| {
            version.namespace != self.cache.inner.config.namespace || version.key != build.key
        }) {
            return Err(invalid("checkpoint version belongs to another scope"));
        }
        let bytes = encode(build, ArtifactKind::CompleteCheckpoint, chunks)?;
        let Some(claim) = build.claim_checkpoint(
            &self.cache,
            expected.map(|version| (version.payload.as_str(), version.digest.as_slice())),
        )?
        else {
            return Ok(None);
        };
        let payload = config::identifier()?;
        let manifest = ManifestCommit {
            build,
            kind: ArtifactKind::CompleteCheckpoint,
            chunks,
            checkpoint_payload: Some(&payload),
        };
        let published = Coordinator::with_renewal(&self.cache, &claim, || {
            self.cache
                .publish_inner(&claim, build.access.as_ref(), &bytes, Some(&manifest))
        })?;
        if !published {
            return Ok(None);
        }
        // Never look up "the latest" here: another roll may already exist.
        // Unknown COMMIT confirmation in publish_inner binds this exact fresh
        // payload and digest, not merely the directory key or expected old root.
        build.access.check(&build.key)?;
        process::check_operation()?;
        Ok(Some(CheckpointVersion {
            namespace: self.cache.inner.config.namespace.clone(),
            key: build.key.clone(),
            payload,
            digest: hash(&bytes)?,
            generation: claim.generation,
        }))
    }

    pub fn get_checkpoint_verified(
        &self,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
    ) -> Result<Option<CompleteCheckpoint>> {
        let Some(artifact) = self.get_verified(key, ArtifactKind::CompleteCheckpoint, access)?
        else {
            return Ok(None);
        };
        let payload = artifact
            .root
            .identity()
            .ok_or_else(|| invalid("checkpoint has no root hold"))?
            .to_owned();
        let digest = artifact.root.with_bytes(hash)?;
        let c = &self.cache.inner.config;
        let rows = self.cache.inner.pg.query(
            "SELECT generation FROM git_rs_cache.payloads WHERE namespace=$1::text::uuid AND id=$2::text::uuid AND key_hash=$3 AND digest=$4",
            &[&c.namespace, &payload, &&key.hash[..], &&digest[..]], 1, 64)?;
        let generation = rows
            .first()
            .ok_or_else(|| invalid("checkpoint identity absent"))?
            .try_get(0)?;
        artifact.root.with_bytes(|_| Ok(()))?;
        Ok(Some(CompleteCheckpoint {
            version: CheckpointVersion {
                namespace: c.namespace.clone(),
                key: key.clone(),
                payload,
                digest,
                generation,
            },
            artifact,
        }))
    }

    pub fn get_verified(
        &self,
        key: &ArtifactKey,
        kind: ArtifactKind,
        access: Arc<dyn Access>,
    ) -> Result<Option<CompleteArtifact>> {
        self.get_complete(key, kind, access, None)
    }

    /// An authenticated reader never falls back to accepting an unsigned body.
    /// Verification finishes inside root read/source fences, before any child
    /// bytes or a ready-body owner can escape. Failure is an exact-root miss.
    pub(crate) fn get_body_authenticated(
        &self,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
        verify: impl Fn(&[u8], &[u8; 32]) -> Result<()>,
    ) -> Result<Option<CompleteArtifact>> {
        self.get_complete(key, ArtifactKind::CompleteBody, access, Some(&verify))
    }

    fn get_complete(
        &self,
        key: &ArtifactKey,
        kind: ArtifactKind,
        access: Arc<dyn Access>,
        verify: Option<BodyVerifier<'_>>,
    ) -> Result<Option<CompleteArtifact>> {
        let Some(root) = self.cache.get_verified(key, access.clone())? else {
            return Ok(None);
        };
        let other_kind = root.with_bytes(|bytes| {
            Ok(bytes.starts_with(HEADER)
                && bytes
                    .get(HEADER.len())
                    .is_some_and(|code| (1..=3).contains(code) && *code != kind.code() as u8))
        })?;
        if other_kind {
            return Ok(None);
        }
        let load = || -> Result<CompleteArtifact> {
            let specs = root.with_bytes(|bytes| {
                let (manifest, tag) = manifest_bytes(bytes, kind)?;
                if let Some(verify) = verify {
                    verify(
                        manifest,
                        tag.ok_or_else(|| invalid("unsigned complete body"))?,
                    )?;
                }
                decode(manifest, kind)
            })?;
            let mut chunks = Vec::with_capacity(specs.len());
            let mut total = 0;
            let root_hold = root
                .hold
                .as_ref()
                .ok_or_else(|| invalid("manifest has no read hold"))?;
            let c = &self.cache.inner.config;
            let expected_total = specs.iter().map(|(size, _)| *size as i64).sum::<i64>();
            let rows = self.cache.inner.pg.query(
                "SELECT git_rs_cache.check_manifest($1::text::uuid,$2::text::uuid,$3::text::uuid,$4::text::uuid,$5,$6,$7)",
                &[&c.namespace, &c.owner, &root_hold.payload, &root_hold.reader, &kind.code(), &(specs.len() as i32), &expected_total], 1, 64)?;
            if !rows[0].try_get::<_, bool>(0)? {
                return Err(invalid("complete manifest descriptor absent or expired"));
            }
            for (ordinal, (size, digest)) in specs.iter().enumerate() {
                process::check_operation()?;
                let reader = config::identifier()?;
                let started = Instant::now();
                let rows = self.cache.inner.pg.query(
                    "SELECT * FROM git_rs_cache.acquire_manifest_child($1::text::uuid,$2::text::uuid,$3::text::uuid,$4::text::uuid,$5,$6::text::uuid,$7,$8,$9,$10)",
                    &[&c.namespace, &c.owner, &root_hold.payload, &root_hold.reader, &(ordinal as i32), &reader,
                        &c.read_seconds, &(c.max_payload_bytes as i64), &(*size as i64), &&digest[..]], 1, 1024)?;
                let child_key = key.body_chunk(digest);
                let child_access: Arc<dyn Access> = Arc::new(ScopedAccess {
                    key: key.clone(),
                    source: access.clone(),
                });
                let mut chunk = self
                    .cache
                    .load_read(&child_key, child_access, reader, started, rows)?
                    .ok_or_else(|| invalid("manifest child absent, retired or expired"))?;
                chunk.with_bytes(|bytes| {
                    if bytes.len() != *size || hash(bytes)? != *digest {
                        return Err(invalid("manifest child integrity mismatch"));
                    }
                    Ok(())
                })?;
                // The admitted root hold retains every referenced child, even
                // if the root retires mid-read. Keep one hold for the complete
                // result, not a destructor performing SQL for every chunk.
                drop(chunk.hold.take());
                total += *size;
                chunks.push(chunk);
            }
            root.with_bytes(|_| Ok(()))?;
            // Move root only after the borrowing load closure has finished.
            Ok(CompleteArtifact {
                root: Payload {
                    data: root.data.clone(),
                    hold: root.hold.clone(),
                    access: access.clone(),
                    key: key.clone(),
                },
                chunks,
                size: total,
                kind,
            })
        };
        match load() {
            Ok(artifact) => Ok(Some(artifact)),
            Err(_) => {
                process::check_operation()?;
                access.check(key)?;
                if let Some(id) = root.identity() {
                    let _ = self.cache.invalidate(key, id);
                }
                Ok(None)
            }
        }
    }
}

fn encode(build: &BuildLease, kind: ArtifactKind, chunks: &[StagedChunk]) -> Result<Vec<u8>> {
    if chunks.len() > MAX_CHUNKS {
        return Err(invalid("complete-artifact chunk count exceeded"));
    }
    let mut bytes = HEADER.to_vec();
    bytes.push(kind.code() as u8);
    bytes.extend_from_slice(&(chunks.len() as u32).to_be_bytes());
    let mut size = 0usize;
    for chunk in chunks {
        if chunk.root != build.key {
            return Err(invalid("chunk belongs to another artifact scope"));
        }
        if hash(&chunk.payload.data.bytes)? != chunk.digest {
            return Err(invalid("complete-artifact chunk mismatch"));
        }
        size = size
            .checked_add(chunk.len())
            .ok_or_else(|| invalid("artifact size overflow"))?;
        bytes.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&chunk.digest);
    }
    if size > MAX_BODY_BYTES {
        return Err(invalid("complete-artifact body limit exceeded"));
    }
    Ok(bytes)
}

fn manifest_bytes(bytes: &[u8], kind: ArtifactKind) -> Result<(&[u8], Option<&[u8; 32]>)> {
    let header = HEADER.len() + 5;
    if bytes.len() < header
        || !bytes.starts_with(HEADER)
        || bytes[HEADER.len()] != kind.code() as u8
    {
        return Err(invalid("unsupported complete-artifact manifest"));
    }
    let count = u32::from_be_bytes(bytes[HEADER.len() + 1..header].try_into()?) as usize;
    if count > MAX_CHUNKS {
        return Err(invalid("invalid complete-artifact manifest length"));
    }
    let length = header + count * 36;
    if bytes.len() == length {
        return Ok((bytes, None));
    }
    if kind == ArtifactKind::CompleteBody
        && bytes.len() == length + BODY_AUTH.len() + 32
        && &bytes[length..length + BODY_AUTH.len()] == BODY_AUTH
    {
        return Ok((
            &bytes[..length],
            Some(bytes[length + BODY_AUTH.len()..].try_into()?),
        ));
    }
    Err(invalid("invalid complete-artifact authentication envelope"))
}

fn decode(bytes: &[u8], kind: ArtifactKind) -> Result<Vec<(usize, [u8; 32])>> {
    let (bytes, _) = manifest_bytes(bytes, kind)?;
    let header = HEADER.len() + 5;
    let count = u32::from_be_bytes(bytes[HEADER.len() + 1..header].try_into()?) as usize;
    let mut specs = Vec::with_capacity(count);
    let mut total = 0usize;
    // The exact manifest length above already excludes a trailing partial entry.
    let (entries, _) = bytes[header..].as_chunks::<36>();
    for entry in entries {
        let size = u32::from_be_bytes(entry[..4].try_into()?) as usize;
        if size == 0 || size > MAX_CHUNK_BYTES {
            return Err(invalid("invalid manifest chunk size"));
        }
        total += size;
        specs.push((size, entry[4..].try_into()?));
    }
    if total > MAX_BODY_BYTES {
        return Err(invalid("manifest body limit exceeded"));
    }
    Ok(specs)
}

pub(super) struct ManifestCommit<'a> {
    build: &'a BuildLease,
    kind: ArtifactKind,
    chunks: &'a [StagedChunk],
    pub(super) checkpoint_payload: Option<&'a str>,
}
impl ManifestCommit<'_> {
    pub(super) fn commit(
        &self,
        cache: &DerivedCache,
        claim: &Claim,
        id: &str,
        inline: Option<&[u8]>,
    ) -> Result<Vec<tokio_postgres::Row>> {
        let c = &cache.inner.config;
        let ids: Vec<_> = self.chunks.iter().map(|chunk| chunk.id.clone()).collect();
        let digests: Vec<_> = self
            .chunks
            .iter()
            .map(|chunk| chunk.digest.to_vec())
            .collect();
        let sizes: Vec<_> = self.chunks.iter().map(|chunk| chunk.len() as i64).collect();
        let sql = if self.checkpoint_payload.is_some() {
            "SELECT git_rs_cache.publish_checkpoint($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6::text::uuid,$7,$8,$9,$10::text::uuid,$11,$12::text[],$13::bytea[],$14::bigint[],$15)"
        } else {
            "SELECT git_rs_cache.publish_manifest($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6::text::uuid,$7,$8,$9,$10::text::uuid,$11,$12::text[],$13::bytea[],$14::bigint[],$15)"
        };
        cache.inner.pg.query(
            sql,
            &[
                &c.namespace,
                &c.owner,
                &&claim.key.hash[..],
                &claim.token,
                &claim.generation,
                &id,
                &inline,
                &c.ttl_seconds,
                &&self.build.key.hash[..],
                &self.build.token,
                &self.build.generation,
                &ids,
                &digests,
                &sizes,
                &self.kind.code(),
            ],
            1,
            64,
        )
    }
}
