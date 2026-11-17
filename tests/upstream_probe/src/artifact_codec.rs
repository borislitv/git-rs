#![forbid(unsafe_code)]

//! Physical codec proof only. The byte/step caps do not bound native workspace
//! or replace job admission and a worker deadline.

use std::borrow::Cow;

use sha2::{Digest, Sha256};
use zstd::zstd_safe::{self, CCtx, CParameter, DCtx, DParameter, InBuffer, OutBuffer};

pub const MAX_BYTES: usize = 8 * 1024 * 1024;
const STEP: usize = 64 * 1024;
const MIN_COMPRESS_BYTES: usize = 4096;
const MIN_SAVED_BYTES: usize = 128;
const LEVEL: i32 = 3;
const WINDOW_LOG: u32 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    RawV1,
    ZstdV1,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub stored: usize,
    pub logical: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    pub encoding: Encoding,
    pub stored_size: usize,
    pub logical_size: usize,
    pub stored_digest: [u8; 32],
    pub logical_digest: [u8; 32],
}

#[derive(Debug)]
pub struct Encoded<'a> {
    pub metadata: Metadata,
    pub stored: Cow<'a, [u8]>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum ReadPolicy {
    #[default]
    StrictV1,
    /// Qualification only: validate one complete ordinary frame, exact size and
    /// both digests. Cannot require a checksum flag or independently validate
    /// the declared window. WindowLogMax is not a strict header check.
    ProbeSingleFrame,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    StoredLimit,
    LogicalLimit,
    StoredIntegrity,
    LogicalIntegrity,
    RawMetadata,
    StrictHeaderUnavailable,
    FrameHeader,
    TrailingInput,
    DecodedSize,
    ExtraOutput,
    Truncated,
    NoProgress,
    Allocation,
    Zstd(usize),
}

pub fn encode(logical: &[u8], limits: Limits) -> Result<Encoded<'_>, Error> {
    if logical.len() > limits.logical.min(MAX_BYTES) {
        return Err(Error::LogicalLimit);
    }
    let logical_digest = digest(logical);
    let stored_limit = limits.stored.min(MAX_BYTES);
    let compressed = if logical.len() >= MIN_COMPRESS_BYTES {
        let saved = MIN_SAVED_BYTES.max(logical.len().div_ceil(16));
        compress(logical, stored_limit.min(logical.len() - saved))?
    } else {
        None
    };
    let (encoding, stored, stored_digest) = match compressed {
        Some(frame) => {
            let stored_digest = digest(&frame);
            (Encoding::ZstdV1, Cow::Owned(frame), stored_digest)
        }
        None if logical.len() <= stored_limit => {
            (Encoding::RawV1, Cow::Borrowed(logical), logical_digest)
        }
        None => return Err(Error::StoredLimit),
    };
    Ok(Encoded {
        metadata: Metadata {
            encoding,
            stored_size: stored.len(),
            logical_size: logical.len(),
            stored_digest,
            logical_digest,
        },
        stored,
    })
}

pub fn decode(
    stored: &[u8],
    metadata: &Metadata,
    limits: Limits,
    policy: ReadPolicy,
) -> Result<Vec<u8>, Error> {
    if metadata.stored_size > limits.stored.min(MAX_BYTES) {
        return Err(Error::StoredLimit);
    }
    if metadata.logical_size > limits.logical.min(MAX_BYTES) {
        return Err(Error::LogicalLimit);
    }
    if metadata.encoding == Encoding::RawV1
        && (metadata.stored_size != metadata.logical_size
            || metadata.stored_digest != metadata.logical_digest)
    {
        return Err(Error::RawMetadata);
    }
    if stored.len() != metadata.stored_size || digest(stored) != metadata.stored_digest {
        return Err(Error::StoredIntegrity);
    }

    let logical = match (metadata.encoding, policy) {
        (Encoding::RawV1, _) => {
            let mut logical = buffer(stored.len())?;
            for (dst, src) in logical.chunks_mut(STEP).zip(stored.chunks(STEP)) {
                dst.copy_from_slice(src);
            }
            logical
        }
        (Encoding::ZstdV1, ReadPolicy::StrictV1) => return Err(Error::StrictHeaderUnavailable),
        (Encoding::ZstdV1, ReadPolicy::ProbeSingleFrame) => {
            decompress(stored, metadata.logical_size)?
        }
    };
    if digest(&logical) != metadata.logical_digest {
        return Err(Error::LogicalIntegrity);
    }
    Ok(logical)
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut sha = Sha256::new();
    for piece in bytes.chunks(STEP) {
        sha.update(piece);
    }
    sha.finalize().into()
}

fn buffer(size: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    bytes.resize(size, 0);
    Ok(bytes)
}

fn compress(logical: &[u8], cap: usize) -> Result<Option<Vec<u8>>, Error> {
    if cap == 0 {
        return Ok(None);
    }
    let mut stored = buffer(cap)?;
    let mut ctx = CCtx::try_create().ok_or(Error::Allocation)?;
    for parameter in [
        CParameter::CompressionLevel(LEVEL),
        CParameter::WindowLog(WINDOW_LOG),
        CParameter::NbWorkers(0),
        CParameter::EnableLongDistanceMatching(false),
        CParameter::ChecksumFlag(true),
        CParameter::ContentSizeFlag(true),
    ] {
        ctx.set_parameter(parameter).map_err(Error::Zstd)?;
    }
    ctx.set_pledged_src_size(Some(logical.len() as u64))
        .map_err(Error::Zstd)?;

    let (mut read, mut written) = (0, 0);
    while written < cap {
        let mut output = OutBuffer::around(&mut stored[written..(written + STEP).min(cap)]);
        if read == logical.len() {
            let remaining = ctx.end_stream(&mut output).map_err(Error::Zstd)?;
            let produced = output.pos();
            written += produced;
            if remaining == 0 {
                stored.truncate(written);
                return Ok(Some(stored));
            }
            if produced == 0 {
                return Err(Error::NoProgress);
            }
        } else {
            let mut input = InBuffer::around(&logical[read..(read + STEP).min(logical.len())]);
            ctx.compress_stream(&mut output, &mut input)
                .map_err(Error::Zstd)?;
            if input.pos == 0 && output.pos() == 0 {
                return Err(Error::NoProgress);
            }
            read += input.pos;
            written += output.pos();
        }
    }
    Ok(None)
}

fn decompress(stored: &[u8], size: usize) -> Result<Vec<u8>, Error> {
    // Metadata selected zstd-v1 already. The fixed magic discriminator rejects
    // skippable frames even at size zero; field interpretation stays upstream.
    if !stored.starts_with(&zstd_safe::MAGICNUMBER.to_le_bytes())
        || zstd_safe::get_frame_content_size(stored).ok() != Some(Some(size as u64))
        || zstd_safe::get_dict_id_from_frame(stored).is_some()
    {
        return Err(Error::FrameHeader);
    }
    let mut logical = buffer(size)?;
    let mut ctx = DCtx::try_create().ok_or(Error::Allocation)?;
    ctx.set_parameter(DParameter::WindowLogMax(WINDOW_LOG))
        .map_err(Error::Zstd)?;
    let (mut read, mut written) = (0, 0);
    let mut overflow = [0];
    loop {
        let mut input = InBuffer::around(&stored[read..(read + STEP).min(stored.len())]);
        let at_limit = written == size;
        // At the exact cap, let zstd consume a footer without accepting another
        // output byte. A zero-length slice would not distinguish the two.
        let dst = if at_limit {
            &mut overflow[..]
        } else {
            &mut logical[written..(written + STEP).min(size)]
        };
        let mut output = OutBuffer::around(dst);
        let remaining = ctx
            .decompress_stream(&mut output, &mut input)
            .map_err(Error::Zstd)?;
        let produced = output.pos();
        if at_limit && produced != 0 {
            return Err(Error::ExtraOutput);
        }
        read += input.pos;
        written += produced;
        if remaining == 0 {
            if read != stored.len() {
                return Err(Error::TrailingInput);
            }
            if written != size {
                return Err(Error::DecodedSize);
            }
            return Ok(logical);
        }
        if input.pos == 0 && produced == 0 {
            return Err(if read == stored.len() {
                Error::Truncated
            } else {
                Error::NoProgress
            });
        }
    }
}
