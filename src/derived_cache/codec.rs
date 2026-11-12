//! Physical cache encoding only. Keys, descriptors and returned bytes stay logical.
//! Native work uses the owned dependency's safe estimator/header/streaming APIs.
//! Estimates are admission budgets, not allocator-enforced or process RSS limits.
use super::{
    hash,
    payload::{Budget, OwnedBytes},
};
use crate::{Result, invalid, process};
use std::{error::Error, fmt, sync::Arc};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MIN_COMPRESS_BYTES: usize = 4096;
const MIN_SAVED_BYTES: usize = 128;
pub(super) const STEP_BYTES: usize = 65536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Encoding {
    Raw,
    Zstd,
}
impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw-v1",
            Self::Zstd => "zstd-v1",
        }
    }
}

#[derive(Debug)]
struct MetadataError(&'static str);
impl fmt::Display for MetadataError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "invalid cache encoding metadata: {}", self.0)
    }
}
impl Error for MetadataError {}
fn metadata_error(message: &'static str) -> Box<dyn Error + Send + Sync> {
    Box::new(MetadataError(message))
}

pub(super) struct Metadata {
    pub encoding: Encoding,
    pub logical_size: usize,
    pub logical_digest: [u8; 32],
    pub stored_size: usize,
    pub stored_digest: [u8; 32],
}
impl Metadata {
    /// v4 SQL resolves nullable legacy raw physical fields before returning them.
    /// Validate again before allocating: database metadata is not source authority.
    pub fn parse(
        encoding: &str,
        logical_size: i64,
        logical_digest: &[u8],
        stored_size: i64,
        stored_digest: &[u8],
        limit: usize,
    ) -> Result<Self> {
        let encoding = match encoding {
            "raw-v1" => Encoding::Raw,
            "zstd-v1" => Encoding::Zstd,
            _ => return Err(metadata_error("unsupported encoding")),
        };
        let size = |value: i64| -> Result<usize> {
            let size = usize::try_from(value).map_err(|_| metadata_error("negative size"))?;
            if size > limit || size > MAX_BYTES {
                return Err(metadata_error("size limit exceeded"));
            }
            Ok(size)
        };
        let metadata = Self {
            encoding,
            logical_size: size(logical_size)?,
            logical_digest: logical_digest
                .try_into()
                .map_err(|_| metadata_error("logical digest length"))?,
            stored_size: size(stored_size)?,
            stored_digest: stored_digest
                .try_into()
                .map_err(|_| metadata_error("stored digest length"))?,
        };
        if encoding == Encoding::Raw
            && (metadata.logical_size != metadata.stored_size
                || metadata.logical_digest != metadata.stored_digest)
        {
            return Err(metadata_error("raw physical identity differs"));
        }
        if encoding == Encoding::Zstd && metadata.stored_size == 0 {
            return Err(metadata_error("empty compressed frame"));
        }
        Ok(metadata)
    }

    pub fn verify_stored(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() != self.stored_size || hash(bytes)? != self.stored_digest {
            return Err(invalid("cache stored payload integrity mismatch"));
        }
        Ok(())
    }

    pub fn verify_logical(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() != self.logical_size || hash(bytes)? != self.logical_digest {
            return Err(invalid("cache logical payload integrity mismatch"));
        }
        Ok(())
    }
}

enum PhysicalBytes<'a> {
    Raw(&'a [u8]),
    Compressed(OwnedBytes),
}
pub(super) struct EncodedPayload<'a> {
    pub metadata: Metadata,
    bytes: PhysicalBytes<'a>,
}
impl EncodedPayload<'_> {
    pub fn bytes(&self) -> &[u8] {
        match &self.bytes {
            PhysicalBytes::Raw(bytes) => bytes,
            PhysicalBytes::Compressed(owner) => &owner.bytes,
        }
    }
}

pub(super) fn encode<'a>(
    bytes: &'a [u8],
    budget: &Arc<Budget>,
    limit: usize,
) -> Result<EncodedPayload<'a>> {
    process::check_operation()?;
    if bytes.len() > limit || bytes.len() > MAX_BYTES {
        return Err(invalid("artifact payload limit exceeded"));
    }
    let digest = hash(bytes)?;
    let mut encoded = EncodedPayload {
        metadata: Metadata {
            encoding: Encoding::Raw,
            logical_size: bytes.len(),
            logical_digest: digest,
            stored_size: bytes.len(),
            stored_digest: digest,
        },
        bytes: PhysicalBytes::Raw(bytes),
    };
    if bytes.len() >= MIN_COMPRESS_BYTES {
        let saved = MIN_SAVED_BYTES.max(bytes.len().div_ceil(16));
        if let Some(owner) = native::encode(bytes, bytes.len() - saved, budget)? {
            encoded.metadata.encoding = Encoding::Zstd;
            encoded.metadata.stored_size = owner.bytes.len();
            encoded.metadata.stored_digest = hash(&owner.bytes)?;
            encoded.bytes = PhysicalBytes::Compressed(owner);
        }
    }
    process::check_operation()?;
    Ok(encoded)
}

/// PG passes a borrowed binary Row slice; no temporary compressed Vec is needed.
pub(super) fn decode(
    bytes: &[u8],
    metadata: &Metadata,
    budget: &Arc<Budget>,
) -> Result<OwnedBytes> {
    if metadata.logical_size > MAX_BYTES || metadata.stored_size > MAX_BYTES {
        return Err(metadata_error("size limit exceeded"));
    }
    metadata.verify_stored(bytes)?;
    let owner = match metadata.encoding {
        Encoding::Raw => {
            let mut owner = OwnedBytes::allocate(metadata.logical_size, budget)?;
            for (source, target) in bytes
                .chunks(STEP_BYTES)
                .zip(owner.bytes.chunks_mut(STEP_BYTES))
            {
                process::check_operation()?;
                target.copy_from_slice(source);
            }
            owner
        }
        Encoding::Zstd => native::decode(bytes, metadata.logical_size, budget)?,
    };
    metadata.verify_logical(&owner.bytes)?;
    process::check_operation()?;
    Ok(owner)
}

mod native {
    use super::*;
    use zstd::zstd_safe::{
        self, CCtx, CParameter, DCtx, DParameter, FrameHeaderStatus, FrameType, InBuffer, OutBuffer,
    };

    const LEVEL: i32 = 3;
    const WINDOW_LOG: u32 = 20;
    const WINDOW_BYTES: usize = 1 << WINDOW_LOG;
    // A fresh single-threaded context has a small number of native allocations
    // (context, workspace and streaming buffers), not one per input block. Admit
    // 16 conventional 4 KiB pages for allocator rounding/metadata, Rust context
    // state, counters and the overflow byte. This is accounting headroom, not a
    // bound on allocator arenas, retained pages or RSS. No contexts are pooled.
    const ACCOUNTING_HEADROOM: usize = 64 * 1024;

    fn native_result(value: zstd_safe::SafeResult) -> Result<usize> {
        value.map_err(|code| invalid(&format!("cache zstd: {}", zstd_safe::get_error_name(code))))
    }

    fn charge(estimate: usize) -> Result<usize> {
        estimate
            .checked_add(ACCOUNTING_HEADROOM)
            .ok_or_else(|| invalid("cache zstd native admission overflow"))
    }

    fn check_context(actual: usize, estimate: usize) -> Result<()> {
        if actual > estimate {
            return Err(invalid(
                "cache zstd context exceeded admitted native estimate",
            ));
        }
        process::check_operation()
    }

    // Strict framing applies only after explicit zstd-v1 metadata dispatch.
    // Header parsing allocates no native context and examines at most 18 bytes.
    fn check_header(bytes: &[u8], logical_size: usize) -> Result<()> {
        process::check_operation()?;
        let prefix_size = usize::try_from(zstd_safe::FRAMEHEADERSIZE_MAX)?;
        let header = match zstd_safe::get_frame_header(&bytes[..bytes.len().min(prefix_size)]) {
            Ok(FrameHeaderStatus::Complete(header)) => header,
            Ok(FrameHeaderStatus::Incomplete { .. }) => {
                return Err(invalid("cache zstd truncated frame header"));
            }
            Err(code) => {
                return Err(invalid(&format!(
                    "cache zstd frame header: {}",
                    zstd_safe::get_error_name(code),
                )));
            }
        };
        if header.frame_type != FrameType::Zstandard
            || header.content_size != Some(u64::try_from(logical_size)?)
            || header.window_size > u64::try_from(WINDOW_BYTES)?
            || header.dict_id.is_some()
            || !header.checksum
        {
            return Err(invalid("cache zstd unsupported frame profile"));
        }
        Ok(())
    }

    pub(super) fn encode(
        bytes: &[u8],
        stored_limit: usize,
        budget: &Arc<Budget>,
    ) -> Result<Option<OwnedBytes>> {
        process::check_operation()?;
        // The pinned native level-3 large-source preset is dfast with window 21.
        // We only cap that window at 20, leaving match tables/strategy/LDM at
        // their defaults. Its small-source presets have no larger match tables
        // and the same sequence-space divisor. The documented unknown-source
        // stream estimate is therefore the conservative upper budget for this
        // specific profile, not permission for arbitrary parameter overrides.
        // Requalify this relation if the native preset table or profile changes.
        let estimate = native_result(zstd_safe::estimate_cstream_size(LEVEL))?;
        let _native = budget.reserve(charge(estimate)?)?;
        let mut candidate = OwnedBytes::allocate(stored_limit, budget)?;
        // Declaration/drop order matters: context and candidate die before the
        // native admission is refunded, including on errors/cancellation.
        let mut context = CCtx::try_create()
            .ok_or_else(|| invalid("cache zstd compression context allocation failed"))?;
        check_context(context.sizeof(), estimate)?;
        for parameter in [
            CParameter::CompressionLevel(LEVEL),
            CParameter::WindowLog(WINDOW_LOG),
            CParameter::NbWorkers(0),
            CParameter::EnableLongDistanceMatching(false),
            CParameter::ChecksumFlag(true),
            CParameter::ContentSizeFlag(true),
        ] {
            let result = context.set_parameter(parameter);
            check_context(context.sizeof(), estimate)?;
            native_result(result)?;
        }
        let result = context.set_pledged_src_size(Some(u64::try_from(bytes.len())?));
        check_context(context.sizeof(), estimate)?;
        native_result(result)?;

        let mut consumed = 0usize;
        let mut written = 0usize;
        while consumed < bytes.len() {
            process::check_operation()?;
            if written == stored_limit {
                return Ok(None);
            }
            let input_end = (consumed + STEP_BYTES).min(bytes.len());
            let output_end = (written + STEP_BYTES).min(stored_limit);
            let (read, produced) = {
                let mut input = InBuffer {
                    src: &bytes[consumed..input_end],
                    pos: 0,
                };
                let mut output = OutBuffer::around(&mut candidate.bytes[written..output_end]);
                let result = context.compress_stream(&mut output, &mut input);
                check_context(context.sizeof(), estimate)?;
                native_result(result)?;
                (input.pos, output.pos())
            };
            if read == 0 && produced == 0 {
                return Err(invalid("cache zstd compressor made no progress"));
            }
            consumed = consumed
                .checked_add(read)
                .filter(|size| *size <= bytes.len())
                .ok_or_else(|| invalid("cache zstd consumed input overflow"))?;
            written = written
                .checked_add(produced)
                .filter(|size| *size <= stored_limit)
                .ok_or_else(|| invalid("cache zstd stored output overflow"))?;
        }
        loop {
            process::check_operation()?;
            if written == stored_limit {
                return Ok(None);
            }
            let output_end = (written + STEP_BYTES).min(stored_limit);
            let (remaining, produced) = {
                let mut output = OutBuffer::around(&mut candidate.bytes[written..output_end]);
                let result = context.end_stream(&mut output);
                check_context(context.sizeof(), estimate)?;
                (native_result(result)?, output.pos())
            };
            written = written
                .checked_add(produced)
                .filter(|size| *size <= stored_limit)
                .ok_or_else(|| invalid("cache zstd stored output overflow"))?;
            if remaining == 0 {
                candidate.bytes.truncate(written);
                check_header(&candidate.bytes, bytes.len())?;
                return Ok(Some(candidate));
            }
            if produced == 0 {
                return Err(invalid("cache zstd frame finish made no progress"));
            }
        }
    }

    pub(super) fn decode(
        bytes: &[u8],
        logical_size: usize,
        budget: &Arc<Budget>,
    ) -> Result<OwnedBytes> {
        // In particular, WindowLogMax alone does not reject an oversized declared
        // window when native zstd takes its complete-frame shortcut.
        check_header(bytes, logical_size)?;
        let estimate = native_result(zstd_safe::estimate_dstream_size(WINDOW_BYTES))?;
        let _native = budget.reserve(charge(estimate)?)?;
        let mut owner = OwnedBytes::allocate(logical_size, budget)?;
        let mut context = DCtx::try_create()
            .ok_or_else(|| invalid("cache zstd decompression context allocation failed"))?;
        check_context(context.sizeof(), estimate)?;
        let result = context.set_parameter(DParameter::WindowLogMax(WINDOW_LOG));
        check_context(context.sizeof(), estimate)?;
        native_result(result)?;
        let mut consumed = 0usize;
        let mut written = 0usize;
        let mut overflow = [0u8; 1];
        loop {
            process::check_operation()?;
            let input_end = (consumed + STEP_BYTES).min(bytes.len());
            let at_limit = written == logical_size;
            let (remaining, read, produced) = {
                let mut input = InBuffer {
                    src: &bytes[consumed..input_end],
                    pos: 0,
                };
                // At the exact output cap, still let the decoder consume a
                // footer/checksum. A single extra byte proves output overflow.
                let target = if at_limit {
                    &mut overflow[..]
                } else {
                    let output_end = (written + STEP_BYTES).min(logical_size);
                    &mut owner.bytes[written..output_end]
                };
                let mut output = OutBuffer::around(target);
                let result = context.decompress_stream(&mut output, &mut input);
                check_context(context.sizeof(), estimate)?;
                (native_result(result)?, input.pos, output.pos())
            };
            if at_limit && produced != 0 {
                return Err(invalid("cache zstd decoded output limit exceeded"));
            }
            consumed = consumed
                .checked_add(read)
                .filter(|size| *size <= bytes.len())
                .ok_or_else(|| invalid("cache zstd consumed input overflow"))?;
            written = written
                .checked_add(produced)
                .filter(|size| *size <= logical_size)
                .ok_or_else(|| invalid("cache zstd decoded output limit exceeded"))?;
            if remaining == 0 {
                if consumed != bytes.len() || written != logical_size {
                    return Err(invalid(
                        "cache zstd trailing bytes or decoded size mismatch",
                    ));
                }
                process::check_operation()?;
                return Ok(owner);
            }
            if read == 0 && produced == 0 {
                return Err(invalid(if consumed == bytes.len() {
                    "cache zstd truncated frame"
                } else {
                    "cache zstd decoder made no progress"
                }));
            }
        }
    }
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
