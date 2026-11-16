//! Explicit blame-only resource policy; never an attribution or qualification selector.
use crate::{Result, invalid};
use git_rs_core::{ReadLimits, blame::BlameOptions, pack::DecoderOptions};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResourceProfile {
    #[default]
    BaselineV1,
    LargeV1,
}

impl ResourceProfile {
    /// Parse a startup-supplied value without reading process-global environment.
    pub fn from_os_value(value: Option<&std::ffi::OsStr>) -> Result<Self> {
        match value {
            None => Ok(Self::BaselineV1),
            Some(value) => Self::parse(
                value
                    .to_str()
                    .ok_or_else(|| invalid("GIT_RS_BLAME_RESOURCE_PROFILE must be UTF-8"))?,
            ),
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "baseline-v1" => Ok(Self::BaselineV1),
            "large-v1" => Ok(Self::LargeV1),
            _ => Err(invalid("unknown GIT_RS_BLAME_RESOURCE_PROFILE")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::BaselineV1 => "baseline-v1",
            Self::LargeV1 => "large-v1",
        }
    }

    pub fn validate_capacity(self, workers: usize, queued: usize) -> Result<()> {
        if !(1..=4).contains(&workers) || queued > 3 || workers + queued > 4 {
            return Err(invalid("invalid bounded blame capacity"));
        }
        if self == Self::LargeV1 && (workers != 1 || queued != 0 || usize::BITS < 64) {
            return Err(invalid(
                "large-v1 requires one actual worker, zero queued jobs and a 64-bit runtime",
            ));
        }
        Ok(())
    }

    pub fn job_bytes(self) -> usize {
        let bytes = match self {
            Self::BaselineV1 => 1u64 << 30,
            Self::LargeV1 => 4u64 << 30,
        };
        usize::try_from(bytes).expect("resource profile requires supported address width")
    }

    pub fn read_limits(self, options: &BlameOptions) -> ReadLimits {
        let mut limits = options.read_limits();
        if self == Self::LargeV1 {
            // Existing core also uses this as a workspace-plan ceiling. Actual
            // allocations still share the finite job budget; this is NOT RSS.
            limits.max_decode_bytes =
                usize::try_from(8u64 << 30).expect("large-v1 requires a 64-bit runtime");
        }
        limits
    }

    pub fn decoder_options(self) -> DecoderOptions {
        DecoderOptions {
            cache_slots: 4096,
            cache_bytes: match self {
                Self::BaselineV1 => 32 * 1024 * 1024,
                Self::LargeV1 => 512 * 1024 * 1024,
            },
            max_cache_entry_bytes: match self {
                Self::BaselineV1 => 1024 * 1024,
                Self::LargeV1 => 128 * 1024 * 1024,
            },
            header_slots: 4096,
            // Used by mapped PackSet only, not the serving RangePackSet path.
            retained_workspace_bytes: match self {
                Self::BaselineV1 => 0,
                Self::LargeV1 => 64 * 1024 * 1024,
            },
        }
    }

    pub(crate) fn canonical_bytes(self, limits: ReadLimits) -> Vec<u8> {
        let decoder = self.decoder_options();
        let mut bytes = b"git-rs/blame-resources/v1\0".to_vec();
        bytes.extend_from_slice(self.name().as_bytes());
        bytes.push(0);
        let (workers, queued, total) = match self {
            Self::BaselineV1 => (4, 3, 4),
            Self::LargeV1 => (1, 0, 1),
        };
        for value in [
            workers,
            queued,
            total,
            self.job_bytes(),
            decoder.cache_slots,
            decoder.cache_bytes,
            decoder.max_cache_entry_bytes,
            decoder.header_slots,
            decoder.retained_workspace_bytes,
            limits.max_object_bytes,
            limits.max_metadata_bytes,
            limits.max_graph_objects,
            limits.max_graph_edges,
            limits.max_delta_depth,
            limits.max_decode_bytes,
            super::MAX_BLOB as usize,
            super::MAX_OUTPUT as usize,
            super::batch::WORK_BYTES,
            super::batch::OBJECT_BYTES,
        ] {
            bytes.extend_from_slice(&(value as u64).to_be_bytes());
        }
        bytes
    }

    pub(crate) fn digest(self, limits: ReadLimits) -> [u8; 32] {
        openssl::sha::sha256(&self.canonical_bytes(limits))
    }
}
