use crate::{Result, invalid};
use git_rs_core::{ReadLimits, receive::ReceiveLimits};

/// Server-side selection; a client can never select its resource envelope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResourceProfile {
    #[default]
    BaselineV1,
    LargeBootstrapV1,
}

impl ResourceProfile {
    pub fn from_os_value(value: Option<&std::ffi::OsStr>) -> Result<Self> {
        match value {
            None => Ok(Self::BaselineV1),
            Some(v) if v == "baseline-v1" => Ok(Self::BaselineV1),
            Some(v) if v == "large-bootstrap-v1" => Ok(Self::LargeBootstrapV1),
            _ => Err(invalid("unknown GIT_RS_RECEIVE_RESOURCE_PROFILE")),
        }
    }

    pub fn validate_capacity(self, workers: usize, queued: usize) -> Result<()> {
        if self == Self::LargeBootstrapV1 && (workers != 1 || queued != 0 || usize::BITS < 64) {
            return Err(invalid(
                "large-bootstrap-v1 requires one worker, zero waiters and 64-bit Rust",
            ));
        }
        Ok(())
    }

    pub(crate) fn job_bytes(self) -> usize {
        match self {
            Self::BaselineV1 => crate::cpu_jobs::JOB_BYTES,
            Self::LargeBootstrapV1 => {
                usize::try_from(10u64 << 30).expect("validated 64-bit profile")
            }
        }
    }

    pub(crate) const fn body_bytes(self) -> u64 {
        match self {
            Self::BaselineV1 => crate::http::MAX_BODY,
            // Below the S3 single-PUT ceiling, even if measured in decimal GB.
            Self::LargeBootstrapV1 => 4608u64 << 20,
        }
    }

    pub(super) fn limits(self) -> (ReadLimits, ReceiveLimits) {
        match self {
            Self::BaselineV1 => (ReadLimits::default(), ReceiveLimits::default()),
            Self::LargeBootstrapV1 => (
                ReadLimits {
                    max_graph_objects: 600_000,
                    max_graph_edges: 64_000_000,
                    ..ReadLimits::default()
                },
                ReceiveLimits {
                    max_incoming_objects: 600_000,
                    max_object_reads: 64_000_000,
                    max_decoded_bytes: 8u64 << 40,
                    ..ReceiveLimits::default()
                },
            ),
        }
    }
}
