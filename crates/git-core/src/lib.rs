#![forbid(unsafe_code)]

//! Shared, cooperative computation foundation. No process-wide Git configuration
//! or interrupt state is used. Git object adapters are added separately.

pub mod blame;
pub mod bundle;
pub mod control;
pub mod executor;
pub mod graph;
pub mod history;
pub mod objects;
pub mod pack;
pub mod porcelain;
pub mod receive;
pub mod selection;
mod storage;
pub mod upload;
pub mod write_pack;

/// Maximum catalog refs supported by authoritative publication and its readers.
pub const MAX_REFS: usize = 10_000;
/// Maximum byte length of a catalog ref name, independent of UTF-8 encoding.
pub const MAX_REF_BYTES: usize = 1024;

pub use blame::memo::MemoryMappingRequest;
pub use blame::{
    BlameError, BlameOptions, BlameRequest, BlameStatistics, BlameTuning, CompleteBlame,
    ProducerVersion,
};
pub use iconv_native::runtime::Error as CodecRuntimeError;
pub use porcelain::{CompletePorcelain, Porcelain};

pub use graph::Reachable;
pub use objects::{
    AdmittedVec, Object, ReadLimits, SnapshotIdentity, SnapshotOdb, SnapshotSession,
    VerifiedObjectProvider,
};

pub use control::{Control, CpuPoller, StopReason};
pub use executor::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, CpuExecutor,
    ExecutorConfig, JobError, JobOptions, SubmitError, Ticket, WaitError,
};
