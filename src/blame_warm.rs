//! Explicit one-path warming after confirmed publication.
use super::{
    Blame, Outcome, Request,
    derive::{self, Environment, Expected},
};
use crate::{
    Result,
    metadata::Snapshot,
    process,
    protocol::{Oid, Update},
};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// An operator opts in to ONE literal percent-encoded path, not a repository scan.
/// Select at most one changed branch tip from each confirmed atomic publication.
/// Attribution still uses the same full general-DAG engine, never first-parent.
#[derive(Clone)]
pub struct WarmPolicy {
    path: Vec<u8>,
    admission: Arc<process::Admission>,
    completed: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmDisposition {
    Disabled,
    NoTarget,
    Busy,
    Submitted,
    Rejected,
}
#[derive(Clone, Copy, Debug)]
/// Finished/failed counters describe callbacks that actually ran. A queued job
/// discarded before its callback releases admission but is not called a failure.
pub struct WarmStatistics {
    pub admitted: usize,
    pub completed: usize,
    pub failed: usize,
}
impl WarmPolicy {
    pub fn one_path(encoded: &str) -> Result<Self> {
        if encoded.len() > 3 * 4096 {
            return Err(crate::invalid("oversized encoded warm path"));
        }
        let query = Request::parse(
            "1111111111111111111111111111111111111111",
            &format!("path={encoded}"),
        )?;
        Ok(Self {
            path: query.path,
            admission: process::Admission::new(1),
            completed: Arc::new(AtomicUsize::new(0)),
            failed: Arc::new(AtomicUsize::new(0)),
        })
    }
    pub fn admitted(&self) -> usize {
        self.admission.active()
    }
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::Acquire)
    }
    pub fn failed(&self) -> usize {
        self.failed.load(Ordering::Acquire)
    }
    pub fn statistics(&self) -> WarmStatistics {
        WarmStatistics {
            admitted: self.admitted(),
            completed: self.completed(),
            failed: self.failed(),
        }
    }
}

struct Hint {
    slug: String,
    principal: String,
    repository: String,
    generation: u64,
    epoch: u64,
    branch: Vec<u8>,
    tip: Oid,
    _confirmed_operation: String,
}
// Owner order keeps bounded warm admission through environment/hint cleanup.
struct Task {
    environment: Environment,
    hint: Hint,
    policy: WarmPolicy,
    _warm: process::Permit,
}
impl Task {
    fn run(
        self,
        control: &git_rs_core::Control,
        budget: &git_rs_core::CapacityBudget,
    ) -> Result<()> {
        let _warm = self._warm;
        let policy = self.policy;
        let hint = self.hint;
        let environment = self.environment;
        let result = derive::derive(
            environment,
            hint.slug,
            hint.principal,
            Request {
                commit: hint.tip,
                path: policy.path.clone(),
            },
            Some(Expected {
                repository: hint.repository,
                generation: hint.generation,
                epoch: hint.epoch,
                branch: hint.branch,
            }),
            control,
            budget,
        );
        match result {
            Ok(Outcome::Complete(body)) => {
                // Drop actual output/source/pins BEFORE releasing warm/work slots.
                drop(body);
                policy.completed.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Ok(_) => Ok(()), // stale hint/missing configured path/size policy: skip
            Err(error) => {
                policy.failed.fetch_add(1, Ordering::AcqRel);
                Err(error)
            }
        }
    }
}

impl Blame {
    /// This call NEVER returns a publication error, waits, acquires a source
    /// lease, reads a tree or contacts SQL/S3. Only owned bounded hints cross the
    /// confirmed boundary. Panics are observations, not a failed confirmed push.
    #[allow(clippy::too_many_arguments)] // Mirrors the confirmed publication context.
    pub(super) fn try_warm_confirmed(
        &self,
        environment: Environment,
        slug: &str,
        principal: &str,
        snapshot: &Snapshot,
        generation: u64,
        operation: &str,
        commands: &[Update],
    ) -> WarmDisposition {
        catch_unwind(AssertUnwindSafe(|| {
            let Some(policy) = &self.warming else {
                return WarmDisposition::Disabled;
            };
            if environment.derived.is_none() || environment.writer.is_none() {
                return WarmDisposition::Disabled;
            }
            if slug.len() > 4096 || principal.len() > 256 || operation.len() > 36 {
                return WarmDisposition::Rejected;
            }
            // At most 128 command names / 64 KiB are inspected. No tree/ref scan.
            let mut scanned_bytes = 0usize;
            let Some(command) = commands
                .iter()
                .take(128)
                .take_while(|command| {
                    scanned_bytes = scanned_bytes.saturating_add(command.name.len());
                    scanned_bytes <= 64 * 1024
                })
                .find(|command| {
                    command.new != Oid::ZERO
                        && command.new != command.old
                        && command.name.starts_with(b"refs/heads/")
                        && command.name.len() <= 4096
                })
            else {
                return WarmDisposition::NoTarget;
            };
            let Some(warm) = policy.admission.try_admit() else {
                return WarmDisposition::Busy;
            };
            let task = Task {
                environment,
                hint: Hint {
                    slug: slug.to_owned(),
                    principal: principal.to_owned(),
                    repository: snapshot.id.clone(),
                    generation,
                    epoch: snapshot.storage_epoch,
                    branch: command.name.clone(),
                    tip: command.new,
                    _confirmed_operation: operation.to_owned(),
                },
                policy: policy.clone(),
                _warm: warm,
            };
            // Independent 30s absolute deadline is assigned HERE, before queueing.
            // Queue time consumes it; no new budget/deadline is made on dequeue.
            match self
                .jobs
                .try_background(30, move |control, budget| task.run(control, budget))
            {
                Ok(true) => WarmDisposition::Submitted,
                Ok(false) => WarmDisposition::Busy,
                Err(_) => WarmDisposition::Rejected,
            }
        }))
        .unwrap_or(WarmDisposition::Rejected)
    }
}
