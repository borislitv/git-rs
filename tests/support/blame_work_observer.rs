//! Test-only completed-work metrics, never estimated progress counters.
use git_rs::{
    Result,
    blame::{Observer, Phase, WorkStatistics},
};
use git_rs_core::Control;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Default)]
struct Record {
    completions: u64,
    last: Option<WorkStatistics>,
}
pub struct WorkObserver {
    gate: Option<Arc<dyn Observer>>,
    records: Mutex<[Record; 2]>,
    body_hits: AtomicU64,
    body_claims: [AtomicU64; 3],
}
impl WorkObserver {
    pub fn new(gate: Option<Arc<dyn Observer>>) -> Self {
        Self {
            gate,
            records: Mutex::new([Record::default(); 2]),
            body_hits: AtomicU64::new(0),
            body_claims: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
    pub fn render(&self) -> String {
        let records = *self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut out = format!(
            "git_rs_fixture_body_hits_total {}\n",
            self.body_hits.load(Ordering::Relaxed)
        );
        for (outcome, count) in ["owned", "pending", "unavailable"]
            .into_iter()
            .zip(&self.body_claims)
        {
            out.push_str(&format!(
                "git_rs_fixture_body_build_claims_total{{outcome=\"{outcome}\"}} {}\n",
                count.load(Ordering::Relaxed)
            ));
        }
        for (kind, record) in ["foreground", "warm"].into_iter().zip(records) {
            out.push_str(&format!(
                "git_rs_fixture_work_completions{{kind=\"{kind}\"}} {}\n",
                record.completions
            ));
            if let Some(stats) = record.last {
                for (name, value) in [
                    ("commits", stats.commits_traversed as u64),
                    ("mapping_edges", stats.edges_visited),
                    ("mapping_hits", stats.mapping_hits),
                    ("mapping_computations", stats.fresh_mapping_computations),
                    ("mapping_rejected", stats.rejected_mappings),
                    ("mapping_completed", stats.completed_mappings),
                    ("objects_reused", stats.objects_reused),
                    ("imported_objects_reused", stats.imported_objects_reused),
                    ("imported_mapping_loads", stats.imported_mapping_loads),
                ] {
                    out.push_str(&format!(
                        "git_rs_fixture_work_last_{name}{{kind=\"{kind}\"}} {value}\n"
                    ));
                }
                out.push_str(&format!(
                    "git_rs_fixture_work_last_seconds{{kind=\"{kind}\"}} {}\n",
                    stats.elapsed.as_secs_f64()
                ));
            }
        }
        out
    }
}
impl Observer for WorkObserver {
    fn observe(&self, phase: Phase, control: &Control) -> Result<()> {
        if phase == Phase::CacheHit {
            self.body_hits.fetch_add(1, Ordering::Relaxed);
        }
        let claim = match phase {
            Phase::BodyBuildClaimed => Some(0),
            Phase::BodyBuildPending => Some(1),
            Phase::BodyBuildUnavailable => Some(2),
            _ => None,
        };
        if let Some(index) = claim {
            self.body_claims[index].fetch_add(1, Ordering::Relaxed);
        }
        if let Some(gate) = &self.gate {
            gate.observe(phase, control)?;
        }
        Ok(())
    }
    fn work_completed(&self, warm: bool, stats: WorkStatistics, _control: &Control) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = &mut records[usize::from(warm)];
        record.completions = record.completions.saturating_add(1);
        record.last = Some(stats);
    }
}
