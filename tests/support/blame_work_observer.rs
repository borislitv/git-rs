//! Test-only completed-work metrics, never estimated progress counters.
use git_rs::{
    Result,
    blame::{Observer, Phase, WorkStatistics},
};
use git_rs_core::Control;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Default)]
struct Record {
    completions: u64,
    last: Option<WorkStatistics>,
}
pub struct WorkObserver {
    gate: Option<Arc<dyn Observer>>,
    records: Mutex<[Record; 2]>,
}
impl WorkObserver {
    pub fn new(gate: Option<Arc<dyn Observer>>) -> Self {
        Self {
            gate,
            records: Mutex::new([Record::default(); 2]),
        }
    }
    pub fn render(&self) -> String {
        let records = *self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut out = String::new();
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
