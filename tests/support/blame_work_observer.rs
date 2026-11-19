//! Test-only completed-work metrics, never estimated progress counters.
use git_rs::{
    Result,
    blame::{Observer, Phase, WorkStatistics},
};
use git_rs_core::Control;
use git_rs_core::blame::BlameProgressSnapshot;
use git_rs_core::pack::ranges::VerificationStats;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

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
    progress_enabled: bool,
    progress: Mutex<ProgressState>,
    range: Mutex<(u64, VerificationStats)>,
}

type ProgressState = (u64, Option<(BlameProgressSnapshot, Duration, bool)>);
impl WorkObserver {
    pub fn new(gate: Option<Arc<dyn Observer>>, progress_enabled: bool) -> Self {
        Self {
            gate,
            records: Mutex::new([Record::default(); 2]),
            body_hits: AtomicU64::new(0),
            body_claims: std::array::from_fn(|_| AtomicU64::new(0)),
            progress_enabled,
            progress: Mutex::new((0, None)),
            range: Mutex::new((0, VerificationStats::default())),
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
                    ("completed_map_lines", u64::from(stats.completed_map_lines)),
                    ("completed_map_miss", u64::from(stats.completed_map_miss)),
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
        if self.progress_enabled {
            let (closed, s) = *self.range.lock().unwrap_or_else(|e| e.into_inner());
            for (key, value) in [
                ("providers", closed.into()),
                ("sha1_calls", s.sha1_calls.into()),
                ("sha1_bytes", s.sha1_bytes.into()),
                ("sha1_ns", s.sha1_time.as_nanos()),
                ("sha256_bytes", s.sha256_bytes.into()),
                ("sha256_ns", s.sha256_time.as_nanos()),
                ("journal_records", s.journal_records.into()),
                ("recorded_sha1_bytes", s.recorded_sha1_bytes.into()),
                ("journal_hits", s.journal_hits.into()),
                ("avoided_sha1_bytes", s.avoided_sha1_bytes.into()),
                ("unrecorded_sha1_calls", s.unrecorded_sha1_calls.into()),
                ("journal_mismatches", s.journal_mismatches.into()),
            ] {
                out.push_str(&format!(
                    "git_rs_fixture_range_closed_{key}_total {value}\n"
                ));
            }
            out.push_str(&format!(
                "git_rs_fixture_range_closed_journal_capacity_bytes_max {}\n",
                s.journal_capacity_bytes
            ));
            let (attempts, last) = *self
                .progress
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            out.push_str(&format!(
                "git_rs_fixture_attribution_attempts_total {attempts}\n"
            ));
            if let Some((s, elapsed, completed)) = last {
                for (key, value) in [
                    ("core_ok", u128::from(completed)),
                    ("wall_ns", elapsed.as_nanos()),
                    ("commit_watermark", s.commit_watermark as u128),
                    ("mapping_boundaries", s.mapping_boundaries.into()),
                    (
                        "mapping_edges_visited_at_boundary",
                        s.mapping.edges_visited.into(),
                    ),
                    (
                        "mapping_computations_at_boundary",
                        s.mapping.computations.into(),
                    ),
                    ("mapping_hits_at_boundary", s.mapping.hits.into()),
                    (
                        "mapping_units_delivered_at_boundary",
                        s.mapping.completed.into(),
                    ),
                    ("last_mapping_wall_ns", s.last_mapping_elapsed.as_nanos()),
                    ("raw_blob_calls", s.raw_blob_calls.into()),
                    ("raw_blob_ns", s.raw_blob_elapsed.as_nanos()),
                    (
                        "raw_blob_ns_at_last_mapping",
                        s.raw_blob_elapsed_at_last_mapping.as_nanos(),
                    ),
                ] {
                    out.push_str(&format!("git_rs_fixture_attribution_last_{key} {value}\n"));
                }
            }
        }
        out
    }
}
impl Observer for WorkObserver {
    fn range_verification_closed(&self, stats: VerificationStats) {
        let mut state = self.range.lock().unwrap_or_else(|e| e.into_inner());
        state.0 = state.0.saturating_add(1);
        let total = &mut state.1;
        macro_rules! sum {
            ($($field:ident),*) => { $(total.$field = total.$field.saturating_add(stats.$field);)* };
        }
        sum!(
            sha1_calls,
            sha1_bytes,
            sha1_time,
            sha256_bytes,
            sha256_time,
            journal_records,
            recorded_sha1_bytes,
            journal_hits,
            avoided_sha1_bytes,
            unrecorded_sha1_calls,
            journal_mismatches
        );
        total.journal_capacity_bytes = total
            .journal_capacity_bytes
            .max(stats.journal_capacity_bytes);
    }

    fn wants_attribution_progress(&self) -> bool {
        self.progress_enabled
    }

    fn attribution_progress(
        &self,
        snapshot: BlameProgressSnapshot,
        elapsed: Duration,
        completed: bool,
    ) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        progress.0 = progress.0.saturating_add(1);
        progress.1 = Some((snapshot, elapsed, completed));
    }

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
