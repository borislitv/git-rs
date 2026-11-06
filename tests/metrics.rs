#![forbid(unsafe_code)]

use git_rs::metrics::{
    CacheResult, Direction, Listener, Metric, Outcome, Phase, Registry, Transport,
};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn histograms_preserve_boundaries_overflow_errors_and_prometheus_framing() {
    let registry = Registry::new();
    registry.observe(
        Metric::Phase(Phase::Snapshot),
        Duration::from_millis(25),
        true,
    );
    registry.observe(
        Metric::Phase(Phase::Snapshot),
        Duration::from_nanos(25_000_001),
        true,
    );
    registry.observe(
        Metric::Phase(Phase::Snapshot),
        Duration::from_secs(121),
        false,
    );
    let text = registry.render(3, 8192);
    for expected in [
        "# TYPE git_rs_phase_duration_seconds histogram\n",
        "git_rs_phase_duration_seconds_bucket{phase=\"snapshot\",le=\"0.01\"} 0\n",
        "git_rs_phase_duration_seconds_bucket{phase=\"snapshot\",le=\"0.025\"} 1\n",
        "git_rs_phase_duration_seconds_bucket{phase=\"snapshot\",le=\"0.05\"} 2\n",
        "git_rs_phase_duration_seconds_bucket{phase=\"snapshot\",le=\"120\"} 2\n",
        "git_rs_phase_duration_seconds_bucket{phase=\"snapshot\",le=\"+Inf\"} 3\n",
        "git_rs_phase_duration_seconds_count{phase=\"snapshot\"} 3\n",
        "git_rs_phase_duration_seconds_sum{phase=\"snapshot\"} 121.050000001\n",
        "git_rs_phase_errors_total{phase=\"snapshot\"} 1\n",
        "git_rs_active_requests 3\n",
        "git_rs_cache_allocated_bytes 8192\n",
    ] {
        assert!(text.contains(expected), "missing sample: {expected}");
    }
    assert!(text.ends_with('\n'));
    assert!(text.len() < 64 * 1024, "exposition must remain bounded");
}

#[test]
fn concurrent_observations_do_not_lose_counts_or_change_series_cardinality() {
    let registry = Arc::new(Registry::new());
    let initial_series = registry
        .render(0, 0)
        .lines()
        .filter(|line| !line.starts_with('#'))
        .count();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let registry = &registry;
            scope.spawn(move || {
                for _ in 0..1000 {
                    registry.observe(Metric::Phase(Phase::Graph), Duration::from_millis(4), false);
                    registry.request(Transport::Ssh, Outcome::Success, Duration::from_millis(10));
                    registry.cache(CacheResult::Hit);
                    registry.reject(Listener::Http);
                    registry.transferred(Direction::Download, 23);
                }
            });
        }
    });
    let text = registry.render(0, 0);
    for expected in [
        "git_rs_phase_duration_seconds_bucket{phase=\"graph\",le=\"0.005\"} 8000\n",
        "git_rs_phase_duration_seconds_count{phase=\"graph\"} 8000\n",
        "git_rs_phase_duration_seconds_sum{phase=\"graph\"} 32.000000000\n",
        "git_rs_phase_errors_total{phase=\"graph\"} 8000\n",
        "git_rs_requests_total{transport=\"ssh\",outcome=\"success\"} 8000\n",
        "git_rs_cache_lookups_total{result=\"hit\"} 8000\n",
        "git_rs_admission_rejections_total{listener=\"http\"} 8000\n",
        "git_rs_s3_transferred_bytes_total{direction=\"download\"} 184000\n",
    ] {
        assert!(text.contains(expected), "missing sample: {expected}");
    }
    assert_eq!(
        text.lines().filter(|line| !line.starts_with('#')).count(),
        initial_series
    );
}

fn sample(name: &str) -> u64 {
    let text = git_rs::metrics::global().render(0, 0);
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn real_cache_hits_misses_and_bypass_are_observable() {
    let hit = sample("git_rs_cache_lookups_total{result=\"hit\"}");
    let miss = sample("git_rs_cache_lookups_total{result=\"miss\"}");
    let bypass = sample("git_rs_cache_lookups_total{result=\"bypass\"}");
    let cache = git_rs::cache::Cache::new(4, 4);
    let first = cache
        .get_or_load("private-repository:private-object", 4, |bytes| {
            bytes.copy_from_slice(b"data");
            Ok(())
        })
        .unwrap()
        .unwrap();
    let second = cache
        .get_or_load("private-repository:private-object", 4, |_| {
            panic!("cached bytes must be reused")
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.as_ref().as_ref(), b"data");
    assert_eq!(second.as_ref().as_ref(), b"data");
    assert!(
        cache
            .get_or_load("too-large", 5, |_| panic!("oversized entry must bypass"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sample("git_rs_cache_lookups_total{result=\"hit\"}"),
        hit + 1
    );
    assert_eq!(
        sample("git_rs_cache_lookups_total{result=\"miss\"}"),
        miss + 1
    );
    assert_eq!(
        sample("git_rs_cache_lookups_total{result=\"bypass\"}"),
        bypass + 1
    );
    let text = git_rs::metrics::global().render(0, cache.allocated());
    assert!(!text.contains("private-repository"));
    assert!(!text.contains("private-object"));
}

#[test]
fn failed_subprocess_spawn_and_unwinding_phase_are_observable() {
    let errors = sample("git_rs_subprocess_errors_total{program=\"other\"}");
    assert!(
        git_rs::process::capture(
            &mut std::process::Command::new("/nonexistent/git-rs-metrics-test"),
            b"",
            4096
        )
        .is_err()
    );
    assert_eq!(
        sample("git_rs_subprocess_errors_total{program=\"other\"}"),
        errors + 1
    );
    let count = sample("git_rs_phase_duration_seconds_count{phase=\"upload\"}");
    let errors = sample("git_rs_phase_errors_total{phase=\"upload\"}");
    let panicked = std::panic::catch_unwind(|| {
        git_rs::metrics::measure::<()>(Metric::Phase(Phase::Upload), || panic!("interrupted phase"))
    });
    assert!(panicked.is_err());
    assert_eq!(
        sample("git_rs_phase_duration_seconds_count{phase=\"upload\"}"),
        count + 1
    );
    assert_eq!(
        sample("git_rs_phase_errors_total{phase=\"upload\"}"),
        errors + 1
    );
}
