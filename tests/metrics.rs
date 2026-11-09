#![forbid(unsafe_code)]

use git_rs::metrics::{
    CacheResult, Direction, Listener, Metric, Outcome, Phase, Registry, Stage, Transport,
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
    registry.observe(
        Metric::Stage(Stage::SqlQuery),
        Duration::from_millis(2),
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
        "# TYPE git_rs_stage_duration_seconds histogram\n",
        "git_rs_stage_duration_seconds_bucket{stage=\"sql_query\",le=\"0.001\"} 0\n",
        "git_rs_stage_duration_seconds_bucket{stage=\"sql_query\",le=\"0.005\"} 1\n",
        "git_rs_stage_duration_seconds_count{stage=\"sql_query\"} 1\n",
        "git_rs_stage_duration_seconds_sum{stage=\"sql_query\"} 0.002000000\n",
        "git_rs_stage_errors_total{stage=\"sql_query\"} 1\n",
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
    use std::io::Write;
    let hit = sample("git_rs_cache_lookups_total{result=\"hit\"}");
    let miss = sample("git_rs_cache_lookups_total{result=\"miss\"}");
    let bypass = sample("git_rs_cache_lookups_total{result=\"bypass\"}");
    let fills = sample("git_rs_stage_duration_seconds_count{stage=\"cache_fill\"}");
    let cache = git_rs::cache::Cache::new(4096, 4);
    let first = cache
        .get_or_load("private-repository:private-object", 4, |mut file| {
            file.write_all(b"data")?;
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
    assert_eq!(std::fs::read(first.path()).unwrap(), b"data");
    assert_eq!(std::fs::read(second.path()).unwrap(), b"data");
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
    assert_eq!(
        sample("git_rs_stage_duration_seconds_count{stage=\"cache_fill\"}"),
        fills + 1
    );
    let text = git_rs::metrics::global().render(0, cache.allocated());
    assert!(!text.contains("private-repository"));
    assert!(!text.contains("private-object"));
}

#[test]
fn failed_subprocess_spawn_and_unwinding_phase_are_observable() {
    let errors = sample("git_rs_subprocess_errors_total{program=\"other\"}");
    let spawns = sample("git_rs_stage_errors_total{stage=\"process_spawn\"}");
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
    assert_eq!(
        sample("git_rs_stage_errors_total{stage=\"process_spawn\"}"),
        spawns + 1
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

#[test]
fn timing_log_fixture() {
    if std::env::var("GIT_RS_TIMING_FIXTURE").as_deref() != Ok("1") {
        return;
    }
    let _timing_guard = git_rs::metrics::init_timing_logs();
    let service_guard = if std::env::var("GIT_RS_TIMING_BACKPRESSURE").as_deref() == Ok("1") {
        git_rs::metrics::init_service_logs().unwrap()
    } else {
        None
    };
    use git_rs::{
        metrics::{self, RequestTimer, Timer},
        process::Operation,
    };
    use std::io::Write;
    let operation = Operation::new(2);
    operation.enter(|| {
        let mut request = RequestTimer::start(Transport::Ssh);
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    operation.enter(|| {
                        let cache = git_rs::cache::Cache::new(4096, 4);
                        let file = cache
                            .get_or_load("private-repository:private-object", 4, |mut file| {
                                file.write_all(b"data")?;
                                Ok(())
                            })
                            .unwrap()
                            .unwrap();
                        assert_eq!(
                            git_rs::blob::digest(file.path()).unwrap(),
                            "3a6eb0790f39ac87c94f3856b2dd2c5d110e6811602261a9a923d3bb23adc8b7"
                        );
                    })
                })
                .join()
                .unwrap();
        });
        assert!(
            metrics::measure::<()>(Metric::Stage(Stage::RefValidate), || {
                Err(git_rs::invalid("private-ref:private-principal"))
            })
            .is_err()
        );
        assert!(
            git_rs::process::capture(
                std::process::Command::new("/nonexistent/private-command")
                    .arg("private-credential"),
                b"",
                4096,
            )
            .is_err()
        );
        drop(Timer::start(Metric::Stage(Stage::GitWait)));
        operation.cancel();
        Operation::new(1).enter(|| {
            metrics::measure(
                Metric::Stage(Stage::LeaseRelease),
                git_rs::process::check_operation,
            )
            .unwrap();
        });
        request.outcome(Outcome::Success);
    });
    Operation::new(2).enter(|| {
        let mut request = RequestTimer::start(Transport::Http);
        assert!(
            metrics::measure::<()>(Metric::Phase(Phase::Snapshot), || {
                Err(git_rs::invalid("private-sql:private-object"))
            })
            .is_err()
        );
        request.outcome(Outcome::Unauthorized);
    });
    assert!(Operation::current().is_none());
    metrics::measure(Metric::Phase(Phase::Materialize), || Ok(())).unwrap();
    if std::env::var("GIT_RS_TIMING_BACKPRESSURE").as_deref() == Ok("1") {
        use std::fs;
        use std::os::unix::fs::DirBuilderExt;
        // External deletion is a reachable cleanup failure. Exercise the real
        // public RAII owners, not a mocked log call, with the sink already full.
        let root = std::env::temp_dir().join(format!(
            "git-rs-cleanup-{}",
            git_rs::scratch::identifier().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let scratch = git_rs::scratch::Scratch::new(&root, 1).unwrap();
        fs::remove_dir_all(&scratch.path).unwrap();
        let hook = git_rs::hooks::PrivateSocket::new().unwrap();
        fs::remove_dir_all(hook.path.parent().unwrap()).unwrap();
        let socket_path = root.join("serve.sock");
        let serving = git_rs::hooks::ServingSocket::bind(&socket_path).unwrap();
        fs::remove_file(&socket_path).unwrap();
        let admission = git_rs::process::Admission::new(1);
        let permit = admission.try_admit().unwrap();
        Operation::new(1).with_interrupt(
            || {},
            || {
                let _permit = permit;
                drop(scratch);
                drop(hook);
                drop(serving);
                for _ in 0..8192 {
                    assert!(metrics::service_event(
                        metrics::ServiceEvent::LeaseReleaseFailed
                    ));
                    metrics::measure(
                        Metric::Stage(Stage::GitWait),
                        git_rs::process::check_operation,
                    )
                    .unwrap();
                }
            },
        );
        assert_eq!(admission.active(), 0);
        assert!(admission.try_admit().is_some());
        fs::remove_dir_all(root).unwrap();
        assert!(sample("git_rs_service_logs_dropped_total") > 0);
        assert!(
            sample("git_rs_timing_logs_dropped_total") > 0,
            "the full diagnostic queue must drop records rather than block"
        );
        // Let the parent fill stdout after the test harness startup output. Exit
        // directly after guard destruction so harness output cannot mask a hang.
        // With one test thread, libtest leaves its test-name prefix on this line.
        // Keep the control marker separate from harness output for the parent.
        println!("\ntiming_fixture_ready");
        std::io::stdout().flush().unwrap();
        use std::io::Read;
        std::io::stdin().read_exact(&mut [0]).unwrap();
        drop(_timing_guard);
        drop(service_guard);
        std::process::exit(0);
    }
}

#[test]
fn service_log_fixture() {
    if std::env::var("GIT_RS_SERVICE_LOG_FIXTURE").as_deref() != Ok("1") {
        return;
    }
    let _timings = git_rs::metrics::init_timing_logs();
    let _service = git_rs::metrics::init_service_logs().unwrap();
    use git_rs::metrics::{ServiceEvent::*, service_event};
    for event in [
        HttpOperationFailed,
        SshOperationFailed,
        HttpAcceptFailed,
        HookRejected,
        HookSocketCleanupFailed,
        ServingSocketCleanupFailed,
        LeaseReleaseFailed,
        ScratchCleanupFailed,
    ] {
        assert!(service_event(event));
    }
}

#[test]
fn service_diagnostics_have_only_fixed_records_with_or_without_timing_logs() {
    for timings in ["0", "1"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "service_log_fixture", "--nocapture"])
            .env("GIT_RS_SERVICE_LOG_FIXTURE", "1")
            .env("GIT_RS_DEBUG_TIMINGS", timings)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            concat!(
                "git_rs_error event=http_operation_failed\n",
                "git_rs_error event=ssh_operation_failed\n",
                "git_rs_error event=http_accept_failed\n",
                "git_rs_error event=hook_rejected\n",
                "git_rs_error event=hook_socket_cleanup_failed\n",
                "git_rs_error event=serving_socket_cleanup_failed\n",
                "git_rs_error event=lease_release_failed\n",
                "git_rs_error event=scratch_cleanup_failed\n",
            )
        );
    }
}

#[test]
fn blocked_timing_sink_cannot_hold_request_admission() {
    use std::io::{BufRead, BufReader, Write};
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    use std::process::{Command, Stdio};
    use std::time::Instant;
    fn fill(writer: &mut UnixStream) {
        writer.set_nonblocking(true).unwrap();
        loop {
            match writer.write(&[b'x'; 8192]) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("fill diagnostic sink: {error}"),
            }
        }
        writer.set_nonblocking(false).unwrap();
    }
    let (_unread, mut writer) = UnixStream::pair().unwrap();
    fill(&mut writer);
    let fd: OwnedFd = writer.into();
    let (reader, mut stdout_writer) = UnixStream::pair().unwrap();
    reader
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let stdout_fd: OwnedFd = stdout_writer.try_clone().unwrap().into();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "timing_log_fixture", "--nocapture"])
        .env("GIT_RS_TIMING_FIXTURE", "1")
        .env("GIT_RS_TIMING_BACKPRESSURE", "1")
        .env("GIT_RS_DEBUG_TIMINGS", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(fd))
        .spawn()
        .unwrap();
    let mut output = BufReader::new(reader);
    loop {
        let mut line = String::new();
        if !matches!(output.read_line(&mut line), Ok(count) if count > 0) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("a blocked diagnostic sink held the request and admission");
        }
        if line.trim() == "timing_fixture_ready" {
            break;
        }
    }
    fill(&mut stdout_writer);
    child.stdin.take().unwrap().write_all(b"x").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("blocked stdout/stderr prevented diagnostic shutdown");
        }
        std::thread::yield_now();
    };
    assert!(
        status.success(),
        "request must complete even when diagnostic records are discarded"
    );
}

#[test]
fn timing_logs_are_opt_in_correlated_and_exclude_private_values() {
    use std::collections::BTreeMap;
    for enabled in [false, true] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "timing_log_fixture", "--nocapture"])
            .env("GIT_RS_TIMING_FIXTURE", "1")
            .env_remove("GIT_RS_DEBUG_TIMINGS");
        if enabled {
            child.env("GIT_RS_DEBUG_TIMINGS", "1");
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            !stderr.contains("private-"),
            "diagnostics must not include values or command arguments"
        );
        let records: Vec<BTreeMap<_, _>> = stderr
            .lines()
            .filter_map(|line| line.strip_prefix("git_rs_timing "))
            .map(|line| {
                line.split_ascii_whitespace()
                    .map(|field| field.split_once('=').unwrap())
                    .collect()
            })
            .collect();
        if !enabled {
            assert!(
                records.is_empty(),
                "timing logs must be disabled by default"
            );
            continue;
        }
        assert_eq!(records.len(), 11, "{stderr}");
        let find = |kind, name| {
            records
                .iter()
                .find(|row| row["kind"] == kind && row["name"] == name)
                .unwrap()
        };
        let first = find("request", "ssh")["operation"];
        let second = find("request", "http")["operation"];
        assert_ne!(first, "0");
        assert_ne!(
            first, second,
            "independent requests require independent IDs"
        );
        assert_ne!(second, "0");
        for (kind, name, outcome) in [
            ("stage", "cache_fill", "success"),
            ("stage", "object_hash", "success"),
            ("stage", "ref_validate", "error"),
            ("stage", "process_spawn", "error"),
            ("subprocess", "other", "error"),
            ("stage", "git_wait", "error"),
            ("stage", "lease_release", "success"),
            ("request", "ssh", "success"),
        ] {
            let row = find(kind, name);
            assert_eq!(
                row["operation"], first,
                "worker and cleanup spans must retain request context"
            );
            assert_eq!(row["outcome"], outcome);
        }
        assert_eq!(find("phase", "snapshot")["operation"], second);
        assert_eq!(find("phase", "snapshot")["outcome"], "error");
        assert_eq!(find("request", "http")["outcome"], "unauthorized");
        assert_eq!(find("phase", "materialize")["operation"], "0");
        for row in records {
            assert_eq!(row.len(), 8);
            assert_eq!(row["level"], "debug");
            assert!(row["pid"].parse::<u32>().unwrap() > 0);
            let elapsed = row["elapsed_ms"].parse::<f64>().unwrap();
            let duration = row["duration_ms"].parse::<f64>().unwrap();
            assert!(
                elapsed.is_finite()
                    && duration.is_finite()
                    && elapsed >= duration
                    && duration >= 0.0
            );
        }
    }
}
