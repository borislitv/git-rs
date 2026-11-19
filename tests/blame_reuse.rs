#![forbid(unsafe_code)]
//! Real-source gates. Run only in the owned PG/S3/private-tmpfs fixture.
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase, Request, Response},
    derived_cache::CacheConfig,
    engine::Engine,
    metadata,
    process::Operation,
    protocol::{Oid, Update},
};
use git_rs_core::{Control, blame::BlameOptions};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[allow(dead_code)]
#[path = "support/blame_source_support.rs"]
mod source_support;

#[path = "support/blame_immutable_body.rs"]
mod immutable_body;

#[path = "support/blame_cache_support.rs"]
mod cache_support;

#[path = "support/blame_completed_transport.rs"]
mod completed_transport;

fn native_output(root: &Path, args: &[&str], input: &[u8]) -> std::process::Output {
    native_output_with_deadline(root, args, input, "20s")
}

fn native_output_with_deadline(
    root: &Path,
    args: &[&str],
    input: &[u8],
    deadline: &str,
) -> std::process::Output {
    let mut command = Command::new("timeout");
    command
        .args(["--kill-after=2s", deadline, "git"])
        .arg("-C")
        .arg(root)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("LC_ALL", "C")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(path) = std::env::var_os("GIT_RS_TEST_ORACLE_EXEC_PATH") {
        command.env("GIT_EXEC_PATH", path);
    }
    let mut child = command.spawn().unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}
fn native(root: &Path, args: &[&str], input: &[u8]) -> Vec<u8> {
    let out = native_output(root, args, input);
    assert!(
        out.status.success(),
        "native fixture {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}
fn text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap().trim().to_owned()
}
fn native_porcelain(root: &Path, commit: &str) -> Vec<u8> {
    native(
        root,
        &[
            "--literal-pathspecs",
            "blame",
            "--root",
            "--line-porcelain",
            "--no-textconv",
            "--no-progress",
            "--diff-algorithm=myers",
            commit,
            "--",
            "file",
        ],
        b"",
    )
}
fn unregistered_config() -> CacheConfig {
    let id = git_rs::scratch::identifier().unwrap();
    CacheConfig::new(
        format!(
            "{}-{}-{}-{}-{}",
            &id[..8],
            &id[8..12],
            &id[12..16],
            &id[16..20],
            &id[20..]
        ),
        "22222222-2222-2222-2222-222222222222".into(),
    )
}
fn config() -> CacheConfig {
    let config = unregistered_config();
    Operation::new(30)
        .enter(|| {
            git_rs::derived_cache::DerivedCache::open(config.clone())
                .unwrap()
                .migrate()
        })
        .unwrap();
    config
}
fn history(root: &Path, count: usize, base: &str) -> String {
    // One finite native importer, no per-edge native subprocess or oracle reuse.
    let mut input = b"reset refs/heads/history\n\n".to_vec();
    for n in 1..=count {
        write!(input, "commit refs/heads/history\nmark :{n}\ncommitter Synthetic <synthetic@example.invalid> {} +0000\ndata 7\nchange\n\n", 1_000_000_000 + n).unwrap();
        if n > 1 {
            writeln!(input, "from :{}", n - 1).unwrap();
        } else {
            writeln!(input, "from {base}").unwrap();
        }
        let content = format!("unchanged\nversion {n}\n");
        write!(
            input,
            "M 100644 inline file\ndata {}\n{content}\n",
            content.len()
        )
        .unwrap();
    }
    native(root, &["fast-import", "--quiet", "--force"], &input);
    text(native(root, &["rev-parse", "refs/heads/history"], b""))
}
fn publish_history(fixture: &source_support::Fixture, tip: &str) {
    publish_history_from(fixture, &fixture.commit, tip);
}

#[derive(Default)]
struct SegmentProbe {
    planning: AtomicBool,
    reject: std::sync::Mutex<Option<SegmentReject>>,
    reads: std::sync::Mutex<Vec<Oid>>,
}

enum SegmentReject {
    Cancel,
    Revoke(String),
}

impl Observer for SegmentProbe {
    fn observe(&self, phase: Phase, control: &Control) -> Result<()> {
        if phase == Phase::SegmentPlanning {
            self.reads.lock().unwrap().clear();
            self.planning.store(true, Ordering::Release);
        } else if phase == Phase::SegmentPlanned {
            self.planning.store(false, Ordering::Release);
            match self.reject.lock().unwrap().take() {
                Some(SegmentReject::Cancel) => control.cancel(),
                Some(SegmentReject::Revoke(slug)) => {
                    metadata::grant(&slug, "alice", "bob", "none", b"refs/")?;
                }
                None => {}
            }
        }
        Ok(())
    }

    fn source_read(&self, id: &gix::hash::oid, _: bool, _: &Control) {
        if self.planning.load(Ordering::Acquire) {
            self.reads
                .lock()
                .unwrap()
                .push(Oid(id.as_bytes().try_into().unwrap()));
        }
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn cold_large_v1_uses_segments_and_matches_native() {
    use git_rs::blame::ResourceProfile;

    assert!(std::thread::available_parallelism().unwrap().get() >= 2);
    let git = std::env::var_os("GIT_RS_TEST_ORACLE_GIT").expect("qualified native Git 2.56");
    let mut oracle = Command::new(git);
    oracle
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("LC_ALL", "C");
    let version = oracle.arg("--version").output().unwrap();
    assert!(version.status.success());
    assert_eq!(version.stdout, b"git version 2.56.0\n");

    let probe = Arc::new(SegmentProbe::default());
    let stage_count = |stage: &str, outcome: &str| -> u64 {
        let metrics = git_rs::metrics::global().render(0, 0);
        let prefix =
            format!("git_rs_blame_segment_stage_total{{stage=\"{stage}\",outcome=\"{outcome}\"}} ");
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap()
            .parse()
            .unwrap()
    };
    let blame = Blame::new_with_resource_profile(1, 0, ResourceProfile::LargeV1)
        .unwrap()
        .with_observer(probe.clone());
    let fixture = source_support::Fixture::new(blame, 10);
    let native_root = fixture.root.join("native");
    let tip = history(&native_root, 40, &fixture.commit);
    publish_history(&fixture, &tip);
    let request = Request::parse(&tip, "path=file").unwrap();
    let response = Operation::new(30)
        .enter(|| fixture.engine.blame(&fixture.slug, "alice", &request))
        .unwrap();
    let Response::Complete(body) = response else {
        panic!("cold large-v1 blame did not complete");
    };
    let stats = body.work_statistics().unwrap().segments;
    assert!(
        stats.used && stats.lanes >= 2,
        "ordinary fallback: {stats:?}"
    );
    assert_eq!(stats.scanned_edges, 40);
    assert!(stats.sampled_capacity_bytes <= ResourceProfile::LargeV1.job_bytes());
    let objects = text(native(&native_root, &["rev-list", "--objects", &tip], b""));
    let blobs: Vec<_> = objects
        .lines()
        .filter_map(|line| line.strip_suffix(" file"))
        .map(|id| Oid::parse(id.as_bytes()).unwrap())
        .collect();
    assert!(blobs.len() >= 40);
    let reads = probe.reads.lock().unwrap();
    assert!(!reads.is_empty());
    assert!(
        !reads.iter().any(|id| blobs.contains(id)),
        "planner touched physical blob"
    );
    drop(reads);
    eprintln!("cold large-v1 segments: {stats:?}");
    let mut bytes = Vec::new();
    Operation::new(10)
        .enter(|| body.write_to(&mut bytes))
        .unwrap();
    let output = Command::new("timeout")
        .args(["--kill-after=2s", "20s"])
        .arg(oracle.get_program())
        .env_clear()
        .envs(
            oracle
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value))),
        )
        .arg("-C")
        .arg(&native_root)
        .args([
            "--literal-pathspecs",
            "blame",
            "--root",
            "--line-porcelain",
            "--no-textconv",
            "--no-progress",
            "--diff-algorithm=myers",
            &tip,
            "--",
            "file",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(bytes, output.stdout);
    drop(body);
    wait_for(|| fixture.leases() == 0);

    let stopped = stage_count("planning", "stopped");
    *probe.reject.lock().unwrap() = Some(SegmentReject::Cancel);
    let cancelled =
        Operation::new(30).enter(|| fixture.engine.blame(&fixture.slug, "alice", &request));
    assert!(cancelled.is_err());
    // Shared errors retain their CPU ownership until the caller drops them.
    drop(cancelled);
    wait_for(|| fixture.leases() == 0 && fixture.engine.blame.admitted() == 0);
    assert!(stage_count("planning", "stopped") > stopped);

    fixture.restored_reader();
    *probe.reject.lock().unwrap() = Some(SegmentReject::Revoke(fixture.slug.clone()));
    let revoked = Operation::new(30).enter(|| fixture.engine.blame(&fixture.slug, "bob", &request));
    assert!(
        probe.reject.lock().unwrap().is_none(),
        "revocation hook not reached"
    );
    assert!(revoked.is_err());
    assert!(matches!(
        Operation::new(30)
            .enter(|| fixture.engine.blame(&fixture.slug, "alice", &request))
            .unwrap(),
        Response::Busy
    ));
    drop(revoked);
    wait_for(|| fixture.leases() == 0 && fixture.engine.blame.admitted() == 0);

    // Delete the path, then re-add it and create a long linear suffix. A
    // present-path prefix is not proof that the deletion is a root.
    let mut deletion = format!(
        "commit refs/heads/history\ncommitter Synthetic <synthetic@example.invalid> 1000000050 +0000\ndata 6\ndelete\nfrom {tip}\nD file\n\n"
    );
    native(
        &native_root,
        &["fast-import", "--quiet", "--force"],
        deletion.as_bytes(),
    );
    deletion = text(native(
        &native_root,
        &["rev-parse", "refs/heads/history"],
        b"",
    ));
    let readded = history(&native_root, 40, &deletion);
    publish_history_from(&fixture, &tip, &readded);
    let request = Request::parse(&readded, "path=file").unwrap();
    let fell_back = stage_count("fallback", "completed");
    let response = Operation::new(30)
        .enter(|| fixture.engine.blame(&fixture.slug, "alice", &request))
        .unwrap();
    let body = match response {
        Response::Complete(body) => body,
        Response::Busy => panic!("fallback unexpectedly busy"),
        Response::NotFound => panic!("re-added path not found"),
        Response::TooLarge => panic!("fallback exceeded output limit"),
    };
    let stats = body.work_statistics().unwrap().segments;
    assert!(stats.attempted && !stats.used && stats.lanes == 0 && stats.scanned_edges >= 40);
    assert!(stage_count("fallback", "completed") > fell_back);
    let mut bytes = Vec::new();
    Operation::new(10)
        .enter(|| body.write_to(&mut bytes))
        .unwrap();
    assert_eq!(bytes, native_porcelain(&native_root, &readded));
    drop(body);
    wait_for(|| fixture.leases() == 0);
}

fn publish_history_from(fixture: &source_support::Fixture, previous: &str, tip: &str) {
    Operation::new(30).enter(|| {
        let native_root = fixture.root.join("native");
        let prefix = native_root.join("objects/pack/pack");
        let hash = text(native(
            &native_root,
            &["pack-objects", "--revs", prefix.to_str().unwrap()],
            format!("{tip}\n").as_bytes(),
        ));
        let (snapshot, lease) = metadata::leased_snapshot(&fixture.slug, "alice", true).unwrap();
        let operation = git_rs::scratch::identifier().unwrap();
        let pack = fixture
            .engine
            .store
            .upload_bytes(
                &fixture.repo,
                &operation,
                &hash,
                &std::fs::read(native_root.join(format!("objects/pack/pack-{hash}.pack"))).unwrap(),
                &std::fs::read(native_root.join(format!("objects/pack/pack-{hash}.idx"))).unwrap(),
            )
            .unwrap();
        metadata::publish(
            &snapshot,
            &lease,
            &operation,
            "alice",
            &[Update {
                old: Oid::parse(previous.as_bytes()).unwrap(),
                new: Oid::parse(tip.as_bytes()).unwrap(),
                name: b"refs/heads/main".to_vec(),
            }],
            &[true],
            &[pack],
        )
        .unwrap();
    });
}

struct StopAtCheckpoint(AtomicBool);
impl Observer for StopAtCheckpoint {
    fn observe(&self, phase: Phase, _control: &Control) -> Result<()> {
        if phase == Phase::CheckpointPublished {
            self.0.store(true, Ordering::Release);
            return Err(git_rs::invalid(
                "fixture intentional partial-checkpoint stop",
            ));
        }
        Ok(())
    }
}
struct FenceImported {
    repo: String,
    fired: AtomicBool,
    installed: AtomicBool,
    after_outer: bool,
    outer_succeeded: AtomicBool,
}
impl Observer for FenceImported {
    fn observe(&self, phase: Phase, _control: &Control) -> Result<()> {
        if phase == Phase::CheckpointInstalled {
            self.installed.store(true, Ordering::Release);
        }
        if phase == Phase::CheckpointImported {
            self.outer_succeeded.store(true, Ordering::Release);
        }
        if phase
            == if self.after_outer {
                Phase::CheckpointImported
            } else {
                Phase::CheckpointDecoded
            }
        {
            self.fired.store(true, Ordering::Release);
            metadata::sql(&format!(
                "UPDATE repositories SET storage_epoch=storage_epoch+1 WHERE id={}::uuid",
                metadata::text(&self.repo)
            ))?;
        }
        Ok(())
    }
}

#[test]
#[ignore = "child entrypoint for exclusively owned PostgreSQL/S3 fixture"]
fn checkpoint_reuse_child() {
    let Ok(mode) = std::env::var("GIT_RS_REUSE_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os("GIT_RS_REUSE_ROOT").unwrap());
    let slug = std::env::var("GIT_RS_REUSE_SLUG").unwrap();
    let tip = std::env::var("GIT_RS_REUSE_TIP").unwrap();
    if let Some(mode) = mode.strip_prefix("completed-") {
        completed_transport::serve(mode, &root, &slug, &tip);
        return;
    }
    if let Some(mode) = mode.strip_prefix("immutable-") {
        immutable_body::serve(mode, &root, &slug, &tip);
        return;
    }
    // Explicit fixture profile only; production BlameOptions::default is unchanged.
    let mut tuning = BlameOptions::default().tuning();
    tuning.reuse_line_tokens = true;
    tuning.retain_current_input_tokens = true;
    tuning.reuse_unchanged_runs = true;
    tuning.bounded_resource_cache = true;
    tuning.index_changes = true;
    let options = BlameOptions::native_root(tuning);
    let mut blame = Blame::new(1, 0)
        .unwrap()
        .with_authenticated_cache_key(CacheConfig::from_env().unwrap(), &[7; 32])
        .unwrap()
        .with_test_options(options, mode != "off");
    let stopped = Arc::new(StopAtCheckpoint(AtomicBool::new(false)));
    if mode == "partial" {
        blame = blame.with_observer(stopped.clone());
    }
    let fence = Arc::new(FenceImported {
        repo: std::env::var("GIT_RS_REUSE_REPO").unwrap(),
        fired: AtomicBool::new(false),
        installed: AtomicBool::new(false),
        after_outer: mode == "fence-after",
        outer_succeeded: AtomicBool::new(false),
    });
    if mode.starts_with("fence") {
        blame = blame.with_observer(fence.clone());
    }
    let engine = Engine {
        blame,
        store: git_rs::blob::Store::from_env().unwrap(),
        cache: Arc::new(git_rs::cache::Cache::new(1024 * 1024, 8 * 1024 * 1024)),
        operation_seconds: 120,
    };
    let query = Request::parse(&tip, "path=file").unwrap();
    let request_started = Instant::now();
    let response = Operation::new(120).enter(|| engine.blame(&slug, "bob", &query));
    let request_millis = request_started.elapsed().as_millis();
    if mode == "partial" || mode.starts_with("fence") {
        if mode == "partial" {
            assert!(
                stopped.0.load(Ordering::Acquire),
                "partial fixture did not confirm a rolling publication; error={:?}",
                response.as_ref().err().map(ToString::to_string)
            );
        }
        assert!(
            response.is_err(),
            "stopped/fenced computation exposed a complete result"
        );
        if mode.starts_with("fence") {
            assert_eq!(
                fence.outer_succeeded.load(Ordering::Acquire),
                mode == "fence-after",
                "outer post-consumption fence did not behave as expected"
            );
            assert!(
                fence.fired.load(Ordering::Acquire),
                "test did not reach imported temporary snapshot"
            );
            assert!(
                !fence.installed.load(Ordering::Acquire),
                "fenced temporary units were installed in the live workset"
            );
        }
        // Returned errors retain actual work admission until their owner is dropped.
        drop(response);
    } else {
        let Response::Complete(body) = response.unwrap() else {
            panic!("expected full real-source result");
        };
        let stats = body
            .work_statistics()
            .expect("forced ready-body miss must expose actual core work");
        let mut output = Vec::new();
        Operation::new(10)
            .enter(|| body.write_to(&mut output))
            .unwrap();
        assert_eq!(
            output,
            fs::read(root.join("expected")).unwrap(),
            "native owner/byte parity"
        );
        if mode == "off" {
            assert_eq!(stats.mapping_hits, 0);
            assert!(stats.fresh_mapping_computations > 256);
        }
        if mode == "warm" {
            assert!(
                stats.mapping_hits > 0,
                "new process did not consume persisted mappings"
            );
        }
        fs::write(
            root.join(format!("{mode}.stats")),
            format!(
                "{} {} {} {} {} {} {} {} {} {}\n",
                stats.mapping_hits,
                stats.fresh_mapping_computations,
                stats.completed_mappings,
                stats.commits_traversed,
                stats.edges_visited,
                stats.objects_reused,
                stats.imported_objects_reused,
                stats.imported_mapping_loads,
                stats.elapsed.as_millis(),
                request_millis
            ),
        )
        .unwrap();
        drop(body);
    }
    let end = Instant::now() + Duration::from_secs(3);
    while engine.blame.admitted() != 0 {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
}

fn child(fixture: &source_support::Fixture, config: &CacheConfig, tip: &str, mode: &str) {
    let out = Command::new("timeout")
        .args(["--kill-after=2s", "150s"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "checkpoint_reuse_child",
            "--ignored",
            "--nocapture",
        ])
        .env("GIT_RS_REUSE_CHILD", mode)
        .env("GIT_RS_REUSE_ROOT", &fixture.root)
        .env("GIT_RS_REUSE_SLUG", &fixture.slug)
        .env("GIT_RS_REUSE_TIP", tip)
        .env("GIT_RS_REUSE_BASE", &fixture.commit)
        .env("GIT_RS_REUSE_REPO", &fixture.repo)
        .env("GIT_RS_CACHE_NAMESPACE", &config.namespace)
        .env("GIT_RS_CACHE_OWNER", &config.owner)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{mode} child failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if mode.starts_with("immutable-replacement-") {
        print!("{}", String::from_utf8_lossy(&out.stdout));
        if mode.ends_with("-20k") {
            eprint!("{}", String::from_utf8_lossy(&out.stderr));
        }
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn real_cross_process_partial_checkpoint_reduces_actual_diff_work_and_import_fence_discards() {
    let fixture = source_support::Fixture::new(Blame::default(), 10);
    let native_root = fixture.root.join("native");
    let tip = history(&native_root, 2000, &fixture.commit);
    publish_history(&fixture, &tip);
    fs::write(
        fixture.root.join("expected"),
        native_porcelain(&native_root, &tip),
    )
    .unwrap();
    let config = config();
    child(&fixture, &config, &tip, "off");
    child(&fixture, &config, &tip, "partial"); // first persisted rolling batch, then real callback failure
    child(&fixture, &config, &tip, "warm"); // entirely new process, no final-body reuse
    let numbers = |mode: &str| {
        fs::read_to_string(fixture.root.join(format!("{mode}.stats")))
            .unwrap()
            .split_whitespace()
            .map(|n| n.parse::<u64>().unwrap())
            .collect::<Vec<_>>()
    };
    let off = numbers("off");
    let warm = numbers("warm");
    println!(
        "ROOT_REUSE tuning=all tip={tip} fields=[hits,computations,completed,commits,edges,objects,imported_objects,imported_mapping_loads,attribution_ms,request_ms] off={off:?} warm={warm:?}"
    );
    assert_eq!(off[3], 2001);
    assert_eq!(off[4], 2000);
    assert_eq!(warm[4], 2000);
    assert_eq!(off[6], 0);
    assert!(warm[6] > 0, "no persisted physical object was reused");
    assert_eq!(
        warm[7], warm[0],
        "imported loads must be actual accepted hits in this fixture"
    );
    assert!(
        warm[0] > 0 && warm[1] < off[1],
        "actual reused hits/computations: off={off:?} warm={warm:?}"
    );
    assert_eq!(
        warm[3], off[3],
        "reuse must preserve full graph traversal semantics"
    );
    // Independently qualify a complete bounded workset, not only its first batch.
    let full_config = self::config();
    child(&fixture, &full_config, &tip, "cold");
    child(&fixture, &full_config, &tip, "warm-full");
    let full = numbers("warm-full");
    let cold = numbers("cold");
    println!("ROOT_FULL_REUSE tuning=all tip={tip} cold={cold:?} full={full:?}");
    assert_eq!(cold[1], 2000);
    assert_eq!(full[0], 2000);
    assert_eq!(
        full[1], 0,
        "full reuse must bypass every actual fresh diff computation"
    );
    assert_eq!(full[3], 2001);
    assert_eq!(full[4], 2000);
    assert!(full[6] > 0);
    assert_eq!(full[7], 2000);
    child(&fixture, &config, &tip, "fence");
    assert_eq!(
        fixture.leases(),
        0,
        "failed imported owner retained source lease"
    );
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn fresh_source_fence_after_successful_outer_consumption_still_prevents_installation() {
    let fixture = source_support::Fixture::new(Blame::default(), 10);
    let tip = history(&fixture.root.join("native"), 3, &fixture.commit);
    publish_history(&fixture, &tip);
    let config = config();
    child(&fixture, &config, &tip, "partial");
    child(&fixture, &config, &tip, "fence-after");
    assert_eq!(fixture.leases(), 0);
}

fn source_cache_bytes(large_fixture: bool) -> usize {
    // The 1 MiB small-case budget cannot admit a full 4 MiB source range.
    if large_fixture { 64 << 20 } else { 1 << 20 }
}

struct HttpServer {
    child: std::process::Child,
    address: String,
    health: String,
    control: std::os::unix::net::UnixListener,
    stderr_drain: Option<std::thread::JoinHandle<()>>,
}
impl HttpServer {
    fn start(fixture: &source_support::Fixture, config: &CacheConfig, warm: bool) -> Self {
        Self::start_with_seconds(fixture, config, warm, "30", false)
    }

    fn start_with_seconds(
        fixture: &source_support::Fixture,
        config: &CacheConfig,
        warm: bool,
        seconds: &str,
        large_fixture: bool,
    ) -> Self {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;
        let control_path = fixture.root.join("warm-control");
        let control = std::os::unix::net::UnixListener::bind(&control_path).unwrap();
        control.set_nonblocking(true).unwrap();
        let mut command = Command::new("timeout");
        command
            .process_group(0)
            .args(["--kill-after=2s", if warm { "75s" } else { "240s" }])
            .arg(
                std::env::var_os("GIT_RS_BLAME_HTTP_FIXTURE")
                    .expect("qualified HTTP fixture executable required"),
            )
            .args([
                "--seconds",
                seconds,
                "--concurrency",
                "1",
                "--waiters",
                "0",
                "--principal",
                "alice",
                "--cache",
                "--checkpoint-fixture-key",
                "--measure-work",
            ])
            .arg("--object-cache-bytes")
            .arg(source_cache_bytes(large_fixture).to_string())
            .arg("--socket")
            .arg(fixture.root.join("serving.sock"));
        if warm {
            command
                .args(["--warm-path", "file", "--phase", "source", "--control"])
                .arg(control_path);
        }
        let mut child = command
            .env("GIT_RS_CACHE_NAMESPACE", &config.namespace)
            .env("GIT_RS_CACHE_OWNER", &config.owner)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut reader = std::io::BufReader::new(child.stderr.take().unwrap());
        let mut server = Self {
            child,
            address: String::new(),
            health: String::new(),
            control,
            stderr_drain: None,
        };
        let (startup, ready) = std::sync::mpsc::sync_channel(1);
        // Drain request logging while the server runs; reap the reader after
        // stopping the owned process group, including on a startup error.
        server.stderr_drain = Some(std::thread::spawn(move || {
            use std::io::Read;
            let addresses = (|| {
                let mut health = String::new();
                let mut address = String::new();
                (&mut reader).take(128).read_line(&mut health).ok()?;
                (&mut reader).take(128).read_line(&mut address).ok()?;
                Some((health, address))
            })();
            let _ = startup.send(addresses);
            let _ = std::io::copy(&mut reader, &mut std::io::sink());
        }));
        let (health, address) = ready
            .recv_timeout(Duration::from_secs(10))
            .ok()
            .flatten()
            .expect("fixture startup addresses unavailable");
        server.health = health
            .trim()
            .strip_prefix("health ")
            .expect("fixture health address")
            .to_owned();
        server.address = address
            .trim()
            .strip_prefix("development HTTP ")
            .expect("fixture HTTP address")
            .to_owned();
        server
    }
    fn metrics(&self) -> Option<String> {
        use std::io::Read;
        const MAX_BYTES: usize = 128 * 1024;
        const REQUEST: &[u8] = b"GET /metrics HTTP/1.1\r\nHost: fixture\r\n\r\n";
        let address: std::net::SocketAddr = self.health.parse().ok()?;
        if !address.ip().is_loopback() {
            return None;
        }
        let budget = Duration::from_secs(2);
        let end = Instant::now() + budget;
        let mut stream = std::net::TcpStream::connect_timeout(&address, budget).ok()?;
        stream
            .set_write_timeout(Some(end.checked_duration_since(Instant::now())?))
            .ok()?;
        if stream.write(REQUEST).ok()? != REQUEST.len() {
            return None;
        }
        let mut output = Vec::new();
        let mut chunk = [0; 4096];
        loop {
            stream
                .set_read_timeout(Some(end.checked_duration_since(Instant::now())?))
                .ok()?;
            let count = stream.read(&mut chunk).ok()?;
            if count == 0 {
                break;
            }
            if output.len() + count > MAX_BYTES {
                return None;
            }
            output.extend_from_slice(&chunk[..count]);
        }
        let output = String::from_utf8(output).ok()?;
        let (headers, body) = output.split_once("\r\n\r\n")?;
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))?
            .parse()
            .ok()?;
        (headers.starts_with("HTTP/1.1 200 ") && body.len() == length).then(|| body.to_owned())
    }
    fn value(&self, name: &str) -> u64 {
        metric_value(&self.metrics().expect("fixture metrics unavailable"), name).unwrap_or(0)
    }
    fn receive_diagnostics(&self) -> String {
        use std::fmt::Write;
        let Some(metrics) = self.metrics() else {
            return "receive metrics unavailable".into();
        };
        let active = metric_value::<u64>(&metrics, "git_rs_active_requests");
        let cache = metric_value::<u64>(&metrics, "git_rs_cache_allocated_bytes");
        let mut summary = format!("receive metrics: active={active:?} cache_bytes={cache:?}");
        // The fixture process owns only this repo's receive endpoint. Export
        // only fixed metric names/labels and parsed numbers, not the wire body.
        for phase in [
            "snapshot",
            "materialize",
            "scratch_init",
            "git",
            "pre_receive",
            "validate",
            "graph",
            "upload",
            "publish",
            "receive_pack_prepare",
            "upload_serve",
        ] {
            let labels = format!("{{phase=\"{phase}\"}}");
            let count = metric_value::<u64>(
                &metrics,
                &format!("git_rs_phase_duration_seconds_count{labels}"),
            );
            let errors =
                metric_value::<u64>(&metrics, &format!("git_rs_phase_errors_total{labels}"));
            let seconds = metric_value::<f64>(
                &metrics,
                &format!("git_rs_phase_duration_seconds_sum{labels}"),
            );
            if let (Some(count), Some(errors), Some(seconds)) = (count, errors, seconds)
                && seconds.is_finite()
                && seconds >= 0.0
            {
                let _ = write!(
                    summary,
                    "\n  {phase}: count={count} errors={errors} total_s={seconds:.9}"
                );
            }
        }
        summary
    }
    fn release(&self, command: u8) {
        use std::io::BufRead;
        let end = Instant::now() + Duration::from_secs(10);
        let mut peer = loop {
            match self.control.accept() {
                Ok((peer, _)) => break peer,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < end,
                        "actual warm source boundary not reached"
                    );
                    std::thread::yield_now();
                }
                Err(error) => panic!("warm control accept: {error}"),
            }
        };
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut event = String::new();
        std::io::BufReader::new(&peer)
            .read_line(&mut event)
            .unwrap();
        assert!(event.contains("SourceAcquired"));
        peer.write_all(&[command]).unwrap();
    }
}

fn metric_value<T: std::str::FromStr>(metrics: &str, name: &str) -> Option<T> {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let _ = Command::new("timeout")
            .args([
                "5s",
                "kill",
                "-TERM",
                "--",
                &format!("-{}", self.child.id()),
            ])
            .status();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.stderr_drain.take() {
            let _ = reader.join();
        }
    }
}
fn wait_for(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < end, "warm ownership did not converge");
        std::thread::yield_now();
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn actual_confirmed_http_push_warms_reusable_work_and_later_warm_failure_does_not_fail_push() {
    warm_push_gate(false);
}
fn warm_push_gate(cache_outage: bool) {
    let cache_config = if cache_outage {
        unregistered_config()
    } else {
        config()
    };
    let blame = Blame::new(1, 0)
        .unwrap()
        .with_authenticated_cache_key(cache_config.clone(), &[7; 32])
        .unwrap()
        .with_test_options(BlameOptions::default(), true);
    let fixture = source_support::Fixture::new(blame, 10);
    let server = HttpServer::start(&fixture, &cache_config, true);
    let native_root = fixture.root.join("native");
    let tip = history(&native_root, 3, &fixture.commit);
    let url = format!("http://{}/{}.git", server.address, fixture.slug);
    // Only the real confirmed receive path can schedule this warm. Default CPU
    // capacity is one: receive ownership must be cleaned up before submission.
    native(
        &native_root,
        &[
            "push",
            "--force",
            &url,
            "refs/heads/history:refs/heads/main",
        ],
        b"",
    );
    wait_for(|| server.value("git_rs_fixture_blame_admitted") == 1 && fixture.leases() == 1);
    // A departed query socket has no ownership of this no-waiter warm job.
    let mut departed = std::net::TcpStream::connect(&server.address).unwrap();
    write!(
        departed,
        "GET /{}.git/blame/{tip}?path=file HTTP/1.1\r\nHost: fixture\r\n\r\n",
        fixture.slug
    )
    .unwrap();
    drop(departed);
    assert_eq!(server.value("git_rs_fixture_blame_admitted"), 1);
    assert_eq!(
        fixture.leases(),
        1,
        "warm source must remain held independently"
    );
    server.release(b'G');
    wait_for(|| {
        server.value("git_rs_fixture_warm_completed") == 1
            && server.value("git_rs_fixture_blame_admitted") == 0
    });
    assert!(server.value("git_rs_fixture_work_last_mapping_computations{kind=\"warm\"}") > 0);
    let query = Request::parse(&tip, "path=file").unwrap();
    let response = Operation::new(20)
        .enter(|| fixture.engine.blame(&fixture.slug, "alice", &query))
        .unwrap();
    let Response::Complete(body) = response else {
        panic!("foreground after real warm was not complete");
    };
    if cache_outage {
        assert_eq!(body.work_statistics().unwrap().mapping_hits, 0);
        assert!(body.work_statistics().unwrap().fresh_mapping_computations > 0);
    } else {
        assert!(
            body.work_statistics().unwrap().mapping_hits > 0,
            "warm checkpoint must avoid actual fresh mapping work"
        );
    }
    let mut output = Vec::new();
    Operation::new(10)
        .enter(|| body.write_to(&mut output))
        .unwrap();
    assert_eq!(
        output,
        native(
            &native_root,
            &[
                "blame",
                "--root",
                "--line-porcelain",
                "--no-textconv",
                "--no-progress",
                &tip,
                "--",
                "file"
            ],
            b""
        )
    );
    drop(body);
    wait_for(|| fixture.engine.blame.admitted() == 0);
    let second = history(&native_root, 4, &fixture.commit);
    native(
        &native_root,
        &[
            "push",
            "--force",
            &url,
            "refs/heads/history:refs/heads/main",
        ],
        b"",
    );
    wait_for(|| server.value("git_rs_fixture_blame_admitted") == 1 && fixture.leases() == 1);
    let (snapshot, lease) = Operation::new(5)
        .enter(|| metadata::leased_snapshot(&fixture.slug, "alice", false))
        .unwrap();
    assert_eq!(
        snapshot.refs.get(b"refs/heads/main".as_slice()),
        Some(&Oid::parse(second.as_bytes()).unwrap()),
        "warm failure must not roll back or reject confirmed source publication"
    );
    drop(lease);
    // With CPU1/queue0, a paused warm owns the only shared slot. A new push is
    // rejected before publication; it is not a confirmed push failed by warming.
    let third = history(&native_root, 5, &fixture.commit);
    let busy = native_output(
        &native_root,
        &[
            "push",
            "--force",
            &url,
            "refs/heads/history:refs/heads/main",
        ],
        b"",
    );
    assert!(
        !busy.status.success(),
        "shared admission cannot accept another push"
    );
    assert!(
        !matches!(busy.status.code(), Some(124 | 137)),
        "busy must not wait for the warm deadline"
    );
    assert_eq!(server.value("git_rs_fixture_blame_admitted"), 1);
    assert_eq!(server.value("git_rs_fixture_blame_waiters"), 0);
    server.release(b'C');
    wait_for(|| {
        server.value("git_rs_fixture_warm_failed") == 1
            && server.value("git_rs_fixture_blame_admitted") == 0
    });
    let (snapshot, lease) = Operation::new(5)
        .enter(|| metadata::leased_snapshot(&fixture.slug, "alice", false))
        .unwrap();
    assert_eq!(
        snapshot.refs.get(b"refs/heads/main".as_slice()),
        Some(&Oid::parse(second.as_bytes()).unwrap()),
        "a rejected new push must not change the previously confirmed publication"
    );
    drop(lease);
    // After actual warm cleanup, retry the same native push. It can confirm and
    // schedule a new independent warm; failure of that warm cannot roll it back.
    native(
        &native_root,
        &[
            "push",
            "--force",
            &url,
            "refs/heads/history:refs/heads/main",
        ],
        b"",
    );
    wait_for(|| server.value("git_rs_fixture_blame_admitted") == 1 && fixture.leases() == 1);
    server.release(b'E');
    wait_for(|| {
        server.value("git_rs_fixture_warm_failed") == 2
            && server.value("git_rs_fixture_blame_admitted") == 0
    });
    let (snapshot, lease) = Operation::new(5)
        .enter(|| metadata::leased_snapshot(&fixture.slug, "alice", false))
        .unwrap();
    assert_eq!(
        snapshot.refs.get(b"refs/heads/main".as_slice()),
        Some(&Oid::parse(third.as_bytes()).unwrap())
    );
    drop(lease);
    drop(server);
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "child entrypoint for exclusively owned PostgreSQL/S3 fixture"]
fn warm_cache_outage_child() {
    if std::env::var("GIT_RS_WARM_CACHE_OUTAGE").as_deref() == Ok("1") {
        warm_push_gate(true);
    }
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn actual_cache_network_outage_keeps_warming_optional_and_push_confirmed() {
    // Own a bounded loopback listener without accepting: cache PostgreSQL
    // handshakes time out, while source metadata uses its original healthy pool.
    let blackhole = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = blackhole.local_addr().unwrap().port();
    let output = Command::new("timeout")
        .args(["--kill-after=2s", "80s"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "warm_cache_outage_child",
            "--ignored",
            "--nocapture",
        ])
        .env("GIT_RS_WARM_CACHE_OUTAGE", "1")
        .env(
            "GIT_RS_CACHE_DSN",
            format!("host=127.0.0.1 port={port} user=synthetic dbname=synthetic"),
        )
        .env("GIT_RS_CACHE_PGSSLMODE", "disable")
        .env("GIT_RS_DEV_HTTP", "1")
        .output()
        .unwrap();
    drop(blackhole);
    assert!(
        output.status.success(),
        "cache-outage child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
