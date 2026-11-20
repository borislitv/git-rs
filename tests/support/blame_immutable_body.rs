//! Immutable-body serving across real pushes and independent serving processes.
use super::{
    HttpServer, child, config, native, native_porcelain, source_cache_bytes,
    source_support::Fixture, text,
};
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase, WorkStatistics},
    blob::Store,
    cache::Cache,
    derived_cache::CacheConfig,
    engine::Engine,
    http, metadata,
    process::Operation,
    protocol::Oid,
};
use git_rs_core::Control;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Trace {
    phases: Mutex<Vec<Phase>>,
    work: Mutex<Vec<WorkStatistics>>,
    started: Option<Instant>,
}
impl Observer for Trace {
    fn observe(&self, phase: Phase, _: &Control) -> Result<()> {
        if let Some(started) = self.started
            && matches!(
                phase,
                Phase::SourceAcquired
                    | Phase::WorkSetPrepared
                    | Phase::BodyBuildClaimed
                    | Phase::Staging
                    | Phase::Complete
            )
        {
            eprintln!(
                "replacement phase={phase:?} elapsed_ms={}",
                started.elapsed().as_millis()
            );
        }
        if matches!(
            phase,
            Phase::PreflightTip
                | Phase::PreflightWalk
                | Phase::CacheHit
                | Phase::ParentBodyHit
                | Phase::ReplacementSeedHit
                | Phase::WorkSetPrepared
        ) {
            self.phases.lock().unwrap().push(phase);
        }
        Ok(())
    }

    fn work_completed(&self, warm: bool, stats: WorkStatistics, _: &Control) {
        assert!(!warm, "there is no push warmer in the serving probe");
        if self.started.is_some() {
            eprintln!(
                "replacement work commits={} compute_ms={}",
                stats.commits_traversed,
                stats.elapsed.as_millis()
            );
        }
        self.work.lock().unwrap().push(stats);
    }
}

fn get(engine: &Engine, slug: &str, commit: &str) -> Vec<u8> {
    get_path(engine, slug, commit, "file")
}

pub(super) fn get_path(engine: &Engine, slug: &str, commit: &str, path: &str) -> Vec<u8> {
    let (wire, result) = get_path_result(engine, slug, commit, path);
    result.unwrap();
    wire
}

pub(super) fn get_path_result(
    engine: &Engine,
    slug: &str,
    commit: &str,
    path: &str,
) -> (Vec<u8>, git_rs::Result<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(engine.operation_seconds + 5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            http::handle(stream, engine, "bob")
        });
        write!(
            client,
            "GET /{slug}.git/blame/{commit}?path={path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).unwrap();
        (bytes, server.join().unwrap()) // includes body write/flush and drop of serving holds
    })
}

pub(super) fn serve(mode: &str, root: &Path, slug: &str, tip: &str) {
    let (mode, large_fixture) = mode
        .strip_suffix("-20k")
        .map_or((mode, false), |mode| (mode, true));
    let trace = Arc::new(Trace {
        started: large_fixture.then(Instant::now),
        ..Trace::default()
    });
    let signing_key = if mode == "replacement-wrong-key" {
        [8; 32]
    } else {
        [7; 32]
    };
    let engine = Engine {
        blame: Blame::new(1, 0)
            .unwrap()
            .with_authenticated_cache_key(CacheConfig::from_env().unwrap(), &signing_key)
            .unwrap()
            .with_observer(trace.clone()),
        store: Store::from_env().unwrap(),
        cache: Arc::new(Cache::new(
            source_cache_bytes(large_fixture),
            8 * 1024 * 1024,
        )),
        operation_seconds: if mode.starts_with("replacement-") {
            120
        } else {
            20
        },
    };
    let base = std::env::var("GIT_RS_REUSE_BASE").unwrap();
    let native_root = root.join("native");
    let request = |commit: &str, phases: &[Phase], compute: bool, denied: bool| {
        let started = std::time::Instant::now();
        let wire = get(&engine, slug, commit);
        let elapsed = started.elapsed();
        let boundary = wire
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .expect("complete HTTP response headers");
        let (headers, body) = (&wire[..boundary], &wire[boundary + 4..]);
        let status = if denied {
            b"HTTP/1.1 404 Not Found\r\n".as_slice()
        } else {
            b"HTTP/1.1 200 OK\r\n".as_slice()
        };
        assert!(headers.starts_with(status), "{mode} {commit}: {headers:?}");
        let lengths: Vec<_> = std::str::from_utf8(headers)
            .unwrap()
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            .collect();
        assert_eq!(lengths, [body.len()], "{mode}: complete HTTP framing");
        if denied {
            assert!(body.is_empty(), "unreachable commit exposed a blame body");
        } else {
            assert_eq!(
                body,
                native_porcelain(&native_root, commit),
                "{mode} {commit}"
            );
        }
        super::wait_for(|| engine.blame.admitted() == 0);
        assert_eq!(
            std::mem::take(&mut *trace.phases.lock().unwrap()),
            phases,
            "{mode} {commit}: exact preflight and lookup path"
        );
        let work = std::mem::take(&mut *trace.work.lock().unwrap());
        assert_eq!(
            work.len(),
            usize::from(compute),
            "{mode} {commit}: attribution runs"
        );
        if mode == "changed" {
            assert!(
                work[0].fresh_mapping_computations > 0,
                "changed file must compute a mapping"
            );
        }
        if mode == "replacement-wrong-key" {
            assert!(work[0].commits_traversed > 0);
            assert_eq!(work[0].replacement_suffix_edges, 0);
        }
        if let Some(edges) = mode.strip_prefix("replacement-hit-") {
            let edges: usize = edges.parse().unwrap();
            let stats = work[0];
            assert_eq!(stats.replacement_suffix_edges, edges);
            assert_eq!(stats.replacement_edges, edges);
            assert_eq!(stats.commits_traversed, 0);
            assert_eq!(stats.fresh_mapping_computations, 0);
            println!(
                "replacement edges={edges} attribution_walk=0 response_ms={}",
                elapsed.as_millis()
            );
        }
    };

    use Phase::{
        CacheHit, ParentBodyHit, PreflightTip, PreflightWalk, ReplacementSeedHit, WorkSetPrepared,
    };
    match mode {
        "seed" => {
            request(tip, &[PreflightTip, WorkSetPrepared], true, false);
            request(tip, &[PreflightTip, CacheHit], false, false); // actual ready tip: zero walks
        }
        "restart" => {
            request(&base, &[PreflightWalk, CacheHit], false, false);
            request(tip, &[PreflightTip, ParentBodyHit], false, false);
        }
        "changed" => request(tip, &[PreflightTip, WorkSetPrepared], true, false),
        "unreachable" => request(&base, &[PreflightWalk], false, true),
        "replacement-base" | "replacement-wrong-key" => {
            request(tip, &[PreflightTip, WorkSetPrepared], true, false);
        }
        "replacement-hit-1" | "replacement-hit-100" => {
            request(tip, &[PreflightTip, ReplacementSeedHit], true, false);
        }
        _ => panic!("unknown immutable-body fixture mode: {mode}"),
    }
}

fn replace_history(root: &Path, base: &str, first: usize, count: usize) -> String {
    let mut input = Vec::new();
    for i in first..first + count {
        let parent = if i == first {
            base.to_owned()
        } else {
            format!(":{}", i - 1)
        };
        let mut content = (0..128)
            .map(|n| format!("synthetic line {n}\n"))
            .collect::<Vec<_>>();
        content[1] = format!("replacement {i}\n");
        let content = content.concat();
        write!(input, "commit refs/heads/replacement\nmark :{i}\ncommitter Synthetic <synthetic@example.invalid> {} +0000\ndata 7\nchange\n\nfrom {parent}\nM 100644 inline file\ndata {}\n{content}\n", 1_000_000_000 + i, content.len()).unwrap();
    }
    input.extend_from_slice(b"done\n");
    native(root, &["fast-import", "--quiet"], &input);
    text(native(root, &["rev-parse", "refs/heads/replacement"], b""))
}

fn signed_replacement_restart(base_commits: usize, check_wrong_key: bool) {
    let fixture = Fixture::new(Blame::default(), 128);
    let config = config();
    let root = fixture.root.join("native");
    let large_fixture = base_commits == 20_000;
    let probe = |tip: &str, mode: &str| {
        let scale = if large_fixture { "-20k" } else { "" };
        child(
            &fixture,
            &config,
            tip,
            &format!("immutable-replacement-{mode}{scale}"),
        );
    };
    let server = HttpServer::start_with_seconds(&fixture, &config, false, "90", large_fixture);
    let url = format!("http://{}/{}.git", server.address, fixture.slug);
    let base = replace_history(&root, &fixture.commit, 1, base_commits - 1);
    // Initial fixture loading is separate from the measured pushes and reads.
    super::publish_history(&fixture, &base);
    probe(&base, "base");
    let one = replace_history(&root, &base, base_commits, 1);
    let started = Instant::now();
    let output = super::native_output_with_deadline(
        &root,
        &["push", &url, &format!("{one}:refs/heads/main")],
        b"",
        "95s",
    );
    if !output.status.success() {
        let elapsed_ms = started.elapsed().as_millis();
        // Report the known native transport failure without printing stderr.
        let curl_52 = output.stderr[..output.stderr.len().min(8192)]
            .split(|&byte| byte == b'\n')
            .any(|line| {
                line.starts_with(b"error: RPC failed;")
                    && line
                        .windows(b" curl 52 ".len())
                        .any(|part| part == b" curl 52 ")
            });
        panic!(
            "replacement +1 native push: {} after {elapsed_ms} ms (curl_52={curl_52})\n{}",
            output.status,
            server.receive_diagnostics()
        );
    }
    super::wait_for(|| fixture.leases() == 0);
    probe(&one, "hit-1");
    let hundred = replace_history(&root, &one, base_commits + 1, 99);
    // Use the production catalog transaction with an independently generated
    // pack for the bulk advance. The +1 case exercises native HTTP receive;
    // the larger fixture isolates serving from receive throughput and limits.
    super::publish_history_from(&fixture, &one, &hundred);
    probe(&hundred, "hit-100");
    if check_wrong_key {
        // Same cache owner, namespace and source. Only the signer changes;
        // the known-ready base seed must now fall back to normal attribution.
        probe(&hundred, "wrong-key");
    }
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn signed_replacement_seed_serves_changed_tips_after_restart() {
    signed_replacement_restart(8, true);
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn signed_replacement_seed_serves_changed_tips_after_restart_at_20k() {
    signed_replacement_restart(20_000, false);
}

fn append(root: &Path, parent: &str, path: &str, content: &str) -> String {
    let input = format!(
        "commit refs/heads/history\ncommitter Synthetic <synthetic@example.invalid> 1000000001 +0000\ndata 7\nchange\n\nfrom {parent}\nM 100644 inline {path}\ndata {}\n{content}\ndone\n",
        content.len()
    );
    native(root, &["fast-import", "--quiet"], input.as_bytes());
    text(native(root, &["rev-parse", "refs/heads/history"], b""))
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3 and private tmpfs"]
fn signed_immutable_body_serves_after_push_and_process_restart_but_not_after_unreachability() {
    let fixture = Fixture::new(Blame::default(), 10);
    let config = config();
    let native_root = fixture.root.join("native");
    let probe = |mode: &str, tip: &str| {
        child(&fixture, &config, tip, mode);
        assert_eq!(fixture.leases(), 0, "{mode}: source holds must drain");
    };
    // Publish through smart HTTP; warming must not seed or mask either lookup.
    let server = HttpServer::start(&fixture, &config, false);
    let url = format!("http://{}/{}.git", server.address, fixture.slug);
    let push = |commit: &str, branch: &str, force: bool| {
        let refspec = format!(
            "{}{commit}:refs/heads/{branch}",
            if force { "+" } else { "" }
        );
        native(&native_root, &["push", &url, &refspec], b"");
        super::wait_for(|| fixture.leases() == 0);
    };
    // Keep the real default-branch protection in force. Give the fixture a
    // separate default branch that cannot retain the tested ancestry.
    push(&fixture.dangling, "default", false);
    fixture.fence("default_ref=convert_to('refs/heads/default','UTF8')");
    let assert_refs = |tip: &str| {
        let snapshot = Operation::new(5)
            .enter(|| metadata::snapshot(&fixture.slug, "alice", false))
            .unwrap();
        assert_eq!(snapshot.default_ref, b"refs/heads/default");
        assert_eq!(snapshot.refs.len(), 2, "no other refs may retain ancestry");
        assert_eq!(
            snapshot.refs.get(b"refs/heads/main".as_slice()),
            Some(&Oid::parse(tip.as_bytes()).unwrap())
        );
        assert_eq!(
            snapshot.refs.get(b"refs/heads/default".as_slice()),
            Some(&Oid::parse(fixture.dangling.as_bytes()).unwrap())
        );
    };
    assert_refs(&fixture.commit);
    probe("immutable-seed", &fixture.commit);

    let child_tip = append(&native_root, &fixture.commit, "other", "another file\n");
    push(&child_tip, "main", false);
    assert_refs(&child_tip);
    probe("immutable-restart", &child_tip);

    let changed = append(
        &native_root,
        &child_tip,
        "file",
        "synthetic line 0\nnew line\n",
    );
    push(&changed, "main", false);
    assert_refs(&changed);
    probe("immutable-changed", &changed);

    // The replacement was already stored, but has no ancestry in common with base.
    push(&fixture.dangling, "main", true);
    assert_refs(&fixture.dangling);
    probe("immutable-unreachable", &fixture.dangling);
}
