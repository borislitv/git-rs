//! Immutable-body serving across real pushes and independent serving processes.
use super::{HttpServer, child, config, native, native_porcelain, source_support::Fixture, text};
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
    time::Duration,
};

#[derive(Default)]
struct Trace {
    phases: Mutex<Vec<Phase>>,
    work: Mutex<Vec<WorkStatistics>>,
}
impl Observer for Trace {
    fn observe(&self, phase: Phase, _: &Control) -> Result<()> {
        if matches!(
            phase,
            Phase::PreflightTip
                | Phase::PreflightWalk
                | Phase::CacheHit
                | Phase::ParentBodyHit
                | Phase::WorkSetPrepared
        ) {
            self.phases.lock().unwrap().push(phase);
        }
        Ok(())
    }

    fn work_completed(&self, warm: bool, stats: WorkStatistics, _: &Control) {
        assert!(!warm, "there is no push warmer in the serving probe");
        self.work.lock().unwrap().push(stats);
    }
}

fn get(engine: &Engine, slug: &str, commit: &str) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(25)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            let (stream, _) = listener.accept().unwrap();
            http::handle(stream, engine, "bob").unwrap();
        });
        write!(
            client,
            "GET /{slug}.git/blame/{commit}?path=file HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).unwrap();
        server.join().unwrap(); // includes body write/flush and drop of serving holds
        bytes
    })
}

pub(super) fn serve(mode: &str, root: &Path, slug: &str, tip: &str) {
    let trace = Arc::new(Trace::default());
    let engine = Engine {
        blame: Blame::new(1, 0)
            .unwrap()
            .with_authenticated_cache_key(CacheConfig::from_env().unwrap(), &[7; 32])
            .unwrap()
            .with_observer(trace.clone()),
        store: Store::from_env().unwrap(),
        cache: Arc::new(Cache::new(1024 * 1024, 8 * 1024 * 1024)),
        operation_seconds: 20,
    };
    let base = std::env::var("GIT_RS_REUSE_BASE").unwrap();
    let native_root = root.join("native");
    let request = |commit: &str, phases: &[Phase], compute: bool, denied: bool| {
        let wire = get(&engine, slug, commit);
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
    };

    use Phase::{CacheHit, ParentBodyHit, PreflightTip, PreflightWalk, WorkSetPrepared};
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
        _ => panic!("unknown immutable-body fixture mode: {mode}"),
    }
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
