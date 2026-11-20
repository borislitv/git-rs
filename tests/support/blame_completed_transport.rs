//! Serving contract for general completed maps, independent of legacy seeds.
use super::{HttpServer, cache_support::Admin, child, config, immutable_body::get_path_result};
use super::{native, source_support::Fixture, text, wait_for};
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase, WorkStatistics},
    blob::Store,
    cache::Cache,
    derived_cache::{CacheConfig, MAX_CHUNK_BYTES},
    engine::Engine,
    metadata,
    process::Operation,
    protocol::Oid,
};
use git_rs_core::Control;
use std::{
    fs,
    io::Write,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

const PATH: &str = "docs/subject";
const CASE: &str = "general completed map";
const PRE_BASE_FILE: &str = "completed-pre-base";

struct Trace {
    phases: Mutex<Vec<Phase>>,
    work: Mutex<Vec<WorkStatistics>>,
    pre_base: [Oid; 2],
    old_headers: AtomicUsize,
    old_objects: AtomicUsize,
}

impl Observer for Trace {
    fn observe(&self, phase: Phase, _: &Control) -> Result<()> {
        self.phases.lock().unwrap().push(phase);
        Ok(())
    }

    fn work_completed(&self, warm: bool, stats: WorkStatistics, _: &Control) {
        assert!(!warm);
        self.work.lock().unwrap().push(stats);
    }

    fn source_read(&self, id: &gix::hash::oid, header_only: bool, _: &Control) {
        if self
            .pre_base
            .iter()
            .any(|old| id.as_bytes() == old.0.as_slice())
        {
            let count = if header_only {
                &self.old_headers
            } else {
                &self.old_objects
            };
            count.fetch_add(1, Relaxed);
        }
    }
}

pub(super) fn serve(mode: &str, root: &Path, slug: &str, tip: &str) {
    let inputs = fs::read_to_string(root.join(PRE_BASE_FILE)).unwrap();
    let ids = inputs.lines().collect::<Vec<_>>();
    let [root_commit, prior_commit] = ids.as_slice() else {
        panic!("fixture needs exactly the two pre-base commit IDs");
    };
    let trace = Arc::new(Trace {
        phases: Mutex::default(),
        work: Mutex::default(),
        pre_base: [root_commit, prior_commit].map(|id| Oid::parse(id.as_bytes()).unwrap()),
        old_headers: AtomicUsize::new(0),
        old_objects: AtomicUsize::new(0),
    });
    let engine = Engine {
        blame: Blame::new(1, 0)
            .unwrap()
            .with_authenticated_cache_key(
                CacheConfig::from_env().unwrap(),
                &[if mode == "wrong-key" { 8 } else { 7 }; 32],
            )
            .unwrap()
            .with_observer(trace.clone()),
        store: Store::from_env().unwrap(),
        cache: Arc::new(Cache::new(64 << 20, 8 << 20)),
        operation_seconds: 120,
    };
    let (wire, result) = get_path_result(&engine, slug, tip, PATH);
    let end = wire.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let headers = std::str::from_utf8(&wire[..end]).unwrap();
    let body = &wire[end + 4..];
    let phases = trace.phases.lock().unwrap();
    let work = trace.work.lock().unwrap();
    let old_reads = (
        trace.old_headers.load(Relaxed),
        trace.old_objects.load(Relaxed),
    );
    if mode == "revoked" {
        // ACL denial must not deliver a complete or partial attribution.
        assert!(result.is_err());
        assert!(
            headers.starts_with("HTTP/1.1 500 Internal Server Error\r\n"),
            "{headers}"
        );
        assert!(body.is_empty());
        assert!(work.is_empty());
        assert_eq!(old_reads, (0, 0));
        assert!(!phases.contains(&Phase::CompletedMapHit));
        assert!(!phases.contains(&Phase::CompletedMapPublished));
        return;
    }
    result.unwrap();
    assert!(
        headers.starts_with("HTTP/1.1 200 OK\r\n"),
        "{mode}: {headers}"
    );
    let lengths: Vec<_> = headers
        .split("\r\n")
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .collect();
    assert_eq!(lengths, [body.len()]);
    let expected = native(
        &root.join("native"),
        &[
            "--no-replace-objects",
            "--literal-pathspecs",
            "blame",
            "--root",
            "--line-porcelain",
            "--no-textconv",
            "--no-progress",
            "--diff-algorithm=myers",
            tip,
            "--",
            PATH,
        ],
        b"",
    );
    assert!(
        body == expected,
        "{mode}: native porcelain mismatch ({} vs {} bytes)",
        body.len(),
        expected.len()
    );
    assert!(phases.contains(&Phase::PreflightTip), "{mode}: {phases:?}");
    for alternative in [
        Phase::CacheHit,
        Phase::ParentBodyHit,
        Phase::ReplacementSeedHit,
    ] {
        assert!(!phases.contains(&alternative), "{mode}: {phases:?}");
    }
    let [stats] = work.as_slice() else {
        panic!("{mode}: expected one attribution run")
    };
    assert_eq!(stats.replacement_suffix_edges, 0);
    match mode {
        "seed" => {
            assert!(phases.contains(&Phase::CompletedMapPublished));
            assert!(!phases.contains(&Phase::CompletedMapHit));
            assert_eq!(stats.completed_map_lines, 0);
        }
        "hit-1" | "hit-33" => {
            let suffix = if mode == "hit-1" { 1 } else { 33 };
            assert!(phases.contains(&Phase::CompletedMapHit));
            assert!(!phases.contains(&Phase::CompletedMapPublished));
            assert!(!stats.completed_map_miss);
            assert!(stats.completed_map_lines >= 16, "{mode}: {stats:?}");
            // Provider counters include preflight and discovery, unlike core stats.
            assert_eq!(
                old_reads,
                (0, 0),
                "{mode}: pre-base (header, object) lookups"
            );
            assert!(stats.commits_traversed <= suffix + 1, "{mode}: {stats:?}");
            assert!(
                stats.fresh_mapping_computations > 0,
                "{mode}: changed suffix must diff"
            );
        }
        "ordinary" | "wrong-key" | "wrong-epoch" | "reordered" => {
            assert!(
                !phases.contains(&Phase::CompletedMapHit),
                "{mode}: {phases:?}"
            );
            assert_eq!(stats.completed_map_lines, 0);
            if mode != "ordinary" {
                assert!(stats.completed_map_miss, "{mode}: {stats:?}");
            }
            assert!(
                old_reads.0 > 0 && old_reads.1 > 0,
                "{mode}: expected pre-base header and object lookups, got {old_reads:?}"
            );
            assert!(
                stats.commits_traversed > 34,
                "{mode}: must traverse below base: {stats:?}"
            );
        }
        _ => panic!("unknown {CASE} child mode: {mode}"),
    }
}

fn history(root: &Path, start: &str) -> Vec<String> {
    let mut stream = Vec::new();
    // The base changes one line which survives every suffix. A large synthetic
    // subject for that line forces genuine multipart map transport without
    // hundreds of thousands of attribution rows or quadratic diff input.
    let wide_subject = "s".repeat(MAX_CHUNK_BYTES + 8192) + "\n";
    for step in 0..=34 {
        let message = if step == 1 {
            &wide_subject[..]
        } else {
            "Synthetic commit\n"
        };
        let parent = if step == 0 {
            start.to_owned()
        } else {
            format!(":{step}")
        };
        let mark = step + 1;
        writeln!(stream, "commit refs/heads/completed-map\nmark :{mark}\ncommitter Synthetic <synthetic@example.invalid> {} +0000\ndata {}", 1_000_010_000 + step, message.len()).unwrap();
        write!(stream, "{message}\nfrom {parent}\n").unwrap();
        let mut file = format!("{}\n", if step == 0 { "old line" } else { "base owner" });
        for n in 0..24 {
            if step >= 2 && n == 3 {
                continue;
            }
            writeln!(file, "stable-{n:02}\ttext").unwrap();
            if step >= 2 && n == 9 {
                writeln!(file, "inserted anchor\nedited suffix {step}").unwrap();
            }
        }
        let other = format!("independently edited sibling {step}\n");
        for (path, contents) in [(PATH, &file), ("other", &other)] {
            write!(
                stream,
                "M 100644 inline {path}\ndata {}\n{contents}\n",
                contents.len()
            )
            .unwrap();
        }
        stream.push(b'\n');
    }
    stream.extend_from_slice(b"done\n");
    native(root, &["fast-import", "--quiet"], &stream);
    text(native(
        root,
        &[
            "rev-list",
            "--reverse",
            "refs/heads/completed-map",
            "--not",
            start,
        ],
        b"",
    ))
    .lines()
    .map(str::to_owned)
    .collect()
}

fn map_parts(admin: &Admin, cfg: &CacheConfig) -> (String, Vec<usize>) {
    let namespace = &cfg.namespace;
    let root = admin.scalar(&format!(
        "SELECT m.payload::text FROM git_rs_cache.manifest_roots m \
        JOIN git_rs_cache.directory d ON d.namespace=m.namespace AND d.payload=m.payload \
        JOIN git_rs_cache.jobs j ON j.namespace=d.namespace AND j.key_hash=d.key_hash \
        WHERE m.namespace='{namespace}' AND d.state='ready' AND m.kind=1 \
        AND position(convert_to('blame-completed-attribution-v1','UTF8') in j.canonical)>0"
    ));
    let parts = admin.scalar(&format!(
        "SELECT string_agg(size::text,',' ORDER BY ordinal) \
        FROM git_rs_cache.manifest_children WHERE namespace='{namespace}' AND parent='{root}'"
    ));
    (root, parts.split(',').map(|s| s.parse().unwrap()).collect())
}

fn root_count(admin: &Admin, cfg: &CacheConfig) -> String {
    admin.scalar(&format!(
        "SELECT count(*)::text FROM git_rs_cache.manifest_roots \
        WHERE namespace='{}' AND kind=1",
        cfg.namespace
    ))
}

#[test]
#[ignore = "requires exclusively owned loopback PostgreSQL/S3, private tmpfs and Git 2.56.0"]
fn signed_general_map_survives_native_fast_forwards_and_serving_restarts() {
    let fixture = Fixture::new(Blame::default(), 4);
    let native_root = fixture.root.join("native");
    assert_eq!(
        text(native(&native_root, &["--version"], b"")),
        "git version 2.56.0"
    );
    let commits = history(&native_root, &fixture.commit);
    assert_eq!(commits.len(), 35);
    fs::write(
        fixture.root.join(PRE_BASE_FILE),
        format!("{}\n{}\n", fixture.commit, commits[0]),
    )
    .unwrap();
    let cfg = config();
    let wrong_key = config();
    let reorder = config();
    let admin = Admin::new();
    admin.sql("SET ROLE git_rs_cache; SET search_path=git_rs_cache,pg_catalog");
    let server = HttpServer::start_with_seconds(&fixture, &cfg, false, "60", true);
    let url = format!("http://{}/{}.git", server.address, fixture.slug);
    let snapshot = || {
        Operation::new(5)
            .enter(|| metadata::snapshot(&fixture.slug, "alice", false))
            .unwrap()
    };
    let initial = snapshot();
    let mut generation = initial.generation;
    let mut push = |tip: &str| {
        native(
            &native_root,
            &["push", &url, &format!("{tip}:refs/heads/main")],
            b"",
        );
        wait_for(|| fixture.leases() == 0);
        let current = snapshot();
        generation += 1;
        assert_eq!(current.generation, generation);
        assert_eq!(current.storage_epoch, initial.storage_epoch);
        assert_eq!(
            current.refs.get(b"refs/heads/main".as_slice()),
            Some(&Oid::parse(tip.as_bytes()).unwrap())
        );
    };
    let probe = |cache: &CacheConfig, mode: &str, tip: &str| {
        child(&fixture, cache, tip, &format!("completed-{mode}"));
        assert_eq!(fixture.leases(), 0, "{mode}: source holds must drain");
    };
    let (base, one, thirty_three) = (&commits[1], &commits[2], &commits[34]);
    push(&commits[0]);
    push(base);
    for cache in [&cfg, &wrong_key, &reorder] {
        probe(cache, "seed", base);
        let (_, parts) = map_parts(&admin, cache);
        assert_eq!(parts.len(), 2, "fixture must publish a two-part map");
        assert_eq!(parts[0], MAX_CHUNK_BYTES);
        assert!((1..MAX_CHUNK_BYTES).contains(&parts[1]));
    }
    let original_roots = root_count(&admin, &cfg);
    push(one);
    probe(&cfg, "hit-1", one);
    assert_eq!(
        root_count(&admin, &cfg),
        original_roots,
        "graft must not publish at child"
    );
    push(&commits[9]); // advance without deriving/warming an intermediate map
    push(&commits[17]);
    push(thirty_three);
    drop(server);
    probe(&cfg, "hit-33", thirty_three);
    assert_eq!(
        root_count(&admin, &cfg),
        original_roots,
        "graft must not publish at tip"
    );
    fixture.revoke();
    probe(&cfg, "revoked", thirty_three);
    fixture.restored_reader();
    probe(&cfg, "hit-33", thirty_three);

    // Same authorized target and oracle, but no ready body, map or checkpoint
    // in this separate namespace. Neither tampering nor a prior miss can seed it.
    let ordinary = config();
    assert_eq!(root_count(&admin, &ordinary), "0");
    probe(&ordinary, "ordinary", thirty_three);

    probe(&wrong_key, "wrong-key", thirty_three);
    let (map, mut parts) = map_parts(&admin, &reorder);
    // Swap only the stored edge ordinals. Leave the signed descriptor and
    // physical parts intact; a reader must never accept the reordered edges.
    admin.sql(&format!(
        "BEGIN; UPDATE git_rs_cache.manifest_children SET ordinal=ordinal+2 \
        WHERE namespace='{}' AND parent='{map}'; \
        UPDATE git_rs_cache.manifest_children SET ordinal=3-ordinal \
        WHERE namespace='{}' AND parent='{map}'; COMMIT",
        reorder.namespace, reorder.namespace
    ));
    parts.reverse();
    assert_eq!(map_parts(&admin, &reorder), (map, parts));
    probe(&reorder, "reordered", thirty_three);
    assert_eq!(
        map_parts(&admin, &cfg).1.len(),
        2,
        "base remains ready before epoch change"
    );
    fixture.fence("storage_epoch=storage_epoch+1");
    probe(&cfg, "wrong-epoch", thirty_three);
}
use std::fmt::Write as _;
