#[path = "support/blame_fixture.rs"]
mod fixture;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use fixture::{Fixture, Provider, control};
use git_rs_core::{
    BlameOptions, BlameRequest, CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity,
    SnapshotSession, VerifiedObjectProvider, blame::MAX_BODY_BYTES, objects::Result,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

const PATH: &[u8] = b"file";
const IMPORT_REF: &str = "refs/heads/replacement";

fn request(commit: &ObjectId) -> BlameRequest<'_> {
    BlameRequest { commit, path: PATH }
}

fn import_history(
    fixture: &Fixture,
    base_len: usize,
    last_hit: usize,
) -> (Provider, Vec<ObjectId>) {
    let count = base_len + last_hit + 1; // Also import one commit beyond the last hit.
    let tail: String = (2..128).map(|i| format!("stable-{i:03}\n")).collect();
    let mut stream = Vec::new();
    for i in 0..count {
        // Make the last successful edge a no-op. Its changed line still belongs
        // to the preceding commit, not the requested tip.
        let version = if i == base_len - 1 + last_hit {
            i - 1
        } else {
            i
        };
        let blob = format!("stable-000\nreplacement-{version:05}\n{tail}");
        let message = format!("Update {i}");
        let mark = i + 1;
        let time = 1_700_000_000 + i;
        write!(
            stream,
            "commit {IMPORT_REF}\nmark :{mark}\nauthor Author <author@example.invalid> {time} -0700\n\
             committer Committer <committer@example.invalid> {time} +0530\ndata {}\n{message}\n",
            message.len(),
        )
        .unwrap();
        if i != 0 {
            writeln!(stream, "from :{i}").unwrap();
        }
        write!(
            stream,
            "M 100644 inline file\ndata {}\n{blob}\n\n",
            blob.len()
        )
        .unwrap();
    }
    stream.extend_from_slice(b"done\n");
    fixture.git(&["fast-import", "--quiet"], None, &stream);
    drop(stream);

    let revs = fixture.git(
        &["rev-list", "--reverse", "--first-parent", IMPORT_REF],
        None,
        b"",
    );
    let commits: Vec<_> = std::str::from_utf8(&revs)
        .unwrap()
        .lines()
        .map(|id| ObjectId::from_hex(id.as_bytes()).unwrap())
        .collect();
    assert_eq!(commits.len(), count);

    // Only fixture setup touches native Git. Subsequent provider reads use the
    // physical object bytes, including binary tree records (not pretty-print).
    let batch = fixture.git(&["cat-file", "--batch-all-objects", "--batch"], None, b"");
    let mut remaining = batch.as_slice();
    let mut objects = BTreeMap::new();
    while !remaining.is_empty() {
        let header_len = remaining.iter().position(|byte| *byte == b'\n').unwrap();
        let (header, rest) = remaining.split_at(header_len);
        let mut fields = std::str::from_utf8(header)
            .unwrap()
            .split_ascii_whitespace();
        let id = ObjectId::from_hex(fields.next().unwrap().as_bytes()).unwrap();
        let kind = match fields.next().unwrap() {
            "commit" => Kind::Commit,
            "tree" => Kind::Tree,
            "blob" => Kind::Blob,
            other => panic!("unexpected fixture object type: {other}"),
        };
        let size: usize = fields.next().unwrap().parse().unwrap();
        assert!(fields.next().is_none());
        let (contents, rest) = rest[1..].split_at(size);
        remaining = rest.strip_prefix(b"\n").unwrap();
        assert!(objects.insert(id, (kind, contents.to_vec())).is_none());
    }
    let mut provider = fixture.provider();
    provider.objects = Arc::new(objects);
    (provider, commits)
}

#[derive(Default, Debug)]
struct ForbiddenReads {
    headers: AtomicUsize,
    bodies: AtomicUsize,
}

impl ForbiddenReads {
    fn total(&self) -> usize {
        self.headers.load(Relaxed) + self.bodies.load(Relaxed)
    }
}

struct SuffixOnly<'a> {
    inner: &'a Provider,
    old_commits: &'a BTreeSet<ObjectId>,
    reads: &'a ForbiddenReads,
}

impl SuffixOnly<'_> {
    fn reject_old(&self, id: &oid, counter: &AtomicUsize) -> Result<()> {
        // Count attempts by ID before delegation, including reads that would
        // miss in the backing map. The base itself remains readable.
        if self.old_commits.contains(id) {
            counter.fetch_add(1, Relaxed);
            return Err("commit below the replacement base is unavailable".into());
        }
        Ok(())
    }
}

impl VerifiedObjectProvider for SuffixOnly<'_> {
    fn identity(&self) -> &SnapshotIdentity {
        self.inner.identity()
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        self.reject_old(id, &self.reads.headers)?;
        self.inner.header(id, control)
    }

    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.reject_old(id, &self.reads.bodies)?;
        self.inner.object(id, control, budget, limits)
    }
}

fn open<P: VerifiedObjectProvider>(provider: P, budget: &CapacityBudget) -> SnapshotSession<P> {
    let identity = provider.identity().clone();
    SnapshotSession::open(
        provider,
        &identity,
        Control::new(Instant::now() + Duration::from_secs(120)),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap()
}

fn open_guarded<'a>(
    inner: &'a Provider,
    old_commits: &'a BTreeSet<ObjectId>,
    reads: &'a ForbiddenReads,
    budget: &CapacityBudget,
) -> SnapshotSession<SuffixOnly<'a>> {
    open(
        SuffixOnly {
            inner,
            old_commits,
            reads,
        },
        budget,
    )
}

fn check_suffix_reuse(base_len: usize, last_hit: usize) {
    let fixture = Fixture::new();
    let (mut provider, commits) = import_history(&fixture, base_len, last_hit);
    let base_index = base_len - 1;
    let results = [1, last_hit].map(|edges| {
        let tip = commits[base_index + edges];
        (edges, tip, fixture.native(tip, PATH))
    });
    let options = BlameOptions::default();
    let seed_budget = CapacityBudget::new(256 * 1024 * 1024);
    let preparation = Provider {
        identity: provider.identity.clone(),
        objects: Arc::clone(&provider.objects),
    };
    let seed_session = open(preparation, &seed_budget);
    let seed = seed_session
        .prepare_replacement_seed(request(&commits[base_index]), &options)
        .unwrap()
        .expect("one aligned non-root slot in the complete base blame");
    let saved = seed_session.encode_replacement_seed(&seed).unwrap();
    // Preserve only durable bytes; reload with a fresh budget and provider.
    let wire = saved.as_slice().to_vec();
    drop((saved, seed));
    drop(seed_session);
    assert_eq!(seed_budget.used(), 0);

    let old_commits = commits[..base_index]
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let objects = Arc::make_mut(&mut provider.objects);
    for id in &old_commits {
        assert!(matches!(objects.remove(id), Some((Kind::Commit, _))));
    }

    let reuse_budget = CapacityBudget::new(64 * 1024 * 1024);
    for (edges, tip, expected) in results {
        let reads = ForbiddenReads::default();
        let session = open_guarded(&provider, &old_commits, &reads, &reuse_budget);
        let seed = session
            .decode_authenticated_replacement_seed(request(&commits[base_index]), &wire)
            .unwrap()
            .expect("the original certificate must round-trip");
        let result = session.reuse_replacement(request(&tip), &seed, MAX_BODY_BYTES);
        assert_eq!(reads.total(), 0, "+{edges} touched old commits: {reads:?}");
        let reused = result
            .unwrap()
            .unwrap_or_else(|| panic!("+{edges} declined"));
        assert_eq!(reused.suffix_edges, edges);
        drop((seed, session));
        assert!(reuse_budget.used() > 0, "live output must remain admitted");
        let actual: Vec<u8> = reused.body.chunks().flatten().copied().collect();
        assert!(
            actual == expected,
            "+{edges} differs from native line porcelain"
        );
        drop(reused);
        assert_eq!(reuse_budget.used(), 0);
    }

    // Control for a regression that falls back to ordinary complete blame: the
    // same tip still needs the root's stable lines, and the guard must catch it.
    let reads = ForbiddenReads::default();
    let session = open_guarded(&provider, &old_commits, &reads, &reuse_budget);
    let tip = commits[base_index + 1];
    assert!(session.blame_complete(request(&tip), &options).is_err());
    assert!(
        reads.total() > 0,
        "ordinary blame never tried the old history"
    );
    drop(session);
    assert_eq!(reuse_budget.used(), 0);

    if last_hit == 100 {
        let reads = ForbiddenReads::default();
        let session = open_guarded(&provider, &old_commits, &reads, &reuse_budget);
        let seed = session
            .decode_authenticated_replacement_seed(request(&commits[base_index]), &wire)
            .unwrap()
            .unwrap();
        let tip = commits[base_index + 101];
        assert!(
            session
                .reuse_replacement(request(&tip), &seed, MAX_BODY_BYTES)
                .unwrap()
                .is_none()
        );
        assert_eq!(reads.total(), 0, "+101 touched old commits: {reads:?}");
        drop((seed, session));
        assert_eq!(reuse_budget.used(), 0);
    }
}

#[test]
fn replacement_matches_native_without_reading_below_the_base() {
    check_suffix_reuse(4, 3);
}

#[test]
#[ignore = "manual 20,000-commit complete-blame and native oracle gate"]
fn replacement_at_20k_base_matches_native_at_plus_one_and_plus_100() {
    check_suffix_reuse(20_000, 100);
}

#[test]
fn completed_candidate_checks_raw_path_without_reading_parent() {
    let mut fixture = Fixture::new();
    let path = b"dir/f\xff";
    let parent = fixture.commit(&[(PATH, b"old\n", "100644")], &[], 1, b"Old\n", None);
    let blob = fixture.insert(Kind::Blob, b"new\n");
    let nested = fixture.tree(&[(b"f\xff", "100755", blob)]);
    let tree = fixture.tree(&[(b"dir", "40000", nested), (PATH, "100644", blob)]);
    let child = fixture.commit_tree(tree, &[parent], 2, b"Child\n", None);
    let merge = fixture.commit_tree(tree, &[child, parent], 3, b"Merge\n", None);
    let provider = fixture.provider();
    let forbidden = BTreeSet::from([parent]);
    let reads = ForbiddenReads::default();
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let session = open_guarded(&provider, &forbidden, &reads, &budget);

    assert_eq!(
        session
            .completed_candidate_parent(BlameRequest {
                commit: &child,
                path
            })
            .unwrap(),
        Some(parent),
        "the edge comes from the child, not any singleton or unchanged-file proof"
    );
    for (commit, path) in [(&child, b"dir/f\xfe".as_slice()), (&merge, path.as_slice())] {
        assert_eq!(
            session
                .completed_candidate_parent(BlameRequest { commit, path })
                .unwrap(),
            None
        );
    }
    assert_eq!(reads.total(), 0, "discovery took a speculative parent read");
}

#[test]
fn replacement_declines_uncertifiable_base_or_suffix() {
    let mut fixture = Fixture::new();
    let mut commit = |path: &[u8], lines: &[u8], parents: &[ObjectId], time| {
        fixture.commit(&[(path, lines, "100644")], parents, time, b"Update\n", None)
    };
    let root = commit(PATH, b"first\nroot\nlast\n", &[], 1);
    let base = commit(PATH, b"first\nbase\nlast\n", &[root], 2);
    let unchanged = commit(PATH, b"first\nbase\nlast\n", &[base], 3);
    let duplicate = commit(PATH, b"first\nfirst\nlast\n", &[base], 4);
    let merge = commit(PATH, b"first\nmerge\nlast\n", &[base, unchanged], 5);
    let wrong_slot = commit(PATH, b"first\nbase\nchanged\n", &[base], 6);
    let renamed = commit(b"other", b"first\nnew\nlast\n", &[base], 7);

    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let session = fixture.session(budget.clone(), control());
    let options = BlameOptions::default();
    for (case, tip) in [("duplicate base lines", duplicate), ("merge base", merge)] {
        assert!(
            session
                .prepare_replacement_seed(request(&tip), &options)
                .unwrap()
                .is_none(),
            "accepted {case}"
        );
    }
    let seed = session
        .prepare_replacement_seed(request(&base), &options)
        .unwrap()
        .expect("certifiable base");
    for (case, tip, path) in [
        ("empty suffix", base, PATH),
        ("unchanged suffix", unchanged, PATH),
        ("duplicate suffix lines", duplicate, PATH),
        ("merge suffix", merge, PATH),
        ("wrong changed slot", wrong_slot, PATH),
        ("different path", renamed, b"other"),
    ] {
        assert!(
            session
                .reuse_replacement(BlameRequest { commit: &tip, path }, &seed, MAX_BODY_BYTES)
                .unwrap()
                .is_none(),
            "accepted {case}"
        );
    }
    drop((seed, session));
    assert_eq!(budget.used(), 0);
}
