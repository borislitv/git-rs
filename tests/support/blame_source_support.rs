//! Real source publication/authority fixture. Native Git is only a synthetic
//! pack producer and independent oracle; the serving engine uses the Rust core.
use git_rs::{
    blame::{Blame, CompleteResponse, Request, Response},
    blob::Store,
    cache::Cache,
    engine::Engine,
    metadata,
    process::Operation,
    protocol::{Oid, Update},
};
use std::{
    fs,
    io::Write,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

pub struct Fixture {
    pub root: PathBuf,
    pub slug: String,
    pub repo: String,
    pub commit: String,
    pub dangling: String,
    pub selected: String,
    pub unrelated: String,
    pub expected: Vec<u8>,
    pub engine: Arc<Engine>,
}

fn git(root: &Path, args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = Command::new("timeout")
        .args(["--kill-after=2s", "20s", "git"])
        .arg("-C")
        .arg(root)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("LC_ALL", "C")
        .env("GIT_AUTHOR_NAME", "Synthetic Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.invalid")
        .env("GIT_COMMITTER_NAME", "Synthetic Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.invalid")
        .env("GIT_AUTHOR_DATE", "1000000000 +0000")
        .env("GIT_COMMITTER_DATE", "1000000000 +0000")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "native fixture/oracle {args:?}: {:?}",
        output.stderr
    );
    output.stdout
}
fn text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap().trim().to_owned()
}
fn commit(root: &Path, contents: &[u8], message: &[u8]) -> String {
    let blob = text(git(root, &["hash-object", "-w", "--stdin"], contents));
    let tree = text(git(
        root,
        &["mktree"],
        format!("100644 blob {blob}\tfile\n").as_bytes(),
    ));
    text(git(root, &["commit-tree", &tree], message))
}
fn pack(root: &Path, revisions: &str) -> String {
    let prefix = root.join("objects/pack/pack");
    text(git(
        root,
        &["pack-objects", "--revs", prefix.to_str().unwrap()],
        revisions.as_bytes(),
    ))
}

impl Fixture {
    pub fn new(blame: Blame, lines: usize) -> Self {
        Operation::new(30).enter(|| {
            metadata::migrate().unwrap();
            let id = git_rs::scratch::identifier().unwrap();
            let root = std::env::temp_dir().join(format!("root-blame-source-{id}"));
            fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
            let native = root.join("native");
            fs::create_dir(&native).unwrap();
            git(&native, &["init", "--bare", "--quiet"], b"");
            let content = (0..lines)
                .map(|n| format!("synthetic line {n}\n"))
                .collect::<String>();
            let commit = commit(&native, content.as_bytes(), b"published\n");
            let dangling = self::commit(&native, b"stored but unreachable\n", b"dangling\n");
            let selected = pack(&native, &format!("{commit}\n{dangling}\n"));
            let other = self::commit(&native, b"unrelated pack\n", b"other\n");
            let unrelated = pack(&native, &format!("{other}\n"));
            let expected = git(
                &native,
                &[
                    "--literal-pathspecs",
                    "blame",
                    "--root",
                    "--line-porcelain",
                    "--no-textconv",
                    "--no-progress",
                    &commit,
                    "--",
                    "file",
                ],
                b"",
            );
            let slug = format!("synthetic/root-blame-{id}");
            metadata::create(&slug, "alice").unwrap();
            metadata::grant(&slug, "alice", "bob", "reader", b"refs/").unwrap();
            let (snapshot, lease) = metadata::leased_snapshot(&slug, "alice", true).unwrap();
            let repo = snapshot.id.clone();
            let store = Store::from_env().unwrap();
            let operation = git_rs::scratch::identifier().unwrap();
            let directory = native.join("objects/pack");
            let packs = [&selected, &unrelated]
                .iter()
                .map(|hash| store.upload(&repo, &operation, &directory, hash).unwrap())
                .collect::<Vec<_>>();
            metadata::publish(
                &snapshot,
                &lease,
                &operation,
                "alice",
                &[Update {
                    old: Oid::ZERO,
                    new: Oid::parse(commit.as_bytes()).unwrap(),
                    name: b"refs/heads/main".to_vec(),
                }],
                &[true],
                &packs,
            )
            .unwrap();
            drop(lease);
            let scratch = root.join("scratch");
            fs::create_dir(&scratch).unwrap();
            let engine = Arc::new(Engine {
                blame,
                store,
                cache: Arc::new(Cache::new(1024 * 1024, 1024 * 1024)),
                scratch_root: scratch,
                operation_seconds: 10,
                pack_threads: 1,
            });
            Self {
                root,
                slug,
                repo,
                commit,
                dangling,
                selected,
                unrelated,
                expected,
                engine,
            }
        })
    }
    pub fn request(&self) -> Request {
        Request::parse(&self.commit, "path=file").unwrap()
    }
    pub fn complete(&self) -> CompleteResponse {
        match Operation::new(20)
            .enter(|| self.engine.blame(&self.slug, "bob", &self.request()))
            .unwrap()
        {
            Response::Complete(body) => body,
            _ => panic!("published physical source did not produce complete blame"),
        }
    }
    pub fn sql(&self, statement: &str) -> String {
        Operation::new(5)
            .enter(|| metadata::sql(statement))
            .unwrap()
    }
    pub fn leases(&self) -> usize {
        self.sql(&format!(
            "SELECT count(*) FROM repository_leases WHERE repo_id={}::uuid",
            metadata::text(&self.repo)
        ))
        .trim()
        .parse()
        .unwrap()
    }
    pub fn fence(&self, assignment: &str) {
        self.sql(&format!(
            "UPDATE repositories SET {assignment} WHERE id={}::uuid",
            metadata::text(&self.repo)
        ));
    }
    pub fn revoke(&self) {
        Operation::new(5)
            .enter(|| metadata::grant(&self.slug, "alice", "bob", "none", b"refs/"))
            .unwrap();
    }
    pub fn restored_reader(&self) {
        Operation::new(5)
            .enter(|| metadata::grant(&self.slug, "alice", "bob", "reader", b"refs/"))
            .unwrap();
    }
    pub fn source_files(&self) -> Vec<String> {
        fs::read_dir(&self.engine.scratch_root)
            .unwrap()
            .flat_map(|entry| {
                fs::read_dir(entry.unwrap().path().join("objects/pack"))
                    .unwrap()
                    .map(|file| file.unwrap().file_name().into_string().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    pub fn put(&self, key: &str, path: &Path) {
        // Only the per-unit synthetic bucket exists in this network namespace.
        let url = format!(
            "{}/{}/{}",
            std::env::var("GIT_RS_S3_ENDPOINT").unwrap(),
            std::env::var("GIT_RS_S3_BUCKET").unwrap(),
            key
        );
        let result = Command::new("curl")
            .args([
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "5",
                "--aws-sigv4",
                "aws:amz:us-east-1:s3",
                "--user",
                "test-access:synthetic-secret-key",
                "--upload-file",
            ])
            .arg(path)
            .arg(url)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "owned S3 fixture upload failed: {:?}",
            result.stderr
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

pub fn metric(operation: &str) -> u64 {
    let key = format!("git_rs_dependency_duration_seconds_count{{operation=\"{operation}\"}}");
    git_rs::metrics::global()
        .render(0, 0)
        .lines()
        .find_map(|line| line.strip_prefix(&key).map(|n| n.trim().parse().unwrap()))
        .unwrap()
}
