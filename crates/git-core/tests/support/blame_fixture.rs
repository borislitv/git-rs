#![allow(dead_code)] // Shared fixture is intentionally used by two focused targets.

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::{
    BlameOptions, CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

type Store = BTreeMap<ObjectId, (Kind, Vec<u8>)>;

pub struct Fixture {
    root: PathBuf,
    objects: Store,
}

impl Fixture {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "core-blame-synthetic-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let fixture = Self {
            root,
            objects: BTreeMap::new(),
        };
        fixture.git(&["init", "--bare", "--template=", "-q"], None, b"");
        fixture
    }

    pub fn git(&self, args: &[&str], path: Option<&[u8]>, input: &[u8]) -> Vec<u8> {
        // Ubuntu 26.04's default Rust timeout rejects non-UTF-8 argv before Git
        // sees it. GNU timeout preserves the literal byte-path oracle contract.
        let timeout = if std::path::Path::new("/usr/bin/gnutimeout").exists() {
            "/usr/bin/gnutimeout"
        } else {
            "timeout"
        };
        let mut command = Command::new(timeout);
        command
            .args(["20s", "git", "--no-pager", "--no-replace-objects"])
            .args([
                "-c",
                "core.quotePath=true",
                "-c",
                "i18n.logOutputEncoding=UTF-8",
                "-c",
                "core.attributesFile=/dev/null",
                "-c",
                "mailmap.file=/dev/null",
            ])
            .args(args)
            .current_dir(&self.root)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("LC_ALL", "C")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_ATTR_NOSYSTEM", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(path) = path {
            command.arg(std::ffi::OsStr::from_bytes(path));
        }
        let mut child = command.spawn().unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "native oracle {args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        result.stdout
    }

    pub fn insert(&mut self, kind: Kind, bytes: &[u8]) -> ObjectId {
        let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, bytes).unwrap();
        let kind_name = match kind {
            Kind::Blob => "blob",
            Kind::Tree => "tree",
            Kind::Commit => "commit",
            Kind::Tag => "tag",
        };
        let native = self.git(
            &[
                "hash-object",
                "--literally",
                "-w",
                "-t",
                kind_name,
                "--stdin",
            ],
            None,
            bytes,
        );
        assert_eq!(
            native.strip_suffix(b"\n").unwrap(),
            id.to_string().as_bytes()
        );
        self.objects.insert(id, (kind, bytes.to_vec()));
        id
    }

    pub fn tree(&mut self, entries: &[(&[u8], &str, ObjectId)]) -> ObjectId {
        let mut entries = entries.to_vec();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        let mut bytes = Vec::new();
        for (path, mode, id) in entries {
            bytes.extend_from_slice(mode.as_bytes());
            bytes.push(b' ');
            bytes.extend_from_slice(path);
            bytes.push(0);
            bytes.extend_from_slice(id.as_bytes());
        }
        self.insert(Kind::Tree, &bytes)
    }

    pub fn commit(
        &mut self,
        files: &[(&[u8], &[u8], &str)],
        parents: &[ObjectId],
        time: u32,
        message: &[u8],
        encoding: Option<&str>,
    ) -> ObjectId {
        let entries: Vec<_> = files
            .iter()
            .map(|(path, bytes, mode)| (*path, *mode, self.insert(Kind::Blob, bytes)))
            .collect();
        let tree = self.tree(&entries);
        self.commit_tree(tree, parents, time, message, encoding)
    }

    pub fn commit_tree(
        &mut self,
        tree: ObjectId,
        parents: &[ObjectId],
        time: u32,
        message: &[u8],
        encoding: Option<&str>,
    ) -> ObjectId {
        let mut bytes = format!("tree {tree}\n").into_bytes();
        for parent in parents {
            bytes.extend_from_slice(format!("parent {parent}\n").as_bytes());
        }
        bytes.extend_from_slice(format!("author Author <author@example.invalid> {time} -0000\ncommitter Committer <committer@example.invalid> {time} +0530\n").as_bytes());
        if let Some(encoding) = encoding {
            bytes.extend_from_slice(format!("encoding {encoding}\n").as_bytes());
        }
        bytes.push(b'\n');
        bytes.extend_from_slice(message);
        self.insert(Kind::Commit, &bytes)
    }

    pub fn native(&self, commit: ObjectId, path: &[u8]) -> Vec<u8> {
        self.git(
            &[
                "--literal-pathspecs",
                "blame",
                "--root",
                "--line-porcelain",
                "--no-textconv",
                "--no-progress",
                "--diff-algorithm=myers",
                &commit.to_string(),
                "--",
            ],
            Some(path),
            b"",
        )
    }

    pub fn provider(&self) -> Provider {
        Provider {
            identity: SnapshotIdentity {
                source: b"synthetic-source".to_vec(),
                generation: b"immutable-one".to_vec(),
                object_hash: gix_hash::Kind::Sha1,
            },
            objects: Arc::new(self.objects.clone()),
        }
    }

    pub fn session(&self, budget: CapacityBudget, control: Control) -> SnapshotSession<Provider> {
        let provider = self.provider();
        let identity = provider.identity.clone();
        SnapshotSession::open(
            provider,
            &identity,
            control,
            budget,
            BlameOptions::default().read_limits(),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub struct Provider {
    pub identity: SnapshotIdentity,
    pub objects: Arc<Store>,
}
impl VerifiedObjectProvider for Provider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, control: &Control) -> git_rs_core::objects::Result<Option<Header>> {
        control.check()?;
        Ok(self.objects.get(id).map(|(kind, bytes)| Header {
            kind: *kind,
            size: bytes.len() as u64,
        }))
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> git_rs_core::objects::Result<Option<Object>> {
        control.check()?;
        let Some((kind, bytes)) = self.objects.get(id) else {
            return Ok(None);
        };
        if bytes.len() > limits.max_object_bytes {
            return Err("synthetic object bound".into());
        }
        let mut data = budget.buffer(bytes.len())?;
        for chunk in bytes.chunks(8192) {
            let start = data.as_slice().len();
            data.resize_controlled(start + chunk.len(), 0, control)?;
            data.as_mut_slice()[start..].copy_from_slice(chunk);
        }
        Ok(Some(Object { kind: *kind, data }))
    }
}

pub fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(30))
}

pub fn render(fixture: &Fixture, commit: ObjectId, path: &[u8], options: &BlameOptions) -> Vec<u8> {
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = control();
    let session = fixture.session(budget.clone(), control.clone());
    let blame = session
        .blame_complete(
            git_rs_core::BlameRequest {
                commit: &commit,
                path,
            },
            options,
        )
        .unwrap();
    let output = git_rs_core::Porcelain::encode_complete(
        &blame,
        &control,
        &budget,
        git_rs_core::blame::MAX_BODY_BYTES,
    )
    .unwrap();
    let bytes = output.chunks().flatten().copied().collect();
    drop(output);
    drop(blame);
    drop(session);
    assert_eq!(
        budget.used(),
        0,
        "all job/output capacity must drain after their actual owners"
    );
    bytes
}
