#![forbid(unsafe_code)]

#[path = "support/range_source.rs"]
mod ranges;

use git_rs::{Result, protocol::Oid};
use git_rs_core::{
    CapacityBudget, Control, ReadLimits,
    upload::{UploadSession, Version},
};
use ranges::{Pack, Source, full, oid};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::{ffi::OsStrExt, fs::DirBuilderExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

struct Root(PathBuf);
impl Root {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "git-rs-range-snapshot-{}",
            git_rs::scratch::identifier()?
        ));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
    fn bare(&self) -> Result<PathBuf> {
        native(
            &self.0,
            &["init", "--bare", "--initial-branch=main", "oracle.git"],
            b"",
        )?;
        Ok(self.0.join("oracle.git"))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove owned native oracle");
    }
}

fn native(path: &Path, arguments: &[&str], input: &[u8]) -> Result<Vec<u8>> {
    let mut child = Command::new("/usr/bin/git")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Synthetic Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.invalid")
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00+00:00")
        .env("GIT_COMMITTER_NAME", "Synthetic Author")
        .env("GIT_COMMITTER_EMAIL", "author@example.invalid")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00+00:00")
        .arg("-C")
        .arg(path)
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .expect("fixture input")
        .write_all(input)?;
    let result = child.wait_with_output()?;
    assert!(
        result.status.success(),
        "native fixture/oracle {arguments:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(result.stdout)
}

fn load_source(path: &Path) -> Result<Source> {
    let mut indexes = fs::read_dir(path.join("objects/pack"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    indexes.retain(|path| path.extension().is_some_and(|extension| extension == "idx"));
    indexes.sort();
    let packs = indexes
        .into_iter()
        .map(|index| Pack::from_bytes(fs::read(index.with_extension("pack"))?, fs::read(index)?))
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !packs.is_empty(),
        "native oracle must supply a physical pack/index"
    );
    Ok(Source::new(packs))
}

fn pack(path: &Path, tips: &[u8]) -> Result<Source> {
    let bytes = native(
        path,
        &[
            "pack-objects",
            "--stdout",
            "--window=32",
            "--depth=8",
            "--no-delta-base-offset",
        ],
        tips,
    )?;
    native(path, &["index-pack", "--stdin"], &bytes)?;
    load_source(path)
}

fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(30))
}
fn capacity() -> CapacityBudget {
    CapacityBudget::new(16 * 1024 * 1024)
}

fn synthetic() -> Result<(Source, gix::ObjectId, gix::ObjectId)> {
    let blob = oid(gix::objs::Kind::Blob, b"content\n")?;
    let mut tree_bytes = b"100644 byte-\xff\0".to_vec();
    tree_bytes.extend_from_slice(blob.as_slice());
    let tree = oid(gix::objs::Kind::Tree, &tree_bytes)?;
    let commit_bytes = format!("tree {tree}\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\ninitial\n").into_bytes();
    let commit = oid(gix::objs::Kind::Commit, &commit_bytes)?;
    let source = Source::new(vec![Pack::new(&[
        (blob, full(gix::objs::Kind::Blob, b"content\n")),
        (tree, full(gix::objs::Kind::Tree, &tree_bytes)),
        (commit, full(gix::objs::Kind::Commit, &commit_bytes)),
    ])?]);
    Ok((source, commit, blob))
}

#[test]
fn snapshot_ref_advertisement_preserves_byte_names_without_a_repository() -> Result<()> {
    let (source, commit, blob) = synthetic()?;
    let refs = BTreeMap::from([
        (b"refs/heads/main".to_vec(), commit),
        (b"refs/heads/nested/branch".to_vec(), commit),
        (b"refs/heads/\xff".to_vec(), commit),
        (b"refs/notes/commits".to_vec(), commit),
        (b"refs/tags/blob".to_vec(), blob),
        (b"refs/tags/release".to_vec(), commit),
    ]);
    let budget = capacity();
    let session = UploadSession::new(
        source.open(control(), budget.clone(), ReadLimits::default())?,
        refs.iter().map(|(name, id)| (name.as_slice(), *id)),
        b"refs/heads/\xff",
        Version::V0,
    )?;
    let mut wire = Vec::new();
    session.advertise(false, &mut wire)?;
    let mut input = wire.as_slice();
    let mut lines = Vec::new();
    while let Some(line) = git_rs::protocol::read_packet(&mut input)? {
        lines.push(
            line.split(|byte| *byte == 0)
                .next()
                .unwrap()
                .trim_ascii_end()
                .to_vec(),
        );
    }
    assert!(input.is_empty());
    assert_eq!(
        lines,
        [
            format!("{commit} HEAD").into_bytes(),
            format!("{commit} refs/heads/main").into_bytes(),
            format!("{commit} refs/heads/nested/branch").into_bytes(),
            [format!("{commit} refs/heads/").as_bytes(), b"\xff"].concat(),
            format!("{commit} refs/notes/commits").into_bytes(),
            format!("{blob} refs/tags/blob").into_bytes(),
            format!("{commit} refs/tags/release").into_bytes(),
        ]
    );
    drop(session);
    assert_eq!(budget.used(), 0);
    let empty = UploadSession::new(
        Source::new(Vec::new()).open(control(), budget.clone(), ReadLimits::default())?,
        std::iter::empty(),
        b"refs/heads/unborn",
        Version::V0,
    )?;
    let mut wire = Vec::new();
    empty.advertise(false, &mut wire)?;
    let line = git_rs::protocol::read_packet(&mut wire.as_slice())?.unwrap();
    assert!(line.starts_with(b"0000000000000000000000000000000000000000 capabilities^{}\0"));
    assert!(
        line.windows(b"symref=HEAD:refs/heads/unborn".len())
            .any(|part| part == b"symref=HEAD:refs/heads/unborn")
    );
    Ok(())
}

#[test]
fn invalid_git_ref_names_are_rejected_when_commands_are_parsed() {
    for name in [
        "refs/heads/bad..name",
        "refs/heads/.hidden",
        "refs/heads/branch.lock",
        "refs/heads/trailing.",
        "refs/heads/a@{b",
        "refs/heads/a\\b",
        "refs/heads/a[b",
        "refs/heads//empty",
    ] {
        let command = format!("{} {} {name}", "0".repeat(40), "1".repeat(40));
        assert!(
            git_rs::protocol::Update::parse(command.as_bytes()).is_err(),
            "malformed ref {name}"
        );
    }
}

#[test]
fn invalid_snapshot_targets_are_rejected_before_advertisement() -> Result<()> {
    for invalid in [
        "missing",
        "missing-empty-tree",
        "non-commit",
        "bad-default",
        "bad-name",
    ] {
        let (source, _, blob) = synthetic()?;
        let witness = source.witness();
        let mut refs = BTreeMap::from([(b"refs/tags/aa-valid".to_vec(), blob)]);
        let mut default_ref = b"refs/heads/main".as_slice();
        match invalid {
            "missing" => {
                refs.insert(
                    b"refs/tags/zz-missing".to_vec(),
                    gix::ObjectId::from([1; 20]),
                );
            }
            "missing-empty-tree" => {
                refs.insert(
                    b"refs/tags/zz-missing".to_vec(),
                    gix::ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904")?,
                );
            }
            "non-commit" => {
                refs.insert(b"refs/heads/not-a-commit".to_vec(), blob);
            }
            "bad-default" => default_ref = b"refs/heads/bad..name",
            "bad-name" => {
                refs.insert(b"refs/tags/zz-invalid.lock".to_vec(), blob);
            }
            _ => unreachable!(),
        }
        let budget = capacity();
        let result = UploadSession::new(
            source.open(control(), budget.clone(), ReadLimits::default())?,
            refs.iter().map(|(name, id)| (name.as_slice(), *id)),
            default_ref,
            Version::V0,
        );
        assert!(
            result.is_err(),
            "invalid catalog {invalid} must not yield an advertisable session"
        );
        drop(result);
        assert!(witness.upgrade().is_none());
        assert_eq!(
            budget.used(),
            0,
            "rejected {invalid} must release all source metadata"
        );
    }
    Ok(())
}

#[test]
fn ref_name_validation_matches_native_git_for_every_non_nul_byte() -> Result<()> {
    for byte in 1u8..=255 {
        let mut name = b"refs/heads/".to_vec();
        name.push(byte);
        let status = Command::new("/usr/bin/git")
            .env_clear()
            .env("HOME", "/nonexistent")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .arg("check-ref-format")
            .arg(std::ffi::OsStr::from_bytes(&name))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        let mut command = format!("{} {} ", "0".repeat(40), "1".repeat(40)).into_bytes();
        command.extend_from_slice(&name);
        assert_eq!(
            git_rs::protocol::Update::parse(&command).is_ok(),
            status.success(),
            "byte-oriented ref {name:?}"
        );
    }
    Ok(())
}

#[test]
fn isolated_snapshot_ignores_git_environment() -> Result<()> {
    const CHILD: &str = "GIT_RS_SNAPSHOT_ISOLATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let (source, commit, blob) = synthetic()?;
        let budget = capacity();
        let odb = source.open(control(), budget.clone(), ReadLimits::default())?;
        let (id, object) = odb
            .lookup_blob(&commit, b"byte-\xff")?
            .expect("catalog-only byte lookup");
        assert_eq!(id, blob);
        assert_eq!(object.data.as_slice(), b"content\n");
        assert!(
            odb.object(&gix::ObjectId::from_hex(
                b"4b825dc642cb6eb9a060e54bf8d69288fbee4904"
            )?)?
            .is_none()
        );
        return Ok(());
    }
    let foreign = Root::new()?;
    let config = foreign.0.join("invalid-config");
    fs::write(&config, b"[invalid Git configuration\n")?;
    let result = Command::new(std::env::current_exe()?)
        .env(CHILD, "1")
        .env("GIT_CONFIG_SYSTEM", &config)
        .env("GIT_CONFIG_GLOBAL", &config)
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "init.defaultBranch")
        .env("GIT_CONFIG_VALUE_0", "foreign-branch")
        .env("GIT_DEFAULT_HASH", "sha256")
        .env("GIT_DIR", &foreign.0)
        .env("GIT_COMMON_DIR", &foreign.0)
        .env("GIT_WORK_TREE", &foreign.0)
        .env("GIT_OBJECT_DIRECTORY", foreign.0.join("foreign-objects"))
        .env("GIT_NAMESPACE", "foreign-namespace")
        .args(["--exact", "isolated_snapshot_ignores_git_environment"])
        .output()?;
    assert!(
        result.status.success(),
        "foreign Git environment: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read_dir(&foreign.0)?.count(), 1);
    assert_eq!(fs::read(config)?, b"[invalid Git configuration\n");
    Ok(())
}

#[test]
fn snapshot_refs_resolve_native_ref_delta_commits_through_ranges() -> Result<()> {
    let root = Root::new()?;
    let path = root.bare()?;
    let blob = native(
        &path,
        &["hash-object", "-w", "--stdin"],
        b"shared content\n",
    )?;
    let blob = std::str::from_utf8(&blob)?.trim();
    let tree = native(
        &path,
        &["mktree"],
        format!("100644 blob {blob}\tfile.txt\n").as_bytes(),
    )?;
    let tree = std::str::from_utf8(&tree)?.trim();
    let message = "repeated synthetic commit message\n".repeat(128);
    let mut commits: Vec<Oid> = Vec::new();
    for _ in 0..16 {
        let mut args = vec!["commit-tree", tree];
        let parent = commits.last().map(|id| id.hex());
        if let Some(parent) = &parent {
            args.extend(["-p", parent]);
        }
        let commit = native(&path, &args, message.as_bytes())?;
        commits.push(Oid::parse(commit.trim_ascii())?);
    }
    let ids = [
        format!("{}\n", commits[15].hex()),
        format!("{tree}\n{blob}\n"),
        commits
            .iter()
            .take(15)
            .map(|id| id.hex() + "\n")
            .collect::<String>(),
    ]
    .concat();
    let source = pack(&path, ids.as_bytes())?;
    let index = fs::read_dir(path.join("objects/pack"))?
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "idx"))
        .unwrap();
    let inventory = native(&path, &["verify-pack", "-v", index.to_str().unwrap()], b"")?;
    assert!(
        inventory.split(|byte| *byte == b'\n').any(|line| {
            let fields = line
                .split(|byte| *byte == b' ')
                .filter(|field| !field.is_empty())
                .collect::<Vec<_>>();
            fields.len() == 7 && fields[1] == b"commit"
        }),
        "native fixture must contain actual delta-compressed commits"
    );
    let witness = source.witness();
    let budget = capacity();
    let request = control();
    let odb = source.open(request.clone(), budget.clone(), ReadLimits::default())?;
    let id = gix::ObjectId::from(commits[15].0);
    assert!(odb.pack_location(&id)?.is_some());
    let body = odb.object(&id)?.expect("native physical commit");
    assert_eq!(body.kind, gix::objs::Kind::Commit);
    assert_eq!(
        body.data.as_slice(),
        native(&path, &["cat-file", "commit", &commits[15].hex()], b"")?
    );
    let (_, content) = odb.lookup_blob(&id, b"file.txt")?.unwrap();
    assert_eq!(content.data.as_slice(), b"shared content\n");
    assert!(odb.is_ancestor(gix::ObjectId::from(commits[0].0).as_ref(), &id)?);
    assert_eq!(
        odb.reachable_from_refs([(b"refs/heads/main".as_slice(), id)])?
            .len(),
        16
    );
    let mut output = odb.find_buffer(body.data.as_slice().len())?;
    assert_eq!(
        odb.find_in(&id, &mut output)?.unwrap().data,
        body.data.as_slice()
    );
    request.cancel();
    assert!(odb.object(&id).is_err());
    assert!(
        witness.upgrade().is_some(),
        "cancellation is not source-release acknowledgment"
    );
    drop(output);
    drop(content);
    drop(body);
    drop(odb);
    assert!(witness.upgrade().is_none());
    assert_eq!(budget.used(), 0);
    native(&path, &["fsck", "--strict", "--full"], b"")?;
    Ok(())
}

#[test]
fn physical_object_headers_match_native_types_and_sizes() -> Result<()> {
    let root = Root::new()?;
    let path = root.bare()?;
    let blob = native(&path, &["hash-object", "-w", "--stdin"], b"a\0\r\n\xff")?;
    let blob = std::str::from_utf8(&blob)?.trim();
    let empty = native(&path, &["hash-object", "-w", "--stdin"], b"")?;
    let empty = std::str::from_utf8(&empty)?.trim();
    let tree = native(
        &path,
        &["mktree"],
        format!("100644 blob {blob}\tfile.txt\n").as_bytes(),
    )?;
    let tree = std::str::from_utf8(&tree)?.trim();
    let commit = native(&path, &["commit-tree", tree], b"header fixture\n")?;
    let commit = std::str::from_utf8(&commit)?.trim();
    let tag = native(&path, &["mktag"], format!("object {commit}\ntype commit\ntag synthetic\ntagger Synthetic Author <author@example.invalid> 946684800 +0000\n\nheader fixture\n").as_bytes())?;
    let tag = std::str::from_utf8(&tag)?.trim();
    let source = pack(
        &path,
        format!("{blob}\n{empty}\n{tree}\n{commit}\n{tag}\n").as_bytes(),
    )?;
    let request = control();
    let odb = source.open(request.clone(), capacity(), ReadLimits::default())?;
    for (id, kind) in [
        (blob, gix::objs::Kind::Blob),
        (empty, gix::objs::Kind::Blob),
        (tree, gix::objs::Kind::Tree),
        (commit, gix::objs::Kind::Commit),
        (tag, gix::objs::Kind::Tag),
    ] {
        let native_size = native(&path, &["cat-file", "-s", id], b"")?;
        let header = odb
            .header(&gix::ObjectId::from_hex(id.as_bytes())?)?
            .unwrap();
        assert_eq!(header.kind, kind);
        assert_eq!(
            header.size,
            std::str::from_utf8(&native_size)?.trim().parse::<u64>()?
        );
    }
    assert!(odb.header(&gix::ObjectId::from([0; 20]))?.is_none());
    request.cancel();
    assert!(
        odb.header(&gix::ObjectId::from_hex(blob.as_bytes())?)
            .is_err()
    );
    assert!(odb.header(&gix::ObjectId::from([0; 20])).is_err());
    Ok(())
}

#[test]
fn large_ref_delta_headers_fit_a_small_address_space() -> Result<()> {
    const SOURCE: &str = "GIT_RS_LARGE_HEADER_SOURCE";
    const CASES: &str = "GIT_RS_LARGE_HEADER_CASES";
    if let Some(source) = std::env::var_os(SOURCE) {
        let source = load_source(Path::new(&source))?;
        let reads = source.reads.clone();
        let budget = capacity();
        let limits = ReadLimits {
            max_object_bytes: git_rs::blame::MAX_BLOB as usize + 4096,
            max_decode_bytes: 1024 * 1024 * 1024,
            ..ReadLimits::default()
        };
        let odb = source.open(control(), budget.clone(), limits)?;
        for case in std::env::var(CASES)?.split(',') {
            let (id, size) = case.split_once(':').unwrap();
            let header = odb
                .header(&gix::ObjectId::from_hex(id.as_bytes())?)?
                .unwrap();
            assert_eq!(header.kind, gix::objs::Kind::Blob);
            assert_eq!(header.size, size.parse::<u64>()?);
        }
        assert!(reads.largest.load(Ordering::Relaxed) <= 32 * 1024);
        assert!(
            reads.total.load(Ordering::Relaxed) < 1024 * 1024,
            "header preflight must not read full 128 MiB objects"
        );
        drop(odb);
        assert_eq!(budget.used(), 0);
        return Ok(());
    }
    let root = Root::new()?;
    let path = root.bare()?;
    let mut cases = Vec::new();
    for size in [
        git_rs::blame::MAX_BLOB - 1,
        git_rs::blame::MAX_BLOB,
        git_rs::blame::MAX_BLOB + 1,
        git_rs::blame::MAX_BLOB + 4096,
    ] {
        let file = root.0.join(format!("synthetic-{size}.bin"));
        fs::File::create(&file)?.set_len(size)?;
        let id = native(&path, &["hash-object", "-w", file.to_str().unwrap()], b"")?;
        let id = std::str::from_utf8(&id)?.trim().to_owned();
        assert_eq!(
            native(&path, &["cat-file", "-s", &id], b"")?,
            format!("{size}\n").as_bytes()
        );
        cases.push((id, size));
        fs::remove_file(file)?;
    }
    let ids = cases
        .iter()
        .map(|(id, _)| id.clone() + "\n")
        .collect::<String>();
    pack(&path, ids.as_bytes())?;
    let index = fs::read_dir(path.join("objects/pack"))?
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|extension| extension == "idx"))
        .unwrap();
    let inventory = native(&path, &["verify-pack", "-v", index.to_str().unwrap()], b"")?;
    assert!(
        inventory.split(|byte| *byte == b'\n').any(|line| {
            let fields = line
                .split(|byte| *byte == b' ')
                .filter(|field| !field.is_empty())
                .collect::<Vec<_>>();
            fields.len() == 7 && fields[0] == cases[2].0.as_bytes() && fields[1] == b"blob"
        }),
        "oversized target must actually use native REF_DELTA"
    );
    let expected = cases
        .iter()
        .map(|(id, size)| format!("{id}:{size}"))
        .collect::<Vec<_>>()
        .join(",");
    let result = Command::new("/usr/bin/prlimit")
        .args(["--as=100663296", "--core=0", "--"])
        .arg(std::env::current_exe()?)
        .args([
            "--exact",
            "large_ref_delta_headers_fit_a_small_address_space",
            "--test-threads=1",
        ])
        .env("MALLOC_ARENA_MAX", "1")
        .env(SOURCE, &path)
        .env(CASES, expected)
        .output()?;
    assert!(
        result.status.success(),
        "headers must fit 96 MiB without full inflation: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(())
}
