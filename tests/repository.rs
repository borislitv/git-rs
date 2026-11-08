#![forbid(unsafe_code)]

use git_rs::{Result, metrics, protocol::Oid, scratch::Scratch};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

struct Root(PathBuf);

impl Root {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "git-rs-library-snapshot-{}",
            git_rs::scratch::identifier()?
        ));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove owned snapshot fixture");
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

#[test]
fn snapshot_initialization_and_ref_restoration_do_not_execute_git() -> Result<()> {
    let root = Root::new()?;
    let scratch = Scratch::new(&root.0, 2)?;
    let blob = native(
        &scratch.path,
        &["hash-object", "-w", "--stdin"],
        b"content\n",
    )?;
    let blob = std::str::from_utf8(&blob)?.trim();
    let tree = native(
        &scratch.path,
        &["mktree"],
        format!("100644 blob {blob}\tfile.txt\n").as_bytes(),
    )?;
    let tree = std::str::from_utf8(&tree)?.trim();
    let commit = native(&scratch.path, &["commit-tree", tree], b"initial\n")?;
    let commit = Oid::parse(commit.trim_ascii())?;
    let refs = BTreeMap::from([
        (b"refs/heads/main".to_vec(), commit),
        (b"refs/heads/nested/branch".to_vec(), commit),
        (b"refs/heads/\xff".to_vec(), commit),
        (b"refs/tags/release".to_vec(), commit),
        (b"refs/tags/blob".to_vec(), Oid::parse(blob.as_bytes())?),
        (b"refs/notes/commits".to_vec(), commit),
    ]);
    scratch.restore_refs(&refs, b"refs/heads/\xff")?;
    assert_eq!(
        fs::read(scratch.path.join("HEAD"))?,
        b"ref: refs/heads/\xff\n"
    );
    assert!(!scratch.path.join("hooks").exists());
    assert!(!scratch.path.join("logs").exists());
    assert!(!scratch.path.join("index").exists());
    let expected = refs
        .iter()
        .flat_map(|(name, oid)| {
            let mut line = name.clone();
            line.push(b' ');
            line.extend_from_slice(oid.hex().as_bytes());
            line.push(b'\n');
            line
        })
        .collect::<Vec<_>>();
    assert_eq!(
        native(
            &scratch.path,
            &["for-each-ref", "--format=%(refname) %(objectname)"],
            b"",
        )?,
        expected,
        "native Git must read every restored byte-oriented ref exactly"
    );
    assert_eq!(
        native(&scratch.path, &["rev-parse", "--is-bare-repository"], b"")?,
        b"true\n"
    );
    let empty = Scratch::new(&root.0, 1)?;
    empty.restore_refs(&BTreeMap::new(), b"refs/heads/unborn")?;
    assert_eq!(
        fs::read(empty.path.join("HEAD"))?,
        b"ref: refs/heads/unborn\n"
    );
    let text = metrics::global().render(0, 0);
    assert!(
        text.lines()
            .any(|line| line == "git_rs_subprocess_duration_seconds_count{program=\"git\"} 0"),
        "snapshot initialization/restoration must be library-only, including the cold template"
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
            "reject malformed ref {name} before mutation or subprocess execution"
        );
    }
}

#[test]
fn invalid_snapshot_targets_are_rejected_before_any_refs_are_written() -> Result<()> {
    let root = Root::new()?;
    for invalid in [
        "missing",
        "missing-empty-tree",
        "non-commit",
        "bad-default",
        "bad-name",
    ] {
        let scratch = Scratch::new(&root.0, 1)?;
        let blob = native(&scratch.path, &["hash-object", "-w", "--stdin"], b"text\n")?;
        let blob = Oid::parse(blob.trim_ascii())?;
        let mut refs = BTreeMap::from([(b"refs/tags/aa-valid".to_vec(), blob)]);
        let mut default_ref = b"refs/heads/main".as_slice();
        match invalid {
            "missing" => {
                refs.insert(b"refs/tags/zz-missing".to_vec(), Oid([1; 20]));
            }
            "missing-empty-tree" => {
                refs.insert(
                    b"refs/tags/zz-missing".to_vec(),
                    Oid::parse(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904")?,
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
        assert!(
            scratch.restore_refs(&refs, default_ref).is_err(),
            "invalid snapshot {invalid} must fail before restoring any ref"
        );
        assert_eq!(
            native(&scratch.path, &["for-each-ref"], b"")?,
            b"",
            "invalid snapshot {invalid} must leave the private ref store empty"
        );
        assert_eq!(
            fs::read(scratch.path.join("HEAD"))?,
            b"ref: refs/heads/main\n"
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
            "byte-oriented ref validation differs for {name:?}"
        );
    }
    Ok(())
}

#[test]
fn isolated_snapshot_ignores_git_environment() -> Result<()> {
    const CHILD: &str = "GIT_RS_SNAPSHOT_ISOLATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let root = Root::new()?;
        let scratch = Scratch::new(&root.0, 1)?;
        assert_eq!(
            fs::read(scratch.path.join("HEAD"))?,
            b"ref: refs/heads/main\n"
        );
        scratch.restore_refs(&BTreeMap::new(), b"refs/heads/\xff")?;
        assert_eq!(
            fs::read(scratch.path.join("HEAD"))?,
            b"ref: refs/heads/\xff\n"
        );
        assert!(!scratch.path.join("hooks").exists());
        assert!(!scratch.path.join("index").exists());
        assert!(!scratch.path.join("logs").exists());
        assert!(scratch.path.join("objects/pack").is_dir());
        assert!(
            !fs::read(scratch.path.join("config"))?
                .windows(b"sha256".len())
                .any(|window| window == b"sha256")
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
        "isolated initialization/restoration with foreign Git environment: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read_dir(&foreign.0)?.count(), 1);
    assert_eq!(fs::read(config)?, b"[invalid Git configuration\n");
    Ok(())
}

#[test]
fn snapshot_refs_resolve_ref_delta_commits_in_sealed_ram_packs() -> Result<()> {
    let root = Root::new()?;
    let source = Scratch::new(&root.0, 1)?;
    let blob = native(
        &source.path,
        &["hash-object", "-w", "--stdin"],
        b"shared content\n",
    )?;
    let blob = std::str::from_utf8(&blob)?.trim();
    let tree = native(
        &source.path,
        &["mktree"],
        format!("100644 blob {blob}\tfile.txt\n").as_bytes(),
    )?;
    let tree = std::str::from_utf8(&tree)?.trim();
    let message = "repeated synthetic commit message\n".repeat(128);
    let mut commits: Vec<Oid> = Vec::new();
    for _ in 0..16 {
        let mut arguments = vec!["commit-tree", tree];
        let parent = commits.last().map(|id| id.hex());
        if let Some(parent) = &parent {
            arguments.extend(["-p", parent.as_str()]);
        }
        let commit = native(&source.path, &arguments, message.as_bytes())?;
        commits.push(Oid::parse(commit.trim_ascii())?);
    }
    let tips = commits.iter().map(|id| id.hex() + "\n").collect::<String>();
    let pack = native(
        &source.path,
        &[
            "pack-objects",
            "--stdout",
            "--revs",
            "--window=32",
            "--depth=8",
            "--no-delta-base-offset",
        ],
        tips.as_bytes(),
    )?;
    native(&source.path, &["index-pack", "--stdin"], &pack)?;
    let cache = git_rs::cache::Cache::new(1024 * 1024, 1024 * 1024);
    let target = Scratch::new(&root.0, 1)?;
    let mut pins = Vec::new();
    for entry in fs::read_dir(source.path.join("objects/pack"))? {
        let path = entry?.path();
        if !matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("pack" | "idx")
        ) {
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "idx") {
            let inventory = native(
                &source.path,
                &["verify-pack", "-v", path.to_str().expect("fixture path")],
                b"",
            )?;
            assert!(
                inventory.split(|byte| *byte == b'\n').any(|line| {
                    let fields = line
                        .split(|byte| *byte == b' ')
                        .filter(|field| !field.is_empty())
                        .collect::<Vec<_>>();
                    fields.len() == 7 && fields[1] == b"commit"
                }),
                "the native-generated pack must actually exercise delta-compressed commit headers"
            );
        }
        let bytes = fs::read(&path)?;
        let mut input = bytes.as_slice();
        let pin = cache
            .get_or_load(
                path.to_str().expect("fixture path"),
                bytes.len(),
                |mut output| {
                    std::io::copy(&mut input, &mut output)?;
                    Ok(())
                },
            )?
            .expect("small native pack/index must fit the RAM fixture");
        let destination = target
            .path
            .join("objects/pack")
            .join(path.file_name().expect("pack filename"));
        std::os::unix::fs::symlink(pin.path(), &destination)?;
        assert!(fs::symlink_metadata(destination)?.file_type().is_symlink());
        pins.push(pin);
    }
    assert_eq!(
        pins.len(),
        2,
        "both pack and index must remain pinned in sealed RAM"
    );
    let invalid = BTreeMap::from([
        (b"refs/heads/aa-valid".to_vec(), commits[0]),
        (
            b"refs/heads/zz-not-a-commit".to_vec(),
            Oid::parse(blob.as_bytes())?,
        ),
    ]);
    assert!(target.restore_refs(&invalid, b"refs/heads/main").is_err());
    assert_eq!(
        native(&target.path, &["for-each-ref"], b"")?,
        b"",
        "a packed non-commit branch must fail before any valid ref is restored"
    );
    let mut refs = commits
        .iter()
        .enumerate()
        .map(|(index, id)| (format!("refs/heads/branch-{index}").into_bytes(), *id))
        .collect::<BTreeMap<_, _>>();
    refs.insert(b"refs/tags/blob".to_vec(), Oid::parse(blob.as_bytes())?);
    refs.insert(b"refs/tags/tree".to_vec(), Oid::parse(tree.as_bytes())?);
    let restored = target.restore_refs(&refs, b"refs/heads/branch-15");
    assert!(
        restored.is_ok(),
        "library snapshot restoration must read sealed RAM pack/index links: {restored:?}"
    );
    assert_eq!(
        native(&target.path, &["rev-parse", "HEAD"], b"")?,
        format!("{}\n", commits[15].hex()).as_bytes()
    );
    native(&target.path, &["fsck", "--strict", "--full"], b"")?;
    Ok(())
}
