use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io::Write,
    os::unix::ffi::OsStrExt,
    path::Path,
    process::{Command, Stdio},
};

use git_rs_upstream_probe::blame_backend::{Annotation, AttributedRange, annotate};
use gix_hash::{ObjectId, oid};
use gix_object::{Data, Find, FindExt, FindHeader, Header, Kind};

pub struct Fixture {
    repo: tempfile::TempDir,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
}

impl Fixture {
    pub fn new() -> Self {
        let fixture = Self {
            repo: tempfile::tempdir().unwrap(),
            objects: BTreeMap::new(),
        };
        fixture.git(
            &[
                "init",
                "--bare",
                "--object-format=sha1",
                "--template=",
                "-q",
            ],
            None,
            b"",
        );
        fixture
    }

    fn git(&self, args: &[&str], path: Option<&[u8]>, input: &[u8]) -> Vec<u8> {
        // GNU timeout preserves raw path arguments on systems with a Rust timeout.
        let timeout = if Path::new("/usr/bin/gnutimeout").exists() {
            "/usr/bin/gnutimeout"
        } else {
            "timeout"
        };
        let mut command = Command::new(timeout);
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("LC_ALL", "C")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_ATTR_NOSYSTEM", "1")
            .current_dir(self.repo.path())
            .args([
                "20s",
                "git",
                "--no-pager",
                "--no-replace-objects",
                "--literal-pathspecs",
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(path) = path {
            command.arg(OsStr::from_bytes(path));
        }
        let mut child = command.spawn().expect("native Git oracle (with timeout)");
        child.stdin.take().unwrap().write_all(input).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "native Git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    fn insert(&mut self, kind: Kind, data: Vec<u8>) -> ObjectId {
        let output = self.git(
            &["hash-object", "-w", "-t", &kind.to_string(), "--stdin"],
            None,
            &data,
        );
        let id = ObjectId::from_hex(output.strip_suffix(b"\n").unwrap()).unwrap();
        self.objects.insert(id, (kind, data));
        id
    }

    pub fn commit(
        &mut self,
        path: &[u8],
        blob: &[u8],
        parents: &[ObjectId],
        time: u32,
        title: &str,
    ) -> ObjectId {
        let blob = self.insert(Kind::Blob, blob.to_vec());
        let mut tree = b"100644 ".to_vec();
        tree.extend_from_slice(path);
        tree.push(0);
        tree.extend_from_slice(blob.as_bytes());
        let tree = self.insert(Kind::Tree, tree);
        let mut commit = Vec::new();
        writeln!(commit, "tree {tree}").unwrap();
        for parent in parents {
            writeln!(commit, "parent {parent}").unwrap();
        }
        writeln!(
            commit,
            "author Probe Author <author@example.invalid> {time} -0000"
        )
        .unwrap();
        writeln!(
            commit,
            "committer Probe Committer <committer@example.invalid> {time} +0530"
        )
        .unwrap();
        writeln!(commit, "\n{title}").unwrap();
        self.insert(Kind::Commit, commit)
    }

    pub fn native(&self, commit: ObjectId, path: &[u8]) -> Vec<u8> {
        self.git(
            &[
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

    pub fn annotate(&self, commit: ObjectId, path: &[u8]) -> Annotation {
        annotate(self, commit, path.into()).expect("public blame must cover the whole file")
    }

    #[allow(dead_code)] // Only the git2 integration-test target imports the objects.
    pub fn objects(&self) -> impl Iterator<Item = (ObjectId, Kind, &[u8])> + '_ {
        self.objects
            .iter()
            .map(|(&oid, (kind, raw))| (oid, *kind, raw.as_slice()))
    }
}

impl Find for Fixture {
    fn try_find<'a>(&self, id: &oid, buf: &'a mut Vec<u8>) -> gix_error::Result<Option<Data<'a>>> {
        Ok(self.objects.get(&id.to_owned()).map(|(kind, data)| {
            buf.clear();
            buf.extend_from_slice(data);
            Data::new(buf, *kind, id.kind())
        }))
    }
}

impl FindHeader for Fixture {
    fn try_header(&self, id: &oid) -> gix_error::Result<Option<Header>> {
        Ok(self.objects.get(&id.to_owned()).map(|(kind, data)| Header {
            kind: *kind,
            size: data.len() as u64,
        }))
    }
}

pub fn header(row: &AttributedRange, offset: u32) -> String {
    let mut out = format!(
        "{} {} {}",
        row.commit,
        row.source.start + offset + 1,
        row.target.start + offset + 1
    );
    if offset == 0 {
        out.push_str(&format!(" {}", row.target.end - row.target.start));
    }
    out
}

fn quote(path: &[u8]) -> String {
    if path
        .iter()
        .all(|&b| matches!(b, b' '..=b'~') && !matches!(b, b'"' | b'\\'))
    {
        return String::from_utf8(path.to_vec()).unwrap();
    }
    let mut out = String::from("\"");
    for &byte in path {
        match byte {
            b'\t' => out.push_str("\\t"),
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b' '..=b'~' => out.push(char::from(byte)),
            0x80..=0xff => out.push_str(&format!("\\{byte:03o}")),
            _ => panic!("path escape outside these fixtures"),
        }
    }
    out.push('"');
    out
}

// Fixture-only serialization, not a native metadata implementation. For a
// single parent, require either a positive path record or same-path object proof.
fn render(f: &Fixture, annotation: &Annotation) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut lines = annotation.blob.split_inclusive(|&byte| byte == b'\n');
    let mut buf = Vec::new();
    for row in &annotation.ranges {
        let commit = f.find_commit(row.commit.as_ref(), &mut buf).unwrap();
        let source_path: &[u8] = row.source_path.as_ref();
        let previous = match commit.parents.as_slice() {
            [] => None,
            [parent] => {
                let mut matching = annotation.debug_path.iter().flatten().filter(|edge| {
                    let edge_path: &[u8] = edge.source_file_path.as_ref();
                    edge.commit_id == row.commit && edge_path == source_path
                });
                let previous = match (matching.next(), matching.next()) {
                    (Some(edge), None) if edge.parent_index == 0 => {
                        edge.previous_source_file_path.as_ref().map(|path| {
                            (
                                std::str::from_utf8(parent.as_ref()).unwrap().to_owned(),
                                quote(path.as_ref()),
                            )
                        })
                    }
                    (None, None) => git_rs_upstream_probe::blame_backend::same_path_previous(
                        f,
                        row.commit,
                        source_path.into(),
                    )
                    .map_err(|err| err.to_string())?
                    .map(|id| (id.to_string(), quote(source_path))),
                    _ => None,
                };
                Some(previous.ok_or_else(|| {
                    format!(
                        "missing or ambiguous previous evidence for {} ({source_path:?})",
                        row.commit
                    )
                })?)
            }
            _ => {
                return Err(format!(
                    "no native predecessor selection for merge {}",
                    row.commit
                ));
            }
        };
        let subject = std::str::from_utf8(commit.message.as_ref())
            .unwrap()
            .strip_suffix('\n')
            .unwrap();
        for offset in 0..row.target.end - row.target.start {
            writeln!(out, "{}", header(row, offset)).unwrap();
            for (label, signature) in [("author", commit.author), ("committer", commit.committer)] {
                let signature = std::str::from_utf8(signature.as_ref()).unwrap();
                let (name, tail) = signature.split_once(" <").unwrap();
                let (email, date) = tail.split_once("> ").unwrap();
                let (time, tz) = date.split_once(' ').unwrap();
                writeln!(
                    out,
                    "{label} {name}\n{label}-mail <{email}>\n{label}-time {time}\n{label}-tz {tz}"
                )
                .unwrap();
            }
            writeln!(out, "summary {subject}").unwrap();
            if let Some((parent, path)) = &previous {
                writeln!(out, "previous {parent} {path}").unwrap();
            }
            writeln!(out, "filename {}", quote(source_path)).unwrap();
            let line = lines.next().expect("annotation promised complete coverage");
            out.push(b'\t');
            out.extend_from_slice(line);
            if !line.ends_with(b"\n") {
                out.push(b'\n');
            }
        }
    }
    assert!(lines.next().is_none());
    Ok(out)
}

pub fn assert_full(f: &Fixture, commit: ObjectId, path: &[u8], native: &[u8]) {
    let actual = f.annotate(commit, path);
    let bytes =
        render(f, &actual).expect("strict full native capability gate requires all metadata");
    assert_eq!(bytes, native, "full native --line-porcelain differs");
}
