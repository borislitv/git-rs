//! Attribution only: no `previous`, boundary, or full-porcelain parity claim.
#![forbid(unsafe_code)]

use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::Path};

use git2::{BlameOptions, ObjectType, Odb, Oid, Repository, Version};
use gix_hash::ObjectId;
use gix_object::Kind;

#[path = "support/blame.rs"]
#[allow(dead_code)] // The separate gix test target uses the other fixture helpers.
mod support;

use support::Fixture;

// Source commit, 1-based source and target starts, and group length.
type Group = (ObjectId, usize, usize, usize);

fn as_oid(id: ObjectId) -> Oid {
    Oid::from_bytes(id.as_bytes()).unwrap()
}

fn assert_attribution(
    f: &Fixture,
    tip: ObjectId,
    path: &[u8],
    porcelain_path: &str,
    groups: &[Group],
) {
    // Check the whole expected group/line sequence against the independent
    // Git 2.56 oracle supplied on PATH by the qualification runner.
    let native = f.native(tip, path);
    let native_projection: Vec<_> = native
        .split(|&b| b == b'\n')
        .filter(|line| {
            line.starts_with(b"filename ")
                || (line.get(40) == Some(&b' ') && line[..40].iter().all(u8::is_ascii_hexdigit))
        })
        .collect();
    let mut expected_projection = Vec::new();
    for &(owner, source, target, count) in groups {
        for offset in 0..count {
            let mut header = format!("{owner} {} {}", source + offset, target + offset);
            if offset == 0 {
                header.push_str(&format!(" {count}"));
            }
            expected_projection.push(header.into_bytes());
            expected_projection.push(format!("filename {porcelain_path}").into_bytes());
        }
    }
    assert_eq!(
        native_projection,
        expected_projection
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>(),
        "independent native attribution fixture"
    );

    let version = Version::get();
    let (major, minor, patch) = version.libgit2_version();
    eprintln!(
        "libgit2 {major}.{minor}.{patch}; bundled={}",
        version.vendored()
    );
    assert_eq!(version.libgit2_version(), (1, 9, 7));
    assert!(version.vendored());
    let repo = Repository::from_odb(Odb::new().unwrap()).unwrap();
    let odb = repo.odb().unwrap();
    let _mempack = odb.add_new_mempack_backend(1000).unwrap();
    for (id, kind, raw) in f.objects() {
        let kind = match kind {
            Kind::Blob => ObjectType::Blob,
            Kind::Tree => ObjectType::Tree,
            Kind::Commit => ObjectType::Commit,
            Kind::Tag => ObjectType::Tag,
        };
        assert_eq!(odb.write(kind, raw).unwrap(), as_oid(id));
    }

    let mut options = BlameOptions::new();
    options.newest_commit(as_oid(tip));
    let blame = repo
        .blame_file(Path::new(OsStr::from_bytes(path)), Some(&mut options))
        .unwrap();
    let actual: Vec<_> = blame
        .iter()
        .map(|h| {
            (
                h.orig_commit_id(),
                h.path().unwrap().as_os_str().as_bytes().to_vec(),
                h.orig_start_line(),
                h.final_start_line(),
                h.lines_in_hunk(),
            )
        })
        .collect();
    let expected: Vec<_> = groups
        .iter()
        .map(|&(owner, source, target, count)| {
            (as_oid(owner), path.to_vec(), source, target, count)
        })
        .collect();
    assert_eq!(actual, expected, "libgit2 attribution differs from native");
}

#[test]
fn git2_attribution_independent_root_merge() {
    let mut f = Fixture::new();
    let left = f.commit(b"f", b"left\n", &[], 20, "Left root");
    let right = f.commit(b"f", b"right\n", &[], 10, "Right root");
    let tip = f.commit(b"f", b"left\nright\n", &[left, right], 30, "Merge");
    assert_attribution(&f, tip, b"f", "f", &[(left, 1, 1, 1), (right, 1, 2, 1)]);
}

#[test]
fn git2_attribution_merge_prefers_same_path_before_rename() {
    let mut f = Fixture::new();
    let path = b"f\t\xfe";
    let blob = b"first line\nsecond line\n";
    let left = f.commit(b"old\t\xff", blob, &[], 20, "Rename candidate");
    let right = f.commit(path, blob, &[], 10, "Same-path candidate");
    let tip = f.commit(path, blob, &[left, right], 30, "Merge");
    assert_attribution(&f, tip, path, r#""f\t\376""#, &[(right, 1, 1, 2)]);
}
