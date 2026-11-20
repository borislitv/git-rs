#![forbid(unsafe_code)]

#[path = "support/blame.rs"]
mod support;

use support::{Fixture, assert_full, header};

#[test]
fn native_full_root_and_empty_controls() {
    let mut f = Fixture::new();
    for blob in [b"first\r\nlast\0\xff".as_slice(), b""] {
        let root = f.commit(b"f", blob, &[], 10, "Root");
        assert_full(&f, root, b"f", &f.native(root, b"f"));
    }
}

#[test]
fn native_full_same_path_edit_with_surviving_line() {
    let mut f = Fixture::new();
    let root = f.commit(b"f", b"anchor\nold\n", &[], 10, "Root");
    let tip = f.commit(b"f", b"anchor\nnew\n", &[root], 20, "Edit");
    assert_full(&f, tip, b"f", &f.native(tip, b"f"));
}

#[test]
fn native_full_grouping_gap_after_middle_line_deletion() {
    let mut f = Fixture::new();
    let root = f.commit(b"f", b"first\ndeleted\nlast\n", &[], 10, "Root");
    let tip = f.commit(b"f", b"first\nlast\n", &[root], 20, "Delete middle");
    assert_full(&f, tip, b"f", &f.native(tip, b"f"));
}

#[test]
fn native_full_rename_edit_with_raw_paths() {
    let mut f = Fixture::new();
    let old = b"old\t\xff";
    let new = b"new\t\xfe";
    let root = f.commit(
        old,
        b"a long first anchor kept intact\nold\na long last anchor kept intact\n",
        &[],
        10,
        "Root",
    );
    let tip = f.commit(
        new,
        b"a long first anchor kept intact\nnew\na long last anchor kept intact\n",
        &[root],
        20,
        "Rename and edit",
    );
    assert_full(&f, tip, new, &f.native(tip, new));
}

fn replacement(f: &mut Fixture) -> (gix_hash::ObjectId, gix_hash::ObjectId) {
    let root = f.commit(b"f", b"old\n", &[], 10, "Root");
    let tip = f.commit(b"f", b"new\n", &[root], 20, "Replace entire line");
    (root, tip)
}

#[test]
fn native_ranges_only_after_total_replacement() {
    let mut f = Fixture::new();
    let (_, tip) = replacement(&mut f);
    let native = f.native(tip, b"f");
    let actual = f.annotate(tip, b"f");
    let [range] = actual.ranges.as_slice() else {
        panic!("expected a single attributed range: {:?}", actual.ranges);
    };
    assert_eq!(
        header(range, 0).as_bytes(),
        native.split(|&b| b == b'\n').next().unwrap(),
        "range projection only, not previous or complete porcelain"
    );
}

#[test]
fn native_full_previous_after_total_replacement() {
    let mut f = Fixture::new();
    let (root, tip) = replacement(&mut f);
    let native = f.native(tip, b"f");
    let previous = format!("previous {root} f");
    assert!(
        native
            .split(|&b| b == b'\n')
            .any(|line| line == previous.as_bytes())
    );
    assert_full(&f, tip, b"f", &native);
}

#[test]
fn same_path_previous_does_not_infer_a_rename_from_identical_blob() {
    let mut f = Fixture::new();
    let root = f.commit(b"old", b"identical\n", &[], 10, "Root");
    let tip = f.commit(b"new", b"identical\n", &[root], 20, "New path");
    assert_eq!(
        git_rs_upstream_probe::blame_backend::same_path_previous(&f, tip, b"new".as_slice().into())
            .unwrap(),
        None,
    );
}

#[test]
fn native_full_same_path_whole_parent_beats_earlier_exact_rename() {
    let mut f = Fixture::new();
    let blob = b"whole file\n";
    let rename = f.commit(b"old", blob, &[], 20, "Earlier parent, other path");
    let same = f.commit(b"new", blob, &[], 10, "Later parent, same path");
    let tip = f.commit(b"new", blob, &[rename, same], 30, "Merge");
    let native = f.native(tip, b"new");
    assert!(native.starts_with(format!("{same} ").as_bytes()));
    assert_full(&f, tip, b"new", &native);
}

#[test]
fn native_full_symlink_target_replacement() {
    let mut f = Fixture::new();
    let root = f.commit_mode(b"120000", b"link", b"old-target", &[], 10, "Old target");
    let tip = f.commit_mode(b"120000", b"link", b"new-target", &[root], 20, "New target");
    assert_full(&f, root, b"link", &f.native(root, b"link"));
    assert_full(&f, tip, b"link", &f.native(tip, b"link"));
}

#[test]
fn same_path_previous_rejects_equal_blob_and_class_change() {
    use git_rs_upstream_probe::blame_backend::same_path_previous;

    let mut f = Fixture::new();
    let root = f.commit(b"f", b"target", &[], 10, "Regular");
    let chmod = f.commit_mode(b"100755", b"f", b"target", &[root], 20, "Executable");
    let link = f.commit_mode(b"120000", b"f", b"other-target", &[root], 20, "Link");
    for tip in [chmod, link] {
        assert_eq!(
            same_path_previous(&f, tip, b"f".as_slice().into()).unwrap(),
            None
        );
    }
}

#[test]
fn native_full_myers_repeated_line_tie() {
    let mut f = Fixture::new();
    let root = f.commit(
        b"f",
        b"start\nrepeat\npivot\nrepeat\nend\n",
        &[],
        10,
        "Root",
    );
    let tip = f.commit(
        b"f",
        b"start\nrepeat\nrepeat\npivot\nend\n",
        &[root],
        20,
        "Reorder",
    );
    assert_full(&f, tip, b"f", &f.native(tip, b"f"));
}

#[test]
fn native_full_rename_origin_after_its_only_anchor_is_edited() {
    let mut f = Fixture::new();
    let root = f.commit(
        b"old",
        b"a long anchor survives the rename\nold\n",
        &[],
        10,
        "Root",
    );
    let renamed = f.commit(
        b"new",
        b"a long anchor survives the rename\nnew\n",
        &[root],
        20,
        "Rename and edit",
    );
    let tip = f.commit(
        b"new",
        b"the anchor is gone\nnew\n",
        &[renamed],
        30,
        "Edit anchor",
    );
    let native = f.native(tip, b"new");
    let previous = format!("previous {root} old");
    assert!(
        native
            .split(|&b| b == b'\n')
            .any(|line| line == previous.as_bytes())
    );
    assert_full(&f, tip, b"new", &native);
}

#[test]
fn rename_previous_requires_more_than_half_complete_shared_spans() {
    use git_rs_upstream_probe::blame_backend::rename_previous;

    let mut f = Fixture::new();
    let root = f.commit(b"old", b"same\nold!\n", &[], 10, "Root");
    let tip = f.commit(b"new", b"same\nnew!\n", &[root], 20, "Only half shared");
    assert_eq!(
        rename_previous(&f, tip, b"new".as_slice().into()).unwrap(),
        None
    );
}

#[test]
fn native_full_independent_root_merge() {
    let mut f = Fixture::new();
    let left = f.commit(b"f", b"left\n", &[], 20, "Left root");
    let right = f.commit(b"f", b"right\n", &[], 10, "Right root");
    let merge = f.commit(b"f", b"left\nright\n", &[left, right], 30, "Merge");
    let native = f.native(merge, b"f");
    for root in [left, right] {
        let id = root.to_string();
        assert!(
            native
                .split(|&b| b == b'\n')
                .any(|line| line.starts_with(id.as_bytes()))
        );
    }
    assert_full(&f, merge, b"f", &native);
}
