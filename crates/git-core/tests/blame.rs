#[path = "support/blame_fixture.rs"]
mod fixture;

use fixture::{Fixture, control, render};
use git_rs_core::{
    BlameError, BlameOptions, BlameRequest, CapacityBudget, CapacityError, ProducerVersion,
    StopReason,
};

#[test]
fn complete_diamond_and_unrelated_root_dags_match_native_bytes() {
    let mut fixture = Fixture::new();
    let root = fixture.commit(
        &[(b"file", b"one\ntwo\nthree\n", "100644")],
        &[],
        90,
        b"Root\n",
        None,
    );
    let left = fixture.commit(
        &[(b"file", b"one\nleft\nthree\n", "100644")],
        &[root],
        50,
        b"Left\n",
        None,
    );
    let right = fixture.commit(
        &[(b"file", b"one\ntwo\nright\n", "100644")],
        &[root],
        50,
        b"Right\n",
        None,
    );
    let merge = fixture.commit(
        &[(b"file", b"one\nleft\nright\n", "100644")],
        &[left, right],
        10,
        b"Merge\n",
        None,
    );
    let unrelated = fixture.commit(
        &[(b"file", b"from unrelated\n", "100644")],
        &[],
        1,
        b"Other root\n",
        None,
    );
    let joined = fixture.commit(
        &[(b"file", b"one\nleft\nright\nfrom unrelated\n", "100644")],
        &[merge, unrelated],
        1,
        b"Join unrelated roots\n",
        None,
    );
    for commit in [root, merge, joined] {
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file")
        );
    }
}

#[test]
fn woof_cold_blame_after_oversized_terminal_ref_admission_matches_native() {
    use git_rs_core::{Porcelain, ReadLimits, SnapshotSession, VerifiedObjectProvider};
    let mut fixture = Fixture::new();
    let tip = fixture.commit(&[(b"file", b"small\n", "100644")], &[], 1, b"Small\n", None);
    let terminal = fixture.insert(gix_object::Kind::Blob, &[b'x'; 2048]);
    fixture.git(
        &["update-ref", "refs/heads/main", &tip.to_string()],
        None,
        b"",
    );
    fixture.git(
        &["update-ref", "refs/tags/bytes", &terminal.to_string()],
        None,
        b"",
    );
    fixture.git(&["fsck", "--strict"], None, b"");
    let provider = fixture.provider();
    let identity = provider.identity().clone();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let ctl = control();
    let session = SnapshotSession::open(
        provider,
        &identity,
        ctl.clone(),
        budget.clone(),
        ReadLimits {
            max_object_bytes: 512,
            ..BlameOptions::default().read_limits()
        },
    )
    .unwrap();
    assert!(
        session.object(&terminal).is_err(),
        "a full read really is over limit"
    );
    let visible = session
        .reachable_from_refs([
            (b"refs/heads/main".as_slice(), tip),
            (b"refs/tags/bytes".as_slice(), terminal),
        ])
        .expect("an unrelated terminal blob must not prevent cold foreground admission");
    assert_eq!(visible.iter().collect::<Vec<_>>(), vec![tip]);
    let complete = session
        .blame_complete(
            BlameRequest {
                commit: &tip,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .unwrap();
    let output =
        Porcelain::encode_complete(&complete, &ctl, &budget, git_rs_core::blame::MAX_BODY_BYTES)
            .unwrap();
    assert_eq!(
        output.chunks().flatten().copied().collect::<Vec<_>>(),
        fixture.native(tip, b"file")
    );
    drop(output);
    drop(complete);
    drop(visible);
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn woof_cold_blame_of_wide_trees_matches_native_and_retains_failure_boundaries() {
    let mut fixture = Fixture::new();
    let filler = fixture.insert(gix_object::Kind::Blob, b"small\n");
    let before = fixture.insert(gix_object::Kind::Blob, b"before\nanchor\n");
    let after = fixture.insert(gix_object::Kind::Blob, b"after\nanchor\n");
    let names: Vec<_> = (0..2000).map(|n| format!("file{n:04}")).collect();
    let mut entries: Vec<_> = names
        .iter()
        .map(|name| (name.as_bytes(), "100644", filler))
        .collect();
    entries[1999].2 = before;
    let old_tree = fixture.tree(&entries);
    let root = fixture.commit_tree(old_tree, &[], 1, b"Wide root\n", None);
    entries[1999].2 = after;
    let tree = fixture.tree(&entries);
    assert_eq!(
        fixture.git(&["cat-file", "-s", &tree.to_string()], None, b""),
        b"72000\n"
    );
    let tip = fixture.commit_tree(tree, &[root], 2, b"Wide edit\n", None);
    fixture.git(
        &["update-ref", "refs/heads/main", &tip.to_string()],
        None,
        b"",
    );
    fixture.git(&["fsck", "--strict"], None, b"");
    for commit in [root, tip] {
        let native = fixture.native(commit, b"file1999");
        for options in [
            BlameOptions::default(),
            BlameOptions::native_root(Default::default()),
        ] {
            assert_eq!(render(&fixture, commit, b"file1999", &options), native);
        }
    }
    // A valid wide tree must reach admission, not be mislabeled corrupt. Its
    // actual backing still cannot exceed the shared capacity budget.
    let budget = CapacityBudget::new(64 * 1024);
    let session = fixture.session(budget.clone(), control());
    assert!(matches!(
        session.blame_complete(
            BlameRequest {
                commit: &tip,
                path: b"file1999"
            },
            &BlameOptions::default()
        ),
        Err(BlameError::Capacity(_))
    ));
    assert_eq!(budget.used(), 0);
    drop(session);
    let mut malformed = fixture.provider().objects[&tree].1.clone();
    malformed.extend_from_slice(b"100644 truncated\0\x01");
    let broken_tree = fixture.insert(gix_object::Kind::Tree, &malformed);
    let broken = fixture.commit_tree(broken_tree, &[], 3, b"Malformed tree\n", None);
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let ctl = control();
    let session = fixture.session(budget.clone(), ctl.clone());
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &broken,
                    path: b"file1999"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::Corrupt("physical tree entry"))
    );
    assert_eq!(budget.used(), 0);
    ctl.cancel();
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &tip,
                    path: b"file1999"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::Stopped(StopReason::Cancelled))
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn octopus_and_crisscross_dags_match_complete_native_output() {
    let mut fixture = Fixture::new();
    let root = fixture.commit(
        &[(b"file", b"a\nb\nc\n", "100644")],
        &[],
        20,
        b"Root\n",
        None,
    );
    let a = fixture.commit(
        &[(b"file", b"A\nb\nc\n", "100644")],
        &[root],
        10,
        b"A\n",
        None,
    );
    let b = fixture.commit(
        &[(b"file", b"a\nB\nc\n", "100644")],
        &[root],
        10,
        b"B\n",
        None,
    );
    let c = fixture.commit(
        &[(b"file", b"a\nb\nC\n", "100644")],
        &[root],
        10,
        b"C\n",
        None,
    );
    let octopus = fixture.commit(
        &[(b"file", b"A\nB\nC\n", "100644")],
        &[a, b, c],
        1,
        b"Octopus\n",
        None,
    );
    let ab = fixture.commit(
        &[(b"file", b"A\nB\nc\n", "100644")],
        &[a, b],
        4,
        b"AB\n",
        None,
    );
    let ba = fixture.commit(
        &[(b"file", b"A\nB\nc\n", "100644")],
        &[b, a],
        4,
        b"BA\n",
        None,
    );
    let crossed = fixture.commit(
        &[(b"file", b"A\nB\nC\n", "100644")],
        &[ab, ba, c],
        1,
        b"Crisscross\n",
        None,
    );
    for tip in [octopus, crossed] {
        assert_eq!(
            render(&fixture, tip, b"file", &BlameOptions::default()),
            fixture.native(tip, b"file")
        );
    }
}

#[test]
fn annotated_refs_peel_physically_but_blame_requires_an_explicit_commit() {
    let mut fixture = Fixture::new();
    let tip = fixture.commit(
        &[(b"file", b"tagged\n", "100644")],
        &[],
        1,
        b"Tagged\n",
        None,
    );
    let dangling = fixture.commit(
        &[(b"file", b"dangling\n", "100644")],
        &[],
        2,
        b"Unpublished\n",
        None,
    );
    let tag = fixture.insert(gix_object::Kind::Tag, format!("object {tip}\ntype commit\ntag synthetic\ntagger Tagger <tagger@example.invalid> 3 +0000\n\nAnnotated\n").as_bytes());
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let session = fixture.session(budget.clone(), control());
    let visible = session
        .reachable_from_refs([(b"refs/tags/synthetic".as_slice(), tag)])
        .unwrap();
    assert_eq!(visible.iter().collect::<Vec<_>>(), vec![tip]);
    assert!(!visible.iter().any(|id| id == dangling));
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &tag,
                    path: b"file"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::InvalidRequest)
    );
    assert_eq!(
        render(&fixture, tip, b"file", &BlameOptions::default()),
        fixture.native(tag, b"file")
    );
    // Physical availability is not authority: root must check its published refs
    // before calling blame. The core never fabricates a ref for a dangling OID.
    drop(visible);
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn large_metadata_messages_on_nested_tags_and_their_commit_match_native() {
    let mut fixture = Fixture::new();
    let mut message = vec![b'\n'; 70_000];
    message.extend_from_slice(b"First nonblank subject\n");
    let root = fixture.commit(&[(b"file", b"line\n", "100644")], &[], 1, &message, None);
    let mut tip = root;
    for kind in [gix_object::Kind::Commit, gix_object::Kind::Tag] {
        let mut bytes = format!(
            "object {tip}\ntype {kind}\ntag release\ntagger T <t@example.invalid> 1 +0000\n\n"
        )
        .into_bytes();
        bytes.extend_from_slice(&vec![b'm'; 70_000]);
        bytes.extend_from_slice(b"\n-----BEGIN PGP SIGNATURE-----\nopaque\n");
        tip = fixture.insert(gix_object::Kind::Tag, &bytes);
    }
    fixture.git(
        &["update-ref", "refs/tags/large", &tip.to_string()],
        None,
        b"",
    );
    fixture.git(&["fsck", "--strict"], None, b"");
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let session = fixture.session(budget.clone(), control());
    let visible = session
        .reachable_from_refs([(b"refs/tags/large".as_slice(), tip)])
        .expect("physical nested tags may carry separately bounded large messages");
    assert_eq!(visible.iter().collect::<Vec<_>>(), vec![root]);
    drop(visible);
    drop(session);
    assert_eq!(budget.used(), 0);
    assert_eq!(
        render(&fixture, root, b"file", &BlameOptions::default()),
        fixture.native(tip, b"file")
    );
}

#[test]
fn rename_edit_raw_paths_and_previous_fields_match_native_bytes() {
    let mut fixture = Fixture::new();
    let before: &[u8] = b"old\t\xff";
    let after: &[u8] = b"new\n\"\\\xfe";
    let root = fixture.commit(
        &[(before, b"anchor\nsecond\nthird\n", "100644")],
        &[],
        3,
        b"Before\n",
        None,
    );
    let renamed = fixture.commit(
        &[(after, b"anchor\nchanged\nthird\n", "100644")],
        &[root],
        2,
        b"Rename and edit\n",
        None,
    );
    let changed = fixture.commit(
        &[(after, b"anchor\nchanged again\nthird\n", "100644")],
        &[renamed],
        1,
        b"Edit\n",
        None,
    );
    let options = BlameOptions::native_root(gix_blame::Tuning {
        reuse_line_tokens: true,
        retain_current_input_tokens: true,
        reuse_unchanged_runs: true,
        bounded_resource_cache: true,
        index_changes: true,
    });
    assert_eq!(
        render(&fixture, changed, after, &options),
        fixture.native(changed, after)
    );
}

#[test]
fn symlink_type_change_same_blob_and_nested_literal_path_match_native() {
    let mut fixture = Fixture::new();
    let root = fixture.commit(
        &[(b"link", b"target\n", "100644")],
        &[],
        5,
        b"Regular\n",
        None,
    );
    let link = fixture.commit(
        &[(b"link", b"target\n", "120000")],
        &[root],
        4,
        b"Symlink\n",
        None,
    );
    assert_eq!(
        render(&fixture, link, b"link", &BlameOptions::default()),
        fixture.native(link, b"link")
    );
    let blob = fixture.insert(gix_object::Kind::Blob, b"nested\r\nlast");
    let subtree = fixture.tree(&[(b"*literal", "100755", blob)]);
    let tree = fixture.tree(&[(b"directory", "40000", subtree)]);
    let nested = fixture.commit_tree(tree, &[], 2, b"Nested\n", None);
    assert_eq!(
        render(
            &fixture,
            nested,
            b"directory/*literal",
            &BlameOptions::default()
        ),
        fixture.native(nested, b"directory/*literal")
    );
}

#[test]
fn options_and_producer_have_stable_versioned_non_debug_identities() {
    let first = BlameOptions::default();
    let second = BlameOptions::native_root(gix_blame::Tuning {
        index_changes: true,
        ..Default::default()
    });
    assert_eq!(first.canonical_bytes().len(), 32);
    assert_eq!(&first.canonical_bytes()[..8], b"GRCBLM01");
    assert_eq!(
        first.canonical_bytes(),
        BlameOptions::default().canonical_bytes()
    );
    assert_ne!(first.canonical_bytes(), second.canonical_bytes());
    assert!(first.read_limits().max_graph_objects >= 160_000);
    let producer = ProducerVersion::current().cache_id();
    let digest = producer.rsplit('/').next().unwrap();
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(producer, ProducerVersion::current().cache_id());
}

#[test]
fn qualified_default_keeps_the_native_profile_and_explicit_conservative_selection() {
    let options = BlameOptions::default();
    assert_eq!(
        options.canonical_bytes(),
        b"GRCBLM01\x01\x01\x01\x32\x00\x00\x03\xe8\x1f\x01\x00\x0f\x42\x40\x08\x00\x00\x00\x10\x00\x00\x00\x02\x00"
    );
    assert_eq!(
        options.tuning(),
        git_rs_core::BlameTuning {
            reuse_line_tokens: true,
            retain_current_input_tokens: true,
            reuse_unchanged_runs: true,
            bounded_resource_cache: true,
            index_changes: true,
        }
    );
    let conservative = BlameOptions::native_root(git_rs_core::BlameTuning::default());
    assert_eq!(conservative.canonical_bytes()[16], 0);
    assert_eq!(
        &options.canonical_bytes()[..16],
        &conservative.canonical_bytes()[..16]
    );
    assert_eq!(
        &options.canonical_bytes()[17..],
        &conservative.canonical_bytes()[17..]
    );
    for limits in [options.read_limits(), conservative.read_limits()] {
        assert_eq!(limits.max_object_bytes, 128 * 1024 * 1024);
        assert_eq!(limits.max_metadata_bytes, 64 * 1024);
        assert_eq!(limits.max_graph_objects, 1_000_000);
        assert_eq!(limits.max_graph_edges, 8_000_000);
    }
}

#[test]
fn invalid_missing_cancelled_and_capacity_errors_never_return_partial_blame() {
    let mut fixture = Fixture::new();
    let commit = fixture.commit(
        &[(b"file", b"one\ntwo\n", "100644")],
        &[],
        1,
        b"Root\n",
        None,
    );
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let ctl = control();
    let session = fixture.session(budget.clone(), ctl.clone());
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &commit,
                    path: b"../file"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::InvalidRequest)
    );
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &commit,
                    path: b"absent"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::NotFound)
    );
    ctl.cancel();
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &commit,
                    path: b"file"
                },
                &BlameOptions::default()
            )
            .err(),
        Some(BlameError::Stopped(StopReason::Cancelled))
    );
    drop(session);
    assert_eq!(budget.used(), 0);
    for limit in [0, 1024] {
        let tiny = CapacityBudget::new(limit);
        let session = fixture.session(tiny.clone(), control());
        assert_eq!(
            session
                .blame_complete(
                    BlameRequest {
                        commit: &commit,
                        path: b"file"
                    },
                    &BlameOptions::default()
                )
                .err(),
            Some(BlameError::Capacity(CapacityError::Exhausted))
        );
        drop(session);
        assert_eq!(
            tiny.used(),
            0,
            "failed admission must not leak or erase the typed cause"
        );
    }
}

#[test]
fn explicit_work_cap_fails_instead_of_turning_a_truncated_walk_into_a_boundary() {
    let mut fixture = Fixture::new();
    let root = fixture.commit(&[(b"file", b"stable\n", "100644")], &[], 9, b"Root\n", None);
    let tip = fixture.commit(
        &[(b"file", b"stable\n", "100644")],
        &[root],
        1,
        b"Tip\n",
        None,
    );
    let provider = fixture.provider();
    let identity = provider.identity.clone();
    let options = BlameOptions::default();
    let limits = git_rs_core::ReadLimits {
        max_graph_objects: 1,
        ..options.read_limits()
    };
    let budget = CapacityBudget::new(4 * 1024 * 1024);
    let session =
        git_rs_core::SnapshotSession::open(provider, &identity, control(), budget.clone(), limits)
            .unwrap();
    assert_eq!(
        session
            .blame_complete(
                BlameRequest {
                    commit: &tip,
                    path: b"file"
                },
                &options
            )
            .err(),
        Some(BlameError::WorkLimit)
    );
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn corrupted_physical_bytes_cannot_become_a_complete_attribution() {
    let mut fixture = Fixture::new();
    let tip = fixture.commit(&[(b"file", b"one\n", "100644")], &[], 1, b"Root\n", None);
    let blob =
        gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, b"one\n").unwrap();
    let mut provider = fixture.provider();
    std::sync::Arc::make_mut(&mut provider.objects)
        .get_mut(&blob)
        .unwrap()
        .1[0] = b'X';
    let identity = provider.identity.clone();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let session = git_rs_core::SnapshotSession::open(
        provider,
        &identity,
        control(),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap();
    assert!(matches!(
        session.blame_complete(
            BlameRequest {
                commit: &tip,
                path: b"file"
            },
            &BlameOptions::default()
        ),
        Err(BlameError::Corrupt(_))
    ));
    drop(session);
    assert_eq!(budget.used(), 0);
}
