#[path = "support/blame_fixture.rs"]
mod fixture;

use std::sync::atomic::{AtomicU8, Ordering};

use fixture::{Fixture, Provider, control};
use git_rs_core::blame::{BlameError, CompletedUse, MAX_BODY_BYTES, MAX_COMPLETED_MAP_BYTES};
use git_rs_core::{
    BlameOptions, BlameRequest, CapacityBudget, Control, Object, Porcelain, ReadLimits,
    SnapshotIdentity, SnapshotSession, VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

struct Guarded {
    inner: Provider,
    mode: AtomicU8,
    below_base: [ObjectId; 3],
}

impl Guarded {
    fn check(&self, id: &oid) {
        match self.mode.load(Ordering::Relaxed) {
            0 => {}
            1 => panic!("map encode/decode must not access the object source: {id}"),
            _ => assert!(
                !self.below_base.contains(&id.to_owned()),
                "hit accessed pre-base history: {id}"
            ),
        }
    }
}

impl VerifiedObjectProvider for &Guarded {
    fn identity(&self) -> &SnapshotIdentity {
        &self.inner.identity
    }
    fn header(&self, id: &oid, control: &Control) -> git_rs_core::objects::Result<Option<Header>> {
        self.check(id);
        self.inner.header(id, control)
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> git_rs_core::objects::Result<Option<Object>> {
        self.check(id);
        self.inner.object(id, control, budget, limits)
    }
}

#[test]
fn completed_map_matches_full_native_porcelain_and_skips_old_metadata() {
    let mut repo = Fixture::new();
    let old = b"old-\xff";
    let new = b"new-\xfe";
    let mut root_bytes = Vec::new();
    for index in 0..768 {
        root_bytes.extend_from_slice(
            format!("anchor {index:04} has a distinct context\nrepeat\nrepeat\n").as_bytes(),
        );
    }
    let root_blob = repo.insert(Kind::Blob, &root_bytes);
    let root_tree = repo.tree(&[(old, "100644", root_blob)]);
    let root = repo.commit_tree(root_tree, &[], 1, b"older metadata\n", None);
    let position = root_bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    let mut base_bytes = root_bytes.clone();
    base_bytes.splice(position..position, b"base replacement\n".iter().copied());
    let base = repo.commit(
        &[(old, &base_bytes, "100644")],
        &[root],
        2,
        b"m\xf6re metadata\n",
        Some("ISO-8859-1"),
    );
    let old_guarded = Guarded {
        inner: repo.provider(),
        mode: AtomicU8::new(0),
        below_base: [root, root_tree, root_blob],
    };
    let rename = repo.commit(
        &[(new, &base_bytes, "100644")],
        &[base],
        3,
        b"raw path rename\n",
        None,
    );
    let mut tip_bytes = base_bytes.clone();
    let position = position + b"base replacement\n".len();
    tip_bytes.splice(
        position..position + b"repeat\n".len(),
        b"changed\n".iter().copied(),
    );
    let next = position + b"changed\n".len();
    tip_bytes.drain(next..next + b"repeat\n".len());
    tip_bytes.splice(0..0, b"new first\n".iter().copied());
    tip_bytes.extend_from_slice(b"unterminated last");
    let tip = repo.commit(
        &[(new, &tip_bytes, "100644")],
        &[rename],
        4,
        b"edited tip\n",
        None,
    );
    let expected = repo.native(tip, new);
    assert!(base_bytes.len() > 16 * 1024);

    let mut newer = repo.provider();
    newer.identity.generation = b"immutable-two".to_vec();
    let guarded = Guarded {
        inner: newer,
        mode: AtomicU8::new(0),
        below_base: [root, root_tree, root_blob],
    };
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = control();
    let options = BlameOptions::default();
    let old_session = SnapshotSession::open(
        &old_guarded,
        &old_guarded.inner.identity,
        control.clone(),
        budget.clone(),
        options.read_limits(),
    )
    .unwrap();
    let session = SnapshotSession::open(
        &guarded,
        &guarded.inner.identity,
        control.clone(),
        budget.clone(),
        options.read_limits(),
    )
    .unwrap();
    let base_request = BlameRequest {
        commit: &base,
        path: old,
    };
    let tip_request = BlameRequest {
        commit: &tip,
        path: new,
    };
    let ns = [17; 32];
    let ordinary = session.blame_complete(tip_request, &options).unwrap();
    let baseline = ordinary.statistics();
    let render = |blame: &git_rs_core::blame::CompleteBlame| {
        let body = Porcelain::encode_complete(blame, &control, &budget, MAX_BODY_BYTES).unwrap();
        body.chunks().flatten().copied().collect::<Vec<_>>()
    };
    assert_eq!(ordinary.completed_use(), CompletedUse::NotRequested);
    assert_eq!(render(&ordinary), expected);
    let prior = old_session.blame_complete(base_request, &options).unwrap();
    old_guarded.mode.store(1, Ordering::Relaxed);
    guarded.mode.store(1, Ordering::Relaxed);
    let wire = old_session
        .encode_completed_blame(base_request, &options, ns, &prior, MAX_COMPLETED_MAP_BYTES)
        .unwrap();
    assert!(
        old_session
            .encode_completed_blame(
                BlameRequest {
                    path: new,
                    ..base_request
                },
                &options,
                ns,
                &prior,
                MAX_COMPLETED_MAP_BYTES
            )
            .is_err()
    );
    assert!(matches!(
        session.encode_completed_blame(base_request, &options, ns, &prior, MAX_COMPLETED_MAP_BYTES),
        Err(BlameError::InvalidRequest)
    ));
    drop(prior);
    // These sessions share the same control and budget, but replay must be
    // decoded after the new snapshot's fence, not carried over as an old map.
    let stale_map = old_session
        .decode_authenticated_completed_blame(base_request, base_request, &options, ns, &wire)
        .unwrap()
        .unwrap();
    assert!(matches!(
        session.blame_complete_with_completed(base_request, &options, ns, Some(&stale_map), None),
        Err(BlameError::InvalidRequest)
    ));
    drop(stale_map);
    let map = session
        .decode_authenticated_completed_blame(base_request, tip_request, &options, ns, &wire)
        .unwrap()
        .unwrap();
    assert!(matches!(
        session.blame_complete_with_completed(base_request, &options, ns, Some(&map), None),
        Err(BlameError::InvalidRequest)
    ));
    assert!(
        session
            .decode_authenticated_completed_blame(
                base_request,
                tip_request,
                &options,
                [18; 32],
                &wire
            )
            .unwrap()
            .is_none()
    );
    assert!(
        session
            .decode_authenticated_completed_blame(
                base_request,
                tip_request,
                &BlameOptions::native_root(Default::default()),
                ns,
                &wire
            )
            .unwrap()
            .is_none()
    );
    let mut truncated = budget.buffer(wire.as_slice().len() - 1).unwrap();
    truncated
        .extend_from_slice(&wire.as_slice()[..wire.as_slice().len() - 1])
        .unwrap();
    let used = budget.used();
    assert!(
        session
            .decode_authenticated_completed_blame(
                base_request,
                tip_request,
                &options,
                ns,
                &truncated
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        budget.used(),
        used,
        "partial decode refunds all path/row/metadata storage"
    );
    drop(truncated);
    // Stand in for an authenticated producer record whose physical blob does
    // not match. Decoding alone cannot establish membership; the fork may not graft it.
    let mut wrong_blob = budget.buffer(wire.as_slice().len()).unwrap();
    wrong_blob.extend_from_slice(wire.as_slice()).unwrap();
    let blob_offset = 8 + 4 + 32 + 32 + 20;
    wrong_blob.as_mut_slice()[blob_offset..blob_offset + 20].copy_from_slice(root_blob.as_bytes());
    let wrong_map = session
        .decode_authenticated_completed_blame(base_request, tip_request, &options, ns, &wrong_blob)
        .unwrap()
        .unwrap();

    guarded.mode.store(2, Ordering::Relaxed);
    let exact_map = session
        .decode_authenticated_completed_blame(base_request, base_request, &options, ns, &wire)
        .unwrap()
        .unwrap();
    let exact = session
        .blame_complete_with_completed(base_request, &options, ns, Some(&exact_map), None)
        .unwrap();
    drop(exact_map);
    assert!(matches!(exact.completed_use(), CompletedUse::Hit { .. }));
    assert!(!exact.can_certify_local_attribution());
    assert!(matches!(
        session.encode_completed_blame(base_request, &options, ns, &exact, MAX_COMPLETED_MAP_BYTES),
        Err(BlameError::InvalidRequest)
    ));
    assert_eq!(exact.statistics().mapping.computations, 0);
    assert_eq!(render(&exact), repo.native(base, old));
    let incremental = session
        .blame_complete_with_completed(tip_request, &options, ns, Some(&map), None)
        .unwrap();
    assert!(matches!(incremental.completed_use(), CompletedUse::Hit { lines } if lines > 1000));
    assert!(incremental.statistics().commits_traversed < baseline.commits_traversed);
    assert!(incremental.statistics().mapping.computations < baseline.mapping.computations);
    drop(map);
    drop(wire);
    // The output owns metadata independently of the map and never uses the provider at render time.
    guarded.mode.store(1, Ordering::Relaxed);
    assert_eq!(render(&incremental), expected);
    guarded.mode.store(0, Ordering::Relaxed);
    let unauthorized_blob = session
        .blame_complete_with_completed(tip_request, &options, ns, Some(&wrong_map), None)
        .unwrap();
    assert_eq!(unauthorized_blob.completed_use(), CompletedUse::Miss);
    assert_eq!(render(&unauthorized_blob), expected);
    drop(wrong_map);
    drop(wrong_blob);
    let miss = session
        .blame_complete_with_completed(tip_request, &options, ns, None, None)
        .unwrap();
    assert_eq!(miss.completed_use(), CompletedUse::Miss);
    assert_eq!(miss.statistics(), baseline);
    assert_eq!(render(&miss), expected);
    drop((ordinary, exact, incremental, unauthorized_blob, miss));
    assert_eq!(budget.used(), 0);
}

#[test]
fn admitted_multipart_buffer_larger_than_one_mib_round_trips() {
    let mut repo = Fixture::new();
    let summary = vec![b'x'; 1_048_576 + 64];
    let commit = repo.commit(&[(b"f", b"line\n", "100644")], &[], 1, &summary, None);
    let request = BlameRequest {
        commit: &commit,
        path: b"f",
    };
    let options = BlameOptions::default();
    let budget = CapacityBudget::new(32 * 1024 * 1024);
    let control = control();
    let session = repo.session(budget.clone(), control.clone());
    let complete = session.blame_complete(request, &options).unwrap();
    let ns = [1; 32];
    let wire = session
        .encode_completed_blame(request, &options, ns, &complete, MAX_COMPLETED_MAP_BYTES)
        .unwrap();
    assert!(wire.as_slice().len() > 1_048_576);
    let mut assembled = budget.buffer(wire.as_slice().len()).unwrap();
    for part in wire.as_slice().chunks(1_048_576) {
        assembled
            .extend_controlled(part, MAX_COMPLETED_MAP_BYTES, &control)
            .unwrap();
    }
    drop((wire, complete));
    let used = budget.used();
    let fill = budget.reservation(32 * 1024 * 1024 - used).unwrap();
    assert!(matches!(
        session.decode_authenticated_completed_blame(request, request, &options, ns, &assembled),
        Err(BlameError::Capacity(_))
    ));
    drop(fill);
    assert_eq!(budget.used(), used);
    let map = session
        .decode_authenticated_completed_blame(request, request, &options, ns, &assembled)
        .unwrap()
        .unwrap();
    let hit = session
        .blame_complete_with_completed(request, &options, ns, Some(&map), None)
        .unwrap();
    assert_eq!(hit.completed_use(), CompletedUse::Hit { lines: 1 });
    let output = Porcelain::encode_complete(&hit, &control, &budget, MAX_BODY_BYTES).unwrap();
    assert_eq!(
        output.chunks().flatten().copied().collect::<Vec<_>>(),
        repo.native(commit, b"f")
    );
    control.cancel();
    assert!(matches!(
        session.blame_complete_with_completed(request, &options, ns, Some(&map), None),
        Err(BlameError::Stopped(git_rs_core::StopReason::Cancelled))
    ));
    drop(map);
    drop((assembled, hit, output));
    assert_eq!(budget.used(), 0);
}

#[test]
fn completed_map_declines_a_mixed_parent_frontier() {
    let mut repo = Fixture::new();
    let path = b"f";
    let root = repo.commit(
        &[(path, b"top\nanchor\nbottom\n", "100644")],
        &[],
        1,
        b"root\n",
        None,
    );
    let left = repo.commit(
        &[(path, b"left\nanchor\nbottom\n", "100644")],
        &[root],
        3,
        b"left\n",
        None,
    );
    let right = repo.commit(
        &[(path, b"top\nanchor\nright\n", "100644")],
        &[root],
        2,
        b"right\n",
        None,
    );
    let merge = repo.commit(
        &[(path, b"left\nanchor\nright\n", "100644")],
        &[left, right],
        4,
        b"merge\n",
        None,
    );
    let base = BlameRequest {
        commit: &root,
        path,
    };
    let target = BlameRequest {
        commit: &merge,
        path,
    };
    let ns = [23; 32];
    let options = BlameOptions::default();
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let control = control();
    let session = repo.session(budget.clone(), control.clone());
    let local = session.blame_complete(base, &options).unwrap();
    let wire = session
        .encode_completed_blame(base, &options, ns, &local, MAX_COMPLETED_MAP_BYTES)
        .unwrap();
    let map = session
        .decode_authenticated_completed_blame(base, target, &options, ns, &wire)
        .unwrap()
        .unwrap();
    let result = session
        .blame_complete_with_completed(target, &options, ns, Some(&map), None)
        .unwrap();
    assert_eq!(result.completed_use(), CompletedUse::Miss);
    let output = Porcelain::encode_complete(&result, &control, &budget, MAX_BODY_BYTES).unwrap();
    assert_eq!(
        output.chunks().flatten().copied().collect::<Vec<_>>(),
        repo.native(merge, path)
    );
    drop(map);
    drop((wire, local, result, output));
    assert_eq!(budget.used(), 0);
}
