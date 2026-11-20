#[path = "support/blame_fixture.rs"]
mod fixture;

use fixture::{Fixture, control, render};
use git_rs_core::blame::{
    BlameError, CompletedUse, MAX_BODY_BYTES,
    segments::{Endpoint, LocalSegmentRun},
};
use git_rs_core::{BlameOptions, BlameRequest, CapacityBudget, Control, Porcelain, StopReason};
use gix_object::Kind;

#[test]
fn independent_segments_match_flat_and_native_porcelain() {
    let mut repo = Fixture::new();
    let path = b"raw-\xff";
    let history: &[&[u8]] = &[
        b"start\nrepeat\nrepeat\nanchor\nold-a\nold-b\nold-c\nend\n",
        b"start\nrepeat\nrepeat\nanchor\nnew-a\nnew-b\nnew-c\nend\n",
        b"inserted\nstart\nrepeat\nanchor\nnew-a\nintervening\nnew-b\nnew-c\nend\n",
        b"inserted\nstart\nrepeat\nanchor\nnew-a\nnew-b\nchanged\nend\ntail",
    ];
    let mut points: Vec<Endpoint> = Vec::new();
    for (index, bytes) in history.iter().enumerate() {
        let blob = repo.insert(Kind::Blob, bytes);
        let parents: Vec<_> = points
            .last()
            .map(|point| point.commit)
            .into_iter()
            .collect();
        let commit = repo.commit(
            &[(path, bytes, "100644")],
            &parents,
            (index + 1) as u32,
            b"edit\n",
            None,
        );
        points.push(Endpoint { commit, blob });
    }
    let tip = points[3].commit;
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = control();
    let options = BlameOptions::default();
    let expected = repo.native(tip, path);
    assert_eq!(render(&repo, tip, path, &options), expected);

    let request = BlameRequest { commit: &tip, path };
    let session = repo.session(budget.clone(), control.clone());
    let bad = Endpoint {
        commit: points[2].commit,
        blob: points[1].blob,
    };
    assert!(
        session
            .summarize_blame_segment(request, Some(bad), &options)
            .unwrap()
            .is_none()
    );
    let tiny_budget = CapacityBudget::new(1);
    let tiny = repo.session(tiny_budget.clone(), control.clone());
    assert!(matches!(
        tiny.summarize_blame_segment(request, Some(points[2]), &options),
        Err(BlameError::Capacity(_))
    ));
    drop(tiny);
    assert_eq!(tiny_budget.used(), 0);
    let [young, middle, tail] = std::thread::scope(|scope| {
        let spawn = |start: Endpoint, end| {
            let worker = repo.session(budget.clone(), control.clone());
            let options = &options;
            scope.spawn(move || {
                worker
                    .summarize_blame_segment(
                        BlameRequest {
                            commit: &start.commit,
                            path,
                        },
                        end,
                        options,
                    )
                    .unwrap()
                    .expect("eligible linear interval")
            })
        };
        let young = spawn(points[3], Some(points[2]));
        let middle = spawn(points[2], Some(points[1]));
        let tail = spawn(points[1], None);
        [
            young.join().unwrap(),
            middle.join().unwrap(),
            tail.join().unwrap(),
        ]
    });
    assert!(
        session
            .compose_blame_segments(&young, &tail, &options)
            .unwrap()
            .is_none()
    );
    let mut tuning = options.tuning();
    tuning.reuse_line_tokens = !tuning.reuse_line_tokens;
    assert!(
        session
            .compose_blame_segments(&young, &middle, &BlameOptions::native_root(tuning))
            .unwrap()
            .is_none()
    );
    let front = session
        .compose_blame_segments(&young, &middle, &options)
        .unwrap()
        .expect("first join");
    let complete = session
        .compose_blame_segments(&front, &tail, &options)
        .unwrap()
        .expect("second join");
    let result = session
        .finish_blame_segments(complete, request, &options)
        .unwrap()
        .expect("complete joined blame");
    assert_eq!(result.completed_use(), CompletedUse::NotRequested);
    let body = Porcelain::encode_complete(&result, &control, &budget, MAX_BODY_BYTES).unwrap();
    let actual = body.chunks().flatten().copied().collect::<Vec<_>>();
    // Clipping drops new-c, and deleting the intervening line must join two
    // fragments of the same older source group into one two-line group.
    let group = format!("{} 5 5 2\n", points[1].commit);
    assert!(
        expected
            .windows(group.len())
            .any(|window| window == group.as_bytes())
    );
    assert_eq!(
        String::from_utf8_lossy(&actual),
        String::from_utf8_lossy(&expected),
        "segment porcelain differs from native"
    );
    assert_eq!(actual, expected);
    drop((body, result, front, tail));
    let mut local = LocalSegmentRun::default();
    assert!(local.append_segment(&session, young, &options).unwrap());
    assert!(local.append_segment(&session, middle, &options).unwrap());
    drop(session);

    let renewed = Control::new(control.deadline());
    let session = repo.session(budget.clone(), renewed.clone());
    let tail_request = BlameRequest {
        commit: &points[1].commit,
        path,
    };
    let piece = session
        .summarize_blame_segment(tail_request, None, &options)
        .unwrap()
        .unwrap();
    assert!(local.append_segment(&session, piece, &options).unwrap());
    let result = local.finish(&session, request, &options).unwrap().unwrap();
    let body = Porcelain::encode_complete(&result, &renewed, &budget, MAX_BODY_BYTES).unwrap();
    assert!(body.chunks().flatten().eq(expected.iter()));
    drop((body, result));

    let foreign = repo.session(budget.clone(), control);
    let piece = foreign
        .summarize_blame_segment(request, None, &options)
        .unwrap()
        .unwrap();
    assert!(
        !LocalSegmentRun::default()
            .append_segment(&session, piece, &options)
            .unwrap()
    );
    drop((foreign, session));
    assert_eq!(budget.used(), 0);
}

#[test]
fn bounded_rename_declines_and_cancellation_is_an_error() {
    let mut repo = Fixture::new();
    let bytes = b"unmodified contents\n";
    let blob = repo.insert(Kind::Blob, bytes);
    let base = repo.commit(&[(b"old", bytes, "100644")], &[], 1, b"base\n", None);
    let tip = repo.commit(&[(b"new", bytes, "100644")], &[base], 2, b"rename\n", None);
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = control();
    let session = repo.session(budget.clone(), control.clone());
    let options = BlameOptions::default();
    let request = BlameRequest {
        commit: &tip,
        path: b"new",
    };
    let end = Some(Endpoint { commit: base, blob });
    assert!(
        session
            .summarize_blame_segment(request, end, &options)
            .unwrap()
            .is_none()
    );
    control.cancel();
    assert!(matches!(
        session.summarize_blame_segment(request, end, &options),
        Err(BlameError::Stopped(StopReason::Cancelled))
    ));
    drop(session);
    assert_eq!(budget.used(), 0);
}
