use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

use gix_imara_diff::{
    Algorithm, Diff, Hunk,
    byte_input::ByteInput,
    control::{BYTE_CHUNK, Cancelled, Stage},
};

#[test]
fn histogram_bounds_adversarial_recursive_search_without_partial_success() {
    use gix_imara_diff::{Token, control::MAX_SEARCH_DEPTH};

    // Successively longer common blocks, separated by unequal lines, make the
    // histogram choose the last block and recursively search the entire prefix.
    let (mut before, mut after) = (Vec::new(), Vec::new());
    let mut next = 0;
    for length in 1..=MAX_SEARCH_DEPTH + 3 {
        for _ in 0..length {
            before.push(Token(next));
            after.push(Token(next));
            next += 1;
        }
        before.push(Token(next));
        after.push(Token(next + 1));
        next += 2;
    }
    let result = Diff::try_compute_with(Algorithm::Histogram, &before, &after, next, &mut |_| Ok(()));
    assert!(
        matches!(result, Err(Cancelled)),
        "search depth must fail instead of returning a partial diff"
    );
}

#[test]
fn byte_index_growth_is_fallible_and_cancelled_rehash_preserves_published_tokens() {
    use gix_imara_diff::{Interner, Token};
    let mut interner = Interner::<Vec<u8>>::new(0);
    for id in 0..32 {
        assert_eq!(
            interner
                .try_intern_bytes(format!("line-{id}\n").as_bytes(), &mut |_| Ok(()))
                .unwrap(),
            Token(id)
        );
    }
    let mut entered = false;
    let result = interner.try_intern_bytes(b"not-published\n", &mut |event| {
        if event.stage == Stage::Index {
            entered = true;
            Err(Cancelled)
        } else {
            Ok(())
        }
    });
    assert!(entered && matches!(result, Err(Cancelled)));
    assert_eq!(interner.num_tokens(), 32, "failed rehash cannot publish a token");
    for id in 0..4096 {
        let line = format!("line-{id}\n");
        assert_eq!(
            interner.try_intern_bytes(line.as_bytes(), &mut |_| Ok(())).unwrap(),
            Token(id)
        );
    }
    for id in (0..4096).rev() {
        let line = format!("line-{id}\n");
        assert_eq!(
            interner.try_intern_bytes(line.as_bytes(), &mut |_| Ok(())).unwrap(),
            Token(id)
        );
        assert_eq!(interner[Token(id)], line.as_bytes());
    }
    assert_eq!(interner.num_tokens(), 4096);
}

#[test]
fn stage_entry_is_not_a_substitute_for_inner_hash_equal_middle_or_output_checks() {
    use gix_imara_diff::Token;
    for stage in [Stage::Hash, Stage::Equal, Stage::MiddleSnake, Stage::Output] {
        let mut entered = false;
        let mut stopped_inside = false;
        let mut check = |event: gix_imara_diff::control::Checkpoint| {
            if event.stage == stage {
                if event.processed == 0 {
                    entered = true;
                } else if entered {
                    stopped_inside = true;
                    return Err(Cancelled);
                }
            }
            Ok(())
        };
        let result = match stage {
            Stage::Hash | Stage::Equal => {
                let data = vec![b'x'; BYTE_CHUNK * 4];
                ByteInput::try_new(&data, &data, &mut check).map(|_| ())
            }
            Stage::MiddleSnake => {
                let mut before = vec![Token(1)];
                before.extend(std::iter::repeat_n(Token(0), 8192));
                before.push(Token(2));
                let mut after = vec![Token(2)];
                after.extend(std::iter::repeat_n(Token(0), 8192));
                after.push(Token(1));
                Diff::try_compute_with(Algorithm::MyersMinimal, &before, &after, 3, &mut check).map(|_| ())
            }
            Stage::Output => {
                let same = vec![Token(0); 8192];
                let diff = Diff::try_compute_with(Algorithm::MyersMinimal, &same, &same, 1, &mut |_| Ok(())).unwrap();
                diff.try_hunks(&mut check).map(|_| ())
            }
            _ => unreachable!(),
        };
        assert!(entered, "must enter real {stage:?} work before requesting stop");
        assert!(stopped_inside, "outer-only {stage:?} check acknowledged no inner stop");
        assert!(
            matches!(result, Err(Cancelled)),
            "must return no successful {stage:?} outcome"
        );
    }
}

#[test]
fn controlled_sort_preserves_stable_ties_and_stops_before_mutating_on_compare_failure() {
    use gix_imara_diff::control::{try_cmp_bytes, try_sort_by};
    let mut items = [(2, "first"), (1, "middle"), (2, "last"), (0, "zero")];
    try_sort_by(&mut items, |a, b, _| Ok(a.0.cmp(&b.0)), &mut |_| Ok(())).unwrap();
    assert_eq!(items, [(0, "zero"), (1, "middle"), (2, "first"), (2, "last")]);

    let first = vec![b'x'; BYTE_CHUNK * 3];
    let mut second = first.clone();
    second[BYTE_CHUNK * 2] = b'y';
    let mut bytes = [second.as_slice(), first.as_slice()];
    let original = bytes;
    let mut reached = false;
    let result = try_sort_by(&mut bytes, |a, b, check| try_cmp_bytes(a, b, check), &mut |event| {
        if event.stage == Stage::Equal && event.processed == BYTE_CHUNK {
            reached = true;
            Err(Cancelled)
        } else {
            Ok(())
        }
    });
    assert_eq!(result, Err(Cancelled));
    assert!(reached, "must stop inside a real long-key comparison");
    assert_eq!(bytes, original, "comparison failure must precede permutation");
}

#[test]
fn controlled_capacity_denial_precedes_growth_and_cleanup_ignores_repeated_stop() {
    use gix_imara_diff::{Interner, Token, control::try_reserve};
    let mut bytes = Vec::<u8>::new();
    let mut requested = None;
    assert_eq!(
        try_reserve(&mut bytes, BYTE_CHUNK, &mut |event| {
            assert_eq!(event.stage, Stage::Allocate);
            requested = Some(event.processed);
            Err(Cancelled)
        }),
        Err(Cancelled)
    );
    assert_eq!(requested, Some(BYTE_CHUNK));
    assert_eq!(bytes.capacity(), 0, "denied allocation must not grow capacity");

    let mut interner = Interner::<Vec<u8>>::new(0);
    interner.try_intern_bytes(b"first", &mut |_| Ok(())).unwrap();
    interner.try_intern_bytes(b"second", &mut |_| Ok(())).unwrap();
    let mut cleanup = Vec::new();
    interner.clear_with_control(&mut |event| {
        assert_eq!(event.stage, Stage::Cleanup);
        cleanup.push(event.processed);
        Err(Cancelled)
    });
    assert_eq!(interner.num_tokens(), 0, "stop must not skip owned-token cleanup");
    assert_eq!(cleanup.last(), Some(&0));
    assert_eq!(interner.try_intern_bytes(b"fresh", &mut |_| Ok(())).unwrap(), Token(0));
}

#[test]
fn controlled_byte_lines_preserve_exact_boundaries_and_edits() {
    let input = ByteInput::try_new(b"a\r\n\xff\nlast", b"a\r\nnew\nlast", &mut |_| Ok(())).unwrap();
    let input = input.as_input();
    assert_eq!(
        input
            .before
            .iter()
            .map(|&t| input.interner[t].as_slice())
            .collect::<Vec<_>>(),
        vec![b"a\r\n".as_slice(), b"\xff\n", b"last"]
    );
    assert_eq!(input.before[0], input.after[0]);
    assert_eq!(input.before[2], input.after[2]);
    for algorithm in [Algorithm::Histogram, Algorithm::Myers, Algorithm::MyersMinimal] {
        let diff = Diff::try_compute_with(
            algorithm,
            &input.before,
            &input.after,
            input.interner.num_tokens(),
            &mut |_| Ok(()),
        )
        .unwrap()
        .try_postprocess_lines(input, &mut |_| Ok(()))
        .unwrap();
        assert_eq!(
            diff.try_hunks(&mut |_| Ok(())).unwrap(),
            vec![Hunk {
                before: 1..2,
                after: 1..2
            }]
        );
    }
}

#[test]
fn huge_single_line_cancels_inside_each_byte_operation_without_affecting_another_job() {
    for stage in [Stage::Tokenize, Stage::Hash, Stage::Equal, Stage::Copy] {
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = cancelled.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let data = vec![b'x'; 4 * 1024 * 1024];
            let mut gated = false;
            let result = ByteInput::try_new(&data, &data, &mut |event| {
                if !gated && event.stage == stage && event.processed >= BYTE_CHUNK {
                    gated = true;
                    started_tx.send(event).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                if flag.load(Ordering::Relaxed) {
                    Err(Cancelled)
                } else {
                    Ok(())
                }
            });
            done_tx.send(result.err()).unwrap();
        });
        // This event comes from actual inner byte work, not task admission.
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("inner byte checkpoint was not reached");
        let healthy = ByteInput::try_new(b"old\n", b"new\n", &mut |_| Ok(())).unwrap();
        let healthy = healthy.as_input();
        let diff = Diff::try_compute_with(
            Algorithm::Myers,
            &healthy.before,
            &healthy.after,
            healthy.interner.num_tokens(),
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            diff.try_hunks(&mut |_| Ok(())).unwrap(),
            vec![Hunk {
                before: 0..1,
                after: 0..1
            }]
        );
        cancelled.store(true, Ordering::Relaxed);
        resume_tx.send(()).unwrap();
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("cancelled byte worker did not acknowledge stop"),
            Some(Cancelled)
        );
        worker.join().unwrap();
    }
}

#[test]
fn controlled_common_scan_preprocess_myers_postprocess_and_output_are_fallible() {
    use gix_imara_diff::Token;
    let same = vec![Token(0); 8192];
    let mut seen = false;
    let result = Diff::try_compute_with(Algorithm::MyersMinimal, &same, &same, 1, &mut |event| {
        if event.stage == Stage::Common && event.processed == 1024 {
            seen = true;
            Err(Cancelled)
        } else {
            Ok(())
        }
    });
    assert!(seen && matches!(result, Err(Cancelled)));

    let before: Vec<_> = (0..256).map(Token).collect();
    let after: Vec<_> = (0..256).rev().map(Token).collect();
    for algorithm in [Algorithm::Myers, Algorithm::MyersMinimal, Algorithm::Histogram] {
        for stage in [Stage::Preprocess, Stage::Output] {
            let mut seen = false;
            let result = Diff::try_compute_with(algorithm, &before, &after, 256, &mut |event| {
                if event.stage == stage && event.processed != 0 {
                    seen = true;
                    Err(Cancelled)
                } else {
                    Ok(())
                }
            });
            assert!(
                seen && matches!(result, Err(Cancelled)),
                "missing {algorithm:?}/{stage:?} inner check"
            );
        }
    }
    for algorithm in [Algorithm::Myers, Algorithm::MyersMinimal] {
        let mut visits = 0;
        let result = Diff::try_compute_with(algorithm, &before, &after, 256, &mut |event| {
            if event.stage == Stage::Myers {
                visits += 1;
            }
            // Past allocation and setup: this must reach the search frontiers.
            if visits == 32 { Err(Cancelled) } else { Ok(()) }
        });
        assert_eq!(visits, 32);
        assert!(matches!(result, Err(Cancelled)));
    }
    let diff = Diff::try_compute_with(Algorithm::Myers, &same, &same, 1, &mut |_| Ok(())).unwrap();
    assert_eq!(
        diff.try_hunks(&mut |event| if event.stage == Stage::Output {
            Err(Cancelled)
        } else {
            Ok(())
        }),
        Err(Cancelled)
    );
    let diff = Diff::try_compute_with(Algorithm::Myers, &same, &same, 1, &mut |_| Ok(())).unwrap();
    assert!(matches!(
        diff.try_postprocess_with(&same, &same, gix_imara_diff::NoSliderHeuristic, &mut |event| {
            if event.stage == Stage::Postprocess && event.processed >= 1024 {
                Err(Cancelled)
            } else {
                Ok(())
            }
        }),
        Err(Cancelled)
    ));
}

#[test]
fn controlled_ascii_indent_checks_inside_a_single_cr_only_line() {
    let line = vec![b'\r'; 2 * BYTE_CHUNK];
    assert_eq!(
        gix_imara_diff::IndentLevel::try_for_ascii_line(&line, 8, &mut |event| {
            assert_eq!(event.stage, Stage::Postprocess);
            assert_eq!(event.processed, BYTE_CHUNK);
            Err(Cancelled)
        }),
        Err(Cancelled)
    );
}

#[test]
fn cancellation_reaches_a_long_middle_snake_not_only_the_outer_search() {
    use gix_imara_diff::Token;
    let mut before = vec![Token(1)];
    before.extend(std::iter::repeat_n(Token(0), 8192));
    before.push(Token(2));
    let mut after = vec![Token(2)];
    after.extend(std::iter::repeat_n(Token(0), 8192));
    after.push(Token(1));
    for algorithm in [Algorithm::Myers, Algorithm::MyersMinimal] {
        let mut reached = false;
        let result = Diff::try_compute_with(algorithm, &before, &after, 3, &mut |event| {
            if event.stage == Stage::MiddleSnake && event.processed >= 1024 {
                reached = true;
                Err(Cancelled)
            } else {
                Ok(())
            }
        });
        assert!(reached, "the fixture must extend a long inner {algorithm:?} snake");
        assert!(matches!(result, Err(Cancelled)));
    }
}

#[test]
fn controlled_and_generic_interner_calls_share_one_identity_namespace() {
    let mut interner = gix_imara_diff::Interner::<Vec<u8>>::default();
    let first = interner.intern(b"first\n".to_vec());
    let second = interner.try_intern_bytes(b"second\n", &mut |_| Ok(())).unwrap();
    assert_eq!(interner.try_intern_bytes(b"first\n", &mut |_| Ok(())).unwrap(), first);
    assert_eq!(interner.intern_ref(b"second\n".as_slice()), second);
    interner.erase_tokens_after(second);
    assert_eq!(interner.try_intern_bytes(b"first\n", &mut |_| Ok(())).unwrap(), first);
    assert_eq!(interner.try_intern_bytes(b"second\n", &mut |_| Ok(())).unwrap(), second);
}
