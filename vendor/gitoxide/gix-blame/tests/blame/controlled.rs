use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::Duration;

use gix_diff::blob::control::{BYTE_CHUNK, Cancelled, Stage};
use gix_object::bstr::BStr;

use super::{Baseline, Fixture};

#[test]
fn admitted_blame_pins_capacity_until_output_drop_and_handles_budget_failure() -> gix_testtools::TestResult {
    use gix_diff::blob::control::{Allocation, Allocator, Cancelled, Stage};
    use std::sync::Mutex;
    struct Budget {
        used: Arc<Mutex<usize>>,
        limit: usize,
    }
    struct Lease {
        used: Arc<Mutex<usize>>,
        bytes: usize,
        limit: usize,
    }
    impl Allocation for Lease {
        fn resize(&mut self, bytes: usize) -> Result<(), Cancelled> {
            let mut used = self.used.lock().unwrap();
            let next = (*used - self.bytes)
                .checked_add(bytes)
                .filter(|&n| n <= self.limit)
                .ok_or(Cancelled)?;
            *used = next;
            self.bytes = bytes;
            Ok(())
        }
    }
    impl Drop for Lease {
        fn drop(&mut self) {
            *self.used.lock().unwrap() -= self.bytes;
        }
    }
    impl Allocator for Budget {
        fn reserve(&self, bytes: usize) -> Result<Box<dyn Allocation>, Cancelled> {
            let mut lease = Lease {
                used: self.used.clone(),
                bytes: 0,
                limit: self.limit,
            };
            lease.resize(bytes)?;
            Ok(Box::new(lease))
        }
    }
    struct NoGrowth<T>(T);
    impl<T: gix_object::Find + gix_object::FindHeader> gix_object::Find for NoGrowth<T> {
        fn try_find<'a>(
            &self,
            id: &gix_hash::oid,
            buffer: &'a mut Vec<u8>,
        ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
            if self
                .0
                .try_header(id)?
                .is_some_and(|h| h.size > buffer.capacity() as u64)
            {
                return Err(gix_error::message("provider refuses unadmitted destination growth").into());
            }
            self.0.try_find(id, buffer)
        }
    }
    impl<T: gix_object::FindHeader> gix_object::FindHeader for NoGrowth<T> {
        fn try_header(&self, id: &gix_hash::oid) -> gix_error::Result<Option<gix_object::Header>> {
            self.0.try_header(id)
        }
    }
    for (limit, cancel, clear) in [
        (1, false, false),
        (16 * 1024 * 1024, false, false),
        (16 * 1024 * 1024, false, true),
        (16 * 1024 * 1024, true, false),
    ] {
        let Fixture {
            odb,
            mut resource_cache,
            suspect,
        } = Fixture::new()?;
        let used = Arc::new(Mutex::new(0));
        let allocator = Arc::new(Budget {
            used: used.clone(),
            limit,
        });
        let mut stopped = false;
        let mut cleaned_after_stop = false;
        let result = gix_blame::file_with_allocations(
            NoGrowth(odb),
            gix_blame::Start::Commit(suspect),
            None,
            &mut resource_cache,
            "simple.txt".into(),
            gix_blame::Options {
                diff_algorithm: gix_diff::blob::Algorithm::MyersMinimal,
                ..Default::default()
            },
            Default::default(),
            allocator,
            &mut |event| {
                if cancel && event.stage == Stage::Myers && event.processed != 0 {
                    stopped = true;
                }
                if stopped && event.stage == Stage::Cleanup {
                    if !cleaned_after_stop {
                        assert!(*used.lock().unwrap() > 0, "scratch cleanup must precede charge release");
                    }
                    cleaned_after_stop = true;
                }
                if stopped { Err(Cancelled) } else { Ok(()) }
            },
        );
        if cancel {
            assert!(stopped && result.is_err(), "must cancel inside actual diff work");
            assert!(
                cleaned_after_stop,
                "request-owned scratch must be cleaned before stop acknowledgement"
            );
        } else if limit == 1 {
            assert!(result.is_err(), "capacity denial must not produce a partial blame");
        } else {
            let mut result = result?;
            assert_eq!(result.outcome().entries.iter().map(|e| e.len.get()).sum::<u32>(), 4);
            assert!(
                *used.lock().unwrap() >= result.outcome().blob.capacity(),
                "live output must remain charged"
            );
            if clear {
                result.clear_with_control(&mut |_| Err(Cancelled));
                assert_eq!(result.outcome().blob.capacity(), 0, "clear must free backing storage");
                let remaining_charge = *used.lock().unwrap();
                assert_eq!(remaining_charge, 0, "clear must release charges despite repeated stop");
                result.clear_with_control(&mut |_| Err(Cancelled));
            }
            drop(result);
        }
        assert_eq!(
            *used.lock().unwrap(),
            0,
            "all charges must be released after their backing owners"
        );
    }
    Ok(())
}

#[test]
fn cancelling_a_huge_content_line_does_not_cancel_an_independent_blame() -> gix_testtools::TestResult {
    // Acquire public upstream fixtures before entering the cancellation gate.
    let mut healthy = Fixture::new()?;
    let flag = Arc::new(AtomicBool::new(false));
    let worker_flag = flag.clone();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let Fixture {
            odb,
            mut resource_cache,
            suspect,
        } = Fixture::new().unwrap();
        let mut gated = false;
        let result = gix_blame::file_with_control(
            &odb,
            gix_blame::Start::Contents {
                first_suspect: suspect,
                contents: vec![b'x'; 4 * 1024 * 1024].into(),
            },
            None,
            &mut resource_cache,
            BStr::new("simple.txt"),
            Default::default(),
            Default::default(),
            &mut |event| {
                if !gated && event.stage == Stage::Tokenize && event.processed >= BYTE_CHUNK {
                    gated = true;
                    started_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                if worker_flag.load(Ordering::Relaxed) {
                    Err(Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .err()
        .map(|err| err.to_string());
        done_tx.send(result).unwrap();
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("blame did not enter the single-line tokenizer");
    let result = healthy.blame_file(BStr::new("simple.txt"), Default::default());
    flag.store(true, Ordering::Relaxed);
    resume_tx.send(()).unwrap();
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("cancelled blame did not acknowledge stop"),
        Some("diff operation cancelled".to_owned())
    );
    worker.join().unwrap();
    assert_eq!(result?.entries.len(), 4);
    Ok(())
}

#[test]
fn request_local_tuning_keeps_general_attribution_for_all_combinations() -> gix_testtools::TestResult {
    let expected = Baseline::collect(super::fixture_path()?.join(".git/simple.baseline"), "simple.txt".into())?;
    for bits in 0..32 {
        let Fixture {
            odb,
            mut resource_cache,
            suspect,
        } = Fixture::new()?;
        let result = gix_blame::file_with_tuning(
            &odb,
            gix_blame::Start::Commit(suspect),
            None,
            &mut resource_cache,
            BStr::new("simple.txt"),
            Default::default(),
            gix_blame::Tuning {
                reuse_line_tokens: bits & 1 != 0,
                retain_current_input_tokens: bits & 2 != 0,
                reuse_unchanged_runs: bits & 4 != 0,
                bounded_resource_cache: bits & 8 != 0,
                index_changes: bits & 16 != 0,
            },
        )?;
        assert_eq!(result.entries, expected, "request-local tuning {bits:05b}");
    }
    Ok(())
}

#[test]
fn output_cancels_inside_one_long_line_without_partial_success() {
    let outcome = gix_blame::Outcome {
        entries: vec![gix_blame::BlameEntry::new(
            0..1,
            0..1,
            gix_hash::Kind::Sha1.null(),
            None,
        )],
        blob: vec![b'x'; 4 * BYTE_CHUNK],
        ..Default::default()
    };
    let result = outcome.try_entries_with_lines(&mut |event| {
        if event.stage == Stage::Output && event.processed >= BYTE_CHUNK {
            Err(Cancelled)
        } else {
            Ok(())
        }
    });
    assert_eq!(result, Err(Cancelled));
    let complete = outcome.try_entries_with_lines(&mut |_| Ok(())).unwrap();
    assert_eq!(complete.len(), 1);
    assert_eq!(complete[0].1[0].as_slice(), outcome.blob);
}

#[test]
fn opposing_tuning_values_are_isolated_during_concurrent_traversal() -> gix_testtools::TestResult {
    let expected = Baseline::collect(super::fixture_path()?.join(".git/simple.baseline"), "simple.txt".into())?;
    let (started_tx, started_rx) = mpsc::sync_channel(2);
    let mut workers = Vec::new();
    let mut resumes = Vec::new();
    for enabled in [false, true] {
        let started_tx = started_tx.clone();
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        resumes.push(resume_tx);
        workers.push(std::thread::spawn(move || {
            let Fixture {
                odb,
                mut resource_cache,
                suspect,
            } = Fixture::new().unwrap();
            let mut gated = false;
            gix_blame::file_with_control(
                &odb,
                gix_blame::Start::Commit(suspect),
                None,
                &mut resource_cache,
                BStr::new("simple.txt"),
                Default::default(),
                gix_blame::Tuning {
                    reuse_line_tokens: enabled,
                    retain_current_input_tokens: enabled,
                    reuse_unchanged_runs: enabled,
                    bounded_resource_cache: enabled,
                    index_changes: enabled,
                },
                &mut |event| {
                    // Both requests have already traversed actual history before
                    // either can continue. Their choices are immutable values.
                    if !gated && event.stage == Stage::Graph && event.processed >= 1 {
                        gated = true;
                        started_tx.send(enabled).unwrap();
                        resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    }
                    Ok(())
                },
            )
            .unwrap()
            .entries
        }));
    }
    let first = started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("first request did not traverse history");
    let second = started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("second request did not traverse history");
    assert_ne!(first, second);
    for resume in resumes {
        resume.send(()).unwrap();
    }
    for worker in workers {
        assert_eq!(worker.join().unwrap(), expected);
    }
    Ok(())
}
