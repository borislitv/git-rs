#[path = "support/blame_fixture.rs"]
mod fixture;

use std::sync::{Arc, Weak};

use sha2::{Digest, Sha256};

use fixture::{Fixture, Provider, control};
use git_rs_core::blame::memo::{
    MappingBoundary, MappingKey, MappingMemo, MappingOptions, MappingStatistics,
    MemoryMappingRequest, UNIT_BYTES,
};
use git_rs_core::{
    BlameError, BlameOptions, BlameRequest, BlameTuning, CapacityBudget, CapacityBuffer,
    CapacityError, Control, Porcelain, ProducerVersion, SnapshotSession, StopReason,
};

// One real changed edge: fixed-size workset metadata, admitted owned bytes, no
// test-side replacement algorithm or fabricated mapping. Only cold core output
// is trusted for this same source scope and reused on the warm call.
struct Memory {
    unit: Option<(MappingKey, CapacityBuffer)>,
    budget: CapacityBudget,
    control: Control,
    expected: MappingOptions,
    loads: usize,
    stores: usize,
    fail_store: bool,
    stop_load: bool,
}

impl MappingMemo for Memory {
    fn load(&mut self, key: &MappingKey, into: &mut [u8]) -> Result<Option<usize>, BlameError> {
        self.loads += 1;
        assert!(
            self.budget.used() >= UNIT_BYTES,
            "vendor slot must be charged to the original job before memory callbacks"
        );
        assert_eq!(key.producer, self.expected.producer);
        assert_eq!(key.options, self.expected.options);
        if self.stop_load {
            self.control.cancel();
            self.control.check()?;
        }
        let Some((saved, bytes)) = &self.unit else {
            return Ok(None);
        };
        if saved != key {
            return Ok(None);
        }
        into[..bytes.as_slice().len()].copy_from_slice(bytes.as_slice());
        Ok(Some(bytes.as_slice().len()))
    }

    fn completed(&mut self, key: &MappingKey, encoded: &[u8]) -> Result<(), BlameError> {
        self.stores += 1;
        if self.fail_store {
            // Reachable workset admission failure, not an arbitrary fake error.
            // Vendor scratch is still live, so a full-job extra reservation fails.
            let _denied = self.budget.buffer(64 * 1024 * 1024)?;
            panic!("memo copy unexpectedly escaped the shared job limit");
        }
        let mut bytes = self.budget.buffer(encoded.len())?;
        bytes.resize_controlled(encoded.len(), 0, &self.control)?;
        bytes.as_mut_slice().copy_from_slice(encoded);
        self.unit = Some((key.clone(), bytes));
        Ok(())
    }
}

struct Boundary {
    // Observe the session's actual provider owner, not the fixture's source map.
    source: Weak<ProviderObjects>,
    budget: CapacityBudget,
    control: Control,
    calls: usize,
    last: MappingStatistics,
    cancel: bool,
}

type ProviderObjects = std::collections::BTreeMap<gix_hash::ObjectId, (gix_object::Kind, Vec<u8>)>;

impl MappingBoundary for Boundary {
    fn completed(&mut self, statistics: MappingStatistics) -> Result<(), BlameError> {
        assert!(
            self.source.upgrade().is_some(),
            "source owner lost before boundary"
        );
        assert!(
            self.budget.used() >= UNIT_BYTES,
            "live vendor slot lost its original job admission before boundary"
        );
        self.calls += 1;
        self.last = statistics;
        if self.cancel {
            self.control.cancel();
        }
        Ok(())
    }
}

fn open(
    provider: Provider,
    budget: &CapacityBudget,
    control: &Control,
) -> SnapshotSession<Provider> {
    let identity = provider.identity.clone();
    SnapshotSession::open(
        provider,
        &identity,
        control.clone(),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap()
}

fn history() -> (Fixture, gix_hash::ObjectId) {
    let mut fixture = Fixture::new();
    let root = fixture.commit(
        &[(b"file", b"a\na\nanchor\r\nlast", "100644")],
        &[],
        2,
        b"Distinct duplicate origins\n",
        None,
    );
    let tip = fixture.commit(
        &[(b"file", b"a\nanchor\r\nlast", "100644")],
        &[root],
        1,
        b"Delete duplicate\n",
        None,
    );
    (fixture, tip)
}

#[test]
fn hook_free_statistics_forward_real_changed_edges_not_commit_count() {
    let mut fixture = Fixture::new();
    let root = fixture.commit(&[(b"file", b"a\n", "100644")], &[], 5, b"Root\n", None);
    let unchanged = fixture.commit(
        &[(b"file", b"a\n", "100644")],
        &[root],
        4,
        b"Identical tree\n",
        None,
    );
    let mut tip = unchanged;
    for contents in [b"a\nb\n".as_slice(), b"a\nb\nc\n", b"a\nb\nc\nd\n"] {
        tip = fixture.commit(
            &[(b"file", contents, "100644")],
            &[tip],
            3,
            b"Append one line\n",
            None,
        );
    }
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let session = fixture.session(budget.clone(), control());
    // Five commits, but only three changed parent/child raw pairs. Root and
    // identical-tree traversal must not manufacture mapping work.
    for (commit, expected) in [(root, 0), (unchanged, 0), (tip, 3)] {
        let blame = session
            .blame_complete(
                BlameRequest {
                    commit: &commit,
                    path: b"file",
                },
                &BlameOptions::default(),
            )
            .unwrap();
        let mapping = blame.statistics().mapping;
        assert_eq!(
            (mapping.edges_visited, mapping.computations),
            (expected, expected),
            "hook-free raw mapping work for {commit}"
        );
        assert_eq!(
            (mapping.hits, mapping.rejected, mapping.completed),
            (0, 0, 0)
        );
    }
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn hook_free_off_cold_and_warm_match_native_with_real_work_and_owned_capacity() {
    let (fixture, tip) = history();
    let expected = fixture.native(tip, b"file");
    // Complete all native fixture/oracle work before the first runtime proof.
    for tuning in [
        BlameTuning::default(),
        BlameTuning {
            reuse_line_tokens: true,
            retain_current_input_tokens: true,
            reuse_unchanged_runs: true,
            bounded_resource_cache: true,
            index_changes: true,
        },
    ] {
        let options = BlameOptions::native_root(tuning);
        assert_eq!(options.tuning(), tuning);
        let identities = options.mapping_options(true).unwrap();
        let producer: [u8; 32] =
            Sha256::digest(ProducerVersion::verified().unwrap().cache_id().as_bytes()).into();
        let canonical_options: [u8; 32] = Sha256::digest(options.canonical_bytes()).into();
        assert_eq!(identities.producer, producer);
        assert_eq!(identities.options, canonical_options);
        let budget = CapacityBudget::new(64 * 1024 * 1024);
        let control = control();
        let provider = fixture.provider();
        let source = Arc::downgrade(&provider.objects);
        let session = open(provider, &budget, &control);
        let mut memory = Memory {
            unit: None,
            budget: budget.clone(),
            control: control.clone(),
            expected: options.mapping_options(true).unwrap(),
            loads: 0,
            stores: 0,
            fail_store: false,
            stop_load: false,
        };
        let mut boundary = Boundary {
            source: source.clone(),
            budget: budget.clone(),
            control: control.clone(),
            calls: 0,
            last: MappingStatistics::default(),
            cancel: false,
        };
        let request = BlameRequest {
            commit: &tip,
            path: b"file",
        };
        let mut cold = None;
        for mode in 0..4 {
            let blame = if mode == 0 {
                session.blame_complete(request, &options)
            } else {
                session.blame_complete_with_memo(
                    request,
                    &options,
                    MemoryMappingRequest {
                        reuse: mode != 1,
                        memo: &mut memory,
                        boundary: Some(&mut boundary),
                    },
                )
            }
            .unwrap();
            let statistics = blame.statistics();
            assert_eq!(statistics.commits_traversed, 2);
            match mode {
                0 => assert_eq!(
                    statistics.mapping,
                    MappingStatistics {
                        edges_visited: 1,
                        computations: 1,
                        ..MappingStatistics::default()
                    }
                ),
                1 => {
                    assert_eq!(memory.loads, 0);
                    assert_eq!(memory.stores, 0);
                    assert_eq!(boundary.calls, 0);
                    assert_eq!(statistics.mapping.computations, 1);
                }
                2 => {
                    assert_eq!(statistics.mapping.hits, 0);
                    assert_eq!(statistics.mapping.computations, 1);
                    assert_eq!(statistics.mapping.completed, 1);
                    assert_eq!(boundary.calls, 1);
                    cold = Some(statistics.mapping);
                }
                3 => {
                    let cold = cold.unwrap();
                    assert_eq!(statistics.mapping.hits, 1);
                    assert_eq!(statistics.mapping.computations, 0);
                    assert_eq!(statistics.mapping.edges_visited, cold.edges_visited);
                    assert_eq!(boundary.calls, 2);
                    assert_eq!(boundary.last, statistics.mapping);
                }
                _ => unreachable!(),
            }
            let body = Porcelain::encode_complete(
                &blame,
                &control,
                &budget,
                git_rs_core::blame::MAX_BODY_BYTES,
            )
            .unwrap();
            assert_eq!(body.statistics(), statistics);
            assert_eq!(
                body.chunks().flatten().copied().collect::<Vec<_>>(),
                expected
            );
            drop(blame);
            assert!(budget.used() > 0, "sealed output must remain admitted");
            drop(body);
        }
        drop(session);
        assert!(
            source.upgrade().is_none(),
            "callbacks retained source unexpectedly"
        );
        assert!(
            budget.used() > 0,
            "memo bytes must stay charged until dropped"
        );
        // A trusted warm mapping cannot replace physical source availability.
        let mut missing = fixture.provider();
        Arc::make_mut(&mut missing.objects).retain(|_, (kind, bytes)| {
            *kind != gix_object::Kind::Blob || bytes != b"a\na\nanchor\r\nlast"
        });
        let missing_session = open(missing, &budget, &control);
        assert!(matches!(
            missing_session
                .blame_complete_with_memo(
                    request,
                    &options,
                    MemoryMappingRequest {
                        reuse: true,
                        memo: &mut memory,
                        boundary: None
                    },
                )
                .err(),
            Some(BlameError::Corrupt(_))
        ));
        drop(missing_session);
        drop(memory);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn qualified_default_isolates_conservative_mappings_under_the_verified_producer() {
    let (fixture, tip) = history();
    let expected = fixture.native(tip, b"file");
    let conservative = BlameOptions::native_root(BlameTuning::default());
    let qualified = BlameOptions::default();
    let old_scope = conservative.mapping_options(true).unwrap();
    let new_scope = qualified.mapping_options(true).unwrap();
    assert_ne!(old_scope.options, new_scope.options);
    let producer: [u8; 32] =
        Sha256::digest(ProducerVersion::verified().unwrap().cache_id().as_bytes()).into();
    assert_eq!(old_scope.producer, producer);
    assert_eq!(new_scope.producer, producer);
    let no_reuse = qualified.mapping_options(false).unwrap();
    assert!(!no_reuse.reuse);
    assert!(new_scope.reuse);
    assert_eq!(no_reuse.producer, new_scope.producer);
    assert_eq!(no_reuse.options, new_scope.options);
    eprintln!(
        "QUALIFIED_DEFAULT producer={} options={:02x?}",
        ProducerVersion::verified().unwrap().cache_id(),
        new_scope.options
    );
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = control();
    let session = open(fixture.provider(), &budget, &control);
    let mut memory = Memory {
        unit: None,
        budget: budget.clone(),
        control: control.clone(),
        expected: old_scope,
        loads: 0,
        stores: 0,
        fail_store: false,
        stop_load: false,
    };
    for (options, hits, computations) in [
        (&conservative, 0, 1),
        (&qualified, 0, 1),
        (&qualified, 1, 0),
    ] {
        memory.expected = options.mapping_options(true).unwrap();
        let blame = session
            .blame_complete_with_memo(
                BlameRequest {
                    commit: &tip,
                    path: b"file",
                },
                options,
                MemoryMappingRequest {
                    reuse: true,
                    memo: &mut memory,
                    boundary: None,
                },
            )
            .unwrap();
        assert_eq!(blame.statistics().mapping.hits, hits);
        assert_eq!(blame.statistics().mapping.computations, computations);
        let body = Porcelain::encode_complete(
            &blame,
            &control,
            &budget,
            git_rs_core::blame::MAX_BODY_BYTES,
        )
        .unwrap();
        assert_eq!(
            body.chunks().flatten().copied().collect::<Vec<_>>(),
            expected
        );
    }
    assert_eq!((memory.loads, memory.stores), (3, 2));
    drop(memory);
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn callback_admission_and_boundary_cancellation_preserve_typed_causes_and_cleanup() {
    let (fixture, tip) = history();
    for (fail_store, stop_load) in [(true, false), (false, false), (false, true)] {
        let budget = CapacityBudget::new(64 * 1024 * 1024);
        let control = control();
        let provider = fixture.provider();
        let source = Arc::downgrade(&provider.objects);
        let session = open(provider, &budget, &control);
        let options = BlameOptions::default();
        let mut memory = Memory {
            unit: None,
            budget: budget.clone(),
            control: control.clone(),
            expected: options.mapping_options(true).unwrap(),
            loads: 0,
            stores: 0,
            fail_store,
            stop_load,
        };
        let mut boundary = Boundary {
            source,
            budget: budget.clone(),
            control: control.clone(),
            calls: 0,
            last: MappingStatistics::default(),
            cancel: !fail_store,
        };
        let result = session.blame_complete_with_memo(
            BlameRequest {
                commit: &tip,
                path: b"file",
            },
            &options,
            MemoryMappingRequest {
                reuse: true,
                memo: &mut memory,
                boundary: Some(&mut boundary),
            },
        );
        assert_eq!(
            result.err(),
            Some(if fail_store {
                BlameError::Capacity(CapacityError::Exhausted)
            } else {
                BlameError::Stopped(StopReason::Cancelled)
            })
        );
        assert_eq!(memory.loads, 1, "must reach the actual mapping callback");
        assert_eq!(memory.stores, usize::from(!stop_load));
        assert_eq!(boundary.calls, usize::from(!fail_store && !stop_load));
        drop(memory);
        drop(session);
        assert_eq!(budget.used(), 0);
    }
}
