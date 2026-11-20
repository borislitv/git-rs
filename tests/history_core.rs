#![forbid(unsafe_code)]
//! Public physical-snapshot history, rewrite and bundle contracts. Native bundle
//! restore and SQL publication are separate actual-image integration boundaries.

#[path = "support/range_source.rs"]
mod ranges;

use git_rs::Result;
use git_rs_core::{CapacityBudget, Control, ReadLimits};
use ranges::Graph;
use std::{
    io::{self, Cursor, Write},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(10))
}
fn limits() -> ReadLimits {
    ReadLimits {
        max_graph_objects: 64,
        max_graph_edges: 128,
        max_decode_bytes: 4096,
        ..ReadLimits::default()
    }
}

#[test]
fn topology_keeps_all_merge_parents_despite_reversed_commit_clocks() -> Result<()> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"byte-\xff", blob)?;
    let root = graph.commit(tree, &[], 900)?;
    let left = graph.commit(tree, &[root], 1)?;
    let right = graph.commit(tree, &[root], 1000)?;
    let tip = graph.commit(tree, &[left, right], 2)?;
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let snapshot = graph.source()?.open(control(), budget.clone(), limits())?;
    let complete = snapshot.history(tip, 64)?;
    assert_eq!(
        complete.iter().collect::<Vec<_>>(),
        vec![tip, right, left, root]
    );
    assert_eq!(
        snapshot.history(tip, 2)?.iter().collect::<Vec<_>>(),
        vec![tip, right]
    );
    assert!(snapshot.history(tip, 0).is_err());
    assert!(
        snapshot.history(blob, 64).is_err(),
        "a physical blob is not a history root"
    );
    drop(snapshot);
    assert_eq!(complete.len(), 4);
    assert!(
        budget.used() > 0,
        "delivered history retains its backing charge"
    );
    drop(complete);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn rewrite_drops_cut_parents_and_signatures_but_preserves_other_raw_bytes() -> Result<()> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let root = graph.commit(tree, &[], 1)?;
    let preserved = b"author A <a@example.invalid> 2 +0000\ncommitter A <a@example.invalid> 2 +0000\nencoding ISO-8859-1\nx-extra kept\n continuation\n";
    let mut original = format!("tree {tree}\nparent {root}\n").into_bytes();
    original.extend_from_slice(preserved);
    original.extend_from_slice(b"gpgsig discarded\n signature continuation\ngpgsig-sha256 discarded\n signature continuation\nmergetag discarded\n tag continuation\n\nmessage-\xff\n");
    let tip = graph.object(gix::objs::Kind::Commit, &original)?;
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let snapshot = graph.source()?.open(control(), budget.clone(), limits())?;
    let name = b"refs/heads/byte-\xff".as_slice();
    let rewritten = snapshot.rewrite_history([(name, tip)], 1, b"fresh-operation")?;
    let refs = rewritten.refs().collect::<Vec<_>>();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0, name);
    let mut expected = format!("tree {tree}\n").into_bytes();
    expected.extend_from_slice(preserved);
    expected.extend_from_slice(b"\nmessage-\xff\n");
    assert_eq!(refs[0].1, ranges::oid(gix::objs::Kind::Commit, &expected)?);
    let object = rewritten
        .snapshot()
        .object(&refs[0].1)?
        .expect("rewritten physical commit");
    assert_eq!(object.data.as_slice(), expected);
    assert_eq!(rewritten.snapshot().history(refs[0].1, 64)?.len(), 1);
    let (_, contents) = rewritten
        .snapshot()
        .lookup_blob(&refs[0].1, b"file")?
        .expect("retained original tree/blob");
    assert_eq!(contents.data.as_slice(), b"contents\n");
    drop((object, contents, rewritten));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn standalone_bundle_has_exact_refs_complete_closure_and_corruption_checks() -> Result<()> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let tip = graph.commit(tree, &[], 1)?;
    graph.object(gix::objs::Kind::Blob, b"unpublished object\n")?;
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let snapshot = graph.source()?.open(control(), budget.clone(), limits())?;
    let mut bytes = Vec::new();
    let manifest = snapshot.export_bundle(
        [(b"refs/heads/main".as_slice(), tip)],
        b"refs/heads/main",
        &mut bytes,
    )?;
    let envelope = format!("# v2 git bundle\n{tip} refs/heads/main\n{tip} HEAD\n\n");
    assert!(bytes.starts_with(envelope.as_bytes()));
    assert_eq!(
        manifest.object_count, 3,
        "only the complete authorized closure belongs in the bundle"
    );
    assert_eq!(
        &bytes[envelope.len()..envelope.len() + 12],
        b"PACK\0\0\0\x02\0\0\0\x03"
    );
    manifest.verify(&mut Cursor::new(&bytes))?;
    for offset in [0, envelope.len() + 12, bytes.len() - 1] {
        let mut damaged = bytes.clone();
        damaged[offset] ^= 1;
        assert!(
            manifest.verify(&mut Cursor::new(damaged)).is_err(),
            "corruption at {offset}"
        );
    }
    assert!(
        manifest
            .verify(&mut Cursor::new(&bytes[..bytes.len() - 1]))
            .is_err()
    );
    bytes.push(0);
    assert!(manifest.verify(&mut Cursor::new(&bytes)).is_err());
    drop((manifest, snapshot));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn bundle_preserves_supported_large_ref_envelopes_and_rejects_overflow() -> Result<()> {
    for (count, long_names) in [(10_000, false), (10_000, true), (10_001, false)] {
        let mut graph = Graph::default();
        let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
        let tree = graph.tree(b"file", blob)?;
        let tip = graph.commit(tree, &[], 1)?;
        let refs: Vec<_> = (0..count)
            .map(|number| {
                let mut name = format!("refs/heads/b{number:05}").into_bytes();
                if long_names {
                    while name.len() < 1024 {
                        name.push(b'/');
                        name.extend(std::iter::repeat_n(b'n', (1024 - name.len()).min(200)));
                    }
                }
                name
            })
            .collect();
        let budget = CapacityBudget::new(64 * 1024 * 1024);
        let snapshot = graph
            .source()?
            .open(control(), budget.clone(), ReadLimits::default())?;
        let mut output = Vec::new();
        let result = snapshot.export_bundle(
            refs.iter().map(|name| (name.as_slice(), tip)),
            &refs[0],
            &mut output,
        );
        if count == 10_001 {
            assert!(
                result.is_err(),
                "the preserved catalog count limit remains bounded"
            );
            assert!(
                output.is_empty(),
                "invalid envelope must not publish partial output"
            );
        } else {
            let manifest = result?;
            manifest.verify(&mut Cursor::new(&output))?;
            let end = output
                .windows(2)
                .position(|bytes| bytes == b"\n\n")
                .unwrap();
            let lines: Vec<_> = output[..end].split(|byte| *byte == b'\n').collect();
            assert_eq!(lines.len(), count + 2, "signature, every ref and HEAD");
            let expected_last =
                [format!("{tip} ").as_bytes(), refs.last().unwrap(), b"\n"].concat();
            assert!(
                output
                    .windows(expected_last.len())
                    .any(|bytes| bytes == expected_last)
            );
            assert_eq!(manifest.object_count, 3);
        }
        drop(snapshot);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[test]
fn rewrite_keeps_every_supported_branch_past_the_former_shared_bound() -> Result<()> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let tip = graph.commit(tree, &[], 1)?;
    let refs: Vec<_> = (0..4_097)
        .map(|number| format!("refs/heads/b{number:05}").into_bytes())
        .collect();
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let snapshot = graph
        .source()?
        .open(control(), budget.clone(), ReadLimits::default())?;
    let rewritten = snapshot.rewrite_history(
        refs.iter().map(|name| (name.as_slice(), tip)),
        1,
        b"rewrite-many-refs",
    )?;
    let actual: Vec<_> = rewritten.refs().map(|(name, _)| name.to_vec()).collect();
    assert_eq!(actual, refs);
    drop(rewritten);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn cancelled_bundle_output_stops_before_pack_generation_and_releases_capacity() -> Result<()> {
    struct CancelAfterEnvelope {
        control: Control,
        bytes: Vec<u8>,
    }
    impl Write for CancelAfterEnvelope {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            self.control
                .cancellation_flag()
                .store(true, Ordering::Release);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let tip = graph.commit(tree, &[], 1)?;
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let control = control();
    let snapshot = graph
        .source()?
        .open(control.clone(), budget.clone(), limits())?;
    let mut output = CancelAfterEnvelope {
        control,
        bytes: Vec::new(),
    };
    assert!(
        snapshot
            .export_bundle(
                [(b"refs/heads/main".as_slice(), tip)],
                b"refs/heads/main",
                &mut output
            )
            .is_err()
    );
    assert_eq!(
        output.bytes,
        format!("# v2 git bundle\n{tip} refs/heads/main\n{tip} HEAD\n\n").as_bytes()
    );
    drop(snapshot);
    assert_eq!(budget.used(), 0);
    Ok(())
}
