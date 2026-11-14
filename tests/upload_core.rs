#![forbid(unsafe_code)]
//! Public server-wire contracts over physical immutable packs. External native
//! clients separately qualify generated pack contents and transport integration.

#[path = "support/range_source.rs"]
mod ranges;

use git_rs::Result;
use git_rs_core::{
    CapacityBudget, Control, ReadLimits,
    pack::ranges::RangePackSet,
    upload::{UploadSession, Version},
};
use ranges::{Graph, Source};
use std::{
    io::{self, Cursor, Read},
    time::{Duration, Instant},
};

type Session = UploadSession<RangePackSet<Source>>;

fn packet(bytes: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(format!("{:04x}", bytes.len() + 4).as_bytes());
    output.extend_from_slice(bytes);
}

fn fixture(version: Version) -> Result<(Session, gix::ObjectId, gix::ObjectId, CapacityBudget)> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let tip = graph.commit(tree, &[], 1)?;
    let hidden = graph.commit(tree, &[], 2)?;
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let snapshot = graph.source()?.open(
        control,
        budget.clone(),
        ReadLimits {
            max_graph_objects: 64,
            max_graph_edges: 128,
            max_decode_bytes: 4096,
            ..ReadLimits::default()
        },
    )?;
    let session = UploadSession::new(
        snapshot,
        [(b"refs/heads/main".as_slice(), tip)],
        b"refs/heads/main",
        version,
    )?;
    Ok((session, tip, hidden, budget))
}

fn v0(want: gix::ObjectId) -> Vec<u8> {
    let mut bytes = Vec::new();
    packet(format!("want {want}\n").as_bytes(), &mut bytes);
    bytes.extend_from_slice(b"0000");
    packet(b"done\n", &mut bytes);
    bytes
}

#[test]
fn upload_advertises_all_ten_thousand_supported_catalog_refs() -> Result<()> {
    let mut graph = Graph::default();
    let blob = graph.object(gix::objs::Kind::Blob, b"contents\n")?;
    let tree = graph.tree(b"file", blob)?;
    let tip = graph.commit(tree, &[], 1)?;
    let refs: Vec<_> = (0..10_000)
        .map(|number| format!("refs/heads/b{number:05}").into_bytes())
        .collect();
    let budget = CapacityBudget::new(64 * 1024 * 1024);
    let control = Control::new(Instant::now() + Duration::from_secs(10));
    let snapshot = graph
        .source()?
        .open(control, budget.clone(), ReadLimits::default())?;
    let session = UploadSession::new(
        snapshot,
        refs.iter().map(|name| (name.as_slice(), tip)),
        &refs[0],
        Version::V0,
    )?;
    let mut output = Vec::new();
    session.advertise(false, &mut output)?;
    let mut wire = output.as_slice();
    let mut advertised = Vec::new();
    loop {
        let Some(packet) = git_rs::protocol::read_packet(&mut wire)? else {
            break;
        };
        advertised.push(packet);
    }
    assert_eq!(
        advertised.len(),
        10_001,
        "all catalog refs plus HEAD are advertised"
    );
    assert_eq!(
        advertised.last().unwrap(),
        format!("{tip} refs/heads/b09999\n").as_bytes()
    );
    assert!(wire.is_empty());
    drop(session);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn physical_but_unadvertised_wants_fail_before_any_success_or_pack_bytes() -> Result<()> {
    for version in [Version::V0, Version::V1, Version::V2] {
        let (mut session, _, hidden, budget) = fixture(version)?;
        let mut bytes = Vec::new();
        if version == Version::V2 {
            packet(b"command=fetch\n", &mut bytes);
            bytes.extend_from_slice(b"0001");
            packet(format!("want {hidden}\n").as_bytes(), &mut bytes);
            packet(b"done\n", &mut bytes);
            bytes.extend_from_slice(b"0000");
        } else {
            bytes = v0(hidden);
        }
        let mut output = Vec::new();
        assert!(
            session
                .run(&mut bytes.as_slice(), &mut output, true)
                .is_err()
        );
        assert!(
            output.is_empty(),
            "{version:?} must authorize before success output"
        );
        drop(session);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[test]
fn ssh_done_terminates_at_the_protocol_boundary_without_waiting_for_stdin_eof() -> Result<()> {
    struct NoEof(Cursor<Vec<u8>>);
    impl Read for NoEof {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            assert!(
                (self.0.position() as usize) < self.0.get_ref().len(),
                "SSH attempted to drain client EOF"
            );
            self.0.read(bytes)
        }
    }
    let (mut session, tip, _, budget) = fixture(Version::V0)?;
    let mut input = NoEof(Cursor::new(v0(tip)));
    let mut output = Vec::new();
    session.run(&mut input, &mut output, false)?;
    assert_eq!(input.0.position() as usize, input.0.get_ref().len());
    assert!(output.starts_with(b"0008NAK\nPACK\0\0\0\x02\0\0\0\x03"));
    let pack = &output[b"0008NAK\n".len()..];
    assert_eq!(
        ranges::hash(&pack[..pack.len() - 20])?.as_slice(),
        &pack[pack.len() - 20..]
    );
    drop(session);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn v2_ls_refs_preserves_delimiters_symrefs_and_stateless_response_end() -> Result<()> {
    let (mut session, tip, _, _) = fixture(Version::V2)?;
    let mut request = Vec::new();
    packet(b"command=ls-refs\n", &mut request);
    request.extend_from_slice(b"0001");
    packet(b"symrefs\n", &mut request);
    packet(b"ref-prefix HEAD\n", &mut request);
    request.extend_from_slice(b"0000");
    let mut output = Vec::new();
    session.run(&mut request.as_slice(), &mut output, true)?;
    let mut expected = Vec::new();
    packet(
        format!("{tip} HEAD symref-target:refs/heads/main\n").as_bytes(),
        &mut expected,
    );
    expected.extend_from_slice(b"00000002");
    assert_eq!(output, expected);
    let delimiter = request
        .windows(4)
        .position(|bytes| bytes == b"0001")
        .expect("explicit delimiter");
    request[delimiter..delimiter + 4].copy_from_slice(b"0000");
    let mut rejected = Vec::new();
    assert!(
        session
            .run(&mut request.as_slice(), &mut rejected, true)
            .is_err()
    );
    assert!(rejected.is_empty());
    Ok(())
}

#[test]
fn stateless_fetch_rejects_trailing_body_before_ack_or_pack_publication() -> Result<()> {
    let (mut session, tip, _, _) = fixture(Version::V0)?;
    let mut request = v0(tip);
    request.push(b'x');
    let mut output = Vec::new();
    assert!(
        session
            .run(&mut request.as_slice(), &mut output, true)
            .is_err()
    );
    assert!(output.is_empty());
    Ok(())
}

#[test]
fn unsupported_versions_and_unadvertised_filter_capability_are_rejected() -> Result<()> {
    for version in ["version=3", "version=2\n", "version=1:version=2"] {
        assert!(Version::parse(version).is_err());
    }
    let (mut session, tip, _, _) = fixture(Version::V0)?;
    let mut request = Vec::new();
    packet(format!("want {tip} filter\n").as_bytes(), &mut request);
    request.extend_from_slice(b"0000");
    packet(b"done\n", &mut request);
    let mut output = Vec::new();
    assert!(
        session
            .run(&mut request.as_slice(), &mut output, true)
            .is_err()
    );
    assert!(output.is_empty());
    Ok(())
}
