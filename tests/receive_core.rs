//! Application adapter contracts, not the Stage 8 native-client/SQL/S3 gate.
//! Include private adapters in this test-only crate to exercise the production
//! implementations with finite fixtures without exporting source/budget guards.
use git_rs::receive::wire;
use git_rs::{
    Result, http, invalid,
    protocol::{Oid, Update},
};
use git_rs::{
    cpu_jobs::{CpuIoJob, CpuJobs},
    process::Operation,
    receive_io::ReceiveInput,
};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
    receive::{CommandBatch, CommandLimits, IncomingObjects, ReceiveLimits},
};
use gix::odb::pack as vendor;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufReader, Cursor, Read, Seek, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    time::{Duration, Instant},
};

#[path = "../src/receive/budget.rs"]
mod budget;
#[path = "../src/receive/pack.rs"]
mod pack;
#[path = "../src/receive/transport.rs"]
mod transport;

fn vendor_check(control: &Control) -> gix::error::Result {
    control
        .check()
        .map_err(|error| gix::error::Error::from_boxed(Box::new(error)))
}
fn control() -> Control {
    Control::new(Instant::now() + Duration::from_secs(10))
}
fn limits() -> ReadLimits {
    ReadLimits {
        max_object_bytes: 1024 * 1024,
        max_decode_bytes: 4 * 1024 * 1024,
        max_graph_objects: 1024,
        max_graph_edges: 4096,
        ..ReadLimits::default()
    }
}
fn object_id(kind: gix::objs::Kind, bytes: &[u8]) -> gix::ObjectId {
    let mut hash = gix::hash::hasher(gix::hash::Kind::Sha1);
    hash.update(&gix::objs::encode::loose_header(kind, bytes.len() as u64));
    hash.update(bytes);
    hash.try_finalize().unwrap()
}
fn append(pack: &mut Vec<u8>, header: vendor::data::entry::Header, bytes: &[u8]) {
    header.write_to(bytes.len() as u64, &mut *pack).unwrap();
    let mut encoder =
        gix::zlib::stream::deflate::Write::new(Vec::new(), gix::zlib::Compression::DEFAULT);
    encoder.write_all(bytes).unwrap();
    encoder.flush().unwrap();
    pack.extend_from_slice(&encoder.into_inner());
}
fn seal(mut pack: Vec<u8>) -> Vec<u8> {
    let mut hash = gix::hash::hasher(gix::hash::Kind::Sha1);
    hash.update(&pack);
    pack.extend_from_slice(hash.try_finalize().unwrap().as_slice());
    pack
}
fn commands(rows: &[(gix::ObjectId, &[u8])], caps: &[u8], budget: &CapacityBudget) -> CommandBatch {
    let mut input = Vec::new();
    for (index, (id, name)) in rows.iter().enumerate() {
        let mut command = format!("{} {} ", "0".repeat(40), id).into_bytes();
        command.extend_from_slice(name);
        if index == 0 {
            command.push(0);
            command.extend_from_slice(caps);
        }
        gix_packetline::blocking_io::encode::data_to_write(&command, &mut input).unwrap();
    }
    gix_packetline::blocking_io::encode::flush_to_write(&mut input).unwrap();
    CommandBatch::read(
        &mut input.as_slice(),
        &control(),
        budget.clone(),
        CommandLimits::default(),
    )
    .unwrap()
}

#[derive(Default)]
struct Old {
    objects: BTreeMap<gix::ObjectId, (gix::objs::Kind, Vec<u8>)>,
}
fn identity() -> SnapshotIdentity {
    SnapshotIdentity {
        source: b"fixture".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix::hash::Kind::Sha1,
    }
}
struct Provider {
    old: Old,
    identity: SnapshotIdentity,
}
impl VerifiedObjectProvider for Provider {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &gix::hash::oid, _: &Control) -> Result<Option<gix::objs::Header>> {
        Ok(self
            .old
            .objects
            .get(&id.to_owned())
            .map(|(kind, bytes)| gix::objs::Header {
                kind: *kind,
                size: bytes.len() as u64,
            }))
    }
    fn object(
        &self,
        id: &gix::hash::oid,
        _: &Control,
        budget: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<Object>> {
        self.old
            .objects
            .get(&id.to_owned())
            .map(|(kind, bytes)| {
                let mut data = budget.buffer(bytes.len())?;
                data.resize(bytes.len(), 0)?;
                data.as_mut_slice().copy_from_slice(bytes);
                Ok(Object { kind: *kind, data })
            })
            .transpose()
    }
}
fn old_session(old: Old, budget: &CapacityBudget, control: &Control) -> SnapshotSession<Provider> {
    SnapshotSession::open(
        Provider {
            old,
            identity: identity(),
        },
        &identity(),
        control.clone(),
        budget.clone(),
        limits(),
    )
    .unwrap()
}
struct NoRefs;
impl git_rs_core::receive::RefSnapshot for NoRefs {
    fn ref_oid(&self, _: &[u8]) -> Option<gix::ObjectId> {
        None
    }
}

#[test]
fn admitted_normalization_enumerates_physical_duplicates_and_keeps_its_charges() {
    let capacity = CapacityBudget::new(32 * 1024 * 1024);
    let baseline = capacity.used();
    let mut bytes = b"PACK\0\0\0\x02\0\0\0\x02".to_vec();
    append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
    append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
    let bytes = seal(bytes);
    {
        let budget = budget::Budget::new(capacity.clone()).unwrap();
        let prepared = vendor::Bundle::prepare_stream_admitted(
            &mut bytes.as_slice(),
            gix::hash::Kind::Sha1,
            pack::options(limits()),
            &budget,
            gix::zlib::Workspace::ZLIB_RS_0_6_8,
            &|_, _| Ok(None),
            &|| Ok(()),
        )
        .unwrap();
        assert!(capacity.used() >= prepared.pack.capacity() + prepared.index.capacity());
        let normalized = pack::Normalized::open(&prepared).unwrap();
        let mut entries = pack::Entries::new(Some(&normalized));
        assert_eq!(entries.object_count(), 2);
        for _ in 0..2 {
            let (id, object) = entries
                .next_object(&control(), &capacity, limits())
                .unwrap()
                .unwrap();
            assert_eq!(id.to_string(), "f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f");
            assert_eq!(object.data.as_slice(), b"abc");
        }
        assert!(
            entries
                .next_object(&control(), &capacity, limits())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        capacity.used(),
        baseline,
        "all structural and adapter owners released"
    );
}

#[test]
fn normalized_thin_forward_and_ofs_entries_have_exact_payloads() {
    let abc = object_id(gix::objs::Kind::Blob, b"abc");
    let abcd = object_id(gix::objs::Kind::Blob, b"abcd");
    for variant in ["thin", "forward", "ofs"] {
        let capacity = CapacityBudget::new(32 * 1024 * 1024);
        let budget = budget::Budget::new(capacity.clone()).unwrap();
        let bytes = delta_fixture(variant);
        let prepared = vendor::Bundle::prepare_stream_admitted(
            &mut bytes.as_slice(),
            gix::hash::Kind::Sha1,
            pack::options(limits()),
            &budget,
            gix::zlib::Workspace::ZLIB_RS_0_6_8,
            &|id, out| {
                if variant == "thin" && id == abc.as_ref() {
                    out[..3].copy_from_slice(b"abc");
                    Ok(Some((gix::objs::Kind::Blob, 3)))
                } else {
                    Ok(None)
                }
            },
            &|| Ok(()),
        )
        .unwrap();
        let normalized = pack::Normalized::open(&prepared).unwrap();
        let mut entries = pack::Entries::new(Some(&normalized));
        let mut actual = BTreeMap::new();
        while let Some((id, object)) = entries
            .next_object(&control(), &capacity, limits())
            .unwrap()
        {
            actual.insert(id, object.data.as_slice().to_vec());
        }
        assert_eq!(actual.get(&abcd).unwrap(), b"abcd", "{variant}");
        assert_eq!(
            actual.len(),
            if variant == "thin" { 1 } else { 2 },
            "{variant}"
        );
        if variant != "thin" {
            assert_eq!(actual.get(&abc).unwrap(), b"abc");
        }
    }
}

fn delta_fixture(variant: &str) -> Vec<u8> {
    let mut bytes = b"PACK\0\0\0\x02".to_vec();
    bytes.extend_from_slice(&(if variant == "thin" { 1u32 } else { 2 }).to_be_bytes());
    if variant == "ofs" {
        append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
        let base_distance = bytes.len() as u64 - 12;
        append(
            &mut bytes,
            vendor::data::entry::Header::OfsDelta { base_distance },
            b"\x03\x04\x90\x03\x01d",
        );
    } else {
        append(
            &mut bytes,
            vendor::data::entry::Header::RefDelta {
                base_id: object_id(gix::objs::Kind::Blob, b"abc"),
            },
            b"\x03\x04\x90\x03\x01d",
        );
        if variant == "forward" {
            append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
        }
    }
    seal(bytes)
}

#[test]
#[ignore = "requires the pinned native Git image and verified 220-file closure"]
fn native_strict_indexes_match_normalized_thin_forward_and_ofs_manifests() {
    fn git(directory: &std::path::Path, arguments: &[&str]) -> Vec<u8> {
        let output = std::process::Command::new("/usr/bin/git")
            .arg("-C")
            .arg(directory)
            .args(arguments)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", directory)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
    let abc = object_id(gix::objs::Kind::Blob, b"abc");
    let abcd = object_id(gix::objs::Kind::Blob, b"abcd");
    for variant in ["thin", "forward", "ofs"] {
        let files = Files::new();
        git(&files.root, &["init", "--bare", "--quiet"]);
        let capacity = CapacityBudget::new(32 * 1024 * 1024);
        let budget = budget::Budget::new(capacity).unwrap();
        let bytes = delta_fixture(variant);
        let prepared = vendor::Bundle::prepare_stream_admitted(
            &mut bytes.as_slice(),
            gix::hash::Kind::Sha1,
            pack::options(limits()),
            &budget,
            gix::zlib::Workspace::ZLIB_RS_0_6_8,
            &|id, out| {
                if variant == "thin" && id == abc.as_ref() {
                    out[..3].copy_from_slice(b"abc");
                    Ok(Some((gix::objs::Kind::Blob, 3)))
                } else {
                    Ok(None)
                }
            },
            &|| Ok(()),
        )
        .unwrap();
        files.file("normalized.pack", &prepared.pack);
        git(
            &files.root,
            &[
                "index-pack",
                "--strict",
                "--index-version=2",
                "-o",
                "normalized.idx",
                "normalized.pack",
            ],
        );
        assert_eq!(
            fs::read(files.root.join("normalized.idx")).unwrap(),
            &prepared.index[..],
            "{variant} index"
        );
        let manifest =
            String::from_utf8(git(&files.root, &["verify-pack", "-v", "normalized.idx"])).unwrap();
        let mut actual = BTreeMap::new();
        for line in manifest.lines() {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if fields.len() >= 3 && fields[0].len() == 40 {
                assert_eq!(fields[1], "blob");
                actual.insert(fields[0].to_owned(), fields[2].parse::<usize>().unwrap());
            }
        }
        let mut expected = BTreeMap::from([(abcd.to_string(), 4usize)]);
        if variant != "thin" {
            expected.insert(abc.to_string(), 3);
        }
        assert_eq!(actual, expected, "{variant} physical native manifest");
    }
}

#[test]
fn combined_provider_rejects_unreachable_semantics_and_the_second_missing_tip() {
    for malformed in [true, false] {
        let capacity = CapacityBudget::new(32 * 1024 * 1024);
        let budget = budget::Budget::new(capacity.clone()).unwrap();
        let check = control();
        let mut bytes = b"PACK\0\0\0\x02".to_vec();
        bytes.extend_from_slice(&(if malformed { 3u32 } else { 1 }).to_be_bytes());
        append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
        if malformed {
            // Its tree is physically present: rejecting this fixture must not
            // depend on an unrelated missing-tree edge hiding bad semantics.
            append(&mut bytes, vendor::data::entry::Header::Tree, b"");
            append(&mut bytes, vendor::data::entry::Header::Commit,
                b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor invalid\ncommitter A <a@example.invalid> 1 +0000\n\nmessage\n");
        }
        let bytes = seal(bytes);
        let prepared = vendor::Bundle::prepare_stream_admitted(
            &mut bytes.as_slice(),
            gix::hash::Kind::Sha1,
            pack::options(limits()),
            &budget,
            gix::zlib::Workspace::ZLIB_RS_0_6_8,
            &|_, _| Ok(None),
            &|| Ok(()),
        )
        .unwrap();
        let normalized = pack::Normalized::open(&prepared).unwrap();
        let old = old_session(Old::default(), &capacity, &check);
        let identity = identity();
        let combined = pack::Combined {
            old: &old,
            incoming: Some(&normalized),
            identity: &identity,
        };
        let session =
            SnapshotSession::open(combined, &identity, check, capacity.clone(), limits()).unwrap();
        let abc = object_id(gix::objs::Kind::Blob, b"abc");
        let missing = object_id(gix::objs::Kind::Blob, b"not physically present");
        let rows: Vec<(gix::ObjectId, &[u8])> = if malformed {
            vec![(abc, b"refs/tags/one")]
        } else {
            vec![(abc, b"refs/tags/one"), (missing, b"refs/tags/two")]
        };
        let batch = commands(&rows, b"report-status", &capacity); // deliberately no atomic
        let error = git_rs_core::receive::ReceiveSession::new(session, ReceiveLimits::default())
            .unwrap()
            .run(batch, &NoRefs, &mut pack::Entries::new(Some(&normalized)))
            .err()
            .expect("structural validity must not authorize either malformed batch");
        let message = error.to_string();
        if !malformed {
            assert!(message.contains("missing"), "{message}");
        }
    }
}

#[test]
fn combined_provider_accepts_the_complete_old_and_incoming_target_batch() {
    let capacity = CapacityBudget::new(32 * 1024 * 1024);
    let budget = budget::Budget::new(capacity.clone()).unwrap();
    let check = control();
    let mut bytes = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
    append(&mut bytes, vendor::data::entry::Header::Blob, b"abc");
    let bytes = seal(bytes);
    let prepared = vendor::Bundle::prepare_stream_admitted(
        &mut bytes.as_slice(),
        gix::hash::Kind::Sha1,
        pack::options(limits()),
        &budget,
        gix::zlib::Workspace::ZLIB_RS_0_6_8,
        &|_, _| Ok(None),
        &|| Ok(()),
    )
    .unwrap();
    let normalized = pack::Normalized::open(&prepared).unwrap();
    let stored = object_id(gix::objs::Kind::Blob, b"stored");
    let mut objects = BTreeMap::new();
    objects.insert(stored, (gix::objs::Kind::Blob, b"stored".to_vec()));
    let old = old_session(Old { objects }, &capacity, &check);
    let identity = identity();
    let combined = pack::Combined {
        old: &old,
        incoming: Some(&normalized),
        identity: &identity,
    };
    let session =
        SnapshotSession::open(combined, &identity, check, capacity.clone(), limits()).unwrap();
    let batch = commands(
        &[
            (stored, b"refs/tags/stored"),
            (
                object_id(gix::objs::Kind::Blob, b"abc"),
                b"refs/tags/incoming",
            ),
        ],
        b"report-status",
        &capacity,
    );
    let validated = git_rs_core::receive::ReceiveSession::new(session, ReceiveLimits::default())
        .unwrap()
        .run(batch, &NoRefs, &mut pack::Entries::new(Some(&normalized)))
        .unwrap();
    assert_eq!(validated.incoming_objects(), 1);
    assert_eq!(validated.fast_forward(), &[true, true]);
    assert_eq!(validated.commands().commands().len(), 2);
    assert_eq!(
        validated.commands().commands()[0].name(),
        b"refs/tags/stored"
    );
    assert_eq!(
        validated.commands().commands()[1].name(),
        b"refs/tags/incoming"
    );
}

#[test]
fn application_projection_retains_every_ordered_command_and_byte_until_drop() {
    let capacity = CapacityBudget::new(8 * 1024 * 1024);
    let target = object_id(gix::objs::Kind::Blob, b"abc");
    let batch = commands(
        &[(target, b"refs/tags/z"), (target, b"refs/tags/\xff")],
        b"report-status",
        &capacity,
    );
    let baseline = capacity.used();
    let projected =
        git_rs::receive::CommandProjection::from_batch(&batch, &capacity, &control()).unwrap();
    assert_eq!(
        projected.updates(),
        &[
            Update {
                old: Oid::ZERO,
                new: Oid::parse(b"f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f").unwrap(),
                name: b"refs/tags/z".to_vec()
            },
            Update {
                old: Oid::ZERO,
                new: Oid::parse(b"f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f").unwrap(),
                name: b"refs/tags/\xff".to_vec()
            },
        ]
    );
    assert!(capacity.used() > baseline);
    drop(projected);
    assert_eq!(capacity.used(), baseline);
}

#[test]
fn status_is_whole_batch_ordered_and_sideband_contains_the_nested_flush() {
    let capacity = CapacityBudget::new(8 * 1024 * 1024);
    let id = object_id(gix::objs::Kind::Blob, b"abc");
    let names: &[&[u8]] = &[b"refs/tags/z", b"refs/tags/\xff"];
    let updates: Vec<_> = names
        .iter()
        .map(|name| Update {
            old: Oid::ZERO,
            new: Oid::parse(id.to_string().as_bytes()).unwrap(),
            name: name.to_vec(),
        })
        .collect();
    for sideband in [false, true] {
        let batch = commands(
            &[(id, names[0]), (id, names[1])],
            if sideband {
                b"report-status side-band-64k"
            } else {
                b"report-status"
            },
            &capacity,
        );
        for success in [false, true] {
            let mut output = Vec::new();
            wire::report_status(&updates, batch.capabilities(), success, &mut output).unwrap();
            let nested = if sideband {
                let packets = decode_packets(&output);
                let mut nested = Vec::new();
                for packet in packets {
                    assert_eq!(packet[0], 1);
                    nested.extend_from_slice(&packet[1..]);
                }
                nested
            } else {
                output
            };
            let report = decode_packets(&nested);
            assert_eq!(report.len(), 3);
            if success {
                assert_eq!(
                    report,
                    vec![
                        b"unpack ok\n".to_vec(),
                        b"ok refs/tags/z\n".to_vec(),
                        b"ok refs/tags/\xff\n".to_vec()
                    ]
                );
            } else {
                assert_eq!(
                    report,
                    vec![
                        b"unpack receive rejected\n".to_vec(),
                        b"ng refs/tags/z receive rejected\n".to_vec(),
                        b"ng refs/tags/\xff receive rejected\n".to_vec()
                    ]
                );
            }
        }
    }
}

// Independent tiny test decoder: expected bytes are not computed by the writer
// under test, and a missing inner or outer terminal flush is an assertion error.
fn decode_packets(mut input: &[u8]) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    loop {
        assert!(input.len() >= 4, "missing packet header/flush");
        let length = usize::from_str_radix(std::str::from_utf8(&input[..4]).unwrap(), 16).unwrap();
        input = &input[4..];
        if length == 0 {
            assert!(input.is_empty());
            break;
        }
        assert!(length >= 4 && length - 4 <= input.len());
        packets.push(input[..length - 4].to_vec());
        input = &input[length - 4..];
    }
    packets
}

#[test]
fn receive_advertisement_has_only_the_implemented_capabilities_and_empty_sentinel() {
    let mut snapshot = git_rs::metadata::Snapshot {
        id: "fixture".into(),
        generation: 1,
        storage_epoch: 1,
        default_ref: b"refs/heads/main".to_vec(),
        refs: BTreeMap::new(),
        packs: Vec::new(),
    };
    let mut empty = Vec::new();
    wire::advertise(&snapshot, false, &mut empty).unwrap();
    let packets = decode_packets(&empty);
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0], b"0000000000000000000000000000000000000000 capabilities^{}\0report-status side-band-64k delete-refs ofs-delta atomic object-format=sha1 agent=git-rs\n");
    snapshot
        .refs
        .insert(b"refs/tags/one".to_vec(), Oid([0x11; 20]));
    let mut http = Vec::new();
    wire::advertise(&snapshot, true, &mut http).unwrap();
    assert_eq!(&http[..35], b"001f# service=git-receive-pack\n0000");
    let packets = decode_packets(&http[35..]);
    assert_eq!(packets.len(), 1);
    assert!(packets[0].starts_with(b"1111111111111111111111111111111111111111 refs/tags/one\0"));
    assert!(
        !packets[0]
            .windows(b"push-options".len())
            .any(|window| window == b"push-options")
    );
    assert!(
        !packets[0]
            .windows(b"report-status-v2".len())
            .any(|window| window == b"report-status-v2")
    );
}

struct Files {
    root: PathBuf,
}
impl Files {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "receive-adapter-test-{}",
            git_rs::scratch::identifier().unwrap()
        ));
        fs::create_dir(&root).unwrap();
        Self { root }
    }
    fn file(&self, name: &str, bytes: &[u8]) -> File {
        let mut file = transport::private_file(&self.root.join(name)).unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        file
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn gzip_finalization_checks_crc_truncation_tails_and_concatenated_members() {
    let files = Files::new();
    let valid = gzip(b"abc");
    let mut corrupt = valid.clone();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    let mut tail = valid.clone();
    tail.push(0xff);
    let mut multiple = valid.clone();
    multiple.extend_from_slice(&gzip(b"de"));
    let mut bad_size = valid.clone();
    let size = bad_size.len() - 4;
    bad_size[size] ^= 1;
    for (name, bytes, expected) in [
        ("valid", valid.clone(), Some(&b"abc"[..])),
        ("truncated", valid[..valid.len() - 1].to_vec(), None),
        ("crc", corrupt, None),
        ("isize", bad_size, None),
        ("tail", tail, None),
        ("members", multiple, Some(&b"abcde"[..])),
    ] {
        let mut input = files.file(&format!("input-{name}"), &bytes);
        let mut output = files.file(&format!("output-{name}"), b"");
        let capacity = CapacityBudget::new(8 * 1024 * 1024);
        let result = transport::decode_gzip(&mut input, &mut output, &control(), &capacity);
        if let Some(expected) = expected {
            result.unwrap();
            output.rewind().unwrap();
            let mut decoded = Vec::new();
            output.read_to_end(&mut decoded).unwrap();
            assert_eq!(decoded, expected, "{name}");
        } else {
            assert!(result.is_err(), "bad gzip case {name} accepted");
        }
        assert_eq!(capacity.used(), 0);
    }
}

#[test]
fn finite_http_spooling_requires_content_length_or_chunk_terminal_completion() {
    for (headers, body, valid) in [
        ("Content-Length: 3\r\n", "abc", true),
        ("Content-Length: 4\r\n", "abc", false),
        (
            "Transfer-Encoding: chunked\r\n",
            "3\r\nabc\r\n0\r\n\r\n",
            true,
        ),
        ("Transfer-Encoding: chunked\r\n", "3\r\nabc\r\n0\r\n", false),
        (
            "Transfer-Encoding: chunked\r\n",
            "3\r\nabcXX0\r\n\r\n",
            false,
        ),
    ] {
        let bytes = format!(
            "POST /fixture.git/git-receive-pack HTTP/1.1\r\nContent-Type: application/x-git-receive-pack-request\r\n{headers}\r\n{body}"
        );
        let mut input = BufReader::new(Cursor::new(bytes));
        let request = http::Request::parse(&mut input).unwrap();
        let mut output = Vec::new();
        let result = transport::copy_bounded(
            &mut request.body(&mut input),
            &mut output,
            http::MAX_BODY,
            &|| Ok(()),
        );
        assert_eq!(result.is_ok(), valid, "{headers:?} {body:?}");
        if valid {
            assert_eq!(output, b"abc");
        }
    }
    assert!(transport::Encoding::parse("gzip").is_ok());
    assert!(transport::Encoding::parse("br").is_err());
}

#[test]
fn cancellation_before_gzip_read_never_produces_completed_output() {
    let files = Files::new();
    let mut input = files.file("input", &gzip(b"abc"));
    let mut output = files.file("output", b"");
    let capacity = CapacityBudget::new(8 * 1024 * 1024);
    let check = control();
    check.cancel();
    assert!(transport::decode_gzip(&mut input, &mut output, &check, &capacity).is_err());
    assert_eq!(output.metadata().unwrap().len(), 0);
    assert_eq!(capacity.used(), 0);
}

/// Test-only assembly of the production boundary/structural/provider adapters.
/// It intentionally has no SQL/S3 seam: successful reports here certify this
/// finite fixture's validation, not an application publication or Stage 8 gate.
struct OwnedSshFixture {
    deleting: bool,
    entered: Option<std::sync::mpsc::Sender<()>>,
}
struct SshEvidence {
    report: Vec<u8>,
    payloads: Vec<Vec<u8>>,
    fast_forward: Vec<bool>,
}
struct FixtureRefs(bool);
impl git_rs_core::receive::RefSnapshot for FixtureRefs {
    fn ref_oid(&self, name: &[u8]) -> Option<gix::ObjectId> {
        (self.0 && name == b"refs/tags/one").then(|| object_id(gix::objs::Kind::Blob, b"abc"))
    }
}
impl CpuIoJob for OwnedSshFixture {
    type Output = SshEvidence;
    fn run(
        self,
        input: &mut ReceiveInput,
        control: &Control,
        capacity: &CapacityBudget,
    ) -> Result<SshEvidence> {
        let _reader_capacity = capacity.reservation(32 * 1024)?;
        let mut input = BufReader::with_capacity(32 * 1024, input);
        if let Some(entered) = self.entered {
            entered.send(())?;
        }
        let batch = CommandBatch::read(
            &mut input,
            control,
            capacity.clone(),
            CommandLimits::default(),
        )?;
        let caps = batch.capabilities();
        let updates: Vec<_> = batch
            .commands()
            .iter()
            .map(|command| Update {
                old: Oid(command.old().as_slice().try_into().unwrap()),
                new: Oid(command.new().as_slice().try_into().unwrap()),
                name: command.name().to_vec(),
            })
            .collect();
        let budget = budget::Budget::new(capacity.clone())?;
        let outcome = pack::prepare(
            &mut input,
            &batch,
            pack::Boundary::Ssh,
            limits(),
            &budget,
            &|_, _| Ok(None),
            &|| vendor_check(control),
        )?;
        let normalized = outcome.as_ref().map(pack::Normalized::open).transpose()?;
        let mut entries = pack::Entries::new(normalized.as_ref());
        let mut payloads = Vec::new();
        while let Some((_, object)) = entries.next_object(control, capacity, limits())? {
            payloads.push(object.data.as_slice().to_vec());
        }
        let old = old_session(Old::default(), capacity, control);
        let identity = identity();
        let combined = pack::Combined {
            old: &old,
            incoming: normalized.as_ref(),
            identity: &identity,
        };
        let session = SnapshotSession::open(
            combined,
            &identity,
            control.clone(),
            capacity.clone(),
            limits(),
        )?;
        let validated =
            git_rs_core::receive::ReceiveSession::new(session, ReceiveLimits::default())?.run(
                batch,
                &FixtureRefs(self.deleting),
                &mut pack::Entries::new(normalized.as_ref()),
            )?;
        let mut report = Vec::new();
        wire::report_status(&updates, caps, true, &mut report)?;
        Ok(SshEvidence {
            report,
            payloads,
            fast_forward: validated.fast_forward().to_vec(),
        })
    }
}

fn ssh_request(deleting: bool) -> Vec<u8> {
    let target = "f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f";
    let zero = "0000000000000000000000000000000000000000";
    let (old, new) = if deleting {
        (target, zero)
    } else {
        (zero, target)
    };
    let command = format!("{old} {new} refs/tags/one\0report-status");
    let mut request = Vec::new();
    gix_packetline::blocking_io::encode::data_to_write(command.as_bytes(), &mut request).unwrap();
    gix_packetline::blocking_io::encode::flush_to_write(&mut request).unwrap();
    if !deleting {
        let mut pack = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
        append(&mut pack, vendor::data::entry::Header::Blob, b"abc");
        request.extend_from_slice(&seal(pack));
    }
    request
}

#[test]
fn http_delete_boundary_rejects_garbage_and_objects_but_accepts_no_pack_or_empty_pack() {
    let empty_pack = seal(b"PACK\0\0\0\x02\0\0\0\0".to_vec());
    let mut nonempty = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
    append(&mut nonempty, vendor::data::entry::Header::Blob, b"abc");
    let nonempty = seal(nonempty);
    for (body, accepted) in [
        (&b""[..], true),
        (&empty_pack[..], true),
        (&b"garbage"[..], false),
        (&nonempty[..], false),
    ] {
        let capacity = CapacityBudget::new(32 * 1024 * 1024);
        let budget = budget::Budget::new(capacity.clone()).unwrap();
        let request = ssh_request(true);
        let batch = CommandBatch::read(
            &mut request.as_slice(),
            &control(),
            capacity.clone(),
            CommandLimits::default(),
        )
        .unwrap();
        let mut input = body;
        let result = pack::prepare(
            &mut input,
            &batch,
            pack::Boundary::Http,
            limits(),
            &budget,
            &|_, _| Ok(None),
            &|| Ok(()),
        );
        assert_eq!(result.is_ok(), accepted, "delete-only HTTP body {body:?}");
        if accepted && let Some(outcome) = result.unwrap() {
            assert_eq!(outcome.outcome.num_objects, 0);
        }
    }
}

#[test]
fn owned_ssh_single_pack_and_delete_flush_finish_while_client_input_remains_open() {
    for deleting in [false, true] {
        let cpu = CpuJobs::new(1, 0).unwrap();
        let operation = Operation::new(10);
        let admission = cpu.try_admit(operation).unwrap().unwrap();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        client
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        server
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let input =
            ReceiveInput::new(server.try_clone().unwrap().into(), admission.io_control()).unwrap();
        std::thread::scope(|scope| {
            let client = scope.spawn(move || {
                client.write_all(&ssh_request(deleting)).unwrap();
                // No shutdown(Write), drop, EOF, or extra byte. The client waits
                // for server status with its input half deliberately still open.
                let mut packets = Vec::new();
                while let Some(packet) = git_rs::protocol::read_packet(&mut client).unwrap() {
                    packets.push(packet);
                }
                assert_eq!(
                    packets,
                    vec![b"unpack ok\n".to_vec(), b"ok refs/tags/one\n".to_vec()]
                );
            });
            let output = admission
                .run_io(
                    input,
                    OwnedSshFixture {
                        deleting,
                        entered: None,
                    },
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                output.payloads,
                if deleting {
                    Vec::<Vec<u8>>::new()
                } else {
                    vec![b"abc".to_vec()]
                }
            );
            assert_eq!(output.fast_forward, vec![!deleting]);
            assert_eq!(
                cpu.admitted(),
                1,
                "completed evidence still owns its actual slot"
            );
            server.write_all(&output.report).unwrap();
            client.join().unwrap();
            drop(output);
        });
        assert_eq!(cpu.admitted(), 0);
    }
}

#[test]
fn owned_ssh_cancellation_without_client_eof_does_not_return_a_validation_result() {
    let cpu = CpuJobs::new(1, 0).unwrap();
    let operation = Operation::new(10);
    let admission = cpu.try_admit(operation.clone()).unwrap().unwrap();
    let (client, server) = UnixStream::pair().unwrap();
    let input = ReceiveInput::new(server.into(), admission.io_control()).unwrap();
    let (entered, started) = std::sync::mpsc::channel();
    let (completed, finish) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let _open_client = client;
            started.recv_timeout(Duration::from_secs(3)).unwrap();
            operation.cancel();
            finish.recv_timeout(Duration::from_secs(3)).unwrap();
        });
        let result = admission.run_io(
            input,
            OwnedSshFixture {
                deleting: false,
                entered: Some(entered),
            },
        );
        assert!(
            result.is_err(),
            "cancel must not become a completed/no-pack receive"
        );
        drop(result);
        completed.send(()).unwrap();
    });
    assert_eq!(
        cpu.admitted(),
        0,
        "stop acknowledgment and owned input cleanup completed"
    );
}

#[test]
fn inherited_stdin_context_cancels_without_closing_fd_zero_or_mutating_flags() {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "inherited_pipe_stdin_child",
            "--ignored",
            "--nocapture",
        ])
        .env("GIT_RS_RECEIVE_STDIN_CHILD", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // The test process's stdin is never replaced. Only this owned child receives
    // a pipe at fd 0; the byte independently proves it remains usable after Drop.
    child.stdin.take().unwrap().write_all(b"x").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stdin child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "entry point for the isolated inherited-stdin fixture"]
fn inherited_pipe_stdin_child() {
    use std::os::fd::AsFd;
    assert_eq!(std::env::var("GIT_RS_RECEIVE_STDIN_CHILD").unwrap(), "1");
    let protected = std::io::stdin();
    let flags = rustix::fs::fcntl_getfl(&protected).unwrap();
    let control = git_rs::receive_io::IoControl::new(Operation::new(3)).unwrap();
    let mut input = ReceiveInput::stdin(control.clone()).unwrap();
    control.cancel();
    assert_eq!(
        input.read_exact(&mut [0]).unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    drop(input);
    assert_eq!(rustix::fs::fcntl_getfl(&protected).unwrap(), flags);
    let mut byte = [0];
    assert_eq!(rustix::io::read(protected.as_fd(), &mut byte).unwrap(), 1);
    assert_eq!(byte, [b'x']);
}
