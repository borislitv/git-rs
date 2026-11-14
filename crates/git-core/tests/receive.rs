//! Public semantic contracts. Structural ingest, transport EOF and publication
//! have separate joined application gates; none is mocked as a successful boolean.

use git_rs_core::objects::Result;
use git_rs_core::receive::{
    CommandBatch, CommandLimits, CommandSection, IncomingObjects, ReceiveLimits, ReceiveSession,
    RefSnapshot, WarningKind,
};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};
use std::collections::{BTreeMap, VecDeque};
use std::io::{Cursor, Read, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Physical {
    id: ObjectId,
    kind: Kind,
    bytes: Vec<u8>,
}

fn physical(kind: Kind, bytes: impl Into<Vec<u8>>) -> Physical {
    let bytes = bytes.into();
    Physical {
        id: gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap(),
        kind,
        bytes,
    }
}

fn commit(tree: ObjectId, parents: &[ObjectId], time: u64) -> Physical {
    let mut bytes = format!("tree {tree}\n");
    for parent in parents {
        bytes.push_str(&format!("parent {parent}\n"));
    }
    bytes.push_str(&format!(
        "author A <a@example.invalid> {time} +0000\ncommitter A <a@example.invalid> {time} +0000\n\nmessage\n"
    ));
    physical(Kind::Commit, bytes.into_bytes())
}

struct Source {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, Physical>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Source {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

fn copied(value: &Physical, budget: &CapacityBudget) -> Result<Object> {
    let mut data = budget.buffer(value.bytes.len())?;
    data.resize(value.bytes.len(), 0)?;
    data.as_mut_slice().copy_from_slice(&value.bytes);
    Ok(Object {
        kind: value.kind,
        data,
    })
}

impl VerifiedObjectProvider for Source {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, _: &Control) -> Result<Option<Header>> {
        Ok(self.objects.get(id).map(|value| Header {
            kind: value.kind,
            size: value.bytes.len() as u64,
        }))
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        self.objects
            .get(id)
            .map(|value| copied(value, budget))
            .transpose()
    }
}

struct Incoming {
    count: usize,
    objects: VecDeque<Physical>,
}

impl IncomingObjects for Incoming {
    fn object_count(&self) -> usize {
        self.count
    }
    fn next_object(
        &mut self,
        control: &Control,
        budget: &CapacityBudget,
        _: ReadLimits,
    ) -> Result<Option<(ObjectId, Object)>> {
        control.check()?;
        self.objects
            .pop_front()
            .map(|value| Ok((value.id, copied(&value, budget)?)))
            .transpose()
    }
}

// Keep the immutable authorized-ref adapter local to this public API fixture.
struct Refs(BTreeMap<Vec<u8>, ObjectId>);
impl RefSnapshot for Refs {
    fn ref_oid(&self, name: &[u8]) -> Option<ObjectId> {
        self.0.get(name).copied()
    }
}

fn packet(bytes: &[u8], output: &mut Vec<u8>) {
    gix_packetline::blocking_io::encode::data_to_write(bytes, output).unwrap();
}

fn command_bytes(commands: &[(ObjectId, ObjectId, &[u8])], capabilities: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    for (index, (old, new, name)) in commands.iter().enumerate() {
        let mut line = format!("{old} {new} ").into_bytes();
        line.extend_from_slice(name);
        if index == 0 {
            line.push(0);
            line.extend_from_slice(capabilities);
        }
        packet(&line, &mut output);
    }
    gix_packetline::blocking_io::encode::flush_to_write(&mut output).unwrap();
    output
}

struct Fixture {
    budget: CapacityBudget,
    control: Control,
    dropped: Arc<AtomicBool>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            budget: CapacityBudget::new(32 * 1024 * 1024),
            control: Control::new(Instant::now() + Duration::from_secs(10)),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
    fn batch(&self, bytes: &[u8]) -> CommandBatch {
        CommandBatch::read(
            &mut Cursor::new(bytes),
            &self.control,
            self.budget.clone(),
            CommandLimits::default(),
        )
        .unwrap()
    }
    fn session(&self, objects: &[Physical], limits: ReceiveLimits) -> ReceiveSession<Source> {
        let identity = SnapshotIdentity {
            source: b"receive-synthetic".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        };
        let source = Source {
            identity: identity.clone(),
            objects: objects
                .iter()
                .cloned()
                .map(|object| (object.id, object))
                .collect(),
            dropped: self.dropped.clone(),
        };
        ReceiveSession::new(
            SnapshotSession::open(
                source,
                &identity,
                self.control.clone(),
                self.budget.clone(),
                ReadLimits::default(),
            )
            .unwrap(),
            limits,
        )
        .unwrap()
    }
    fn incoming(&self, objects: &[Physical]) -> Incoming {
        Incoming {
            count: objects.len(),
            objects: objects.iter().cloned().collect(),
        }
    }
}

fn zero() -> ObjectId {
    ObjectId::null(gix_hash::Kind::Sha1)
}

#[test]
fn native_separator_is_accepted_without_weakening_capability_boundaries() {
    let f = Fixture::new();
    let id = physical(Kind::Blob, b"data".to_vec()).id;
    let caps = b" report-status side-band-64k atomic object-format=sha1 agent=git/2.56.0-Linux";
    let batch = f.batch(&command_bytes(&[(zero(), id, b"refs/tags/native")], caps));
    assert_eq!(batch.raw_capabilities(), caps);
    assert!(
        batch.capabilities().report_status()
            && batch.capabilities().sideband()
            && batch.capabilities().atomic()
    );
    drop(batch);
    for caps in [
        b"  report-status".as_slice(),
        b" report-status ",
        b" report-status report-status",
        b"\treport-status",
        b" report-status\0atomic",
        b" push-options",
    ] {
        let bytes = command_bytes(&[(zero(), id, b"refs/tags/native")], caps);
        assert!(
            CommandBatch::read(
                &mut bytes.as_slice(),
                &f.control,
                f.budget.clone(),
                CommandLimits::default()
            )
            .is_err()
        );
    }
    let bytes = command_bytes(&[(zero(), id, b"refs/tags/native")], b" report-status");
    assert!(
        CommandBatch::read(
            &mut bytes.as_slice(),
            &f.control,
            f.budget.clone(),
            CommandLimits {
                max_capability_bytes: 13,
                ..Default::default()
            }
        )
        .is_err(),
        "raw separator byte must remain charged"
    );
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn empty_command_flush_has_a_typed_noop_without_reading_pack_or_eof() {
    let f = Fixture::new();
    let mut input = Cursor::new(b"0000unread".to_vec());
    let section = CommandSection::read(
        &mut input,
        &f.control,
        f.budget.clone(),
        CommandLimits::default(),
    )
    .unwrap();
    assert!(matches!(section, CommandSection::Noop(_)));
    assert_eq!(input.position(), 4);
    drop(section);
    assert_eq!(f.budget.used(), 0);
    for bytes in [b"".as_slice(), b"000", b"0001", b"0002"] {
        let mut input = bytes;
        assert!(
            CommandSection::read(
                &mut input,
                &f.control,
                f.budget.clone(),
                CommandLimits::default()
            )
            .is_err()
        );
    }
}

#[test]
fn gitmodules_review_counterexample_is_rejected_before_a_validation_witness() {
    let blob = physical(
        Kind::Blob,
        b"[submodule \"x\"]\n\tpath = -evil\n\turl = https://example.invalid/x\n".to_vec(),
    );
    assert_eq!(
        blob.id.to_string(),
        "f7c64f494ac830a8bd41d82d4f3d18b7d5445854"
    );
    let mut tree_bytes = b"100644 .gitmodules\0".to_vec();
    tree_bytes.extend_from_slice(blob.id.as_slice());
    let tree = physical(Kind::Tree, tree_bytes);
    assert_eq!(
        tree.id.to_string(),
        "176b92700401d61c379c2d2f25301830b54c50f1"
    );
    let commit = physical(Kind::Commit, b"tree 176b92700401d61c379c2d2f25301830b54c50f1\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\nfixture\n".to_vec());
    assert_eq!(
        commit.id.to_string(),
        "f403f05e85af6a54fb5a5890c750a915e7d83875"
    );
    let f = Fixture::new();
    let objects = [blob, tree, commit.clone()];
    let batch = f.batch(&command_bytes(
        &[(zero(), commit.id, b"refs/heads/main")],
        b"report-status",
    ));
    let actual = f.session(&objects, ReceiveLimits::default()).run(
        batch,
        &Refs(BTreeMap::new()),
        &mut f.incoming(&objects),
    );
    assert!(
        actual.is_err(),
        "strict .gitmodulesPath content must not authorize a receive"
    );
    drop(actual);
    assert!(f.dropped.load(Ordering::Acquire));
    assert_eq!(f.budget.used(), 0);
}

// Expected outcomes are native Git 2.56.0 strict-index observations, not a
// reimplementation of the validator. Malformed config is a non-fatal warning.
fn gitmodules_cases() -> Vec<(&'static str, &'static [u8], bool, bool)> {
    vec![
        ("valid", b"[submodule \"x\"]\npath = sub\nurl = https://example.invalid/x\n", true, false),
        ("option path", b"[submodule \"x\"]\npath = -evil\n", false, false),
        ("option url", b"[submodule \"x\"]\nurl = -evil\n", false, false),
        ("parent path", b"[submodule \"x\"]\npath = ../escape\n", true, false),
        ("dotgit path", b"[submodule \"x\"]\npath = .git\n", true, false),
        ("nested dotgit", b"[submodule \"x\"]\npath = dir/.git/config\n", true, false),
        ("absolute path", b"[submodule \"x\"]\npath = /absolute\n", true, false),
        ("windows path", b"[submodule \"x\"]\npath = C:/absolute\n", true, false),
        ("backslash path", b"[submodule \"x\"]\npath = \"dir\\\\sub\"\n", true, false),
        ("double slash", b"[submodule \"x\"]\npath = dir//sub\n", true, false),
        ("quoted space", b"[submodule \"x\"]\npath = \" -evil\"\n", true, false),
        ("parent name", b"[submodule \"../x\"]\npath = sub\n", false, false),
        ("empty name", b"[submodule \"\"]\npath = sub\n", false, false),
        ("ext helper", b"[submodule \"x\"]\nurl = ext::sh -c evil\n", true, false),
        ("relative url", b"[submodule \"x\"]\nurl = ../other\n", true, false),
        ("percent LF", b"[submodule \"x\"]\nurl = https://example.invalid/a%0ab\n", false, false),
        ("percent CR", b"[submodule \"x\"]\nurl = https://example.invalid/a%0Db\n", true, false),
        ("escaped LF", b"[submodule \"x\"]\nurl = \"https://example.invalid/a\\nb\"\n", false, false),
        ("escaped tab", b"[submodule \"x\"]\nurl = \"https://example.invalid/a\\tb\"\n", true, false),
        ("escaped CR", b"[submodule \"x\"]\nurl = \"https://example.invalid/a\rb\"\n", true, false),
        ("encoded NUL before LF", b"[submodule \"x\"]\nurl = ../a%00b%0ac\n", false, false),
        ("literal NUL before LF", b"[submodule \"x\"]\nurl = ../a\0b%0ac\n", true, false),
        ("backslash relative escape", b"[submodule \"x\"]\nurl = \"..\\\\:host\"\n", false, false),
        ("normalized removed newline", b"[submodule \"x\"]\nurl = https://example.invalid/a%0ab/../safe\n", true, false),
        ("normalized malformed removed escape", b"[submodule \"x\"]\nurl = https://example.invalid/a%zz/../safe\n", false, false),
        ("command update", b"[submodule \"x\"]\nupdate = !evil\n", false, false),
        ("arbitrary update", b"[submodule \"x\"]\nupdate = whatever\n", true, false),
        ("malformed", b"[submodule \"x\"\npath = sub\n", true, true),
        ("old header", b"[submodule.x]\npath = sub\nurl = ../x\n", true, false),
        ("mixed case", b"[SUBMODULE \"x\"]\nPATH = -evil\n", false, false),
        ("duplicate path", b"[submodule \"x\"]\npath = -evil\npath = good\n", false, false),
        ("duplicate url", b"[submodule \"x\"]\nurl = -evil\nurl = ../good\n", false, false),
        ("duplicate update", b"[submodule \"x\"]\nupdate = !evil\nupdate = checkout\n", false, false),
        ("valid duplicate", b"[submodule \"x\"]\npath = good\npath = other\n", true, false),
        ("include not expanded", b"[include]\npath = /does/not/exist\n[submodule \"x\"]\npath = good\n", true, false),
        ("implicit path", b"[submodule \"x\"]\npath\n", true, false),
        ("implicit url", b"[submodule \"x\"]\nurl\n", true, false),
        ("implicit update", b"[submodule \"x\"]\nupdate\n", true, false),
        ("NUL value", b"[submodule \"x\"]\npath = good\0-evil\n", true, false),
        ("NUL suffix", b"[submodule \"x\"]\npath = good\n\0[submodule \"y\"]\npath = -evil\n", true, true),
        ("folding", b";comment\n[submodule \"x\"]\npath = sub\\\nmodule #comment\nurl = \"../up\\tstream\"\n", true, false),
        ("bad suffix cannot hide path", b"[submodule \"x\"]\npath = -evil\n[broken\n", false, true),
        ("bad suffix cannot hide url", b"[submodule \"x\"]\nurl = -evil\n[broken\n", false, true),
        ("bad suffix cannot hide update", b"[submodule \"x\"]\nupdate = !evil\n[broken\n", false, true),
    ]
}

fn gitmodules_tree(blob: ObjectId, name: &[u8]) -> Physical {
    let mut bytes = b"100644 ".to_vec();
    bytes.extend_from_slice(name);
    bytes.push(0);
    bytes.extend_from_slice(blob.as_slice());
    physical(Kind::Tree, bytes)
}

#[test]
fn gitmodules_config_policy_checks_all_values_and_preserves_historical_warnings() {
    let mut mismatches = Vec::new();
    for (label, bytes, accepted, warning) in gitmodules_cases() {
        let f = Fixture::new();
        let blob = physical(Kind::Blob, bytes.to_vec());
        let tree = gitmodules_tree(blob.id, b".gitmodules");
        let objects = [blob, tree.clone()];
        let batch = f.batch(&command_bytes(&[(zero(), tree.id, b"refs/tags/tree")], b""));
        let result = f.session(&objects, ReceiveLimits::default()).run(
            batch,
            &Refs(BTreeMap::new()),
            &mut f.incoming(&objects),
        );
        if result.is_ok() != accepted {
            mismatches.push(format!(
                "{label}: expected accepted={accepted}, actual={:?}",
                result.as_ref().err()
            ));
        }
        if let Ok(result) = &result
            && result
                .warnings()
                .iter()
                .any(|entry| entry.kind == WarningKind::GitmodulesParse)
                != warning
        {
            mismatches.push(format!("{label}: expected parse warning={warning}"));
        }
        drop(result);
        assert_eq!(f.budget.used(), 0, "{label}");
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn gitmodules_context_cannot_be_skipped_by_ordinary_prior_visits_or_old_blobs() {
    for name in [
        b".gitmodules".as_slice(),
        b".GITMODULES",
        b".gitmodules ",
        b".gitmodules:stream",
        b".\xe2\x80\x8cgitmodules",
        b"dir\\.gitmodules",
    ] {
        for old_blob in [false, true] {
            let f = Fixture::new();
            let blob = physical(Kind::Blob, b"[submodule \"x\"]\npath = -evil\n".to_vec());
            let tree = gitmodules_tree(blob.id, name);
            // The only requested root is the ordinary blob. The dangerous tree
            // is unreachable incoming data, possibly referencing an old blob.
            let objects = [blob.clone(), tree.clone()];
            let incoming = if old_blob {
                vec![tree]
            } else {
                objects.to_vec()
            };
            let batch = f.batch(&command_bytes(
                &[(zero(), blob.id, b"refs/tags/ordinary")],
                b"",
            ));
            let result = f.session(&objects, ReceiveLimits::default()).run(
                batch,
                &Refs(BTreeMap::new()),
                &mut f.incoming(&incoming),
            );
            assert!(result.is_err(), "name={name:?}, old={old_blob}");
            drop(result);
            assert!(f.dropped.load(Ordering::Acquire));
            assert_eq!(f.budget.used(), 0);
        }
    }
    let f = Fixture::new();
    let blob = physical(Kind::Blob, b"[submodule \"x\"]\npath = -evil\n".to_vec());
    let tree = gitmodules_tree(blob.id, b"ordinary");
    let objects = [blob, tree.clone()];
    let batch = f.batch(&command_bytes(
        &[(zero(), tree.id, b"refs/tags/ordinary")],
        b"",
    ));
    let result = f.session(&objects, ReceiveLimits::default()).run(
        batch,
        &Refs(BTreeMap::new()),
        &mut f.incoming(&objects),
    );
    assert!(result.is_ok(), "ordinary blobs must remain opaque");
    drop(result);
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn command_batch_preserves_order_bytes_and_leaves_the_pack_unread() {
    let f = Fixture::new();
    let id = physical(Kind::Blob, b"data".to_vec()).id;
    let commands = [
        (zero(), id, b"refs/tags/\xff".as_slice()),
        (id, zero(), b"refs/heads/remove".as_slice()),
    ];
    let mut bytes = command_bytes(
        &commands,
        b"report-status side-band-64k atomic object-format=sha1",
    );
    bytes.extend_from_slice(b"PACK untouched");
    let mut input = Cursor::new(bytes);
    let batch = CommandBatch::read(
        &mut input,
        &f.control,
        f.budget.clone(),
        CommandLimits::default(),
    )
    .unwrap();
    assert_eq!(batch.commands().len(), 2);
    for (actual, (old, new, name)) in batch.commands().iter().zip(commands) {
        assert_eq!(
            (actual.old(), actual.new(), actual.name()),
            (old, new, name)
        );
    }
    assert!(batch.capabilities().atomic());
    assert!(batch.capabilities().report_status());
    assert!(batch.capabilities().sideband());
    let mut rest = Vec::new();
    input.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"PACK untouched");
    drop(batch);
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn capabilities_duplicates_malformed_boundaries_and_missing_flush_are_rejected() {
    let f = Fixture::new();
    let id = physical(Kind::Blob, b"data".to_vec()).id;
    let command = [(zero(), id, b"refs/tags/a".as_slice())];
    let mut cases = Vec::new();
    for caps in [
        b"push-options".as_slice(),
        b"report-status-v2",
        b"object-format=sha256",
        b"atomic atomic",
        b"report-status\0atomic",
        b"atomic  ofs-delta",
    ] {
        cases.push(command_bytes(&command, caps));
    }
    let mut second_caps = command_bytes(&command, b"atomic");
    second_caps.truncate(second_caps.len() - 4);
    packet(
        format!("{} {id} refs/tags/b\0atomic", zero()).as_bytes(),
        &mut second_caps,
    );
    second_caps.extend_from_slice(b"0000");
    cases.push(second_caps);
    cases.push(command_bytes(&[command[0], command[0]], b""));
    let mut missing_flush = command_bytes(&command, b"");
    missing_flush.truncate(missing_flush.len() - 4);
    cases.push(missing_flush);
    cases.push(b"0001".to_vec());
    cases.push(b"0002".to_vec());
    cases.push(b"0000".to_vec());
    for bytes in cases {
        assert!(
            CommandBatch::read(
                &mut bytes.as_slice(),
                &f.control,
                f.budget.clone(),
                CommandLimits::default()
            )
            .is_err(),
            "accepted {bytes:?}"
        );
        assert_eq!(
            f.budget.used(),
            0,
            "failed command parse must release all capacity"
        );
    }
}

#[test]
fn command_count_byte_and_capacity_limits_are_checked_before_returning_a_batch() {
    let f = Fixture::new();
    let id = physical(Kind::Blob, b"data".to_vec()).id;
    let bytes = command_bytes(
        &[(zero(), id, b"refs/tags/a"), (zero(), id, b"refs/tags/b")],
        b"atomic",
    );
    for limits in [
        CommandLimits {
            max_commands: 1,
            ..CommandLimits::default()
        },
        CommandLimits {
            max_command_bytes: bytes.len() - 5,
            ..CommandLimits::default()
        },
        CommandLimits {
            max_ref_bytes: 5,
            ..CommandLimits::default()
        },
        CommandLimits {
            max_capability_bytes: 5,
            ..CommandLimits::default()
        },
        CommandLimits {
            max_capabilities: 0,
            ..CommandLimits::default()
        },
    ] {
        assert!(
            CommandBatch::read(&mut bytes.as_slice(), &f.control, f.budget.clone(), limits,)
                .is_err()
        );
        assert_eq!(f.budget.used(), 0);
    }
    let mut input = Cursor::new(bytes);
    assert!(
        CommandBatch::read(
            &mut input,
            &f.control,
            CapacityBudget::new(1024),
            CommandLimits::default(),
        )
        .is_err()
    );
    assert_eq!(
        input.position(),
        0,
        "admit the maintained reader before reading input"
    );
}

#[test]
fn deletion_without_incoming_objects_retains_the_same_full_batch_and_false_ff() {
    let f = Fixture::new();
    let old = physical(Kind::Blob, b"stored previously".to_vec()).id;
    let name = b"refs/tags/remove";
    let batch = f.batch(&command_bytes(&[(old, zero(), name)], b"report-status"));
    assert!(batch.delete_only());
    let mut refs = BTreeMap::new();
    refs.insert(name.to_vec(), old);
    let result = f
        .session(&[], ReceiveLimits::default())
        .run(batch, &Refs(refs), &mut f.incoming(&[]))
        .unwrap();
    assert_eq!(result.commands().commands().len(), 1);
    assert_eq!(result.commands().commands()[0].name(), name);
    assert_eq!(result.fast_forward(), &[false]);
    assert_eq!(result.incoming_objects(), 0);
    result.check().unwrap();
    drop(result);
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn full_batch_old_oid_check_rejects_a_stale_second_command_without_consuming_objects() {
    let f = Fixture::new();
    let object = physical(Kind::Blob, b"data".to_vec());
    let commands = f.batch(&command_bytes(
        &[
            (zero(), object.id, b"refs/tags/a"),
            (object.id, zero(), b"refs/tags/b"),
        ],
        b"",
    ));
    let mut incoming = f.incoming(std::slice::from_ref(&object));
    let result = f.session(&[object], ReceiveLimits::default()).run(
        commands,
        &Refs(BTreeMap::new()),
        &mut incoming,
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("stale expected ref")
    );
    assert_eq!(
        incoming.objects.len(),
        1,
        "preflight must reject the entire batch"
    );
    assert!(f.dropped.load(Ordering::Acquire));
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn unreachable_malformed_objects_and_missing_or_wrong_typed_edges_are_rejected() {
    for bad in [
        physical(
            Kind::Commit,
            b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor invalid\ncommitter invalid\n\n"
                .to_vec(),
        ),
        physical(
            Kind::Tag,
            b"object 4b825dc642cb6eb9a060e54bf8d69288fbee4904\ntype invalid\ntag x\n\n".to_vec(),
        ),
        physical(Kind::Tree, b"100644 unterminated".to_vec()),
        commit(
            ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap(),
            &[],
            1,
        ),
    ] {
        let f = Fixture::new();
        let blob = physical(Kind::Blob, b"existing".to_vec());
        let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/good")], b""));
        let mut incoming = f.incoming(std::slice::from_ref(&bad));
        assert!(
            f.session(&[blob, bad], ReceiveLimits::default())
                .run(batch, &Refs(BTreeMap::new()), &mut incoming)
                .is_err()
        );
        assert_eq!(f.budget.used(), 0);
    }
    let f = Fixture::new();
    let blob = physical(Kind::Blob, b"not a tree".to_vec());
    let bad = commit(blob.id, &[], 1);
    let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/good")], b""));
    assert!(
        f.session(&[blob, bad.clone()], ReceiveLimits::default())
            .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[bad]))
            .is_err()
    );
}

#[test]
fn all_parents_type_checks_and_source_ownership_survive_nonmonotonic_merge_validation() {
    let f = Fixture::new();
    let tree = physical(Kind::Tree, Vec::new());
    let root = commit(tree.id, &[], 900);
    let side = commit(tree.id, &[], 1);
    let merge = commit(tree.id, &[side.id, root.id], 2);
    let batch = f.batch(&command_bytes(
        &[(root.id, merge.id, b"refs/heads/main")],
        b"report-status",
    ));
    let mut refs = BTreeMap::new();
    refs.insert(b"refs/heads/main".to_vec(), root.id);
    let objects = [tree, root, side, merge.clone()];
    let result = f
        .session(&objects, ReceiveLimits::default())
        .run(batch, &Refs(refs), &mut f.incoming(&[merge]))
        .unwrap();
    assert_eq!(result.fast_forward(), &[true]);
    assert_eq!(result.incoming_objects(), 1);
    assert!(
        !f.dropped.load(Ordering::Acquire),
        "validated result must retain source hold"
    );
    assert!(f.budget.used() > 0);
    drop(result);
    assert!(f.dropped.load(Ordering::Acquire));
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn every_physical_duplicate_is_rehashed_and_declared_enumeration_is_exact() {
    let f = Fixture::new();
    let blob = physical(Kind::Blob, b"valid".to_vec());
    let mut corrupt = blob.clone();
    corrupt.bytes = b"corrupt bytes carrying the prior ID".to_vec();
    for (count, entries) in [
        (2, vec![blob.clone(), corrupt]),
        (2, vec![blob.clone()]),
        (0, vec![blob.clone()]),
    ] {
        let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/a")], b""));
        let mut incoming = Incoming {
            count,
            objects: entries.into(),
        };
        assert!(
            f.session(std::slice::from_ref(&blob), ReceiveLimits::default())
                .run(batch, &Refs(BTreeMap::new()), &mut incoming)
                .is_err()
        );
        assert_eq!(f.budget.used(), 0);
    }
}

#[test]
fn a_hash_verified_empty_blob_cannot_satisfy_a_tree_edge_even_when_it_parses_as_empty_tree() {
    let f = Fixture::new();
    let blob = physical(Kind::Blob, Vec::new());
    let bad = commit(blob.id, &[], 1);
    let batch = f.batch(&command_bytes(&[(zero(), bad.id, b"refs/heads/main")], b""));
    let result = f
        .session(&[blob, bad.clone()], ReceiveLimits::default())
        .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[bad]));
    assert!(
        result.is_err(),
        "syntax validity cannot substitute for edge type"
    );
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn valid_128_mib_commit_message_and_full_blob_are_accepted_but_one_extra_byte_is_not() {
    for kind in [Kind::Commit, Kind::Blob] {
        for extra in [false, true] {
            let mut f = Fixture::new();
            f.budget = CapacityBudget::new(1024 * 1024 * 1024);
            f.control = Control::new(Instant::now() + Duration::from_secs(120));
            let tree = physical(Kind::Tree, Vec::new());
            let mut bytes = if kind == Kind::Commit {
                commit(tree.id, &[], 1).bytes
            } else {
                Vec::new()
            };
            bytes.resize(128 * 1024 * 1024 + usize::from(extra), b'x');
            let object = physical(kind, bytes);
            let batch = f.batch(&command_bytes(
                &[(zero(), object.id, b"refs/tags/full")],
                b"",
            ));
            let result = f
                .session(&[tree, object.clone()], ReceiveLimits::default())
                .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[object]));
            assert_eq!(result.is_err(), extra, "kind={kind:?}, extra={extra}");
            drop(result);
            assert!(f.dropped.load(Ordering::Acquire));
            assert_eq!(f.budget.used(), 0);
        }
    }
}

#[test]
fn shared_capacity_denial_keeps_foreign_reservations_and_releases_all_receive_owners() {
    for spare in [0, 128, 4096] {
        let f = Fixture::new();
        let tree = physical(Kind::Tree, Vec::new());
        let object = commit(tree.id, &[], 1);
        let batch = f.batch(&command_bytes(
            &[(zero(), object.id, b"refs/heads/main")],
            b"",
        ));
        let session = f.session(&[tree, object.clone()], ReceiveLimits::default());
        let foreign_bytes = 32 * 1024 * 1024 - f.budget.used() - spare;
        let foreign = f.budget.reservation(foreign_bytes).unwrap();
        let result = session.run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[object]));
        assert!(
            result.is_err(),
            "unexpected success with {spare} spare bytes"
        );
        assert!(f.dropped.load(Ordering::Acquire));
        assert_eq!(f.budget.used(), foreign_bytes);
        drop(foreign);
        assert_eq!(f.budget.used(), 0);
    }
}

/// The remaining maintained path-component calls are allocation-free linear
/// scans, not resumable operations. Qualify their finite maximum-input work at
/// the existing 128 MiB object bound rather than inventing a filename-size cap.
#[test]
fn gitlinks_are_external_but_blob_edges_and_branch_targets_are_not() {
    let f = Fixture::new();
    let missing = ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
    let mut bytes = b"160000 external\0".to_vec();
    bytes.extend_from_slice(missing.as_slice());
    let tree = physical(Kind::Tree, bytes);
    let batch = f.batch(&command_bytes(&[(zero(), tree.id, b"refs/tags/tree")], b""));
    assert!(
        f.session(std::slice::from_ref(&tree), ReceiveLimits::default())
            .run(
                batch,
                &Refs(BTreeMap::new()),
                &mut f.incoming(std::slice::from_ref(&tree))
            )
            .is_ok()
    );
    let batch = f.batch(&command_bytes(
        &[(zero(), tree.id, b"refs/heads/not-commit")],
        b"",
    ));
    assert!(
        f.session(std::slice::from_ref(&tree), ReceiveLimits::default())
            .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[tree]))
            .is_err()
    );
}

#[test]
fn opaque_messages_folded_extensions_and_optional_tagger_do_not_scan_as_headers() {
    let f = Fixture::new();
    let tree = physical(Kind::Tree, Vec::new());
    let mut body = format!("tree {}\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\ngpgsig first\n second\n \n\n", tree.id).into_bytes();
    body.extend_from_slice(b"\0opaque\xff\nauthor invalid\n");
    let root = physical(Kind::Commit, body);
    let tag = physical(
        Kind::Tag,
        format!("object {}\ntype commit\ntag release\n\n", root.id).into_bytes(),
    );
    let batch = f.batch(&command_bytes(
        &[(zero(), tag.id, b"refs/tags/release")],
        b"",
    ));
    let result = f
        .session(&[tree, root, tag.clone()], ReceiveLimits::default())
        .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[tag]))
        .unwrap();
    assert_eq!(result.warnings().len(), 1);
    assert_eq!(result.warnings()[0].kind, WarningKind::MissingTagger);
}

#[test]
fn cancellation_and_aggregate_work_limits_return_no_semantic_result_and_release_capacity() {
    let f = Fixture::new();
    let blob = physical(Kind::Blob, b"12345".to_vec());
    let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/a")], b""));
    let session = f.session(
        std::slice::from_ref(&blob),
        ReceiveLimits {
            max_decoded_bytes: 4,
            ..ReceiveLimits::default()
        },
    );
    assert!(
        session
            .run(
                batch,
                &Refs(BTreeMap::new()),
                &mut f.incoming(std::slice::from_ref(&blob))
            )
            .is_err()
    );
    assert_eq!(f.budget.used(), 0);
    let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/a")], b""));
    let session = f.session(std::slice::from_ref(&blob), ReceiveLimits::default());
    f.control.cancel();
    assert!(
        session
            .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[blob]))
            .is_err()
    );
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn cancellation_after_incoming_delivery_releases_the_object_batch_and_source() {
    struct CancellingIncoming(Incoming);
    impl IncomingObjects for CancellingIncoming {
        fn object_count(&self) -> usize {
            self.0.object_count()
        }
        fn next_object(
            &mut self,
            control: &Control,
            budget: &CapacityBudget,
            limits: ReadLimits,
        ) -> Result<Option<(ObjectId, Object)>> {
            let object = self.0.next_object(control, budget, limits)?;
            // An upstream read may observe cancellation while delivering its
            // final buffer. The receiver must not certify or retain that buffer.
            control.cancel();
            Ok(object)
        }
    }
    let f = Fixture::new();
    let blob = physical(Kind::Blob, b"delivered before cancellation".to_vec());
    let batch = f.batch(&command_bytes(&[(zero(), blob.id, b"refs/tags/a")], b""));
    let session = f.session(std::slice::from_ref(&blob), ReceiveLimits::default());
    let error = session
        .run(
            batch,
            &Refs(BTreeMap::new()),
            &mut CancellingIncoming(f.incoming(&[blob])),
        )
        .err()
        .expect("mid-read cancellation must not produce a semantic result");
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(f.dropped.load(Ordering::Acquire));
    assert_eq!(f.budget.used(), 0);
}

#[test]
fn tree_order_duplicate_names_modes_and_wrong_type_after_prior_visit_are_checked() {
    let blob = physical(Kind::Blob, b"data".to_vec());
    for records in [
        vec![(b"100644".as_slice(), b"z".as_slice()), (b"100644", b"a")],
        vec![(b"100644".as_slice(), b"a".as_slice()), (b"100644", b"a")],
        vec![(b"040000".as_slice(), b"directory".as_slice())],
        vec![(b"999999".as_slice(), b"invalid-mode".as_slice())],
        vec![(b"100644".as_slice(), b".git".as_slice())],
        vec![(b"100644".as_slice(), b"nested/path".as_slice())],
        vec![(b"40000".as_slice(), b"wrong-type".as_slice())],
    ] {
        let f = Fixture::new();
        let mut bytes = Vec::new();
        for (mode, name) in records {
            bytes.extend_from_slice(mode);
            bytes.push(b' ');
            bytes.extend_from_slice(name);
            bytes.push(0);
            bytes.extend_from_slice(blob.id.as_slice());
        }
        let tree = physical(Kind::Tree, bytes);
        // First root visits the blob as Blob; a later Tree edge must still type-check.
        let batch = f.batch(&command_bytes(
            &[
                (zero(), blob.id, b"refs/tags/blob"),
                (zero(), tree.id, b"refs/tags/tree"),
            ],
            b"",
        ));
        assert!(
            f.session(&[blob.clone(), tree.clone()], ReceiveLimits::default())
                .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[tree]))
                .is_err()
        );
        assert_eq!(f.budget.used(), 0);
    }
}

#[test]
fn repeated_reserved_headers_bad_dates_and_header_boundary_cannot_hide_in_messages() {
    let tree = physical(Kind::Tree, Vec::new());
    let valid = commit(tree.id, &[], 1);
    let text = String::from_utf8(valid.bytes).unwrap();
    let variants = [
        text.replace(
            "\n\nmessage",
            "\nauthor A <a@example.invalid> 1 +0000\n\nmessage",
        ),
        text.replace(" 1 +0000", " 01 +0000"),
        text.replace(" 1 +0000", " 1 --700"),
        text.replace("\n\nmessage", "\nmessage"),
        text.replace("author A", "author \0A"),
    ];
    for bytes in variants {
        let f = Fixture::new();
        let object = physical(Kind::Commit, bytes.into_bytes());
        let batch = f.batch(&command_bytes(&[(zero(), object.id, b"refs/heads/a")], b""));
        assert!(
            f.session(&[tree.clone(), object.clone()], ReceiveLimits::default())
                .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[object]))
                .is_err()
        );
    }
}

#[test]
fn a_positive_parent_match_never_hides_another_missing_parent_or_physical_empty_tree() {
    for missing_tree in [false, true] {
        let f = Fixture::new();
        let tree = physical(Kind::Tree, Vec::new());
        let old = commit(tree.id, &[], 999);
        let missing = ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
        let parents = if missing_tree {
            vec![old.id]
        } else {
            vec![old.id, missing]
        };
        let new = commit(tree.id, &parents, 1);
        let mut objects = vec![old.clone(), new.clone()];
        if !missing_tree {
            objects.push(tree);
        }
        let mut refs = BTreeMap::new();
        refs.insert(b"refs/heads/main".to_vec(), old.id);
        let batch = f.batch(&command_bytes(&[(old.id, new.id, b"refs/heads/main")], b""));
        assert!(
            f.session(&objects, ReceiveLimits::default())
                .run(batch, &Refs(refs), &mut f.incoming(&[new]))
                .is_err()
        );
        assert_eq!(f.budget.used(), 0);
    }
}

#[test]
fn an_oversized_header_fails_while_the_same_bytes_in_an_opaque_message_are_valid() {
    for in_header in [false, true] {
        let f = Fixture::new();
        let tree = physical(Kind::Tree, Vec::new());
        let mut bytes = format!(
            "tree {}\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n",
            tree.id
        ).into_bytes();
        if in_header {
            bytes.extend_from_slice(b"extension ");
        } else {
            bytes.push(b'\n');
        }
        bytes.extend(std::iter::repeat_n(b'x', 64 * 1024));
        bytes.extend_from_slice(b"\n\n");
        let object = physical(Kind::Commit, bytes);
        let batch = f.batch(&command_bytes(
            &[(zero(), object.id, b"refs/heads/main")],
            b"",
        ));
        let result = f
            .session(&[tree, object.clone()], ReceiveLimits::default())
            .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[object]));
        assert_eq!(result.is_err(), in_header);
        drop(result);
        assert_eq!(f.budget.used(), 0);
    }
}

/// Characterization, not an assumption: compare our strict-error/INFO distinction
/// to an isolated native index-pack --strict. Must run explicitly in the later
/// oracle-enabled gate; it is not part of this LIGHT implementation's evidence.
#[test]
#[ignore = "requires isolated native Git oracle; run after HEAVY/oracle authorization"]
fn native_strict_warning_and_tree_mode_policy_matches_semantic_validation() {
    use std::process::{Command, Stdio};
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    let identity = format!(
        "receive-oracle-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let directory = Directory(std::env::temp_dir().join(identity));
    std::fs::create_dir(&directory.0).unwrap();
    let source = directory.0.join("source");
    std::fs::create_dir(&source).unwrap();
    let git = |cwd: &std::path::Path, args: &[&str], input: &[u8]| {
        let mut child = Command::new("timeout")
            .args(["10s", "git", "-C"])
            .arg(cwd)
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", &directory.0)
            .env("LC_ALL", "C")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        let output = child.wait_with_output().unwrap();
        assert_ne!(output.status.code(), Some(124), "native oracle timed out");
        output
    };
    assert!(
        git(&source, &["init", "--bare", "--object-format=sha1"], b"")
            .status
            .success()
    );
    let blob = physical(Kind::Blob, b"data".to_vec());
    let empty_tree = physical(Kind::Tree, Vec::new());
    let tag_missing = physical(
        Kind::Tag,
        format!("object {}\ntype blob\ntag release\n\n", blob.id).into_bytes(),
    );
    let tag_bad_name = physical(
        Kind::Tag,
        format!(
            "object {}\ntype blob\ntag bad name\ntagger A <a@example.invalid> 1 +0000\n\n",
            blob.id
        )
        .into_bytes(),
    );
    let mut cases = vec![
        (tag_missing, Some("missingTaggerEntry")),
        (tag_bad_name, Some("badTagName")),
    ];
    for mode in [b"100644".as_slice(), b"040000", b"100664"] {
        let mut bytes = mode.to_vec();
        bytes.extend_from_slice(b" name\0");
        let target = if mode == b"040000" {
            empty_tree.id
        } else {
            blob.id
        };
        bytes.extend_from_slice(target.as_slice());
        let diagnostic = match mode {
            b"040000" => Some("zeroPaddedFilemode"),
            b"100664" => Some("badFilemode"),
            _ => None,
        };
        cases.push((physical(Kind::Tree, bytes), diagnostic));
    }
    for name in [b".\xe2\x80\x8cgit".as_slice(), b".git   ", b"ordinary\xff"] {
        let mut bytes = b"100644 ".to_vec();
        bytes.extend_from_slice(name);
        bytes.push(0);
        bytes.extend_from_slice(blob.id.as_slice());
        cases.push((physical(Kind::Tree, bytes), None));
    }
    for date in [
        "1 +0000",
        "-1 +0000",
        "01 +0000",
        "1 +2500",
        "1 +1260",
        "1 +000",
        "1 +00000",
        "1 -0000",
        "9223372036854775808 +0000",
        "18446744073709551615 +0000",
        "18446744073709551616 +0000",
        "99999999999999999999999 +0000",
    ] {
        cases.push((
            physical(
                Kind::Commit,
                format!(
                    "tree {}\nauthor A <a@example.invalid> {date}\ncommitter A <a@example.invalid> {date}\n\nmessage\n",
                    empty_tree.id,
                )
                .into_bytes(),
            ),
            None,
        ));
    }
    for (index, (object, diagnostic)) in cases.into_iter().enumerate() {
        for value in [&blob, &empty_tree, &object] {
            let kind = match value.kind {
                Kind::Blob => "blob",
                Kind::Tree => "tree",
                Kind::Tag => "tag",
                Kind::Commit => "commit",
            };
            let output = git(
                &source,
                &["hash-object", "--literally", "-w", "-t", kind, "--stdin"],
                &value.bytes,
            );
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let ids = format!("{}\n{}\n{}\n", blob.id, empty_tree.id, object.id);
        let pack = git(&source, &["pack-objects", "--stdout"], ids.as_bytes());
        assert!(
            pack.status.success(),
            "{}",
            String::from_utf8_lossy(&pack.stderr)
        );
        let destination = directory.0.join(format!("destination-{index}"));
        std::fs::create_dir(&destination).unwrap();
        assert!(
            git(
                &destination,
                &["init", "--bare", "--object-format=sha1"],
                b""
            )
            .status
            .success()
        );
        let oracle = git(
            &destination,
            &["index-pack", "--strict", "--stdin"],
            &pack.stdout,
        );
        if let Some(diagnostic) = diagnostic {
            assert!(String::from_utf8_lossy(&oracle.stderr).contains(diagnostic));
        }
        let f = Fixture::new();
        let batch = f.batch(&command_bytes(
            &[(zero(), object.id, b"refs/tags/fixture")],
            b"",
        ));
        let actual = f
            .session(
                &[blob.clone(), empty_tree.clone(), object.clone()],
                ReceiveLimits::default(),
            )
            .run(batch, &Refs(BTreeMap::new()), &mut f.incoming(&[object]));
        let expected_warning = match diagnostic {
            Some("missingTaggerEntry") => Some(WarningKind::MissingTagger),
            Some("badTagName") => Some(WarningKind::BadTagName),
            Some("badFilemode") => Some(WarningKind::BadFileMode),
            _ => None,
        };
        if let Some(expected_warning) = expected_warning {
            assert!(
                actual
                    .as_ref()
                    .unwrap()
                    .warnings()
                    .iter()
                    .any(|warning| warning.kind == expected_warning)
            );
        }
        assert_eq!(
            actual.is_ok(),
            oracle.status.success(),
            "case {index}: semantic error={:?}, native={}",
            actual.err().map(|error| error.to_string()),
            String::from_utf8_lossy(&oracle.stderr)
        );
    }
}
