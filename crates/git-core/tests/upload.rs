use std::{
    collections::BTreeMap,
    io::{self, Cursor, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider,
    objects::Result,
    upload::{UploadSession, Version},
};

// The same bounded, in-memory VerifiedObjectProvider pattern as the graph
// and history integration fixtures. All objects are real hashed Git objects;
// neither fixture creation nor the upload server invokes native Git.
struct Source {
    identity: SnapshotIdentity,
    objects: BTreeMap<ObjectId, (Kind, Vec<u8>)>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Source {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

impl VerifiedObjectProvider for Source {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>> {
        control.check()?;
        Ok(self.objects.get(id).map(|(kind, bytes)| Header {
            kind: *kind,
            size: bytes.len() as u64,
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
        let Some((kind, bytes)) = self.objects.get(id) else {
            return Ok(None);
        };
        let mut data = budget.buffer(bytes.len())?;
        data.resize(bytes.len(), 0)?;
        data.as_mut_slice().copy_from_slice(bytes);
        Ok(Some(Object { kind: *kind, data }))
    }
}

fn object(source: &mut Source, kind: Kind, bytes: Vec<u8>) -> ObjectId {
    let id = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, &bytes).unwrap();
    source.objects.insert(id, (kind, bytes));
    id
}

struct Fixture {
    upload: UploadSession<'static, Source>,
    middle: ObjectId,
    tip: ObjectId,
    budget: CapacityBudget,
    dropped: Arc<AtomicBool>,
}

impl Fixture {
    fn new(version: Version) -> Self {
        let dropped = Arc::new(AtomicBool::new(false));
        let identity = SnapshotIdentity {
            source: b"upload-depth-round".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix_hash::Kind::Sha1,
        };
        let mut source = Source {
            identity: identity.clone(),
            objects: BTreeMap::new(),
            dropped: dropped.clone(),
        };
        let tree = object(&mut source, Kind::Tree, Vec::new());
        let mut parent = None;
        let mut commits = Vec::new();
        for time in 1..=3 {
            let mut bytes = format!("tree {tree}\n");
            if let Some(parent) = parent {
                bytes.push_str(&format!("parent {parent}\n"));
            }
            bytes.push_str(&format!(
                "author A <a@example.invalid> {time} +0000\ncommitter A <a@example.invalid> {time} +0000\n\nsynthetic {time}\n"
            ));
            let id = object(&mut source, Kind::Commit, bytes.into_bytes());
            parent = Some(id);
            commits.push(id);
        }
        let budget = CapacityBudget::new(4 * 1024 * 1024);
        let snapshot = SnapshotSession::open(
            source,
            &identity,
            Control::new(Instant::now() + Duration::from_secs(10)),
            budget.clone(),
            ReadLimits::default(),
        )
        .unwrap();
        let upload = UploadSession::new(
            snapshot,
            [(b"refs/heads/main".as_slice(), commits[2])],
            b"refs/heads/main",
            version,
        )
        .unwrap();
        Self {
            upload,
            middle: commits[1],
            tip: commits[2],
            budget,
            dropped,
        }
    }

    fn request(&self, capabilities: &str, arguments: &[String]) -> Vec<u8> {
        let mut bytes = packet(&format!("want {} {capabilities}\n", self.tip));
        for argument in arguments {
            bytes.extend(packet(argument));
        }
        bytes.extend_from_slice(b"0000");
        bytes
    }

    fn assert_released(self) {
        assert!(!self.dropped.load(Ordering::Acquire));
        drop(self.upload);
        assert!(self.dropped.load(Ordering::Acquire));
        assert_eq!(self.budget.used(), 0, "no session or round capacity leaked");
    }
}

fn packet(line: &str) -> Vec<u8> {
    format!("{:04x}{line}", line.len() + 4).into_bytes()
}

struct Output {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
    limit: usize,
}

impl Output {
    fn new(fixture: &Fixture, limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            dropped: fixture.dropped.clone(),
            limit,
        }
    }
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(
            !self.dropped.load(Ordering::Acquire),
            "source must outlive writes"
        );
        let count = bytes.len().min(self.limit - self.bytes.len());
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "test output closed",
            ));
        }
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        assert!(
            !self.dropped.load(Ordering::Acquire),
            "source must outlive flush"
        );
        Ok(())
    }
}

// gitprotocol-pack(5), "Packfile Negotiation": upload-request ends in
// flush-pkt and a positive depth request receives shallow-update, itself
// ending in flush-pkt, before haves. gitprotocol-http(5), "Session State":
// HTTP RPCs are stateless. This first RPC is not the final pack exchange.
#[test]
fn stateless_depth_info_only_clone_round_succeeds() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let request = f.request("multi_ack_detailed", &["deepen 1\n".into()]);
        let expected = format!("0035shallow {}\n0000", f.tip).into_bytes();
        let mut output = Output::new(&f, usize::MAX);
        let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
        assert_eq!(
            output.bytes, expected,
            "{version:?}: shallow-info only, no NAK/pack"
        );
        assert!(
            result.is_ok(),
            "{version:?}: complete first RPC must succeed: {result:?}"
        );
        f.assert_released();
    }
}

#[test]
fn stateless_depth_info_only_relative_deepen_round_succeeds() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let request = f.request(
            "multi_ack_detailed deepen-relative",
            &[format!("shallow {}\n", f.tip), "deepen 1\n".into()],
        );
        let expected =
            format!("0035shallow {}\n0037unshallow {}\n0000", f.middle, f.tip).into_bytes();
        let mut output = Output::new(&f, usize::MAX);
        let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
        assert_eq!(
            output.bytes, expected,
            "{version:?}: move exactly one parent edge"
        );
        assert!(
            result.is_ok(),
            "{version:?}: complete first RPC must succeed: {result:?}"
        );
        f.assert_released();
    }
}

#[test]
fn stateless_depth_info_only_unshallow_round_succeeds() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let request = f.request(
            "multi_ack_detailed",
            &[format!("shallow {}\n", f.tip), "deepen 2147483647\n".into()],
        );
        let expected = format!("0037unshallow {}\n0000", f.tip).into_bytes();
        let mut output = Output::new(&f, usize::MAX);
        let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
        assert_eq!(
            output.bytes, expected,
            "{version:?}: remove the client's boundary"
        );
        assert!(
            result.is_ok(),
            "{version:?}: complete first RPC must succeed: {result:?}"
        );
        f.assert_released();
    }
}

#[test]
fn stateless_depth_info_only_unchanged_boundaries_still_complete_the_round() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let request = f.request("multi_ack_detailed", &["deepen 4\n".into()]);
        let mut output = Output::new(&f, usize::MAX);
        let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
        assert_eq!(
            output.bytes, b"0000",
            "{version:?}: empty shallow-update, not a pack"
        );
        assert!(
            result.is_ok(),
            "{version:?}: complete first RPC must succeed: {result:?}"
        );
        f.assert_released();
    }
}

#[test]
fn depth_info_eof_exception_does_not_accept_other_negotiation_eof() {
    for version in [Version::V0, Version::V1] {
        for (stateless, deepening, have) in [
            (true, false, false),
            (false, true, false),
            (true, true, true),
        ] {
            let mut f = Fixture::new(version);
            let arguments = if deepening {
                vec!["deepen 1\n".into()]
            } else {
                vec![]
            };
            let mut request = f.request("multi_ack_detailed", &arguments);
            if have {
                request.extend(packet(&format!("have {}\n", f.tip)));
            }
            let mut output = Output::new(&f, usize::MAX);
            let result = f
                .upload
                .run(&mut Cursor::new(request), &mut output, stateless);
            assert!(
                result.is_err(),
                "{version:?}: stateless={stateless}, deepening={deepening}, have={have}"
            );
            assert!(!output.bytes.windows(4).any(|bytes| bytes == b"PACK"));
            drop(result);
            f.assert_released();
        }
    }
}

#[test]
fn depth_info_round_still_consumes_a_complete_have_round() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let mut request = f.request("multi_ack_detailed", &["deepen 1\n".into()]);
        request.extend(packet(&format!("have {}\n", f.tip)));
        request.extend_from_slice(b"0000");
        let expected = format!(
            "0035shallow {}\n00000038ACK {} common\n0008NAK\n",
            f.tip, f.tip,
        )
        .into_bytes();
        let mut output = Output::new(&f, usize::MAX);
        let mut input = Cursor::new(request);
        let result = f.upload.run(&mut input, &mut output, true);
        assert!(
            result.is_ok(),
            "{version:?}: complete negotiation round: {result:?}"
        );
        assert_eq!(input.position() as usize, input.get_ref().len());
        assert_eq!(
            output.bytes, expected,
            "{version:?}: do not ignore a following have"
        );
        f.assert_released();
    }
}

#[test]
fn depth_info_round_rejects_partial_packets_and_malformed_negotiation() {
    for version in [Version::V0, Version::V1] {
        for suffix in [
            b"0".to_vec(),
            b"000".to_vec(),
            b"0009don".to_vec(),
            b"zzzz".to_vec(),
            b"0001".to_vec(),
            b"0002".to_vec(),
            packet("unexpected\n"),
            packet("have invalid\n"),
        ] {
            let mut f = Fixture::new(version);
            let mut request = f.request("multi_ack_detailed", &["deepen 1\n".into()]);
            request.extend_from_slice(&suffix);
            let mut output = Output::new(&f, usize::MAX);
            let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
            assert!(
                result.is_err(),
                "{version:?}: must reject suffix {suffix:?}"
            );
            assert!(!output.bytes.windows(4).any(|bytes| bytes == b"PACK"));
            drop(result);
            f.assert_released();
        }
    }
}

#[test]
fn depth_info_round_does_not_hide_failure_writing_its_terminal_flush() {
    for version in [Version::V0, Version::V1] {
        let mut f = Fixture::new(version);
        let request = f.request("multi_ack_detailed", &["deepen 1\n".into()]);
        let expected = format!("0035shallow {}\n0000", f.tip).into_bytes();
        let mut output = Output::new(&f, expected.len() - 1);
        let result = f.upload.run(&mut Cursor::new(request), &mut output, true);
        assert!(
            result.is_err(),
            "{version:?}: incomplete response is not success"
        );
        assert_eq!(output.bytes, expected[..expected.len() - 1]);
        drop(result);
        f.assert_released();
    }
}
