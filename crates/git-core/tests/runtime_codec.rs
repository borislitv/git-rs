//! Native-runtime probes run in fresh processes: no process-wide environment
//! mutation or test-only production validation bypass is needed.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use git_rs_core::{
    BlameError, BlameOptions, BlameRequest, CapacityBudget, CodecRuntimeError, Control, Object,
    ProducerVersion, ReadLimits, SnapshotIdentity, SnapshotSession, VerifiedObjectProvider,
};
use gix_hash::{ObjectId, oid};
use gix_object::Header;

#[path = "support/blame_fixture.rs"]
mod fixture;

struct UnreadSource {
    identity: SnapshotIdentity,
    calls: Arc<AtomicUsize>,
}
impl VerifiedObjectProvider for UnreadSource {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, _: &oid, _: &Control) -> git_rs_core::objects::Result<Option<Header>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
    fn object(
        &self,
        _: &oid,
        _: &Control,
        _: &CapacityBudget,
        _: ReadLimits,
    ) -> git_rs_core::objects::Result<Option<Object>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

fn rejected_before_source(expected: CodecRuntimeError) {
    assert_eq!(
        ProducerVersion::verified().err(),
        Some(BlameError::CodecRuntime(expected))
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let identity = SnapshotIdentity {
        source: b"runtime-probe".to_vec(),
        generation: b"one".to_vec(),
        object_hash: gix_hash::Kind::Sha1,
    };
    let budget = CapacityBudget::new(1024 * 1024);
    let session = SnapshotSession::open(
        UnreadSource {
            identity: identity.clone(),
            calls: calls.clone(),
        },
        &identity,
        Control::new(Instant::now() + Duration::from_secs(30)),
        budget.clone(),
        BlameOptions::default().read_limits(),
    )
    .unwrap();
    let commit = ObjectId::from_hex(b"1111111111111111111111111111111111111111").unwrap();
    let error = session
        .blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .err();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "runtime rejection precedes even source headers"
    );
    assert_eq!(error, Some(BlameError::CodecRuntime(expected)));
    assert_eq!(
        BlameOptions::default().mapping_options(true).err(),
        Some(BlameError::CodecRuntime(expected)),
        "memo identities require runtime verification, not pure producer bytes"
    );
    struct NoCallbacks;
    impl git_rs_core::blame::memo::MappingMemo for NoCallbacks {
        fn load(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &mut [u8],
        ) -> Result<Option<usize>, BlameError> {
            panic!("unverified runtime reached a memory callback");
        }
        fn completed(
            &mut self,
            _: &git_rs_core::blame::memo::MappingKey,
            _: &[u8],
        ) -> Result<(), BlameError> {
            panic!("unverified runtime published a memory unit");
        }
    }
    assert_eq!(
        session
            .blame_complete_with_memo(
                BlameRequest {
                    commit: &commit,
                    path: b"file"
                },
                &BlameOptions::default(),
                git_rs_core::MemoryMappingRequest {
                    reuse: true,
                    memo: &mut NoCallbacks,
                    boundary: None,
                },
            )
            .err(),
        Some(BlameError::CodecRuntime(expected))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn matching_runtime_verifies_the_same_pure_producer_identity() {
    assert!(std::ptr::eq(
        ProducerVersion::verified().unwrap(),
        ProducerVersion::current()
    ));
    assert!(std::ptr::eq(
        ProducerVersion::verified().unwrap(),
        ProducerVersion::current()
    ));
}

#[test]
#[ignore = "fresh-process fixture; invoked by runtime codec tests"]
fn runtime_child() {
    match std::env::var("GIT_RS_CODEC_PROBE").unwrap().as_str() {
        "override" => rejected_before_source(CodecRuntimeError::Override),
        "search-path" => {
            assert!(std::ptr::eq(
                ProducerVersion::verified().unwrap(),
                ProducerVersion::current()
            ));
        }
        "changed" => rejected_before_source(CodecRuntimeError::Mismatch),
        "warm-changed" => {
            // Complete native fixture preparation before the first proof, as in
            // the oracle suites. Derivation below establishes the warm proof.
            let mut fixture = fixture::Fixture::new();
            let commit = fixture.commit(
                &[(b"file", b"one\n", "100644")],
                &[],
                1,
                b"warm runtime fixture\n",
                None,
            );
            let budget = CapacityBudget::new(64 * 1024 * 1024);
            let control = fixture::control();
            let session = fixture.session(budget.clone(), control.clone());
            let blame = session
                .blame_complete(
                    BlameRequest {
                        commit: &commit,
                        path: b"file",
                    },
                    &BlameOptions::default(),
                )
                .unwrap();
            std::fs::write(std::env::var_os("GIT_RS_CODEC_READY").unwrap(), b"ready").unwrap();
            let mut signal = [0];
            std::io::Read::read_exact(&mut std::io::stdin(), &mut signal).unwrap();
            rejected_before_source(CodecRuntimeError::Mismatch);
            let retained = budget.used();
            let error = git_rs_core::Porcelain::encode_complete(
                &blame,
                &control,
                &budget,
                git_rs_core::blame::MAX_BODY_BYTES,
            )
            .err();
            assert_eq!(
                error,
                Some(BlameError::CodecRuntime(CodecRuntimeError::Mismatch)),
                "a completed blame cannot be serialized under a changed codec runtime"
            );
            assert_eq!(
                budget.used(),
                retained,
                "rejected serialization retains no new output"
            );
            drop(blame);
            drop(session);
            assert_eq!(budget.used(), 0);
        }
        "mapped-changed" => {
            // The executable and libc are already mapped by exec, but no codec
            // proof exists yet. A byte-identical replacement defeats a mere
            // filesystem hash check; actual mapped inode validation must reject.
            std::fs::write(std::env::var_os("GIT_RS_CODEC_READY").unwrap(), b"ready").unwrap();
            let mut signal = [0];
            std::io::Read::read_exact(&mut std::io::stdin(), &mut signal).unwrap();
            rejected_before_source(CodecRuntimeError::Mismatch);
        }
        other => panic!("unknown codec probe {other}"),
    }
}

struct Probe(Child);
impl Probe {
    fn spawn(scenario: &str, ready: Option<&Path>, override_name: Option<&str>) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "runtime_child", "--ignored", "--nocapture"])
            .env("GIT_RS_CODEC_PROBE", scenario)
            .stdin(Stdio::piped());
        if let Some(ready) = ready {
            command.env("GIT_RS_CODEC_READY", ready);
        }
        if let Some(name) = override_name {
            command.env(name, "/nonexistent-synthetic-codec-path");
        }
        Self(command.spawn().unwrap())
    }
    fn success(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "runtime probe {status}");
                return;
            }
            assert!(Instant::now() < deadline, "runtime probe timeout");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn gconv_path_and_loader_overrides_fail_before_any_core_source_access() {
    for name in ["GCONV_PATH", "LD_PRELOAD"] {
        Probe::spawn("override", None, Some(name)).success();
    }
}

#[test]
fn cargo_library_search_path_keeps_effective_runtime_validation_enabled() {
    Probe::spawn("search-path", None, Some("LD_LIBRARY_PATH")).success();
}

// This ignored probe mutates only an explicitly owned disposable container's
// codec file, never a developer host. Run this target alone with one test thread.
#[test]
#[ignore = "requires explicitly owned disposable root container; changes and restores its TSCII module"]
fn changed_module_rejects_cold_and_already_verified_processes() {
    assert_eq!(
        std::env::var("GIT_RS_OWNED_CODEC_CONTAINER").as_deref(),
        Ok("1")
    );
    assert!(
        Path::new("/.dockerenv").is_file(),
        "never mutate host codec files"
    );
    struct Restore {
        original: PathBuf,
        bytes: Vec<u8>,
        ready: PathBuf,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            std::fs::write(&self.original, &self.bytes).expect("restore original codec module");
            let _ = std::fs::remove_file(&self.ready);
        }
    }
    let original = PathBuf::from("/usr/lib/x86_64-linux-gnu/gconv/TSCII.so");
    let ready = std::env::temp_dir().join(format!("codec-ready-{}", std::process::id()));
    assert!(!ready.exists());
    let mut child = Probe::spawn("warm-changed", Some(&ready), None);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "warm verifier exited before barrier"
        );
        assert!(Instant::now() < deadline, "warm verifier barrier timeout");
        std::thread::sleep(Duration::from_millis(5));
    }
    let restore = Restore {
        original: original.clone(),
        bytes: std::fs::read(&original).unwrap(),
        ready,
    };
    // Keep valid native code and the mapped inode/path intact, so cold hashing
    // and warm freshness (not incidental dlopen failure) must reject the drift.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&original)
        .unwrap()
        .write_all(b"synthetic-core-runtime-drift")
        .unwrap();
    Probe::spawn("changed", None, None).success();
    child.0.stdin.take().unwrap().write_all(b"x").unwrap();
    child.success();
    drop(restore);
}

#[test]
#[ignore = "requires explicitly owned disposable root container; replaces/restores libc with identical bytes"]
fn loaded_libc_must_match_the_hashed_file_inode_not_only_its_bytes() {
    assert_eq!(
        std::env::var("GIT_RS_OWNED_CODEC_CONTAINER").as_deref(),
        Ok("1")
    );
    assert!(
        Path::new("/.dockerenv").is_file(),
        "never replace host native libraries"
    );
    let original = PathBuf::from("/usr/lib/x86_64-linux-gnu/libc.so.6");
    // The backup is outside the fixed qualified file paths, so every hashed path
    // and byte remains identical after replacement. Only the mapped inode differs.
    let backup = original.with_file_name(format!("codec-libc-backup-{}", std::process::id()));
    let ready = std::env::temp_dir().join(format!("codec-mapped-ready-{}", std::process::id()));
    assert!(!backup.exists());
    assert!(!ready.exists());
    let mut child = Probe::spawn("mapped-changed", Some(&ready), None);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "mapped verifier exited before barrier"
        );
        assert!(Instant::now() < deadline, "mapped verifier barrier timeout");
        std::thread::sleep(Duration::from_millis(5));
    }
    let bytes = std::fs::read(&original).unwrap();
    let permissions = std::fs::metadata(&original).unwrap().permissions();
    struct Restore {
        original: PathBuf,
        backup: PathBuf,
        ready: PathBuf,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            std::fs::rename(&self.backup, &self.original).expect("restore original libc inode");
            let _ = std::fs::remove_file(&self.ready);
        }
    }
    std::fs::rename(&original, &backup).unwrap();
    let restore = Restore {
        original: original.clone(),
        backup,
        ready,
    };
    std::fs::write(&original, bytes).unwrap();
    std::fs::set_permissions(&original, permissions).unwrap();
    child.0.stdin.take().unwrap().write_all(b"x").unwrap();
    child.success();
    drop(restore);
}
