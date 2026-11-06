#![forbid(unsafe_code)]
use git_rs::{
    auth,
    cache::Cache,
    http::Request,
    protocol::{self, Oid, Update},
};
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

fn fill(mut file: &File, bytes: &[u8]) -> git_rs::Result<()> {
    file.write_all(bytes)?;
    Ok(())
}

#[test]
fn packet_framing_distinguishes_flush_truncation_and_invalid_length() {
    assert_eq!(
        protocol::read_packet(&mut b"0008test".as_slice()).unwrap(),
        Some(b"test".to_vec())
    );
    assert_eq!(
        protocol::read_packet(&mut b"0000".as_slice()).unwrap(),
        None
    );
    for input in [b"0003".as_slice(), b"ffff", b"0008tes", b"zzzz", b""] {
        assert!(protocol::read_packet(&mut &*input).is_err(), "{input:?}");
    }
    let mut wire = Vec::new();
    protocol::write_packet(&mut wire, b"abc\0def").unwrap();
    protocol::flush(&mut wire).unwrap();
    assert_eq!(wire, b"000babc\0def0000");
}

#[test]
fn ref_commands_preserve_non_utf8_bytes_and_reject_duplicates() {
    let mut line = format!("{} {} refs/heads/", "0".repeat(40), "1".repeat(40)).into_bytes();
    line.push(255);
    let command = Update::parse(&line).unwrap();
    assert_eq!(command.old, Oid::ZERO);
    assert_eq!(command.name.last(), Some(&255));
    assert_eq!(command.line(), line);
    let mut wire = Vec::new();
    protocol::write_packet(&mut wire, &line).unwrap();
    protocol::write_packet(&mut wire, &line).unwrap();
    protocol::flush(&mut wire).unwrap();
    assert!(protocol::read_updates(&mut wire.as_slice()).is_err());
    assert!(Oid::parse(b"1234").is_err());
}

#[test]
fn ssh_contract_has_no_shell_or_path_escape() {
    let (service, slug) = auth::ssh_command("git-upload-pack 'team/project.git'").unwrap();
    assert_eq!(service, auth::Service::Upload);
    assert_eq!(slug, "team/project");
    for value in [
        "sh -c 'id'",
        "git-upload-pack '../project.git'",
        "git-upload-pack 'team/project.git';id",
        "git-receive-pack '--upload-pack=id.git'",
        "git-upload-pack '/absolute.git'",
        "git-upload-pack 'team/a%2fb.git'",
    ] {
        assert!(auth::ssh_command(value).is_err(), "{value}");
    }
}

#[test]
fn authorized_key_mapping_is_exact_and_always_restricted() {
    let generate = |mapping: &str, key: &str| {
        auth::authorized_key(
            mapping,
            "ssh-ed25519",
            key,
            "/usr/local/bin/git-rs",
            "/run/git-rs/private/ssh.sock",
        )
    };
    let mapping = "# Server-managed identities\nalice ssh-ed25519 AAAABBBB= arbitrary comment\nbob ssh-ed25519 CCCCDDDD=\n";
    assert_eq!(
        generate(mapping, "AAAABBBB=").unwrap().unwrap(),
        "restrict,command=\"/usr/local/bin/git-rs ssh --socket /run/git-rs/private/ssh.sock --principal alice\" ssh-ed25519 AAAABBBB=\n"
    );
    assert!(generate(mapping, "UNKNOWN=").unwrap().is_none());
    assert!(generate("alice ssh-ed25519 AAAA\nbob ssh-ed25519 AAAA", "AAAA").is_err());
    assert!(generate("alice;id ssh-ed25519 AAAA", "AAAA").is_err());
    assert!(generate(mapping, "AAAA\ncommand=x").is_err());
    assert!(
        auth::authorized_key(
            mapping,
            "ssh-ed25519",
            "AAAABBBB=",
            "/bin/git-rs",
            "/tmp/a;id"
        )
        .is_err()
    );
}

#[test]
fn http_streams_chunked_binary_and_rejects_ambiguous_framing() {
    let wire=b"POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-git-receive-pack-request\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na\0b\r\n2\r\ncd\r\n0\r\n\r\n";
    let mut input = Cursor::new(wire);
    let request = Request::parse(&mut input).unwrap();
    let mut body = Vec::new();
    request.body(&mut input).read_to_end(&mut body).unwrap();
    assert_eq!(body, b"a\0bcd");
    for extra in [
        "Content-Length: 1\r\nTransfer-Encoding: chunked\r\n",
        "Content-Length: 1\r\nContent-Length: 1\r\n",
        "Content-Length: -1\r\n",
        "Transfer-Encoding: gzip, chunked\r\n",
        "Expect: 100-continue\r\n",
    ] {
        let wire = format!(
            "POST /team/project.git/git-receive-pack HTTP/1.1\r\nContent-Type: application/x-git-receive-pack-request\r\n{extra}\r\n"
        );
        assert!(Request::parse(&mut Cursor::new(wire)).is_err(), "{extra}");
    }
    for payload in [
        b"5\r\nabc".as_slice(),
        b"1\r\nx\r\n0\r\nbad\r\n",
        b"100000001\r\n",
    ] {
        let wire=b"POST /team/project.git/git-receive-pack HTTP/1.1\r\nContent-Type: application/x-git-receive-pack-request\r\nTransfer-Encoding: chunked\r\n\r\n";
        let mut input = Cursor::new([wire.as_slice(), payload].concat());
        let request = Request::parse(&mut input).unwrap();
        assert!(
            request
                .body(&mut input)
                .read_to_end(&mut Vec::new())
                .is_err()
        );
    }
}

#[test]
fn eviction_does_not_forget_pinned_memory() {
    let cache = Cache::new(4096, 8);
    let pinned = cache
        .get_or_load("repo-a:pack", 8, |file| fill(file, b"12345678"))
        .unwrap()
        .unwrap();
    let bypass = cache
        .get_or_load("repo-b:pack", 8, |_| {
            panic!("pinned memory must force bypass")
        })
        .unwrap();
    assert!(bypass.is_none());
    assert_eq!(cache.allocated(), 4096);
    assert_eq!(fs::read(pinned.path()).unwrap(), b"12345678");
    drop(pinned);
    assert_eq!(cache.allocated(), 0);
    let next = cache
        .get_or_load("repo-b:pack", 8, |file| fill(file, b"abcdefgh"))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(next.path()).unwrap(), b"abcdefgh");
    assert_eq!(cache.allocated(), 4096);
}

#[test]
fn concurrent_misses_share_one_complete_immutable_load() {
    let cache = Arc::new(Cache::new(4096, 4096));
    let start = Arc::new(Barrier::new(16));
    let loads = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let start = start.clone();
            let loads = loads.clone();
            workers.push(scope.spawn(move || {
                start.wait();
                cache
                    .get_or_load("repo:immutable:digest", 4096, |file| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        fill(file, &[73; 4096])
                    })
                    .unwrap()
                    .unwrap()
            }));
        }
        let buffers: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        for buffer in &buffers {
            assert!(Arc::ptr_eq(&buffers[0], buffer));
            assert_eq!(fs::read(buffer.path()).unwrap(), [73; 4096]);
        }
    });
    assert_eq!(cache.allocated(), 4096);
}

#[test]
fn failed_load_releases_reservation_and_does_not_poison_retry() {
    let cache = Cache::new(4096, 8);
    assert!(
        cache
            .get_or_load("key", 8, |_| Err(git_rs::invalid("injected read failure")))
            .is_err()
    );
    assert_eq!(cache.allocated(), 0);
    let result = cache
        .get_or_load("key", 8, |file| fill(file, &[9; 8]))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(result.path()).unwrap(), [9; 8]);
    assert!(
        Cache::new(0, 8)
            .get_or_load("key", 8, |_| panic!("disabled cache must bypass"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn shared_cache_files_are_sealed_and_reopened_without_a_copy() {
    use memfd::{FileSeal, Memfd};
    let cache = Cache::new(8192, 8);
    let entry = cache
        .get_or_load("repo:shared", 8, |file| fill(file, b"abcdefgh"))
        .unwrap()
        .unwrap();
    let hit = cache
        .get_or_load("repo:shared", 8, |_| {
            panic!("cached bytes must not be reloaded")
        })
        .unwrap()
        .unwrap();
    let mut left = File::open(entry.path()).unwrap();
    let mut right = File::open(hit.path()).unwrap();
    let mut sample = [0; 3];
    left.read_exact(&mut sample).unwrap();
    assert_eq!(&sample, b"abc");
    right.read_exact(&mut sample).unwrap();
    assert_eq!(
        &sample, b"abc",
        "each Git reader needs an independent offset"
    );
    assert_eq!(
        left.metadata().unwrap().ino(),
        right.metadata().unwrap().ino()
    );
    assert_eq!(
        left.metadata().unwrap().dev(),
        right.metadata().unwrap().dev()
    );
    let memory = Memfd::try_from_file(left).unwrap();
    let seals = memory.seals().unwrap();
    for seal in [
        FileSeal::SealWrite,
        FileSeal::SealGrow,
        FileSeal::SealShrink,
        FileSeal::SealSeal,
    ] {
        assert!(seals.contains(&seal), "shared bytes require {seal:?}");
    }
    // Reopening the parent descriptor after exec must work without inheriting
    // unrelated serving-process file descriptors into native Git.
    let output = std::process::Command::new("/usr/bin/dd")
        .arg(format!("if={}", hit.path().display()))
        .args(["bs=8", "count=1", "status=none"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"abcdefgh");
    assert_eq!(cache.allocated(), 4096);
}

#[test]
fn cache_fill_is_size_bounded_and_short_data_is_never_published() {
    let cache = Cache::new(4096, 8);
    assert!(
        cache
            .get_or_load("short", 8, |file| {
                assert!(
                    file.set_len(0).is_err(),
                    "a fill must not shrink its reservation"
                );
                assert!(
                    file.set_len(9).is_err(),
                    "a fill must not grow its reservation"
                );
                fill(file, b"short")
            })
            .is_err()
    );
    assert_eq!(cache.allocated(), 0);
    assert!(
        cache
            .get_or_load("long", 8, |file| fill(file, b"123456789"))
            .is_err()
    );
    assert_eq!(cache.allocated(), 0);
    let complete = cache
        .get_or_load("short", 8, |file| fill(file, b"complete"))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(complete.path()).unwrap(), b"complete");
    assert_eq!(cache.allocated(), 4096);
}

#[test]
fn cache_does_not_reuse_a_key_with_a_different_expected_size() {
    let cache = Cache::new(8192, 8);
    let entry = cache
        .get_or_load("same-key", 8, |file| fill(file, b"complete"))
        .unwrap()
        .unwrap();
    assert!(
        cache
            .get_or_load("same-key", 7, |_| panic!(
                "identity mismatch is not a refill"
            ))
            .is_err()
    );
    assert_eq!(fs::read(entry.path()).unwrap(), b"complete");
}

#[test]
fn pinned_cache_files_exhaust_the_descriptor_budget_before_the_byte_budget() {
    let cache = Cache::new(2 * 1024 * 1024, 8);
    let mut pinned = Vec::new();
    for number in 0..256 {
        pinned.push(
            cache
                .get_or_load(&format!("key-{number}"), 8, |file| fill(file, b"complete"))
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(cache.allocated(), 1024 * 1024);
    assert!(
        cache
            .get_or_load("overflow", 8, |_| panic!(
                "pinned descriptors must force bypass"
            ))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        cache.allocated(),
        1024 * 1024,
        "eviction must retain pinned reservations"
    );
    pinned.pop();
    let retry = cache
        .get_or_load("overflow", 8, |file| fill(file, b"released"))
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(retry.path()).unwrap(), b"released");
    assert_eq!(fs::read(pinned[0].path()).unwrap(), b"complete");
    assert_eq!(cache.allocated(), 1024 * 1024);
}

#[test]
fn waiting_for_another_cache_fill_obeys_the_request_deadline() {
    use std::sync::mpsc;
    use std::time::Duration;
    let cache = Cache::new(4096, 8);
    let (started, filling) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, result) = mpsc::channel();
    std::thread::scope(|scope| {
        let cache = &cache;
        let loader = scope.spawn(move || {
            cache.get_or_load("shared", 8, |file| {
                started.send(()).unwrap();
                released.recv().unwrap();
                fill(file, b"complete")
            })
        });
        filling.recv().unwrap();
        scope.spawn(move || {
            let operation = git_rs::process::Operation::new(1);
            let timed_out = operation.enter(|| {
                cache
                    .get_or_load("shared", 8, |_| {
                        panic!("waiting must not start a second fill")
                    })
                    .is_err()
            });
            finished.send(timed_out).unwrap();
        });
        let timed_out = result.recv_timeout(Duration::from_secs(3));
        release.send(()).unwrap();
        let entry = loader.join().unwrap().unwrap().unwrap();
        assert!(
            timed_out.unwrap(),
            "the waiter must time out while the loader is still blocked"
        );
        assert_eq!(fs::read(entry.path()).unwrap(), b"complete");
    });
}

#[test]
fn reused_native_scratch_template_excludes_request_head_and_hooks() {
    use git_rs::{hooks, process, scratch::Scratch};
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::path::PathBuf;

    struct Root(PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    let path = std::env::temp_dir().join(format!(
        "git-rs-template-{}",
        git_rs::scratch::identifier().unwrap()
    ));
    fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
    let root = Root(path);
    let first = Scratch::new(&root.0).unwrap();
    let changed = process::git()
        .arg("-C")
        .arg(&first.path)
        .args(["symbolic-ref", "HEAD"])
        .arg(OsStr::from_bytes(b"refs/heads/\xff"))
        .output()
        .unwrap();
    assert!(
        changed.status.success(),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    hooks::install(
        &first.path,
        &root.0.join("unused.sock"),
        "synthetic-nonce",
        &mut process::git(),
    )
    .unwrap();
    let first_path = first.path.clone();
    let second = Scratch::new(&root.0).unwrap();
    assert_ne!(first.path, second.path);
    assert_eq!(
        fs::read(first.path.join("HEAD")).unwrap(),
        b"ref: refs/heads/\xff\n"
    );
    assert_eq!(
        fs::read(second.path.join("HEAD")).unwrap(),
        b"ref: refs/heads/main\n"
    );
    assert!(!second.path.join("hooks").exists());
    assert_eq!(
        fs::metadata(&second.path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    drop(first);
    assert!(!first_path.exists());
    assert_eq!(
        process::capture(
            process::git()
                .arg("-C")
                .arg(&second.path)
                .args(["rev-parse", "--is-bare-repository"]),
            b"",
            4096,
        )
        .unwrap(),
        b"true\n"
    );
}

#[test]
fn ssh_transport_requires_explicit_success_after_complete_payload() {
    use std::io::Write;
    let mut wire = Vec::new();
    let mut framed = protocol::Framed(&mut wire);
    framed.write_all(b"0008PACK\0binary").unwrap();
    framed.finish(true).unwrap();
    let mut output = Vec::new();
    protocol::receive_framed(&mut wire.as_slice(), &mut output).unwrap();
    assert_eq!(output, b"0008PACK\0binary");
    for broken in [&wire[..wire.len() - 1], &wire[..wire.len() - 5]] {
        assert!(protocol::receive_framed(&mut &*broken, &mut Vec::new()).is_err());
    }
    let mut rejected = Vec::new();
    protocol::Framed(&mut rejected).finish(false).unwrap();
    assert!(protocol::receive_framed(&mut rejected.as_slice(), &mut Vec::new()).is_err());
}

#[test]
fn interactive_input_reaches_the_child_before_client_eof() {
    use std::io::Write;
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    let request = b"0014command=ls-refs\n\x000001\0binary-request-fields\n0000";
    let (mut client, mut input) = UnixStream::pair().unwrap();
    let mut child = Command::new("/usr/bin/dd")
        .args(["bs=1", &format!("count={}", request.len()), "status=none"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    let forwarding =
        std::thread::spawn(move || git_rs::process::forward_input(&mut input, &mut stdin));
    let response = std::thread::spawn(move || {
        let mut echoed = vec![0; request.len()];
        let result = stdout.read_exact(&mut echoed).map(|()| echoed);
        let _ = sent.send(result);
    });

    client.write_all(request).unwrap();
    let result = received.recv_timeout(Duration::from_secs(3));
    // Only close input after the response deadline. Interactive Git waits for a
    // response before sending another request or EOF.
    client.shutdown(Shutdown::Write).unwrap();
    let copied = forwarding.join().unwrap().unwrap();
    response.join().unwrap();
    let status = child.wait().unwrap();
    assert_eq!(
        result
            .expect("child must respond before client EOF")
            .unwrap(),
        request
    );
    assert_eq!(copied, request.len() as u64);
    assert!(status.success());
}
