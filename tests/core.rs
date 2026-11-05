#![forbid(unsafe_code)]
use git_rs::{
    auth,
    cache::Cache,
    http::Request,
    protocol::{self, Oid, Update},
};
use std::io::{Cursor, Read};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

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
    let cache = Cache::new(8, 8);
    let pinned = cache
        .get_or_load("repo-a:pack", 8, |bytes| {
            bytes.copy_from_slice(b"12345678");
            Ok(())
        })
        .unwrap()
        .unwrap();
    let bypass = cache
        .get_or_load("repo-b:pack", 8, |_| {
            panic!("pinned memory must force bypass")
        })
        .unwrap();
    assert!(bypass.is_none());
    assert_eq!(cache.allocated(), 8);
    assert_eq!(pinned.as_ref().as_ref(), b"12345678");
    drop(pinned);
    assert_eq!(cache.allocated(), 0);
    let next = cache
        .get_or_load("repo-b:pack", 8, |bytes| {
            bytes.copy_from_slice(b"abcdefgh");
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(next.as_ref().as_ref(), b"abcdefgh");
    assert_eq!(cache.allocated(), 8);
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
                    .get_or_load("repo:immutable:digest", 4096, |bytes| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        bytes.fill(73);
                        Ok(())
                    })
                    .unwrap()
                    .unwrap()
            }));
        }
        let buffers: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        for buffer in &buffers {
            assert!(Arc::ptr_eq(&buffers[0], buffer));
            assert_eq!(buffer.as_ref().as_ref(), [73; 4096]);
        }
    });
    assert_eq!(cache.allocated(), 4096);
}

#[test]
fn failed_load_releases_reservation_and_does_not_poison_retry() {
    let cache = Cache::new(8, 8);
    assert!(
        cache
            .get_or_load("key", 8, |_| Err(git_rs::invalid("injected read failure")))
            .is_err()
    );
    assert_eq!(cache.allocated(), 0);
    let result = cache
        .get_or_load("key", 8, |bytes| {
            bytes.fill(9);
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(result.as_ref().as_ref(), [9; 8]);
    assert!(
        Cache::new(0, 8)
            .get_or_load("key", 8, |_| panic!("disabled cache must bypass"))
            .unwrap()
            .is_none()
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
