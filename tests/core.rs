#![forbid(unsafe_code)]
use git_rs::{
    auth,
    cache::Cache,
    http::Request,
    protocol::{self, Oid, Update},
};
use std::io::{Cursor, Read, Write};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

fn fill(output: &mut [u8], bytes: &[u8]) -> git_rs::Result<()> {
    if output.len() != bytes.len() {
        return Err(git_rs::invalid("incomplete synthetic range"));
    }
    output.copy_from_slice(bytes);
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
    let wire = b"POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-git-receive-pack-request\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na\0b\r\n2\r\ncd\r\n0\r\n\r\n";
    let mut input = Cursor::new(wire);
    let request = Request::parse(&mut input).unwrap();
    let mut body = Vec::new();
    request.body(&mut input).read_to_end(&mut body).unwrap();
    assert_eq!(body, b"a\0bcd");
    let headers = &wire[..wire
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4];
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
        let mut input = Cursor::new([headers, payload].concat());
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
    let cache = Cache::new(4096, 2048);
    let pinned = cache
        .get_or_load("repo-a:pack", 2048, |out| fill(out, &[1; 2048]))
        .unwrap()
        .unwrap();
    let allocated = cache.allocated();
    assert!((2048..=4096).contains(&allocated));
    assert!(
        cache
            .get_or_load("repo-b:pack", 2048, |_| panic!(
                "pinned memory must force bypass"
            ))
            .unwrap()
            .is_none()
    );
    assert_eq!(cache.allocated(), allocated);
    assert_eq!(&pinned[..], &[1; 2048]);
    drop(pinned);
    assert_eq!(
        cache.allocated(),
        allocated,
        "the cache still owns the entry"
    );
    let next = cache
        .get_or_load("repo-b:pack", 2048, |out| fill(out, &[2; 2048]))
        .unwrap()
        .unwrap();
    assert_eq!(&next[..], &[2; 2048]);
    assert!(cache.allocated() <= 4096);
}

#[test]
fn cache_pressure_keeps_pinned_entries_reusable() {
    let cache = Cache::new(8192, 3072);
    let pinned = cache
        .get_or_load("pinned", 3072, |out| fill(out, &[1; 3072]))
        .unwrap()
        .unwrap();
    drop(
        cache
            .get_or_load("evictable", 3072, |out| fill(out, &[2; 3072]))
            .unwrap()
            .unwrap(),
    );
    let replacement = cache
        .get_or_load("replacement", 3072, |out| fill(out, &[3; 3072]))
        .unwrap()
        .unwrap();
    let hit = cache
        .get_or_load("pinned", 3072, |_| {
            panic!("a live pin must remain reusable")
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&pinned, &hit));
    assert_eq!(&hit[..], &[1; 3072]);
    assert_eq!(&replacement[..], &[3; 3072]);
    let allocated = cache.allocated();
    assert!(allocated <= 8192);
    assert!(
        cache
            .get_or_load("overflow", 3072, |_| panic!("all pinned must bypass"))
            .unwrap()
            .is_none()
    );
    let after = cache
        .get_or_load("replacement", 3072, |_| panic!("bypass must preserve pins"))
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&replacement, &after));
    assert_eq!(cache.allocated(), allocated);
}

#[test]
fn cache_slot_pressure_replaces_cold_entries_without_byte_pressure() {
    let cache = Cache::new(1024 * 1024, 8);
    let pinned = cache
        .get_or_load("key-0", 8, |out| fill(out, b"pinned00"))
        .unwrap()
        .unwrap();
    for number in 1..256 {
        drop(
            cache
                .get_or_load(&format!("key-{number}"), 8, |out| fill(out, b"colddata"))
                .unwrap()
                .unwrap(),
        );
    }
    let loads = AtomicUsize::new(0);
    for _ in 0..2 {
        let next = cache
            .get_or_load("new-hot-key", 8, |out| {
                loads.fetch_add(1, Ordering::SeqCst);
                fill(out, b"hot-data")
            })
            .unwrap()
            .unwrap();
        assert_eq!(&next[..], b"hot-data");
    }
    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "a full slot table must replace an unpinned cold entry"
    );
    let hit = cache
        .get_or_load("key-0", 8, |_| {
            panic!("slot pressure must preserve live pins")
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&pinned, &hit));
    let mut reloaded = false;
    drop(
        cache
            .get_or_load("key-1", 8, |out| {
                reloaded = true;
                fill(out, b"colddata")
            })
            .unwrap()
            .unwrap(),
    );
    assert!(
        reloaded,
        "the oldest unpinned entry is replaced before newer entries"
    );
    assert!(
        cache.allocated() < 1024 * 1024 / 2,
        "this is slot pressure, not byte pressure"
    );
}

#[test]
fn cache_slot_replacement_waits_for_a_reader_pin_to_be_released() {
    let cache = Cache::new(1024 * 1024, 8);
    let mut pins = Vec::new();
    for number in 0..256 {
        pins.push(
            cache
                .get_or_load(&format!("key-{number}"), 8, |out| fill(out, b"pinned00"))
                .unwrap()
                .unwrap(),
        );
    }
    let private = cache
        .get_or_load("new", 8, |out| fill(out, b"new-data"))
        .unwrap()
        .unwrap();
    for number in [0, 127, 255] {
        let hit = cache
            .get_or_load(&format!("key-{number}"), 8, |_| {
                panic!("all live pins remain reusable")
            })
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&hit, &pins[number]));
    }
    drop(pins.pop());
    let cached = cache
        .get_or_load("new", 8, |out| fill(out, b"new-data"))
        .unwrap()
        .unwrap();
    assert!(
        !Arc::ptr_eq(&private, &cached),
        "the all-pinned fill was private"
    );
    let hit = cache
        .get_or_load("new", 8, |_| {
            panic!("released slot must accept the new key")
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&cached, &hit));
    assert_eq!(&private[..], b"new-data");
}

#[test]
fn concurrent_misses_share_one_complete_immutable_load() {
    let cache = Arc::new(Cache::new(8192, 4096));
    let start = Arc::new(Barrier::new(16));
    let loads = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..16 {
            let (cache, start, loads) = (cache.clone(), start.clone(), loads.clone());
            workers.push(scope.spawn(move || {
                start.wait();
                cache
                    .get_or_load("repo:immutable:digest", 4096, |out| {
                        loads.fetch_add(1, Ordering::SeqCst);
                        fill(out, &[73; 4096])
                    })
                    .unwrap()
                    .unwrap()
            }));
        }
        let buffers: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        for buffer in &buffers {
            assert!(Arc::ptr_eq(&buffers[0], buffer));
            assert_eq!(&buffer[..], &[73; 4096]);
        }
    });
    assert!((4096..=8192).contains(&cache.allocated()));
}

#[test]
fn failed_load_releases_reservation_and_does_not_poison_retry() {
    let cache = Cache::new(4096, 8);
    assert!(
        cache
            .get_or_load("key", 8, |out| {
                out[..3].copy_from_slice(b"bad");
                Err(git_rs::invalid("injected read failure"))
            })
            .is_err()
    );
    assert_eq!(cache.allocated(), 0);
    let result = cache
        .get_or_load("key", 8, |out| fill(out, &[9; 8]))
        .unwrap()
        .unwrap();
    assert_eq!(&result[..], &[9; 8]);
    assert!(
        Cache::new(0, 8)
            .get_or_load("key", 8, |_| panic!("disabled cache must bypass"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn source_invalidation_forgets_lookups_without_refunding_live_pins() {
    let cache = Cache::new(8192, 512);
    let key = "repo-a:pack:digest:0:512";
    let pinned = cache
        .get_or_load(key, 512, |out| fill(out, &[7; 512]))
        .unwrap()
        .unwrap();
    let unrelated = cache
        .get_or_load("repo-b:pack:digest:0:512", 512, |out| fill(out, &[9; 512]))
        .unwrap()
        .unwrap();
    let charged = cache.allocated();
    cache.invalidate_prefix("repo-a:pack:");
    assert_eq!(
        cache.allocated(),
        charged,
        "a discarded lookup cannot refund the reader's backing"
    );
    assert_eq!(&pinned[..], &[7; 512]);
    let unaffected = cache
        .get_or_load("repo-b:pack:digest:0:512", 512, |_| {
            panic!("unrelated source must stay cached")
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&unrelated, &unaffected));
    let replacement = cache
        .get_or_load(key, 512, |out| fill(out, &[7; 512]))
        .unwrap()
        .unwrap();
    assert!(
        !Arc::ptr_eq(&pinned, &replacement),
        "rejected source must be reacquired"
    );
    assert!(cache.allocated() > charged);
    cache.invalidate_prefix("");
    drop((pinned, replacement, unrelated, unaffected));
    assert_eq!(cache.allocated(), 0);
}

#[test]
fn a_fill_crossing_source_invalidation_cannot_reinstall_its_lookup() {
    use std::{sync::mpsc, time::Duration};
    let cache = Cache::new(8192, 8);
    let (started, filling) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (invalidated, observed) = mpsc::channel();
    std::thread::scope(|scope| {
        let cache = &cache;
        let loader = scope.spawn(move || {
            cache.get_or_load("repo:pack:range", 8, |out| {
                started.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(5)).unwrap();
                fill(out, b"complete")
            })
        });
        filling.recv_timeout(Duration::from_secs(3)).unwrap();
        let invalidation = scope.spawn(move || {
            cache.invalidate_prefix("repo:pack:");
            invalidated.send(()).unwrap();
        });
        let bounded = observed.recv_timeout(Duration::from_secs(3));
        release.send(()).unwrap();
        let old = loader.join().unwrap().unwrap().unwrap();
        invalidation.join().unwrap();
        bounded.expect("invalidation must not wait for the blocked source loader");
        let fresh = cache
            .get_or_load("repo:pack:range", 8, |out| fill(out, b"complete"))
            .unwrap()
            .unwrap();
        assert!(
            !Arc::ptr_eq(&old, &fresh),
            "an invalidated in-flight fill must stay private"
        );
        assert_eq!(&old[..], b"complete");
        assert_eq!(&fresh[..], b"complete");
    });
}

#[test]
fn shared_cache_bytes_have_independent_reader_offsets_without_a_copy() {
    let cache = Cache::new(8192, 8);
    let entry = cache
        .get_or_load("repo:shared", 8, |out| fill(out, b"abcdefgh"))
        .unwrap()
        .unwrap();
    let hit = cache
        .get_or_load("repo:shared", 8, |_| {
            panic!("cached bytes must not be reloaded")
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&entry, &hit));
    let mut left = Cursor::new(&entry[..]);
    let mut right = Cursor::new(&hit[..]);
    assert_eq!(left.get_ref().as_ptr(), right.get_ref().as_ptr());
    let mut sample = [0; 3];
    left.read_exact(&mut sample).unwrap();
    assert_eq!(&sample, b"abc");
    left.read_exact(&mut sample).unwrap();
    assert_eq!(&sample, b"def");
    right.read_exact(&mut sample).unwrap();
    assert_eq!(&sample, b"abc");
    drop(cache);
    assert_eq!(
        &entry[..],
        b"abcdefgh",
        "reader pins own their backing after cache release"
    );
}

#[test]
fn cache_fill_is_size_bounded_and_incomplete_ranges_are_never_published() {
    let cache = Cache::new(4096, 8);
    for data in [b"short".as_slice(), b"123456789"] {
        assert!(
            cache
                .get_or_load("range", 8, |out| {
                    assert_eq!(
                        out.len(),
                        8,
                        "loader cannot grow or shrink its admitted slice"
                    );
                    std::io::Read::read_exact(&mut &*data, out)?;
                    if data.len() != out.len() {
                        return Err(git_rs::invalid("range length changed"));
                    }
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(cache.allocated(), 0);
    }
    let complete = cache
        .get_or_load("range", 8, |out| fill(out, b"complete"))
        .unwrap()
        .unwrap();
    assert_eq!(&complete[..], b"complete");
}

#[test]
fn cache_does_not_reuse_a_key_with_a_different_expected_size() {
    let cache = Cache::new(8192, 8);
    let entry = cache
        .get_or_load("same-key", 8, |out| fill(out, b"complete"))
        .unwrap()
        .unwrap();
    assert!(
        cache
            .get_or_load("same-key", 7, |_| panic!(
                "identity mismatch is not a refill"
            ))
            .is_err()
    );
    assert_eq!(&entry[..], b"complete");
}

#[test]
fn pinned_cache_backing_exhausts_capacity_and_released_pins_allow_retry() {
    const CAPACITY: usize = 2 * 1024 * 1024;
    const ENTRY: usize = 4096;
    let cache = Cache::new(CAPACITY, ENTRY);
    let mut pinned = Vec::new();
    let mut bypassed = false;
    // Each pin owns at least ENTRY bytes. Metadata may force bypass sooner.
    for number in 0..=CAPACITY / ENTRY {
        let value = cache
            .get_or_load(&format!("key-{number}"), ENTRY, |out| {
                fill(out, &[7; ENTRY])
            })
            .unwrap();
        assert!(cache.allocated() <= CAPACITY);
        if let Some(value) = value {
            pinned.push(value);
        } else {
            bypassed = true;
            break;
        }
    }
    assert!(
        bypassed,
        "live pins cannot exceed the byte budget even after the lookup table is full"
    );
    assert!(!pinned.is_empty());
    let allocated = cache.allocated();
    assert!(
        cache
            .get_or_load("overflow", ENTRY, |_| panic!(
                "live backing must force bypass"
            ))
            .unwrap()
            .is_none()
    );
    assert_eq!(cache.allocated(), allocated);
    drop(pinned.pop());
    let retry = cache
        .get_or_load("overflow", ENTRY, |out| fill(out, &[8; ENTRY]))
        .unwrap()
        .unwrap();
    assert_eq!(&retry[..], &[8; ENTRY]);
    assert_eq!(&pinned[0][..], &[7; ENTRY]);
    assert!(cache.allocated() <= CAPACITY);
}

#[test]
fn waiting_for_another_cache_fill_obeys_the_request_deadline() {
    use std::{sync::mpsc, time::Duration};
    let cache = Cache::new(4096, 8);
    let (started, filling) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (finished, result) = mpsc::channel();
    std::thread::scope(|scope| {
        let cache = &cache;
        let loader = scope.spawn(move || {
            cache.get_or_load("shared", 8, |out| {
                started.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(5)).unwrap();
                fill(out, b"complete")
            })
        });
        filling.recv_timeout(Duration::from_secs(3)).unwrap();
        scope.spawn(move || {
            let timed_out = git_rs::process::Operation::new(1).enter(|| {
                cache
                    .get_or_load("shared", 8, |_| {
                        panic!("waiting must not start another fill")
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
            "the waiter must time out before the loader is released"
        );
        assert_eq!(&entry[..], b"complete");
    });
}

#[test]
fn immutable_source_keys_isolate_repository_and_catalog_generations() {
    let cache = Cache::new(8192, 8);
    for (key, value) in [
        ("repo-a:g1:pack", b"aaaaaaaa"),
        ("repo-a:g2:pack", b"bbbbbbbb"),
        ("repo-b:g1:pack", b"cccccccc"),
    ] {
        let pin = cache
            .get_or_load(key, 8, |out| fill(out, value))
            .unwrap()
            .unwrap();
        assert_eq!(&pin[..], value);
    }
    let old = cache
        .get_or_load("repo-a:g1:pack", 8, |_| {
            panic!("other identities must not replace this entry")
        })
        .unwrap()
        .unwrap();
    assert_eq!(&old[..], b"aaaaaaaa");
}

#[test]
fn ssh_transport_requires_explicit_success_after_complete_payload() {
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
fn interactive_input_reaches_the_backend_before_client_eof() {
    use git_rs::receive_io::{IoControl, ReceiveInput};
    use std::{net::Shutdown, os::unix::net::UnixStream, sync::mpsc, time::Duration};
    let request = b"0014command=ls-refs\n\x000001\0binary-request-fields\n0000";
    let (mut client, input) = UnixStream::pair().unwrap();
    let (output, mut backend) = UnixStream::pair().unwrap();
    backend
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let input = ReceiveInput::new(
        input.into(),
        IoControl::new(git_rs::process::Operation::new(5)).unwrap(),
    )
    .unwrap();
    let (sent, received) = mpsc::channel();
    let forwarding = std::thread::spawn(move || input.forward_to(&output));
    let response = std::thread::spawn(move || {
        let mut echoed = vec![0; request.len()];
        let result = backend.read_exact(&mut echoed).map(|()| echoed);
        let _ = sent.send(result);
    });
    client.write_all(request).unwrap();
    let result = received.recv_timeout(Duration::from_secs(3));
    client.shutdown(Shutdown::Write).unwrap();
    let copied = forwarding.join().unwrap().unwrap();
    response.join().unwrap();
    assert_eq!(
        result
            .expect("backend must receive the round before client EOF")
            .unwrap(),
        request
    );
    assert_eq!(copied, request.len() as u64);
}
