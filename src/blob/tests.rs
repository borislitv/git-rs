use super::*;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum Reply {
    Complete(Vec<u8>),
    Redirect(SocketAddr),
    Paused {
        headers: bool,
        reached: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    },
}

struct Server {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    requested: Arc<AtomicBool>,
    worker: Option<JoinHandle<Vec<SocketAddr>>>,
}

impl Server {
    fn new(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let requested = Arc::new(AtomicBool::new(false));
        let observed = requested.clone();
        let worker = thread::spawn(move || {
            let mut connection: Option<(BufReader<TcpStream>, SocketAddr)> = None;
            let mut peers = Vec::new();
            for reply in replies {
                loop {
                    if connection.is_none() {
                        let (stream, peer) = listener.accept().unwrap();
                        if stopping.load(Ordering::Acquire) {
                            return peers;
                        }
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        connection = Some((BufReader::new(stream), peer));
                    }
                    let (stream, peer) = connection.as_mut().unwrap();
                    let mut header = String::new();
                    if stream.read_line(&mut header).unwrap_or(0) == 0 {
                        connection = None;
                        continue;
                    }
                    assert!(header.starts_with("GET /test-bucket"), "{header:?}");
                    observed.store(true, Ordering::Release);
                    let mut headers = 0;
                    loop {
                        header.clear();
                        assert!(stream.read_line(&mut header).unwrap() > 0);
                        if header == "\r\n" {
                            break;
                        }
                        headers += 1;
                        assert!(headers <= 100);
                    }
                    peers.push(*peer);
                    let socket = stream.get_mut();
                    match reply {
                        Reply::Complete(body) => {
                            write!(
                                socket,
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                                body.len()
                            )
                            .unwrap();
                            socket.write_all(&body).unwrap();
                        }
                        Reply::Redirect(address) => {
                            write!(socket, "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/test-bucket/?versioning\r\nContent-Length: 0\r\n\r\n").unwrap();
                        }
                        Reply::Paused {
                            headers,
                            reached,
                            release,
                        } => {
                            if headers {
                                socket
                                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\ncomp")
                                    .unwrap();
                            }
                            reached.send(()).unwrap();
                            release.recv_timeout(Duration::from_secs(5)).unwrap();
                            let rest = if headers {
                                b"lete".as_slice()
                            } else {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\ncomplete"
                            };
                            // Cancellation is expected to close the peer first.
                            let _ = socket.write_all(rest);
                        }
                    }
                    break;
                }
            }
            peers
        });
        Self {
            address,
            stop,
            requested,
            worker: Some(worker),
        }
    }

    fn store(&self) -> Store {
        Store {
            endpoint: format!("http://{}", self.address),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access: "synthetic-access".into(),
            secret: "synthetic-secret".into(),
        }
    }

    fn finish(mut self) -> Vec<SocketAddr> {
        self.worker.take().unwrap().join().unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.address);
            worker.join().unwrap();
        }
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "git-rs-s3-{}",
            crate::scratch::identifier().unwrap()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn consecutive_s3_requests_reuse_a_connection() {
    let policy = b"<VersioningConfiguration/>";
    let server = Server::new((0..3).map(|_| Reply::Complete(policy.to_vec())).collect());
    let store = server.store();
    for _ in 0..3 {
        store.require_unversioned().unwrap();
    }
    let peers = server.finish();
    assert_eq!(peers.len(), 3);
    assert_eq!(
        peers.into_iter().collect::<BTreeSet<_>>().len(),
        1,
        "sequential S3 calls must reuse their authenticated HTTP connection"
    );
}

#[test]
fn redirects_are_rejected_instead_of_forwarding_a_signed_request() {
    let target = Server::new(vec![Reply::Complete(
        b"<VersioningConfiguration/>".to_vec(),
    )]);
    let source = Server::new(vec![Reply::Redirect(target.address)]);
    let operation = process::Operation::new(2);
    assert!(
        operation
            .enter(|| source.store().require_unversioned())
            .is_err()
    );
    assert_eq!(source.finish().len(), 1);
    assert!(
        !target.requested.load(Ordering::Acquire),
        "redirect target must never receive a request"
    );
    // Drop wakes a target that was correctly never contacted.
    drop(target);
}

#[test]
fn failed_response_integrity_does_not_poison_a_later_complete_fill() {
    let server = Server::new(vec![
        Reply::Complete(b"too-large".to_vec()),
        Reply::Complete(b"comp".to_vec()),
        Reply::Complete(b"corrupt!".to_vec()),
        Reply::Complete(b"complete".to_vec()),
    ]);
    let store = server.store();
    let cache = Cache::new(4096, 8);
    let directory = Directory::new();
    let destination = directory.0.join("object");
    let hash = crate::protocol::hex(&openssl::sha::sha256(b"complete"));
    for _ in 0..3 {
        assert!(
            store
                .materialize("repo", "object.pack", &hash, 8, &destination, &cache)
                .is_err()
        );
        assert!(!destination.exists());
        assert_eq!(cache.allocated(), 0);
    }
    let entry = store
        .materialize("repo", "object.pack", &hash, 8, &destination, &cache)
        .unwrap()
        .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"complete");
    assert_eq!(fs::read(entry.path()).unwrap(), b"complete");
    assert_eq!(server.finish().len(), 4);
}

#[test]
fn bucket_policy_response_cannot_grow_past_its_budget() {
    let mut policy = b"<VersioningConfiguration/>".to_vec();
    policy.resize(16385, b' ');
    let server = Server::new(vec![Reply::Complete(policy)]);
    assert!(server.store().require_unversioned().is_err());
    assert_eq!(server.finish().len(), 1);
}

#[test]
#[ignore = "entry point for the isolated TLS fixture"]
fn isolated_tls_client() {
    let expected = std::env::var("GIT_RS_TEST_TLS_SUCCESS").unwrap() == "1";
    let result = process::Operation::new(2).enter(|| Store::from_env()?.require_unversioned());
    assert_eq!(
        result.is_ok(),
        expected,
        "unexpected TLS verification outcome"
    );
}

#[test]
fn tls_requires_a_trusted_certificate_and_matching_hostname() {
    use openssl::{
        asn1::Asn1Time,
        hash::MessageDigest,
        pkey::PKey,
        rsa::Rsa,
        x509::{
            X509, X509NameBuilder,
            extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName},
        },
    };
    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_text("CN", "localhost").unwrap();
    let name = name.build();
    let mut certificate = X509::builder().unwrap();
    certificate.set_version(2).unwrap();
    let serial = openssl::bn::BigNum::from_u32(1)
        .unwrap()
        .to_asn1_integer()
        .unwrap();
    certificate.set_serial_number(&serial).unwrap();
    certificate.set_subject_name(&name).unwrap();
    certificate.set_issuer_name(&name).unwrap();
    certificate.set_pubkey(&key).unwrap();
    certificate
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    certificate
        .set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    certificate
        .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
        .unwrap();
    certificate
        .append_extension(
            KeyUsage::new()
                .digital_signature()
                .key_encipherment()
                .key_cert_sign()
                .build()
                .unwrap(),
        )
        .unwrap();
    certificate
        .append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap())
        .unwrap();
    let subject = SubjectAlternativeName::new()
        .dns("localhost")
        .build(&certificate.x509v3_context(None, None))
        .unwrap();
    certificate.append_extension(subject).unwrap();
    certificate.sign(&key, MessageDigest::sha256()).unwrap();
    let certificate = certificate.build().to_pem().unwrap();
    let identity =
        native_tls::Identity::from_pkcs8(&certificate, &key.private_key_to_pem_pkcs8().unwrap())
            .unwrap();
    let directory = Directory::new();
    let ca = directory.0.join("ca.pem");
    fs::write(&ca, &certificate).unwrap();

    for (trust, host, success) in [
        (true, "localhost", true),
        (false, "localhost", false),
        (true, "127.0.0.1", false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = native_tls::TlsAcceptor::new(identity.clone()).unwrap();
        let server = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            if let Ok(socket) = acceptor.accept(socket) {
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                while socket.read_line(&mut line).unwrap_or(0) != 0 {
                    if line == "\r\n" {
                        socket.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 26\r\n\r\n<VersioningConfiguration/>").unwrap();
                        break;
                    }
                    line.clear();
                }
            }
        });
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--ignored", "--exact", "blob::tests::isolated_tls_client"])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", "/nonexistent")
            .env(
                "GIT_RS_S3_ENDPOINT",
                format!("https://{host}:{}", address.port()),
            )
            .env("GIT_RS_S3_BUCKET", "test-bucket")
            .env("GIT_RS_S3_ACCESS_KEY", "synthetic-access")
            .env("GIT_RS_S3_SECRET_KEY", "synthetic-secret")
            .env("GIT_RS_TEST_TLS_SUCCESS", if success { "1" } else { "0" });
        if trust {
            child.env("SSL_CERT_FILE", &ca);
        }
        let result = child.output().unwrap();
        // Wake accept even if client configuration failed before connecting.
        let _ = TcpStream::connect(address);
        server.join().unwrap();
        assert!(
            result.status.success(),
            "trust={trust} host={host}: {} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

#[test]
fn cancelling_s3_headers_or_body_releases_the_cache_fill_before_the_server_replies() {
    for headers in [false, true] {
        let (reached, requested) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (completed, result) = mpsc::channel();
        let server = Server::new(vec![Reply::Paused {
            headers,
            reached,
            release: released,
        }]);
        let store = server.store();
        let directory = Directory::new();
        let cache = Cache::new(4096, 8);
        let operation = process::Operation::new(30);
        thread::scope(|scope| {
            let cache = &cache;
            let operation = &operation;
            let destination = directory.0.join("download");
            scope.spawn(move || {
                let failed = operation.enter(|| {
                    store
                        .materialize(
                            "repo",
                            "object.pack",
                            &crate::protocol::hex(&openssl::sha::sha256(b"complete")),
                            8,
                            &destination,
                            cache,
                        )
                        .is_err()
                });
                completed.send(failed).unwrap();
            });
            requested.recv_timeout(Duration::from_secs(5)).unwrap();
            operation.cancel();
            let stopped = result.recv_timeout(Duration::from_secs(2));
            release.send(()).unwrap();
            assert!(stopped.expect("cancelled S3 must stop without waiting for the peer"));
        });
        server.finish();
        assert_eq!(
            cache.allocated(),
            0,
            "cancelled bytes must not remain cached"
        );
        assert!(!directory.0.join("download").exists());
    }
}
