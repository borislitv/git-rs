#![forbid(unsafe_code)]

use git_rs::serving_socket::ServingSocket;
use std::{
    fs,
    os::unix::{
        fs::{DirBuilderExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "git-rs-serving-socket-{}",
            git_rs::scratch::identifier().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove owned listener fixture");
    }
}

#[test]
fn listener_is_private_exclusive_and_reusable_after_actual_owner_drop() {
    let root = Root::new();
    let path = root.0.join("ssh.sock");
    let first = ServingSocket::bind(&path).unwrap();
    let original = fs::symlink_metadata(&path).unwrap();
    assert_eq!(original.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        fs::metadata(path.with_extension("lock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(ServingSocket::bind(&path).is_err());
    assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), original.ino());
    let peer = UnixStream::connect(&path).unwrap();
    let accepted = first.listener.accept().unwrap();
    drop(peer);
    drop(accepted);
    drop(first);
    assert!(!path.exists());
    let second = ServingSocket::bind(&path).unwrap();
    assert!(UnixStream::connect(&path).is_ok());
    drop(second);
    assert!(!path.exists());
}

#[test]
fn listener_reclaims_a_stale_socket_but_preserves_live_or_non_socket_paths() {
    let root = Root::new();
    let path = root.0.join("ssh.sock");
    let live = UnixListener::bind(&path).unwrap();
    let identity = fs::symlink_metadata(&path).unwrap().ino();
    assert!(ServingSocket::bind(&path).is_err());
    assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), identity);
    drop(live);
    let owner = ServingSocket::bind(&path).unwrap();
    assert!(UnixStream::connect(&path).is_ok());
    drop(owner);
    fs::write(&path, b"preserve unrelated data").unwrap();
    assert!(ServingSocket::bind(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"preserve unrelated data");
}

#[test]
fn listener_drop_never_unlinks_a_replacement_or_follows_a_lock_symlink() {
    let root = Root::new();
    let path = root.0.join("ssh.sock");
    let owner = ServingSocket::bind(&path).unwrap();
    fs::remove_file(&path).unwrap();
    fs::write(&path, b"replacement").unwrap();
    drop(owner);
    assert_eq!(fs::read(&path).unwrap(), b"replacement");
    fs::remove_file(&path).unwrap();
    let lock = path.with_extension("lock");
    fs::remove_file(&lock).unwrap();
    let protected = root.0.join("protected");
    fs::write(&protected, b"protected").unwrap();
    std::os::unix::fs::symlink(&protected, &lock).unwrap();
    assert!(ServingSocket::bind(&path).is_err());
    assert!(!path.exists());
    assert_eq!(fs::read(&protected).unwrap(), b"protected");
}

#[test]
fn listener_rejects_a_shared_parent_before_creating_any_paths() {
    let root = Root::new();
    fs::set_permissions(&root.0, fs::Permissions::from_mode(0o755)).unwrap();
    let path = root.0.join("ssh.sock");
    assert!(ServingSocket::bind(&path).is_err());
    assert!(fs::read_dir(&root.0).unwrap().next().is_none());
}
