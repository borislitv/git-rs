//! Exclusive ownership of the private SSH bridge listener.

use crate::{
    Result, invalid,
    metrics::{self, ServiceEvent},
};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
};

pub struct ServingSocket {
    pub listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
    _lock: File,
}

impl ServingSocket {
    pub fn bind(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("socket parent required"))?;
        let metadata = fs::symlink_metadata(parent)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "serving socket parent must be a private directory (0700)",
            ));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path.with_extension("lock"))?;
        let metadata = lock.metadata()?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "serving socket lock must be a private regular file",
            ));
        }
        lock.try_lock()
            .map_err(|_| invalid("serving socket is owned by another process"))?;
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                if !metadata.file_type().is_socket() {
                    return Err(invalid("refusing to replace non-socket path"));
                }
                match UnixStream::connect(path) {
                    Ok(_) => return Err(invalid("serving socket is live")),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                        ) => {}
                    Err(error) => return Err(error.into()),
                }
                fs::remove_file(path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(path)?;
        let metadata = fs::symlink_metadata(path)?;
        let socket = Self {
            listener,
            path: path.into(),
            identity: (metadata.dev(), metadata.ino()),
            _lock: lock,
        };
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(socket)
    }
}

impl Drop for ServingSocket {
    fn drop(&mut self) {
        let cleanup = || -> std::io::Result<()> {
            match fs::symlink_metadata(&self.path) {
                Ok(metadata) if (metadata.dev(), metadata.ino()) == self.identity => {
                    fs::remove_file(&self.path)
                }
                Ok(_) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        };
        if let Err(error) = cleanup()
            && !metrics::service_event(ServiceEvent::ServingSocketCleanupFailed)
        {
            eprintln!("serving socket cleanup failed: {error}");
        }
    }
}
