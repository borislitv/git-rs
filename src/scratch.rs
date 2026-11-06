use crate::{
    Result,
    metrics::{self, Metric, Phase},
    process,
};
use std::fs::{self, DirBuilder, File};
use std::io::Read;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

pub fn identifier() -> Result<String> {
    let mut bytes = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(crate::protocol::hex(&bytes))
}

pub struct Scratch {
    pub path: PathBuf,
}

impl Scratch {
    pub fn new(root: &Path) -> Result<Self> {
        metrics::measure(Metric::Phase(Phase::ScratchInit), || Self::create(root))
    }

    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!("request-{}", identifier()?));
        DirBuilder::new().mode(0o700).create(&path)?;
        let scratch = Self { path };
        process::capture(
            process::git()
                .args([
                    "init",
                    "--bare",
                    "--object-format=sha1",
                    "--initial-branch=main",
                    "--template=",
                ])
                .arg(&scratch.path),
            b"",
            4096,
        )?;
        // This is entirely application-owned configuration in a new bare repo.
        // One write avoids spawning a Git process for each constant setting.
        fs::write(
            scratch.path.join("config"),
            concat!(
                "[core]\nrepositoryformatversion = 0\nbare = true\nfilemode = true\n",
                "[receive]\nprocReceiveRefs = refs\nunpackLimit = 0\nfsckObjects = true\nmaxInputSize = 4294967296\nshallowUpdate = false\nautoGC = false\n",
                "[transfer]\nfsckObjects = true\n[gc]\nauto = 0\n[maintenance]\nauto = false\n",
                "[uploadpack]\nallowAnySHA1InWant = false\n[http]\nreceivepack = true\n",
                "[pack]\nthreads = 1\nwindowMemory = 32m\ndeltaCacheSize = 16m\n"
            ),
        )?;
        Ok(scratch)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!("scratch cleanup failed: {error}");
        }
    }
}
