use crate::{
    Result,
    cache::CachedFile,
    metrics::{self, Metric, Phase, Stage},
    process,
};
use std::fs::{self, DirBuilder, File};
use std::io::Read;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

struct Template {
    directories: Vec<PathBuf>,
    files: Vec<(PathBuf, Vec<u8>)>,
}

impl Template {
    fn read(root: &Path) -> Result<Self> {
        fn visit(root: &Path, relative: &Path, template: &mut Template) -> Result<()> {
            for entry in fs::read_dir(root.join(relative))? {
                let entry = entry?;
                let path = relative.join(entry.file_name());
                if entry.file_type()?.is_dir() {
                    template.directories.push(path.clone());
                    visit(root, &path, template)?;
                } else if entry.file_type()?.is_file() {
                    template.files.push((path, fs::read(entry.path())?));
                } else {
                    return Err(crate::invalid("unexpected native Git template entry"));
                }
            }
            Ok(())
        }
        let mut template = Self {
            directories: Vec::new(),
            files: Vec::new(),
        };
        visit(root, Path::new(""), &mut template)?;
        Ok(template)
    }

    fn write(&self, root: &Path) -> Result<()> {
        for path in &self.directories {
            fs::create_dir(root.join(path))?;
        }
        for (path, bytes) in &self.files {
            fs::write(root.join(path), bytes)?;
        }
        Ok(())
    }
}

static TEMPLATE: OnceLock<Template> = OnceLock::new();

pub fn identifier() -> Result<String> {
    let mut bytes = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(crate::protocol::hex(&bytes))
}

pub struct Scratch {
    pub path: PathBuf,
    packs: Vec<Arc<CachedFile>>,
}

impl Scratch {
    pub fn new(root: &Path, pack_threads: u8) -> Result<Self> {
        metrics::measure(Metric::Phase(Phase::ScratchInit), || {
            let scratch = Self::create(root)?;
            // Write configuration after copying the template: it must reflect
            // this engine's operator-selected budget, not the first request's.
            fs::write(
                scratch.path.join("config"),
                format!(
                    concat!(
                        "[core]\nrepositoryformatversion = 0\nbare = true\nfilemode = true\n",
                        "[receive]\nprocReceiveRefs = refs\nunpackLimit = 0\nfsckObjects = true\nmaxInputSize = 4294967296\nshallowUpdate = false\nautoGC = false\n",
                        "[transfer]\nfsckObjects = true\n[gc]\nauto = 0\n[maintenance]\nauto = false\n",
                        "[uploadpack]\nallowAnySHA1InWant = false\n[http]\nreceivepack = true\n",
                        "[pack]\nthreads = {}\nwindowMemory = 32m\ndeltaCacheSize = 16m\n"
                    ),
                    pack_threads
                ),
            )?;
            Ok(scratch)
        })
    }

    fn create(root: &Path) -> Result<Self> {
        let path = root.join(format!("request-{}", identifier()?));
        DirBuilder::new().mode(0o700).create(&path)?;
        let scratch = Self {
            path,
            packs: Vec::new(),
        };
        if let Some(template) = TEMPLATE.get() {
            template.write(&scratch.path)?;
            return Ok(scratch);
        }
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
        // Capture only this freshly initialized, application-owned repository.
        // Racing first requests may each initialize once; no failed/cancelled
        // initialization is retained. The reusable template lives only in RAM.
        let _ = TEMPLATE.set(Template::read(&scratch.path)?);
        Ok(scratch)
    }

    pub(crate) fn retain(&mut self, file: Option<Arc<CachedFile>>) {
        if let Some(file) = file {
            self.packs.push(file);
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let timer = metrics::Timer::start(Metric::Stage(Stage::ScratchCleanup));
        let result = fs::remove_dir_all(&self.path);
        timer.finish(result.is_ok());
        if let Err(error) = result {
            eprintln!("scratch cleanup failed: {error}");
        }
    }
}
