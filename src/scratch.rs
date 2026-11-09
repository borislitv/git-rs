use crate::{
    Result,
    cache::CachedFile,
    metrics::{self, Metric, Phase, Stage},
    process,
    protocol::Oid,
    repository,
};
use gix::bstr::ByteSlice;
use std::collections::BTreeMap;
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
                    return Err(crate::invalid("unexpected repository template entry"));
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
        process::check_operation()?;
        gix::ThreadSafeRepository::init_opts(
            &scratch.path,
            gix::create::Kind::Bare,
            gix::create::Options {
                object_hash: Some(gix::hash::Kind::Sha1),
                ..Default::default()
            },
            gix::open::Options::isolated()
                .strict_config(true)
                .config_overrides(["init.defaultBranch=main"]),
        )?;
        // gix includes built-in sample hooks. The request template must contain
        // no hook directory; only our later, operation-owned broker may add one.
        fs::remove_dir_all(scratch.path.join("hooks"))?;
        process::check_operation()?;
        // Capture only this freshly initialized, application-owned repository.
        // Racing first requests may each initialize once; no failed/cancelled
        // initialization is retained. The reusable template lives only in RAM.
        let _ = TEMPLATE.set(Template::read(&scratch.path)?);
        Ok(scratch)
    }

    /// Read a physical object's kind and uncompressed size without decoding its body.
    /// The workspace and its verified pack/index cache pins stay alive during lookup.
    pub fn object_header(&self, id: Oid) -> Result<Option<gix::odb::find::Header>> {
        use gix::odb::Header;

        process::check_operation()?;
        let header = repository::ObjectHeaders::open(self)?
            .try_header(gix::ObjectId::from(id.0).as_ref())?;
        process::check_operation()?;
        Ok(header)
    }

    /// Restore one leased snapshot into this request-private workspace.
    pub fn restore_refs(&self, refs: &BTreeMap<Vec<u8>, Oid>, default_ref: &[u8]) -> Result<()> {
        use gix::odb::HeaderExt;
        use gix::refs::transaction::{PreviousValue, RefEdit};

        process::check_operation()?;
        // Validate the complete set before modifying this private workspace.
        let edits = refs
            .iter()
            .map(|(name, oid)| {
                Ok(RefEdit::update(
                    repository::ref_name(name)?,
                    gix::ObjectId::from(oid.0),
                    PreviousValue::MustNotExist,
                    "",
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let default_ref = repository::ref_name(default_ref)?;
        let mut repo = gix::open_opts(
            &self.path,
            gix::open::Options::isolated()
                .strict_config(true)
                .open_path_as_is(true),
        )?;
        repo.refs.write_reflog = gix::refs::store::WriteReflog::Disable;
        let restore = metrics::Timer::start(Metric::Stage(Stage::RestoreRefs));
        if !edits.is_empty() {
            let objects = repository::ObjectHeaders::open(self)?;
            for (name, oid) in refs {
                process::check_operation()?;
                // Match update-ref's physical existence/type checks without
                // inflating blobs or synthesizing the absent empty tree.
                let header = objects.header(gix::ObjectId::from(oid.0))?;
                if name.starts_with(b"refs/heads/") && header.kind() != gix::objs::Kind::Commit {
                    return Err(crate::invalid("branch must point directly to a commit"));
                }
            }
            repo.edit_references_as(edits, None)?;
        }
        process::check_operation()?;
        restore.finish(true);
        if default_ref.as_bstr() != b"refs/heads/main".as_bstr() {
            let head = metrics::Timer::start(Metric::Stage(Stage::RestoreHead));
            // gix-ref 0.69.1 formats symbolic targets through Display, which
            // replaces non-UTF-8 bytes. This validated HEAD belongs exclusively
            // to the unexposed request workspace, so write its bytes directly.
            let mut bytes = b"ref: ".to_vec();
            bytes.extend_from_slice(default_ref.as_bstr());
            bytes.push(b'\n');
            fs::write(self.path.join("HEAD"), bytes)?;
            process::check_operation()?;
            head.finish(true);
        }
        Ok(())
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
        if let Err(error) = result
            && !metrics::service_event(metrics::ServiceEvent::ScratchCleanupFailed)
        {
            eprintln!("scratch cleanup failed: {error}");
        }
    }
}
