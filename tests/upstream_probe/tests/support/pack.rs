use std::{
    ffi::OsString,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
    process::{Command, Stdio},
};

use flate2::{Compression, write::ZlibEncoder};
use git_rs_upstream_probe::pack_decoder::{IndexedEntry, Pack, RangeSource};
use gix_error::ResultExt;
use gix_hash::{Kind::Sha1, ObjectId};
use gix_pack::data::{self, entry::Header};

pub const BASE: &[u8] = b"abcdefgh";
pub const MID: &[u8] = b"abcdefgh!";
pub const TOP: &[u8] = b"abcdefgh!?";

pub struct Fixture(Vec<u8>);

impl Fixture {
    pub fn new(count: u32) -> Self {
        Self(data::header::encode(data::Version::V2, count).to_vec())
    }

    pub fn offset(&self) -> u64 {
        self.0.len() as u64
    }

    pub fn entry(&mut self, header: Header, plaintext: &[u8]) -> u64 {
        let offset = self.offset();
        header
            .write_to(plaintext.len() as u64, &mut self.0)
            .unwrap();
        let mut stream = ZlibEncoder::new(&mut self.0, Compression::default());
        stream.write_all(plaintext).unwrap();
        stream.finish().unwrap();
        offset
    }

    pub fn append_byte(&mut self, header: Header, base: &[u8], result: &[u8]) {
        let n = u8::try_from(base.len()).unwrap();
        assert!((1..=126).contains(&n));
        assert_eq!(&result[..base.len()], base);
        assert_eq!(result.len(), base.len() + 1);
        // Two one-byte sizes, copy the entire short base from offset zero,
        // then insert one byte. Native Git, not the probe, indexes the result.
        self.entry(header, &[n, n + 1, 0x90, n, 1, result[base.len()]]);
    }

    /// Fixture only: add a redundant zero size group to a one-byte size field.
    pub fn extend_size_encoding_at(&mut self, offset: u64) {
        let start = usize::try_from(offset).unwrap();
        assert_eq!(self.0[start] & 0x80, 0);
        self.0[start] |= 0x80;
        self.0.insert(start + 1, 0);
    }

    pub fn finish(mut self) -> Vec<u8> {
        let mut hash = gix_hash::hasher(Sha1);
        hash.update(&self.0);
        self.0
            .extend_from_slice(hash.try_finalize().unwrap().as_slice());
        self.0
    }
}

pub struct Stored {
    pub path: PathBuf,
    pub entries: Vec<IndexedEntry>,
    entries_end: u64,
}

impl Stored {
    pub fn catalog(&self) -> Pack<'_> {
        Pack {
            entries_end: self.entries_end,
            entries: &self.entries,
        }
    }
}

pub struct NativeGit(tempfile::TempDir);

impl NativeGit {
    pub fn new() -> Self {
        let git = Self(tempfile::tempdir().unwrap());
        git.run(
            &[
                "init",
                "--quiet",
                "--bare",
                "--object-format=sha1",
                "--template=",
            ],
            b"",
        );
        git
    }

    fn run(&self, args: &[&str], input: &[u8]) -> Vec<u8> {
        let mut child = Command::new("git")
            .args(args)
            .current_dir(self.0.path())
            .env_clear()
            .env(
                "PATH",
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            )
            .env("HOME", self.0.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("LC_ALL", "C")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    pub fn oid(&self, bytes: &[u8]) -> ObjectId {
        let hex = self.run(&["hash-object", "--stdin"], bytes);
        ObjectId::from_hex(hex.trim_ascii_end()).unwrap()
    }

    pub fn kind(&self, id: ObjectId) -> gix_object::Kind {
        assert_eq!(
            self.run(&["cat-file", "-t", &id.to_string()], b""),
            b"blob\n"
        );
        gix_object::Kind::Blob
    }

    pub fn blob(&self, id: ObjectId) -> Vec<u8> {
        self.run(&["cat-file", "blob", &id.to_string()], b"")
    }

    pub fn install(&self, bytes: &[u8], thin: bool) -> Stored {
        let mut args = vec!["index-pack", "--stdin", "--strict", "--index-version=2"];
        if thin {
            args.push("--fix-thin");
        }
        let result = self.run(&args, bytes);
        let name = std::str::from_utf8(&result)
            .unwrap()
            .split_ascii_whitespace()
            .last()
            .unwrap();
        let path = self
            .0
            .path()
            .join("objects/pack")
            .join(format!("pack-{name}.pack"));
        let idx = fs::read(path.with_extension("idx")).unwrap();
        let index =
            gix_pack::index::File::from_data(idx.as_slice(), Default::default(), Sha1).unwrap();
        let mut entries: Vec<_> = index
            .iter()
            .map(|e| IndexedEntry {
                offset: e.pack_offset,
                oid: e.oid,
                crc32: e.crc32.unwrap(),
            })
            .collect();
        entries.sort_unstable_by_key(|e| e.offset);
        let entries_end = fs::metadata(&path).unwrap().len() - Sha1.len_in_bytes() as u64;
        Stored {
            path,
            entries,
            entries_end,
        }
    }

    /// `index-pack --fix-thin` appends the base; use its independently generated
    /// index only for the unchanged original delta entry.
    pub fn original_thin(&self, raw: &[u8], completed: &Stored) -> Stored {
        let entries_end = raw.len() - Sha1.len_in_bytes();
        assert_eq!(completed.entries.len(), 2);
        assert_eq!(completed.entries[1].offset, entries_end as u64);
        let complete_bytes = fs::read(&completed.path).unwrap();
        let entry = data::header::SIZE..entries_end;
        assert_eq!(raw[entry.clone()], complete_bytes[entry]);
        let path = self.0.path().join("original-thin.pack");
        fs::write(&path, raw).unwrap();
        Stored {
            path,
            entries: vec![completed.entries[0]],
            entries_end: entries_end as u64,
        }
    }
}

pub struct Source {
    files: Vec<(File, u64)>,
    pub reads: Vec<(usize, Range<u64>)>,
    pub fail_at: Option<(usize, u64)>,
    pub first_prefix_override: Option<(usize, u64, Vec<u8>)>,
}

impl Source {
    pub fn open(packs: &[&Stored]) -> Self {
        Self {
            files: packs
                .iter()
                .map(|p| (File::open(&p.path).unwrap(), p.entries_end))
                .collect(),
            reads: Vec::new(),
            fail_at: None,
            first_prefix_override: None,
        }
    }

    pub fn requested_bytes(&self) -> usize {
        self.reads
            .iter()
            .map(|(_, r)| (r.end - r.start) as usize)
            .sum()
    }
}

impl RangeSource for Source {
    fn read_exact(&mut self, pack: usize, offset: u64, out: &mut [u8]) -> gix_error::Result {
        let end = offset + out.len() as u64;
        let (file, entries_end) = &mut self.files[pack];
        assert!(offset >= data::header::SIZE as u64 && end <= *entries_end);
        self.reads.push((pack, offset..end));
        if self.fail_at == Some((pack, offset)) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "fixture source unavailable",
            ))
            .or_error();
        }
        file.seek(SeekFrom::Start(offset)).or_error()?;
        file.read_exact(out).or_error()?;
        if self
            .first_prefix_override
            .as_ref()
            .is_some_and(|(p, start, _)| (*p, *start) == (pack, offset))
        {
            let (_, _, prefix) = self.first_prefix_override.take().unwrap();
            out[..prefix.len()].copy_from_slice(&prefix);
        }
        Ok(())
    }
}
