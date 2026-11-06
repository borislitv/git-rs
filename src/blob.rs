use crate::{
    Result,
    cache::{Cache, CachedFile},
    invalid,
    metadata::Pack,
    metrics::{self, Dependency, Direction, Metric, Stage},
    process,
};
use openssl::sha::Sha256;
use std::fs::{self, File};
use std::io::{Read, Seek};
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Store {
    endpoint: String,
    bucket: String,
    region: String,
    access: String,
    secret: String,
}

fn quote(value: &str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(invalid("control character in curl configuration"));
    }
    Ok(format!(
        "\"{}\"",
        value.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

pub fn digest(path: &Path) -> Result<String> {
    digest_reader(File::open(path)?)
}

fn digest_reader(mut input: impl Read) -> Result<String> {
    let timer = metrics::Timer::start(Metric::Stage(Stage::ObjectHash));
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        process::check_operation()?;
        let count = input.read(&mut buffer)?;
        if count == 0 {
            let hash = crate::protocol::hex(&digest.finish());
            timer.finish(true);
            return Ok(hash);
        }
        digest.update(&buffer[..count]);
    }
}

impl Store {
    pub fn from_env() -> Result<Self> {
        let endpoint = std::env::var("GIT_RS_S3_ENDPOINT")?;
        let local_http = std::env::var("GIT_RS_DEV_HTTP").as_deref() == Ok("1")
            && (endpoint.starts_with("http://127.0.0.1:")
                || endpoint.starts_with("http://localhost:"));
        if !endpoint.starts_with("https://") && !local_http {
            return Err(invalid(
                "S3 requires HTTPS (loopback development opt-in only)",
            ));
        }
        let store = Self {
            endpoint: endpoint.trim_end_matches('/').into(),
            bucket: std::env::var("GIT_RS_S3_BUCKET")?,
            region: std::env::var("GIT_RS_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access: std::env::var("GIT_RS_S3_ACCESS_KEY")?,
            secret: std::env::var("GIT_RS_S3_SECRET_KEY")?,
        };
        if store.bucket.is_empty()
            || !store
                .bucket
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
        {
            return Err(invalid("invalid S3 bucket"));
        }
        Ok(store)
    }

    fn transfer(
        &self,
        key: &str,
        file: &Path,
        upload: Option<&str>,
        delete: bool,
        max_download: Option<u64>,
        output: Option<&File>,
    ) -> Result<()> {
        if key.is_empty()
            || !key
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"/-_.".contains(&c))
            || key.split('/').any(|p| p == ".." || p == ".")
        {
            return Err(invalid("invalid object key"));
        }
        let mut config = format!(
            "url = {}\naws-sigv4 = {}\nuser = {}\n",
            quote(&format!("{}/{}/{}", self.endpoint, self.bucket, key))?,
            quote(&format!("aws:amz:{}:s3", self.region))?,
            quote(&format!("{}:{}", self.access, self.secret))?
        );
        match upload {
            Some(hash) => config.push_str(&format!(
                "upload-file = {}\nheader = {}\noutput = \"/dev/null\"\n",
                quote(
                    file.to_str()
                        .ok_or_else(|| invalid("non UTF-8 scratch path"))?
                )?,
                quote(&format!("x-amz-content-sha256: {hash}"))?
            )),
            None => config.push_str(&format!(
                "output = {}\n",
                quote(
                    file.to_str()
                        .ok_or_else(|| invalid("non UTF-8 scratch path"))?
                )?
            )),
        }
        if delete {
            config.push_str("request = \"DELETE\"\n");
        }
        if let Some(limit) = max_download {
            config.push_str(&format!("max-filesize = {limit}\n"));
        }
        let mut cmd = process::command("/usr/bin/curl");
        cmd.args([
            "--disable",
            "--silent",
            "--fail",
            "--proto",
            "=http,https",
            "--connect-timeout",
            "5",
            "--max-time",
            "110",
            "--config",
            "-",
        ]);
        // Credentials go through stdin, not argv or child environment. No redirects,
        // retries, proxy inheritance, or certificate-verification bypass.
        for name in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
            if let Some(value) = std::env::var_os(name) {
                cmd.env(name, value);
            }
        }
        let operation = if delete {
            Some(Dependency::S3Delete)
        } else if upload.is_some() {
            Some(Dependency::S3Put)
        } else {
            None
        };
        let mut transfer = || {
            if let Some(file) = output {
                process::capture_to_file(&mut cmd, config.as_bytes(), file)?;
            } else {
                process::capture(&mut cmd, config.as_bytes(), 4096)?;
            }
            if upload.is_some() {
                metrics::global().transferred(Direction::Upload, fs::metadata(file)?.len());
            }
            Ok(())
        };
        if let Some(operation) = operation {
            metrics::measure(Metric::Dependency(operation), transfer)
        } else {
            transfer()
        }
    }

    fn verify(&self, path: &Path, expected_size: u64, expected_digest: &str) -> Result<()> {
        if fs::metadata(path)?.len() != expected_size || digest(path)? != expected_digest {
            return Err(invalid("object integrity check failed"));
        }
        Ok(())
    }

    fn download(&self, key: &str, destination: &Path, size: u64, hash: &str) -> Result<()> {
        metrics::measure(Metric::Dependency(Dependency::S3Get), || {
            self.transfer(key, destination, None, false, Some(size), None)?;
            self.verify(destination, size, hash)?;
            metrics::global().transferred(Direction::Download, size);
            Ok(())
        })
    }

    pub fn materialize(
        &self,
        repo: &str,
        key: &str,
        hash: &str,
        size: u64,
        destination: &Path,
        cache: &Cache,
    ) -> Result<Option<Arc<CachedFile>>> {
        let cache_key = format!("{repo}:{key}:{hash}:{size}");
        let cached = cache.get_or_load(
            &cache_key,
            usize::try_from(size).unwrap_or(usize::MAX),
            |mut file| {
                metrics::measure(Metric::Dependency(Dependency::S3Get), || {
                    self.transfer(key, Path::new("-"), None, false, Some(size), Some(file))?;
                    // Preallocation and size seals prevent growth, so length alone
                    // cannot distinguish a short response from trailing zeroes.
                    if file.stream_position()? != size {
                        return Err(invalid("object integrity check failed"));
                    }
                    file.rewind()?;
                    if digest_reader(file)? != hash {
                        return Err(invalid("object integrity check failed"));
                    }
                    metrics::global().transferred(Direction::Download, size);
                    Ok(())
                })
            },
        )?;
        if let Some(buffer) = &cached {
            metrics::measure(Metric::Stage(Stage::CacheLink), || {
                Ok(symlink(buffer.path(), destination)?)
            })?;
        } else {
            self.download(key, destination, size, hash)?;
        }
        Ok(cached)
    }

    pub fn upload(
        &self,
        repo: &str,
        operation: &str,
        directory: &Path,
        native_hash: &str,
    ) -> Result<Pack> {
        let pack_path = directory.join(format!("pack-{native_hash}.pack"));
        let idx_path = directory.join(format!("pack-{native_hash}.idx"));
        let pack_digest = digest(&pack_path)?;
        let idx_digest = digest(&idx_path)?;
        let prefix = format!("repos/{repo}/operations/{operation}/{native_hash}");
        let pack = Pack {
            hash: native_hash.into(),
            pack_key: format!("{prefix}.pack"),
            idx_key: format!("{prefix}.idx"),
            pack_size: fs::metadata(&pack_path)?.len(),
            idx_size: fs::metadata(&idx_path)?.len(),
            pack_digest,
            idx_digest,
        };
        crate::metadata::sql(&format!(
            "INSERT INTO staged_objects(repo_id,operation_id,object_key,state) VALUES({}::uuid,{}::uuid,{},'planned'),({}::uuid,{}::uuid,{},'planned');",
            crate::metadata::text(repo),
            crate::metadata::text(operation),
            crate::metadata::text(&pack.pack_key),
            crate::metadata::text(repo),
            crate::metadata::text(operation),
            crate::metadata::text(&pack.idx_key)
        ))?;
        self.transfer(
            &pack.pack_key,
            &pack_path,
            Some(&pack.pack_digest),
            false,
            None,
            None,
        )?;
        crate::metadata::sql(&format!(
            "UPDATE staged_objects SET state='uploaded' WHERE object_key={};",
            crate::metadata::text(&pack.pack_key)
        ))?;
        self.transfer(
            &pack.idx_key,
            &idx_path,
            Some(&pack.idx_digest),
            false,
            None,
            None,
        )?;
        crate::metadata::sql(&format!(
            "UPDATE staged_objects SET state='uploaded' WHERE object_key={};",
            crate::metadata::text(&pack.idx_key)
        ))?;
        Ok(pack)
    }

    pub fn delete_retired(&self, key: &str) -> Result<()> {
        self.transfer(key, Path::new("/dev/null"), None, true, None, None)
    }

    /// Physical reclamation currently supports only never-versioned buckets.
    /// Enabled/suspended/unknown policies require a version-aware GC instead.
    pub fn require_unversioned(&self) -> Result<()> {
        let config = format!(
            "url = {}\naws-sigv4 = {}\nuser = {}\n",
            quote(&format!("{}/{}?versioning", self.endpoint, self.bucket))?,
            quote(&format!("aws:amz:{}:s3", self.region))?,
            quote(&format!("{}:{}", self.access, self.secret))?
        );
        let mut cmd = process::command("/usr/bin/curl");
        cmd.args([
            "--disable",
            "--silent",
            "--fail",
            "--proto",
            "=http,https",
            "--connect-timeout",
            "5",
            "--max-time",
            "30",
            "--config",
            "-",
        ]);
        for name in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
            if let Some(value) = std::env::var_os(name) {
                cmd.env(name, value);
            }
        }
        let response = metrics::measure(Metric::Dependency(Dependency::S3Versioning), || {
            process::capture(&mut cmd, config.as_bytes(), 16384)
        })?;
        let mut xml = std::str::from_utf8(&response)?.trim();
        if xml.starts_with("<?xml ") {
            xml = xml
                .split_once("?>")
                .ok_or_else(|| invalid("invalid bucket versioning response"))?
                .1
                .trim();
        }
        if !xml.starts_with("<VersioningConfiguration") {
            return Err(invalid("unrecognized bucket versioning policy"));
        }
        let (opening, body) = xml
            .split_once('>')
            .ok_or_else(|| invalid("invalid bucket versioning response"))?;
        let empty = if opening.ends_with('/') {
            body.trim().is_empty()
        } else {
            body.strip_suffix("</VersioningConfiguration>")
                .is_some_and(|value| value.trim().is_empty())
        };
        if !empty {
            return Err(invalid(
                "physical cleanup requires an unversioned bucket; version-aware deletion is not implemented",
            ));
        }
        Ok(())
    }
}
