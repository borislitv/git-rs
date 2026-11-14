use crate::{
    Result, invalid,
    metadata::Pack,
    metrics::{self, Dependency, Direction, Metric},
    process,
};
use openssl::sha::Sha256;

mod maintenance;
mod multipart;
mod transport;
mod xml;
pub(crate) use multipart::UploadPlan;
pub(crate) use xml::VersioningBlocked;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct Store {
    endpoint: String,
    bucket: String,
    region: String,
    access: String,
    secret: String,
}

impl Store {
    pub fn from_env() -> Result<Self> {
        Self::from_env_prefix("GIT_RS_S3_")
    }

    pub(crate) fn from_cache_env() -> Result<Self> {
        Self::from_env_prefix("GIT_RS_CACHE_S3_")
    }

    fn from_env_prefix(prefix: &str) -> Result<Self> {
        let setting = |name: &str| std::env::var(format!("{prefix}{name}"));
        let endpoint = setting("ENDPOINT")?;
        let url = reqwest::Url::parse(&endpoint).map_err(|_| invalid("invalid S3 endpoint"))?;
        let local_http = std::env::var("GIT_RS_DEV_HTTP").as_deref() == Ok("1")
            && url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "localhost"));
        if url.scheme() != "https" && !local_http {
            return Err(invalid(
                "S3 requires HTTPS (loopback development opt-in only)",
            ));
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || endpoint.chars().any(char::is_control)
        {
            return Err(invalid(
                "S3 endpoint must not contain credentials, query or fragment",
            ));
        }
        let store = Self {
            endpoint: url.as_str().trim_end_matches('/').into(),
            bucket: setting("BUCKET")?,
            region: setting("REGION").unwrap_or_else(|_| "us-east-1".into()),
            access: setting("ACCESS_KEY")?,
            secret: setting("SECRET_KEY")?,
        };
        if store.bucket.is_empty()
            || !store
                .bucket
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
        {
            return Err(invalid("invalid S3 bucket"));
        }
        for value in [&store.region, &store.access, &store.secret] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(invalid("invalid S3 signing configuration"));
            }
        }
        transport::initialize()?;
        Ok(store)
    }

    pub(crate) fn put_cache_bytes(&self, key: &str, bytes: &[u8], hash: &str) -> Result<()> {
        validate_key(key)?;
        self.request_memory(
            reqwest::Method::PUT,
            &format!("{}/{}/{}", self.endpoint, self.bucket, key),
            Some(hash),
            Some(bytes),
            None,
            &mut std::io::sink(),
            4096,
            30,
        )
    }

    /// Read and authenticate an entire small immutable object, such as an index,
    /// directly into caller-admitted Rust backing. Large pack data uses ranges.
    pub(crate) fn source_bytes(
        &self,
        key: &str,
        size: u64,
        expected_digest: &str,
        control: &git_rs_core::Control,
        budget: &git_rs_core::CapacityBudget,
    ) -> Result<git_rs_core::CapacityBuffer> {
        validate_key(key)?;
        control.check()?;
        let size = usize::try_from(size)?;
        let mut bytes = budget.buffer(size)?;
        bytes.resize_controlled(size, 0, control)?;
        self.source_into(key, expected_digest, bytes.as_mut_slice(), control)?;
        Ok(bytes)
    }

    pub(crate) fn source_into(
        &self,
        key: &str,
        expected_digest: &str,
        bytes: &mut [u8],
        control: &git_rs_core::Control,
    ) -> Result<()> {
        validate_key(key)?;
        control.check()?;
        let size = bytes.len();
        metrics::measure(Metric::Dependency(Dependency::S3Get), || -> Result<()> {
            let mut output = std::io::Cursor::new(&mut *bytes);
            self.request_memory_controlled(
                reqwest::Method::GET,
                &format!("{}/{}/{}", self.endpoint, self.bucket, key),
                None,
                None,
                None,
                &mut output,
                size as u64,
                110,
                control,
            )?;
            if output.position() != size as u64 {
                return Err(invalid("immutable source object was truncated"));
            }
            let mut hash = Sha256::new();
            for chunk in bytes.chunks(32 * 1024) {
                control.check()?;
                hash.update(chunk);
            }
            if crate::protocol::hex(&hash.finish()) != expected_digest {
                return Err(invalid("immutable source object digest mismatch"));
            }
            metrics::global().transferred(Direction::Download, size as u64);
            Ok(())
        })?;
        control.check()?;
        Ok(())
    }

    /// Range framing is verified by the transport. The physical pack reader
    /// verifies entry CRC and reconstructed object identity before use; a range
    /// alone is deliberately not represented as a full-pack digest witness.
    pub(crate) fn source_range(
        &self,
        key: &str,
        total: u64,
        offset: u64,
        output: &mut [u8],
        control: &git_rs_core::Control,
    ) -> Result<()> {
        validate_key(key)?;
        if output.is_empty() {
            return Ok(());
        }
        let end = offset
            .checked_add(output.len() as u64)
            .filter(|end| *end <= total)
            .ok_or_else(|| invalid("immutable source range exceeds catalog size"))?;
        metrics::measure(Metric::Dependency(Dependency::S3Get), || -> Result<()> {
            let length = output.len() as u64;
            let mut destination = std::io::Cursor::new(output);
            self.request_memory_controlled(
                reqwest::Method::GET,
                &format!("{}/{}/{}", self.endpoint, self.bucket, key),
                None,
                None,
                Some((offset, end, total)),
                &mut destination,
                length,
                110,
                control,
            )?;
            if destination.position() != length {
                return Err(invalid("immutable source range was truncated"));
            }
            metrics::global().transferred(Direction::Download, length);
            Ok(())
        })
    }

    /// The normalized pack/index owners remain admitted in the caller through
    /// both PUTs and SQL publication. Durable intent precedes even the first PUT.
    pub fn upload_bytes(
        &self,
        repo: &str,
        operation: &str,
        native_hash: &str,
        pack_bytes: &[u8],
        index_bytes: &[u8],
    ) -> Result<Pack> {
        crate::protocol::Oid::parse(native_hash.as_bytes())?;
        let digest = |bytes: &[u8]| -> Result<String> {
            let mut hash = Sha256::new();
            for chunk in bytes.chunks(32 * 1024) {
                process::check_operation()?;
                hash.update(chunk);
            }
            Ok(crate::protocol::hex(&hash.finish()))
        };
        let prefix = format!("repos/{repo}/operations/{operation}/{native_hash}");
        let pack = Pack {
            hash: native_hash.to_owned(),
            pack_key: format!("{prefix}.pack"),
            idx_key: format!("{prefix}.idx"),
            pack_size: pack_bytes.len() as u64,
            idx_size: index_bytes.len() as u64,
            pack_digest: digest(pack_bytes)?,
            idx_digest: digest(index_bytes)?,
        };
        validate_key(&pack.pack_key)?;
        validate_key(&pack.idx_key)?;
        crate::metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; INSERT INTO staged_objects(repo_id,operation_id,object_key,state) VALUES({}::uuid,{}::uuid,{},'planned'),({}::uuid,{}::uuid,{},'planned'); COMMIT;",
            crate::metadata::text(repo),
            crate::metadata::text(operation),
            crate::metadata::text(&pack.pack_key),
            crate::metadata::text(repo),
            crate::metadata::text(operation),
            crate::metadata::text(&pack.idx_key),
        ))?;
        for (key, bytes, hash) in [
            (&pack.pack_key, pack_bytes, &pack.pack_digest),
            (&pack.idx_key, index_bytes, &pack.idx_digest),
        ] {
            metrics::measure(Metric::Dependency(Dependency::S3Put), || {
                self.request_memory(
                    reqwest::Method::PUT,
                    &format!("{}/{}/{}", self.endpoint, self.bucket, key),
                    Some(hash),
                    Some(bytes),
                    None,
                    &mut std::io::sink(),
                    4096,
                    110,
                )
            })?;
            metrics::global().transferred(Direction::Upload, bytes.len() as u64);
            let updated = crate::metadata::sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; UPDATE staged_objects s SET state='uploaded' WHERE repo_id={}::uuid AND operation_id={}::uuid AND object_key={} AND retired_at IS NULL AND NOT EXISTS(SELECT 1 FROM retired_objects t WHERE t.object_key=s.object_key) RETURNING 1; COMMIT;",
                crate::metadata::text(repo),
                crate::metadata::text(operation),
                crate::metadata::text(key),
            ))?;
            if updated.trim() != "1" {
                return Err(invalid("upload intent was retired or replaced"));
            }
        }
        Ok(pack)
    }

    pub(crate) fn get_cache_bytes(&self, key: &str, size: usize) -> Result<Vec<u8>> {
        validate_key(key)?;
        let mut bytes = Vec::with_capacity(size);
        self.request_memory(
            reqwest::Method::GET,
            &format!("{}/{}/{}", self.endpoint, self.bucket, key),
            None,
            None,
            None,
            &mut bytes,
            size as u64,
            30,
        )?;
        if bytes.len() != size {
            return Err(invalid("cache object size mismatch"));
        }
        Ok(bytes)
    }

    pub fn delete_retired(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        metrics::measure(Metric::Dependency(Dependency::S3Delete), || {
            self.request_memory(
                reqwest::Method::DELETE,
                &format!("{}/{}/{}", self.endpoint, self.bucket, key),
                None,
                None,
                None,
                &mut std::io::sink(),
                4096,
                110,
            )
        })
    }

    /// Physical reclamation currently supports only never-versioned buckets.
    /// Enabled/suspended/unknown policies require a version-aware GC instead.
    pub fn require_unversioned(&self) -> Result<()> {
        let mut response = Vec::new();
        metrics::measure(Metric::Dependency(Dependency::S3Versioning), || {
            self.request_memory(
                reqwest::Method::GET,
                &format!("{}/{}?versioning", self.endpoint, self.bucket),
                None,
                None,
                None,
                &mut response,
                16384,
                30,
            )
        })?;
        xml::document(&response, b"VersioningConfiguration", false)?;
        Ok(())
    }
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 1024
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"/-_.".contains(&byte))
        || key
            .split('/')
            .any(|component| component == ".." || component == ".")
    {
        return Err(invalid("invalid object key"));
    }
    Ok(())
}
