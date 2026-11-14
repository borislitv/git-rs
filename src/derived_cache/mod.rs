//! Optional, independently migrated derived artifacts. Source authorization and
//! reader ownership remain with the caller; cache leases never grant source access.
mod artifacts;
mod build;
mod codec;
mod config;
mod coordinator;
mod key;
mod maintenance;
mod payload;
mod postgres;

pub use artifacts::{
    ArtifactKind, BlameArtifacts, CheckpointVersion, CompleteArtifact, CompleteCheckpoint,
    MAX_BODY_BYTES, MAX_CHUNK_BYTES, StagedChunk,
};
pub use build::{BlameBuilds, BuildLease, BuildLimits, BuildLookup};
pub use config::CacheConfig;
pub use coordinator::{Coordinator, Derivation};
pub use key::{ArtifactKey, KeyParts};
pub use maintenance::{CacheMaintenance, MaintenanceLease};
pub use payload::Payload;

use crate::{Result, invalid, process};
use payload::{Budget, Objects, OwnedBytes, ReadHold};
use postgres::Postgres;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// The application supplies an authorized, independently owned source hold.
/// Implementations check ACL/security scope AND current source lifetime/fences.
/// Retaining this object must retain the actual source hold, not just its ID.
pub trait Access: Send + Sync {
    fn check(&self, key: &ArtifactKey) -> Result<()>;
}

pub(crate) struct Inner {
    config: CacheConfig,
    pg: Postgres,
    objects: Option<Objects>,
    budget: Arc<Budget>,
}

#[derive(Clone)]
pub struct DerivedCache {
    inner: Arc<Inner>,
}

pub struct Claim {
    cache: Arc<Inner>,
    key: ArtifactKey,
    token: String,
    generation: i64,
}

pub enum Lookup {
    Hit(Payload),
    Claimed(Claim),
    Pending,
}

impl DerivedCache {
    pub fn from_env() -> Result<Self> {
        Self::open(CacheConfig::from_env()?)
    }

    /// Does not migrate or create a namespace. Operators run cache-migrate first.
    pub fn open(config: CacheConfig) -> Result<Self> {
        config.validate()?;
        let pg = Postgres::from_env(config.connections)?;
        let objects = Objects::from_env(config.object_prefix())?;
        let budget = Budget::new(config.memory_bytes);
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                pg,
                objects,
                budget,
            }),
        })
    }

    pub fn migrate(&self) -> Result<()> {
        self.inner.pg.migrate()?;
        let c = &self.inner.config;
        self.inner.pg.query(
            "SELECT 1 FROM git_rs_cache.register_namespace($1::text::uuid,$2::text::uuid,$3,$4)",
            &[
                &c.namespace,
                &c.owner,
                &c.namespace_rows,
                &c.namespace_bytes,
            ],
            1,
            64,
        )?;
        Ok(())
    }

    pub fn allocated(&self) -> usize {
        self.inner.budget.used()
    }

    pub fn get_verified(
        &self,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
    ) -> Result<Option<Payload>> {
        process::check_operation()?;
        access.check(key)?;
        let c = &self.inner.config;
        let reader = config::identifier()?;
        let started = Instant::now();
        let rows = self.inner.pg.query(
            "SELECT * FROM git_rs_cache.acquire_read_v4($1::text::uuid,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7)",
            &[&c.namespace, &c.owner, &&key.hash[..], &key.canonical, &reader, &c.read_seconds, &(c.max_payload_bytes as i64)], 1, 1024)?;
        self.load_read(key, access, reader, started, rows)
    }

    fn load_read(
        &self,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
        reader: String,
        started: Instant,
        rows: Vec<tokio_postgres::Row>,
    ) -> Result<Option<Payload>> {
        let c = &self.inner.config;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let id: String = row.try_get(0)?;
        let lifetime = u64::try_from(row.try_get::<_, i64>(4)?.max(0))?;
        let hold = ReadHold {
            cache: self.inner.clone(),
            reader,
            payload: id.clone(),
            deadline: started + Duration::from_millis(lifetime),
        };
        let loaded = (|| -> Result<OwnedBytes> {
            hold.check()?;
            let metadata = codec::Metadata::parse(
                row.try_get(5)?,
                row.try_get(2)?,
                row.try_get(1)?,
                row.try_get(6)?,
                row.try_get(7)?,
                c.max_payload_bytes,
            )?;
            let object_key: Option<&str> = row.try_get(3)?;
            // Binary PG protocol/Row and HTTP transfer scratch remain real
            // allocations. Admit conservatively before fetching the stored body.
            let mut wire = self.inner.budget.reserve(
                metadata
                    .stored_size
                    .checked_mul(4)
                    .and_then(|size| size.checked_add(65536))
                    .ok_or_else(|| invalid("cache wire allocation overflow"))?,
            )?;
            let data = if let Some(object_key) = object_key {
                let bytes = self
                    .inner
                    .objects
                    .as_ref()
                    .ok_or_else(|| invalid("cache object storage not configured"))?
                    .get(object_key, metadata.stored_size)?;
                if metadata.encoding == codec::Encoding::Raw {
                    metadata.verify_stored(&bytes)?;
                    metadata.verify_logical(&bytes)?;
                    OwnedBytes::from_reserved(bytes, &mut wire)?
                } else {
                    codec::decode(&bytes, &metadata, &self.inner.budget)?
                }
            } else {
                let stored_size = i32::try_from(metadata.stored_size)?;
                let rows = self.inner.pg.query(
                    "SELECT p.body FROM git_rs_cache.payloads p JOIN git_rs_cache.read_holds h ON h.namespace=p.namespace AND h.payload=p.id WHERE p.namespace=$1::text::uuid AND p.id=$2::text::uuid AND h.reader=$3::text::uuid AND h.lease_until>clock_timestamp() AND octet_length(p.body)=$4",
                    &[&c.namespace, &id, &hold.reader, &stored_size], 1, metadata.stored_size)?;
                let row = rows
                    .first()
                    .ok_or_else(|| invalid("cache payload absent or expired"))?;
                codec::decode(row.try_get::<_, &[u8]>(0)?, &metadata, &self.inner.budget)?
            };
            hold.check()?;
            Ok(data)
        })();
        match loaded {
            Ok(data) => {
                access.check(key)?;
                Ok(Some(Payload {
                    data: Arc::new(data),
                    hold: Some(Arc::new(hold)),
                    access,
                    key: key.clone(),
                }))
            }
            Err(error) => {
                process::check_operation()?;
                access.check(key)?;
                // Compare payload identity: an old corrupt read cannot invalidate
                // a new owner's successful replacement.
                let _ = self.invalidate(key, &id);
                drop(error);
                Ok(None)
            }
        }
    }

    pub fn get_or_claim(&self, key: &ArtifactKey, access: Arc<dyn Access>) -> Result<Lookup> {
        if let Some(payload) = self.get_verified(key, access.clone())? {
            return Ok(Lookup::Hit(payload));
        }
        access.check(key)?;
        let c = &self.inner.config;
        let token = config::identifier()?;
        let query = || {
            self.inner.pg.query(
            "SELECT * FROM git_rs_cache.claim($1::text::uuid,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7)",
            &[&c.namespace, &c.owner, &&key.hash[..], &key.canonical, &token, &c.lease_seconds, &c.job_seconds], 1, 64)
        };
        // Reusing this exact attempt is idempotent, including an unknown COMMIT.
        // An expired token cannot be revived by this confirmation query.
        let rows = query().or_else(|_| {
            process::check_operation()?;
            query()
        })?;
        access.check(key)?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Ok(Lookup::Pending);
        }
        Ok(Lookup::Claimed(Claim {
            cache: self.inner.clone(),
            key: key.clone(),
            token,
            generation: rows[0].try_get(1)?,
        }))
    }

    pub fn renew(&self, claim: &Claim) -> Result<bool> {
        self.check_claim(claim)?;
        let c = &self.inner.config;
        let rows = self.inner.pg.query(
            "SELECT git_rs_cache.renew($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6)",
            &[
                &c.namespace,
                &c.owner,
                &&claim.key.hash[..],
                &claim.token,
                &claim.generation,
                &c.lease_seconds,
            ],
            1,
            64,
        )?;
        Ok(rows[0].try_get(0)?)
    }

    pub fn publish(&self, claim: &Claim, access: &dyn Access, bytes: &[u8]) -> Result<bool> {
        self.publish_inner(claim, access, bytes, None)
    }

    fn publish_inner(
        &self,
        claim: &Claim,
        access: &dyn Access,
        bytes: &[u8],
        manifest: Option<&artifacts::ManifestCommit<'_>>,
    ) -> Result<bool> {
        self.check_claim(claim)?;
        access.check(&claim.key)?;
        let c = &self.inner.config;
        if bytes.len() > c.max_payload_bytes {
            return Err(invalid("artifact payload limit exceeded"));
        }
        let encoded = codec::encode(bytes, &self.inner.budget, c.max_payload_bytes)?;
        let metadata = &encoded.metadata;
        let stored = encoded.bytes();
        let external = stored.len() > c.inline_bytes;
        if external && self.inner.objects.is_none() {
            return Err(invalid(
                "large cache payload requires separate cache S3 configuration",
            ));
        }
        // PG binary parameter encoding and bounded S3 transport chunks are not zero-copy.
        // The physical candidate and native context have separate owned charges.
        let _working = self.inner.budget.reserve(
            stored
                .len()
                .checked_mul(4)
                .and_then(|size| size.checked_add(65536))
                .ok_or_else(|| invalid("cache upload allocation overflow"))?,
        )?;
        let digest = metadata.logical_digest;
        let id = match manifest.and_then(|manifest| manifest.checkpoint_payload) {
            Some(id) => id.to_owned(),
            None => config::identifier()?,
        };
        let prepare = || {
            self.inner.pg.query(
            "SELECT git_rs_cache.prepare_payload_v4($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6::text::uuid,$7,$8,$9,$10,$11,$12)",
            &[&c.namespace, &c.owner, &&claim.key.hash[..], &claim.token, &claim.generation, &id,
                &&digest[..], &(metadata.logical_size as i64), &external, &metadata.encoding.as_str(),
                &&metadata.stored_digest[..], &(metadata.stored_size as i64)], 1, 64)
        };
        let prepared = prepare().or_else(|_| {
            process::check_operation()?;
            prepare()
        })?;
        if !prepared[0].try_get::<_, bool>(0)? {
            return Ok(false);
        }
        if external {
            let objects = self
                .inner
                .objects
                .as_ref()
                .expect("validated cache S3 configuration");
            let result = objects.put(
                &format!("{}{id}", c.object_prefix()),
                stored,
                &metadata.stored_digest,
            );
            let confirmed = result.is_ok();
            // The durable intent precedes PUT. Preserve ambiguous outcomes even
            // when the request/owner expired while the remote store accepted it.
            let record = process::Operation::new(2).enter(|| self.inner.pg.query(
                "SELECT git_rs_cache.upload_result($1::text::uuid,$2::text::uuid,$3::text::uuid,$4)",
                &[&c.namespace, &c.owner, &id, &confirmed], 1, 64));
            result?;
            record?;
        }
        process::check_operation()?;
        access.check(&claim.key)?;
        let inline = (!external).then_some(stored);
        let published = if let Some(manifest) = manifest {
            manifest.commit(self, claim, &id, inline)
        } else {
            self.inner.pg.query(
            "SELECT git_rs_cache.publish_v4($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6::text::uuid,$7,$8)",
            &[&c.namespace, &c.owner, &&claim.key.hash[..], &claim.token, &claim.generation, &id, &inline, &c.ttl_seconds], 1, 64)
        };
        match published {
            Ok(rows) => Ok(rows[0].try_get(0)?),
            Err(error) => {
                process::check_operation()?;
                let rows = self.inner.pg.query(
                    "SELECT EXISTS(SELECT 1 FROM git_rs_cache.directory d JOIN git_rs_cache.payloads p ON p.namespace=d.namespace AND p.id=d.payload JOIN git_rs_cache.jobs j ON j.namespace=d.namespace AND j.key_hash=d.key_hash WHERE d.namespace=$1::text::uuid AND d.key_hash=$2 AND d.payload=$3::text::uuid AND p.digest=$4 AND p.size=$5 AND p.encoding=$6 AND coalesce(p.stored_digest,p.digest)=$7 AND coalesce(p.stored_size,p.size)=$8 AND j.canonical=$9 AND p.token=$10::text::uuid AND p.generation=$11 AND (p.object_key IS NOT NULL)=$12 AND d.state='ready' AND p.state='live' AND d.expires_at>clock_timestamp())",
                    &[&c.namespace, &&claim.key.hash[..], &id, &&digest[..], &(metadata.logical_size as i64),
                        &metadata.encoding.as_str(), &&metadata.stored_digest[..], &(metadata.stored_size as i64),
                        &claim.key.canonical, &claim.token, &claim.generation, &external], 1, 64)?;
                if rows[0].try_get::<_, bool>(0)? {
                    Ok(true)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn invalidate(&self, key: &ArtifactKey, payload_id: &str) -> Result<bool> {
        if !config::uuid(payload_id) {
            return Err(invalid("invalid cache payload identity"));
        }
        let c = &self.inner.config;
        let rows = self.inner.pg.query(
            "SELECT git_rs_cache.invalidate($1::text::uuid,$2::text::uuid,$3,$4::text::uuid)",
            &[&c.namespace, &c.owner, &&key.hash[..], &payload_id],
            1,
            64,
        )?;
        Ok(rows[0].try_get(0)?)
    }

    fn check_claim(&self, claim: &Claim) -> Result<()> {
        process::check_operation()?;
        if !Arc::ptr_eq(&self.inner, &claim.cache) {
            return Err(invalid("claim belongs to another cache instance"));
        }
        Ok(())
    }
}

fn hash(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut digest = openssl::sha::Sha256::new();
    for chunk in bytes.chunks(65536) {
        process::check_operation()?;
        digest.update(chunk);
    }
    Ok(digest.finish())
}
