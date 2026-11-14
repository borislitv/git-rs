use super::{Access, ArtifactKey, Inner};
use crate::{Result, blob::Store, invalid, process};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

pub(crate) struct Budget {
    used: AtomicUsize,
    limit: usize,
}
pub(crate) struct Reservation {
    budget: Arc<Budget>,
    amount: usize,
}
impl Reservation {
    /// Transfer already admitted capacity to an owner without admitting it again.
    pub fn take(&mut self, amount: usize) -> Result<Self> {
        if amount > self.amount {
            return Err(invalid("cache allocation exceeds admitted capacity"));
        }
        self.amount -= amount;
        Ok(Self {
            budget: self.budget.clone(),
            amount,
        })
    }
}
impl Budget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }
    pub fn reserve(self: &Arc<Self>, amount: usize) -> Result<Reservation> {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount)
                    .filter(|total| *total <= self.limit)
            })
            .map_err(|_| invalid("cache RAM budget exhausted"))?;
        Ok(Reservation {
            budget: self.clone(),
            amount,
        })
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.amount, Ordering::AcqRel);
    }
}

pub(crate) struct OwnedBytes {
    pub bytes: Vec<u8>,
    _reservation: Reservation,
}
impl OwnedBytes {
    pub fn from_reserved(bytes: Vec<u8>, working: &mut Reservation) -> Result<Self> {
        let reservation = working.take(bytes.capacity())?;
        Ok(Self {
            bytes,
            _reservation: reservation,
        })
    }

    /// Admission precedes allocation, including bounded capacity headroom.
    /// Native/wire workspace has its own independent reservation.
    pub fn allocate(size: usize, budget: &Arc<Budget>) -> Result<Self> {
        process::check_operation()?;
        let mut working = budget.reserve(
            size.checked_add(size.min(65536))
                .ok_or_else(|| invalid("cache allocation size overflow"))?,
        )?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size)?;
        let reservation = working.take(bytes.capacity())?;
        let mut owner = Self {
            bytes,
            _reservation: reservation,
        };
        while owner.bytes.len() < size {
            process::check_operation()?;
            owner.bytes.resize((owner.bytes.len() + 65536).min(size), 0);
        }
        Ok(owner)
    }

    pub fn new(bytes: Vec<u8>, budget: &Arc<Budget>) -> Result<Arc<Self>> {
        let reservation = budget.reserve(bytes.capacity())?;
        Ok(Arc::new(Self {
            bytes,
            _reservation: reservation,
        }))
    }
}

pub(crate) struct ReadHold {
    pub cache: Arc<Inner>,
    pub reader: String,
    pub payload: String,
    pub deadline: Instant,
}
impl ReadHold {
    pub fn check(&self) -> Result<()> {
        process::check_operation()?;
        if Instant::now() >= self.deadline {
            return Err(invalid("cache read hold expired"));
        }
        let rows = self.cache.pg.query(
            "SELECT EXISTS(SELECT 1 FROM git_rs_cache.read_holds WHERE namespace=$1::text::uuid AND reader=$2::text::uuid AND payload=$3::text::uuid AND lease_until>clock_timestamp())",
            &[&self.cache.config.namespace, &self.reader, &self.payload], 1, 64)?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Err(invalid("cache read hold expired"));
        }
        Ok(())
    }
}
impl Drop for ReadHold {
    fn drop(&mut self) {
        process::Operation::new(2).enter(|| {
            let _ = self.cache.pg.query(
                "SELECT git_rs_cache.release_read($1::text::uuid,$2::text::uuid,$3::text::uuid)",
                &[
                    &self.cache.config.namespace,
                    &self.cache.config.owner,
                    &self.reader,
                ],
                1,
                64,
            );
        });
    }
}

/// Bytes are immutable and RAM-owned. Consume once per artifact, not per line.
/// Access/source authority and the cache read hold are checked on both sides of
/// consumption; callbacks must buffer success rather than stream partial output.
pub struct Payload {
    pub(crate) data: Arc<OwnedBytes>,
    pub(crate) hold: Option<Arc<ReadHold>>,
    pub(crate) access: Arc<dyn Access>,
    pub(crate) key: ArtifactKey,
}
impl Payload {
    pub fn identity(&self) -> Option<&str> {
        self.hold.as_ref().map(|hold| hold.payload.as_str())
    }
    pub fn len(&self) -> usize {
        self.data.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.bytes.is_empty()
    }
    pub fn with_bytes<T>(&self, consume: impl FnOnce(&[u8]) -> Result<T>) -> Result<T> {
        let check = || -> Result<()> {
            process::check_operation()?;
            self.access.check(&self.key)?;
            if let Some(hold) = &self.hold {
                hold.check()?;
            }
            Ok(())
        };
        check()?;
        let value = consume(&self.data.bytes)?;
        check()?;
        Ok(value)
    }
}

pub(crate) struct Objects {
    store: Store,
    prefix: String,
}
impl Objects {
    pub fn from_env(prefix: String) -> Result<Option<Self>> {
        if std::env::var_os("GIT_RS_CACHE_S3_ENDPOINT").is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            store: Store::from_cache_env()?,
            prefix,
        }))
    }
    fn validate(&self, key: &str) -> Result<()> {
        if !key
            .strip_prefix(&self.prefix)
            .is_some_and(super::config::uuid)
        {
            return Err(invalid("object outside owned cache namespace"));
        }
        Ok(())
    }
    pub fn put(&self, key: &str, bytes: &[u8], digest: &[u8]) -> Result<()> {
        self.validate(key)?;
        self.store
            .put_cache_bytes(key, bytes, &crate::protocol::hex(digest))
    }
    pub fn get(&self, key: &str, size: usize) -> Result<Vec<u8>> {
        self.validate(key)?;
        self.store.get_cache_bytes(key, size)
    }
    pub fn delete(&self, key: &str) -> Result<()> {
        self.validate(key)?;
        self.store.require_unversioned()?;
        self.store.delete_retired(key)
    }
}
