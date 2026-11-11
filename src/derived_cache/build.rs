//! Versioned finite cache authority, not source authorization or a CPU executor.
use super::{Access, ArtifactKey, Claim, DerivedCache, Inner, config};
use crate::{
    Result, invalid,
    process::{self, Operation},
};
use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct BuildLimits {
    pub hard_seconds: i32,
    pub lease_seconds: i32,
}
impl Default for BuildLimits {
    fn default() -> Self {
        Self {
            hard_seconds: 3600,
            lease_seconds: 15,
        }
    }
}
impl BuildLimits {
    pub fn validate(self) -> Result<()> {
        if !(1..=3600).contains(&self.hard_seconds)
            || !(1..=60.min(self.hard_seconds)).contains(&self.lease_seconds)
        {
            return Err(invalid("invalid finite blame-build limits"));
        }
        Ok(())
    }
}

pub struct BlameBuilds {
    cache: DerivedCache,
}
pub enum BuildLookup {
    Claimed(BuildLease),
    Pending,
}

/// The caller retains its actual-work permit/provider alongside this owner.
/// `access` must own an independent source hold covering the whole build plus
/// cleanup. A cache lease never extends a request's source lease or authorizes it.
pub struct BuildLease {
    pub(super) cache: Arc<Inner>,
    pub(super) key: ArtifactKey,
    pub(super) token: String,
    pub(super) generation: i64,
    pub(super) access: Arc<dyn Access>,
    deadline: Instant,
    lease_seconds: i32,
}

impl BlameBuilds {
    pub fn new(cache: &DerivedCache) -> Self {
        Self {
            cache: cache.clone(),
        }
    }

    /// Errors are optional-cache failures: callers may recompute under their
    /// separately admitted finite source/CPU budget, never fail a committed push.
    pub fn claim(
        &self,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
        limits: BuildLimits,
    ) -> Result<BuildLookup> {
        process::remaining()?;
        limits.validate()?;
        access.check(key)?;
        let c = &self.cache.inner.config;
        let token = config::identifier()?;
        let started = Instant::now();
        let query = || {
            self.cache.inner.pg.query(
            "SELECT * FROM git_rs_cache.claim_blame_build($1::text::uuid,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7)",
            &[&c.namespace, &c.owner, &&key.hash[..], &key.canonical, &token, &limits.lease_seconds, &limits.hard_seconds], 1, 64)
        };
        // Reconfirm the same attempt after an ambiguous implicit COMMIT. Never
        // issue a fresh token/hard deadline as a retry of this claim.
        let rows = query().or_else(|_| {
            process::check_operation()?;
            query()
        })?;
        access.check(key)?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Ok(BuildLookup::Pending);
        }
        let remaining = u64::try_from(rows[0].try_get::<_, i64>(2)?.max(0))?;
        let lease = BuildLease {
            cache: self.cache.inner.clone(),
            key: key.clone(),
            token,
            generation: rows[0].try_get(1)?,
            access,
            deadline: started + Duration::from_millis(remaining),
            lease_seconds: limits.lease_seconds,
        };
        lease.check_local()?;
        Ok(BuildLookup::Claimed(lease))
    }
}

impl BuildLease {
    pub fn key(&self) -> &ArtifactKey {
        &self.key
    }
    /// Conservative monotonic deadline, anchored before the claim/confirmation
    /// round trips. Renewals cannot move it or the database hard deadline.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Cheap cooperative poll; safe in bounded CPU loops. No SQL/source I/O.
    pub fn check_local(&self) -> Result<()> {
        process::check_operation()?;
        if Instant::now() >= self.deadline {
            return Err(invalid("blame-build hard deadline exceeded"));
        }
        Ok(())
    }
    /// Batch-boundary authority check, not a per-line/delta callback.
    pub fn check_authority(&self) -> Result<()> {
        self.check_local()?;
        self.access.check(&self.key)?;
        let c = &self.cache.config;
        let rows = self.cache.pg.query(
            "SELECT git_rs_cache.check_blame_build($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5)",
            &[&c.namespace, &c.owner, &&self.key.hash[..], &self.token, &self.generation], 1, 64)?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Err(invalid("expired blame-build authority"));
        }
        self.check_local()
    }
    pub fn renew(&self) -> Result<bool> {
        self.check_local()?;
        let c = &self.cache.config;
        let rows = self.cache.pg.query(
            "SELECT git_rs_cache.renew_blame_build($1::text::uuid,$2::text::uuid,$3,$4::text::uuid,$5,$6)",
            &[&c.namespace, &c.owner, &&self.key.hash[..], &self.token, &self.generation, &self.lease_seconds], 1, 64)?;
        self.check_local()?;
        Ok(rows[0].try_get(0)?)
    }

    /// Executes on the caller's already-admitted execution thread, with one
    /// scoped I/O heartbeat. This creates neither a CPU pool nor suspended-DAG
    /// state. One waiter's cancellation must not cancel this independently owned
    /// job; callers arrange source/permit ownership before invoking it.
    pub fn run_with_renewal<T: Send>(&self, work: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        self.check_local()?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        let operation =
            Operation::new(remaining.as_secs() + u64::from(remaining.subsec_nanos() != 0));
        operation.enter(|| {
            self.check_authority()?;
            std::thread::scope(|scope| {
                let (done, finished) = mpsc::channel();
                let operation = operation.clone();
                scope.spawn(move || {
                    let interval =
                        Duration::from_millis((self.lease_seconds as u64 * 1000 / 3).max(100));
                    while finished.recv_timeout(interval) == Err(mpsc::RecvTimeoutError::Timeout) {
                        if !operation.enter(|| self.renew()).unwrap_or(false) {
                            break;
                        }
                    }
                });
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let value = work(self)?;
                    // Losing optional cache authority forbids further durable
                    // publication, not use of a complete local result. Source
                    // authority and the independent hard CPU budget still fence
                    // that result; publication APIs check cache authority anew.
                    self.check_local()?;
                    self.access.check(&self.key)?;
                    Ok(value)
                }))
                .unwrap_or_else(|_| Err(invalid("blame-build callback panicked")));
                let _ = done.send(());
                result
            })
        })
    }

    pub(super) fn claim_payload(
        &self,
        cache: &DerivedCache,
        key: &ArtifactKey,
    ) -> Result<Option<Claim>> {
        self.check_authority()?;
        self.check_cache(cache)?;
        let c = &self.cache.config;
        let token = config::identifier()?;
        let query = || {
            self.cache.pg.query(
            "SELECT * FROM git_rs_cache.claim_blame_payload($1::text::uuid,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7,$8,$9::text::uuid,$10)",
            &[&c.namespace, &c.owner, &&key.hash[..], &key.canonical, &token, &c.lease_seconds, &c.job_seconds,
                &&self.key.hash[..], &self.token, &self.generation], 1, 64)
        };
        let rows = query().or_else(|_| {
            self.check_local()?;
            query()
        })?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Ok(None);
        }
        Ok(Some(Claim {
            cache: self.cache.clone(),
            key: key.clone(),
            token,
            generation: rows[0].try_get(1)?,
        }))
    }

    pub(super) fn check_cache(&self, cache: &DerivedCache) -> Result<()> {
        if !Arc::ptr_eq(&self.cache, &cache.inner) {
            return Err(invalid("build belongs to another cache instance"));
        }
        Ok(())
    }

    pub(super) fn claim_checkpoint(
        &self,
        cache: &DerivedCache,
        previous: Option<(&str, &[u8])>,
    ) -> Result<Option<Claim>> {
        process::remaining()?;
        self.check_cache(cache)?;
        self.check_authority()?;
        let c = &self.cache.config;
        let token = config::identifier()?;
        let previous_id = previous.map(|(id, _)| id);
        let previous_digest = previous.map(|(_, digest)| digest);
        let query = || {
            self.cache.pg.query(
                "SELECT * FROM git_rs_cache.claim_checkpoint($1::text::uuid,$2::text::uuid,$3,$4,$5::text::uuid,$6,$7,$8::text::uuid,$9,$10::text::uuid,$11)",
                &[&c.namespace, &c.owner, &&self.key.hash[..], &self.key.canonical, &token,
                    &c.lease_seconds, &c.job_seconds, &self.token, &self.generation, &previous_id, &previous_digest], 1, 64)
        };
        // Reuse exactly this token and expected version after an unknown COMMIT.
        let rows = query().or_else(|_| {
            self.check_local()?;
            query()
        })?;
        if !rows[0].try_get::<_, bool>(0)? {
            return Ok(None);
        }
        Ok(Some(Claim {
            cache: self.cache.clone(),
            key: self.key.clone(),
            token,
            generation: rows[0].try_get(1)?,
        }))
    }
}
