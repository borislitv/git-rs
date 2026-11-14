use super::{DerivedCache, Inner};
use crate::{Result, invalid, process};
use std::sync::Arc;

pub struct CacheMaintenance {
    cache: Arc<Inner>,
}
pub struct MaintenanceLease {
    cache: Arc<Inner>,
    token: String,
    generation: i64,
}

impl CacheMaintenance {
    pub fn new(cache: &DerivedCache) -> Self {
        Self {
            cache: cache.inner.clone(),
        }
    }

    pub fn claim(&self) -> Result<Option<MaintenanceLease>> {
        process::remaining()?;
        let c = &self.cache.config;
        let token = super::config::identifier()?;
        let query = || {
            self.cache.pg.query("SELECT * FROM git_rs_cache.claim_maintenance($1::text::uuid,$2::text::uuid,$3::text::uuid,$4)",
            &[&c.namespace, &c.owner, &token, &c.lease_seconds], 1, 64)
        };
        let rows = query().or_else(|_| {
            process::check_operation()?;
            query()
        })?;
        Ok(rows[0].try_get::<_, bool>(0)?.then(|| MaintenanceLease {
            cache: self.cache.clone(),
            token,
            generation: rows[0].get(1),
        }))
    }

    fn check(&self, lease: &MaintenanceLease) -> Result<()> {
        process::remaining()?;
        if !Arc::ptr_eq(&self.cache, &lease.cache) {
            return Err(invalid(
                "cache maintenance lease belongs to another instance",
            ));
        }
        Ok(())
    }

    pub fn expire_batch(&self, lease: &MaintenanceLease) -> Result<i64> {
        self.check(lease)?;
        let c = &self.cache.config;
        let rows = self.cache.pg.query(
            "SELECT git_rs_cache.expire_batch($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5)",
            &[
                &c.namespace,
                &c.owner,
                &lease.token,
                &lease.generation,
                &c.batch_rows,
            ],
            1,
            64,
        )?;
        Ok(rows[0].try_get(0)?)
    }

    pub fn retire_payloads(&self, lease: &MaintenanceLease) -> Result<i64> {
        self.retire(lease, false)
    }

    pub fn reconcile_uploads(&self, lease: &MaintenanceLease) -> Result<i64> {
        self.retire(lease, true)
    }

    fn retire(&self, lease: &MaintenanceLease, intents_only: bool) -> Result<i64> {
        self.check(lease)?;
        let c = &self.cache.config;
        let rows = self.cache.pg.query("SELECT git_rs_cache.retire_payloads($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5,$6,$7)",
            &[&c.namespace, &c.owner, &lease.token, &lease.generation, &c.batch_rows, &c.grace_seconds, &intents_only], 1, 64)?;
        Ok(rows[0].try_get(0)?)
    }

    pub fn delete_retired(&self, lease: &MaintenanceLease) -> Result<usize> {
        self.check(lease)?;
        let c = &self.cache.config;
        let rows = self.cache.pg.query("SELECT * FROM git_rs_cache.deletion_batch($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5)",
            &[&c.namespace, &c.owner, &lease.token, &lease.generation, &c.batch_rows], c.batch_rows as usize, 32768)?;
        let mut deleted = 0;
        for row in rows {
            process::check_operation()?;
            let id: String = row.try_get(0)?;
            let key: Option<String> = row.try_get(1)?;
            if let Some(key) = key {
                self.cache.pg.query("SELECT 1 FROM git_rs_cache.guard_maintenance($1::text::uuid,$2::text::uuid,$3::text::uuid,$4)",
                    &[&c.namespace, &c.owner, &lease.token, &lease.generation], 1, 64)?;
                // A delayed request remains harmless: these exact keys are
                // irreversibly retired, never reused by any publisher.
                self.cache
                    .objects
                    .as_ref()
                    .ok_or_else(|| invalid("cache S3 not configured"))?
                    .delete(&key)?;
            }
            self.cache.pg.query("SELECT 1 FROM git_rs_cache.record_delete($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5::text::uuid)",
                &[&c.namespace, &c.owner, &lease.token, &lease.generation, &id], 1, 64)?;
            deleted += 1;
        }
        Ok(deleted)
    }
}
