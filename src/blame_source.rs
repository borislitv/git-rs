//! Job-owned physical ranges and optional authenticated object reuse for blame.

use crate::{Result, blob::Store, cache::Cache, invalid, object_source::RangeStore};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, VerifiedObjectProvider,
    objects::VerifiedObject,
    pack::{PackLocation, ranges::RangePackSet},
};
use std::sync::Arc;

pub(crate) use crate::object_source::Source;

pub(crate) struct LazySnapshot {
    packs: RangePackSet<RangeStore>,
    work: Option<crate::blame::memo::WorkSet>,
    budget: CapacityBudget,
    limits: ReadLimits,
}

impl LazySnapshot {
    pub fn new(
        source: Arc<Source>,
        store: Store,
        cache: Arc<Cache>,
        budget: CapacityBudget,
        limits: ReadLimits,
        control: &Control,
    ) -> Result<Self> {
        Ok(Self {
            packs: RangeStore::open(source, store, cache, budget.clone(), limits, control)?,
            work: None,
            budget,
            limits,
        })
    }

    pub fn with_workset(mut self, work: Option<crate::blame::memo::WorkSet>) -> Self {
        self.work = work;
        self
    }
}

impl VerifiedObjectProvider for LazySnapshot {
    fn identity(&self) -> &SnapshotIdentity {
        self.packs.identity()
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        self.packs.header(id, control)
    }
    fn object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.verified_object(id, control, budget, limits)?
            .map(|object| object.into_object(control, budget))
            .transpose()
    }
    fn verified_object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        control.check()?;
        if !budget.shares_with(&self.budget) {
            return Err(invalid("source budget identity mismatch"));
        }
        let Some(header) = self.packs.header(id, control)? else {
            return Ok(None);
        };
        // A checkpoint owns bytes rather than a physical decoder-chain plan.
        // Preserve the original restriction: tighter chain limits must use the
        // physical decoder and cannot infer a plan from authenticated bytes.
        if let Some(work) = &self.work
            && limits.max_delta_depth == self.limits.max_delta_depth
            && limits.max_decode_bytes == self.limits.max_decode_bytes
        {
            let object = work
                .lock()
                .map_err(|_| invalid("blame workset poisoned"))?
                .object(id.to_owned(), header.kind, control)?;
            if let Some(object) = object {
                if object.kind != header.kind || object.data.as_slice().len() as u64 != header.size
                {
                    return Err(invalid("reusable source header mismatch"));
                }
                return VerifiedObject::verify(id, object, control, budget, limits).map(Some);
            }
        }
        let object = self.packs.verified_object(id, control, budget, limits)?;
        // No source/cache borrow or network I/O survives entry into the WorkSet.
        if let (Some(work), Some(object)) = (&self.work, &object) {
            work.lock()
                .map_err(|_| invalid("blame workset poisoned"))?
                .insert(
                    crate::blame::batch::UnitKey::Object {
                        oid: id.to_owned(),
                        kind: object.header().kind,
                    },
                    object.as_slice(),
                    control,
                )?;
        }
        Ok(object)
    }
    fn pack_location(
        &self,
        id: &gix::hash::oid,
        control: &Control,
    ) -> Result<Option<PackLocation>> {
        self.packs.locate(id, control)
    }
}
