//! Job-owned physical ranges and optional authenticated object reuse for blame.

use crate::{
    Result, blame::Observer, blob::Store, cache::Cache, invalid, object_source::RangeStore,
};
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
    observer: Option<Arc<dyn Observer>>,
    budget: CapacityBudget,
    limits: ReadLimits,
    resources: crate::blame::ResourceProfile,
    operation: Arc<crate::process::Operation>,
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
        Self::new_with_resource_profile(
            source,
            store,
            cache,
            budget,
            limits,
            crate::blame::ResourceProfile::BaselineV1,
            control,
        )
    }

    #[allow(clippy::too_many_arguments)] // Immutable policy accompanies the exact admitted source.
    pub(crate) fn new_with_resource_profile(
        source: Arc<Source>,
        store: Store,
        cache: Arc<Cache>,
        budget: CapacityBudget,
        limits: ReadLimits,
        resources: crate::blame::ResourceProfile,
        control: &Control,
    ) -> Result<Self> {
        let operation = source.operation.clone();
        let packs = RangeStore::open_with_decoder_options(
            source,
            store,
            cache,
            budget.clone(),
            limits,
            resources.decoder_options(),
            control,
        )?
        .with_prepared_reads();
        let packs = if resources == crate::blame::ResourceProfile::LargeV1 {
            packs.with_verification_journal(control)?
        } else {
            packs
        };
        Ok(Self {
            packs,
            work: None,
            observer: None,
            budget,
            limits,
            resources,
            operation,
        })
    }

    pub fn with_workset(mut self, work: Option<crate::blame::memo::WorkSet>) -> Result<Self> {
        self.work = work
            .map(|work| work.attach(self.resources, self.limits, &self.budget))
            .transpose()?;
        Ok(self)
    }

    pub(crate) fn with_observer(mut self, observer: Option<Arc<dyn Observer>>) -> Self {
        if observer
            .as_ref()
            .is_some_and(|o| o.wants_attribution_progress())
        {
            self.packs.enable_verification_stats();
        }
        self.observer = observer;
        self
    }

    fn observe_read(
        &self,
        id: &gix::hash::oid,
        header_only: bool,
        control: &Control,
    ) -> Result<()> {
        if let Some(observer) = &self.observer {
            control.check()?;
            observer.source_read(id, header_only, control);
            control.check()?;
        }
        Ok(())
    }
}

impl Drop for LazySnapshot {
    fn drop(&mut self) {
        if !std::thread::panicking()
            && let Some(observer) = &self.observer
            && observer.wants_attribution_progress()
        {
            observer.range_verification_closed(self.packs.verification_stats());
        }
    }
}

impl VerifiedObjectProvider for LazySnapshot {
    fn identity(&self) -> &SnapshotIdentity {
        self.packs.identity()
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        self.observe_read(id, true, control)?;
        // A prepared header need not enter RangeStore::read_exact.
        self.operation.check()?;
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
        self.observe_read(id, false, control)?;
        self.operation.check()?;
        let Some(header) = self.packs.header(id, control)? else {
            return Ok(None);
        };
        // A checkpoint owns bytes rather than a physical decoder-chain plan.
        // Preserve the original restriction: tighter chain limits must use the
        // physical decoder and cannot infer a plan from authenticated bytes.
        if let Some(work) = &self.work {
            let object = work.object(id.to_owned(), header.kind, limits, control)?;
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
