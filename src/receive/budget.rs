//! Borrowed vendor admission never escapes its scope or detaches a Vec. A fixed
//! table avoids a second unadmitted allocator in the budget callback itself.
use crate::{Result, invalid};
use git_rs_core::{CapacityBudget, CapacityReservation};
use std::sync::Mutex;

const SLOTS: usize = 64;
type Slot = Option<(usize, CapacityReservation)>;

pub(super) struct Budget {
    slots: Mutex<[Slot; SLOTS]>,
    capacity: CapacityBudget,
    _table_capacity: CapacityReservation,
}

impl Budget {
    pub(super) fn new(capacity: CapacityBudget) -> Result<Self> {
        let table = capacity.reservation(std::mem::size_of::<[Slot; SLOTS]>())?;
        Ok(Self {
            slots: Mutex::new(std::array::from_fn(|_| None)),
            capacity,
            _table_capacity: table,
        })
    }
}

impl gix::odb::pack::memory::Budget for Budget {
    fn acquire(&self, bytes: usize) -> gix::error::Result {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let slot = slots
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or_else(|| {
                gix::error::Error::from_boxed(invalid("receive budget slot bound exceeded"))
            })?;
        let reservation = self
            .capacity
            .reservation(bytes)
            .map_err(|error| gix::error::Error::from_boxed(Box::new(error)))?;
        *slot = Some((bytes, reservation));
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Equal-size acquisitions are interchangeable guards of the same budget.
        // Unknown releases retain charges (fail closed); never panic in Drop.
        if let Some(slot) = slots
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|(size, _)| *size == bytes))
        {
            drop(slot.take());
        }
    }
}
