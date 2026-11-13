use crate::objects::{Result, invalid};
use crate::{CapacityBudget, CapacityReservation, Control};
use hashbrown::HashTable;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// Fixed invocation maximum, geometric growth with old+new capacity admitted
/// during replacement. No Vec growth or whole-table unpolled rehash is exposed.
pub(super) struct ChargedVec<T> {
    values: Vec<T>,
    budget: CapacityBudget,
    _capacity: CapacityReservation,
    max: usize,
}

impl<T> ChargedVec<T> {
    pub fn new(budget: CapacityBudget, max: usize) -> Result<Self> {
        let capacity = budget.reservation(0)?;
        Ok(Self {
            values: Vec::new(),
            budget,
            _capacity: capacity,
            max,
        })
    }
    pub fn as_slice(&self) -> &[T] {
        &self.values
    }
    pub fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }
    pub fn push(&mut self, value: T, control: &Control) -> Result<()> {
        control.check()?;
        if self.values.len() >= self.max {
            return Err(invalid("receive worklist/count limit exceeded"));
        }
        if self.values.len() == self.values.capacity() {
            let count = self.values.len().saturating_mul(2).max(16).min(self.max);
            let bytes = count
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| invalid("receive allocation overflow"))?;
            let mut reservation = self.budget.reservation(bytes)?;
            let mut values = Vec::new();
            values.try_reserve_exact(count)?;
            reservation.resize(values.capacity() * std::mem::size_of::<T>())?;
            // Move in bounded units; old allocation remains charged until freed.
            for item in self.values.drain(..) {
                control.check()?;
                values.push(item);
            }
            drop(std::mem::replace(&mut self.values, values));
            self._capacity = reservation;
        }
        self.values.push(value);
        Ok(())
    }
}

impl<T> Drop for ChargedVec<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.values));
    }
}

pub(super) struct Set<T> {
    table: HashTable<T>,
    hash: RandomState,
    budget: CapacityBudget,
    _capacity: CapacityReservation,
    entries: usize,
    max: usize,
}

impl<T: Copy + Eq + Hash> Set<T> {
    pub fn new(budget: CapacityBudget, max: usize) -> Result<Self> {
        Ok(Self {
            table: HashTable::new(),
            hash: RandomState::new(),
            _capacity: budget.reservation(0)?,
            budget,
            entries: 0,
            max,
        })
    }
    pub fn contains(&self, value: &T) -> bool {
        self.table
            .find(self.hash.hash_one(value), |other| other == value)
            .is_some()
    }
    pub fn remove(&mut self, value: &T) {
        if let Ok(entry) = self
            .table
            .find_entry(self.hash.hash_one(value), |other| other == value)
        {
            entry.remove();
        }
    }
    pub fn insert(&mut self, value: T, control: &Control) -> Result<bool> {
        control.check()?;
        if self.contains(&value) {
            return Ok(false);
        }
        if self.table.len() >= self.max {
            return Err(invalid("receive visited-object limit exceeded"));
        }
        if self.table.len() == self.entries {
            let entries = self.entries.saturating_mul(2).max(16).min(self.max);
            let buckets = entries
                .checked_mul(2)
                .and_then(usize::checked_next_power_of_two)
                .ok_or_else(|| invalid("receive hash allocation overflow"))?;
            let bytes = buckets
                .checked_mul(std::mem::size_of::<T>() + 1)
                .and_then(|bytes| bytes.checked_add(64))
                .ok_or_else(|| invalid("receive hash allocation overflow"))?;
            let mut reservation = self.budget.reservation(bytes)?;
            let mut table = HashTable::new();
            table
                .try_reserve(entries, |old| self.hash.hash_one(old))
                .map_err(|_| invalid("receive hash allocation failed"))?;
            reservation.resize(table.allocation_size())?;
            for old in self.table.iter() {
                control.check()?;
                table.insert_unique(self.hash.hash_one(old), *old, |old| self.hash.hash_one(old));
            }
            drop(std::mem::replace(&mut self.table, table));
            self._capacity = reservation;
            self.entries = entries;
        }
        self.table
            .insert_unique(self.hash.hash_one(value), value, |old| {
                self.hash.hash_one(old)
            });
        Ok(true)
    }
}

impl<T> Drop for Set<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.table));
    }
}
