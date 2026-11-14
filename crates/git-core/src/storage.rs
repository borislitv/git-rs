use crate::objects::{Result, invalid};
use crate::{CapacityBudget, CapacityReservation, Control};
use hashbrown::HashTable;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// Fixed invocation maximum, geometric growth with old+new capacity admitted
/// during replacement. No Vec growth or whole-table unpolled rehash is exposed.
pub(crate) struct ChargedVec<T> {
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
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }
    pub fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }
    pub fn push(&mut self, value: T, control: &Control) -> Result<()> {
        control.check()?;
        if self.values.len() >= self.max {
            return Err(invalid("worklist/count limit exceeded"));
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

pub(crate) struct Set<T> {
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
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.table.iter()
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

pub(crate) struct Map<K, V> {
    table: HashTable<(K, V)>,
    hash: RandomState,
    budget: CapacityBudget,
    _capacity: CapacityReservation,
    entries: usize,
    max: usize,
}

impl<K: Copy + Eq + Hash, V: Copy> Map<K, V> {
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
    pub fn get(&self, key: &K) -> Option<&V> {
        self.table
            .find(self.hash.hash_one(key), |(other, _)| other == key)
            .map(|(_, value)| value)
    }
    pub fn insert(&mut self, key: K, value: V, control: &Control) -> Result<()> {
        control.check()?;
        if self.get(&key).is_some() {
            return Err(invalid("duplicate immutable map key"));
        }
        if self.table.len() >= self.max {
            return Err(invalid("map entry limit exceeded"));
        }
        if self.table.len() == self.entries {
            let entries = self.entries.saturating_mul(2).max(16).min(self.max);
            let buckets = entries
                .checked_mul(2)
                .and_then(usize::checked_next_power_of_two)
                .ok_or_else(|| invalid("map allocation overflow"))?;
            let bytes = buckets
                .checked_mul(std::mem::size_of::<(K, V)>() + 1)
                .and_then(|bytes| bytes.checked_add(64))
                .ok_or_else(|| invalid("map allocation overflow"))?;
            let mut reservation = self.budget.reservation(bytes)?;
            let mut table = HashTable::new();
            table
                .try_reserve(entries, |(key, _)| self.hash.hash_one(key))
                .map_err(|_| invalid("map allocation failed"))?;
            reservation.resize(table.allocation_size())?;
            for entry in self.table.iter() {
                control.check()?;
                table.insert_unique(self.hash.hash_one(entry.0), *entry, |(key, _)| {
                    self.hash.hash_one(key)
                });
            }
            drop(std::mem::replace(&mut self.table, table));
            self._capacity = reservation;
            self.entries = entries;
        }
        self.table
            .insert_unique(self.hash.hash_one(key), (key, value), |(key, _)| {
                self.hash.hash_one(key)
            });
        Ok(())
    }
}

impl<K, V> Drop for Map<K, V> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.table));
    }
}

/// Bounded standard-library sorted runs and a polled merge. Temporary backing is
/// charged alongside the original; no uninterruptible whole-history sort occurs.
pub(crate) fn sort_controlled<T: Copy>(
    values: &mut [T],
    budget: &CapacityBudget,
    control: &Control,
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
) -> Result<()> {
    const RUN: usize = 1024;
    for chunk in values.chunks_mut(RUN) {
        control.check()?;
        chunk.sort_unstable_by(|a, b| compare(a, b));
    }
    if values.len() <= RUN {
        return Ok(());
    }
    let mut capacity = budget.reservation(std::mem::size_of_val(values))?;
    let mut temporary = Vec::new();
    temporary.try_reserve_exact(values.len())?;
    capacity.resize(temporary.capacity() * std::mem::size_of::<T>())?;
    for chunk in values.chunks(RUN) {
        control.check()?;
        temporary.extend_from_slice(chunk);
    }
    let mut width = RUN;
    while width < values.len() {
        for start in (0..values.len()).step_by(width * 2) {
            let middle = (start + width).min(values.len());
            let end = (middle + width).min(values.len());
            let (mut left, mut right) = (start, middle);
            for (position, output) in values[start..end].iter_mut().enumerate() {
                if position % 1024 == 0 {
                    control.check()?;
                }
                if left < middle
                    && (right == end || compare(&temporary[left], &temporary[right]).is_le())
                {
                    *output = temporary[left];
                    left += 1;
                } else {
                    *output = temporary[right];
                    right += 1;
                }
            }
        }
        for (source, destination) in values.chunks(RUN).zip(temporary.chunks_mut(RUN)) {
            control.check()?;
            destination.copy_from_slice(source);
        }
        width = width
            .checked_mul(2)
            .ok_or_else(|| invalid("controlled sort width overflow"))?;
    }
    control.check()?;
    Ok(())
}
