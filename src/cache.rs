//! Bounded, immutable Rust-owned source bytes. Evicted reader pins and in-progress fills
//! continue owning their actual capacity until their final reader is gone.

use crate::{
    Result, invalid,
    metrics::{self, CacheResult},
    process,
};
use git_rs_core::{CapacityBudget, CapacityBuffer, CapacityReservation};
use std::{
    ops::Deref,
    sync::{Arc, Condvar, Mutex, Weak},
    time::Duration,
};

const SLOTS: usize = 256;
const MAX_KEY: usize = 2048;

pub struct CachedBytes {
    bytes: CapacityBuffer,
    _metadata: CapacityReservation,
}

impl Deref for CachedBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}
impl AsRef<[u8]> for CachedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

struct Entry {
    key: String,
    tick: u64,
    bytes: Arc<CachedBytes>,
}

struct Flight {
    key: String,
    busy: Mutex<bool>,
    finished: Condvar,
    _metadata: CapacityReservation,
}

struct State {
    entries: [Option<Entry>; SLOTS],
    flights: [Option<Weak<Flight>>; SLOTS],
    tick: u64,
    invalidation: u64,
}

struct Loading(Arc<Flight>);
impl Drop for Loading {
    fn drop(&mut self) {
        *self
            .0
            .busy
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = false;
        self.0.finished.notify_all();
    }
}

pub struct Cache {
    state: Mutex<State>,
    budget: CapacityBudget,
    flight_budget: CapacityBudget,
    max_entry: usize,
}

impl Cache {
    pub fn new(capacity: usize, max_entry: usize) -> Self {
        let flights = (capacity / 8).min(SLOTS * (std::mem::size_of::<Flight>() + MAX_KEY + 64));
        Self {
            state: Mutex::new(State {
                entries: std::array::from_fn(|_| None),
                flights: std::array::from_fn(|_| None),
                tick: 0,
                invalidation: 0,
            }),
            budget: CapacityBudget::new(capacity - flights),
            flight_budget: CapacityBudget::new(flights),
            max_entry,
        }
    }

    pub fn allocated(&self) -> usize {
        self.budget.used() + self.flight_budget.used()
    }

    /// Forget a rejected immutable source's lookup entries. Existing readers
    /// still own their backing and charge; concurrent fills cannot reinstall an
    /// entry across this invalidation boundary. No I/O runs under the table lock.
    pub fn invalidate_prefix(&self, prefix: &str) {
        let removed: [Option<Entry>; SLOTS] = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.invalidation = state.invalidation.wrapping_add(1);
            std::array::from_fn(|index| {
                if state.entries[index]
                    .as_ref()
                    .is_some_and(|entry| entry.key.starts_with(prefix))
                {
                    state.entries[index].take()
                } else {
                    None
                }
            })
        };
        drop(removed);
    }

    fn hit(&self, key: &str, size: usize) -> Result<Option<Arc<CachedBytes>>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        if let Some(entry) = state
            .entries
            .iter_mut()
            .flatten()
            .find(|entry| entry.key == key)
        {
            if entry.bytes.len() != size {
                return Err(invalid("immutable source cache size changed"));
            }
            entry.tick = tick;
            metrics::global().cache(CacheResult::Hit);
            return Ok(Some(entry.bytes.clone()));
        }
        Ok(None)
    }

    fn evict(&self) -> bool {
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let victim = state
                .entries
                .iter()
                .enumerate()
                .filter_map(|(slot, entry)| entry.as_ref().map(|entry| (slot, entry)))
                .filter(|(_, entry)| Arc::strong_count(&entry.bytes) == 1)
                .min_by_key(|(_, entry)| entry.tick)
                .map(|(slot, _)| slot);
            victim.and_then(|slot| state.entries[slot].take())
        };
        // Backing deallocation/admission return never occurs under the table lock.
        let evicted = removed.is_some();
        drop(removed);
        evicted
    }

    /// The loader fills an exact, admitted slice and verifies its required
    /// identity. None means bounded private acquisition by the caller, not an
    /// unadmitted allocation or an authority/membership fallback.
    pub fn get_or_load(
        &self,
        key: &str,
        size: usize,
        load: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Option<Arc<CachedBytes>>> {
        self.load(key, size, None, load)
    }

    pub fn get_or_load_controlled(
        &self,
        key: &str,
        size: usize,
        control: &git_rs_core::Control,
        load: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Option<Arc<CachedBytes>>> {
        self.load(key, size, Some(control), load)
    }

    fn load(
        &self,
        key: &str,
        size: usize,
        control: Option<&git_rs_core::Control>,
        load: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Option<Arc<CachedBytes>>> {
        let check = || -> Result<()> {
            process::check_operation()?;
            if let Some(control) = control {
                control.check()?;
            }
            Ok(())
        };
        check()?;
        if key.is_empty() || key.len() > MAX_KEY {
            return Err(invalid("source cache key bound exceeded"));
        }
        if let Some(hit) = self.hit(key, size)? {
            return Ok(Some(hit));
        }
        if size == 0 || size > self.max_entry {
            metrics::global().cache(CacheResult::Bypass);
            return Ok(None);
        }
        let (flight, invalidation) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let mut found = None;
            for slot in &mut state.flights {
                if let Some(flight) = slot.as_ref().and_then(Weak::upgrade) {
                    if flight.key == key {
                        found = Some(flight);
                    }
                } else {
                    *slot = None;
                }
            }
            if let Some(flight) = found {
                (flight, state.invalidation)
            } else {
                let Some(slot) = state.flights.iter_mut().find(|slot| slot.is_none()) else {
                    metrics::global().cache(CacheResult::Bypass);
                    return Ok(None);
                };
                let Ok(mut metadata) = self
                    .flight_budget
                    .reservation(std::mem::size_of::<Flight>() + key.len() + 64)
                else {
                    metrics::global().cache(CacheResult::Bypass);
                    return Ok(None);
                };
                let mut name = String::new();
                name.try_reserve_exact(key.len())?;
                metadata.resize(std::mem::size_of::<Flight>() + name.capacity() + 64)?;
                name.push_str(key);
                let flight = Arc::new(Flight {
                    key: name,
                    busy: Mutex::new(false),
                    finished: Condvar::new(),
                    _metadata: metadata,
                });
                *slot = Some(Arc::downgrade(&flight));
                (flight, state.invalidation)
            }
        };
        let mut busy = flight
            .busy
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while *busy {
            check()?;
            busy = flight
                .finished
                .wait_timeout(busy, Duration::from_millis(10))
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
        check()?;
        if let Some(hit) = self.hit(key, size)? {
            return Ok(Some(hit));
        }
        *busy = true;
        drop(busy);
        let _loading = Loading(flight);
        let mut metadata = None;
        for _ in 0..=SLOTS {
            check()?;
            if let Ok(reservation) = self
                .budget
                .reservation(std::mem::size_of::<CachedBytes>() + key.len() + 64)
            {
                metadata = Some(reservation);
                break;
            }
            if !self.evict() {
                break;
            }
        }
        let Some(mut metadata) = metadata else {
            metrics::global().cache(CacheResult::Bypass);
            return Ok(None);
        };
        let mut data = None;
        for _ in 0..=SLOTS {
            check()?;
            if let Ok(bytes) = self.budget.buffer(size) {
                data = Some(bytes);
                break;
            }
            if !self.evict() {
                break;
            }
        }
        let Some(mut bytes) = data else {
            metrics::global().cache(CacheResult::Bypass);
            return Ok(None);
        };
        let mut name = String::new();
        name.try_reserve_exact(key.len())?;
        metadata.resize(std::mem::size_of::<CachedBytes>() + name.capacity() + 64)?;
        name.push_str(key);
        for end in (0..size).step_by(32 * 1024) {
            check()?;
            bytes.resize(size.min(end + 32 * 1024), 0)?;
        }
        metrics::global().cache(CacheResult::Miss);
        metrics::measure(metrics::Metric::Stage(metrics::Stage::CacheFill), || {
            load(bytes.as_mut_slice())
        })?;
        check()?;
        let bytes = Arc::new(CachedBytes {
            bytes,
            _metadata: metadata,
        });
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.tick = state.tick.wrapping_add(1);
            let tick = state.tick;
            if state.invalidation == invalidation {
                let slot = state.entries.iter().position(Option::is_none).or_else(|| {
                    state
                        .entries
                        .iter()
                        .enumerate()
                        .filter_map(|(slot, entry)| entry.as_ref().map(|entry| (slot, entry)))
                        .filter(|(_, entry)| Arc::strong_count(&entry.bytes) == 1)
                        .min_by_key(|(_, entry)| entry.tick)
                        .map(|(slot, _)| slot)
                });
                slot.and_then(|slot| {
                    state.entries[slot].replace(Entry {
                        key: name,
                        tick,
                        bytes: bytes.clone(),
                    })
                })
            } else {
                None
            }
        };
        // Slot pressure and byte pressure are independent. Destroy an unpinned
        // victim outside the lock; if every slot is pinned, retain only this
        // admitted reader's backing rather than hiding an unindexed cache owner.
        drop(removed);
        Ok(Some(bytes))
    }
}
