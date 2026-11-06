use crate::{
    Result,
    metrics::{self, CacheResult, Metric},
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

pub struct Buffer {
    bytes: Box<[u8]>,
    budget: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Buffer {
    fn drop(&mut self) {
        self.budget.fetch_sub(self.bytes.len(), Ordering::AcqRel);
    }
}

struct State {
    entries: HashMap<String, (u64, Arc<Buffer>)>,
    flights: HashMap<String, Weak<Flight>>,
    tick: u64,
}
struct Flight {
    busy: Mutex<bool>,
    finished: Condvar,
}

pub struct Cache {
    state: Mutex<State>,
    allocated: Arc<AtomicUsize>,
    capacity: usize,
    max_entry: usize,
}

impl Cache {
    pub fn new(capacity: usize, max_entry: usize) -> Self {
        Self {
            state: Mutex::new(State {
                entries: HashMap::new(),
                flights: HashMap::new(),
                tick: 0,
            }),
            allocated: Arc::new(AtomicUsize::new(0)),
            capacity,
            max_entry,
        }
    }

    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Acquire)
    }

    /// None means bypass to bounded streaming. Allocated bytes include evicted
    /// entries pinned by readers and reservations for concurrent fills.
    pub fn get_or_load(
        &self,
        key: &str,
        size: usize,
        load: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Option<Arc<Buffer>>> {
        if size == 0 || size > self.max_entry || size > self.capacity {
            metrics::global().cache(CacheResult::Bypass);
            return Ok(None);
        }
        let flight = {
            let mut state = self.state.lock().expect("cache state poisoned");
            state.flights.retain(|_, flight| flight.strong_count() > 0);
            if let Some(flight) = state.flights.get(key).and_then(Weak::upgrade) {
                flight
            } else {
                let flight = Arc::new(Flight {
                    busy: Mutex::new(false),
                    finished: Condvar::new(),
                });
                state.flights.insert(key.into(), Arc::downgrade(&flight));
                flight
            }
        };
        let mut busy = flight.busy.lock().expect("cache flight poisoned");
        let wait = (*busy).then(|| metrics::Timer::start(Metric::CacheWait));
        while *busy {
            busy = flight.finished.wait(busy).expect("cache flight poisoned");
        }
        if let Some(wait) = wait {
            wait.finish(true);
        }
        let mut state = self.state.lock().expect("cache state poisoned");
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        if let Some((last, buffer)) = state.entries.get_mut(key) {
            metrics::global().cache(CacheResult::Hit);
            *last = tick;
            return Ok(Some(buffer.clone()));
        }
        while self.allocated().saturating_add(size) > self.capacity {
            let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, (tick, _))| tick)
                .map(|(key, _)| key.clone())
            else {
                metrics::global().cache(CacheResult::Bypass);
                return Ok(None);
            };
            state.entries.remove(&oldest);
        }
        self.allocated.fetch_add(size, Ordering::AcqRel);
        metrics::global().cache(CacheResult::Miss);
        *busy = true;
        drop(state);
        drop(busy);
        // A separate reservation owns accounting even on an I/O error.
        let mut buffer = Buffer {
            bytes: vec![0; size].into_boxed_slice(),
            budget: self.allocated.clone(),
        };
        let outcome = load(&mut buffer.bytes);
        let result = outcome.map(|()| {
            let buffer = Arc::new(buffer);
            let mut state = self.state.lock().expect("cache state poisoned");
            state.tick = state.tick.wrapping_add(1);
            let tick = state.tick;
            state.entries.insert(key.into(), (tick, buffer.clone()));
            Some(buffer)
        });
        *flight.busy.lock().expect("cache flight poisoned") = false;
        flight.finished.notify_all();
        result
    }
}
