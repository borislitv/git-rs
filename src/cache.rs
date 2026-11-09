use crate::{
    Result, invalid,
    metrics::{self, CacheResult, Metric, Stage},
    process,
};
use memfd::{FileSeal, Memfd, MemfdOptions};
use std::collections::HashMap;
use std::fs::{File, Permissions};
use std::io::Seek;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

const PAGE_BYTES: usize = 4096;
const MAX_FILES: usize = 256;

struct Budget {
    bytes: AtomicUsize,
    files: AtomicUsize,
}

struct Reservation {
    bytes: usize,
    budget: Arc<Budget>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.files.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Immutable RAM-only backing shared by native and library readers. Every
/// request linking this path retains the handle until all its readers finish.
pub struct CachedFile {
    _file: Memfd,
    path: PathBuf,
    size: usize,
    _reservation: Reservation,
}
impl CachedFile {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

struct State {
    entries: HashMap<String, (u64, Arc<CachedFile>)>,
    flights: HashMap<String, Weak<Flight>>,
    tick: u64,
}
struct Flight {
    busy: Mutex<bool>,
    finished: Condvar,
}

struct Loading(Arc<Flight>);
impl Drop for Loading {
    fn drop(&mut self) {
        *self.0.busy.lock().expect("cache flight poisoned") = false;
        self.0.finished.notify_all();
    }
}

pub struct Cache {
    state: Mutex<State>,
    allocated: Arc<Budget>,
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
            allocated: Arc::new(Budget {
                bytes: AtomicUsize::new(0),
                files: AtomicUsize::new(0),
            }),
            capacity,
            max_entry,
        }
    }

    pub fn allocated(&self) -> usize {
        self.allocated.bytes.load(Ordering::Acquire)
    }

    /// None means bypass to request-private bounded streaming. The byte budget
    /// rounds payloads to 4 KiB and includes fills and evicted reader-pinned
    /// files. At most 256 live files are admitted, including fills and pins.
    /// The loader writes exactly `size` bytes and verifies their identity before
    /// returning. Growth/shrink seals bound the file even while it is filled.
    pub fn get_or_load(
        &self,
        key: &str,
        size: usize,
        load: impl FnOnce(&File) -> Result<()>,
    ) -> Result<Option<Arc<CachedFile>>> {
        process::check_operation()?;
        let charge = size.checked_next_multiple_of(PAGE_BYTES);
        if size == 0 || size > self.max_entry || charge.is_none_or(|bytes| bytes > self.capacity) {
            metrics::global().cache(CacheResult::Bypass);
            return Ok(None);
        }
        let charge = charge.expect("admitted cache charge");
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
            process::check_operation()?;
            busy = flight
                .finished
                .wait_timeout(busy, Duration::from_millis(10))
                .expect("cache flight poisoned")
                .0;
        }
        process::check_operation()?;
        if let Some(wait) = wait {
            wait.finish(true);
        }
        let mut state = self.state.lock().expect("cache state poisoned");
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        if let Some((last, buffer)) = state.entries.get_mut(key) {
            if buffer.size != size {
                return Err(invalid("cached object size mismatch"));
            }
            metrics::global().cache(CacheResult::Hit);
            *last = tick;
            return Ok(Some(buffer.clone()));
        }
        while self.allocated().saturating_add(charge) > self.capacity
            || self.allocated.files.load(Ordering::Acquire) >= MAX_FILES
        {
            let Some(oldest) = state
                .entries
                .iter()
                // Removing a reader-pinned entry cannot free its reservation.
                // Keep it discoverable so other requests can reuse the same file
                // rather than bypassing to another download of the same bytes.
                .filter(|(_, (_, buffer))| Arc::strong_count(buffer) == 1)
                .min_by_key(|(_, (tick, _))| tick)
                .map(|(key, _)| key.clone())
            else {
                metrics::global().cache(CacheResult::Bypass);
                return Ok(None);
            };
            state.entries.remove(&oldest);
        }
        self.allocated.bytes.fetch_add(charge, Ordering::AcqRel);
        self.allocated.files.fetch_add(1, Ordering::AcqRel);
        metrics::global().cache(CacheResult::Miss);
        *busy = true;
        drop(state);
        drop(busy);
        let _loading = Loading(flight);
        let reservation = Reservation {
            bytes: charge,
            budget: self.allocated.clone(),
        };
        let fill = metrics::Timer::start(Metric::Stage(Stage::CacheFill));
        let file = MemfdOptions::default()
            .allow_sealing(true)
            .create("git-rs-object")?;
        file.as_file().set_len(size as u64)?;
        file.add_seals(&[FileSeal::SealGrow, FileSeal::SealShrink])?;
        let mut output = file.as_file();
        load(output)?;
        if output.stream_position()? != size as u64 {
            return Err(invalid("incomplete cache fill"));
        }
        process::check_operation()?;
        file.add_seals(&[FileSeal::SealWrite, FileSeal::SealSeal])?;
        file.as_file()
            .set_permissions(Permissions::from_mode(0o400))?;
        let buffer = Arc::new(CachedFile {
            path: PathBuf::from(format!(
                "/proc/{}/fd/{}",
                std::process::id(),
                file.as_raw_fd()
            )),
            _file: file,
            size,
            _reservation: reservation,
        });
        let mut state = self.state.lock().expect("cache state poisoned");
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        state.entries.insert(key.into(), (tick, buffer.clone()));
        drop(state);
        fill.finish(true);
        Ok(Some(buffer))
    }
}
