use super::{
    Access, ArtifactKey, Claim, DerivedCache, Lookup, Payload,
    payload::{OwnedBytes, ReadHold},
};
use crate::{
    Result, invalid,
    process::{self, Admission, Operation},
};
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

struct Shared {
    data: Arc<OwnedBytes>,
    hold: Option<Arc<ReadHold>>,
}
struct Flight {
    result: Mutex<Option<std::result::Result<Shared, String>>>,
    changed: Condvar,
    operation: Arc<Operation>,
}

/// One bounded, independently owned local job per key. Waiter cancellation only
/// ends that wait; actual-work admission and source ownership remain with the job.
/// Derivation callbacks are trusted application code and MUST poll check() inside
/// bounded loops. This opaque-artifact seam is not the future Git CPU executor.
pub struct Coordinator {
    cache: DerivedCache,
    admission: Arc<Admission>,
    flights: Mutex<HashMap<ArtifactKey, Arc<Flight>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    unavailable_until: Arc<Mutex<Instant>>,
}

pub struct Derivation<'a> {
    key: &'a ArtifactKey,
    access: &'a dyn Access,
    limit: usize,
}
impl Derivation<'_> {
    pub fn check(&self) -> Result<()> {
        process::check_operation()?;
        self.access.check(self.key)
    }
    pub fn max_bytes(&self) -> usize {
        self.limit
    }
}

impl Coordinator {
    pub fn new(cache: DerivedCache, concurrency: usize) -> Result<Self> {
        if !(1..=4).contains(&concurrency) {
            return Err(invalid("cache compute concurrency must be in 1..=4"));
        }
        Ok(Self {
            cache,
            admission: Admission::new(concurrency),
            flights: Mutex::new(HashMap::new()),
            threads: Mutex::new(Vec::new()),
            unavailable_until: Arc::new(Mutex::new(Instant::now())),
        })
    }

    pub fn active(&self) -> usize {
        self.admission.active()
    }

    pub fn get_or_compute(
        &self,
        key: ArtifactKey,
        access: Arc<dyn Access>,
        compute: impl FnOnce(&Derivation<'_>) -> Result<Vec<u8>> + Send + 'static,
    ) -> Result<Payload> {
        process::remaining()?;
        access.check(&key)?;
        let completed = {
            let mut flights = self.flights.lock().expect("cache flights poisoned");
            let keys: Vec<_> = flights
                .iter()
                .filter(|(_, flight)| {
                    flight
                        .result
                        .lock()
                        .expect("cache result poisoned")
                        .is_some()
                })
                .map(|(key, _)| key.clone())
                .collect();
            keys.into_iter()
                .filter_map(|key| flights.remove(&key))
                .collect::<Vec<_>>()
        };
        // A completed hit can own the last read hold. Its destructor performs
        // bounded PostgreSQL cleanup; never serialize other keys behind that I/O.
        drop(completed);
        process::check_operation()?;
        let flight = {
            let mut flights = self.flights.lock().expect("cache flights poisoned");
            if let Some(flight) = flights.get(&key) {
                flight.clone()
            } else {
                let permit = self
                    .admission
                    .try_admit()
                    .ok_or_else(|| invalid("cache compute admission exhausted"))?;
                let flight = Arc::new(Flight {
                    result: Mutex::new(None),
                    changed: Condvar::new(),
                    operation: Operation::new(self.cache.inner.config.job_seconds as u64),
                });
                let work = flight.clone();
                let cache = self.cache.clone();
                let source = access.clone();
                let job_key = key.clone();
                let breaker = self.unavailable_until.clone();
                let thread = std::thread::Builder::new()
                    .name("git-rs-artifact".into())
                    .spawn(move || {
                        let _permit = permit;
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            work.operation.enter(|| {
                                Self::compute(&cache, &job_key, source, &breaker, compute)
                            })
                        }))
                        .unwrap_or_else(|_| Err(invalid("artifact computation panicked")));
                        *work.result.lock().expect("cache result poisoned") =
                            Some(result.map_err(|e| e.to_string()));
                        work.changed.notify_all();
                    })?;
                let mut threads = self.threads.lock().expect("cache threads poisoned");
                let mut index = 0;
                while index < threads.len() {
                    if threads[index].is_finished() {
                        let _ = threads.swap_remove(index).join();
                    } else {
                        index += 1;
                    }
                }
                threads.push(thread);
                flights.insert(key.clone(), flight.clone());
                flight
            }
        };
        loop {
            process::check_operation()?;
            // Source checks may perform I/O. Neither result publication nor
            // unrelated-key admission may depend on a waiter's source latency.
            access.check(&key)?;
            process::check_operation()?;
            let result = flight.result.lock().expect("cache result poisoned");
            if let Some(result) = result.as_ref() {
                let shared = result.as_ref().map_err(|e| invalid(e))?;
                return Ok(Payload {
                    data: shared.data.clone(),
                    hold: shared.hold.clone(),
                    access,
                    key,
                });
            }
            drop(
                flight
                    .changed
                    .wait_timeout(result, Duration::from_millis(10))
                    .expect("cache result poisoned"),
            );
        }
    }

    fn compute(
        cache: &DerivedCache,
        key: &ArtifactKey,
        access: Arc<dyn Access>,
        breaker: &Mutex<Instant>,
        compute: impl FnOnce(&Derivation<'_>) -> Result<Vec<u8>>,
    ) -> Result<Shared> {
        let context = Derivation {
            key,
            access: access.as_ref(),
            limit: cache.inner.config.max_payload_bytes,
        };
        let claim = loop {
            context.check()?;
            if Instant::now() < *breaker.lock().expect("cache circuit poisoned") {
                break None;
            }
            match cache.get_or_claim(key, access.clone()) {
                Ok(Lookup::Hit(payload)) => {
                    return Ok(Shared {
                        data: payload.data,
                        hold: payload.hold,
                    });
                }
                Ok(Lookup::Claimed(claim)) => break Some(claim),
                Ok(Lookup::Pending) => std::thread::sleep(Duration::from_millis(25)),
                Err(_) => {
                    context.check()?;
                    *breaker.lock().expect("cache circuit poisoned") =
                        Instant::now() + Duration::from_secs(1);
                    break None;
                }
            }
        };
        let _working = cache.inner.budget.reserve(context.limit)?;
        let derive_and_publish = || -> Result<Shared> {
            context.check()?;
            let bytes = compute(&context)?;
            context.check()?;
            if bytes.len() > context.limit || bytes.capacity() > context.limit {
                return Err(invalid("derived artifact exceeds RAM/payload limit"));
            }
            let data = OwnedBytes::new(bytes, &cache.inner.budget)?;
            if let Some(claim) = &claim
                && cache.publish(claim, access.as_ref(), &data.bytes).is_err()
            {
                *breaker.lock().expect("cache circuit poisoned") =
                    Instant::now() + Duration::from_secs(1);
            }
            context.check()?;
            Ok(Shared { data, hold: None })
        };
        if let Some(claim) = &claim {
            // Upload and descriptor confirmation are part of the owned job too.
            Self::with_renewal(cache, claim, derive_and_publish)
        } else {
            derive_and_publish()
        }
    }

    pub(crate) fn with_renewal<T: Send>(
        cache: &DerivedCache,
        claim: &Claim,
        compute: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let operation = Operation::current()
            .ok_or_else(|| invalid("cache renewal requires a bounded operation"))?;
        std::thread::scope(|scope| {
            let (done, finished) = mpsc::channel();
            scope.spawn(move || {
                let interval = Duration::from_millis(
                    (cache.inner.config.lease_seconds as u64 * 1000 / 3).max(100),
                );
                while finished.recv_timeout(interval) == Err(mpsc::RecvTimeoutError::Timeout) {
                    if !operation.enter(|| cache.renew(claim)).unwrap_or(false) {
                        break;
                    }
                }
            });
            // Catch before leaving the scope: the heartbeat must receive stop
            // even when a callback panics, rather than outliving its useful work.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(compute))
                .unwrap_or_else(|_| Err(invalid("artifact computation panicked")));
            let _ = done.send(());
            result
        })
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        for flight in self
            .flights
            .get_mut()
            .expect("cache flights poisoned")
            .values()
        {
            flight.operation.cancel();
        }
        for thread in self
            .threads
            .get_mut()
            .expect("cache threads poisoned")
            .drain(..)
        {
            let _ = thread.join();
        }
    }
}
