use super::DecodeEntry;

/// Request-owned decoding optimizations. These never read process environment.
/// The default disables all four optimizations; enable each explicitly per job.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Options {
    /// Retain intermediate delta objects under the logical cache weight cap.
    pub intermediate_objects: bool,
    /// Borrow cached bases during delta application.
    pub borrowed_objects: bool,
    /// Reuse allocations retired by eviction.
    pub reused_buffers: bool,
    /// Append validated output instead of zero-initializing it.
    pub appended_output: bool,
}

#[cfg(feature = "pack-cache-lru-dynamic")]
mod memory {
    use std::num::NonZeroUsize;

    use clru::WeightScale;

    use super::DecodeEntry;
    use crate::cache::set_vec_to_slice;

    struct Entry {
        data: Vec<u8>,
        kind: gix_object::Kind,
        compressed_size: usize,
    }

    type Key = (u32, u64);
    struct CustomScale;

    impl WeightScale<Key, Entry> for CustomScale {
        fn weight(&self, _key: &Key, value: &Entry) -> usize {
            value.data.len()
        }
    }

    /// An LRU cache with hash map backing and an eviction rule based on the memory usage for object data in bytes.
    pub struct MemoryCappedHashmap {
        inner: clru::CLruCache<Key, Entry, std::collections::hash_map::RandomState, CustomScale>,
        free_list: Vec<Vec<u8>>,
        debug: gix_utils::cache::Debug,
        cache_intermediates: bool,
        borrowed_debug: Option<gix_utils::cache::Debug>,
        reused_debug: Option<gix_utils::cache::Debug>,
        appended_debug: Option<gix_utils::cache::Debug>,
    }

    impl MemoryCappedHashmap {
        /// Return a new instance which evicts least recently used items if it uses more than `memory_cap_in_bytes`
        /// object data.
        pub fn new(memory_cap_in_bytes: usize) -> MemoryCappedHashmap {
            Self::with_options(memory_cap_in_bytes, super::Options::default())
        }

        /// Construct a cache with explicit per-job optimization choices.
        /// The cap is logical byte weight, not an aggregate live-capacity bound.
        pub fn with_options(memory_cap_in_bytes: usize, options: super::Options) -> MemoryCappedHashmap {
            MemoryCappedHashmap {
                inner: clru::CLruCache::with_config(
                    clru::CLruCacheConfig::new(NonZeroUsize::new(memory_cap_in_bytes).expect("non zero"))
                        .with_scale(CustomScale),
                ),
                free_list: Vec::new(),
                debug: gix_utils::cache::Debug::new(format!("MemoryCappedHashmap({memory_cap_in_bytes}B)")),
                cache_intermediates: options.intermediate_objects,
                borrowed_debug: options
                    .borrowed_objects
                    .then(|| gix_utils::cache::Debug::new("BorrowedPackBase".into())),
                reused_debug: options
                    .reused_buffers
                    .then(|| gix_utils::cache::Debug::new("ReusedPackBuffer".into())),
                appended_debug: options
                    .appended_output
                    .then(|| gix_utils::cache::Debug::new("AppendedPackOutput".into())),
            }
        }

        /// Also retain decoded intermediate bases, under the same memory cap.
        pub fn with_intermediate_objects(mut self) -> Self {
            self.cache_intermediates = true;
            self
        }

        /// Allow a decoder to borrow cached bases without copying their bytes.
        pub fn with_borrowed_objects(mut self) -> Self {
            self.borrowed_debug = Some(gix_utils::cache::Debug::new("BorrowedPackBase".into()));
            self
        }

        /// Reuse an evicted entry's allocation before copying new object bytes.
        pub fn with_reused_buffers(mut self) -> Self {
            self.reused_debug = Some(gix_utils::cache::Debug::new("ReusedPackBuffer".into()));
            self
        }

        /// Append decoded output chunks without zero-initializing the result first.
        pub fn with_appended_output(mut self) -> Self {
            self.appended_debug = Some(gix_utils::cache::Debug::new("AppendedPackOutput".into()));
            self
        }

        fn take_reusable_buffer(&mut self, key: Key, size: usize) -> Option<Vec<u8>> {
            self.take_reusable_buffer_controlled(key, size, &|| Ok(()))
                .ok()
                .flatten()
        }

        fn take_reusable_buffer_controlled(
            &mut self,
            key: Key,
            size: usize,
            check: &dyn Fn() -> gix_error::Result,
        ) -> gix_error::Result<Option<Vec<u8>>> {
            check()?;
            let Some(debug) = self.reused_debug.as_mut() else {
                return Ok(None);
            };
            if size >= self.inner.capacity() {
                return Ok(None);
            }
            let mut buffer = self
                .inner
                .pop(&key)
                .map(|entry| entry.data)
                .or_else(|| self.free_list.pop())
                .unwrap_or_default();
            // CLru counts each entry in addition to its byte weight. Retire
            // the same least-recent entries before allocating their replacement.
            while size >= self.inner.capacity() - self.inner.len() - self.inner.weight() {
                check()?;
                let (_, entry) = self.inner.pop_back().expect("a non-fitting entry requires eviction");
                if entry.data.capacity() > buffer.capacity() {
                    buffer = entry.data;
                }
            }
            if buffer.capacity() >= size {
                debug.hit();
            } else {
                debug.miss();
            }
            buffer.clear();
            if buffer.try_reserve_exact(size).is_err() {
                return Ok(None);
            }
            Ok(Some(buffer))
        }
    }

    impl DecodeEntry for MemoryCappedHashmap {
        fn get_controlled(
            &mut self,
            pack_id: u32,
            offset: u64,
            out: &mut Vec<u8>,
            limit: usize,
            check: &dyn Fn() -> gix_error::Result,
        ) -> gix_error::Result<Option<(gix_object::Kind, usize)>> {
            check()?;
            let Some(entry) = self.inner.get(&(pack_id, offset)) else {
                self.debug.miss();
                return Ok(None);
            };
            if entry.data.len() > limit {
                return Err(gix_error::allocation_limit("Cached pack object exceeds allocation limit").into());
            }
            out.clear();
            if out.try_reserve_exact(entry.data.len()).is_err() {
                return Ok(None);
            }
            for bytes in entry.data.chunks(32 * 1024) {
                check()?;
                out.extend_from_slice(bytes);
            }
            check()?;
            self.debug.hit();
            Ok(Some((entry.kind, entry.compressed_size)))
        }

        fn put_controlled(
            &mut self,
            pack_id: u32,
            offset: u64,
            data: &[u8],
            kind: gix_object::Kind,
            compressed_size: usize,
            check: &dyn Fn() -> gix_error::Result,
        ) -> gix_error::Result {
            check()?;
            if data.len() >= self.inner.capacity() {
                return Ok(());
            }
            let mut buffer = if self.reused_debug.is_some() {
                let Some(buffer) = self.take_reusable_buffer_controlled((pack_id, offset), data.len(), check)? else {
                    return Ok(());
                };
                buffer
            } else {
                self.free_list.pop().unwrap_or_default()
            };
            buffer.clear();
            if buffer.try_reserve_exact(data.len()).is_err() {
                return Ok(());
            }
            for bytes in data.chunks(32 * 1024) {
                check()?;
                buffer.extend_from_slice(bytes);
            }
            check()?;
            self.debug.put();
            match self.inner.put_with_weight(
                (pack_id, offset),
                Entry {
                    data: buffer,
                    kind,
                    compressed_size,
                },
            ) {
                Ok(Some(previous)) => self.free_list.push(previous.data),
                Ok(None) => {}
                Err((_, entry)) => self.free_list.push(entry.data),
            }
            Ok(())
        }

        fn append_output(&self) -> bool {
            self.appended_debug.is_some()
        }

        fn cache_intermediate_objects(&self) -> bool {
            self.cache_intermediates
        }

        fn get_borrowed(&mut self, pack_id: u32, offset: u64) -> Option<(gix_object::Kind, &[u8])> {
            let borrowed_debug = self.borrowed_debug.as_mut()?;
            let entry = self.inner.get(&(pack_id, offset))?;
            borrowed_debug.hit();
            if let Some(debug) = &mut self.appended_debug {
                debug.hit();
            }
            self.debug.hit();
            Some((entry.kind, entry.data.as_slice()))
        }

        fn put(&mut self, pack_id: u32, offset: u64, data: &[u8], kind: gix_object::Kind, compressed_size: usize) {
            self.debug.put();
            let data = if self.reused_debug.is_some() {
                let Some(mut buffer) = self.take_reusable_buffer((pack_id, offset), data.len()) else {
                    return;
                };
                buffer.extend_from_slice(data);
                buffer
            } else {
                let Some(buffer) = set_vec_to_slice(self.free_list.pop().unwrap_or_default(), data) else {
                    return;
                };
                buffer
            };
            let res = self.inner.put_with_weight(
                (pack_id, offset),
                Entry {
                    data,
                    kind,
                    compressed_size,
                },
            );
            match res {
                Ok(Some(previous_entry)) => self.free_list.push(previous_entry.data),
                Ok(None) => {}
                Err((_key, value)) => self.free_list.push(value.data),
            }
        }

        fn get(&mut self, pack_id: u32, offset: u64, out: &mut Vec<u8>) -> Option<(gix_object::Kind, usize)> {
            let res = self.inner.get(&(pack_id, offset)).and_then(|e| {
                set_vec_to_slice(out, &e.data)?;
                Some((e.kind, e.compressed_size))
            });
            if res.is_some() {
                self.debug.hit();
            } else {
                self.debug.miss();
            }
            res
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn retired_entry_buffer_is_reused_for_new_content() {
            let mut cache = MemoryCappedHashmap::new(9)
                .with_borrowed_objects()
                .with_reused_buffers();
            cache.put(0, 1, b"original", gix_object::Kind::Blob, 10);
            let previous_allocation = cache.get_borrowed(0, 1).expect("the initial entry fits").1.as_ptr();
            cache.put(0, 2, b"replaced", gix_object::Kind::Blob, 11);
            assert!(cache.get_borrowed(0, 1).is_none(), "the least-recent entry is retired");
            let (kind, bytes) = cache.get_borrowed(0, 2).expect("the replacement fits");
            assert_eq!(kind, gix_object::Kind::Blob);
            assert_eq!(
                bytes, b"replaced",
                "retired storage must not leak its previous contents"
            );
            assert_eq!(
                bytes.as_ptr(),
                previous_allocation,
                "a full cache must reuse retired storage instead of allocating a second full buffer"
            );
            cache.put(0, 2, b"updated", gix_object::Kind::Tag, 12);
            cache.put(0, 2, b"oversized", gix_object::Kind::Blob, 13);
            let mut out = Vec::new();
            assert_eq!(
                cache.get(0, 2, &mut out),
                Some((gix_object::Kind::Tag, 12)),
                "replacement updates metadata, while an oversized put preserves the existing entry"
            );
            assert_eq!(out, b"updated");
        }
    }
}

#[cfg(feature = "pack-cache-lru-dynamic")]
pub use memory::MemoryCappedHashmap;

#[cfg(feature = "pack-cache-lru-static")]
mod _static {
    use super::DecodeEntry;
    use crate::cache::set_vec_to_slice;
    struct Entry {
        pack_id: u32,
        offset: u64,
        data: Vec<u8>,
        kind: gix_object::Kind,
        compressed_size: usize,
    }

    /// A cache using a least-recently-used implementation capable of storing the `SIZE` most recent objects.
    /// The cache must be small as the search is 'naive' and the underlying data structure is a linked list.
    /// Values of 64 seem to improve performance.
    pub struct StaticLinkedList<const SIZE: usize> {
        inner: uluru::LRUCache<Entry, SIZE>,
        last_evicted: Vec<u8>,
        debug: gix_utils::cache::Debug,
        /// the amount of bytes we are currently holding, taking into account the capacities of all Vecs we keep.
        mem_used: usize,
        /// The total amount of memory we should be able to hold with all entries combined.
        mem_limit: usize,
    }

    impl<const SIZE: usize> StaticLinkedList<SIZE> {
        /// Create a new list with a memory limit of `mem_limit` in bytes. If 0, there is no memory limit.
        pub fn new(mem_limit: usize) -> Self {
            StaticLinkedList {
                inner: Default::default(),
                last_evicted: Vec::new(),
                debug: gix_utils::cache::Debug::new(format!("StaticLinkedList<{SIZE}>")),
                mem_used: 0,
                mem_limit: if mem_limit == 0 { usize::MAX } else { mem_limit },
            }
        }
    }

    impl<const SIZE: usize> Default for StaticLinkedList<SIZE> {
        fn default() -> Self {
            Self::new(96 * 1024 * 1024)
        }
    }

    impl<const SIZE: usize> DecodeEntry for StaticLinkedList<SIZE> {
        fn put(&mut self, pack_id: u32, offset: u64, data: &[u8], kind: gix_object::Kind, compressed_size: usize) {
            // We cannot possibly hold this much.
            if data.len() > self.mem_limit {
                return;
            }
            // If we could hold it but are at limit, all we can do is make space.
            let mem_free = self.mem_limit - self.mem_used;
            if data.len() > mem_free {
                // prefer freeing free-lists instead of clearing our cache
                let free_list_cap = self.last_evicted.len();
                self.last_evicted = Vec::new();
                // still not enough? clear everything
                if data.len() > mem_free + free_list_cap {
                    self.inner.clear();
                    self.mem_used = 0;
                } else {
                    self.mem_used -= free_list_cap;
                }
            }
            self.debug.put();
            let mut v = std::mem::take(&mut self.last_evicted);
            self.mem_used -= v.capacity();
            if set_vec_to_slice(&mut v, data).is_none() {
                return;
            }
            self.mem_used += v.capacity();
            if let Some(previous) = self.inner.insert(Entry {
                offset,
                pack_id,
                data: v,
                kind,
                compressed_size,
            }) {
                // No need to adjust capacity as we already counted it.
                self.last_evicted = previous.data;
            }
        }

        fn get(&mut self, pack_id: u32, offset: u64, out: &mut Vec<u8>) -> Option<(gix_object::Kind, usize)> {
            let res = self.inner.lookup(|e: &mut Entry| {
                if e.pack_id == pack_id && e.offset == offset {
                    set_vec_to_slice(&mut *out, &e.data)?;
                    Some((e.kind, e.compressed_size))
                } else {
                    None
                }
            });
            if res.is_some() {
                self.debug.hit();
            } else {
                self.debug.miss();
            }
            res
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn no_limit() {
            let c = StaticLinkedList::<10>::new(0);
            assert_eq!(
                c.mem_limit,
                usize::MAX,
                "zero is automatically turned into a large limit that is equivalent to unlimited"
            );
        }

        #[test]
        fn journey() {
            let mut c = StaticLinkedList::<10>::new(100);
            assert_eq!(c.mem_limit, 100);
            assert_eq!(c.mem_used, 0);

            // enough memory for normal operation
            let mut last_mem_used = 0;
            for _ in 0..10 {
                c.put(0, 0, &[0], gix_object::Kind::Blob, 1);
                assert!(c.mem_used > last_mem_used);
                last_mem_used = c.mem_used;
            }
            assert_eq!(c.mem_used, 80, "there is a minimal vec size");
            assert_eq!(c.inner.len(), 10);
            assert_eq!(c.last_evicted.len(), 0);

            c.put(0, 0, &(0..20).collect::<Vec<_>>(), gix_object::Kind::Blob, 1);
            assert_eq!(c.inner.len(), 10);
            assert_eq!(c.mem_used, 80 + 20);
            assert_eq!(c.last_evicted.len(), 1);

            c.put(0, 0, &(0..50).collect::<Vec<_>>(), gix_object::Kind::Blob, 1);
            assert_eq!(c.inner.len(), 1, "cache clearance wasn't necessary");
            assert_eq!(c.last_evicted.len(), 0, "the free list was cleared");
            assert_eq!(c.mem_used, 50);

            c.put(0, 0, &(0..101).collect::<Vec<_>>(), gix_object::Kind::Blob, 1);
            assert_eq!(
                c.inner.len(),
                1,
                "objects that won't ever fit within the memory limit are ignored"
            );
        }
    }
}

#[cfg(feature = "pack-cache-lru-static")]
pub use _static::StaticLinkedList;
