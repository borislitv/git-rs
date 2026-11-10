//! Service-boundary validation before invoking existing object parsers.
use gix_diff::blob::control::{BYTE_CHUNK, Check, Checkpoint, Stage};
use gix_error::{ErrorExt, Result, message};
use std::cell::RefCell;

pub(super) const METADATA_LIMIT: usize = 65536;
pub(super) const OBJECT_LIMIT: usize = 128 * 1024 * 1024;

/// Scalar priority entries use exactly the legacy key-only BinaryHeap ordering,
/// including equal-key behavior; capacity is admitted before heap growth.
#[derive(Eq)]
struct Entry(i64, gix_hash::ObjectId);
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
#[derive(Default)]
pub(super) struct Queue(std::collections::BinaryHeap<Entry>);
impl Queue {
    pub fn insert(&mut self, time: i64, id: gix_hash::ObjectId, check: &mut Check<'_>) -> Result {
        if self.0.len() == self.0.capacity() {
            let capacity = self.0.capacity().saturating_mul(2).max(8);
            gix_diff::blob::control::admit_bytes(capacity.saturating_mul(std::mem::size_of::<Entry>()), check)
                .map_err(|e| e.raise())?;
            self.0
                .try_reserve_exact(capacity - self.0.len())
                .map_err(|_| message("blame queue allocation failed").raise())?;
        }
        self.0.push(Entry(time, id));
        Ok(())
    }
    pub fn pop_value(&mut self) -> Option<gix_hash::ObjectId> {
        self.0.pop().map(|e| e.1)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub(crate) type Shared<'a, 'b> = RefCell<&'a mut Check<'b>>;

/// A Vec of request-owned allocation owners, with checked cleanup on every exit.
pub(crate) struct VecOwner<'a, 'b, 'c, T> {
    data: Vec<T>,
    shared: &'a Shared<'b, 'c>,
}
impl<'a, 'b, 'c, T> VecOwner<'a, 'b, 'c, T> {
    pub fn new(data: Vec<T>, shared: &'a Shared<'b, 'c>) -> Self {
        Self { data, shared }
    }
    pub fn take(&mut self) -> Vec<T> {
        std::mem::take(&mut self.data)
    }
    pub fn take_iter(&mut self) -> IterOwner<'a, 'b, 'c, T> {
        IterOwner {
            data: self.take().into_iter(),
            shared: self.shared,
        }
    }
}
impl<T> std::ops::Deref for VecOwner<'_, '_, '_, T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Vec<T> {
        &self.data
    }
}
impl<T> std::ops::DerefMut for VecOwner<'_, '_, '_, T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }
}
impl<T> Drop for VecOwner<'_, '_, '_, T> {
    fn drop(&mut self) {
        gix_diff::blob::control::clear_with(&mut self.data, &mut **self.shared.borrow_mut());
    }
}
pub(crate) struct IterOwner<'a, 'b, 'c, T> {
    data: std::vec::IntoIter<T>,
    shared: &'a Shared<'b, 'c>,
}
impl<T> Iterator for IterOwner<'_, '_, '_, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.data.next()
    }
}
impl<T> Drop for IterOwner<'_, '_, '_, T> {
    fn drop(&mut self) {
        while self.data.len() != 0 {
            for item in self.data.by_ref().take(32) {
                drop(item);
            }
            let _ = (**self.shared.borrow_mut())(Checkpoint {
                stage: Stage::Cleanup,
                processed: self.data.len(),
            });
        }
    }
}

pub(super) struct PreparedOwner<'a, 'b, 'c> {
    pub data: Option<super::prepared::PreparedInput>,
    pub shared: &'a Shared<'b, 'c>,
}

pub(super) struct ByteOwner<'a, 'b, 'c> {
    pub data: gix_diff::blob::byte_input::ByteInput,
    pub shared: &'a Shared<'b, 'c>,
}
impl Drop for ByteOwner<'_, '_, '_> {
    fn drop(&mut self) {
        self.data.clear_with_control(&mut **self.shared.borrow_mut());
    }
}
impl std::ops::Deref for PreparedOwner<'_, '_, '_> {
    type Target = Option<super::prepared::PreparedInput>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl std::ops::DerefMut for PreparedOwner<'_, '_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
impl Drop for PreparedOwner<'_, '_, '_> {
    fn drop(&mut self) {
        if let Some(input) = self.data.as_mut() {
            input.clear_with_control(&mut **self.shared.borrow_mut());
        }
    }
}

pub(super) struct Objects<'a, 'borrow, 'check, O> {
    pub inner: O,
    pub check: &'a RefCell<&'borrow mut Check<'check>>,
}

fn tick(check: &mut Check<'_>, processed: usize) -> Result {
    check(Checkpoint {
        stage: Stage::Graph,
        processed,
    })
    .map_err(|e| e.raise())
}

fn validate(data: gix_object::Data<'_>, check: &mut Check<'_>) -> Result {
    if data.data.len() > OBJECT_LIMIT {
        return Err(message("controlled object exceeds 128 MiB limit").into());
    }
    match data.kind {
        gix_object::Kind::Commit => {
            // The maintained commit parser never needs to scan the message for
            // ancestry/blame. Bound the entire header (including continuations),
            // not only individual lines, before committer()/tree_id() sees it.
            let mut previous_lf = false;
            for (n, chunk) in data.data.chunks(BYTE_CHUNK).enumerate() {
                for (off, &byte) in chunk.iter().enumerate() {
                    if n * BYTE_CHUNK + off >= METADATA_LIMIT {
                        return Err(message("controlled commit header exceeds 64 KiB metadata limit").into());
                    }
                    if previous_lf && byte == b'\n' {
                        tick(check, n * BYTE_CHUNK + off + 1)?;
                        return Ok(());
                    }
                    previous_lf = byte == b'\n';
                }
                tick(check, n * BYTE_CHUNK + chunk.len())?;
            }
            // Truncated headers are left to the maintained parser for its error.
        }
        gix_object::Kind::Tree => {
            let mut pos = 0;
            while pos < data.data.len() {
                let mode = data.data[pos..]
                    .iter()
                    .take(8)
                    .position(|&b| b == b' ')
                    .ok_or_else(|| message("invalid controlled tree mode record").raise())?;
                pos += mode + 1;
                let start = pos;
                loop {
                    let end = (pos + BYTE_CHUNK).min(data.data.len());
                    let found = data.data[pos..end].iter().position(|&b| b == 0);
                    tick(check, end)?;
                    if let Some(off) = found {
                        pos += off + 1;
                        break;
                    }
                    pos = end;
                    if pos - start > METADATA_LIMIT || pos == data.data.len() {
                        return Err(
                            message("controlled tree name is truncated or exceeds 64 KiB metadata limit").into(),
                        );
                    }
                }
                if pos - start > METADATA_LIMIT + 1 {
                    return Err(message("controlled tree name exceeds 64 KiB metadata limit").into());
                }
                pos = pos
                    .checked_add(data.object_hash.len_in_bytes())
                    .ok_or_else(|| message("tree offset overflow").raise())?;
                if pos > data.data.len() {
                    return Err(message("truncated controlled tree object id").into());
                }
            }
        }
        _ => {}
    }
    Ok(())
}

impl<O: gix_object::Find + gix_object::FindHeader> gix_object::Find for Objects<'_, '_, '_, O> {
    fn try_find<'b>(&self, id: &gix_hash::oid, buffer: &'b mut Vec<u8>) -> Result<Option<gix_object::Data<'b>>> {
        tick(&mut **self.check.borrow_mut(), 0)?;
        let Some(header) = self.inner.try_header(id)? else {
            return Ok(None);
        };
        if header.size > OBJECT_LIMIT as u64 {
            return Err(message("controlled object header exceeds 128 MiB limit").into());
        }
        let size = usize::try_from(header.size).map_err(|_| message("object size overflow").raise())?;
        let additional = size.saturating_sub(buffer.len());
        gix_diff::blob::control::try_reserve(buffer, additional, &mut **self.check.borrow_mut())
            .map_err(|e| e.raise())?;
        let data = self.inner.try_find(id, buffer)?;
        if let Some(data) = data {
            if data.kind != header.kind || data.data.len() != size {
                return Err(message("controlled object disagrees with its header").into());
            }
            validate(data, &mut **self.check.borrow_mut())?;
        }
        Ok(data)
    }
}

impl<O: gix_object::FindHeader> gix_object::FindHeader for Objects<'_, '_, '_, O> {
    fn try_header(&self, id: &gix_hash::oid) -> Result<Option<gix_object::Header>> {
        tick(&mut **self.check.borrow_mut(), 0)?;
        let header = self.inner.try_header(id)?;
        if header.as_ref().is_some_and(|h| h.size > OBJECT_LIMIT as u64) {
            return Err(message("controlled object header exceeds 128 MiB limit").into());
        }
        Ok(header)
    }
}
