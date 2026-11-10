//! Controlled byte-line preparation, without generic `Hash`, `Eq` or `ToOwned` callbacks.

use std::hash::{DefaultHasher, Hasher};

use crate::control::{BYTE_CHUNK, Cancelled, Check, Stage, poll};
use crate::{InternedInput, Interner, Token};

#[derive(Clone, Copy)]
struct IndexEntry {
    hash: u64,
    token: u32,
    next: u32,
}

/// Byte-only hash index. Growth is fallible and numeric rehashing is checked;
/// neither allocation nor rehash invokes generic Hash/Eq or byte callbacks.
#[derive(Default)]
pub(crate) struct ByteIndex {
    heads: Vec<u32>,
    entries: Vec<IndexEntry>,
}

impl ByteIndex {
    pub(crate) fn clear(&mut self) {
        self.heads.clear();
        self.entries.clear();
    }

    fn candidates(&self, hash: u64) -> impl Iterator<Item = (u64, u32)> + '_ {
        let mut next = if self.heads.is_empty() {
            u32::MAX
        } else {
            self.heads[hash as usize & (self.heads.len() - 1)]
        };
        std::iter::from_fn(move || {
            if next == u32::MAX {
                return None;
            }
            let entry = self.entries[next as usize];
            next = entry.next;
            Some((entry.hash, entry.token))
        })
    }

    pub(crate) fn prepare(&mut self, check: &mut Check<'_>) -> Result<(), Cancelled> {
        if self.heads.is_empty() || self.entries.len() >= self.heads.len() {
            let count = self.heads.len().saturating_mul(2).max(32);
            let mut heads = Vec::new();
            crate::control::try_reserve(&mut heads, count, check)?;
            while heads.len() < count {
                heads.resize((heads.len() + 1024).min(count), u32::MAX);
                poll(check, Stage::Index, heads.len())?;
            }
            let mut entries = Vec::new();
            crate::control::try_reserve(&mut entries, self.entries.len() + 1, check)?;
            for chunk in self.entries.chunks(1024) {
                entries.extend_from_slice(chunk);
                poll(check, Stage::Index, entries.len())?;
            }
            for (id, entry) in entries.iter_mut().enumerate() {
                let bucket = entry.hash as usize & (count - 1);
                entry.next = heads[bucket];
                heads[bucket] = id as u32;
                poll(check, Stage::Index, id + 1)?;
            }
            self.heads = heads;
            self.entries = entries;
        }
        crate::control::try_reserve(&mut self.entries, 1, check)
    }

    pub(crate) fn insert_prepared(&mut self, hash: u64, token: u32) {
        let bucket = hash as usize & (self.heads.len() - 1);
        let next = self.heads[bucket];
        self.heads[bucket] = self.entries.len() as u32;
        self.entries.push(IndexEntry { hash, token, next });
    }
}

/// Immutable owned line tokens prepared with checks within individual long lines.
///
/// The byte-specific index uses cached hashes and numeric keys, so growth
/// cannot invoke arbitrary `Hash` or `Eq` implementations.
/// On failure no partial input is returned. Vector growth is admitted before
/// fallible reservation; the numeric hash index has checked rehashing. The
/// caller must account for aggregate live allocations, not only each capacity.
pub struct ByteInput {
    input: InternedInput<Vec<u8>>,
}

impl ByteInput {
    /// Release per-line owned allocations before acknowledging request stop.
    pub fn clear_with_control(&mut self, check: &mut Check<'_>) {
        self.input.before.clear();
        self.input.after.clear();
        self.input.interner.clear_with_control(check);
    }

    /// Prepare, consume and clean up controlled line ownership, including when
    /// the consumer fails. Returned values cannot borrow this temporary input.
    pub fn with<R>(
        before: &[u8],
        after: &[u8],
        check: &mut Check<'_>,
        consume: impl FnOnce(&InternedInput<Vec<u8>>, &mut Check<'_>) -> Result<R, Cancelled>,
    ) -> Result<R, Cancelled> {
        let mut input = Self::try_new(before, after, check)?;
        let result = consume(input.as_input(), check);
        input.clear_with_control(check);
        result
    }
    /// Tokenize and intern both byte strings, preserving LF, CRLF and final no-LF
    /// semantics of `sources::byte_lines`. Empty input contains no tokens.
    pub fn try_new(before: &[u8], after: &[u8], check: &mut Check<'_>) -> Result<Self, Cancelled> {
        poll(check, Stage::Tokenize, 0)?;
        let mut input = InternedInput::default();
        let result = (|| {
            input.before = tokenize(before, &mut input.interner, check)?;
            input.after = tokenize(after, &mut input.interner, check)?;
            Ok(())
        })();
        if let Err(error) = result {
            input.interner.clear_with_control(check);
            return Err(error);
        }
        Ok(Self { input })
    }

    /// Read-only view for diffing and resolving token contents.
    pub fn as_input(&self) -> &InternedInput<Vec<u8>> {
        &self.input
    }
}

fn tokenize(bytes: &[u8], interner: &mut Interner<Vec<u8>>, check: &mut Check<'_>) -> Result<Vec<Token>, Cancelled> {
    let mut result = Vec::new();
    let mut start = 0;
    for (chunk_index, chunk) in bytes.chunks(BYTE_CHUNK).enumerate() {
        for (offset, &byte) in chunk.iter().enumerate() {
            if byte == b'\n' {
                let end = chunk_index * BYTE_CHUNK + offset + 1;
                let token = interner.try_intern_bytes(&bytes[start..end], check)?;
                crate::control::try_push(&mut result, token, check)?;
                start = end;
            }
        }
        poll(check, Stage::Tokenize, chunk_index * BYTE_CHUNK + chunk.len())?;
    }
    if start < bytes.len() {
        let token = interner.try_intern_bytes(&bytes[start..], check)?;
        crate::control::try_push(&mut result, token, check)?;
    }
    Ok(result)
}

pub(crate) fn hash_bytes(bytes: &[u8], check: &mut Check<'_>) -> Result<u64, Cancelled> {
    poll(check, Stage::Hash, 0)?;
    let mut hasher = DefaultHasher::new();
    for (i, chunk) in bytes.chunks(BYTE_CHUNK).enumerate() {
        hasher.write(chunk);
        poll(check, Stage::Hash, i * BYTE_CHUNK + chunk.len())?;
    }
    Ok(hasher.finish())
}

pub(crate) fn intern(
    bytes: &[u8],
    tokens: &mut Vec<Vec<u8>>,
    index: &mut ByteIndex,
    check: &mut Check<'_>,
) -> Result<Token, Cancelled> {
    let hash = hash_bytes(bytes, check)?;
    for (candidate_hash, id) in index.candidates(hash) {
        poll(check, Stage::Equal, 0)?;
        if candidate_hash != hash {
            continue;
        }
        let candidate = &tokens[id as usize];
        if candidate.len() != bytes.len() {
            poll(check, Stage::Equal, 0)?;
            continue;
        }
        let mut equal = true;
        for (i, (left, right)) in candidate.chunks(BYTE_CHUNK).zip(bytes.chunks(BYTE_CHUNK)).enumerate() {
            equal = left == right;
            poll(check, Stage::Equal, i * BYTE_CHUNK + left.len())?;
            if !equal {
                break;
            }
        }
        if equal {
            return Ok(Token(id));
        }
    }
    let id = u32::try_from(tokens.len()).map_err(|_| Cancelled)?;
    let mut owned = Vec::new();
    crate::control::try_reserve(&mut owned, bytes.len(), check)?;
    for (i, chunk) in bytes.chunks(BYTE_CHUNK).enumerate() {
        owned.extend_from_slice(chunk);
        poll(check, Stage::Copy, i * BYTE_CHUNK + chunk.len())?;
    }
    index.prepare(check)?;
    crate::control::try_push(tokens, owned, check)?;
    index.insert_prepared(hash, id);
    Ok(Token(id))
}
