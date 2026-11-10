// Modified for gitoxide from the upstream imara-diff crate.
// Upstream source: git cat-file -p 32d1e45d3df061e6ccba6db7fdce92db29e345d8:src/util.rs

use crate::Hunk;
use crate::control::{Cancelled, Check, Stage, poll};
use crate::intern::Token;

pub(crate) fn try_common_prefix(a: &[Token], b: &[Token], check: &mut Check<'_>) -> Result<u32, Cancelled> {
    let mut count = 0;
    for (a, b) in a.chunks(1024).zip(b.chunks(1024)) {
        let matched = a.iter().zip(b).take_while(|(a, b)| a == b).count();
        count += matched;
        poll(check, Stage::Common, count)?;
        if matched < a.len().min(b.len()) {
            break;
        }
    }
    Ok(count as u32)
}

pub(crate) fn try_common_postfix(a: &[Token], b: &[Token], check: &mut Check<'_>) -> Result<u32, Cancelled> {
    let mut count = 0;
    for (a, b) in a.rchunks(1024).zip(b.rchunks(1024)) {
        let matched = a.iter().rev().zip(b.iter().rev()).take_while(|(a, b)| a == b).count();
        count += matched;
        poll(check, Stage::Common, count)?;
        if matched < a.len().min(b.len()) {
            break;
        }
    }
    Ok(count as u32)
}

/// Computes the number of common tokens at the start of two sequences.
pub fn common_prefix(file1: &[Token], file2: &[Token]) -> u32 {
    let mut off = 0;
    for (token1, token2) in file1.iter().zip(file2) {
        if token1 != token2 {
            break;
        }
        off += 1;
    }
    off
}

/// Computes the number of common tokens at the end of two sequences.
pub fn common_postfix(file1: &[Token], file2: &[Token]) -> u32 {
    let mut off = 0;
    for (token1, token2) in file1.iter().rev().zip(file2.iter().rev()) {
        if token1 != token2 {
            break;
        }
        off += 1;
    }
    off
}

/// Removes the common prefix from both sequences and returns its length.
pub fn strip_common_prefix(file1: &mut &[Token], file2: &mut &[Token]) -> u32 {
    let off = common_prefix(file1, file2);
    *file1 = &file1[off as usize..];
    *file2 = &file2[off as usize..];
    off
}

/// Removes the common postfix from both sequences and returns its length.
pub fn strip_common_postfix(file1: &mut &[Token], file2: &mut &[Token]) -> u32 {
    let off = common_postfix(file1, file2);
    *file1 = &file1[..file1.len() - off as usize];
    *file2 = &file2[..file2.len() - off as usize];
    off
}

/// Computes an approximation of the square root using bit operations.
/// This returns a u32 instead of a u64 like `xdl_bogosqrt` does, so
/// when `val` has >=63 significant bits the result clamps to `u32::MAX`
/// to avoid an overflow.
pub fn sqrt(val: usize) -> u32 {
    // git's xdl_bogosqrt shifts `n` right by two per doubling and stops
    // once it reaches zero, which rounds the halved bit count up:
    //
    //     for (i = 1; n > 0; n >>= 2) i <<= 1;
    //
    // Take the ceiling here to avoid the integer division rounding down.
    let nbits = (usize::BITS - val.leading_zeros()).div_ceil(2);
    1u32.checked_shl(nbits).unwrap_or(u32::MAX)
}

impl Hunk {
    pub(crate) fn try_next_hunk(
        &mut self,
        removed: &[bool],
        added: &[bool],
        check: &mut Check<'_>,
    ) -> Result<bool, Cancelled> {
        let Some(off) = try_find_next_change(added, self.after.end, check)? else {
            return Ok(false);
        };
        let mut off_before = 0;
        loop {
            debug_assert!(
                removed.len() as u32 != self.before.end || off == 0,
                "broken hunk alignment {self:?} "
            );
            let unchanged_tokens = try_find_next_change(removed, self.before.end, check)?
                .unwrap_or(removed.len() as u32 - self.before.end);
            if off_before + unchanged_tokens > off {
                self.before.start = self.before.end + (off - off_before);
                self.before.end = self.before.start;
                break;
            }
            off_before += unchanged_tokens;
            self.before.start = self.before.end + unchanged_tokens;
            self.before.end = try_find_hunk_end(removed, self.before.end + unchanged_tokens, check)?;
            if off_before == off {
                break;
            }
        }
        self.after.start = self.after.end + off;
        self.after.end = try_find_hunk_end(added, self.after.start, check)?;
        Ok(true)
    }
}

pub(crate) fn try_find_next_change(
    changes: &[bool],
    pos: u32,
    check: &mut Check<'_>,
) -> Result<Option<u32>, Cancelled> {
    for (n, chunk) in changes[pos as usize..].chunks(1024).enumerate() {
        let found = chunk.iter().position(|&c| c);
        poll(check, Stage::Postprocess, n * 1024 + found.unwrap_or(chunk.len()))?;
        if let Some(off) = found {
            return Ok(Some((n * 1024 + off) as u32));
        }
    }
    Ok(None)
}

pub(crate) fn try_find_hunk_end(changes: &[bool], pos: u32, check: &mut Check<'_>) -> Result<u32, Cancelled> {
    let mut count = 0;
    for chunk in changes[pos as usize..].chunks(1024) {
        let matched = chunk.iter().take_while(|&&c| c).count();
        count += matched;
        poll(check, Stage::Postprocess, count)?;
        if matched < chunk.len() {
            break;
        }
    }
    Ok(pos + count as u32)
}

pub(crate) fn try_find_hunk_start(changes: &[bool], pos: u32, check: &mut Check<'_>) -> Result<u32, Cancelled> {
    let mut count = 0;
    for chunk in changes[..pos as usize].rchunks(1024) {
        let matched = chunk.iter().rev().take_while(|&&c| c).count();
        count += matched;
        poll(check, Stage::Postprocess, count)?;
        if matched < chunk.len() {
            break;
        }
    }
    Ok(pos - count as u32)
}
