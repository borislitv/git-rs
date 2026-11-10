// Modified for gitoxide from the upstream imara-diff crate.
// Upstream source: git cat-file -p 32d1e45d3df061e6ccba6db7fdce92db29e345d8:src/myers/slice.rs

use std::mem::take;
use std::ops::RangeBounds;

use crate::control::{Cancelled, Check, Stage, poll};
use crate::intern::Token;
use crate::myers::preprocess::PreprocessedFile;

/// A slice of a preprocessed file used during the Myers algorithm's divide-and-conquer.
///
/// This structure allows the algorithm to work on subproblems while maintaining
/// references to the original file's token indices and change tracking.
#[derive(Default)]
pub struct FileSlice<'a> {
    /// The tokens in this slice.
    pub tokens: &'a [Token],
    /// Maps from slice positions back to original file positions.
    indices: &'a [u32],
    /// Tracks which tokens in the original file have changed.
    changed: &'a mut [bool],
}

impl<'a> FileSlice<'a> {
    pub fn new(file: &'a PreprocessedFile, changed: &'a mut [bool]) -> Self {
        Self {
            tokens: &file.tokens,
            indices: &file.indices,
            changed,
        }
    }

    pub fn try_mark_changed(&mut self, check: &mut Check<'_>) -> Result<(), Cancelled> {
        for (n, indices) in self.indices.chunks(1024).enumerate() {
            for &i in indices {
                self.changed[i as usize] = true;
            }
            poll(check, Stage::Myers, n * 1024 + indices.len())?;
        }
        Ok(())
    }

    pub fn borrow(&mut self) -> FileSlice<'_> {
        FileSlice {
            tokens: self.tokens,
            changed: self.changed,
            indices: self.indices,
        }
    }

    pub fn slice<R: RangeBounds<u32>>(self, range: R) -> Self {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&end) => end + 1,
            std::ops::Bound::Excluded(&end) => end,
            std::ops::Bound::Unbounded => self.len(),
        };

        Self {
            tokens: &self.tokens[start as usize..end as usize],
            changed: self.changed,
            indices: &self.indices[start as usize..end as usize],
        }
    }

    pub fn try_strip_common(&mut self, other: &mut Self, check: &mut Check<'_>) -> Result<(), Cancelled> {
        let start = crate::util::try_common_prefix(self.tokens, other.tokens, check)?;
        let suffix =
            crate::util::try_common_postfix(&self.tokens[start as usize..], &other.tokens[start as usize..], check)?;
        let end = self.len() - suffix;
        *self = take(self).slice(start..end);
        let end = other.len() - suffix;
        *other = take(other).slice(start..end);
        Ok(())
    }

    pub fn len(&self) -> u32 {
        self.tokens.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}
