// Modified for gitoxide from the upstream imara-diff crate.
// Upstream source: git cat-file -p 32d1e45d3df061e6ccba6db7fdce92db29e345d8:src/myers.rs

use crate::control::{Cancelled, Check, Stage, poll};
use std::ptr::NonNull;

use crate::intern::Token;
use crate::myers::middle_snake::{MiddleSnakeSearch, SearchResult};
use crate::myers::slice::FileSlice;
use crate::util::sqrt;

mod middle_snake;
mod preprocess;
mod slice;

/// Computes a diff using the Myers algorithm.
///
/// # Parameters
///
/// * `before` - The token sequence from the first file
/// * `after` - The token sequence from the second file
/// * `removed` - Output array marking removed tokens
/// * `added` - Output array marking added tokens
/// * `minimal` - If true, disables heuristics to guarantee a minimal diff
pub fn diff(before: &[Token], after: &[Token], removed: &mut [bool], added: &mut [bool], minimal: bool) {
    try_diff(before, after, removed, added, minimal, &mut |_| Ok(())).expect("uncontrolled check never fails");
}

pub(crate) fn try_diff(
    mut before: &[Token],
    mut after: &[Token],
    removed: &mut [bool],
    added: &mut [bool],
    minimal: bool,
    check: &mut Check<'_>,
) -> Result<(), Cancelled> {
    // Preserve the complete inputs for occurrence statistics, but defer counting until we know the
    // trimmed region needs a search.
    let full_before = before;
    let full_after = after;
    let common_prefix = crate::util::try_common_prefix(before, after, check)? as usize;
    before = &before[common_prefix..];
    after = &after[common_prefix..];
    let common_postfix = crate::util::try_common_postfix(before, after, check)? as usize;
    before = &before[..before.len() - common_postfix];
    after = &after[..after.len() - common_postfix];
    let removed_end = removed.len() - common_postfix;
    let removed = &mut removed[common_prefix..removed_end];
    let added_end = added.len() - common_postfix;
    let added = &mut added[common_prefix..added_end];

    // If either trimmed input is empty, the other contains only additions or removals. No search
    // or occurrence counting is needed, and we can return early.
    if before.is_empty() {
        for chunk in added.chunks_mut(1024) {
            chunk.fill(true);
            poll(check, Stage::Myers, chunk.len())?;
        }
        return Ok(());
    }
    if after.is_empty() {
        for chunk in removed.chunks_mut(1024) {
            chunk.fill(true);
            poll(check, Stage::Myers, chunk.len())?;
        }
        return Ok(());
    }

    // Like Git's `xdl_prepare_env()`, count every token of both complete inputs. Trimming only
    // limits the search and pruning windows, not the occurrence counts or frequency limits.
    let counts = preprocess::TokenCounts::try_new(full_before, full_after, minimal, check)?;

    // Preprocess the files by removing parts of the file that are not contained in the other file at all.
    // This process remaps the token indices so we have to account for that during the rest of the diff
    let (before, after) = preprocess::try_preprocess(&counts, before, after, removed, added, check)?;

    // Perform the actual diff
    Myers::try_new(before.tokens.len(), after.tokens.len(), check)?.run(
        FileSlice::new(&before, removed),
        FileSlice::new(&after, added),
        minimal,
        check,
        0,
    )
}

/// Minimum edit cost before heuristics are applied to avoid quadratic behavior.
const HEUR_MIN_COST: u32 = 256;
/// Minimum value for the maximum cost threshold.
const MAX_COST_MIN: u32 = 256;

/// The Myers diff algorithm implementation with linear space complexity.
///
/// This structure maintains the internal state needed to compute diffs using
/// Myers' algorithm with forward and backward search.
pub struct Myers {
    /// The backing memory for k-vectors.
    kvec: NonNull<[i32]>,
    /// Pointer to the forward search k-vector.
    kforward: NonNull<i32>,
    /// Pointer to the backward search k-vector.
    kbackward: NonNull<i32>,
    /// Maximum edit cost before applying heuristics.
    max_cost: u32,
}

impl Drop for Myers {
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.kvec.as_ptr())) }
    }
}

impl Myers {
    fn try_new(len1: usize, len2: usize, check: &mut Check<'_>) -> Result<Self, Cancelled> {
        let ndiags = len1 + len2 + 3;
        let len = 2 * ndiags + 2;
        let mut backing = Vec::new();
        crate::control::try_reserve(&mut backing, len, check)?;
        while backing.len() < len {
            backing.resize((backing.len() + 1024).min(len), 0);
            poll(check, Stage::Myers, backing.len())?;
        }
        // All fallible initialization precedes raw ownership transfer. Every
        // later error drops Myers normally and reconstructs this one Box.
        let kvec: *mut [i32] = Box::into_raw(backing.into_boxed_slice());
        let (kforward, kbackward) = unsafe {
            (
                NonNull::new_unchecked((kvec as *mut i32).add(len2 + 1)),
                NonNull::new_unchecked((kvec as *mut i32).add(ndiags + len2 + 1)),
            )
        };
        Ok(Self {
            kvec: unsafe { NonNull::new_unchecked(kvec) },
            kforward,
            kbackward,
            max_cost: sqrt(ndiags).max(MAX_COST_MIN),
        })
    }

    fn run<'f>(
        &mut self,
        mut file1: FileSlice<'f>,
        mut file2: FileSlice<'f>,
        mut need_min: bool,
        check: &mut Check<'_>,
        depth: usize,
    ) -> Result<(), Cancelled> {
        if depth >= crate::control::MAX_SEARCH_DEPTH {
            return Err(Cancelled);
        }
        loop {
            poll(check, Stage::Myers, 0)?;
            file1.try_strip_common(&mut file2, check)?;

            if file1.is_empty() {
                file2.try_mark_changed(check)?;
                return Ok(());
            } else if file2.is_empty() {
                file1.try_mark_changed(check)?;
                return Ok(());
            }

            let split = self.split(&file1, &file2, need_min, check)?;
            self.run(
                file1.borrow().slice(..split.token_idx1 as u32),
                file2.borrow().slice(..split.token_idx2 as u32),
                split.minimized_lo,
                check,
                depth + 1,
            )?;

            file1 = file1.slice(split.token_idx1 as u32..);
            file2 = file2.slice(split.token_idx2 as u32..);
            need_min = split.minimized_hi
        }
    }

    /// See "An O(ND) Difference Algorithm and its Variations", by Eugene Myers.
    /// Basically considers a "box" (off1, off2, lim1, lim2) and scan from both
    /// the forward diagonal starting from (off1, off2) and the backward diagonal
    /// starting from (lim1, lim2). If the K values on the same diagonal crosses
    /// returns the furthest point of reach. We might encounter expensive edge cases
    /// using this algorithm, so a little bit of heuristic is needed to cut the
    /// search and to return a suboptimal point.
    fn split(
        &mut self,
        file1: &FileSlice,
        file2: &FileSlice,
        need_min: bool,
        check: &mut Check<'_>,
    ) -> Result<Split, Cancelled> {
        let mut forward_search = unsafe { MiddleSnakeSearch::<false>::new(self.kforward, file1, file2) };
        let mut backwards_search = unsafe { MiddleSnakeSearch::<true>::new(self.kbackward, file1, file2) };
        let is_odd = file2.len().wrapping_sub(file1.len()) & 1 != 0;

        // Number the edit cost like git's `for (ec = 1;; ec++)` in `xdl_split()`: the heuristics below
        // compare against it and scale their threshold with it, so starting at 0 would apply them one
        // round late and with a smaller threshold.
        let mut ec = 0;

        loop {
            ec += 1;
            poll(check, Stage::Myers, ec as usize)?;
            let mut found_snake = false;
            forward_search.next_d();
            if is_odd {
                if let Some(res) = forward_search.run(
                    file1,
                    file2,
                    |k, token_idx1| backwards_search.contains(k) && backwards_search.x_pos_at_diagonal(k) <= token_idx1,
                    check,
                )? {
                    #[cfg(test)]
                    cov_mark::hit!(ODD_SPLIT);
                    match res {
                        SearchResult::Snake => found_snake = true,
                        SearchResult::Found { token_idx1, token_idx2 } => {
                            return Ok(Split {
                                token_idx1,
                                token_idx2,
                                minimized_lo: true,
                                minimized_hi: true,
                            });
                        }
                    }
                }
            } else {
                found_snake |= forward_search.run(file1, file2, |_, _| false, check)?.is_some()
            };

            backwards_search.next_d();
            if !is_odd {
                if let Some(res) = backwards_search.run(
                    file1,
                    file2,
                    |k, token_idx1| forward_search.contains(k) && token_idx1 <= forward_search.x_pos_at_diagonal(k),
                    check,
                )? {
                    #[cfg(test)]
                    cov_mark::hit!(EVEN_SPLIT);
                    match res {
                        SearchResult::Snake => found_snake = true,
                        SearchResult::Found { token_idx1, token_idx2 } => {
                            return Ok(Split {
                                token_idx1,
                                token_idx2,
                                minimized_lo: true,
                                minimized_hi: true,
                            });
                        }
                    }
                }
            } else {
                found_snake |= backwards_search.run(file1, file2, |_, _| false, check)?.is_some()
            };

            if need_min {
                continue;
            }

            // If the edit cost is above the heuristic trigger and if
            // we got a good snake, we sample current diagonals to see
            // if some of them have reached an "interesting" path. Our
            // measure is a function of the distance from the diagonal
            // corner (i1 + i2) penalized with the distance from the
            // mid-diagonal itself. If this value is above the current
            // edit cost times a magic factor (XDL_K_HEUR) we consider
            // it interesting.
            if found_snake && ec > HEUR_MIN_COST {
                if let Some((token_idx1, token_idx2)) = forward_search.found_snake(ec, file1, file2, check)? {
                    return Ok(Split {
                        token_idx1,
                        token_idx2,
                        minimized_lo: true,
                        minimized_hi: false,
                    });
                }

                if let Some((token_idx1, token_idx2)) = backwards_search.found_snake(ec, file1, file2, check)? {
                    return Ok(Split {
                        token_idx1,
                        token_idx2,
                        minimized_lo: false,
                        minimized_hi: true,
                    });
                }
            }

            // like git, settle for the furthest reaching path once the cost reaches
            // the limit.
            if ec >= self.max_cost {
                break;
            }
        }

        let (distance_forward, token_idx1_forward) = forward_search.best_position(file1, file2, check)?;
        let (distance_backwards, token_idx1_backwards) = backwards_search.best_position(file1, file2, check)?;
        Ok(
            if distance_forward > file1.len() as isize + file2.len() as isize - distance_backwards {
                Split {
                    token_idx1: token_idx1_forward,
                    token_idx2: (distance_forward - token_idx1_forward as isize) as i32,
                    minimized_lo: true,
                    minimized_hi: false,
                }
            } else {
                Split {
                    token_idx1: token_idx1_backwards,
                    token_idx2: (distance_backwards - token_idx1_backwards as isize) as i32,
                    minimized_lo: false,
                    minimized_hi: true,
                }
            },
        )
    }
}

/// Represents a split point in the divide-and-conquer approach.
///
/// The split divides the problem into two subproblems at the given token positions.
#[derive(Debug)]
struct Split {
    /// Token index in the first sequence where the split occurs.
    token_idx1: i32,
    /// Token index in the second sequence where the split occurs.
    token_idx2: i32,
    /// Whether the lower subproblem was minimized.
    minimized_lo: bool,
    /// Whether the upper subproblem was minimized.
    minimized_hi: bool,
}
