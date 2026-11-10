use gix_error::Result;
use std::num::NonZeroU32;

use gix_diff::blob::control::{BYTE_CHUNK, Check, Checkpoint, Stage};
use gix_diff::{blob::TokenSource, tree::Visit};
use gix_error::{ErrorExt, OptionExt, ResultExt, message};
use gix_hash::ObjectId;
use gix_object::{
    FindExt,
    bstr::{BStr, BString},
};
use gix_traverse::commit::find as find_commit;
use smallvec::SmallVec;

fn poll(check: &mut Check<'_>, stage: Stage, processed: usize) -> Result {
    check(Checkpoint { stage, processed }).map_err(|err| err.raise())
}

/// A controlled result pinned to all its conservative per-job capacity charges.
/// Output cannot be detached from its admission owner.
pub struct AdmittedOutcome {
    outcome: Outcome,
    _allocations: gix_diff::blob::control::Allocations,
}

impl AdmittedOutcome {
    /// Borrow output while its backing-capacity charges remain live.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Release output's nested ownership in checked batches before an executor
    /// acknowledges stop (including cancellation during serialization). Errors
    /// cannot skip cleanup; capacity charges remain pinned until this owner drops.
    pub fn clear_with_control(&mut self, check: &mut Check<'_>) {
        gix_diff::blob::control::clear_with(&mut self.outcome.entries, check);
        if let Some(path) = self.outcome.blame_path.as_mut() {
            gix_diff::blob::control::clear_with(path, check);
        }
        self.outcome.blob.clear();
        // Free even the now-empty Vec backings before releasing their charges.
        drop(std::mem::take(&mut self.outcome));
        self._allocations.clear_with_control(check);
    }
}

impl Drop for AdmittedOutcome {
    fn drop(&mut self) {
        self.clear_with_control(&mut |_| Ok(()));
    }
}

/// Production entrypoint with explicit per-job allocation ownership. The
/// provider must implement bounded, controlled header and object reads. Charges
/// are conservative cumulative backing growth, not logical cache weights.
#[expect(clippy::too_many_arguments)]
pub fn file_with_allocations(
    odb: impl gix_object::Find + gix_object::FindHeader,
    start: Start,
    cache: Option<gix_commitgraph::Graph>,
    resource_cache: &mut gix_diff::blob::Platform,
    file_path: &BStr,
    options: Options,
    tuning: crate::Tuning,
    allocator: std::sync::Arc<dyn gix_diff::blob::control::Allocator>,
    check: &mut Check<'_>,
) -> Result<AdmittedOutcome> {
    let mut allocations = gix_diff::blob::control::Allocations::new(allocator).map_err(|e| e.raise())?;
    let result = file_with_control(
        odb,
        start,
        cache,
        resource_cache,
        file_path,
        options,
        tuning,
        &mut |event| {
            check(event)?;
            if event.stage == Stage::Allocate {
                allocations.admit(event.processed)?;
            }
            Ok(())
        },
    );
    match result {
        Ok(outcome) => Ok(AdmittedOutcome {
            outcome,
            _allocations: allocations,
        }),
        Err(error) => {
            allocations.clear_with_control(check);
            Err(error)
        }
    }
}

fn try_copy_bytes(data: &[u8], check: &mut Check<'_>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    gix_diff::blob::control::try_reserve(&mut out, data.len(), check).map_err(|e| e.raise())?;
    for (n, chunk) in data.chunks(BYTE_CHUNK).enumerate() {
        out.extend_from_slice(chunk);
        poll(check, Stage::Copy, n * BYTE_CHUNK + chunk.len())?;
    }
    Ok(out)
}

use super::bounded::{PreparedOwner, VecOwner};
use super::{Change, UnblamedHunk, prepared::PreparedInput};
#[cfg(test)]
use super::{process_changes, process_changes_indexed};
use crate::{
    BlameEntry, Options, Outcome, Statistics,
    types::{BlamePathEntry, Start},
};

/// Produce a list of consecutive [`BlameEntry`] instances to indicate in which commits the ranges of the file
/// at `suspect:<file_path>` originated in.
///
/// ## Parameters
///
/// * `odb`
///    - Access to database objects, also for used for diffing.
///    - Should have an object cache for good diff performance.
/// * `start`
///    - Where to start the blame. Can be either a commit or contents of a worktree file that contains
///      untracked changes.
/// * `cache`
///    - Optionally, the commitgraph cache.
/// * `resource_cache`
///    - Used for diffing trees.
/// * `file_path`
///    - A *slash-separated* worktree-relative path to the file to blame.
/// * `options`
///    - An instance of [`Options`].
///
/// ## The algorithm
///
/// *For brevity, `HEAD` denotes the starting point of the blame operation. It could be any commit, or even commits that
/// represent the worktree state.
///
/// We begin with one or more *Unblamed Hunks* and a single suspect, usually the `HEAD` commit as the commit containing the
/// *Blamed File*, so that it contains the entire file, with the first commit being a candidate for the entire *Blamed File*.
/// We traverse the commit graph starting at the first suspect, and see if there have been changes to `file_path`.
/// If so, we have found a *Source File* and a *Suspect* commit, and have hunks that represent these changes.
/// Now the *Unblamed Hunk* is split at the boundaries of each matching change, creating a new *Unblamed Hunk* on each side,
/// along with a [`BlameEntry`] to represent the match.
/// This is repeated until there are no non-empty *Unblamed Hunk*s left.
///
/// At a high level, what we want to do is the following:
///
/// - get the commit
/// - walk through its parents
///   - for each parent, do a diff and mark lines that don’t have a suspect yet (this is the term
///     used in `libgit2`), but that have been changed in this commit
///
/// The algorithm in `libgit2` works by going through parents and keeping a linked list of blame
/// suspects. It can be visualized as follows:
///
/// <---------------------------------------->
/// <---------------><----------------------->
/// <---><----------><----------------------->
/// <---><----------><-------><-----><------->
/// <---><---><-----><-------><-----><------->
/// <---><---><-----><-------><-----><-><-><->
pub fn file(
    odb: impl gix_object::Find + gix_object::FindHeader,
    start: Start,
    cache: Option<gix_commitgraph::Graph>,
    resource_cache: &mut gix_diff::blob::Platform,
    file_path: &BStr,
    options: Options,
) -> Result<Outcome> {
    file_with_tuning(
        odb,
        start,
        cache,
        resource_cache,
        file_path,
        options,
        crate::Tuning::default(),
    )
}

/// Like [`file`], with immutable request-local optimization choices. The legacy
/// entrypoint uses default tuning; neither entrypoint consults process environment.
pub fn file_with_tuning(
    odb: impl gix_object::Find + gix_object::FindHeader,
    start: Start,
    cache: Option<gix_commitgraph::Graph>,
    resource_cache: &mut gix_diff::blob::Platform,
    file_path: &BStr,
    options: Options,
    tuning: crate::Tuning,
) -> Result<Outcome> {
    file_impl(
        odb,
        start,
        cache,
        resource_cache,
        file_path,
        options,
        tuning,
        &mut |_| Ok(()),
        false,
    )
}

/// Blame with a borrowed request-local fallible work check. Adapters can check
/// both an atomic cancellation flag and an absolute monotonic deadline. Errors
/// discard the entire outcome. Providers and configured conversion pipelines
/// must also be bounded: this callback cannot interrupt arbitrary provider code.
#[expect(clippy::too_many_arguments)]
pub fn file_with_control(
    odb: impl gix_object::Find + gix_object::FindHeader,
    start: Start,
    cache: Option<gix_commitgraph::Graph>,
    resource_cache: &mut gix_diff::blob::Platform,
    file_path: &BStr,
    options: Options,
    tuning: crate::Tuning,
    check: &mut Check<'_>,
) -> Result<Outcome> {
    resource_cache.validate_controlled_raw()?;
    if let Start::Contents { contents, .. } = &start
        && contents.len() > super::bounded::OBJECT_LIMIT
    {
        return Err(message("controlled blame contents exceed 128 MiB limit").into());
    }
    if file_path.len() > super::bounded::METADATA_LIMIT {
        return Err(message("controlled blame path exceeds 64 KiB metadata limit").into());
    }
    let shared = std::cell::RefCell::new(check);
    let objects = super::bounded::Objects {
        inner: odb,
        check: &shared,
    };
    let result = file_impl(
        objects,
        start,
        cache,
        resource_cache,
        file_path,
        options,
        tuning,
        &mut |event| (**shared.borrow_mut())(event),
        true,
    );
    resource_cache.clear_controlled_raw(&mut |event| (**shared.borrow_mut())(event));
    result
}

#[expect(clippy::too_many_arguments)]
fn file_impl(
    odb: impl gix_object::Find + gix_object::FindHeader,
    start: Start,
    cache: Option<gix_commitgraph::Graph>,
    resource_cache: &mut gix_diff::blob::Platform,
    file_path: &BStr,
    options: Options,
    tuning: crate::Tuning,
    check: &mut Check<'_>,
    raw: bool,
) -> Result<Outcome> {
    let shared = std::cell::RefCell::new(check);
    let mut forwarding = |event| (**shared.borrow_mut())(event);
    let check: &mut Check<'_> = &mut forwarding;
    poll(check, Stage::Preprocess, 0)?;
    let _span = if raw {
        None
    } else {
        Some(gix_trace::coarse!("gix_blame::file()", ?file_path, ?start))
    };

    let mut stats = Statistics::default();
    let (mut buf, mut buf2, mut buf3) = (Vec::new(), Vec::new(), Vec::new());

    let InitialState {
        blamed_file_blob,
        hunks_to_blame,
        out,
        first_suspect,
    } = initial_state(
        &odb,
        start,
        cache.as_ref(),
        file_path,
        &options,
        &mut buf,
        &mut buf2,
        &mut stats,
        check,
    )?;
    let mut hunks_to_blame = VecOwner::new(hunks_to_blame, &shared);
    let mut out = VecOwner::new(out, &shared);

    if hunks_to_blame.is_empty() {
        gix_diff::blob::control::try_sort_by(
            &mut out,
            |a, b, _| Ok(a.start_in_blamed_file.cmp(&b.start_in_blamed_file)),
            check,
        )
        .map_err(|e| e.raise())?;
        return Ok(Outcome {
            entries: try_coalesce_blame_entries(out.take(), check)?,
            blob: blamed_file_blob,
            statistics: stats,
            blame_path: None,
        });
    }

    let mut queue = super::bounded::Queue::default();

    if let Some(first_suspect) = first_suspect {
        let commit = find_commit(cache.as_ref(), &odb, &first_suspect, &mut buf)?;
        queue.insert(commit.commit_time()?, first_suspect, check)?;
    }

    let mut diff_state = gix_diff::tree::State::default();
    let mut previous_entry: Option<(ObjectId, ObjectId)> = None;
    let mut blame_path = VecOwner::new(Vec::new(), &shared);

    // Object IDs identify raw immutable bytes, not a path-dependent worktree or
    // text-conversion result. Only the raw object pipeline can reuse by ID.
    let prepared = (tuning.reuse_line_tokens
        && resource_cache.filter_mode == gix_diff::blob::pipeline::Mode::ToGit
        && resource_cache.filter.roots.old_root.is_none()
        && resource_cache.filter.roots.new_root.is_none())
    .then(|| {
        let prepared = PreparedInput::default();
        let prepared = if tuning.retain_current_input_tokens {
            prepared.with_current_input_credit()
        } else {
            prepared
        };
        if tuning.reuse_unchanged_runs {
            prepared.with_unchanged_runs()
        } else {
            prepared
        }
    });
    let mut prepared = PreparedOwner {
        data: prepared,
        shared: &shared,
    };
    let bounded_resource_cache = prepared.is_some() || tuning.bounded_resource_cache;
    let index_changes = tuning.index_changes;
    'outer: while let Some(suspect) = queue.pop_value() {
        poll(check, Stage::Graph, stats.commits_traversed)?;
        // Each revision sets both resources again. The NxM diff cache must not
        // retain every historical fulltext for the entire blame traversal.
        if raw {
            // The raw platform itself retains at most two resources.
        } else if prepared.is_some() {
            resource_cache.retain_current_resources();
        } else if bounded_resource_cache {
            resource_cache.clear_resource_cache_keep_allocation();
        }
        stats.commits_traversed += 1;
        if hunks_to_blame.is_empty() {
            break;
        }

        let mut first_hunk_for_suspect = None;
        for hunk in hunks_to_blame.iter() {
            poll(check, Stage::Attribution, 1)?;
            if hunk.has_suspect(&suspect) {
                first_hunk_for_suspect = Some(hunk);
                break;
            }
        }
        let Some(first_hunk_for_suspect) = first_hunk_for_suspect else {
            // There are no `UnblamedHunk`s associated with this `suspect`, so we can continue with
            // the next one.
            continue 'outer;
        };

        let current_file_path: BString = try_copy_bytes(
            first_hunk_for_suspect
                .source_file_name
                .as_ref()
                .map_or(file_path.as_ref(), |p| p.as_slice()),
            check,
        )?
        .into();

        let commit = find_commit(cache.as_ref(), &odb, &suspect, &mut buf)?;
        let commit_time = commit.commit_time()?;

        if let Some(since) = options.since
            && commit_time < since.seconds
        {
            if try_unblamed_to_out_is_done(&mut hunks_to_blame, &mut out, suspect, check)? {
                break 'outer;
            }

            continue;
        }

        let parent_ids: ParentIds = collect_parents(commit, &odb, cache.as_ref(), &mut buf2, check, raw)?;

        if parent_ids.is_empty() {
            if queue.is_empty() {
                // I’m not entirely sure if this is correct yet. `suspect`, at this point, is the
                // `id` of the last `item` that was yielded by `queue`, so it makes sense to assign
                // the remaining lines to it, even though we don’t explicitly check whether that is
                // true here. We could perhaps use diff-tree-to-tree to compare `suspect` against
                // an empty tree to validate this assumption.
                if try_unblamed_to_out_is_done(&mut hunks_to_blame, &mut out, suspect, check)? {
                    if options.debug_track_path {
                        let entry = previous_entry
                            .take()
                            .filter(|(id, _)| *id == suspect)
                            .map(|(_, entry)| entry);

                        let blame_path_entry = BlamePathEntry {
                            source_file_path: try_copy_bytes(&current_file_path, check)?.into(),
                            previous_source_file_path: None,
                            commit_id: suspect,
                            blob_id: entry.unwrap_or(gix_hash::Kind::shortest().null()),
                            previous_blob_id: gix_hash::Kind::shortest().null(),
                            parent_index: 0,
                        };
                        gix_diff::blob::control::try_push(&mut blame_path, blame_path_entry, check)
                            .map_err(|e| e.raise())?;
                    }

                    break 'outer;
                }
            }
            // There is more, keep looking.
            continue;
        }

        let mut entry = previous_entry
            .take()
            .filter(|(id, _)| *id == suspect)
            .map(|(_, entry)| entry);
        if entry.is_none() {
            entry = find_path_entry_in_commit(
                &odb,
                &suspect,
                current_file_path.as_ref(),
                cache.as_ref(),
                &mut buf,
                &mut buf2,
                &mut stats,
                check,
            )?;
        }

        let Some(entry_id) = entry else {
            continue;
        };

        // This block asserts that, for every `UnblamedHunk`, all lines in the *Blamed File* are
        // identical to the corresponding lines in the *Source File*.
        #[cfg(debug_assertions)]
        {
            let source_blob = odb.find_blob(&entry_id, &mut buf)?.data;
            let owned = super::bounded::ByteOwner {
                data: gix_diff::blob::byte_input::ByteInput::try_new(source_blob, &blamed_file_blob, check)
                    .map_err(|err| err.raise())?,
                shared: &shared,
            };
            let input = owned.data.as_input();

            for hunk in hunks_to_blame.iter() {
                if let Some(range_in_suspect) = hunk.get_range(&suspect) {
                    let range_in_blamed_file = hunk.range_in_blamed_file.clone();

                    let source = &input.before[range_in_suspect.start as usize..range_in_suspect.end as usize];
                    let blamed = &input.after[range_in_blamed_file.start as usize..range_in_blamed_file.end as usize];
                    assert_eq!(source.len(), blamed.len());
                    for (n, (source, blamed)) in source.chunks(1024).zip(blamed.chunks(1024)).enumerate() {
                        assert_eq!(source, blamed);
                        poll(check, Stage::Equal, n * 1024 + source.len())?;
                    }
                }
            }
        }

        for (pid, (parent_id, parent_commit_time)) in parent_ids.iter().enumerate() {
            poll(check, Stage::Graph, pid)?;
            if let Some(parent_entry_id) = find_path_entry_in_commit(
                &odb,
                parent_id,
                current_file_path.as_ref(),
                cache.as_ref(),
                &mut buf,
                &mut buf2,
                &mut stats,
                check,
            )? {
                let no_change_in_entry = entry_id == parent_entry_id;
                if pid == 0 {
                    previous_entry = Some((*parent_id, parent_entry_id));
                }
                if no_change_in_entry {
                    try_pass_blame_from_to(suspect, *parent_id, &mut hunks_to_blame, check)?;
                    queue.insert(*parent_commit_time, *parent_id, check)?;
                    continue 'outer;
                }
            }
        }

        let more_than_one_parent = parent_ids.len() > 1;
        for (index, (parent_id, parent_commit_time)) in parent_ids.iter().enumerate() {
            poll(check, Stage::Graph, index)?;
            queue.insert(*parent_commit_time, *parent_id, check)?;
            let changes_for_file_path = tree_diff_at_file_path(
                &odb,
                current_file_path.as_ref(),
                suspect,
                *parent_id,
                cache.as_ref(),
                &mut stats,
                &mut diff_state,
                resource_cache,
                &mut buf,
                &mut buf2,
                &mut buf3,
                options.rewrites,
                check,
                raw,
            )?;
            let Some(modification) = changes_for_file_path else {
                if more_than_one_parent {
                    // None of the changes affected the file we’re currently blaming.
                    // Copy blame to parent.
                    for unblamed_hunk in hunks_to_blame.iter_mut() {
                        super::admit_hunk(unblamed_hunk, check).map_err(|e| e.raise())?;
                        if unblamed_hunk.suspects.len() >= 128 {
                            return Err(gix_diff::blob::control::Cancelled.raise());
                        }
                        if unblamed_hunk.suspects.len() == unblamed_hunk.suspects.capacity() {
                            gix_diff::blob::control::admit_bytes(
                                (unblamed_hunk.suspects.len() + 1)
                                    * std::mem::size_of::<(ObjectId, std::ops::Range<u32>)>(),
                                check,
                            )
                            .map_err(|e| e.raise())?;
                            unblamed_hunk
                                .suspects
                                .try_reserve_exact(1)
                                .map_err(|_| message("suspect allocation failed").raise())?;
                        }
                        unblamed_hunk.clone_blame(suspect, *parent_id);
                        poll(check, Stage::Attribution, index)?;
                    }
                } else {
                    try_pass_blame_from_to(suspect, *parent_id, &mut hunks_to_blame, check)?;
                }
                continue;
            };

            match modification {
                TreeDiffChange::Addition { id } => {
                    if more_than_one_parent {
                        // Do nothing under the assumption that this always (or almost always)
                        // implies that the file comes from a different parent, compared to which
                        // it was modified, not added.
                    } else if try_unblamed_to_out_is_done(&mut hunks_to_blame, &mut out, suspect, check)? {
                        if options.debug_track_path {
                            let blame_path_entry = BlamePathEntry {
                                source_file_path: try_copy_bytes(&current_file_path, check)?.into(),
                                previous_source_file_path: None,
                                commit_id: suspect,
                                blob_id: id,
                                previous_blob_id: gix_hash::Kind::shortest().null(),
                                parent_index: index,
                            };
                            gix_diff::blob::control::try_push(&mut blame_path, blame_path_entry, check)
                                .map_err(|e| e.raise())?;
                        }

                        break 'outer;
                    }
                }
                TreeDiffChange::Deletion => {
                    unreachable!("We already found file_path in suspect^{{tree}}, so it can't be deleted")
                }
                TreeDiffChange::Modification { previous_id, id } => {
                    let changes = blob_changes(
                        &odb,
                        resource_cache,
                        id,
                        previous_id,
                        file_path,
                        file_path,
                        options.diff_algorithm,
                        &mut stats,
                        &mut prepared,
                        check,
                        raw,
                    )?;
                    *hunks_to_blame = attribute_changes(
                        hunks_to_blame.take(),
                        changes,
                        suspect,
                        *parent_id,
                        index_changes,
                        check,
                    )?;
                    if options.debug_track_path {
                        let mut has_blame_been_passed = false;
                        for hunk in hunks_to_blame.iter() {
                            poll(check, Stage::Attribution, 1)?;
                            if hunk.has_suspect(parent_id) {
                                has_blame_been_passed = true;
                                break;
                            }
                        }

                        if has_blame_been_passed {
                            let blame_path_entry = BlamePathEntry {
                                source_file_path: try_copy_bytes(&current_file_path, check)?.into(),
                                previous_source_file_path: Some(try_copy_bytes(&current_file_path, check)?.into()),
                                commit_id: suspect,
                                blob_id: id,
                                previous_blob_id: previous_id,
                                parent_index: index,
                            };
                            gix_diff::blob::control::try_push(&mut blame_path, blame_path_entry, check)
                                .map_err(|e| e.raise())?;
                        }
                    }
                }
                TreeDiffChange::Rewrite {
                    source_location,
                    source_id,
                    id,
                } => {
                    let changes = blob_changes(
                        &odb,
                        resource_cache,
                        id,
                        source_id,
                        file_path,
                        source_location.as_ref(),
                        options.diff_algorithm,
                        &mut stats,
                        &mut prepared,
                        check,
                        raw,
                    )?;
                    *hunks_to_blame = attribute_changes(
                        hunks_to_blame.take(),
                        changes,
                        suspect,
                        *parent_id,
                        index_changes,
                        check,
                    )?;

                    let mut has_blame_been_passed = false;

                    for hunk in hunks_to_blame.iter_mut() {
                        poll(check, Stage::Attribution, 0)?;
                        if hunk.has_suspect(parent_id) {
                            hunk.source_file_name = Some(try_copy_bytes(&source_location, check)?.into());

                            has_blame_been_passed = true;
                        }
                    }

                    if has_blame_been_passed && options.debug_track_path {
                        let blame_path_entry = BlamePathEntry {
                            source_file_path: try_copy_bytes(&current_file_path, check)?.into(),
                            previous_source_file_path: Some(try_copy_bytes(&source_location, check)?.into()),
                            commit_id: suspect,
                            blob_id: id,
                            previous_blob_id: source_id,
                            parent_index: index,
                        };
                        gix_diff::blob::control::try_push(&mut blame_path, blame_path_entry, check)
                            .map_err(|e| e.raise())?;
                    }
                }
            }
        }

        let mut pending = VecOwner::new(Vec::new(), &shared);
        for mut unblamed_hunk in hunks_to_blame.take_iter() {
            poll(check, Stage::Attribution, pending.len())?;
            super::admit_hunk(&unblamed_hunk, check).map_err(|e| e.raise())?;
            if unblamed_hunk.suspects.len() == 1
                && let Some(entry) =
                    BlameEntry::try_from_unblamed_hunk(&unblamed_hunk, suspect, check).map_err(|e| e.raise())?
            {
                // At this point, we have copied blame for every hunk to a parent. Hunks
                // that have only `suspect` left in `suspects` have not passed blame to any
                // parent, and so they can be converted to a `BlameEntry` and moved to
                // `out`.
                gix_diff::blob::control::try_push(&mut out, entry, check).map_err(|e| e.raise())?;
                continue;
            }
            unblamed_hunk.remove_blame(suspect);
            gix_diff::blob::control::try_push(&mut pending, unblamed_hunk, check).map_err(|e| e.raise())?;
        }
        *hunks_to_blame = pending.take();
    }

    debug_assert_eq!(
        *hunks_to_blame,
        vec![],
        "only if there is no portion of the file left we have completed the blame"
    );

    // I don’t know yet whether it would make sense to use a data structure instead that preserves
    // order on insertion.
    gix_diff::blob::control::try_sort_by(
        &mut out,
        |a, b, _| Ok(a.start_in_blamed_file.cmp(&b.start_in_blamed_file)),
        check,
    )
    .map_err(|e| e.raise())?;
    Ok(Outcome {
        entries: try_coalesce_blame_entries(out.take(), check)?,
        blob: blamed_file_blob,
        statistics: stats,
        blame_path: options.debug_track_path.then(|| blame_path.take()),
    })
}

/// Pass ownership of each unblamed hunk of `from` to `to`.
///
/// This happens when `from` didn't actually change anything in the blamed file.
fn try_pass_blame_from_to(from: ObjectId, to: ObjectId, hunks: &mut [UnblamedHunk], check: &mut Check<'_>) -> Result {
    for (n, hunk) in hunks.iter_mut().enumerate() {
        hunk.pass_blame(from, to);
        poll(check, Stage::Attribution, n + 1)?;
    }
    Ok(())
}

/// Convert each of the unblamed hunk in `hunks_to_blame` into a [`BlameEntry`], consuming them in the process.
///
/// Return `true` if we are done because `hunks_to_blame` is empty.
fn try_unblamed_to_out_is_done(
    hunks: &mut Vec<UnblamedHunk>,
    out: &mut Vec<BlameEntry>,
    suspect: ObjectId,
    check: &mut Check<'_>,
) -> Result<bool> {
    let shared = std::cell::RefCell::new(check);
    let mut forward = |e| (**shared.borrow_mut())(e);
    let check: &mut Check<'_> = &mut forward;
    let mut pending = VecOwner::new(Vec::new(), &shared);
    let mut owned = VecOwner::new(std::mem::take(hunks), &shared);
    for (n, hunk) in owned.take_iter().enumerate() {
        super::admit_hunk(&hunk, check).map_err(|e| e.raise())?;
        if let Some(entry) = BlameEntry::try_from_unblamed_hunk(&hunk, suspect, check).map_err(|e| e.raise())? {
            gix_diff::blob::control::try_push(out, entry, check).map_err(|e| e.raise())?;
        } else {
            gix_diff::blob::control::try_push(&mut pending, hunk, check).map_err(|e| e.raise())?;
        }
        poll(check, Stage::Attribution, n + 1)?;
    }
    *hunks = pending.take();
    Ok(hunks.is_empty())
}

/// This function merges adjacent blame entries. It merges entries that are adjacent both in the
/// blamed file and in the source file that introduced them. This follows `git`’s
/// behaviour. `libgit2`, as of 2024-09-19, only checks whether two entries are adjacent in the
/// blamed file which can result in different blames in certain edge cases. See [the commit][1]
/// that introduced the extra check into `git` for context. See [this commit][2] for a way to test
/// for this behaviour in `git`.
///
/// [1]: https://github.com/git/git/commit/c2ebaa27d63bfb7c50cbbdaba90aee4efdd45d0a
/// [2]: https://github.com/git/git/commit/6dbf0c7bebd1c71c44d786ebac0f2b3f226a0131
fn try_coalesce_blame_entries(lines_blamed: Vec<BlameEntry>, check: &mut Check<'_>) -> Result<Vec<BlameEntry>> {
    let shared = std::cell::RefCell::new(check);
    let mut forward = |e| (**shared.borrow_mut())(e);
    let check: &mut Check<'_> = &mut forward;
    let mut input = VecOwner::new(lines_blamed, &shared);
    let mut acc = VecOwner::<BlameEntry>::new(Vec::new(), &shared);
    for entry in input.take_iter() {
        poll(check, Stage::Output, acc.len())?;
        if let Some(previous) = acc.last_mut()
            && previous.commit_id == entry.commit_id
            && previous.range_in_blamed_file().end == entry.range_in_blamed_file().start
            && previous.range_in_source_file().end == entry.range_in_source_file().start
        {
            previous.len = NonZeroU32::new(previous.len.get() + entry.len.get()).expect("nonempty hunks");
            continue;
        }
        gix_diff::blob::control::try_push(&mut acc, entry, check).map_err(|e| e.raise())?;
    }
    Ok(acc.take())
}

/// The union of [`gix_diff::tree::recorder::Change`] and [`gix_diff::tree_with_rewrites::Change`],
/// keeping only the blame-relevant information.
enum TreeDiffChange {
    Addition {
        id: ObjectId,
    },
    Deletion,
    Modification {
        previous_id: ObjectId,
        id: ObjectId,
    },
    Rewrite {
        source_location: BString,
        source_id: ObjectId,
        id: ObjectId,
    },
}

impl From<gix_diff::tree::recorder::Change> for TreeDiffChange {
    fn from(value: gix_diff::tree::recorder::Change) -> Self {
        use gix_diff::tree::recorder::Change;

        match value {
            Change::Addition { oid, .. } => Self::Addition { id: oid },
            Change::Deletion { .. } => Self::Deletion,
            Change::Modification { previous_oid, oid, .. } => Self::Modification {
                previous_id: previous_oid,
                id: oid,
            },
        }
    }
}

impl From<gix_diff::tree_with_rewrites::Change> for TreeDiffChange {
    fn from(value: gix_diff::tree_with_rewrites::Change) -> Self {
        use gix_diff::tree_with_rewrites::Change;

        match value {
            Change::Addition { id, .. } => Self::Addition { id },
            Change::Deletion { .. } => Self::Deletion,
            Change::Modification { previous_id, id, .. } => Self::Modification { previous_id, id },
            Change::Rewrite {
                source_location,
                source_id,
                id,
                ..
            } => Self::Rewrite {
                source_location,
                source_id,
                id,
            },
        }
    }
}

#[expect(clippy::too_many_arguments)]
fn tree_diff_at_file_path(
    odb: impl gix_object::Find + gix_object::FindHeader,
    file_path: &BStr,
    id: ObjectId,
    parent_id: ObjectId,
    cache: Option<&gix_commitgraph::Graph>,
    stats: &mut Statistics,
    state: &mut gix_diff::tree::State,
    resource_cache: &mut gix_diff::blob::Platform,
    commit_buf: &mut Vec<u8>,
    lhs_tree_buf: &mut Vec<u8>,
    rhs_tree_buf: &mut Vec<u8>,
    rewrites: Option<gix_diff::Rewrites>,
    check: &mut Check<'_>,
    raw: bool,
) -> Result<Option<TreeDiffChange>> {
    let parent_tree_id = find_commit(cache, &odb, &parent_id, commit_buf)
        .or_raise(|| message("Could not find existing iterator over a tree"))?
        .tree_id()
        .or_raise(|| message("Failure to decode commit during traversal"))?;

    let parent_tree_iter = odb
        .find_tree_iter(&parent_tree_id, lhs_tree_buf)
        .or_raise(|| message("Could not find existing iterator over a tree"))?;
    stats.trees_decoded += 1;

    let tree_id = find_commit(cache, &odb, &id, commit_buf)
        .or_raise(|| message("Could not find existing iterator over a tree"))?
        .tree_id()
        .or_raise(|| message("Failure to decode commit during traversal"))?;

    let tree_iter = odb
        .find_tree_iter(&tree_id, rhs_tree_buf)
        .or_raise(|| message("Could not find existing iterator over a tree"))?;
    stats.trees_decoded += 1;

    let result = if raw {
        let change = tree_diff_controlled_at_file_path(
            &odb,
            file_path,
            state,
            resource_cache,
            parent_tree_iter,
            tree_iter,
            None,
            check,
        )?;
        stats.trees_diffed += 1;
        change
    } else {
        tree_diff_without_rewrites_at_file_path(&odb, file_path, stats, state, parent_tree_iter, tree_iter, check)?
    };

    // Here, we follow git’s behaviour. We return when we’ve found a `Modification`. We try a
    // second time with rename tracking when the change is either an `Addition` or a `Deletion`
    // because those can turn out to have been a `Rewrite`.
    // TODO(perf): renames are usually rare enough to not care about the work duplication done here.
    //             But in theory, a rename tracker could be used by us, on demand, and we could stuff the
    //             changes in there and have it find renames, without repeating the diff.
    if matches!(result, Some(TreeDiffChange::Modification { .. })) {
        return Ok(result);
    }
    let Some(rewrites) = rewrites else {
        return Ok(result);
    };

    let result = if raw {
        let change = tree_diff_controlled_at_file_path(
            &odb,
            file_path,
            state,
            resource_cache,
            parent_tree_iter,
            tree_iter,
            Some(rewrites),
            check,
        )?;
        stats.trees_diffed_with_rewrites += 1;
        change
    } else {
        tree_diff_with_rewrites_at_file_path(
            &odb,
            file_path,
            stats,
            state,
            resource_cache,
            parent_tree_iter,
            tree_iter,
            rewrites,
            check,
        )?
    };

    Ok(result)
}

#[expect(clippy::too_many_arguments)]
fn tree_diff_controlled_at_file_path(
    odb: &impl gix_object::FindObjectOrHeader,
    file_path: &BStr,
    state: &mut gix_diff::tree::State,
    resource_cache: &mut gix_diff::blob::Platform,
    lhs: gix_object::TreeRefIter<'_>,
    rhs: gix_object::TreeRefIter<'_>,
    rewrites: Option<gix_diff::Rewrites>,
    check: &mut Check<'_>,
) -> Result<Option<TreeDiffChange>> {
    let shared = std::cell::RefCell::new(check);
    let mut found = None;
    let result = gix_diff::tree_with_rewrites::diff_with_control(
        lhs,
        rhs,
        resource_cache,
        state,
        odb,
        |change| {
            if gix_diff::blob::control::try_cmp_bytes(change.location(), file_path, &mut |e| (**shared.borrow_mut())(e))
                .map_err(|e| e.raise())?
                .is_eq()
            {
                use gix_diff::tree_with_rewrites::ChangeRef;
                found = Some(match change {
                    ChangeRef::Addition { id, .. } => TreeDiffChange::Addition { id },
                    ChangeRef::Deletion { .. } => TreeDiffChange::Deletion,
                    ChangeRef::Modification { previous_id, id, .. } => TreeDiffChange::Modification { previous_id, id },
                    ChangeRef::Rewrite {
                        source_location,
                        source_id,
                        id,
                        ..
                    } => TreeDiffChange::Rewrite {
                        source_location: try_copy_bytes(source_location, &mut |e| (**shared.borrow_mut())(e))?.into(),
                        source_id,
                        id,
                    },
                });
                Ok(std::ops::ControlFlow::Break(()))
            } else {
                Ok(std::ops::ControlFlow::Continue(()))
            }
        },
        gix_diff::tree_with_rewrites::Options {
            location: Some(gix_diff::tree::recorder::Location::Path),
            rewrites,
        },
        &mut |e| (**shared.borrow_mut())(e),
    );
    match result {
        Ok(_) | Err(gix_diff::tree::Error::Cancelled) => Ok(found),
        Err(error) => Err(error.raise()),
    }
}

fn tree_diff_without_rewrites_at_file_path(
    odb: impl gix_object::Find + gix_object::FindHeader,
    file_path: &BStr,
    stats: &mut Statistics,
    state: &mut gix_diff::tree::State,
    parent_tree_iter: gix_object::TreeRefIter<'_>,
    tree_iter: gix_object::TreeRefIter<'_>,
    check: &mut Check<'_>,
) -> Result<Option<TreeDiffChange>> {
    struct FindChangeToPath<'a, 'control> {
        inner: gix_diff::tree::Recorder,
        interesting_path: BString,
        change: Option<gix_diff::tree::recorder::Change>,
        check: &'a mut Check<'control>,
    }

    impl<'a, 'control> FindChangeToPath<'a, 'control> {
        fn new(interesting_path: BString, check: &'a mut Check<'control>) -> Self {
            let inner =
                gix_diff::tree::Recorder::default().track_location(Some(gix_diff::tree::recorder::Location::Path));

            FindChangeToPath {
                inner,
                interesting_path,
                change: None,
                check,
            }
        }
    }

    impl Visit for FindChangeToPath<'_, '_> {
        fn check(&mut self) -> std::result::Result<(), gix_diff::tree::Error> {
            poll(self.check, Stage::Graph, 0).map_err(Into::into)
        }
        fn pop_front_tracked_path_and_set_current(&mut self) {
            self.inner.pop_front_tracked_path_and_set_current();
        }

        fn push_back_tracked_path_component(&mut self, component: &BStr) {
            self.inner.push_back_tracked_path_component(component);
        }

        fn push_path_component(&mut self, component: &BStr) {
            self.inner.push_path_component(component);
        }

        fn pop_path_component(&mut self) {
            self.inner.pop_path_component();
        }

        fn visit(&mut self, change: gix_diff::tree::visit::Change) -> gix_diff::tree::visit::Action {
            use gix_diff::tree::visit::Change::*;

            if self.inner.path() == self.interesting_path {
                self.change = Some(match change {
                    Deletion {
                        entry_mode,
                        oid,
                        relation,
                    } => gix_diff::tree::recorder::Change::Deletion {
                        entry_mode,
                        oid,
                        path: self.inner.path_clone(),
                        relation,
                    },
                    Addition {
                        entry_mode,
                        oid,
                        relation,
                    } => gix_diff::tree::recorder::Change::Addition {
                        entry_mode,
                        oid,
                        path: self.inner.path_clone(),
                        relation,
                    },
                    Modification {
                        previous_entry_mode,
                        previous_oid,
                        entry_mode,
                        oid,
                    } => gix_diff::tree::recorder::Change::Modification {
                        previous_entry_mode,
                        previous_oid,
                        entry_mode,
                        oid,
                        path: self.inner.path_clone(),
                    },
                });

                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }
    }

    let mut recorder = FindChangeToPath::new(file_path.into(), check);
    let result = gix_diff::tree(parent_tree_iter, tree_iter, state, &odb, &mut recorder);
    stats.trees_diffed += 1;

    match result {
        Ok(_) | Err(gix_diff::tree::Error::Cancelled) => Ok(recorder.change.map(Into::into)),
        Err(error) => Err(error.raise()),
    }
}

#[expect(clippy::too_many_arguments)]
fn tree_diff_with_rewrites_at_file_path(
    odb: impl gix_object::Find + gix_object::FindHeader,
    file_path: &BStr,
    stats: &mut Statistics,
    state: &mut gix_diff::tree::State,
    resource_cache: &mut gix_diff::blob::Platform,
    parent_tree_iter: gix_object::TreeRefIter<'_>,
    tree_iter: gix_object::TreeRefIter<'_>,
    rewrites: gix_diff::Rewrites,
    check: &mut Check<'_>,
) -> Result<Option<TreeDiffChange>> {
    let mut change: Option<gix_diff::tree_with_rewrites::Change> = None;

    let options: gix_diff::tree_with_rewrites::Options = gix_diff::tree_with_rewrites::Options {
        location: Some(gix_diff::tree::recorder::Location::Path),
        rewrites: Some(rewrites),
    };
    let result = gix_diff::tree_with_rewrites(
        parent_tree_iter,
        tree_iter,
        resource_cache,
        state,
        &odb,
        |change_ref| {
            poll(check, Stage::Graph, 0)?;
            if change_ref.location() == file_path {
                change = Some(change_ref.into_owned());
                Ok(std::ops::ControlFlow::Break(()))
            } else {
                Ok(std::ops::ControlFlow::Continue(()))
            }
        },
        options,
    );
    stats.trees_diffed_with_rewrites += 1;

    match result {
        Ok(_) | Err(gix_diff::tree::Error::Cancelled) => Ok(change.map(Into::into)),
        Err(error) => Err(error.raise()),
    }
}

#[expect(clippy::too_many_arguments)]
fn blob_changes(
    odb: impl gix_object::Find + gix_object::FindHeader,
    resource_cache: &mut gix_diff::blob::Platform,
    oid: ObjectId,
    previous_oid: ObjectId,
    file_path: &BStr,
    previous_file_path: &BStr,
    diff_algorithm: gix_diff::blob::Algorithm,
    stats: &mut Statistics,
    prepared: &mut Option<PreparedInput>,
    check: &mut Check<'_>,
    raw: bool,
) -> Result<Vec<Change>> {
    poll(check, Stage::Preprocess, 0)?;
    if raw {
        resource_cache.set_resource_controlled_raw(
            previous_oid,
            gix_object::tree::EntryKind::Blob,
            previous_file_path,
            gix_diff::blob::ResourceKind::OldOrSource,
            &odb,
            check,
        )?;
        resource_cache.set_resource_controlled_raw(
            oid,
            gix_object::tree::EntryKind::Blob,
            file_path,
            gix_diff::blob::ResourceKind::NewOrDestination,
            &odb,
            check,
        )?;
    } else {
        resource_cache.set_resource(
            previous_oid,
            // TODO(blame): add a test to show of symlink blaming works.
            gix_object::tree::EntryKind::Blob,
            previous_file_path,
            gix_diff::blob::ResourceKind::OldOrSource,
            &odb,
        )?;

        resource_cache.set_resource(
            oid,
            gix_object::tree::EntryKind::Blob,
            file_path,
            gix_diff::blob::ResourceKind::NewOrDestination,
            &odb,
        )?;
    }

    let outcome = resource_cache.prepare_diff()?;

    let old_data = outcome.old.data.as_slice().unwrap_or_default();
    let new_data = outcome.new.data.as_slice().unwrap_or_default();
    match prepared {
        Some(prepared) if outcome.old.data.as_slice().is_some() && outcome.new.data.as_slice().is_some() => {
            let (input, limits) = prepared
                .try_prepare(previous_oid, old_data, oid, new_data, check)
                .map_err(|err| err.raise())?;
            try_changes_from_input(input, limits, diff_algorithm, stats, check)
        }
        Some(prepared) => {
            prepared.invalidate();
            try_blob_changes_from_data(old_data, new_data, diff_algorithm, stats, check)
        }
        None => try_blob_changes_from_data(old_data, new_data, diff_algorithm, stats, check),
    }
}

fn attribute_changes(
    hunks: Vec<UnblamedHunk>,
    changes: Vec<Change>,
    suspect: ObjectId,
    parent: ObjectId,
    indexed: bool,
    check: &mut Check<'_>,
) -> Result<Vec<UnblamedHunk>> {
    let (hunks, _skipped) = if indexed {
        super::try_process_changes_indexed(hunks, changes, suspect, parent, check).map_err(|err| err.raise())?
    } else {
        (
            super::try_process_changes(hunks, changes, suspect, parent, check).map_err(|err| err.raise())?,
            0,
        )
    };
    Ok(hunks)
}

#[cfg(test)]
fn blob_changes_from_data(
    old_data: &[u8],
    new_data: &[u8],
    diff_algorithm: gix_diff::blob::Algorithm,
    stats: &mut Statistics,
) -> Vec<Change> {
    try_blob_changes_from_data(old_data, new_data, diff_algorithm, stats, &mut |_| Ok(()))
        .expect("uncontrolled check never fails")
}

fn try_blob_changes_from_data(
    old_data: &[u8],
    new_data: &[u8],
    algorithm: gix_diff::blob::Algorithm,
    stats: &mut Statistics,
    check: &mut Check<'_>,
) -> Result<Vec<Change>> {
    gix_diff::blob::byte_input::ByteInput::with(old_data, new_data, check, |input, check| {
        Ok(try_changes_from_input(
            input,
            (input.before.len(), input.after.len()),
            algorithm,
            stats,
            check,
        ))
    })
    .map_err(|err| err.raise())?
}

fn try_changes_from_input<T: AsRef<[u8]>>(
    input: &gix_diff::blob::InternedInput<T>,
    (before_lines, after_lines): (usize, usize),
    diff_algorithm: gix_diff::blob::Algorithm,
    stats: &mut Statistics,
    check: &mut Check<'_>,
) -> Result<Vec<Change>> {
    use gix_diff::blob::{Diff, Hunk};

    let before = &input.before[..before_lines];
    let after = &input.after[..after_lines];
    let diff = Diff::try_compute_with(diff_algorithm, before, after, input.interner.num_tokens(), check)
        .and_then(|diff| diff.try_postprocess_lines_with(before, after, &input.interner, check))
        .map_err(|err| err.raise())?;

    let mut last_seen_after_end = 0;
    let mut changes = Vec::new();
    for hunk in diff.try_hunks(check).map_err(|err| err.raise())? {
        poll(check, Stage::Output, changes.len())?;
        let Hunk { before, after } = hunk;

        // This checks for unchanged hunks.
        if after.start > last_seen_after_end {
            gix_diff::blob::control::try_push(&mut changes, Change::Unchanged(last_seen_after_end..after.start), check)
                .map_err(|e| e.raise())?;
        }

        match (!before.is_empty(), !after.is_empty()) {
            (_, true) => {
                gix_diff::blob::control::try_push(
                    &mut changes,
                    Change::AddedOrReplaced(after.start..after.end, before.end - before.start),
                    check,
                )
                .map_err(|e| e.raise())?;
            }
            (true, false) => {
                gix_diff::blob::control::try_push(
                    &mut changes,
                    Change::Deleted(after.start, before.end - before.start),
                    check,
                )
                .map_err(|e| e.raise())?;
            }
            (false, false) => unreachable!("BUG: imara-diff provided a non-change"),
        }

        last_seen_after_end = after.end;
    }

    // The native-compatible diff view excludes only a shared complete tail.
    // Attribution and subsequent token reuse must still cover the whole file.
    let total_number_of_lines = input.after.len() as u32;
    if input.after.len() > last_seen_after_end as usize {
        gix_diff::blob::control::try_push(
            &mut changes,
            Change::Unchanged(last_seen_after_end..total_number_of_lines),
            check,
        )
        .map_err(|e| e.raise())?;
    }

    stats.blobs_diffed += 1;
    Ok(changes)
}

#[expect(clippy::too_many_arguments)]
fn find_path_entry_in_commit(
    odb: &impl gix_object::Find,
    commit: &gix_hash::oid,
    file_path: &BStr,
    cache: Option<&gix_commitgraph::Graph>,
    buf: &mut Vec<u8>,
    buf2: &mut Vec<u8>,
    stats: &mut Statistics,
    check: &mut Check<'_>,
) -> Result<Option<ObjectId>> {
    let mut tree_id = find_commit(cache, odb, commit, buf)
        .or_raise(|| message("Could not find existing iterator over a tree"))?
        .tree_id()
        .or_raise(|| message("Failure to decode commit during traversal"))?;
    let mut parts = file_path.split(|b| *b == b'/').peekable();
    while let Some(component) = parts.next() {
        poll(check, Stage::Graph, 0)?;
        let tree = odb.find_tree_iter(&tree_id, buf2)?;
        stats.trees_decoded += 1;
        let mut found = None;
        for entry in tree {
            let entry = entry?;
            poll(check, Stage::Graph, 1)?;
            if gix_diff::blob::control::try_cmp_bytes(entry.filename, component, check)
                .map_err(|e| e.raise())?
                .is_eq()
            {
                found = Some((entry.oid.to_owned(), entry.mode));
                break;
            }
        }
        let Some((id, mode)) = found else {
            return Ok(None);
        };
        if parts.peek().is_none() {
            return Ok(Some(id));
        }
        if !mode.is_tree() {
            return Ok(None);
        }
        tree_id = id;
    }
    Ok(None)
}

type ParentIds = SmallVec<[(gix_hash::ObjectId, i64); 2]>;

fn collect_parents(
    commit: gix_traverse::commit::Either<'_, '_>,
    odb: &impl gix_object::Find,
    cache: Option<&gix_commitgraph::Graph>,
    buf: &mut Vec<u8>,
    check: &mut Check<'_>,
    raw: bool,
) -> Result<ParentIds> {
    let mut parent_ids: ParentIds = Default::default();
    // Commit headers are bounded before parsing; cached commitgraphs obey the
    // same explicit parent-count policy. This also bounds SmallVec relocation.
    match commit {
        gix_traverse::commit::Either::CachedCommit(commit) => {
            let cache = cache
                .as_ref()
                .expect("find returned a cached commit, so we expect cache to be present");
            for parent_pos in commit.iter_parents() {
                if parent_ids.len() == 128 {
                    return Err(gix_diff::blob::control::Cancelled.raise());
                }
                gix_diff::blob::control::admit_bytes(128 * std::mem::size_of::<(ObjectId, i64)>(), check)
                    .map_err(|e| e.raise())?;
                parent_ids
                    .try_reserve_exact(1)
                    .map_err(|_| message("parent allocation failed").raise())?;
                poll(check, Stage::Graph, parent_ids.len())?;
                let parent = cache.commit_at(
                    parent_pos.or_raise(|| message("Failed to get parent from commitgraph during traversal"))?,
                );
                parent_ids.push((parent.id().to_owned(), parent.committer_timestamp() as i64));
            }
        }
        gix_traverse::commit::Either::CommitRefIter(commit_ref_iter) => {
            for id in commit_ref_iter.parent_ids() {
                if parent_ids.len() == 128 {
                    return Err(gix_diff::blob::control::Cancelled.raise());
                }
                gix_diff::blob::control::admit_bytes(128 * std::mem::size_of::<(ObjectId, i64)>(), check)
                    .map_err(|e| e.raise())?;
                parent_ids
                    .try_reserve_exact(1)
                    .map_err(|_| message("parent allocation failed").raise())?;
                poll(check, Stage::Graph, parent_ids.len())?;
                let parent = odb.find_commit_iter(id.as_ref(), buf);
                let parent_commit_time = if raw {
                    parent?.committer()?.seconds()
                } else {
                    parent
                        .ok()
                        .and_then(|parent| parent.committer().ok().map(|committer| committer.seconds()))
                        .unwrap_or_default()
                };
                parent_ids.push((id, parent_commit_time));
            }
        }
    }
    Ok(parent_ids)
}

/// Return an iterator over tokens for use in diffing. These are usually lines, but it's important
/// to unify them so the later access shows the right thing.
pub(crate) fn tokens_for_diffing(data: &[u8]) -> impl TokenSource<Token = &[u8]> {
    gix_diff::blob::sources::byte_lines(data)
}

/// The blame input after resolving [`Start`] and before traversing commit history.
struct InitialState {
    /// The final file contents whose lines are being attributed.
    blamed_file_blob: Vec<u8>,
    /// Requested ranges that still need attribution through commit traversal.
    hunks_to_blame: Vec<UnblamedHunk>,
    /// Entries already resolved while comparing worktree contents to the first suspect.
    out: Vec<BlameEntry>,
    /// The first commit to traverse, or `None` if no history traversal is needed.
    first_suspect: Option<ObjectId>,
}

fn initial_hunks(options: &Options, lines: u32, suspect: ObjectId, check: &mut Check<'_>) -> Result<Vec<UnblamedHunk>> {
    let shared = std::cell::RefCell::new(check);
    let mut forward = |e| (**shared.borrow_mut())(e);
    let check: &mut Check<'_> = &mut forward;
    let mut out = VecOwner::new(Vec::new(), &shared);
    let whole = 0..lines;
    let ranges = match &options.ranges {
        crate::BlameRanges::WholeFile => std::slice::from_ref(&whole),
        crate::BlameRanges::PartialFile(ranges) => ranges,
    };
    for range in ranges {
        poll(check, Stage::Attribution, out.len())?;
        let range = if range.end < lines {
            range.clone()
        } else if range.start < lines {
            range.start..lines
        } else {
            continue;
        };
        gix_diff::blob::control::try_push(&mut out, UnblamedHunk::new(range, suspect), check).map_err(|e| e.raise())?;
    }
    Ok(out.take())
}

#[expect(clippy::too_many_arguments)]
fn initial_state(
    odb: &impl gix_object::Find,
    start: Start<'_>,
    cache: Option<&gix_commitgraph::Graph>,
    file_path: &BStr,
    options: &Options,
    buf: &mut Vec<u8>,
    buf2: &mut Vec<u8>,
    stats: &mut Statistics,
    check: &mut Check<'_>,
) -> Result<InitialState> {
    let shared = std::cell::RefCell::new(check);
    let mut forward = |e| (**shared.borrow_mut())(e);
    let check: &mut Check<'_> = &mut forward;
    match start {
        Start::Commit(suspect) => {
            let blamed_file_entry_id = find_path_entry_in_commit(
                &odb, &suspect, file_path, cache, buf, buf2, stats, check,
            )?
            .ok_or_raise(|| {
                message!("The file to blame at \"{file_path}\" wasn't found in the first commit at {suspect}")
                    .not_found()
            })?;
            let blob = odb
                .find_blob(&blamed_file_entry_id, buf)
                .or_raise(|| message("Could not find existing blob or commit"))?
                .data;
            let blamed_file_blob = try_copy_bytes(blob, check)?;
            let num_lines_in_blamed = gix_diff::blob::control::try_line_ranges(&blamed_file_blob, check)
                .map_err(|err| err.raise())?
                .len() as u32;

            // Binary or otherwise empty?
            if num_lines_in_blamed == 0 {
                return Ok(InitialState {
                    blamed_file_blob,
                    hunks_to_blame: Vec::new(),
                    out: Vec::new(),
                    first_suspect: None,
                });
            }

            let hunks_to_blame = initial_hunks(options, num_lines_in_blamed, suspect, check)?;

            Ok(InitialState {
                blamed_file_blob,
                hunks_to_blame,
                out: Vec::new(),
                first_suspect: Some(suspect),
            })
        }
        Start::Contents {
            first_suspect,
            contents,
        } => {
            let null_id = first_suspect.kind().null();
            let blamed_file_blob = match contents {
                std::borrow::Cow::Owned(data) => {
                    gix_diff::blob::control::admit_bytes(data.capacity(), check).map_err(|e| e.raise())?;
                    data
                }
                std::borrow::Cow::Borrowed(data) => try_copy_bytes(data, check)?,
            };
            let num_lines_in_blamed = gix_diff::blob::control::try_line_ranges(&blamed_file_blob, check)
                .map_err(|err| err.raise())?
                .len() as u32;

            if num_lines_in_blamed == 0 {
                return Ok(InitialState {
                    blamed_file_blob,
                    hunks_to_blame: Vec::new(),
                    out: Vec::new(),
                    first_suspect: None,
                });
            }

            let mut hunks_to_blame =
                VecOwner::new(initial_hunks(options, num_lines_in_blamed, null_id, check)?, &shared);

            let mut out = VecOwner::new(Vec::new(), &shared);

            let Some(first_suspect_entry_id) =
                find_path_entry_in_commit(odb, &first_suspect, file_path, cache, buf, buf2, stats, check)?
            else {
                // File does not exist in the starting commit. Treat the requested
                // ranges as entirely "not committed yet".
                for hunk in hunks_to_blame.iter() {
                    poll(check, Stage::Attribution, out.len())?;
                    if let Some(entry) =
                        BlameEntry::try_from_unblamed_hunk(hunk, null_id, check).map_err(|e| e.raise())?
                    {
                        gix_diff::blob::control::try_push(&mut out, entry, check).map_err(|e| e.raise())?;
                    }
                }

                return Ok(InitialState {
                    blamed_file_blob,
                    hunks_to_blame: Vec::new(),
                    out: out.take(),
                    first_suspect: None,
                });
            };

            let first_suspect_blob = odb
                .find_blob(&first_suspect_entry_id, buf)
                .or_raise(|| message("Could not find existing blob or commit"))?
                .data;

            let changes = try_blob_changes_from_data(
                first_suspect_blob,
                &blamed_file_blob,
                options.diff_algorithm,
                stats,
                check,
            )?;
            *hunks_to_blame = super::try_process_changes(hunks_to_blame.take(), changes, null_id, first_suspect, check)
                .map_err(|err| err.raise())?;

            try_unblamed_to_out_is_done(&mut hunks_to_blame, &mut out, null_id, check)?;

            let mut remains = false;
            for hunk in hunks_to_blame.iter() {
                poll(check, Stage::Attribution, 1)?;
                if hunk.has_suspect(&first_suspect) {
                    remains = true;
                    break;
                }
            }
            let first_suspect = remains.then_some(first_suspect);

            Ok(InitialState {
                blamed_file_blob,
                hunks_to_blame: hunks_to_blame.take(),
                out: out.take(),
                first_suspect,
            })
        }
    }
}

#[cfg(test)]
mod indexed_change_tests {
    use super::*;

    #[test]
    fn indexed_attribution_matches_every_short_diff_and_source_range() {
        let suspect = ObjectId::from_bytes_or_panic(&[0; 20]);
        let parent = ObjectId::from_bytes_or_panic(&[1; 20]);
        let unrelated = ObjectId::from_bytes_or_panic(&[2; 20]);
        let inputs: Vec<Vec<u8>> = (0..=4)
            .flat_map(|len| {
                (0..1 << len).map(move |bits| {
                    (0..len)
                        .flat_map(|bit| if bits & (1 << bit) == 0 { *b"a\n" } else { *b"b\n" })
                        .collect()
                })
            })
            .collect();
        for before in &inputs {
            for after in &inputs {
                for algorithm in [
                    gix_diff::blob::Algorithm::Myers,
                    gix_diff::blob::Algorithm::MyersMinimal,
                    gix_diff::blob::Algorithm::Histogram,
                ] {
                    let changes = blob_changes_from_data(before, after, algorithm, &mut Statistics::default());
                    for start in 0..after.len() as u32 / 2 {
                        for end in start + 1..=after.len() as u32 / 2 {
                            let make_hunks = || {
                                vec![UnblamedHunk {
                                    range_in_blamed_file: 0..end - start,
                                    suspects: [(suspect, start..end), (unrelated, 7..7 + end - start)]
                                        .into_iter()
                                        .collect(),
                                    source_file_name: Some("previous.txt".into()),
                                }]
                            };
                            let expected = process_changes(make_hunks(), changes.clone(), suspect, parent);
                            let (actual, _) = process_changes_indexed(make_hunks(), changes.clone(), suspect, parent);
                            assert_eq!(
                                actual, expected,
                                "complete ownership/source positions differ for {before:?} -> {after:?}, {start}..{end}, {algorithm:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}
