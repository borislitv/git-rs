use gix_diff::blob::InternedInput;
use gix_diff::blob::control::{BYTE_CHUNK, Cancelled, Check, Checkpoint, Stage, try_line_ranges};
#[cfg(test)]
use gix_diff::blob::sources::byte_lines;
use gix_hash::ObjectId;

fn poll(check: &mut Check<'_>, stage: Stage, processed: usize) -> Result<(), Cancelled> {
    check(Checkpoint { stage, processed })
}

fn equal(left: &[u8], right: &[u8], check: &mut Check<'_>) -> Result<bool, Cancelled> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (n, (left, right)) in left.chunks(BYTE_CHUNK).zip(right.chunks(BYTE_CHUNK)).enumerate() {
        let same = left == right;
        poll(check, Stage::Equal, n * BYTE_CHUNK + left.len())?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}

fn extend<T: Copy>(out: &mut Vec<T>, src: &[T], check: &mut Check<'_>) -> Result<(), Cancelled> {
    gix_diff::blob::control::try_reserve(out, src.len(), check)?;
    for (n, chunk) in src.chunks(1024).enumerate() {
        out.extend_from_slice(chunk);
        poll(check, Stage::Copy, n * 1024 + chunk.len())?;
    }
    Ok(())
}

const HISTORY_TOKEN_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const LINE_WEIGHT_OVERHEAD_BYTES: usize = 64;

type PreparedView<'a> = (&'a InternedInput<Vec<u8>>, (usize, usize));

/// Query-local preparation retaining complete byte-identical lines for reuse.
/// The diff view separately matches Git's zero-context common-tail trimming.
#[derive(Default)]
pub(super) struct PreparedInput {
    parent_id: Option<ObjectId>,
    input: InternedInput<Vec<u8>>,
    offsets: Vec<usize>,
    spare_offsets: Vec<usize>,
    retained_bytes: usize,
    reuse_runs: bool,
    reused_run_lines: usize,
    current_input_credit: bool,
    current_pair_weight: usize,
    resets: usize,
    credited_pairs: usize,
}

impl PreparedInput {
    pub(super) fn clear_with_control(&mut self, check: &mut Check<'_>) {
        self.input.before.clear();
        self.input.after.clear();
        self.input.interner.clear_with_control(check);
        self.parent_id = None;
    }
    pub(super) fn with_current_input_credit(mut self) -> Self {
        self.current_input_credit = true;
        self
    }

    pub(super) fn with_unchanged_runs(mut self) -> Self {
        self.reuse_runs = true;
        self
    }

    #[cfg(test)]
    pub(super) fn reused_run_lines(&self) -> usize {
        self.reused_run_lines
    }

    #[cfg(test)]
    pub(super) fn resets(&self) -> usize {
        self.resets
    }

    #[cfg(test)]
    pub(super) fn credited_pairs(&self) -> usize {
        self.credited_pairs
    }

    pub(super) fn invalidate(&mut self) {
        self.parent_id = None;
    }

    #[cfg(test)]
    pub(super) fn prepare(
        &mut self,
        parent_id: ObjectId,
        parent: &[u8],
        suspect_id: ObjectId,
        suspect: &[u8],
    ) -> (&InternedInput<Vec<u8>>, (usize, usize)) {
        self.try_prepare(parent_id, parent, suspect_id, suspect, &mut |_| Ok(()))
            .expect("uncontrolled check never fails")
    }

    pub(super) fn try_prepare(
        &mut self,
        parent_id: ObjectId,
        parent: &[u8],
        suspect_id: ObjectId,
        suspect: &[u8],
        check: &mut Check<'_>,
    ) -> Result<PreparedView<'_>, Cancelled> {
        self.reused_run_lines = 0;
        // Bound history retention, allowing the current pair's actual input on
        // top of this budget. A merge, rename or traversal jump may miss reuse.
        let current_weight = if self.current_input_credit {
            self.current_pair_weight
        } else {
            0
        };
        if self.parent_id == Some(suspect_id)
            && self.retained_bytes > HISTORY_TOKEN_BUDGET_BYTES
            && self.retained_bytes.saturating_sub(current_weight) <= HISTORY_TOKEN_BUDGET_BYTES
        {
            self.credited_pairs += 1;
        }
        if self.parent_id != Some(suspect_id)
            || self.retained_bytes.saturating_sub(current_weight) > HISTORY_TOKEN_BUDGET_BYTES
        {
            self.resets += 1;
            self.clear_with_control(check);
            self.retained_bytes = 0;
            self.offsets.clear();
            gix_diff::blob::control::try_push(&mut self.offsets, 0, check)?;
            let mut position = 0;
            for range in try_line_ranges(suspect, check)? {
                let line = &suspect[range];
                let token = self.try_intern(line, check)?;
                gix_diff::blob::control::try_push(&mut self.input.after, token, check)?;
                position += line.len();
                gix_diff::blob::control::try_push(&mut self.offsets, position, check)?;
            }
        } else {
            // The last parent is the current suspect in a linear traversal.
            std::mem::swap(&mut self.input.before, &mut self.input.after);
        }

        let common = try_common_prefix(parent, suspect, check)?;
        let mut prefix_lines = self.offsets.partition_point(|&end| end <= common) - 1;
        // An unterminated final line is not shared if the other side appends
        // bytes to it, even when the entire shorter file is a byte prefix.
        if prefix_lines == self.input.after.len()
            && prefix_lines != 0
            && common < parent.len()
            && suspect.last() != Some(&b'\n')
        {
            prefix_lines -= 1;
        }
        let prefix_bytes = self.offsets[prefix_lines];
        let common_tail = try_common_suffix(parent, suspect, check)?;
        let suffix_bytes = common_tail
            .min(parent.len() - prefix_bytes)
            .min(suspect.len() - prefix_bytes);
        let suffix_begin = suspect.len() - suffix_bytes;
        let mut suffix_line = self
            .offsets
            .partition_point(|&start| start < suffix_begin)
            .min(self.input.after.len());
        let mut parent_suffix = parent.len() - (suspect.len() - self.offsets[suffix_line]);
        // The matching suffix can begin in the middle of the parent's line.
        // After skipping that first line, every remaining boundary is shared.
        if suffix_line < self.input.after.len() && parent_suffix != 0 && parent[parent_suffix - 1] != b'\n' {
            suffix_line += 1;
            parent_suffix = parent.len() - (suspect.len() - self.offsets[suffix_line]);
        }

        self.input.before.clear();
        extend(&mut self.input.before, &self.input.after[..prefix_lines], check)?;
        self.spare_offsets.clear();
        extend(&mut self.spare_offsets, &self.offsets[..=prefix_lines], check)?;
        let mut position = prefix_bytes;
        let mut nearby = prefix_lines;
        let mut lines = try_line_ranges(&parent[prefix_bytes..parent_suffix], check)?.into_iter();
        while let Some(range) = lines.next() {
            let line = &parent[prefix_bytes + range.start..prefix_bytes + range.end];
            // Small separated edits need not re-intern the unchanged lines
            // between them. This is only a lookup hint: reuse requires exact
            // byte equality, and the complete diff still chooses attribution.
            let end = (nearby + 8).min(suffix_line);
            let mut found = None;
            for (offset, &token) in self.input.after[nearby..end].iter().enumerate() {
                if equal(self.input.interner[token].as_slice(), line, check)? {
                    found = Some(offset);
                    break;
                }
            }
            let token = if let Some(offset) = found {
                let first = nearby + offset;
                if self.reuse_runs {
                    // Confirm a whole byte-identical run in the immutable
                    // source, then reuse only complete upstream line tokens.
                    let source_start = self.offsets[first];
                    let matched = try_common_prefix(
                        &parent[position..parent_suffix],
                        &suspect[source_start..self.offsets[suffix_line]],
                        check,
                    )?;
                    let mut end = self.offsets.partition_point(|&end| end <= source_start + matched) - 1;
                    if end == self.input.after.len()
                        && position + matched < parent_suffix
                        && suspect.last() != Some(&b'\n')
                    {
                        end -= 1;
                    }
                    extend(&mut self.input.before, &self.input.after[first..end], check)?;
                    for &end in &self.offsets[first + 1..=end] {
                        gix_diff::blob::control::try_push(
                            &mut self.spare_offsets,
                            position + (end - source_start),
                            check,
                        )?;
                        poll(check, Stage::Copy, self.spare_offsets.len())?;
                    }
                    position += self.offsets[end] - source_start;
                    self.reused_run_lines += end - first;
                    nearby = end;
                    // Existing ranges were relative to the original prefix;
                    // skip the verified run without rescanning or retokenizing it.
                    while lines
                        .as_slice()
                        .first()
                        .is_some_and(|range| prefix_bytes + range.end <= position)
                    {
                        lines.next();
                        poll(check, Stage::Common, position)?;
                    }
                    continue;
                }
                nearby = first + 1;
                self.input.after[nearby - 1]
            } else {
                self.try_intern(line, check)?
            };
            gix_diff::blob::control::try_push(&mut self.input.before, token, check)?;
            position += line.len();
            gix_diff::blob::control::try_push(&mut self.spare_offsets, position, check)?;
        }
        extend(&mut self.input.before, &self.input.after[suffix_line..], check)?;
        let suspect_suffix = self.offsets[suffix_line];
        for &end in &self.offsets[suffix_line + 1..] {
            gix_diff::blob::control::try_push(&mut self.spare_offsets, parent_suffix + (end - suspect_suffix), check)?;
            poll(check, Stage::Copy, self.spare_offsets.len())?;
        }
        std::mem::swap(&mut self.offsets, &mut self.spare_offsets);
        self.parent_id = Some(parent_id);
        // The current pair is already required for diffing. Its conservative
        // line-data/entry weight must not force history eviction on every edge
        // when a single input exceeds the historical-retention budget. Recheck
        // the remaining history at the next edge, before interning its parent.
        self.current_pair_weight = parent.len().saturating_add(suspect.len()).saturating_add(
            self.input
                .before
                .len()
                .saturating_add(self.input.after.len())
                .saturating_mul(LINE_WEIGHT_OVERHEAD_BYTES),
        );

        // Git's xdi_diff() removes equal 1024-byte tail blocks, then recovers
        // through the first LF. Using a longest common suffix instead changes
        // occurrence counts and slider EOF context, and can change blame.
        let matched = common_tail / 1024 * 1024;
        let tail = &parent[parent.len() - matched..];
        let mut recovered = matched;
        for (n, chunk) in tail.chunks(BYTE_CHUNK).enumerate() {
            let found = chunk.iter().position(|&b| b == b'\n');
            poll(check, Stage::Common, n * BYTE_CHUNK + chunk.len())?;
            if let Some(offset) = found {
                recovered = n * BYTE_CHUNK + offset + 1;
                break;
            }
        }
        let trimmed = matched - recovered;
        let parent_lines = self.offsets.partition_point(|&end| end <= parent.len() - trimmed) - 1;
        let suspect_lines = self
            .spare_offsets
            .partition_point(|&end| end <= suspect.len() - trimmed)
            - 1;
        Ok((&self.input, (parent_lines, suspect_lines)))
    }

    fn try_intern(&mut self, line: &[u8], check: &mut Check<'_>) -> Result<gix_diff::blob::Token, Cancelled> {
        let previous_count = self.input.interner.num_tokens();
        let token = self.input.interner.try_intern_bytes(line, check)?;
        if token.0 == previous_count {
            // Account for owned line data, its Vec, allocator and table entry.
            self.retained_bytes = self
                .retained_bytes
                .saturating_add(line.len().saturating_add(LINE_WEIGHT_OVERHEAD_BYTES));
        }
        Ok(token)
    }
}

fn try_common_prefix(left: &[u8], right: &[u8], check: &mut Check<'_>) -> Result<usize, Cancelled> {
    let mut count = 0;
    for (left, right) in left.chunks(BYTE_CHUNK).zip(right.chunks(BYTE_CHUNK)) {
        let matched = left.iter().zip(right).take_while(|(a, b)| a == b).count();
        count += matched;
        poll(check, Stage::Common, count)?;
        if matched != left.len().min(right.len()) {
            break;
        }
    }
    Ok(count)
}

fn try_common_suffix(left: &[u8], right: &[u8], check: &mut Check<'_>) -> Result<usize, Cancelled> {
    let mut count = 0;
    for (left, right) in left.rchunks(BYTE_CHUNK).zip(right.rchunks(BYTE_CHUNK)) {
        let matched = left
            .iter()
            .rev()
            .zip(right.iter().rev())
            .take_while(|(a, b)| a == b)
            .count();
        count += matched;
        poll(check, Stage::Common, count)?;
        if matched != left.len().min(right.len()) {
            break;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gix_diff::blob::{Algorithm, Diff};

    fn check(prepared: &mut PreparedInput, parent: &[u8], suspect: &[u8]) -> gix_error::TestResult {
        let parent_id = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, parent)?;
        let suspect_id = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, suspect)?;
        let (input, _) = prepared.prepare(parent_id, parent, suspect_id, suspect);
        let expected = InternedInput::new(parent, suspect);
        assert_eq!(
            input
                .before
                .iter()
                .map(|&token| input.interner[token].as_slice())
                .collect::<Vec<_>>(),
            byte_lines(parent).collect::<Vec<_>>(),
            "parent token boundaries must equal the upstream tokenizer"
        );
        assert_eq!(
            input
                .after
                .iter()
                .map(|&token| input.interner[token].as_slice())
                .collect::<Vec<_>>(),
            byte_lines(suspect).collect::<Vec<_>>(),
            "suspect token boundaries must equal the upstream tokenizer"
        );
        for algorithm in [Algorithm::Myers, Algorithm::MyersMinimal, Algorithm::Histogram] {
            let mut actual = Diff::compute(algorithm, input);
            actual.postprocess_lines(input);
            let mut reference = Diff::compute(algorithm, &expected);
            reference.postprocess_lines(&expected);
            assert_eq!(
                actual.hunks().collect::<Vec<_>>(),
                reference.hunks().collect::<Vec<_>>(),
                "full-input {algorithm:?} diff and sliders must be unchanged"
            );
        }
        let mut expected_offsets = vec![0];
        for line in byte_lines(parent) {
            expected_offsets.push(expected_offsets.last().copied().expect("the zero offset is present") + line.len());
        }
        assert_eq!(
            prepared.offsets, expected_offsets,
            "reused offsets retain complete parent line boundaries"
        );
        Ok(())
    }

    #[test]
    fn all_short_line_endings_and_repeated_lines_match_full_preparation() -> gix_error::TestResult {
        let mut texts = vec![Vec::new()];
        for length in 1..=4 {
            for mut value in 0..3usize.pow(length) {
                let mut text = Vec::new();
                for _ in 0..length {
                    text.push(b"a\n\r"[value % 3]);
                    value /= 3;
                }
                texts.push(text);
            }
        }
        for suspect in &texts {
            for parent in &texts {
                for mut prepared in [
                    PreparedInput::default(),
                    PreparedInput::default().with_unchanged_runs(),
                    PreparedInput::default().with_current_input_credit(),
                    PreparedInput::default()
                        .with_unchanged_runs()
                        .with_current_input_credit(),
                ] {
                    check(&mut prepared, parent, suspect)?;
                    // Traverse the same edge backwards to exercise identity-bound
                    // token reuse, not just the first pair's initialization.
                    check(&mut prepared, suspect, parent)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn long_lines_repeated_context_jumps_and_budget_reset_match_full_preparation() -> gix_error::TestResult {
        for mut prepared in [
            PreparedInput::default(),
            PreparedInput::default().with_unchanged_runs(),
            PreparedInput::default().with_current_input_credit(),
            PreparedInput::default()
                .with_unchanged_runs()
                .with_current_input_credit(),
        ] {
            for size in [255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049] {
                let mut before = vec![b'x'; size];
                before.extend_from_slice(b"\r\na\na\n\xff\nlast");
                let mut after = before.clone();
                after.splice(size / 2..size / 2 + 1, b"\nnew\n".iter().copied());
                check(&mut prepared, &before, &after)?;
                check(&mut prepared, &after, &before)?;
                prepared.retained_bytes = HISTORY_TOKEN_BUDGET_BYTES + prepared.current_pair_weight + 1;
                check(&mut prepared, &before, &after)?;
                check(&mut prepared, b"unrelated\n", b"branch\n")?;
            }
        }
        Ok(())
    }

    #[test]
    fn native_tail_view_keeps_complete_inputs_for_the_next_edge() -> gix_error::TestResult {
        let mut parent = b"a\nb\n".to_vec();
        parent.extend(std::iter::repeat_n(b'z', 1019));
        parent.push(b'\n');
        let mut suspect = b"a\nb\nx\n".to_vec();
        suspect.extend_from_slice(&parent);
        let parent_id = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, &parent)?;
        let suspect_id = gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, &suspect)?;
        let mut prepared = PreparedInput::default();
        let (input, limits) = prepared.prepare(parent_id, &parent, suspect_id, &suspect);
        assert_eq!(limits, (1, 4), "Git recovers the first LF from the matching 1 KiB tail");
        assert_eq!(
            (input.before.len(), input.after.len()),
            (3, 6),
            "reuse retains the full inputs"
        );
        check(&mut prepared, &suspect, &parent)?;
        Ok(())
    }

    #[test]
    fn separated_edits_reuse_complete_runs_without_extending_an_unterminated_line() -> gix_error::TestResult {
        let mut context = Vec::new();
        for index in 0..1024 {
            context.extend_from_slice(format!("same-{index}\r\nrepeat\n").as_bytes());
            context.extend_from_slice(b"\xff\n");
        }
        for terminated in [false, true] {
            let mut suspect = b"new header\n".to_vec();
            suspect.extend_from_slice(&context);
            suspect.extend_from_slice(b"last");
            let mut parent = b"old header\n".to_vec();
            parent.extend_from_slice(&context);
            parent.extend_from_slice(b"last-extended");
            if terminated {
                suspect.push(b'\n');
                parent.push(b'\n');
            }

            let mut prepared = PreparedInput::default().with_unchanged_runs();
            check(&mut prepared, &parent, &suspect)?;
            assert_eq!(
                prepared.reused_run_lines(),
                3072,
                "the separated edit reuses every complete context line, but not the changed final line"
            );
            check(&mut prepared, &suspect, &parent)?;
            assert_eq!(
                prepared.reused_run_lines(),
                3072,
                "reverse traversal reuses the same complete run"
            );
        }
        Ok(())
    }

    #[test]
    fn a_current_file_larger_than_the_history_budget_retains_its_line_tokens() -> gix_error::TestResult {
        let mut shared = vec![b'x'; 64 * 1024 * 1024];
        shared.extend_from_slice(b"\nend\n");
        let mut parent = b"new header\n".to_vec();
        parent.extend_from_slice(&shared);
        let mut older = b"old header\n".to_vec();
        older.extend_from_slice(&shared);
        let id = |bytes: &[u8]| gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, bytes);
        let mut prepared = PreparedInput::default().with_current_input_credit();
        let shared_id = id(&shared)?;
        let parent_id = id(&parent)?;
        let older_id = id(&older)?;
        let (input, _) = prepared.prepare(parent_id, &parent, shared_id, &shared);
        let shared_token = input.before[1];
        let (input, _) = prepared.prepare(older_id, &older, parent_id, &parent);
        assert_eq!(
            input.after[1], shared_token,
            "the immutable current input must not consume the historical retention budget"
        );
        assert_eq!(input.before[1], shared_token);
        assert_eq!((input.before.len(), input.after.len()), (3, 3));
        assert_eq!(input.interner[shared_token].as_slice(), &shared[..shared.len() - 4]);
        assert_eq!(
            prepared.resets(),
            1,
            "the current pair is reused rather than repeatedly cleared"
        );
        assert_eq!(
            prepared.credited_pairs(),
            1,
            "current input credit prevents the second reset"
        );
        let small = b"only\n";
        let root = b"root\n";
        let first = b"first\n";
        let small_id = id(small)?;
        let root_id = id(root)?;
        prepared.prepare(small_id, small, older_id, &older);
        prepared.prepare(root_id, root, small_id, small);
        let (input, _) = prepared.prepare(id(first)?, first, root_id, root);
        assert_eq!(
            input.interner.num_tokens(),
            2,
            "old large-file tokens are evicted after the current pair becomes small"
        );
        assert_eq!(
            prepared.resets(),
            2,
            "input credit cannot retain a past large file indefinitely"
        );
        Ok(())
    }
}
