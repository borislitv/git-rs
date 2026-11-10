use gix_error::Result;
use std::io::Write;

use gix_error::{ResultExt, message};

use crate::{data::output, exact_vec};

/// An implementation of [`Iterator`] to write [encoded entries][output::Entry] to an inner implementation each time
/// `next()` is called.
pub struct FromEntriesIter<I, W> {
    /// An iterator for input [`output::Entry`] instances
    pub input: I,
    /// A way of writing encoded bytes.
    output: gix_hash::io::Write<W>,
    /// Our trailing hash when done writing all input entries
    trailer: Option<gix_hash::ObjectId>,
    /// The amount of objects in the iteration and the version of the packfile to be written.
    /// Will be `None` to signal the header was written already.
    header_info: Option<(crate::data::Version, u32)>,
    /// The pack data version with which pack entries should be written.
    entry_version: crate::data::Version,
    /// The amount of written bytes thus far
    written: u64,
    /// Required to quickly find offsets by object IDs, as future objects may refer to those in the past to become a delta offset base.
    /// It stores the pack offsets at which objects begin.
    /// Additionally we store if an object was invalid, and if so we will not write it nor will we allow delta objects to it.
    pack_offsets_and_validity: Vec<(u64, bool)>,
    /// If we are done, no additional writes will occur
    is_done: bool,
    expected_entries: Option<usize>,
}

impl<I, W> FromEntriesIter<I, W>
where
    I: Iterator<Item = Result<Vec<output::Entry>>>,
    W: std::io::Write,
{
    /// Construct with an explicit entry-count limit and fallible metadata allocation.
    pub fn new_controlled(
        input: I,
        output: W,
        num_entries: u32,
        version: crate::data::Version,
        object_hash: gix_hash::Kind,
        max_entries: usize,
        check: &dyn Fn() -> Result,
    ) -> Result<Self> {
        check()?;
        let count = usize::try_from(num_entries).map_err(|_| {
            gix_error::Error::from(gix_error::allocation_limit("Pack entry count exceeds address space"))
        })?;
        if count > max_entries {
            return Err(gix_error::allocation_limit("Pack entry count exceeds output limit").into());
        }
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(count)
            .or_raise(|| gix_error::allocation_failure("Could not reserve pack output metadata"))?;
        check()?;
        Ok(Self {
            input,
            output: gix_hash::io::Write::new(output, object_hash),
            trailer: None,
            entry_version: version,
            pack_offsets_and_validity: offsets,
            written: 0,
            header_info: Some((version, num_entries)),
            is_done: false,
            expected_entries: Some(count),
        })
    }

    /// Create a new instance reading [entries][output::Entry] from an `input` iterator and write pack data bytes to
    /// `output` writer, resembling a pack of `version` with exactly `num_entries` amount of objects contained in it.
    /// `object_hash` is the kind of hash to use for the pack checksum and maybe other places, depending on the version.
    ///
    /// The input chunks are expected to be sorted already. You can use the [`InOrderIter`][gix_parallel::InOrderIter] to assure
    /// this happens on the fly holding entire chunks in memory as long as needed for them to be dispensed in order.
    pub fn new(
        input: I,
        output: W,
        num_entries: u32,
        version: crate::data::Version,
        object_hash: gix_hash::Kind,
    ) -> Self {
        FromEntriesIter {
            input,
            output: gix_hash::io::Write::new(output, object_hash),
            trailer: None,
            entry_version: version,
            pack_offsets_and_validity: exact_vec(num_entries as usize),
            written: 0,
            header_info: Some((version, num_entries)),
            is_done: false,
            expected_entries: None,
        }
    }

    /// Consume this instance and return the `output` implementation.
    ///
    /// _Note_ that the `input` iterator can be moved out of this instance beforehand.
    pub fn into_write(self) -> W {
        self.output.inner
    }

    /// Returns the trailing hash over all written entries once done.
    /// It's `None` if we are not yet done writing.
    pub fn digest(&self) -> Option<gix_hash::ObjectId> {
        self.trailer
    }

    /// Advance with checks inside output copying and checksum updates. The input
    /// iterator must itself be controlled, and the sink must bound blocking I/O.
    /// On error this iterator is terminal and no digest is published.
    pub fn next_controlled(&mut self, check: &dyn Fn() -> Result) -> Option<Result<u64>> {
        if self.is_done {
            return None;
        }
        Some(match self.next_inner(check) {
            Err(err) => {
                self.is_done = true;
                Err(err)
            }
            Ok(written) => Ok(written),
        })
    }

    fn next_inner(&mut self, check: &dyn Fn() -> Result) -> Result<u64> {
        check()?;
        let previous_written = self.written;
        if let Some((version, num_entries)) = self.header_info.take() {
            let header_bytes = crate::data::header::encode(version, num_entries);
            gix_zlib::stream::ControlledWrite::new(&mut self.output, check)
                .write_all(&header_bytes[..])
                .map_err(gix_hash::io::from_std_io)?;
            self.written += header_bytes.len() as u64;
        }
        match self.input.next() {
            Some(entries) => {
                for entry in entries.or_raise(|| message("Pack entry input iterator failed"))? {
                    check()?;
                    if self
                        .expected_entries
                        .is_some_and(|count| self.pack_offsets_and_validity.len() >= count)
                    {
                        return Err(gix_error::validation("Pack input exceeds declared entry count").into());
                    }
                    if entry.is_invalid() {
                        if self.expected_entries.is_some() {
                            return Err(
                                gix_error::not_found("Controlled pack output cannot include a missing object").into(),
                            );
                        }
                        self.pack_offsets_and_validity.push((0, false));
                        continue;
                    }
                    if self.expected_entries.is_some()
                        && let output::entry::Kind::DeltaRef { object_index } = entry.kind
                        && (object_index >= self.pack_offsets_and_validity.len()
                            || !self.pack_offsets_and_validity[object_index].1)
                    {
                        return Err(
                            gix_error::corruption("Pack delta refers to a missing or forward output base").into(),
                        );
                    }
                    self.pack_offsets_and_validity.push((self.written, true));
                    let header = entry.to_entry_header(self.entry_version, |index| {
                        let (base_offset, is_valid_object) = self.pack_offsets_and_validity[index];
                        if !is_valid_object {
                            unreachable!("if you see this the object database is correct as a delta refers to a non-existing object")
                        }
                        self.written - base_offset
                    });
                    self.written += header
                        .write_to(
                            entry.decompressed_size as u64,
                            &mut gix_zlib::stream::ControlledWrite::new(&mut self.output, check),
                        )
                        .map_err(gix_hash::io::from_std_io)? as u64;
                    for bytes in entry.compressed_data.chunks(32 * 1024) {
                        check()?;
                        gix_zlib::stream::ControlledWrite::new(&mut self.output, check)
                            .write_all(bytes)
                            .map_err(gix_hash::io::from_std_io)?;
                        self.written += bytes.len() as u64;
                    }
                    check()?;
                }
            }
            None => {
                check()?;
                if self
                    .expected_entries
                    .is_some_and(|count| self.pack_offsets_and_validity.len() != count)
                {
                    return Err(gix_error::validation("Pack input is shorter than declared entry count").into());
                }
                let digest = self.output.hash.clone().try_finalize()?;
                gix_zlib::stream::ControlledWrite::new(&mut self.output.inner, check)
                    .write_all(digest.as_slice())
                    .map_err(gix_hash::io::from_std_io)?;
                self.written += digest.as_slice().len() as u64;
                gix_zlib::stream::ControlledWrite::new(&mut self.output.inner, check)
                    .flush()
                    .map_err(gix_hash::io::from_std_io)?;
                check()?;
                self.is_done = true;
                self.trailer = Some(digest);
            }
        }
        Ok(self.written - previous_written)
    }
}

impl<I, W> Iterator for FromEntriesIter<I, W>
where
    I: Iterator<Item = Result<Vec<output::Entry>>>,
    W: std::io::Write,
{
    /// The amount of bytes written to `out` if `Ok` or the error `E` received from the input.
    type Item = Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done {
            return None;
        }
        self.next_controlled(&|| Ok(()))
    }
}
