use std::cmp::Ordering;

pub(crate) const LARGE_OFFSET_THRESHOLD: u64 = 0x7fff_ffff;
pub(crate) const HIGH_BIT: u32 = 0x8000_0000;

pub(crate) fn fanout(iter: &mut dyn ExactSizeIterator<Item = u8>) -> [u32; 256] {
    let mut fan_out = [0u32; 256];
    let entries_len = iter.len() as u32;
    let mut iter = iter.enumerate();
    let mut idx_and_entry = iter.next();
    let mut upper_bound = 0;

    for (offset_be, byte) in fan_out.iter_mut().zip(0u8..=255) {
        *offset_be = match idx_and_entry.as_ref() {
            Some((_idx, first_byte)) => match first_byte.cmp(&byte) {
                Ordering::Less => unreachable!("ids should be ordered, and we make sure to keep ahead with them"),
                Ordering::Greater => upper_bound,
                Ordering::Equal => {
                    if byte == 255 {
                        entries_len
                    } else {
                        idx_and_entry = iter.find(|(_, first_byte)| *first_byte != byte);
                        upper_bound = idx_and_entry.as_ref().map_or(entries_len, |(idx, _)| *idx as u32);
                        upper_bound
                    }
                }
            },
            None => entries_len,
        };
    }

    fan_out
}

#[cfg(feature = "streaming-input")]
mod function {
    use std::io;

    use gix_error::Result;

    use gix_utils::progress::{self, DynNestedProgress};

    use super::{HIGH_BIT, LARGE_OFFSET_THRESHOLD, fanout};
    use crate::index::V2_SIGNATURE;

    struct Count<W> {
        bytes: u64,
        inner: W,
    }

    impl<W> Count<W> {
        fn new(inner: W) -> Self {
            Count { bytes: 0, inner }
        }
    }

    impl<W> io::Write for Count<W>
    where
        W: io::Write,
    {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let written = self.inner.write(buf)?;
            self.bytes += written as u64;
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    pub(crate) fn write_to(
        out: &mut dyn io::Write,
        entries_sorted_by_oid: Vec<crate::cache::delta::tree::Item<crate::index::write::TreeEntry>>,
        pack_hash: &gix_hash::ObjectId,
        kind: crate::index::Version,
        object_hash: gix_hash::Kind,
        progress: &mut dyn DynNestedProgress,
    ) -> Result<gix_hash::ObjectId> {
        write_impl(
            out,
            &entries_sorted_by_oid,
            pack_hash,
            kind,
            object_hash,
            progress,
            None,
        )
    }

    #[cfg(feature = "generate")]
    pub(crate) fn write_to_controlled(
        out: &mut dyn io::Write,
        entries: &[(gix_hash::ObjectId, u32, u64)],
        pack_hash: &gix_hash::ObjectId,
        object_hash: gix_hash::Kind,
        check: &dyn Fn() -> Result,
    ) -> Result<gix_hash::ObjectId> {
        write_impl(
            out,
            entries,
            pack_hash,
            crate::index::Version::V2,
            object_hash,
            &mut progress::Discard,
            Some(check),
        )
    }

    trait IndexEntry {
        fn id(&self) -> &gix_hash::ObjectId;
        fn crc32(&self) -> u32;
        fn offset(&self) -> u64;
    }

    impl IndexEntry for crate::cache::delta::tree::Item<crate::index::write::TreeEntry> {
        fn id(&self) -> &gix_hash::ObjectId {
            &self.data.id
        }
        fn crc32(&self) -> u32 {
            self.data.crc32
        }
        fn offset(&self) -> u64 {
            self.offset
        }
    }

    impl IndexEntry for (gix_hash::ObjectId, u32, u64) {
        fn id(&self) -> &gix_hash::ObjectId {
            &self.0
        }
        fn crc32(&self) -> u32 {
            self.1
        }
        fn offset(&self) -> u64 {
            self.2
        }
    }

    fn write_impl(
        out: &mut dyn io::Write,
        entries_sorted_by_oid: &[impl IndexEntry],
        pack_hash: &gix_hash::ObjectId,
        kind: crate::index::Version,
        object_hash: gix_hash::Kind,
        progress: &mut dyn DynNestedProgress,
        control: Option<&dyn Fn() -> Result>,
    ) -> Result<gix_hash::ObjectId> {
        use io::Write;
        let check = control.unwrap_or(&|| Ok(()));
        check()?;
        assert_eq!(kind, crate::index::Version::V2, "Can only write V2 packs right now");
        assert!(
            entries_sorted_by_oid.len() <= u32::MAX as usize,
            "a pack cannot have more than u32::MAX objects"
        );

        // Write header
        let mut out = Count::new(std::io::BufWriter::with_capacity(
            8 * 4096,
            gix_hash::io::Write::new(gix_zlib::stream::ControlledWrite::new(out, check), object_hash),
        ));
        out.write_all(V2_SIGNATURE).map_err(gix_hash::io::from_std_io)?;
        out.write_all(&(kind as u32).to_be_bytes())
            .map_err(gix_hash::io::from_std_io)?;

        progress.init(Some(4), progress::steps());
        let start = std::time::Instant::now();
        let _info = progress.add_child_with_id("writing fan-out table".into(), gix_utils::progress::UNKNOWN);
        let fan_out = if control.is_some() {
            let mut counts = [0u32; 256];
            for entry in entries_sorted_by_oid {
                check()?;
                counts[usize::from(entry.id().first_byte())] += 1;
            }
            for byte in 1..256 {
                counts[byte] += counts[byte - 1];
            }
            counts
        } else {
            fanout(&mut entries_sorted_by_oid.iter().map(|e| e.id().first_byte()))
        };

        for value in fan_out.iter() {
            out.write_all(&value.to_be_bytes()).map_err(gix_hash::io::from_std_io)?;
        }

        progress.inc();
        let _info = progress.add_child_with_id("writing ids".into(), gix_utils::progress::UNKNOWN);
        for entry in entries_sorted_by_oid {
            check()?;
            out.write_all(entry.id().as_slice())
                .map_err(gix_hash::io::from_std_io)?;
        }

        progress.inc();
        let _info = progress.add_child_with_id("writing crc32".into(), gix_utils::progress::UNKNOWN);
        for entry in entries_sorted_by_oid {
            check()?;
            out.write_all(&entry.crc32().to_be_bytes())
                .map_err(gix_hash::io::from_std_io)?;
        }

        progress.inc();
        let _info = progress.add_child_with_id("writing offsets".into(), gix_utils::progress::UNKNOWN);
        {
            let mut num_offsets64 = 0u32;
            for entry in entries_sorted_by_oid {
                check()?;
                let offset: u32 = if entry.offset() > LARGE_OFFSET_THRESHOLD {
                    if u64::from(num_offsets64) >= LARGE_OFFSET_THRESHOLD {
                        return Err(gix_error::allocation_limit("Too many large pack offsets").into());
                    }
                    let encoded = num_offsets64 | HIGH_BIT;
                    num_offsets64 += 1;
                    encoded
                } else {
                    entry.offset() as u32
                };
                out.write_all(&offset.to_be_bytes())
                    .map_err(gix_hash::io::from_std_io)?;
            }
            for entry in entries_sorted_by_oid {
                check()?;
                if entry.offset() > LARGE_OFFSET_THRESHOLD {
                    out.write_all(&entry.offset().to_be_bytes())
                        .map_err(gix_hash::io::from_std_io)?;
                }
            }
        }

        out.write_all(pack_hash.as_slice()).map_err(gix_hash::io::from_std_io)?;

        let bytes_written_without_trailer = out.bytes;
        let mut out = out
            .inner
            .into_inner()
            .map_err(io::Error::from)
            .map_err(gix_hash::io::from_std_io)?;
        let index_hash = out.hash.try_finalize()?;
        out.inner
            .write_all(index_hash.as_slice())
            .map_err(gix_hash::io::from_std_io)?;
        out.inner.flush().map_err(gix_hash::io::from_std_io)?;
        check()?;

        progress.inc();
        progress.show_throughput_with(
            start,
            (bytes_written_without_trailer + object_hash.len_in_bytes() as u64) as usize,
            progress::bytes().expect("unit always set"),
            progress::MessageLevel::Success,
        );

        Ok(index_hash)
    }
}
#[cfg(feature = "streaming-input")]
pub(crate) use function::write_to;
#[cfg(all(feature = "streaming-input", feature = "generate"))]
pub(crate) use function::write_to_controlled;
