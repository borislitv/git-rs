//! Controlled streaming pack output composed from maintained header, compressor,
//! checksum and index encoders. Physical encodings retain their authenticated
//! source proof; a delta is reused only when its exact base is in the output.

use std::io::Write;

use gix_hash::ObjectId;

use crate::{
    CapacityBudget, Control, SnapshotSession, VerifiedObjectProvider,
    objects::{Result, invalid},
    selection::ObjectSelection,
    storage::{ChargedVec, sort_controlled},
};

#[derive(Clone, Copy, Debug)]
pub struct PackWriteLimits {
    pub max_output_bytes: u64,
}

impl Default for PackWriteLimits {
    fn default() -> Self {
        Self {
            max_output_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

pub struct PackManifest {
    pub pack_hash: ObjectId,
    pub pack_bytes: u64,
    entries: ChargedVec<(ObjectId, u32, u64)>,
    budget: CapacityBudget,
    control: Control,
}

impl PackManifest {
    pub fn object_count(&self) -> usize {
        self.entries.as_slice().len()
    }

    /// The maintained index encoder consumes this exact output's IDs, CRCs and
    /// offsets. Sorting retains both metadata allocations under the same budget.
    pub fn write_index(&mut self, output: &mut impl Write) -> Result<ObjectId> {
        sort_controlled(
            self.entries.as_mut_slice(),
            &self.budget,
            &self.control,
            |a, b| a.0.cmp(&b.0),
        )?;
        Ok(gix_pack::index::write::write_sorted_entries_controlled(
            output,
            self.entries.as_slice(),
            &self.pack_hash,
            self.pack_hash.kind(),
            &|| check(&self.control),
        )?)
    }
}

struct Counted<'a, W> {
    output: &'a mut W,
    bytes: u64,
    limit: u64,
    crc: Option<crc32fast::Hasher>,
    control: &'a Control,
}

impl<W: Write> Write for Counted<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.control.check().map_err(std::io::Error::other)?;
        let bytes = &bytes[..bytes.len().min(32 * 1024)];
        if bytes.len() as u64 > self.limit.saturating_sub(self.bytes) {
            return Err(std::io::Error::other("pack output byte limit exceeded"));
        }
        let written = self.output.write(bytes)?;
        self.bytes += written as u64;
        if let Some(crc) = &mut self.crc {
            crc.update(&bytes[..written]);
        }
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.control.check().map_err(std::io::Error::other)?;
        self.output.flush()
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    pub fn write_pack(
        &self,
        objects: &ObjectSelection,
        output: &mut impl Write,
        limits: PackWriteLimits,
    ) -> Result<PackManifest> {
        self.control.check()?;
        if objects.identity() != self.identity() || limits.max_output_bytes < 32 {
            return Err(invalid(
                "pack selection identity or output limit is invalid",
            ));
        }
        let count = u32::try_from(objects.len())?;
        let mut entries = ChargedVec::new(self.capacity.clone(), self.limits.max_graph_objects)?;
        let counted = Counted {
            output,
            bytes: 0,
            limit: limits.max_output_bytes,
            crc: None,
            control: &self.control,
        };
        let mut output = gix_hash::io::Write::new(counted, self.identity().object_hash);
        output.write_all(&gix_pack::data::header::encode(
            gix_pack::data::Version::V2,
            count,
        ))?;
        // Admit the selected native workspace before constructing its compressor.
        // It is an audited backend profile, not an arbitrary-allocator RSS cap.
        let _workspace = self
            .capacity
            .reservation(gix_zlib::Workspace::ZLIB_RS_0_6_8.deflate)?;
        let mut compressor =
            gix_zlib::stream::deflate::Compress::new(gix_zlib::Compression::default());
        for id in objects.iter() {
            self.control.check()?;
            let start = output.inner.bytes;
            output.inner.crc = Some(crc32fast::Hasher::new());
            let packed = self.provider.packed_object(&id, &self.control)?;
            let packed =
                packed.filter(|packed| packed.base.is_none_or(|base| objects.contains(&base)));
            if let Some(packed) = packed {
                if packed.id != id {
                    return Err(invalid("packed object witness identity mismatch"));
                }
                let header = if let Some(base_id) = packed.base {
                    // REF bases may occur later in the output. This is a complete
                    // pack, not a thin pack referring to client-local objects.
                    gix_pack::data::entry::Header::RefDelta { base_id }
                } else {
                    packed.entry.header
                };
                header.write_to(packed.entry.decompressed_size, &mut output)?;
                let prefix = packed.entry.header_size();
                for chunk in packed.bytes.as_slice()[prefix..].chunks(32 * 1024) {
                    self.control.check()?;
                    output.write_all(chunk)?;
                }
            } else {
                let object = self
                    .verified_object(&id)?
                    .ok_or_else(|| invalid("missing object during pack generation"))?;
                let header = match object.header().kind {
                    gix_object::Kind::Commit => gix_pack::data::entry::Header::Commit,
                    gix_object::Kind::Tree => gix_pack::data::entry::Header::Tree,
                    gix_object::Kind::Blob => gix_pack::data::entry::Header::Blob,
                    gix_object::Kind::Tag => gix_pack::data::entry::Header::Tag,
                };
                header.write_to(object.as_slice().len() as u64, &mut output)?;
                compressor.write_all_controlled(object.as_slice(), &mut output, &|| {
                    check(&self.control)
                })?;
            }
            let crc = output
                .inner
                .crc
                .take()
                .expect("each entry starts its own CRC")
                .finalize();
            entries.push((id, crc, start), &self.control)?;
        }
        self.control.check()?;
        let pack_hash = output.hash.try_finalize()?;
        output.inner.write_all(pack_hash.as_slice())?;
        output.inner.flush()?;
        let pack_bytes = output.inner.bytes;
        self.control.check()?;
        Ok(PackManifest {
            pack_hash,
            pack_bytes,
            entries,
            budget: self.capacity.clone(),
            control: self.control.clone(),
        })
    }
}

fn check(control: &Control) -> gix_error::Result {
    control
        .check()
        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
}
