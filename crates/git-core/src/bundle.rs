//! Git bundle envelope and complete, hash-verified snapshot pack export.
//! This format is distinct from gix-pack's pack/index Bundle abstraction.

use crate::{
    CapacityBuffer, MAX_REF_BYTES, MAX_REFS, SnapshotSession, VerifiedObjectProvider,
    objects::{Result, invalid},
    storage::ChargedVec,
    write_pack::PackWriteLimits,
};
use gix_hash::ObjectId;
use std::io::{Read, Write};

/// Exact generated envelope and pack identity, retained for independent readback.
pub struct BundleManifest {
    header: CapacityBuffer,
    pub pack_hash: ObjectId,
    pub pack_bytes: u64,
    pub object_count: u32,
    control: crate::Control,
}

impl BundleManifest {
    /// Verify the complete written envelope, header, pack checksum and exact EOF.
    /// Object/typed-closure validation precedes export; this detects storage or
    /// writer corruption without loading the complete bundle into memory.
    pub fn verify(&self, input: &mut impl Read) -> Result<()> {
        let mut bytes = [0u8; 32 * 1024];
        for expected in self.header.as_slice().chunks(bytes.len()) {
            self.control.check()?;
            input.read_exact(&mut bytes[..expected.len()])?;
            if &bytes[..expected.len()] != expected {
                return Err(invalid("bundle envelope readback mismatch"));
            }
        }
        let mut header = [0u8; 12];
        input.read_exact(&mut header)?;
        let (version, count) = gix_pack::data::header::decode(&header)?;
        if version != gix_pack::data::Version::V2 || count != self.object_count {
            return Err(invalid("bundle pack header readback mismatch"));
        }
        let width = self.pack_hash.kind().len_in_bytes();
        let mut remaining = self
            .pack_bytes
            .checked_sub(12 + width as u64)
            .ok_or_else(|| invalid("bundle pack length is invalid"))?;
        let mut hash = gix_hash::hasher(self.pack_hash.kind());
        hash.update(&header);
        while remaining != 0 {
            self.control.check()?;
            let count = remaining.min(bytes.len() as u64) as usize;
            input.read_exact(&mut bytes[..count])?;
            hash.update(&bytes[..count]);
            remaining -= count as u64;
        }
        let mut trailer = gix_hash::Kind::buf();
        input.read_exact(&mut trailer[..width])?;
        if &trailer[..width] != self.pack_hash.as_slice() || hash.try_finalize()? != self.pack_hash
        {
            return Err(invalid("bundle pack checksum readback mismatch"));
        }
        if input.read(&mut bytes[..1])? != 0 {
            return Err(invalid("trailing bundle readback bytes"));
        }
        self.control.check()?;
        Ok(())
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Export a standalone v2 SHA-1 bundle with all authorized refs and HEAD.
    /// There are no prerequisites: the selected typed object closure is complete.
    pub fn export_bundle<'a>(
        &self,
        refs: impl IntoIterator<Item = (&'a [u8], ObjectId)>,
        default_ref: &[u8],
        output: &mut impl Write,
    ) -> Result<BundleManifest> {
        self.control.check()?;
        if self.identity().object_hash != gix_hash::Kind::Sha1 {
            return Err(invalid("v2 bundle requires SHA-1"));
        }
        let mut header = self.capacity.buffer(0)?;
        // Every supported ref may have a maximum-length name. Include its SHA-1
        // prefix/newline plus the bundle signature, optional HEAD and separator.
        let header_limit = MAX_REFS * (40 + 1 + MAX_REF_BYTES + 1) + 128;
        header.extend_controlled(b"# v2 git bundle\n", header_limit, &self.control)?;
        let mut roots = ChargedVec::new(self.capacity.clone(), MAX_REFS)?;
        let mut head = None;
        for (name, id) in refs {
            self.control.check()?;
            if !name.starts_with(b"refs/") || name.len() > MAX_REF_BYTES {
                return Err(invalid("invalid bundle ref name"));
            }
            gix_validate::reference::name(name.into())?;
            if id.kind() != self.identity().object_hash || id.is_null() {
                return Err(invalid("invalid bundle ref ID"));
            }
            roots.push(id, &self.control)?;
            let mut prefix = [0u8; 42];
            let mut writer = std::io::Cursor::new(prefix.as_mut_slice());
            write!(writer, "{id} ")?;
            let len = writer.position() as usize;
            header.extend_controlled(&prefix[..len], header_limit, &self.control)?;
            header.extend_controlled(name, header_limit, &self.control)?;
            header.extend_controlled(b"\n", header_limit, &self.control)?;
            if name == default_ref {
                head = Some(id);
            }
        }
        if roots.as_slice().is_empty() {
            return Err(invalid("refusing to create an empty bundle"));
        }
        if let Some(id) = head {
            let mut bytes = [0u8; 64];
            let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
            writeln!(writer, "{id} HEAD")?;
            let len = writer.position() as usize;
            header.extend_controlled(&bytes[..len], header_limit, &self.control)?;
        }
        header.extend_controlled(b"\n", header_limit, &self.control)?;
        let selection = self.select_objects(roots.as_slice().iter().copied(), None, &[])?;
        for chunk in header.as_slice().chunks(32 * 1024) {
            self.control.check()?;
            output.write_all(chunk)?;
        }
        let pack = self.write_pack(&selection, output, PackWriteLimits::default())?;
        Ok(BundleManifest {
            header,
            pack_hash: pack.pack_hash,
            pack_bytes: pack.pack_bytes,
            object_count: u32::try_from(pack.object_count())?,
            control: self.control.clone(),
        })
    }
}
