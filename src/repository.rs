//! Library-only operations on an application-owned request repository.

use crate::{Result, invalid, process, protocol::MAX_REF, scratch::Scratch};
use gix::bstr::ByteSlice;
use gix::odb::{find::Header, pack};
use std::fs;

const MAX_BASE_RECURSION: usize = 32;

pub(crate) fn ref_name(name: &[u8]) -> Result<gix::refs::FullName> {
    if name.len() > MAX_REF || !name.starts_with(b"refs/") {
        return Err(invalid("invalid ref namespace"));
    }
    Ok(gix::refs::FullName::try_from(name.as_bstr())?)
}

/// Physical object headers from one request's verified, immutable snapshot.
///
/// gix-odb's directory discovery ignores symlinked indexes. Open the published
/// pack/index pairs explicitly through gix-pack so sealed RAM links remain
/// zero-copy. Borrowing the workspace keeps its cache pins alive until these
/// read-only library mappings have been released.
pub(crate) struct ObjectHeaders<'a> {
    packs: Vec<pack::Bundle>,
    loose: gix::odb::loose::Store,
    _workspace: &'a Scratch,
}

impl<'a> ObjectHeaders<'a> {
    pub(crate) fn open(workspace: &'a Scratch) -> Result<Self> {
        let objects = workspace.path.join("objects");
        let mut packs = Vec::new();
        for entry in fs::read_dir(objects.join("pack"))? {
            process::check_operation()?;
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "idx") {
                packs.push(pack::Bundle::at(path, gix::hash::Kind::Sha1)?);
            }
        }
        Ok(Self {
            packs,
            loose: gix::odb::loose::Store::at(objects, gix::hash::Kind::Sha1),
            _workspace: workspace,
        })
    }

    fn try_header_at(
        &self,
        id: &gix::hash::oid,
        depth: usize,
    ) -> gix::error::Result<Option<Header>> {
        process::check_operation().map_err(gix::error::Error::from_boxed)?;
        if depth >= MAX_BASE_RECURSION {
            return Err(gix::error::corruption("pack delta base recursion limit exceeded").into());
        }
        for bundle in &self.packs {
            process::check_operation().map_err(gix::error::Error::from_boxed)?;
            let Some(index) = bundle.index.lookup(id) else {
                continue;
            };
            let entry = bundle
                .pack
                .entry(bundle.index.pack_offset_at_index(index))?;
            let header =
                bundle
                    .pack
                    .decode_header(entry, &mut gix::zlib::Inflate::default(), &|base| {
                        use pack::data::decode::header::ResolvedBase;
                        if let Some(index) = bundle.index.lookup(base) {
                            return bundle
                                .pack
                                .entry(bundle.index.pack_offset_at_index(index))
                                .map(ResolvedBase::InPack)
                                .map(Some);
                        }
                        self.try_header_at(base, depth + 1).map(|header| {
                            header.map(|header| ResolvedBase::OutOfPack {
                                kind: header.kind(),
                                num_deltas: header.num_deltas(),
                            })
                        })
                    })?;
            process::check_operation().map_err(gix::error::Error::from_boxed)?;
            return Ok(Some(header.into()));
        }
        self.loose
            .try_header(id)
            .map(|header| header.map(Into::into))
    }
}

impl gix::odb::Header for ObjectHeaders<'_> {
    fn try_header(&self, id: &gix::hash::oid) -> gix::error::Result<Option<Header>> {
        self.try_header_at(id, 0)
    }
}
