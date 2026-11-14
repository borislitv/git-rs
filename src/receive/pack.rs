use super::vendor_check;
use git_rs_core::receive::CommandBatch;
use std::io::BufRead;

type Resolve<'a> =
    dyn Fn(&gix::hash::oid, &mut [u8]) -> gix::error::Result<Option<(gix::objs::Kind, usize)>> + 'a;

/// Private protocol authority, not a request-controlled EOF flag. HTTP callers
/// construct it only after framed body and (if present) gzip finalization; SSH
/// uses the validated command/single-pack boundary on its owned bounded reader.
#[derive(Clone, Copy)]
pub(super) enum Boundary {
    Http,
    Ssh,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare<'budget>(
    input: &mut impl BufRead,
    commands: &CommandBatch,
    boundary: Boundary,
    limits: ReadLimits,
    budget: &'budget dyn pack::memory::Budget,
    resolve: &Resolve<'_>,
    check: &dyn Fn() -> gix::error::Result,
) -> Result<Option<pack::bundle::write::AdmittedOutcome<'budget>>> {
    check()?;
    match boundary {
        Boundary::Ssh if commands.delete_only() => {
            // The command flush IS the delete-only SSH boundary. In particular,
            // neither fill_buf nor an EOF probe is permitted here. Bytes that
            // have not arrived cannot be disproved without deadlocking a client
            // waiting for status. This is not HTTP's empty-body policy.
            Ok(None)
        }
        Boundary::Ssh => Ok(Some(pack::Bundle::prepare_single_pack_admitted(
            input,
            gix::hash::Kind::Sha1,
            options(limits),
            budget,
            gix::zlib::Workspace::ZLIB_RS_0_6_8,
            resolve,
            check,
        )?)),
        Boundary::Http => {
            if input.fill_buf()?.is_empty() {
                if commands.delete_only() {
                    return Ok(None);
                }
                return Err(invalid("non-delete receive requires a pack"));
            }
            let outcome = pack::Bundle::prepare_stream_admitted(
                input,
                gix::hash::Kind::Sha1,
                options(limits),
                budget,
                gix::zlib::Workspace::ZLIB_RS_0_6_8,
                resolve,
                check,
            )?;
            if commands.delete_only() && outcome.outcome.num_objects != 0 {
                return Err(invalid("unexpected objects in delete-only receive"));
            }
            Ok(Some(outcome))
        }
    }
}
use crate::{Result, invalid};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotSession,
    VerifiedObjectProvider, receive::IncomingObjects,
};
use gix::odb::pack;

pub(super) fn options(limits: ReadLimits) -> pack::bundle::write::ControlledOptions {
    pack::bundle::write::ControlledOptions {
        input_pack_id: 0,
        max_input_bytes: crate::http::MAX_BODY as usize,
        max_output_bytes: crate::http::MAX_BODY as usize,
        max_index_bytes: 64 * 1024 * 1024,
        max_objects: 100_000,
        max_decode_bytes: limits.max_decode_bytes,
        max_resolve_attempts: 1_000_000,
        decode: pack::data::decode::Limits {
            max_delta_depth: limits.max_delta_depth,
            max_delta_bytes: limits.max_decode_bytes as u64,
        },
        compression: gix::zlib::Compression::default(),
    }
}

/// Only admitted normalization constructs this view. Every normalized entry is a
/// full object; lookup and physical enumeration both use these exact bytes.
pub(super) struct Normalized<'a> {
    data: pack::data::File<&'a [u8]>,
    index: pack::index::File<&'a [u8]>,
    bytes: &'a [u8],
}

impl<'a> Normalized<'a> {
    pub(super) fn open(outcome: &'a pack::bundle::write::AdmittedOutcome<'_>) -> Result<Self> {
        let bytes: &[u8] = &outcome.pack;
        Ok(Self {
            data: pack::data::File::from_data(bytes, Default::default(), gix::hash::Kind::Sha1)?,
            index: pack::index::File::from_data(
                &outcome.index[..],
                Default::default(),
                gix::hash::Kind::Sha1,
            )?,
            bytes,
        })
    }

    fn at(
        &self,
        offset: u64,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<(Object, u64)> {
        control.check()?;
        let entry = self.data.entry(offset)?;
        let kind = entry
            .header
            .as_kind()
            .ok_or_else(|| invalid("normalized receive contains a delta"))?;
        let size = usize::try_from(entry.decompressed_size)?;
        if size > limits.max_object_bytes || size > limits.max_decode_bytes {
            return Err(invalid("incoming receive object exceeds read limit"));
        }
        let mut data = budget.buffer(size)?;
        for end in (0..size).step_by(32 * 1024) {
            control.check()?;
            data.resize(size.min(end + 32 * 1024), 0)?;
        }
        let _workspace = budget.reservation(gix::zlib::Workspace::ZLIB_RS_0_6_8.entry_inflate)?;
        let mut inflate = gix::zlib::Inflate::default();
        let start = usize::try_from(entry.data_offset)?;
        let end = self.bytes.len() - 20;
        let (_, consumed, _) =
            inflate.once_controlled(&self.bytes[start..end], data.as_mut_slice(), &|| {
                vendor_check(control)
            })?;
        Ok((Object { kind, data }, entry.data_offset + consumed as u64))
    }

    fn object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.index
            .lookup(id)
            .map(|position| {
                self.at(
                    self.index.pack_offset_at_index(position),
                    control,
                    budget,
                    limits,
                )
                .map(|(object, _)| object)
            })
            .transpose()
    }
}

pub(super) struct Entries<'a, 'b> {
    normalized: Option<&'a Normalized<'b>>,
    next: u64,
    remaining: usize,
}
impl<'a, 'b> Entries<'a, 'b> {
    pub(super) fn new(normalized: Option<&'a Normalized<'b>>) -> Self {
        Self {
            normalized,
            next: 12,
            remaining: normalized.map_or(0, |pack| pack.data.num_objects() as usize),
        }
    }
}
impl IncomingObjects for Entries<'_, '_> {
    fn object_count(&self) -> usize {
        self.normalized
            .map_or(0, |pack| pack.data.num_objects() as usize)
    }
    fn next_object(
        &mut self,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<(gix::ObjectId, Object)>> {
        let Some(pack) = self.normalized else {
            return Ok(None);
        };
        if self.remaining == 0 {
            if self.next != (pack.bytes.len() - 20) as u64 {
                return Err(invalid("normalized physical receive enumeration mismatch"));
            }
            return Ok(None);
        }
        let (object, next) = pack.at(self.next, control, budget, limits)?;
        let mut hash = gix::hash::hasher(gix::hash::Kind::Sha1);
        hash.update(&gix::objs::encode::loose_header(
            object.kind,
            object.data.as_slice().len() as u64,
        ));
        for chunk in object.data.as_slice().chunks(32 * 1024) {
            control.check()?;
            hash.update(chunk);
        }
        let id = hash.try_finalize()?;
        self.next = next;
        self.remaining -= 1;
        Ok(Some((id, object)))
    }
}

pub(super) struct Combined<'a, 'b, P> {
    pub(super) old: &'a SnapshotSession<P>,
    pub(super) incoming: Option<&'a Normalized<'b>>,
    pub(super) identity: &'a SnapshotIdentity,
}
impl<P: VerifiedObjectProvider> VerifiedObjectProvider for Combined<'_, '_, P> {
    fn identity(&self) -> &SnapshotIdentity {
        self.identity
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        control.check()?;
        if let Some(pack) = self.incoming
            && let Some(position) = pack.index.lookup(id)
        {
            let entry = pack.data.entry(pack.index.pack_offset_at_index(position))?;
            return Ok(Some(gix::objs::Header {
                kind: entry
                    .header
                    .as_kind()
                    .ok_or_else(|| invalid("normalized receive contains a delta"))?,
                size: entry.decompressed_size,
            }));
        }
        self.old.header(id)
    }
    fn object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        if let Some(pack) = self.incoming
            && let Some(object) = pack.object(id, control, budget, limits)?
        {
            return Ok(Some(object));
        }
        // The held old session enforces its exact identity/budget/control. Errors
        // remain errors; there is no ambient lookup or native Git fallback.
        self.old.object(id)
    }
}
