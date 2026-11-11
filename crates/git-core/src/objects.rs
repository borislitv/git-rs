//! Physical, pinned snapshot reads. Authorization and catalog leases belong to
//! the application; this module never discovers repositories or virtual objects.

use crate::executor::CapacityReservation;
use crate::{CapacityBudget, CapacityBuffer, Control};
use gix_hash::{ObjectId, oid};
use gix_object::{Header, Kind};

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid(message: &'static str) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

/// A Vec with a reservation for its actual capacity, acquired before allocation.
/// Only bounded operations are exposed; callers cannot silently grow its Vec.
pub struct AdmittedVec {
    data: Vec<u8>,
    reservation: CapacityReservation,
}

impl AdmittedVec {
    pub fn new(budget: &CapacityBudget, capacity: usize) -> Result<Self> {
        let mut reservation = budget.reservation(capacity)?;
        let mut data = Vec::new();
        data.try_reserve_exact(capacity)?;
        reservation.resize(data.capacity())?;
        Ok(Self { data, reservation })
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn resize(&mut self, length: usize, control: &Control) -> Result<()> {
        if length > self.capacity() {
            return Err(invalid("admitted Vec capacity exceeded"));
        }
        while self.data.len() < length {
            control.check()?;
            self.data
                .resize(length.min(self.data.len().saturating_add(32 * 1024)), 0);
        }
        self.data.truncate(length);
        Ok(())
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    pub(crate) fn vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
    pub(crate) fn reconcile(&mut self) -> Result<()> {
        self.reservation.resize(self.data.capacity())?;
        Ok(())
    }
}

impl Drop for AdmittedVec {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.data));
        // Keep the reservation alive until after backing storage is freed.
    }
}

/// Stable identity of an authorized immutable source/catalog generation.
/// Opaque bytes are not interpreted as paths, configuration or ref names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotIdentity {
    pub source: Vec<u8>,
    pub generation: Vec<u8>,
    pub object_hash: gix_hash::Kind,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    pub max_object_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_graph_objects: usize,
    pub max_graph_edges: usize,
    /// Per-chain delta limit; at most 64 to bound synchronous resolver stack use.
    pub max_delta_depth: usize,
    /// Cumulative inflated bytes plus reconstructed delta result bytes. Also
    /// bounds one decoder's combined working-buffer allocation.
    pub max_decode_bytes: usize,
}

impl ReadLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if self.max_object_bytes == 0
            || self.max_metadata_bytes == 0
            || self.max_graph_objects == 0
            || self.max_graph_edges == 0
            || self.max_delta_depth == 0
            || self.max_decode_bytes == 0
        {
            return Err(invalid("snapshot limits must be nonzero"));
        }
        if self.max_delta_depth > 64 {
            return Err(invalid(
                "snapshot delta depth exceeds the supported maximum of 64",
            ));
        }
        Ok(())
    }
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: 128 * 1024 * 1024,
            max_metadata_bytes: 1024 * 1024,
            max_graph_objects: 100_000,
            max_graph_edges: 1_000_000,
            max_delta_depth: 64,
            max_decode_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Full backing capacity remains charged until the last owner is dropped.
/// No conversion to an uncharged Vec is provided.
#[derive(Debug)]
pub struct Object {
    pub kind: Kind,
    pub data: CapacityBuffer,
}

/// A hash-verified immutable value, not evidence of source membership or authority.
/// Private backing and header/identity fields keep the proof bound to these exact
/// bytes. Converting to the mutable Object API consumes this witness.
pub struct VerifiedObject {
    id: gix_hash::ObjectId,
    kind: Kind,
    backing: VerifiedBacking,
    // Pack witnesses preserve the complete chain's checked resource summary,
    // so handing an existing immutable owner to a tighter session cannot hide
    // intermediate sizes/depth. A generic mutable result has no pack attestation.
    pack_plan: Option<crate::pack::DecodePlan>,
}

enum VerifiedBacking {
    Owned(CapacityBuffer),
    Cached(crate::pack::CachedObject),
}

impl std::fmt::Debug for VerifiedObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedObject")
            .field("id", &self.id)
            .field("header", &self.header())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl VerifiedObject {
    /// Verify one mutable provider result before sealing its ownership. This is
    /// the only public constructor; no caller-provided attestation is accepted.
    pub fn verify(
        id: &oid,
        object: Object,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Self> {
        let value = Self {
            id: id.to_owned(),
            kind: object.kind,
            backing: VerifiedBacking::Owned(object.data),
            pack_plan: None,
        };
        value.validate(id, control, budget, limits)?;
        let mut hash = gix_hash::hasher(id.kind());
        hash.update(&gix_object::encode::loose_header(
            value.kind,
            value.as_slice().len() as u64,
        ));
        for chunk in value.as_slice().chunks(32 * 1024) {
            control.check()?;
            hash.update(chunk);
        }
        if hash.try_finalize()?.as_ref() != id {
            return Err(invalid("physical object hash mismatch"));
        }
        control.check()?;
        Ok(value)
    }

    // Trusted pack implementation only: decode_at has checked the complete
    // physical hash, and the caller copies those exact bytes under control.
    pub(crate) fn from_pack_verified(
        id: &oid,
        kind: Kind,
        data: CapacityBuffer,
        plan: crate::pack::DecodePlan,
    ) -> Self {
        Self {
            id: id.to_owned(),
            kind,
            backing: VerifiedBacking::Owned(data),
            pack_plan: Some(plan),
        }
    }

    pub(crate) fn from_cached(value: crate::pack::CachedObject) -> Self {
        let plan = value.plan();
        Self {
            id: value.id(),
            kind: value.header().kind,
            backing: VerifiedBacking::Cached(value),
            pack_plan: Some(plan),
        }
    }

    pub fn id(&self) -> &oid {
        self.id.as_ref()
    }
    pub fn header(&self) -> Header {
        Header {
            kind: self.kind,
            size: self.as_slice().len() as u64,
        }
    }
    pub fn as_slice(&self) -> &[u8] {
        match &self.backing {
            VerifiedBacking::Owned(data) => data.as_slice(),
            VerifiedBacking::Cached(data) => data.as_slice(),
        }
    }
    pub fn capacity(&self) -> usize {
        match &self.backing {
            VerifiedBacking::Owned(data) => data.capacity(),
            VerifiedBacking::Cached(data) => data.capacity(),
        }
    }

    fn validate(
        &self,
        requested: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<()> {
        control.check()?;
        limits.validate()?;
        if self.id() != requested {
            return Err(invalid("verified object identity mismatch"));
        }
        if self.as_slice().len() > limits.max_object_bytes {
            return Err(invalid("object byte limit exceeded"));
        }
        if let Some(plan) = self.pack_plan {
            plan.validate(limits)?;
        }
        let shared = match &self.backing {
            VerifiedBacking::Owned(data) => data.shares_budget(budget),
            VerifiedBacking::Cached(data) => data.shares_budget(budget),
        };
        if !shared {
            return Err(invalid("verified object budget identity mismatch"));
        }
        control.check()?;
        Ok(())
    }

    /// Consume/discard the witness. Mutable returned bytes carry no reusable
    /// attestation; converting an immutable cache pin necessarily copies it.
    pub fn into_object(self, control: &Control, budget: &CapacityBudget) -> Result<Object> {
        control.check()?;
        let kind = self.kind;
        let data = match self.backing {
            VerifiedBacking::Owned(data) => {
                if !data.shares_budget(budget) {
                    return Err(invalid("verified object budget identity mismatch"));
                }
                data
            }
            VerifiedBacking::Cached(pin) => {
                if !pin.shares_budget(budget) {
                    return Err(invalid("verified object budget identity mismatch"));
                }
                let mut data = budget.buffer(pin.as_slice().len())?;
                for chunk in pin.as_slice().chunks(32 * 1024) {
                    control.check()?;
                    let start = data.as_slice().len();
                    data.resize(start + chunk.len(), 0)?;
                    data.as_mut_slice()[start..].copy_from_slice(chunk);
                }
                data
            }
        };
        control.check()?;
        Ok(Object { kind, data })
    }
}

/// Trust boundary for a previously verified catalog and pinned immutable bytes.
///
/// Implementors must retain their source lease/pins for their entire lifetime,
/// including failures and unwinding. Only physical objects may be returned:
/// notably, the known empty-tree ID is not evidence that a tree exists. Reads
/// must allocate through `budget`, enforce `limits`, and poll `control` inside
/// expensive work. Never consult config, alternates, hooks or a working tree.
/// Mutable results are verified on every read by the default verified_object
/// seam. Overrides may return an opaque immutable witness, never a public proof
/// bit attached to mutable Object bytes. Neither form authorizes source access.
pub trait VerifiedObjectProvider {
    fn identity(&self) -> &SnapshotIdentity;
    fn header(&self, id: &oid, control: &Control) -> Result<Option<Header>>;
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>>;

    /// Resolve physical membership/source authority on every call, independently
    /// of any byte-integrity witness. The default reads once and verifies that
    /// mutable result, including after prior successful reads. Missing is final,
    /// not a request to try a second provider read or manufacture a virtual object.
    fn verified_object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        control.check()?;
        self.object(id, control, budget, limits)?
            .map(|object| VerifiedObject::verify(id, object, control, budget, limits))
            .transpose()
    }

    fn pack_location(
        &self,
        _id: &oid,
        _control: &Control,
    ) -> Result<Option<crate::pack::PackLocation>> {
        Ok(None)
    }
}

/// A session owns its provider (which may itself borrow a pinned workspace).
/// Moving it into executor work keeps source ownership with actual CPU work,
/// not with the waiting ticket. The version is part of derived-artifact keys.
pub struct SnapshotSession<P> {
    pub(crate) provider: P,
    pub(crate) control: Control,
    pub(crate) capacity: CapacityBudget,
    pub(crate) limits: ReadLimits,
}

/// The verified object database is the session itself, not another repository
/// discovery layer. It retains exactly the same provider, source pins and control.
pub type SnapshotOdb<P> = SnapshotSession<P>;

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    pub const VERSION: &'static str = concat!("git-rs-core/", env!("CARGO_PKG_VERSION"));

    pub fn open(
        provider: P,
        expected: &SnapshotIdentity,
        control: Control,
        capacity: CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Self> {
        control.check()?;
        if provider.identity() != expected {
            return Err(invalid("snapshot identity mismatch"));
        }
        limits.validate()?;
        Ok(Self {
            provider,
            control,
            capacity,
            limits,
        })
    }

    pub fn identity(&self) -> &SnapshotIdentity {
        self.provider.identity()
    }

    pub fn pack_location(&self, id: &oid) -> Result<Option<crate::pack::PackLocation>> {
        self.control.check()?;
        self.check_hash_kind(id)?;
        self.provider.pack_location(id, &self.control)
    }

    /// Allocate an actual-capacity-owned output for the `Find` interface.
    pub fn find_buffer(&self, capacity: usize) -> Result<AdmittedVec> {
        if capacity > self.limits.max_object_bytes {
            return Err(invalid("find buffer exceeds object limit"));
        }
        AdmittedVec::new(&self.capacity, capacity)
    }

    pub fn find_in<'a>(
        &self,
        id: &oid,
        buffer: &'a mut AdmittedVec,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        gix_object::Find::try_find(self, id, buffer.vec_mut())
    }

    /// Maintained pack decoder resolver interface. The same caller-capacity
    /// contract as `Find` applies; no new uncharged Vec may be grown here.
    pub fn resolve_pack_base(
        &self,
        id: &oid,
        buffer: &mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_pack::data::decode::entry::ResolvedBase>> {
        let object = gix_object::Find::try_find(self, id, buffer)?;
        Ok(object.map(
            |object| gix_pack::data::decode::entry::ResolvedBase::OutOfPack {
                kind: object.kind,
                end: object.data.len(),
            },
        ))
    }

    pub fn header(&self, id: &oid) -> Result<Option<Header>> {
        self.control.check()?;
        self.check_hash_kind(id)?;
        let header = self.provider.header(id, &self.control)?;
        self.control.check()?;
        Ok(header)
    }

    pub fn object(&self, id: &oid) -> Result<Option<Object>> {
        self.verified_object(id)?
            .map(|object| object.into_object(&self.control, &self.capacity))
            .transpose()
    }

    pub fn verified_object(&self, id: &oid) -> Result<Option<VerifiedObject>> {
        self.control.check()?;
        self.check_hash_kind(id)?;
        let object =
            self.provider
                .verified_object(id, &self.control, &self.capacity, self.limits)?;
        self.control.check()?;
        let Some(object) = object else {
            return Ok(None);
        };
        object.validate(id, &self.control, &self.capacity, self.limits)?;
        self.control.check()?;
        Ok(Some(object))
    }

    pub(crate) fn required(&self, id: &oid, kind: Kind) -> Result<Object> {
        let object = self
            .object(id)?
            .ok_or_else(|| invalid("missing physical object"))?;
        if object.kind != kind {
            return Err(invalid("unexpected object type"));
        }
        if kind != Kind::Blob && object.data.as_slice().len() > self.limits.max_metadata_bytes {
            return Err(invalid("metadata byte limit exceeded"));
        }
        Ok(object)
    }

    fn check_hash_kind(&self, id: &oid) -> Result<()> {
        if id.kind() != self.identity().object_hash {
            return Err(invalid("object hash format mismatch"));
        }
        Ok(())
    }

    /// Resolve a byte path beneath a commit or physical tree. Symlinks are blobs,
    /// never followed, and gitlinks are not repository-local trees or blobs.
    pub fn lookup_blob(&self, start: &oid, path: &[u8]) -> Result<Option<(ObjectId, Object)>> {
        if path.is_empty()
            || path.contains(&0)
            || path
                .split(|b| *b == b'/')
                .any(|c| c.is_empty() || c == b"." || c == b"..")
        {
            return Err(invalid("invalid repository-relative byte path"));
        }
        let initial = self
            .object(start)?
            .ok_or_else(|| invalid("missing lookup root"))?;
        if initial.data.as_slice().len() > self.limits.max_metadata_bytes {
            return Err(invalid("metadata byte limit exceeded"));
        }
        let mut tree_id = match initial.kind {
            Kind::Tree => start.to_owned(),
            Kind::Commit => {
                gix_object::CommitRefIter::from_bytes(initial.data.as_slice(), start.kind())
                    .tree_id()?
            }
            _ => return Err(invalid("lookup root must be a commit or tree")),
        };
        drop(initial);
        let mut components = path.split(|b| *b == b'/').peekable();
        while let Some(component) = components.next() {
            self.control.check()?;
            let tree = self.required(&tree_id, Kind::Tree)?;
            let mut found = None;
            for entry in gix_object::TreeRefIter::from_bytes(tree.data.as_slice(), tree_id.kind()) {
                self.control.check()?;
                let entry = entry?;
                let filename: &[u8] = entry.filename.as_ref();
                if filename == component {
                    found = Some((entry.oid.to_owned(), entry.mode));
                }
            }
            let Some((id, mode)) = found else {
                return Ok(None);
            };
            drop(tree);
            if components.peek().is_none() {
                if !mode.is_blob() && !mode.is_link() {
                    return Ok(None);
                }
                return Ok(Some((id, self.required(&id, Kind::Blob)?)));
            }
            if !mode.is_tree() {
                return Ok(None);
            }
            tree_id = id;
        }
        Ok(None)
    }
}

impl<P: VerifiedObjectProvider> gix_object::FindHeader for SnapshotSession<P> {
    fn try_header(&self, id: &oid) -> gix_error::Result<Option<Header>> {
        self.header(id).map_err(gix_error::Error::from_boxed)
    }
}

impl<P: VerifiedObjectProvider> gix_object::Find for SnapshotSession<P> {
    /// The caller owns `buffer` and its admission for its entire lifetime. This
    /// adapter never grows caller-owned capacity. Use `find_buffer`/`find_in` for
    /// a core-owned, charged buffer. An undersized arbitrary Vec is an error,
    /// rather than an unaccounted allocation hidden behind the Find trait.
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        buffer.clear();
        let result = (|| -> Result<Option<gix_object::Kind>> {
            let Some(object) = self.verified_object(id)? else {
                return Ok(None);
            };
            if buffer.capacity() < object.as_slice().len() {
                return Err(invalid(
                    "Find requires an admitted buffer with sufficient capacity",
                ));
            }
            for chunk in object.as_slice().chunks(32 * 1024) {
                self.control.check()?;
                buffer.extend_from_slice(chunk);
            }
            self.control.check()?;
            Ok(Some(object.header().kind))
        })();
        match result {
            Ok(Some(kind)) => Ok(Some(gix_object::Data::new(buffer, kind, id.kind()))),
            Ok(None) => Ok(None),
            Err(error) => {
                buffer.clear();
                Err(gix_error::Error::from_boxed(error))
            }
        }
    }
}
