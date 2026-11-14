//! Byte-preserving ref validation and the authorized source adapter.

use crate::{Result, invalid, protocol::MAX_REF};
use gix::bstr::ByteSlice;

#[path = "blame_source.rs"]
pub(crate) mod blame_source;

pub(crate) fn ref_name(name: &[u8]) -> Result<gix::refs::FullName> {
    if name.len() > MAX_REF || !name.starts_with(b"refs/") {
        return Err(invalid("invalid ref namespace"));
    }
    Ok(gix::refs::FullName::try_from(name.as_bstr())?)
}
