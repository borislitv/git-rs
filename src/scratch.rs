//! Opaque operation identities; source bytes never require a request repository.

use crate::Result;
use std::{fs::File, io::Read};

pub fn identifier() -> Result<String> {
    let mut bytes = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(crate::protocol::hex(&bytes))
}
