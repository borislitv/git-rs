use crate::{Result, invalid};

/// Ordered, length-prefixed fields: separators, NULs and input ordering cannot
/// alias. Callers supply a canonical options encoding, never a display string.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactKey {
    pub(crate) canonical: Vec<u8>,
    pub(crate) hash: [u8; 32],
}

pub struct KeyParts<'a> {
    pub repository: &'a [u8],
    pub security: &'a [u8],
    pub object_format: &'a str,
    pub kind: &'a str,
    pub inputs: &'a [&'a [u8]],
    pub options: &'a [u8],
    pub representation: &'a str,
    pub producer: &'a str,
}

impl ArtifactKey {
    pub fn new(parts: KeyParts<'_>) -> Result<Self> {
        if parts.inputs.is_empty()
            || parts.inputs.len() > 64
            || !matches!(parts.object_format, "sha1" | "sha256" | "opaque")
        {
            return Err(invalid("invalid artifact inputs or object format"));
        }
        let mut bytes = b"git-rs-artifact-key\0\x01".to_vec();
        fn append(out: &mut Vec<u8>, value: &[u8], empty: bool) -> Result<()> {
            if (!empty && value.is_empty())
                || value.len() > 4096
                || out.len() + value.len() + 4 > 8192
            {
                return Err(invalid("artifact key field or total length limit exceeded"));
            }
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
            Ok(())
        }
        for value in [
            parts.repository,
            parts.security,
            parts.object_format.as_bytes(),
            parts.kind.as_bytes(),
            parts.representation.as_bytes(),
            parts.producer.as_bytes(),
        ] {
            append(&mut bytes, value, false)?;
        }
        append(&mut bytes, parts.options, true)?;
        bytes.extend_from_slice(&(parts.inputs.len() as u32).to_be_bytes());
        for value in parts.inputs {
            append(&mut bytes, value, false)?;
        }
        Ok(Self {
            hash: openssl::sha::sha256(&bytes),
            canonical: bytes,
        })
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.hash
    }

    /// Internal complete-body chunks bind the entire root scope and content.
    pub(crate) fn body_chunk(&self, digest: &[u8; 32]) -> Self {
        let mut canonical = b"git-rs-body-chunk\0\x01".to_vec();
        canonical.extend_from_slice(&self.hash);
        canonical.extend_from_slice(digest);
        Self {
            hash: openssl::sha::sha256(&canonical),
            canonical,
        }
    }
}
