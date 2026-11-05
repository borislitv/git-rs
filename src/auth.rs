use crate::{Result, invalid};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

const MAX_CREDENTIAL_FILE: u64 = 65536;
const MAX_HTTP_IDENTITIES: usize = 256;
const MAX_TOKEN: usize = 128;

struct Token {
    bytes: [u8; MAX_TOKEN],
    length: usize,
}

impl Token {
    fn parse(value: &[u8]) -> Option<Self> {
        if !(32..=MAX_TOKEN).contains(&value.len())
            || !value
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(c))
        {
            return None;
        }
        let mut bytes = [0; MAX_TOKEN];
        bytes[..value.len()].copy_from_slice(value);
        Some(Self {
            bytes,
            length: value.len(),
        })
    }

    fn matches(&self, candidate: &Self) -> bool {
        // Fixed-size, full comparison: never stop at the first mismatching byte.
        // black_box keeps each reduction observable to the optimizer; neither
        // credentials nor intermediate comparison values are emitted anywhere.
        let mut difference = self.length ^ candidate.length;
        for (left, right) in self.bytes.iter().zip(&candidate.bytes) {
            difference |= std::hint::black_box(usize::from(left ^ right));
        }
        std::hint::black_box(difference) == 0
    }
}

/// Startup-loaded HTTP Basic identities, independent of repository ACLs.
/// One `PRINCIPAL TOKEN` per nonempty line, exactly one ASCII space. Principals
/// use `principal()` grammar; tokens are 32..=128 `[A-Za-z0-9_-]` bytes. Generate
/// tokens from at least 32 random bytes, encoded as unpadded Base64url. Maximum
/// file size is 64 KiB and maximum identity count is 256. No duplicate principal
/// or token is allowed. Reload requires restarting the serving process.
pub struct HttpCredentials {
    entries: BTreeMap<String, Token>,
}

impl HttpCredentials {
    pub fn load(path: &Path) -> Result<Self> {
        if !std::fs::metadata(path)?.is_file() {
            return Err(invalid("HTTP credentials must be a regular file"));
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(MAX_CREDENTIAL_FILE + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CREDENTIAL_FILE {
            return Err(invalid("HTTP credential mapping exceeds size limit"));
        }
        Self::parse(
            std::str::from_utf8(&bytes).map_err(|_| invalid("invalid HTTP credential mapping"))?,
        )
    }

    pub fn parse(mapping: &str) -> Result<Self> {
        let bad = || invalid("invalid or ambiguous HTTP credential mapping");
        if mapping.len() as u64 > MAX_CREDENTIAL_FILE {
            return Err(bad());
        }
        let mut entries: BTreeMap<String, Token> = BTreeMap::new();
        for line in mapping.lines().filter(|line| !line.is_empty()) {
            let (actor, value) = line.split_once(' ').ok_or_else(bad)?;
            principal(actor).map_err(|_| bad())?;
            let token = Token::parse(value.as_bytes()).ok_or_else(bad)?;
            if entries.len() == MAX_HTTP_IDENTITIES
                || entries.contains_key(actor)
                || entries.values().any(|previous| previous.matches(&token))
            {
                return Err(bad());
            }
            entries.insert(actor.into(), token);
        }
        if entries.is_empty() {
            return Err(bad());
        }
        Ok(Self { entries })
    }

    /// Return only the server-owned principal after verifying a Basic credential.
    pub fn authenticate(&self, authorization: &str) -> Option<&str> {
        let (scheme, encoded) = authorization.split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("Basic") {
            return None;
        }
        let decoded = basic_base64(encoded)?;
        let colon = decoded.iter().position(|byte| *byte == b':')?;
        let actor = std::str::from_utf8(&decoded[..colon]).ok()?;
        let candidate = Token::parse(&decoded[colon + 1..])?;
        let (actor, expected) = self.entries.get_key_value(actor)?;
        expected.matches(&candidate).then_some(actor.as_str())
    }
}

fn basic_base64(encoded: &str) -> Option<Vec<u8>> {
    // At most 128 username bytes, a colon, and 128 token bytes. Require canonical
    // padded RFC 4648 encoding, including zero pad bits; reject whitespace.
    if encoded.is_empty() || encoded.len() > 344 || !encoded.len().is_multiple_of(4) {
        return None;
    }
    let digit = |byte| match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    let mut decoded = Vec::with_capacity(encoded.len() / 4 * 3);
    for (index, block) in encoded.as_bytes().as_chunks::<4>().0.iter().enumerate() {
        let a = digit(block[0])?;
        let b = digit(block[1])?;
        let last = (index + 1) * 4 == encoded.len();
        decoded.push((a << 2) | (b >> 4));
        if block[2] == b'=' {
            if !last || block[3] != b'=' || b & 15 != 0 {
                return None;
            }
        } else {
            let c = digit(block[2])?;
            decoded.push((b << 4) | (c >> 2));
            if block[3] == b'=' {
                if !last || c & 3 != 0 {
                    return None;
                }
            } else {
                decoded.push((c << 6) | digit(block[3])?);
            }
        }
    }
    Some(decoded)
}

pub fn principal(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.@".contains(&c))
    {
        return Err(invalid("invalid principal"));
    }
    Ok(value)
}

pub fn repository(value: &str) -> Result<&str> {
    if value.len() > 200
        || value.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || !part
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
        })
    {
        return Err(invalid("invalid repository slug"));
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Service {
    Upload,
    Receive,
}

impl Service {
    pub fn name(self) -> &'static str {
        match self {
            Self::Upload => "git-upload-pack",
            Self::Receive => "git-receive-pack",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "git-upload-pack" => Ok(Self::Upload),
            "git-receive-pack" => Ok(Self::Receive),
            _ => Err(invalid("unsupported Git service")),
        }
    }
}

pub fn ssh_command(command: &str) -> Result<(Service, &str)> {
    let (service, path) = command
        .split_once(' ')
        .ok_or_else(|| invalid("invalid SSH command"))?;
    // Git's standard SSH transport quotes this deliberately restricted slug grammar.
    let path = path
        .strip_prefix('\'')
        .and_then(|p| p.strip_suffix('\''))
        .ok_or_else(|| invalid("quoted repository required"))?;
    let slug = path
        .strip_suffix(".git")
        .ok_or_else(|| invalid(".git suffix required"))?;
    Ok((Service::parse(service)?, repository(slug)?))
}

/// Convert a server-owned principal/public-key map into a restricted OpenSSH
/// AuthorizedKeysCommand result. No client value can select its principal.
pub fn authorized_key(
    mapping: &str,
    key_type: &str,
    key: &str,
    executable: &str,
    socket: &str,
) -> Result<Option<String>> {
    if mapping.len() > 65536
        || key_type.is_empty()
        || !key_type
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.@+".contains(&c))
        || key.is_empty()
        || key.len() > 16384
        || !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"+/=".contains(&c))
    {
        return Err(invalid("invalid SSH public-key mapping input"));
    }
    for path in [executable, socket] {
        if !path.starts_with('/')
            || !path
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"/-_.".contains(&c))
        {
            return Err(invalid(
                "SSH helper paths require absolute shell-safe paths",
            ));
        }
    }
    let mut matched = None;
    for line in mapping.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let actor = principal(
            fields
                .next()
                .ok_or_else(|| invalid("missing key principal"))?,
        )?;
        let kind = fields
            .next()
            .ok_or_else(|| invalid("missing public-key type"))?;
        let public_key = fields.next().ok_or_else(|| invalid("missing public key"))?;
        if kind == key_type && public_key == key {
            if matched.is_some_and(|previous| previous != actor) {
                return Err(invalid("public key maps to multiple principals"));
            }
            matched = Some(actor);
        }
    }
    Ok(matched.map(|actor| format!(
        "restrict,command=\"{executable} ssh --socket {socket} --principal {actor}\" {key_type} {key}\n"
    )))
}
