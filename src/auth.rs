use crate::{Result, invalid};

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
