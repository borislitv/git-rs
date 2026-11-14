//! Bounded probe framing, shared by frontend and maintenance listeners. Probe
//! parsing never acquires source/cache authority or starts background work.

use crate::{Result, invalid};
use std::io::{BufRead, Read};

pub fn path(input: &mut impl BufRead) -> Result<String> {
    fn line(input: &mut impl BufRead, max: usize) -> Result<String> {
        let mut bytes = Vec::new();
        input.take((max + 1) as u64).read_until(b'\n', &mut bytes)?;
        if bytes.len() > max || !bytes.ends_with(b"\r\n") {
            return Err(invalid("invalid health HTTP line"));
        }
        bytes.truncate(bytes.len() - 2);
        if bytes.iter().any(|byte| *byte < 32 || *byte == 127) {
            return Err(invalid("health HTTP control character"));
        }
        Ok(String::from_utf8(bytes)?)
    }
    let request = line(input, 4096)?;
    let parts: Vec<_> = request.split(' ').collect();
    let ["GET", path, version] = parts.as_slice() else {
        return Err(invalid("invalid health HTTP request"));
    };
    if !["HTTP/1.0", "HTTP/1.1"].contains(version) {
        return Err(invalid("unsupported health HTTP version"));
    }
    let mut remaining = 16384usize;
    for _ in 0..=64 {
        let header = line(input, 8192)?;
        remaining = remaining
            .checked_sub(header.len() + 2)
            .ok_or_else(|| invalid("health HTTP header byte limit exceeded"))?;
        if header.is_empty() {
            return Ok((*path).to_owned());
        }
        let (key, value) = header
            .split_once(':')
            .ok_or_else(|| invalid("invalid health HTTP header"))?;
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid("invalid health HTTP header name"));
        }
        if key.eq_ignore_ascii_case("transfer-encoding")
            || key.eq_ignore_ascii_case("expect")
            || key.eq_ignore_ascii_case("content-length") && value.trim() != "0"
        {
            return Err(invalid("health HTTP request bodies are unsupported"));
        }
    }
    Err(invalid("health HTTP header count limit exceeded"))
}
