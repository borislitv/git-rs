use crate::{
    Result,
    auth::Service,
    engine::{Cgi, Engine},
    invalid,
};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub const MAX_BODY: u64 = 4 * 1024 * 1024 * 1024;

pub struct Request {
    pub slug: String,
    pub service: Service,
    pub protocol: String,
    pub cgi: Cgi,
    chunked: bool,
}

fn line(input: &mut impl BufRead, max: usize) -> Result<String> {
    let mut bytes = Vec::new();
    input.take((max + 1) as u64).read_until(b'\n', &mut bytes)?;
    if bytes.len() > max || !bytes.ends_with(b"\r\n") {
        return Err(invalid("invalid HTTP line"));
    }
    bytes.truncate(bytes.len() - 2);
    if bytes.iter().any(|c| *c < 32 || *c == 127) {
        return Err(invalid("HTTP control character"));
    }
    Ok(String::from_utf8(bytes)?)
}

impl Request {
    pub fn parse(input: &mut impl BufRead) -> Result<Self> {
        let request_line = line(input, 4096)?;
        let parts: Vec<_> = request_line.split(' ').collect();
        let [method, uri, version] = parts.as_slice() else {
            return Err(invalid("invalid HTTP request"));
        };
        if !["HTTP/1.0", "HTTP/1.1"].contains(version) || !["GET", "POST"].contains(method) {
            return Err(invalid("unsupported HTTP request"));
        }
        let mut headers = BTreeMap::new();
        let mut budget: usize = 16384;
        loop {
            let header = line(input, 8192)?;
            budget = budget
                .checked_sub(header.len() + 2)
                .ok_or_else(|| invalid("HTTP header budget exceeded"))?;
            if header.is_empty() {
                break;
            }
            let (key, value) = header
                .split_once(':')
                .ok_or_else(|| invalid("invalid HTTP header"))?;
            if key.is_empty() || !key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') {
                return Err(invalid("invalid HTTP header name"));
            }
            if headers
                .insert(key.to_ascii_lowercase(), value.trim().to_string())
                .is_some()
            {
                return Err(invalid("duplicate HTTP header"));
            }
        }
        if headers.contains_key("expect") {
            return Err(invalid("HTTP Expect is unsupported"));
        }
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let (slug, tail) = path
            .strip_prefix('/')
            .and_then(|p| p.split_once(".git/"))
            .ok_or_else(|| invalid("invalid Git URL"))?;
        crate::auth::repository(slug)?;
        let service =
            match (*method, tail, query) {
                ("GET", "info/refs", "service=git-upload-pack")
                | ("POST", "git-upload-pack", "") => Service::Upload,
                ("GET", "info/refs", "service=git-receive-pack")
                | ("POST", "git-receive-pack", "") => Service::Receive,
                _ => return Err(invalid("unsupported Smart HTTP route")),
            };
        let length: Option<u64> = headers
            .get("content-length")
            .map(|value| {
                if value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
                    return Err(invalid("invalid Content-Length"));
                }
                let length = value.parse()?;
                if length > MAX_BODY {
                    return Err(invalid("HTTP body too large"));
                }
                Ok(length)
            })
            .transpose()?;
        let chunked = match headers.get("transfer-encoding").map(String::as_str) {
            Some("chunked") if length.is_none() => true,
            None => false,
            _ => return Err(invalid("invalid HTTP body framing")),
        };
        if *method == "GET" && (chunked || length.unwrap_or(0) != 0)
            || *method == "POST" && !chunked && length.is_none()
        {
            return Err(invalid("HTTP body framing required"));
        }
        let content_type = headers.remove("content-type").unwrap_or_default();
        if *method == "POST" && content_type != format!("application/x-{}-request", service.name())
        {
            return Err(invalid("unsupported Git content type"));
        }
        let encoding = headers.remove("content-encoding").unwrap_or_default();
        if !["", "gzip", "identity"].contains(&encoding.as_str()) {
            return Err(invalid("unsupported content encoding"));
        }
        Ok(Self {
            slug: slug.into(),
            service,
            protocol: headers.remove("git-protocol").unwrap_or_default(),
            cgi: Cgi {
                path: format!("/{tail}"),
                method: (*method).into(),
                query: query.into(),
                content_type,
                encoding,
                length,
            },
            chunked,
        })
    }

    pub fn body<'a, R: BufRead>(&self, input: &'a mut R) -> Body<'a, R> {
        Body {
            input,
            chunked: self.chunked,
            remaining: if self.chunked {
                0
            } else {
                self.cgi.length.unwrap_or(0)
            },
            total: 0,
            done: false,
        }
    }
}

pub struct Body<'a, R> {
    input: &'a mut R,
    chunked: bool,
    remaining: u64,
    total: u64,
    done: bool,
}

impl<R: BufRead> Read for Body<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.done {
            return Ok(0);
        }
        let bad = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid or oversized HTTP body",
            )
        };
        if self.chunked && self.remaining == 0 {
            let size = line(self.input, 128).map_err(|_| bad())?;
            if size.is_empty() || !size.bytes().all(|c| c.is_ascii_hexdigit()) {
                return Err(bad());
            }
            self.remaining = u64::from_str_radix(&size, 16).map_err(|_| bad())?;
            if self.remaining == 0 {
                if !line(self.input, 2).map_err(|_| bad())?.is_empty() {
                    return Err(bad());
                }
                self.done = true;
                return Ok(0);
            }
            if self.remaining > MAX_BODY.saturating_sub(self.total) {
                return Err(bad());
            }
        }
        if self.remaining == 0 {
            self.done = true;
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let count = self.input.read(&mut buffer[..limit])?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated HTTP body",
            ));
        }
        self.remaining -= count as u64;
        self.total += count as u64;
        if self.chunked && self.remaining == 0 {
            let mut crlf = [0; 2];
            self.input.read_exact(&mut crlf)?;
            if crlf != *b"\r\n" {
                return Err(bad());
            }
        }
        Ok(count)
    }
}

pub fn handle(mut stream: TcpStream, engine: &Engine, principal: &str) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(crate::process::DEADLINE)))?;
    stream.set_write_timeout(Some(Duration::from_secs(crate::process::DEADLINE)))?;
    let mut input = BufReader::new(stream.try_clone()?);
    let interrupt = stream.try_clone()?;
    let request = match Request::parse(&mut input) {
        Ok(request) => request,
        Err(_) => {
            stream.write_all(
                b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            )?;
            return Ok(());
        }
    };
    engine.run(
        &request.slug,
        principal,
        request.service,
        &request.protocol,
        Some(&request.cgi),
        &mut request.body(&mut input),
        &mut stream,
        &|| {
            let _ = interrupt.shutdown(std::net::Shutdown::Read);
        },
    )
}

pub fn loopback(address: &str) -> Result<SocketAddr> {
    let address: SocketAddr = address.parse()?;
    if !address.ip().is_loopback() {
        return Err(invalid(
            "unauthenticated development HTTP must bind loopback",
        ));
    }
    Ok(address)
}
