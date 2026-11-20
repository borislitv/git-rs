use crate::{Result, invalid};
use std::io::{Read, Write};

pub const OID_BYTES: usize = 20;
pub const OID_HEX: usize = OID_BYTES * 2;
pub const MAX_PACKET: usize = 65520;
pub const MAX_COMMANDS: usize = 10000;
pub const MAX_REF: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Oid(pub [u8; OID_BYTES]);

impl Oid {
    pub const ZERO: Self = Self([0; OID_BYTES]);

    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let decoded = unhex(bytes)?;
        Ok(Self(
            decoded
                .try_into()
                .map_err(|_| invalid("invalid SHA-1 OID length"))?,
        ))
    }

    pub fn hex(self) -> String {
        hex(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    pub old: Oid,
    pub new: Oid,
    pub name: Vec<u8>,
}

impl Update {
    pub fn parse(line: &[u8]) -> Result<Self> {
        let line = line.strip_suffix(b"\n").unwrap_or(line);
        if line.len() < OID_HEX * 2 + 3 || line[OID_HEX] != b' ' || line[OID_HEX * 2 + 1] != b' ' {
            return Err(invalid("invalid ref command"));
        }
        let name = line[OID_HEX * 2 + 2..].to_vec();
        if name.len() > MAX_REF
            || !name.starts_with(b"refs/")
            || name.iter().any(|c| *c <= b' ' || *c == 127)
        {
            return Err(invalid("invalid ref namespace"));
        }
        crate::repository::ref_name(&name)?;
        Ok(Self {
            old: Oid::parse(&line[..OID_HEX])?,
            new: Oid::parse(&line[OID_HEX + 1..OID_HEX * 2 + 1])?,
            name,
        })
    }

    pub fn line(&self) -> Vec<u8> {
        let mut line = format!("{} {} ", self.old.hex(), self.new.hex()).into_bytes();
        line.extend_from_slice(&self.name);
        line
    }
}

pub fn read_packet(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    reader.read_exact(&mut header)?;
    let length = usize::from_str_radix(std::str::from_utf8(&header)?, 16)?;
    if length == 0 {
        return Ok(None);
    }
    if !(4..=MAX_PACKET).contains(&length) {
        return Err(invalid("invalid packet length"));
    }
    let mut payload = vec![0; length - 4];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

pub fn write_packet(writer: &mut impl Write, payload: &[u8]) -> Result<()> {
    if payload.len() + 4 > MAX_PACKET {
        return Err(invalid("packet too large"));
    }
    write!(writer, "{:04x}", payload.len() + 4)?;
    writer.write_all(payload)?;
    Ok(())
}

pub fn flush(writer: &mut impl Write) -> Result<()> {
    writer.write_all(b"0000")?;
    writer.flush()?;
    Ok(())
}

pub fn read_updates(reader: &mut impl Read) -> Result<Vec<Update>> {
    let mut commands = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    while let Some(line) = read_packet(reader)? {
        if commands.len() >= MAX_COMMANDS {
            return Err(invalid("too many commands"));
        }
        let command = Update::parse(&line)?;
        if !names.insert(command.name.clone()) {
            return Err(invalid("duplicate ref command"));
        }
        commands.push(command);
    }
    if commands.is_empty() {
        return Err(invalid("empty command list"));
    }
    Ok(commands)
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

pub fn unhex(bytes: &[u8]) -> Result<Vec<u8>> {
    if !bytes.len().is_multiple_of(2) {
        return Err(invalid("invalid hex length"));
    }
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |c: u8| match c {
                b'0'..=b'9' => Ok(c - b'0'),
                b'a'..=b'f' => Ok(c - b'a' + 10),
                _ => Err(invalid("invalid hex digit")),
            };
            Ok(digit(pair[0])? * 16 + digit(pair[1])?)
        })
        .collect()
}

/// Private SSH response framing keeps command exit status out of Git payload.
pub struct Framed<'a, W: Write>(pub &'a mut W);
impl<W: Write> Write for Framed<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = bytes.len().min(65536);
        if length == 0 {
            return Ok(0);
        }
        self.0.write_all(&(length as u32).to_be_bytes())?;
        self.0.write_all(&bytes[..length])?;
        Ok(length)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}
impl<W: Write> Framed<'_, W> {
    pub fn finish(&mut self, success: bool) -> Result<()> {
        self.0.write_all(&0u32.to_be_bytes())?;
        self.0.write_all(&[u8::from(!success)])?;
        self.0.flush()?;
        Ok(())
    }
}

pub fn receive_framed(input: &mut impl Read, output: &mut impl Write) -> Result<()> {
    let mut buffer = [0u8; 65536];
    loop {
        let mut header = [0; 4];
        input.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 {
            let mut status = [0];
            input.read_exact(&mut status)?;
            if status[0] != 0 {
                return Err(invalid("SSH operation failed"));
            }
            return Ok(());
        }
        if length > buffer.len() {
            return Err(invalid("invalid SSH response frame"));
        }
        input.read_exact(&mut buffer[..length])?;
        output.write_all(&buffer[..length])?;
        output.flush()?;
    }
}
