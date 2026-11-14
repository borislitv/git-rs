//! Maintained pkt-line encoding only; no unimplemented extensions advertised.
use crate::{Result, invalid, metadata::Snapshot, protocol::Update};
use git_rs_core::receive::Capabilities;
use gix_packetline::{Channel, blocking_io::encode};
use std::io::{self, Write};

pub fn advertise(snapshot: &Snapshot, smart_http: bool, output: &mut impl Write) -> Result<()> {
    if smart_http {
        encode::text_to_write(b"# service=git-receive-pack", &mut *output)?;
        encode::flush_to_write(&mut *output)?;
    }
    let mut first = true;
    for (name, oid) in &snapshot.refs {
        crate::repository::ref_name(name)?;
        let mut line = oid.hex().into_bytes();
        line.push(b' ');
        line.extend_from_slice(name);
        if first {
            line.push(0);
            line.extend_from_slice(Capabilities::ADVERTISED);
            first = false;
        }
        encode::text_to_write(&line, &mut *output)?;
    }
    if first {
        let mut line = b"0000000000000000000000000000000000000000 capabilities^{}\0".to_vec();
        line.extend_from_slice(Capabilities::ADVERTISED);
        encode::text_to_write(&line, &mut *output)?;
    }
    encode::flush_to_write(&mut *output)?;
    output.flush()?;
    Ok(())
}

/// Whole-batch result only. Error diagnostics never interpolate SQL/provider
/// messages or client-controlled controls into the wire. Without report-status
/// there is no invented successful ACK. The receipt, not the caller, selects this
/// boolean in the real publication path.
pub fn report_status(
    commands: &[Update],
    capabilities: Capabilities,
    confirmed: bool,
    output: &mut impl Write,
) -> Result<()> {
    if !capabilities.report_status() {
        return Ok(());
    }
    if capabilities.sideband() {
        let mut band = Sideband(&mut *output);
        report(commands, confirmed, &mut band)?;
        encode::flush_to_write(&mut *output)?;
    } else {
        report(commands, confirmed, output)?;
    }
    output.flush()?;
    Ok(())
}

fn report(commands: &[Update], confirmed: bool, output: &mut impl Write) -> Result<()> {
    encode::text_to_write(
        if confirmed {
            b"unpack ok"
        } else {
            b"unpack receive rejected"
        },
        &mut *output,
    )?;
    for command in commands {
        // Commands in a receipt came from the immutable core batch. Keep this
        // standalone serializer safe too, including its public fixture interface.
        if command.name.len() > crate::protocol::MAX_REF
            || command.name.iter().any(|byte| *byte <= 32 || *byte == 127)
        {
            return Err(invalid("invalid status ref"));
        }
        let mut line = if confirmed {
            b"ok ".to_vec()
        } else {
            b"ng ".to_vec()
        };
        line.extend_from_slice(&command.name);
        if !confirmed {
            line.extend_from_slice(b" receive rejected");
        }
        encode::text_to_write(&line, &mut *output)?;
    }
    encode::flush_to_write(output)?;
    Ok(())
}

struct Sideband<'a, W>(&'a mut W);
impl<W: Write> Write for Sideband<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(65_515) {
            encode::band_to_write(Channel::Data, chunk, &mut *self.0)?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
