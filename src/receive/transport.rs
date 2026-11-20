use crate::{Result, http, invalid};
use git_rs_core::{CapacityBudget, CapacityBuffer, Control};
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    Identity,
    Gzip,
}

impl Encoding {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "" | "identity" => Ok(Self::Identity),
            "gzip" => Ok(Self::Gzip),
            _ => Err(invalid("unsupported receive content encoding")),
        }
    }
}

/// A controlled, growable Rust buffer with admission for old/new backing overlap.
/// This writer cannot spill into a file or silently exceed the job's live budget.
pub(crate) struct MemoryOutput<'a> {
    pub(crate) bytes: &'a mut CapacityBuffer,
    pub(crate) control: &'a Control,
    pub(crate) limit: usize,
}

impl Write for MemoryOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes
            .extend_controlled(bytes, self.limit, self.control)
            .map_err(std::io::Error::other)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.control.check().map_err(std::io::Error::other)
    }
}

pub(crate) fn copy_bounded(
    input: &mut impl Read,
    output: &mut impl Write,
    max: u64,
    check: &impl Fn() -> Result<()>,
) -> Result<()> {
    let mut bytes = [0; 32 * 1024];
    let mut total = 0u64;
    loop {
        check()?;
        let count = input.read(&mut bytes)?;
        check()?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .filter(|total| *total <= max)
            .ok_or_else(|| invalid("receive body byte limit exceeded"))?;
        output.write_all(&bytes[..count])?;
    }
    output.flush()?;
    check()
}

/// All gzip framing/CRC/ISIZE/header parsing belongs to the maintained decoder,
/// not to an application reconstruction of gzip or a raw inflate wrapper.
pub(crate) fn decode_gzip(
    input: &mut impl Read,
    output: &mut impl Write,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<()> {
    control.check()?;
    // flate2 1.1.10: each name/comment is bounded to 65,535 bytes; extra is u16.
    // Reserve their rounded Vec backing plus reader buffering and the explicitly
    // selected zlib-rs 0.6.8 workspace profile before constructing the decoder.
    let _workspace = budget.reservation(
        gix::zlib::Workspace::ZLIB_RS_0_6_8.framing_inflate + 3 * 128 * 1024 + 32 * 1024,
    )?;
    let mut reader = Checked {
        inner: BufReader::with_capacity(32 * 1024, input),
        control,
    };
    let mut decoder = flate2::bufread::MultiGzDecoder::new(&mut reader);
    copy_bounded(&mut decoder, output, http::MAX_BODY, &|| {
        control.check()?;
        Ok(())
    })?;
    // MultiGzDecoder reaches EOF only after checking every member and consulting
    // the underlying finite input. Invalid/truncated/non-member tails are errors.
    drop(decoder);
    if !reader.fill_buf()?.is_empty() {
        return Err(invalid("trailing receive gzip input"));
    }
    control.check()?;
    Ok(())
}

struct Checked<'a, R> {
    inner: R,
    control: &'a Control,
}
impl<R: BufRead> Read for Checked<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.control.check().map_err(std::io::Error::other)?;
        let max = out.len().min(32 * 1024);
        self.inner.read(&mut out[..max])
    }
}
impl<R: BufRead> BufRead for Checked<'_, R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.control.check().map_err(std::io::Error::other)?;
        let bytes = self.inner.fill_buf()?;
        Ok(&bytes[..bytes.len().min(32 * 1024)])
    }
    fn consume(&mut self, count: usize) {
        self.inner.consume(count);
    }
}
