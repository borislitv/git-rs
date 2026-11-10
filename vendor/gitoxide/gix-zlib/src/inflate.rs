use gix_error::Result;
use gix_error::{ResultExt, message};

#[cfg(not(feature = "sirocco"))]
use crate::FlushDecompress;
use crate::{Inflate, Status};

#[cfg(feature = "sirocco")]
impl Default for Inflate {
    fn default() -> Self {
        Self {
            state: Box::new(sirocco::inflate::Decoder::new(sirocco::Format::Zlib)),
            sirocco_done: false,
        }
    }
}

impl Inflate {
    /// Decode one complete stream with request-local checks between at most 32 KiB
    /// of input and output work. Reject truncated streams and incorrect output sizes.
    ///
    /// This operation uses the resumable zlib-rs backend, including in builds with
    /// `sirocco`, whose prefix API requires retaining contiguous output history.
    /// On error the destination is incomplete and must not be published.
    pub fn once_controlled(
        &mut self,
        input: &[u8],
        out: &mut [u8],
        check: &dyn Fn() -> Result<()>,
    ) -> Result<(Status, usize, usize)> {
        #[cfg(not(feature = "sirocco"))]
        let decoder = &mut self.state;
        #[cfg(feature = "sirocco")]
        let mut controlled = crate::Decompress::default();
        #[cfg(feature = "sirocco")]
        let decoder = &mut controlled;
        decoder.reset();
        let (mut read, mut written) = (0, 0);
        loop {
            check()?;
            let end = input.len().min(read + 32 * 1024);
            let out_end = out.len().min(written + 32 * 1024);
            // Once the promised output is full, consume the trailer with one byte
            // of scratch so an oversized stream cannot masquerade as a complete one.
            let mut extra = [0];
            let destination = if written == out.len() {
                &mut extra[..]
            } else {
                &mut out[written..out_end]
            };
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder.decompress(&input[read..end], destination, crate::FlushDecompress::None)?;
            let consumed = (decoder.total_in() - before_in) as usize;
            let produced = (decoder.total_out() - before_out) as usize;
            read += consumed;
            if written == out.len() && produced != 0 {
                return Err(gix_error::corruption("Zlib output exceeds the declared size").into());
            }
            written += produced;
            check()?;
            if status == Status::StreamEnd {
                if written != out.len() {
                    return Err(gix_error::corruption("Zlib output is shorter than the declared size").into());
                }
                return Ok((status, read, written));
            }
            if consumed == 0 && produced == 0 {
                return Err(gix_error::corruption("Truncated zlib stream made no progress").into());
            }
        }
    }

    /// Decompress the zlib stream at the start of `input` into `out`, returning `(status, consumed_in, written_out)`.
    ///
    /// If `out` can hold the whole decompressed stream, the status is [`Status::StreamEnd`]. Otherwise this behaves
    /// like [`Self::prefix()`].
    /// Cannot be run multiple times without [resetting](Self::reset()) in between.
    pub fn once(&mut self, input: &[u8], out: &mut [u8]) -> Result<(Status, usize, usize)> {
        self.prefix(input, out)
    }

    /// Decompress as much of the zlib stream at the start of `input` as fits into `out` in a single step,
    /// returning `(status, consumed_in, written_out)`.
    ///
    /// Requires a fresh instance or a call to [`Self::reset()`] before starting another stream.
    /// The decompression can be continued with [`Self::rest()`].
    pub fn prefix(&mut self, input: &[u8], out: &mut [u8]) -> Result<(Status, usize, usize)> {
        #[cfg(feature = "sirocco")]
        {
            let p = self
                .state
                .decompress(input, out, true)
                .map_err(sirocco_error)
                .or_raise(|| message("Could not decode zip stream"))?;
            self.sirocco_done = p.done;
            let status = if p.done { Status::StreamEnd } else { Status::Ok };
            Ok((status, p.consumed, p.output.len()))
        }
        #[cfg(not(feature = "sirocco"))]
        {
            let before_in = self.state.total_in();
            let before_out = self.state.total_out();
            let status = self
                .state
                .decompress(input, out, FlushDecompress::None)
                .or_raise(|| message("Could not decode zip stream"))?;
            Ok((
                status,
                (self.state.total_in() - before_in) as usize,
                (self.state.total_out() - before_out) as usize,
            ))
        }
    }

    /// Decompress the rest of the stream that [`Self::prefix()`] started, from `input` (what follows the input it
    /// consumed) until the stream ends or `len` bytes are written.
    ///
    /// `prefix` is what `prefix()` wrote. `out` receives the output after its first `skip` bytes, which includes
    /// `prefix[skip..]`, and is cleared first. Returns how many bytes were written to `out`.
    /// `skip` must not exceed `prefix.len()`, and `len` must fit at least `prefix[skip..]`.
    /// To decode the entire stream, `len` must fit all decompressed bytes after `skip`, including that prefix tail.
    /// If `prefix()` already finished the stream, only the prefix tail is returned and `input` is left unused.
    pub fn rest(&mut self, input: &[u8], prefix: &[u8], skip: usize, out: &mut Vec<u8>, len: usize) -> Result<usize> {
        out.clear();
        #[cfg(feature = "sirocco")]
        {
            if self.sirocco_done {
                out.extend_from_slice(&prefix[skip..]);
                return Ok(out.len());
            }
            // The decoder needs the output so far in front of where it writes.
            out.resize(skip + len, 0);
            out[..prefix.len()].copy_from_slice(prefix);
            let p = self
                .state
                .decompress(input, out, true)
                .map_err(sirocco_error)
                .or_raise(|| message("Could not decode zip stream"))?;
            self.sirocco_done = p.done;
            let n = prefix.len() + p.output.len() - skip;
            out.drain(..skip);
            out.truncate(n);
            Ok(n)
        }
        #[cfg(not(feature = "sirocco"))]
        {
            out.resize(len, 0);
            let held = prefix.len() - skip;
            out[..held].copy_from_slice(&prefix[skip..]);
            let mut input = input;
            let n = crate::stream::inflate::read(&mut input, &mut self.state, &mut out[held..])
                .or_raise(|| message("Could not decode zip stream"))?;
            out.truncate(held + n);
            Ok(held + n)
        }
    }

    /// Ready this instance for decoding another data stream.
    pub fn reset(&mut self) {
        self.state.reset();
        #[cfg(feature = "sirocco")]
        {
            self.sirocco_done = false;
        }
    }
}

/// The error `zlib-rs` would report for a `sirocco` one.
#[cfg(feature = "sirocco")]
fn sirocco_error(err: sirocco::inflate::Error) -> gix_error::Error {
    match err {
        sirocco::inflate::Error::PresetDictionary => {
            message("Decompressing this input requires a dictionary").validation_error()
        }
        sirocco::inflate::Error::BadData | sirocco::inflate::Error::ChecksumMismatch => {
            message("Invalid input data").corrupted_error()
        }
        sirocco::inflate::Error::Truncated => message("Truncated input data").corrupted_error(),
        _ => message("stream error").into(),
    }
}
