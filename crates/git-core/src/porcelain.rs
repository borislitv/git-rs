//! NativeRootV1 line-porcelain. Bytes are assembled privately in admitted chunks;
//! only a complete, validated body can escape this module.

use crate::blame::{BlameError, CompleteBlame, MAX_BODY_BYTES, ProducerVersion, source_error};
use crate::objects::MAX_METADATA_OBJECT_BYTES;
use crate::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, Control, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_hash::oid;
use gix_object::Kind;

const CHUNK: usize = 64 * 1024;
const POLL: usize = 8 * 1024;

pub struct Porcelain;

/// Sealed complete output, with no uncharged Vec conversion or partial builder.
/// Consumers retain this owner through transmission and apply their own authority
/// and deadline checks. A tiny borrowed chunk still pins its full allocation.
pub struct CompletePorcelain {
    chunks: Vec<CapacityBuffer>,
    len: u64,
    _index: CapacityReservation,
    control: Control,
    statistics: crate::BlameStatistics,
}
impl CompletePorcelain {
    pub fn statistics(&self) -> crate::BlameStatistics {
        self.statistics
    }

    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.chunks.iter().map(CapacityBuffer::as_slice)
    }
    pub fn producer(&self) -> &'static ProducerVersion {
        ProducerVersion::current()
    }
}
impl Drop for CompletePorcelain {
    fn drop(&mut self) {
        while !self.chunks.is_empty() {
            for _ in 0..32 {
                drop(self.chunks.pop());
            }
            let _ = self.control.check();
        }
        // Release the index backing before its admission field is destroyed.
        drop(std::mem::take(&mut self.chunks));
    }
}

impl Porcelain {
    pub fn encode_complete(
        blame: &CompleteBlame,
        control: &Control,
        budget: &CapacityBudget,
        max_bytes: usize,
    ) -> Result<CompletePorcelain, BlameError> {
        control.check()?;
        ProducerVersion::verified()?;
        if max_bytes > MAX_BODY_BYTES {
            return Err(BlameError::InvalidRequest);
        }
        // Output belongs to this admitted job, not a freshly reset deadline or
        // unrelated limit counter. Pass clones of its original control/budget.
        if !blame.uses_resources(control, budget) {
            return Err(BlameError::InvalidRequest);
        }
        let mut writer = ChunkWriter::new(
            budget,
            control,
            if blame.blob().is_empty() {
                0
            } else {
                max_bytes
            },
        )?;
        let blob = blame.blob();
        let mut cursor = 0;
        let mut final_line = 0u32;
        for row in blame.trace() {
            control.check()?;
            if row.final_range.start != final_line {
                return Err(BlameError::Corrupt("trace line gap"));
            }
            let metadata = blame
                .metadata(&row.commit_id)
                .ok_or(BlameError::Corrupt("missing attributed commit metadata"))?;
            for offset in 0..row.group_count {
                control.check()?;
                if cursor >= blob.len() {
                    return Err(BlameError::Corrupt("trace exceeds file lines"));
                }
                write_oid(&mut writer, &row.commit_id)?;
                writer.write(b" ")?;
                write_number(
                    &mut writer,
                    u64::from(
                        row.source_range
                            .start
                            .checked_add(offset)
                            .and_then(|line| line.checked_add(1))
                            .ok_or(BlameError::Corrupt("source line overflow"))?,
                    ),
                )?;
                writer.write(b" ")?;
                write_number(&mut writer, u64::from(final_line) + 1)?;
                if offset == 0 {
                    writer.write(b" ")?;
                    write_number(&mut writer, u64::from(row.group_count))?;
                }
                writer.write(b"\n")?;
                writer.write(metadata)?;
                if row.boundary {
                    writer.write(b"boundary\n")?;
                }
                if let Some(previous) = &row.previous {
                    writer.write(b"previous ")?;
                    write_oid(&mut writer, &previous.commit_id)?;
                    writer.write(b" ")?;
                    write_path(&mut writer, previous.source_file_name.as_ref(), control)?;
                    writer.write(b"\n")?;
                }
                writer.write(b"filename ")?;
                write_path(&mut writer, row.source_file_name.as_ref(), control)?;
                writer.write(b"\n\t")?;
                let end = line_end(blob, cursor, control)?;
                writer.write(&blob[cursor..end])?;
                if blob[end - 1] != b'\n' {
                    writer.write(b"\n")?;
                }
                cursor = end;
                final_line = final_line
                    .checked_add(1)
                    .ok_or(BlameError::Corrupt("final line overflow"))?;
            }
        }
        if cursor != blob.len() {
            return Err(BlameError::Corrupt("file exceeds trace lines"));
        }
        control.check()?;
        ProducerVersion::verified()?;
        writer.body.statistics = blame.statistics();
        Ok(writer.body)
    }
}

fn line_end(bytes: &[u8], start: usize, control: &Control) -> Result<usize, BlameError> {
    for (index, chunk) in bytes[start..].chunks(POLL).enumerate() {
        control.check()?;
        if let Some(lf) = chunk.iter().position(|byte| *byte == b'\n') {
            return Ok(start + index * POLL + lf + 1);
        }
    }
    Ok(bytes.len())
}

trait Sink {
    fn write(&mut self, bytes: &[u8]) -> Result<(), BlameError>;
}

struct ChunkWriter<'a> {
    body: CompletePorcelain,
    budget: &'a CapacityBudget,
    control: &'a Control,
    limit: usize,
}
impl<'a> ChunkWriter<'a> {
    fn new(
        budget: &'a CapacityBudget,
        control: &'a Control,
        limit: usize,
    ) -> Result<Self, BlameError> {
        let count = limit.div_ceil(CHUNK);
        let mut reservation = budget.reservation(
            count
                .checked_mul(std::mem::size_of::<CapacityBuffer>())
                .ok_or(CapacityError::Exhausted)?,
        )?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| CapacityError::Allocation)?;
        reservation.resize(
            chunks
                .capacity()
                .checked_mul(std::mem::size_of::<CapacityBuffer>())
                .ok_or(CapacityError::Exhausted)?,
        )?;
        Ok(Self {
            body: CompletePorcelain {
                chunks,
                len: 0,
                _index: reservation,
                control: control.clone(),
                statistics: crate::BlameStatistics::default(),
            },
            budget,
            control,
            limit,
        })
    }
}
impl Sink for ChunkWriter<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> Result<(), BlameError> {
        if bytes.len() > self.limit - self.body.len as usize {
            return Err(BlameError::OutputTooLarge);
        }
        while !bytes.is_empty() {
            self.control.check()?;
            if self
                .body
                .chunks
                .last()
                .is_none_or(|chunk| chunk.as_slice().len() == chunk.capacity())
            {
                if self.body.chunks.len() == self.body.chunks.capacity() {
                    return Err(BlameError::OutputTooLarge);
                }
                self.body.chunks.push(
                    self.budget
                        .buffer(CHUNK.min(self.limit - self.body.len as usize))?,
                );
            }
            let chunk = self.body.chunks.last_mut().expect("chunk was allocated");
            let start = chunk.as_slice().len();
            let count = bytes.len().min(POLL).min(chunk.capacity() - start);
            chunk.resize_controlled(start + count, 0, self.control)?;
            chunk.as_mut_slice()[start..].copy_from_slice(&bytes[..count]);
            self.body.len += count as u64;
            bytes = &bytes[count..];
        }
        Ok(())
    }
}

struct FlatWriter<'a> {
    data: CapacityBuffer,
    control: &'a Control,
    budget: &'a CapacityBudget,
}
impl<'a> FlatWriter<'a> {
    fn new(
        capacity: usize,
        budget: &'a CapacityBudget,
        control: &'a Control,
    ) -> Result<Self, BlameError> {
        Ok(Self {
            data: budget.buffer(capacity)?,
            control,
            budget,
        })
    }
}
impl Sink for FlatWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), BlameError> {
        for chunk in bytes.chunks(POLL) {
            self.control.check()?;
            let start = self.data.as_slice().len();
            let end = start
                .checked_add(chunk.len())
                .ok_or(CapacityError::Exhausted)?;
            if end > MAX_BODY_BYTES {
                return Err(BlameError::OutputTooLarge);
            }
            if end > self.data.capacity() {
                let capacity = end
                    .max(self.data.capacity().saturating_mul(2))
                    .min(MAX_BODY_BYTES);
                let mut grown = Self::new(capacity, self.budget, self.control)?;
                grown.write(self.data.as_slice())?;
                self.data = grown.data;
            }
            self.data.resize_controlled(end, 0, self.control)?;
            self.data.as_mut_slice()[start..end].copy_from_slice(chunk);
        }
        Ok(())
    }
}

fn write_number(sink: &mut impl Sink, mut number: u64) -> Result<(), BlameError> {
    let mut buffer = [0u8; 20];
    let mut start = buffer.len();
    loop {
        start -= 1;
        buffer[start] = b'0' + (number % 10) as u8;
        number /= 10;
        if number == 0 {
            break;
        }
    }
    sink.write(&buffer[start..])
}
fn write_oid(sink: &mut impl Sink, id: &oid) -> Result<(), BlameError> {
    let mut hex = [0u8; 64];
    for (index, byte) in id.as_bytes().iter().enumerate() {
        hex[index * 2] = b"0123456789abcdef"[usize::from(byte >> 4)];
        hex[index * 2 + 1] = b"0123456789abcdef"[usize::from(byte & 15)];
    }
    sink.write(&hex[..id.as_bytes().len() * 2])
}

fn write_path(sink: &mut impl Sink, path: &[u8], control: &Control) -> Result<(), BlameError> {
    let mut quote = false;
    for chunk in path.chunks(POLL) {
        control.check()?;
        quote |= chunk
            .iter()
            .any(|byte| *byte < 32 || *byte >= 127 || matches!(*byte, b'"' | b'\\'));
    }
    if !quote {
        return sink.write(path);
    }
    sink.write(b"\"")?;
    for chunk in path.chunks(POLL) {
        control.check()?;
        for byte in chunk {
            let escape: Option<&[u8]> = match byte {
                7 => Some(b"\\a"),
                8 => Some(b"\\b"),
                9 => Some(b"\\t"),
                10 => Some(b"\\n"),
                11 => Some(b"\\v"),
                12 => Some(b"\\f"),
                13 => Some(b"\\r"),
                b'"' => Some(b"\\\""),
                b'\\' => Some(b"\\\\"),
                _ => None,
            };
            if let Some(escape) = escape {
                sink.write(escape)?;
            } else if *byte < 32 || *byte >= 127 {
                sink.write(&[
                    b'\\',
                    b'0' + (byte >> 6),
                    b'0' + ((byte >> 3) & 7),
                    b'0' + (byte & 7),
                ])?;
            } else {
                sink.write(std::slice::from_ref(byte))?;
            }
        }
    }
    sink.write(b"\"")
}

struct Headers<'a> {
    author: Option<&'a [u8]>,
    committer: Option<&'a [u8]>,
    encoding: Option<&'a [u8]>,
    message: &'a [u8],
}

fn headers<'a>(bytes: &'a [u8], control: &Control) -> Result<Headers<'a>, BlameError> {
    // Native blame's metadata routines consume C strings. A NUL in malformed
    // metadata terminates lookup; it must not reveal later headers or a subject.
    let bytes = cstr(bytes, control)?;
    let mut result = Headers {
        author: None,
        committer: None,
        encoding: None,
        message: &[],
    };
    let mut start = 0;
    while start < bytes.len() {
        let end = line_end(bytes, start, control)?;
        let line = bytes[start..end]
            .strip_suffix(b"\n")
            .unwrap_or(&bytes[start..end]);
        if line.is_empty() {
            result.message = &bytes[end..];
            break;
        }
        if let Some(value) = line.strip_prefix(b"author ") {
            result.author.get_or_insert(value);
        } else if let Some(value) = line.strip_prefix(b"committer ") {
            result.committer.get_or_insert(value);
        } else if let Some(value) = line.strip_prefix(b"encoding ") {
            result.encoding.get_or_insert(value);
        }
        start = end;
    }
    Ok(result)
}

/// Native blame uses find_commit_subject(), not pretty-format %s paragraph
/// folding: skip whitespace-only leading lines, then preserve the first subject
/// line verbatim (including trailing CR/spaces). Empty subject falls back to OID.
fn subject<'a>(message: &'a [u8], control: &Control) -> Result<&'a [u8], BlameError> {
    let message = cstr(message, control)?;
    let mut start = 0;
    while start < message.len() {
        let end = line_end(message, start, control)?;
        let line = message[start..end]
            .strip_suffix(b"\n")
            .unwrap_or(&message[start..end]);
        let mut blank = true;
        for chunk in line.chunks(POLL) {
            control.check()?;
            if !chunk
                .iter()
                .all(|byte| matches!(byte, b' ' | b'\t' | b'\r' | 11 | 12))
            {
                blank = false;
                break;
            }
        }
        if !blank {
            return Ok(line);
        }
        start = end;
    }
    Ok(&[])
}
fn cstr<'a>(bytes: &'a [u8], control: &Control) -> Result<&'a [u8], BlameError> {
    for (index, chunk) in bytes.chunks(POLL).enumerate() {
        control.check()?;
        if let Some(nul) = chunk.iter().position(|byte| *byte == 0) {
            return Ok(&bytes[..index * POLL + nul]);
        }
    }
    control.check()?;
    Ok(bytes)
}

fn signature(sink: &mut impl Sink, prefix: &[u8], raw: Option<&[u8]>) -> Result<(), BlameError> {
    // Header slices already end before the first NUL in the complete message.
    let parsed = raw.and_then(|raw| gix_actor::SignatureRef::from_bytes(raw).ok());
    let (name, email, seconds, timezone) = if let Some(parsed) = &parsed {
        let name: &[u8] = parsed.name.as_ref();
        let name_end = name
            .iter()
            .rposition(|byte| !byte.is_ascii_whitespace())
            .map_or(0, |index| index + 1);
        let mut time = parsed.time.split_ascii_whitespace();
        let seconds = match time.next() {
            Some(number) if number.starts_with('-') => number[1..]
                .parse::<u64>()
                .map(u64::wrapping_neg)
                .map_err(|_| BlameError::Corrupt("signature timestamp overflow"))?,
            Some(number) => number
                .parse::<u64>()
                .map_err(|_| BlameError::Corrupt("signature timestamp"))?,
            None => 0,
        };
        (
            &name[..name_end],
            parsed.email.as_ref(),
            seconds,
            time.next().map(str::as_bytes).unwrap_or(b"(unknown)"),
        )
    } else {
        (
            b"(unknown)".as_slice(),
            b"(unknown)".as_slice(),
            0,
            b"(unknown)".as_slice(),
        )
    };
    sink.write(prefix)?;
    sink.write(b" ")?;
    sink.write(name)?;
    sink.write(b"\n")?;
    sink.write(prefix)?;
    sink.write(b"-mail ")?;
    if parsed.is_some() {
        sink.write(b"<")?;
    }
    sink.write(email)?;
    if parsed.is_some() {
        sink.write(b">")?;
    }
    sink.write(b"\n")?;
    sink.write(prefix)?;
    sink.write(b"-time ")?;
    write_number(sink, seconds)?;
    sink.write(b"\n")?;
    sink.write(prefix)?;
    sink.write(b"-tz ")?;
    sink.write(timezone)?;
    sink.write(b"\n")
}

pub(crate) fn commit_metadata<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    id: &oid,
) -> Result<CapacityBuffer, BlameError> {
    session.control.check()?;
    let header = session
        .header(id)
        .map_err(|e| source_error(e.as_ref()))?
        .ok_or(BlameError::Corrupt("missing attributed commit"))?;
    if header.kind != Kind::Commit || header.size > MAX_METADATA_OBJECT_BYTES as u64 {
        return Err(BlameError::Corrupt(
            "attributed commit metadata bound or type",
        ));
    }
    let object = session
        .required(id, Kind::Commit)
        .map_err(|e| source_error(e.as_ref()))?;
    let original = object.data.as_slice();
    let encoding = headers(original, &session.control)?.encoding;
    let converted = reencode(original, encoding, &session.capacity, &session.control)?;
    let bytes = converted
        .as_ref()
        .map_or(original, CapacityBuffer::as_slice);
    let parsed = headers(bytes, &session.control)?;
    let mut output = FlatWriter::new(
        bytes
            .len()
            .checked_add(512)
            .ok_or(CapacityError::Exhausted)?,
        &session.capacity,
        &session.control,
    )?;
    signature(&mut output, b"author", parsed.author)?;
    signature(&mut output, b"committer", parsed.committer)?;
    output.write(b"summary ")?;
    let summary = subject(parsed.message, &session.control)?;
    if summary.is_empty() {
        output.write(b"(")?;
        write_oid(&mut output, id)?;
        output.write(b")")?;
    } else {
        output.write(summary)?;
    }
    output.write(b"\n")?;
    session.control.check()?;
    Ok(output.data)
}

// Git converts the complete commit, not just its subject. Conversion remains
// private until input succeeds: native failure preserves the original atomically,
// even when valid prefixes have already produced UTF-8 output.
fn reencode(
    bytes: &[u8],
    label: Option<&[u8]>,
    budget: &CapacityBudget,
    control: &Control,
) -> Result<Option<CapacityBuffer>, BlameError> {
    let Some(label) = label else {
        return Ok(None);
    };
    // repo_logmsg_reencode calls reencode_string(), whose input length is strlen,
    // not the physical commit size. This matters for malformed/NUL metadata.
    let bytes = cstr(bytes, control)?;
    use iconv_native::stream::{Converter, Status};
    if label.eq_ignore_ascii_case(b"UTF-8") || label.eq_ignore_ascii_case(b"UTF8") {
        return Ok(None); // Git skips reencoding these two exact names as well.
    }
    let label = if label.eq_ignore_ascii_case(b"UTF-16LE-BOM")
        || label.eq_ignore_ascii_case(b"UTF16LE-BOM")
    {
        b"UTF-16".as_slice() // Git's input-side BOM alias, not an iconv alias.
    } else {
        label
    };
    let mut name = FlatWriter::new(
        label.len().checked_add(1).ok_or(CapacityError::Exhausted)?,
        budget,
        control,
    )?;
    name.write(label)?;
    name.write(b"\0")?;
    let name = std::ffi::CStr::from_bytes_with_nul(name.data.as_slice())
        .map_err(|_| BlameError::Corrupt("encoding name contains NUL"))?;
    control.check()?;
    let Some(mut converter) = Converter::new(name, c"UTF-8").or_else(|| {
        label
            .eq_ignore_ascii_case(b"latin-1")
            .then(|| Converter::new(c"ISO-8859-1", c"UTF-8"))
            .flatten()
    }) else {
        return Ok(None);
    };
    // Grow only through admitted buffers (including simultaneous old/new
    // backing). Do not assume four output bytes per source byte: some native
    // encodings, e.g. TSCII, decompose one byte into multiple Unicode scalars.
    let capacity = bytes
        .len()
        .checked_add(32)
        .ok_or(CapacityError::Exhausted)?;
    let mut output = FlatWriter::new(capacity, budget, control)?;
    let mut scratch = budget.buffer(POLL)?;
    scratch.resize_controlled(POLL, 0, control)?;
    let mut read = 0;
    // Reproduce Git 2.56's logical output windows, including its E2BIG growth
    // formula. Some libc converters expose buffer-boundary-sensitive behavior.
    // Feed one source byte (or one incomplete sequence) at a time so our bounded
    // scratch does not introduce additional output-full boundaries of its own.
    let mut native_remaining = bytes.len();
    let mut end = bytes.len().min(1);
    loop {
        control.check()?;
        let progress = converter.convert(
            &bytes[read..end],
            &mut scratch.as_mut_slice()[..native_remaining.min(POLL)],
        );
        output.write(&scratch.as_slice()[..progress.written])?;
        read += progress.consumed;
        native_remaining -= progress.written;
        match progress.status {
            Status::Invalid => return Ok(None),
            Status::Incomplete => {
                if end == bytes.len() || end - read >= POLL {
                    return Ok(None);
                }
                // Retry the incomplete suffix in the same descriptor, adding
                // only enough input to keep every native call <= POLL bytes.
                end = bytes.len().min(end.saturating_add(1));
            }
            Status::Complete if read == bytes.len() => break,
            Status::Complete => end = bytes.len().min(read.saturating_add(1)),
            Status::OutputFull => {
                native_remaining = (bytes.len() - read)
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(31))
                    .ok_or(CapacityError::Exhausted)?;
            }
        }
    }
    // Git 2.56 closes without a reset/flush call. Do not "fix" this: e.g. a
    // trailing TSCII 0x8a has pending state which flush would visibly append.
    // The adapter's explicit flush contract is tested separately for consumers
    // that do require it; native blame deliberately preserves Git's no-flush.
    control.check()?;
    Ok(Some(output.data))
}
