//! Whole-commit reencoding for Git's UTF-8 metadata view. Header parsing and
//! any rewriting of the `encoding` header belong to the caller.

use gio::glib;
use gio::prelude::ConverterExtManual;
use gio::{CharsetConverter, ConverterFlags, ConverterResult, IOErrorEnum};

pub struct CommitTextCodec;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Bound on the physical commit, including any bytes after the first NUL.
    pub input_bytes: usize,
    /// Bound on calls to `GConverter::convert` for this operation.
    pub steps: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome<'a> {
    Skipped(&'a [u8]),
    RawFallback(&'a [u8]),
    Converted(&'a [u8]),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InputLimit,
    LabelLimit,
    InvalidLabel,
    /// The caller's staging slice cannot hold the next native output window.
    OutputLimit,
    StepLimit,
    /// GIO did not report usable progress; do not publish a truncated result.
    UnobservableProgress,
    Backend(glib::Error),
}

const MAX_LABEL_BYTES: usize = 64 * 1024;

impl CommitTextCodec {
    /// Convert the complete commit before selecting any fields. For absent or
    /// UTF-8 labels, Git returns the original bytes without checking UTF-8.
    /// `staging` is private scratch: only `Converted` commits a prefix of it.
    /// On fallback or error its contents are unspecified. The bound on a
    /// single GIO call is coarse (the admitted input and output window); a
    /// call does not provide interruption or a deadline.
    pub fn convert<'a>(
        commit: &'a [u8],
        label: Option<&[u8]>,
        staging: &'a mut [u8],
        limits: Limits,
    ) -> Result<Outcome<'a>, Error> {
        if commit.len() > limits.input_bytes {
            return Err(Error::InputLimit);
        }
        let Some(label) = label else {
            return Ok(Outcome::Skipped(commit));
        };
        if label.len() > MAX_LABEL_BYTES {
            return Err(Error::LabelLimit);
        }
        if label.contains(&0) {
            return Err(Error::InvalidLabel);
        }
        let Ok(label) = std::str::from_utf8(label) else {
            return Ok(Outcome::RawFallback(commit));
        };
        if label.eq_ignore_ascii_case("UTF-8") || label.eq_ignore_ascii_case("UTF8") {
            return Ok(Outcome::Skipped(commit));
        }
        let Some(converter) = open(label)? else {
            return Ok(Outcome::RawFallback(commit));
        };
        converter.set_use_fallback(false);

        // repo_logmsg_reencode passes strlen, not the physical object size.
        let input = commit.split(|&byte| byte == 0).next().unwrap_or_default();
        let mut remaining = input;
        let mut produced = 0usize;
        let mut window_len = input.len();
        let mut steps = 0..limits.steps;
        while !remaining.is_empty() {
            steps.next().ok_or(Error::StepLimit)?;
            let end = produced.checked_add(window_len).ok_or(Error::OutputLimit)?;
            let window = staging.get_mut(produced..end).ok_or(Error::OutputLimit)?;
            // A valid UTF-8 prefix cannot start with 0xff. If an error hides
            // writes, do not treat it as a clean, zero-progress output retry.
            window[0] = 0xff;
            match converter.convert(remaining, &mut *window, ConverterFlags::NONE) {
                Ok((ConverterResult::Converted, read, written))
                    if read > 0 && read <= remaining.len() && written <= window_len =>
                {
                    remaining = &remaining[read..];
                    produced += written; // bounded by the checked window end
                }
                Ok(_) => return Err(Error::UnobservableProgress),
                Err(err) if err.matches(IOErrorEnum::NoSpace) => {
                    if window[0] != 0xff {
                        return Err(Error::UnobservableProgress);
                    }
                }
                Err(err)
                    if err.matches(IOErrorEnum::InvalidData)
                        || err.matches(IOErrorEnum::PartialInput) =>
                {
                    return Ok(Outcome::RawFallback(commit));
                }
                Err(err) => return Err(Error::Backend(err)),
            }
            if !remaining.is_empty() {
                // A successful short read hides the underlying status in GIO.
                // Retry the unread suffix in the same converter: malformed or
                // incomplete input is then reported, otherwise this is Git's
                // E2BIG window (2 * unread + 32 minus the NUL slot).
                window_len = remaining
                    .len()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(31))
                    .ok_or(Error::OutputLimit)?;
            }
        }
        // In particular, do not flush pending TSCII state at end of input.
        Ok(Outcome::Converted(&staging[..produced]))
    }
}

fn open(label: &str) -> Result<Option<CharsetConverter>, Error> {
    let label = if label.eq_ignore_ascii_case("latin-1") {
        // Git's fallback spelling for ISO Latin-1; never Windows-1252.
        "ISO-8859-1"
    } else if label.eq_ignore_ascii_case("UTF-16LE-BOM")
        || label.eq_ignore_ascii_case("UTF16LE-BOM")
    {
        "UTF-16"
    } else {
        label
    };
    let result = match CharsetConverter::new("UTF-8", label) {
        Err(err) if err.matches(IOErrorEnum::Failed) => {
            // On Linux, the first failed g_iconv_open can initialize GLib's
            // charset.alias cache and overwrite EINVAL (e.g. with ENOENT).
            // Retry once with the cache initialized; only the explicit
            // NotSupported result below authorizes raw fallback.
            CharsetConverter::new("UTF-8", label)
        }
        result => result,
    };
    match result {
        Ok(converter) => Ok(Some(converter)),
        Err(err) if err.matches(IOErrorEnum::NotSupported) => Ok(None),
        Err(err) => Err(Error::Backend(err)),
    }
}
