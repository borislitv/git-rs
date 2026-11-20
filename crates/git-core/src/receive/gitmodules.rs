//! Filename-derived strict fsck policy. Ordinary blobs remain opaque. Includes
//! and external configuration are never resolved by this untrusted parser.
use crate::{
    CapacityBudget, Control,
    objects::{Result, invalid},
};
use gix_object::bstr::{BStr, ByteSlice};

pub(super) fn is_name(name: &BStr, control: &Control) -> Result<bool> {
    let check = || {
        control
            .check()
            .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
    };
    let options = gix_validate::path::component::Options {
        protect_windows: false,
        protect_hfs: true,
        protect_ntfs: true,
    };
    let classify = |value: &BStr, options| -> Result<bool> {
        match gix_validate::path::component_controlled(
            value,
            Some(gix_validate::path::component::Mode::Symlink),
            options,
            &check,
        ) {
            Ok(_) => Ok(false),
            Err(error)
                if matches!(
                    error.downcast_any_ref::<gix_validate::path::component::Error>(),
                    Some(gix_validate::path::component::Error::SymlinkedGitModules)
                ) =>
            {
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    };
    if classify(name, options)? {
        return Ok(true);
    }
    // Native fsck also checks the final Windows path component of raw tree
    // filenames containing backslashes, without outlawing all such Git names.
    let mut last = None;
    for (chunk, bytes) in name.chunks(8192).enumerate() {
        control.check()?;
        if let Some(index) = bytes.iter().rposition(|byte| *byte == b'\\') {
            last = Some(chunk * 8192 + index + 1);
        }
    }
    match last {
        Some(start) if start < name.len() => classify(
            name[start..].as_bstr(),
            gix_validate::path::component::Options {
                protect_hfs: false,
                ..options
            },
        ),
        _ => Ok(false),
    }
}

fn c_string(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len())]
}

/// Returns the native non-fatal gitmodulesParse diagnostic separately from a
/// fatal security error. The existing metadata limit bounds each opaque parser
/// call; a finite repeated-parse work limit handles malformed continued values.
pub(super) fn validate(
    bytes: &[u8],
    control: &Control,
    budget: &CapacityBudget,
    metadata_limit: usize,
) -> Result<bool> {
    let limit = metadata_limit.min(64 * 1024);
    if bytes.len() > limit {
        return Err(invalid(".gitmodules metadata byte limit exceeded"));
    }
    // Span events, section indexes, normalized duplicate values, parser-error
    // copies and URL work coexist. Admit their conservative peak before parsing.
    let _parser = budget.reservation(
        bytes
            .len()
            .checked_mul(512)
            .ok_or_else(|| invalid(".gitmodules parser capacity overflow"))?,
    )?;
    let mut input = bytes;
    let mut warning = false;
    let mut parsed_bytes = 0;
    let config = loop {
        control.check()?;
        parsed_bytes += input.len();
        if parsed_bytes > limit.saturating_mul(8) {
            return Err(invalid(".gitmodules parser work limit exceeded"));
        }
        match gix_config::parse::Events::from_bytes(input, None) {
            Ok(events) => {
                break gix_config::File::from_parse_events_no_includes(
                    events,
                    gix_config::file::Metadata::api(),
                );
            }
            Err(error) => {
                let line = error.line_number();
                warning = true;
                // Let the maintained parser determine the failure boundary. A
                // valid prefix must still be checked: appending bad syntax must
                // not hide an earlier dangerous duplicate/path/url/update value.
                let mut start = 0;
                let mut current = 1;
                for (index, byte) in input.iter().enumerate() {
                    if current >= line {
                        break;
                    }
                    if *byte == b'\n' {
                        start = index + 1;
                        current += 1;
                    }
                }
                if start >= input.len() {
                    return Err(invalid(".gitmodules parser made no progress"));
                }
                input = &input[..start];
            }
        }
    };
    for section in config.sections_by_name("submodule").into_iter().flatten() {
        control.check()?;
        let Some(name) = section.header().subsection_name() else {
            continue;
        };
        if section.body().num_values() == 0 {
            continue;
        }
        gix_validate::submodule::name(c_string(name).as_bstr())?;
        for key in ["path", "url", "update"] {
            for value in section.body().values(key) {
                control.check()?;
                let value = c_string(&value);
                if key == "path" && value.starts_with(b"-") {
                    return Err(invalid(
                        ".gitmodules path must not be a command-line option",
                    ));
                }
                if key == "update" && value.starts_with(b"!") {
                    return Err(invalid(".gitmodules update command is forbidden"));
                }
                if key == "url" {
                    let check = || {
                        control
                            .check()
                            .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
                    };
                    gix_url::validate_fsck_submodule_url(value.as_bstr(), &check)?;
                }
            }
        }
    }
    control.check()?;
    Ok(warning)
}
