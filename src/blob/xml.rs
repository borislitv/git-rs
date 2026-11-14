//! S3 response envelopes are bounded before the maintained XML reader runs.
//! Disallow DTD/processing instructions, multiple roots and recursive envelopes.

use crate::{Result, invalid};
use quick_xml::{Reader, events::Event};

#[derive(Debug)]
pub(crate) struct VersioningBlocked;
impl std::fmt::Display for VersioningBlocked {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("source cleanup requires a never-versioned bucket")
    }
}
impl std::error::Error for VersioningBlocked {}

pub(super) fn document<'a>(input: &'a [u8], root: &[u8], children: bool) -> Result<&'a str> {
    if input.len() > 16384 {
        return Err(invalid("S3 XML response byte bound exceeded"));
    }
    let input = std::str::from_utf8(input).map_err(|_| invalid("S3 XML must be UTF-8"))?;
    let mut reader = Reader::from_str(input);
    let (mut depth, mut seen, mut closed, mut declaration) = (0usize, false, false, false);
    for _ in 0..512 {
        match reader.read_event()? {
            Event::Decl(_) if !seen && !declaration => declaration = true,
            Event::Start(tag) => {
                if depth == 0 {
                    if seen || closed || tag.name().as_ref().as_bytes() != root {
                        return Err(invalid("unexpected S3 XML root"));
                    }
                    seen = true;
                } else if !children {
                    return Err(Box::new(VersioningBlocked));
                }
                depth += 1;
                if depth > 8 {
                    return Err(invalid("S3 XML nesting bound exceeded"));
                }
            }
            Event::Empty(tag) => {
                if depth == 0 {
                    if seen || closed || tag.name().as_ref().as_bytes() != root {
                        return Err(invalid("unexpected S3 XML root"));
                    }
                    seen = true;
                    closed = true;
                } else if !children {
                    return Err(Box::new(VersioningBlocked));
                }
            }
            Event::End(_) if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    closed = true;
                }
            }
            Event::Text(text) if text.as_ref().as_bytes().iter().all(u8::is_ascii_whitespace) => {}
            Event::Text(_) | Event::CData(_) | Event::GeneralRef(_) if depth > 0 && children => {}
            Event::Text(_) | Event::CData(_) | Event::GeneralRef(_) if depth > 0 => {
                return Err(Box::new(VersioningBlocked));
            }
            Event::Comment(_) => {}
            Event::Eof if seen && closed && depth == 0 => return Ok(input),
            _ => return Err(invalid("invalid S3 XML envelope")),
        }
    }
    Err(invalid("S3 XML event bound exceeded"))
}
