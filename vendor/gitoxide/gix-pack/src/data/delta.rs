use gix_error::{ErrorExt, OptionExt, Result, bail};

fn corrupt(message: &'static str) -> gix_error::Message {
    gix_error::message!("Corrupt delta data: {message}").corrupted()
}

/// Given the decompressed pack delta `d`, decode a size in bytes (either the base object size or the result object size)
/// Equivalent to [this canonical git function](https://github.com/git/git/blob/311531c9de557d25ac087c1637818bd2aad6eb3a/delta.h#L89)
pub fn decode_header_size(d: &[u8]) -> Result<(u64, usize)> {
    let mut shift = 0;
    let mut size = 0u64;
    let mut consumed = 0;
    for cmd in d.iter() {
        if shift >= u64::BITS {
            bail!(corrupt("delta header size uses more bits than fit into u64"));
        }
        consumed += 1;
        size |= (u64::from(*cmd) & 0x7f) << shift;
        shift += 7;
        if *cmd & 0x80 == 0 {
            return Ok((size, consumed));
        }
    }
    Err(corrupt("delta header size is truncated").raise())
}

pub(crate) fn apply(base: &[u8], target: &mut [u8], data: &[u8]) -> Result {
    apply_controlled(base, target, data, &|| Ok(()))
}

pub(crate) fn apply_controlled(base: &[u8], mut target: &mut [u8], data: &[u8], check: &dyn Fn() -> Result) -> Result {
    apply_chunks(base, target.len(), data, check, |bytes| {
        let (out, rest) = std::mem::take(&mut target).split_at_mut(bytes.len());
        out.copy_from_slice(bytes);
        target = rest;
    })
}

/// Append validated delta chunks into output storage already reserved by the caller.
pub(crate) fn apply_to_vec(base: &[u8], target: &mut Vec<u8>, data: &[u8], result_size: usize) -> Result {
    apply_to_vec_controlled(base, target, data, result_size, &|| Ok(()))
}

pub(crate) fn apply_to_vec_controlled(
    base: &[u8],
    target: &mut Vec<u8>,
    data: &[u8],
    result_size: usize,
    check: &dyn Fn() -> Result,
) -> Result {
    debug_assert!(
        target.capacity() >= result_size,
        "the decoder reserves the checked result size first"
    );
    target.clear();
    apply_chunks(base, result_size, data, check, |bytes| target.extend_from_slice(bytes))
}

fn apply_chunks(
    base: &[u8],
    mut remaining: usize,
    data: &[u8],
    check: &dyn Fn() -> Result,
    mut write: impl FnMut(&[u8]),
) -> Result {
    fn next_byte(data: &[u8], i: &mut usize) -> Result<u8> {
        let byte = *data
            .get(*i)
            .ok_or_raise(|| corrupt("delta copy instruction is truncated"))?;
        *i += 1;
        Ok(byte)
    }

    let mut i = 0;
    while let Some(cmd) = data.get(i) {
        check()?;
        i += 1;
        let bytes = match cmd {
            cmd if cmd & 0b1000_0000 != 0 => {
                let (mut ofs, mut size): (u32, u32) = (0, 0);
                if cmd & 0b0000_0001 != 0 {
                    ofs = u32::from(next_byte(data, &mut i)?);
                }
                if cmd & 0b0000_0010 != 0 {
                    ofs |= u32::from(next_byte(data, &mut i)?) << 8;
                }
                if cmd & 0b0000_0100 != 0 {
                    ofs |= u32::from(next_byte(data, &mut i)?) << 16;
                }
                if cmd & 0b0000_1000 != 0 {
                    ofs |= u32::from(next_byte(data, &mut i)?) << 24;
                }
                if cmd & 0b0001_0000 != 0 {
                    size = u32::from(next_byte(data, &mut i)?);
                }
                if cmd & 0b0010_0000 != 0 {
                    size |= u32::from(next_byte(data, &mut i)?) << 8;
                }
                if cmd & 0b0100_0000 != 0 {
                    size |= u32::from(next_byte(data, &mut i)?) << 16;
                }
                if size == 0 {
                    size = 0x10000; // 65536
                }
                let ofs = ofs as usize;
                let end = ofs
                    .checked_add(size as usize)
                    .ok_or_raise(|| corrupt("delta copy range overflows"))?;
                base.get(ofs..end)
                    .ok_or_raise(|| corrupt("delta copy range exceeds base object size"))?
            }
            0 => {
                bail!(corrupt("delta command 0 is reserved and invalid"));
            }
            size => {
                let end = i
                    .checked_add(*size as usize)
                    .ok_or_raise(|| corrupt("delta insert range overflows"))?;
                let bytes = data
                    .get(i..end)
                    .ok_or_raise(|| corrupt("delta insert data is truncated"))?;
                i = end;
                bytes
            }
        };
        remaining = remaining
            .checked_sub(bytes.len())
            .ok_or_raise(|| corrupt("delta instructions produced more bytes than promised"))?;
        for chunk in bytes.chunks(32 * 1024) {
            check()?;
            write(chunk);
        }
    }
    debug_assert_eq!(
        i,
        data.len(),
        "delta instructions were not consumed completely, should be impossible"
    );
    if remaining != 0 {
        bail!(corrupt("delta instructions produced fewer bytes than promised"));
    }

    check()
}

#[cfg(test)]
mod tests {
    #[test]
    fn appending_and_slice_outputs_share_strict_instruction_validation() -> gix_error::TestResult {
        let mut output = vec![0xa5; 32];
        let allocation = output.as_ptr();
        super::apply_to_vec(b"0123456789", &mut output, b"\x90\x03\x02XY\x91\x07\x03", 8)?;
        assert_eq!(
            output, b"012XY789",
            "copy and insert chunks produce the complete expected bytes"
        );
        assert_eq!(
            output.as_ptr(),
            allocation,
            "appending uses the already reserved output allocation"
        );
        super::apply_to_vec(b"0123456789", &mut output, b"", 0)?;
        assert!(
            output.is_empty(),
            "an empty result cannot expose an earlier object's bytes"
        );

        for (data, size, message) in [
            (b"\0".as_slice(), 1, "delta command 0 is reserved and invalid"),
            (b"\x91\0".as_slice(), 1, "delta copy instruction is truncated"),
            (
                b"\x91\x14\x02".as_slice(),
                2,
                "delta copy range exceeds base object size",
            ),
            (b"\x03a".as_slice(), 3, "delta insert data is truncated"),
            (
                b"\x01x".as_slice(),
                2,
                "delta instructions produced fewer bytes than promised",
            ),
            (
                b"\x02xy".as_slice(),
                1,
                "delta instructions produced more bytes than promised",
            ),
        ] {
            let mut target = vec![0xa5; size];
            for err in [
                super::apply(b"0123456789", &mut target, data)
                    .expect_err("the slice writer rejects invalid instructions"),
                super::apply_to_vec(b"0123456789", &mut output, data, size)
                    .expect_err("the appending writer rejects the same invalid instructions"),
            ] {
                assert!(err.is_corrupted(), "invalid instructions remain corruption");
                assert!(err.to_string().contains(message), "{message}: {err}");
            }
        }
        Ok(())
    }

    #[test]
    fn instructions_cannot_exceed_the_declared_result_size() {
        let mut error_snapshots = Vec::new();
        for instructions in [b"\x90\x02".as_slice(), b"\x02ab".as_slice()] {
            let err = super::apply(b"ab", &mut [0], instructions)
                .expect_err("neither copying nor inserting may truncate the result");
            error_snapshots.push(gix_testtools::redact_debug_snapshot(&err, &[]));
        }
        insta::assert_debug_snapshot!(error_snapshots, "instructions cannot exceed the declared result size", @"
        [
            Corrupt delta data: delta instructions produced more bytes than promised,
            Corrupt delta data: delta instructions produced more bytes than promised,
        ]
        ");
    }
}
