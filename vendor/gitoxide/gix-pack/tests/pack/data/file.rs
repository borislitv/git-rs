use gix_odb::pack;

use crate::fixture_path;

fn pack_at(at: &str) -> pack::data::File {
    pack::data::File::at(fixture_path(at).as_path(), gix_hash::Kind::Sha1).expect("valid pack file")
}

#[test]
fn unresolved_delta_base_is_not_found() {
    use gix_error::{ErrorExt, message};

    let base_id = gix_hash::ObjectId::empty_blob(gix_hash::Kind::Sha1);
    let err = pack::data::decode::DeltaBaseUnresolved(base_id).and_raise(message("Could not decode object"));
    insta::assert_debug_snapshot!(gix_testtools::redact_debug_snapshot(&(err), &[]), "an unresolved delta base is a missing object", @"
    Could not decode object

    Caused by:
        0: A delta chain could not be followed as the ref base with id Oid(1) could not be found
    ");
    assert!(err.is_not_found(), "an unresolved delta base is a missing object");
    assert!(
        err.probable_cause().is::<pack::data::decode::DeltaBaseUnresolved>(),
        "the missing delta base, not its classification marker or outer context, is the probable cause"
    );
    assert_eq!(
        err.downcast_any_ref::<pack::data::decode::DeltaBaseUnresolved>()
            .expect("the missing delta base is retained")
            .0,
        base_id,
        "the decode failure retains the missing object ID"
    );
}

mod method {
    use std::sync::atomic::AtomicBool;

    use gix_utils::progress;

    use crate::{SMALL_PACK, data::file::pack_at, pack_from_memory_at};

    #[test]
    fn checksum() {
        let p = pack_at(SMALL_PACK);
        assert_eq!(p.checksum(), "0f3ea84cd1bba10c2a03d736a460635082833e59");
    }

    #[test]
    fn verify_checksum() -> gix_testtools::TestResult {
        let p = pack_at(SMALL_PACK);
        assert_eq!(
            p.verify_checksum(&mut progress::Discard, &AtomicBool::new(false))?,
            p.checksum()
        );
        Ok(())
    }

    #[test]
    fn interrupted_checksum_preserves_cancellation_and_io_source() {
        let p = pack_at(SMALL_PACK);
        let err = p
            .verify_checksum(&mut progress::Discard, &AtomicBool::new(true))
            .expect_err("the interrupt flag cancels hashing the pack");
        assert!(err.is_cancelled(), "verification preserves the hashing cancellation");
        assert!(
            !err.is_retryable(),
            "verification must not reclassify cancellation as retryable"
        );
        assert_eq!(
            err.downcast_any_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::Interrupted),
            "the original hashing I/O interruption survives"
        );
    }

    #[test]
    fn verify_checksum_from_memory() -> gix_testtools::TestResult {
        let p = pack_from_memory_at(SMALL_PACK);
        assert_eq!(
            p.verify_checksum(&mut progress::Discard, &AtomicBool::new(false))?,
            p.checksum()
        );
        Ok(())
    }

    #[test]
    fn decode_entry_respects_alloc_limit_bytes() {
        let entry_offset = 1968;
        let mut buf = Vec::new();
        let mut inflate = Default::default();

        let pack = pack_at(SMALL_PACK);
        let entry = pack.entry(entry_offset).expect("valid object type");
        assert!(
            pack.decode_entry(
                entry.clone(),
                &mut buf,
                &mut inflate,
                &|_, _| Ok(None),
                &mut gix_odb::pack::cache::Never
            )
            .is_ok(),
            "without an explicit allocation limit decoding succeeds"
        );

        buf.clear();
        let pack = pack_at(SMALL_PACK).with_alloc_limit_bytes(Some(0));
        let entry = pack.entry(entry_offset).expect("valid object type");
        let err = pack
            .decode_entry(
                entry,
                &mut buf,
                &mut inflate,
                &|_, _| Ok(None),
                &mut gix_odb::pack::cache::Never,
            )
            .expect_err("pack-controlled allocations larger than the configured limit are rejected");
        insta::assert_debug_snapshot!(err, "decode entry respects alloc limit bytes", @"Entry too large to fit in memory");
        assert_eq!(
            err.classify().find_map(|classification| match classification.class() {
                gix_error::Class::ResourceExhaustion(kind) => Some(kind),
                _ => None,
            }),
            Some(gix_error::ResourceExhaustionKind::AllocationLimit)
        );
    }

    #[test]
    fn iter() -> gix_testtools::TestResult {
        let pack = pack_at(SMALL_PACK);
        let it = pack.streaming_iter()?;
        assert_eq!(it.count(), pack.num_objects() as usize);
        Ok(())
    }
}

/// All hardcoded offsets are obtained via `git pack-verify --verbose  tests/fixtures/packs/pack-a2bf8e71d8c18879e499335762dd95119d93d9f1.idx`
mod decode_entry {
    use bstr::ByteSlice;
    use gix_error::Result;
    use gix_pack::{
        cache,
        data::{self, decode::entry::ResolvedBase},
    };

    use crate::{SMALL_PACK, data::file::pack_at, fixture_path, fixup};

    fn content_of(path: &str) -> Vec<u8> {
        fixup(std::fs::read(fixture_path(path)).expect("valid fixture"))
    }

    #[test]
    fn commit() {
        let buf = decode_entry_at_offset(1968);
        assert_eq!(buf.len(), 187);
        assert_eq!(
            buf.capacity(),
            187,
            "for undeltified objects, there is no change in allocation or resizing"
        );
    }

    #[test]
    fn blob_ofs_delta_two_links() {
        let buf = decode_entry_at_offset(3033);
        assert_eq!(buf.len(), 173, "buffer length is the actual object size");
        assert_eq!(
            buf.capacity(),
            2381,
            "capacity is much higher as we allocate everything into a single, bigger, reusable buffer, which depends on base sizes"
        );
        assert_eq!(
            buf.as_bstr(),
            content_of("objects/b8aa61be84b78d7fcff788e8d844406cc97132bf.txt").as_bstr()
        );
    }

    #[test]
    fn blob_ofs_delta_single_link() {
        let buf = decode_entry_at_offset(3569);
        assert_eq!(buf.len(), 1163, "buffer length is the actual object size");
        assert_eq!(
            buf.capacity(),
            2398,
            "capacity is much higher as we allocate everything into a single, bigger, reusable buffer, which depends on base sizes"
        );
        assert_eq!(
            buf.as_bstr(),
            content_of("objects/f139391424a8c623adadf2388caec73e5e90865b.txt").as_bstr()
        );
    }

    #[test]
    fn intermediate_cache_reuses_a_shared_base_without_changing_sibling_bytes() -> gix_testtools::TestResult {
        // Native verify-pack identifies offsets 3033 and 3094 as two-link
        // siblings whose shared, one-link base is at offset 3009.
        for retain_intermediates in [false, true] {
            let pack = pack_at(SMALL_PACK);
            let mut cache = cache::lru::MemoryCappedHashmap::new(16 * 1024);
            if retain_intermediates {
                cache = cache.with_intermediate_objects();
            }
            let mut buffer = Vec::new();
            let mut inflate = Default::default();
            for (offset, expected_id, expected_deltas) in [
                (3033, "b8aa61be84b78d7fcff788e8d844406cc97132bf", 2),
                (
                    3094,
                    "56e9f55deff1ffc20cf0cc66b866cba6b6886577",
                    if retain_intermediates { 1 } else { 2 },
                ),
            ] {
                let outcome = pack.decode_entry(
                    pack.entry(offset)?,
                    &mut buffer,
                    &mut inflate,
                    &|_, _| Ok(None),
                    &mut cache,
                )?;
                assert_eq!(
                    gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, &buffer)?,
                    gix_hash::ObjectId::from_hex(expected_id.as_bytes())?,
                    "cached bases must preserve the object identity reported by native verify-pack"
                );
                assert_eq!(
                    outcome.num_deltas, expected_deltas,
                    "the second sibling must reuse the decoded shared base only in the enabled mode"
                );
                assert_eq!(outcome.kind, gix_object::Kind::Blob);
            }
        }
        Ok(())
    }

    #[test]
    fn borrowed_cached_base_avoids_a_second_full_object_buffer() -> gix_testtools::TestResult {
        // Native verify-pack identifies the one-link base at 3009 and its
        // siblings at 3033 and 3094. Native cat-file reports an 847-byte base.
        // A 1024-byte cap fits either base or result,
        // but not the original decoder's two full-size working buffers.
        for (borrow_cached_base, append_output, limit) in [
            (false, false, 1024),
            (true, false, 0),
            (true, false, 846),
            (true, false, 847),
            (true, false, 1024),
            (true, true, 0),
            (true, true, 846),
            (true, true, 847),
            (true, true, 1024),
        ] {
            for (offset, expected_id) in [
                (3033, "b8aa61be84b78d7fcff788e8d844406cc97132bf"),
                (3094, "56e9f55deff1ffc20cf0cc66b866cba6b6886577"),
            ] {
                let pack = pack_at(SMALL_PACK);
                let mut lru = cache::lru::MemoryCappedHashmap::new(16 * 1024);
                if borrow_cached_base {
                    lru = lru.with_borrowed_objects();
                }
                if append_output {
                    lru = lru.with_appended_output();
                }
                let mut cache: Box<dyn cache::DecodeEntry> = Box::new(lru);
                let mut base = Vec::new();
                let mut inflate = Default::default();
                pack.decode_entry(pack.entry(3009)?, &mut base, &mut inflate, &|_, _| Ok(None), &mut cache)?;
                assert_eq!(
                    gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, &base)?,
                    gix_hash::ObjectId::from_hex(b"e0dc2c78d3bd3d14d20906ad7905a32f0f0918a8")?,
                    "the unlimited decode must seed the native shared base"
                );

                assert_eq!(base.len(), 847, "the seeded base has the native expanded size");
                let pack = pack.with_alloc_limit_bytes(Some(limit));
                let mut buffer = Vec::new();
                let result = pack.decode_entry(
                    pack.entry(offset)?,
                    &mut buffer,
                    &mut inflate,
                    &|_, _| Ok(None),
                    &mut cache,
                );
                if !borrow_cached_base || limit < 847 {
                    let err = result.expect_err("the decoder enforces both base-size and working-buffer caps");
                    assert_eq!(
                        err.classify().find_map(|classification| match classification.class() {
                            gix_error::Class::ResourceExhaustion(kind) => Some(kind),
                            _ => None,
                        }),
                        Some(gix_error::ResourceExhaustionKind::AllocationLimit)
                    );
                    continue;
                }
                let outcome = result?;
                assert_eq!(outcome.num_deltas, 1, "the cached base cuts both sibling chains short");
                assert_eq!(outcome.kind, gix_object::Kind::Blob);
                assert!(
                    buffer.capacity() <= limit,
                    "the decoder respects the configured allocation cap"
                );
                assert_eq!(
                    gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &buffer)?,
                    gix_hash::ObjectId::from_hex(expected_id.as_bytes())?,
                    "borrowing the base must retain the native object identity"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn borrowed_cached_base_rejects_corruption_before_caching_the_result() -> gix_testtools::TestResult {
        let native = pack_at(SMALL_PACK);
        let entry = native.entry(3033)?;
        let mut original_delta = vec![0; entry.decompressed_size as usize];
        native.decompress_entry(&entry, &mut Default::default(), &mut original_delta)?;

        for (scenario, append_output) in [
            "short-stream",
            "long-stream",
            "wrong-base",
            "truncated-copy",
            "combined-cap",
            "missing-base-size",
            "missing-result-size",
        ]
        .into_iter()
        .flat_map(|scenario| [false, true].map(move |append_output| (scenario, append_output)))
        {
            let mut delta = original_delta.clone();
            let mut declared_size = delta.len() as u64;
            let mut limit = 1024;
            let expected_message = match scenario {
                "short-stream" => {
                    declared_size += 1;
                    "decompressed size does not match entry header"
                }
                "long-stream" => {
                    declared_size -= 1;
                    "decompressed size does not match entry header"
                }
                "wrong-base" => {
                    delta[0] ^= 1;
                    "delta base size does not match base object size"
                }
                "truncated-copy" => {
                    delta.pop();
                    declared_size -= 1;
                    "delta copy instruction is truncated"
                }
                "combined-cap" => {
                    // The native base is 847 bytes. Declaring an equally large
                    // result fits individually, but result + instructions do not.
                    delta.copy_within(..2, 2);
                    limit = 847;
                    "Entry too large to fit in memory"
                }
                "missing-base-size" => {
                    delta.clear();
                    declared_size = 0;
                    "delta header size is truncated"
                }
                "missing-result-size" => {
                    delta.truncate(2);
                    declared_size = 2;
                    "delta header size is truncated"
                }
                _ => unreachable!("all scenarios are enumerated above"),
            };
            let damaged = pack_with_replaced_sibling_delta(&delta, declared_size)?;
            let mut lru = cache::lru::MemoryCappedHashmap::new(16 * 1024).with_borrowed_objects();
            if append_output {
                lru = lru.with_appended_output();
            }
            let mut cache: Box<dyn cache::DecodeEntry> = Box::new(lru);
            let mut buffer = Vec::new();
            let mut inflate = Default::default();
            damaged.decode_entry(
                damaged.entry(3009)?,
                &mut buffer,
                &mut inflate,
                &|_, _| Ok(None),
                &mut cache,
            )?;
            let damaged = damaged.with_alloc_limit_bytes(Some(limit));
            let mut result = vec![0xa5; limit];
            let err = damaged
                .decode_entry(
                    damaged.entry(3033)?,
                    &mut result,
                    &mut inflate,
                    &|_, _| Ok(None),
                    &mut cache,
                )
                .expect_err("malformed deltas must not produce a cached object");
            assert!(err.to_string().contains(expected_message), "{scenario}: {err}");
            assert!(
                result.capacity() <= limit,
                "{scenario}: no allocation may bypass the cap"
            );
            if scenario == "combined-cap" {
                assert!(err.classify().any(|classification| classification.class()
                    == gix_error::Class::ResourceExhaustion(gix_error::ResourceExhaustionKind::AllocationLimit)));
            } else {
                assert!(
                    err.is_corrupted(),
                    "{scenario}: the malformed delta remains classified as corruption"
                );
            }

            let clean = data::File::from_data(
                std::fs::read(fixture_path(SMALL_PACK))?,
                "borrowed-base-validation.pack".into(),
                gix_hash::Kind::Sha1,
            )?
            .with_alloc_limit_bytes(Some(1024));
            let outcome = clean.decode_entry(
                clean.entry(3033)?,
                &mut result,
                &mut inflate,
                &|_, _| Ok(None),
                &mut cache,
            )?;
            assert_eq!(
                outcome.num_deltas, 1,
                "{scenario}: an error must not cache the target entry"
            );
            assert_eq!(
                gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &result)?,
                gix_hash::ObjectId::from_hex(b"b8aa61be84b78d7fcff788e8d844406cc97132bf")?,
                "{scenario}: the same cache remains usable for a valid native object"
            );
        }
        Ok(())
    }

    #[test]
    fn reused_buffers_preserve_native_siblings_and_weighted_lru_boundaries() -> gix_testtools::TestResult {
        let base_id = "e0dc2c78d3bd3d14d20906ad7905a32f0f0918a8";
        let left_id = "b8aa61be84b78d7fcff788e8d844406cc97132bf";
        let right_id = "56e9f55deff1ffc20cf0cc66b866cba6b6886577";
        // Native expanded sizes are base=847, left=173 and right=302.
        // Base + right + two key weights fit at 1151 bytes, but not 1150.
        for reuse_buffers in [false, true] {
            for limit in [1150, 1151] {
                let pack = pack_at(SMALL_PACK);
                let mut cache = cache::lru::MemoryCappedHashmap::new(limit).with_borrowed_objects();
                if reuse_buffers {
                    cache = cache.with_reused_buffers();
                }
                let deltas = if limit == 1150 { [2, 0, 0] } else { [1, 1, 1] };
                let mut buffer = Vec::new();
                let mut inflate = Default::default();
                for (offset, expected_id, expected_deltas) in [
                    (3009, base_id, 1),
                    (3033, left_id, 1),
                    (3094, right_id, 1),
                    (3033, left_id, deltas[0]),
                    (3094, right_id, deltas[1]),
                    (3033, left_id, deltas[2]),
                ] {
                    let outcome = pack.decode_entry(
                        pack.entry(offset)?,
                        &mut buffer,
                        &mut inflate,
                        &|_, _| Ok(None),
                        &mut cache,
                    )?;
                    assert_eq!(outcome.kind, gix_object::Kind::Blob);
                    assert_eq!(
                        outcome.num_deltas, expected_deltas,
                        "buffer reuse must preserve weighted-LRU residency at the key-plus-data boundary"
                    );
                    assert_eq!(
                        gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &buffer)?,
                        gix_hash::ObjectId::from_hex(expected_id.as_bytes())?,
                        "eviction and buffer reuse must retain the native sibling object identities"
                    );
                }
            }
        }
        Ok(())
    }

    fn pack_with_replaced_sibling_delta(
        delta: &[u8],
        declared_size: u64,
    ) -> gix_testtools::TestResult<data::File<Vec<u8>>> {
        use std::io::Write;

        let native = pack_at(SMALL_PACK);
        let entry = native.entry(3033)?;
        let mut original = vec![0; entry.decompressed_size as usize];
        let compressed_size = native.decompress_entry(&entry, &mut Default::default(), &mut original)?;
        let mut replacement = Vec::new();
        entry.header.write_to(declared_size, &mut replacement)?;
        let mut deflate = gix_zlib::stream::deflate::Write::new(replacement, gix_zlib::Compression::BEST_SPEED);
        deflate.write_all(delta)?;
        deflate.flush()?;
        let mut bytes = std::fs::read(fixture_path(SMALL_PACK))?;
        bytes.splice(3033..entry.data_offset as usize + compressed_size, deflate.into_inner());
        Ok(data::File::from_data(
            bytes,
            "borrowed-base-validation.pack".into(),
            gix_hash::Kind::Sha1,
        )?)
    }

    fn decode_entry_at_offset(offset: u64) -> Vec<u8> {
        fn resolve_with_panic(_oid: &gix_hash::oid, _out: &mut Vec<u8>) -> Result<Option<ResolvedBase>> {
            panic!("should not want to resolve an id here")
        }

        let p = pack_at(SMALL_PACK);
        let entry = p.entry(offset).expect("valid object type");
        let mut buf = Vec::new();
        p.decode_entry(
            entry,
            &mut buf,
            &mut Default::default(),
            &resolve_with_panic,
            &mut cache::Never,
        )
        .expect("valid offset provides valid entry");
        buf
    }
}

/// All hardcoded offsets are obtained via `git pack-verify --verbose  tests/fixtures/packs/pack-a2bf8e71d8c18879e499335762dd95119d93d9f1.idx`
mod resolve_header {
    use gix_error::Result;

    use crate::{SMALL_PACK, data::file::pack_at};

    #[test]
    fn commit() {
        let out = resolve_header_at_offset(1968);
        assert_eq!(out.kind, gix_object::Kind::Commit);
        assert_eq!(out.object_size, 187);
        assert_eq!(out.num_deltas, 0);
    }

    #[test]
    fn blob_ofs_delta_two_links() {
        let out = resolve_header_at_offset(3033);
        assert_eq!(out.kind, gix_object::Kind::Blob);
        assert_eq!(out.object_size, 173);
        assert_eq!(out.num_deltas, 2);
    }

    #[test]
    fn blob_ofs_delta_single_link() {
        let out = resolve_header_at_offset(3569);
        assert_eq!(out.kind, gix_object::Kind::Blob);
        assert_eq!(out.object_size, 1163);
        assert_eq!(out.num_deltas, 1);
    }

    #[test]
    fn tree() {
        let out = resolve_header_at_offset(2097);
        assert_eq!(out.kind, gix_object::Kind::Tree);
        assert_eq!(out.object_size, 34);
        assert_eq!(out.num_deltas, 0);
    }

    fn resolve_header_at_offset(offset: u64) -> gix_pack::data::decode::header::Outcome {
        fn resolve_with_panic(_oid: &gix_hash::oid) -> Result<Option<gix_pack::data::decode::header::ResolvedBase>> {
            panic!("should not want to resolve an id here")
        }

        let p = pack_at(SMALL_PACK);
        let entry = p.entry(offset).expect("valid object type");
        p.decode_header(entry, &mut Default::default(), &resolve_with_panic)
            .expect("valid offset provides valid entry")
    }
}

mod decompress_entry {
    use gix_object::bstr::ByteSlice;

    use crate::{SMALL_PACK, data::file::pack_at};

    #[test]
    fn commit() {
        let buf = decompress_entry_at_offset(1968);
        assert_eq!(buf.as_bstr(), b"tree e90926b07092bccb7bf7da445fae6ffdfacf3eae\nauthor Sebastian Thiel <byronimo@gmail.com> 1286529993 +0200\ncommitter Sebastian Thiel <byronimo@gmail.com> 1286529993 +0200\n\nInitial commit\n".as_bstr());
        assert_eq!(buf.len(), 187);
    }

    #[test]
    fn blob() {
        let buf = decompress_entry_at_offset(2142);
        assert_eq!(
            buf.as_bstr(),
            b"GitPython is a python library used to interact with Git repositories.\n\nHi there\n\nHello Other\n"
                .as_bstr()
        );
        assert_eq!(buf.len(), 93);
    }

    #[test]
    fn blob_with_two_chain_links() {
        let buf = decompress_entry_at_offset(3033);
        assert_eq!(buf.len(), 6, "it decompresses delta objects, but won't resolve them");
    }

    #[test]
    fn tree() {
        let buf = decompress_entry_at_offset(2097);
        assert_eq!(buf[..13].as_bstr(), b"100644 README".as_bstr());
        assert_eq!(buf.len(), 34);
        assert_eq!(
            buf.capacity(),
            34,
            "capacity must be controlled by the caller to be big enough"
        );
    }

    #[test]
    fn caller_provided_buffer_ignores_alloc_limit() {
        let p = pack_at(SMALL_PACK).with_alloc_limit_bytes(Some(0));
        let entry = p.entry(1968).expect("valid object type");
        let mut buf = vec![0; entry.decompressed_size as usize];

        p.decompress_entry(&entry, &mut Default::default(), &mut buf)
            .expect("caller-provided buffers should not be rejected by allocation limits");

        assert_eq!(
            buf.len(),
            187,
            "the buffer is larger than the alloc limit and that's alright"
        );
    }

    #[test]
    fn caller_provided_buffer_must_be_large_enough() {
        let p = pack_at(SMALL_PACK);
        let entry = p.entry(1968).expect("valid object type");
        let mut buf = vec![0; entry.decompressed_size as usize - 1];

        let err = p
            .decompress_entry(&entry, &mut Default::default(), &mut buf)
            .expect_err("an undersized caller-provided buffer is invalid input");
        insta::assert_debug_snapshot!(err, "caller provided buffer must be large enough", @"Output buffer is too small for the decompressed entry");
        assert!(err.is_validation());
    }

    fn decompress_entry_at_offset(offset: u64) -> Vec<u8> {
        let p = pack_at(SMALL_PACK);
        let entry = p.entry(offset).expect("valid object type");

        let size = entry.decompressed_size as usize;
        let mut buf = vec![0; size];
        p.decompress_entry(&entry, &mut Default::default(), &mut buf)
            .expect("valid offset");

        buf.resize(entry.decompressed_size as usize, 0);
        buf
    }
}
