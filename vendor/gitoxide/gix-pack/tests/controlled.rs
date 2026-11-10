//! Synthetic regression tests for request-local pack operations.
use std::{
    cell::Cell,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

use gix_error::Result;
use gix_pack::{
    cache::{self, DecodeEntry},
    data::{self, entry::Header},
};

const LARGE: usize = 4 * 1024 * 1024;

fn limits() -> data::decode::Limits {
    data::decode::Limits {
        max_delta_depth: 128,
        max_delta_bytes: 8 * 1024 * 1024,
    }
}

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut writer = gix_zlib::stream::deflate::Write::new(Vec::new(), gix_zlib::Compression::DEFAULT);
    writer.write_all(bytes).unwrap();
    writer.flush().unwrap();
    writer.into_inner()
}

fn size(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let low = (value & 127) as u8;
        value >>= 7;
        out.push(low | if value != 0 { 128 } else { 0 });
        if value == 0 {
            break;
        }
    }
}

fn copy_delta(len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    size(len, &mut out);
    size(len, &mut out);
    out.extend_from_slice(&[0xf0, len as u8, (len >> 8) as u8, (len >> 16) as u8]);
    out
}

fn append(pack: &mut Vec<u8>, header: Header, bytes: &[u8]) -> u64 {
    let offset = pack.len() as u64;
    header.write_to(bytes.len() as u64, pack).unwrap();
    pack.extend_from_slice(&compress(bytes));
    offset
}

fn chain(len: usize, depth: usize) -> (data::File<Vec<u8>>, Vec<u64>) {
    let mut bytes = data::header::encode(data::Version::V2, (depth + 1) as u32).to_vec();
    let mut offsets = vec![append(&mut bytes, Header::Blob, &vec![b'A'; len])];
    for _ in 0..depth {
        let distance = bytes.len() as u64 - offsets.last().unwrap();
        offsets.push(append(
            &mut bytes,
            Header::OfsDelta {
                base_distance: distance,
            },
            &copy_delta(len),
        ));
    }
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(&bytes);
    bytes.extend_from_slice(hasher.try_finalize().unwrap().as_slice());
    let pack = data::File::from_data(bytes, PathBuf::from("synthetic.pack"), gix_hash::Kind::Sha1)
        .unwrap()
        .with_alloc_limit_bytes(Some(32 * 1024 * 1024));
    (pack, offsets)
}

fn cancellation<'a>(polls: &'a Cell<usize>, flag: &'a AtomicBool, after: usize) -> impl Fn() -> Result + 'a {
    move || {
        polls.set(polls.get() + 1);
        if polls.get() >= after {
            flag.store(true, Ordering::Release);
        }
        if flag.load(Ordering::Acquire) {
            Err(gix_error::cancelled("request cancelled").into())
        } else {
            Ok(())
        }
    }
}

#[test]
fn enormous_inflate_stops_inside_one_stream_and_rejects_size_mismatches() -> Result {
    let source = vec![b'A'; LARGE];
    let encoded = compress(&source);
    let mut output = vec![0; LARGE];
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    let mut inflate = gix_zlib::Inflate::default();
    let err = inflate
        .once_controlled(&encoded, &mut output, &cancellation(&polls, &flag, 4))
        .unwrap_err();
    assert!(err.is_cancelled());
    assert!(output[..32 * 1024].iter().all(|byte| *byte == b'A'));
    assert_eq!(
        output[LARGE - 1],
        0,
        "the entire huge object was not inflated before acknowledging cancellation"
    );
    let (status, consumed, written) = inflate.once_controlled(&encoded, &mut output, &|| Ok(()))?;
    assert_eq!(status, gix_zlib::Status::StreamEnd);
    assert_eq!((consumed, written), (encoded.len(), LARGE));
    assert_eq!(output, source);
    for len in [LARGE - 1, LARGE + 1] {
        assert!(
            inflate
                .once_controlled(&encoded, &mut vec![0; len], &|| Ok(()))
                .unwrap_err()
                .is_corrupted()
        );
    }
    assert!(
        inflate
            .once_controlled(&encoded[..encoded.len() - 1], &mut output, &|| Ok(()))
            .unwrap_err()
            .is_corrupted()
    );
    let mut empty = [];
    inflate.once_controlled(&compress(b""), &mut empty, &|| Ok(()))?;
    Ok(())
}

#[test]
fn another_thread_can_cancel_at_a_deterministic_inner_inflate_gate() -> Result {
    use std::{sync::mpsc, time::Duration};
    let encoded = compress(&vec![b'A'; LARGE]);
    let flag = AtomicBool::new(false);
    let (reached_tx, reached_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    std::thread::scope(|scope| {
        let flag = &flag;
        let worker = scope.spawn(move || {
            let polls = Cell::new(0);
            let check = || {
                polls.set(polls.get() + 1);
                if polls.get() == 4 {
                    reached_tx.send(()).expect("the controller is waiting at the gate");
                    resume_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the controller releases the worker");
                }
                if flag.load(Ordering::Acquire) {
                    Err(gix_error::cancelled("request cancelled").into())
                } else {
                    Ok(())
                }
            };
            let mut out = vec![0; LARGE];
            let result = gix_zlib::Inflate::default().once_controlled(&encoded, &mut out, &check);
            (result, out)
        });
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("one enormous stream reaches an inner poll");
        flag.store(true, Ordering::Release);
        resume_tx
            .send(())
            .expect("worker is still alive until cancellation acknowledgment");
        let (result, out) = worker.join().expect("worker did not panic");
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(out[LARGE - 1], 0);
    });
    Ok(())
}

#[test]
fn cache_optimization_options_are_instance_local() {
    let mut selected = cache::lru::MemoryCappedHashmap::with_options(
        1024,
        cache::lru::Options {
            intermediate_objects: true,
            borrowed_objects: true,
            reused_buffers: true,
            appended_output: true,
        },
    );
    let mut ordinary = cache::lru::MemoryCappedHashmap::new(1024);
    selected.put(0, 12, b"selected", gix_object::Kind::Blob, 9);
    ordinary.put(0, 12, b"ordinary", gix_object::Kind::Blob, 9);
    assert!(selected.append_output());
    assert!(selected.cache_intermediate_objects());
    assert_eq!(
        selected.get_borrowed(0, 12).expect("borrowing is enabled").1,
        b"selected"
    );
    assert!(!ordinary.append_output());
    assert!(!ordinary.cache_intermediate_objects());
    assert!(ordinary.get_borrowed(0, 12).is_none());
}

#[test]
fn every_typed_optimization_combination_preserves_decoded_bytes() -> Result {
    let (pack, offsets) = chain(4096, 4);
    for bits in 0..16 {
        let mut cache = cache::lru::MemoryCappedHashmap::with_options(
            12 * 1024,
            cache::lru::Options {
                intermediate_objects: bits & 1 != 0,
                borrowed_objects: bits & 2 != 0,
                reused_buffers: bits & 4 != 0,
                appended_output: bits & 8 != 0,
            },
        );
        let mut output = Vec::new();
        let mut inflate = gix_zlib::Inflate::default();
        for offset in [offsets[2], offsets[3], offsets[4], offsets[4]] {
            let result = pack.decode_entry_controlled(
                pack.entry(offset)?,
                &mut output,
                &mut inflate,
                &|_, _| Ok(None),
                &mut cache,
                limits(),
                &|| Ok(()),
            )?;
            assert_eq!(result.object_size, 4096, "options {bits:04b}");
            assert_eq!(output, vec![b'A'; 4096], "options {bits:04b}");
        }
    }
    Ok(())
}

#[test]
fn full_object_cancellation_clears_output_and_does_not_poison_an_independent_read() -> Result {
    let (pack, offsets) = chain(LARGE, 0);
    let mut output = vec![0; LARGE];
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    let mut inflate = gix_zlib::Inflate::default();
    let err = pack
        .decode_entry_controlled(
            pack.entry(offsets[0])?,
            &mut output,
            &mut inflate,
            &|_, _| Ok(None),
            &mut cache::Never,
            limits(),
            &cancellation(&polls, &flag, 8),
        )
        .unwrap_err();
    assert!(err.is_cancelled());
    assert!(output.is_empty());
    let outcome = pack.decode_entry_controlled(
        pack.entry(offsets[0])?,
        &mut output,
        &mut inflate,
        &|_, _| Ok(None),
        &mut cache::Never,
        limits(),
        &|| Ok(()),
    )?;
    assert_eq!(outcome.object_size, LARGE as u64);
    assert_eq!(output, vec![b'A'; LARGE]);
    Ok(())
}

struct RecordingCache {
    inner: cache::lru::MemoryCappedHashmap,
    storing: Cell<bool>,
}

impl DecodeEntry for RecordingCache {
    fn append_output(&self) -> bool {
        self.inner.append_output()
    }
    fn cache_intermediate_objects(&self) -> bool {
        self.inner.cache_intermediate_objects()
    }
    fn get_borrowed(&mut self, pack: u32, offset: u64) -> Option<(gix_object::Kind, &[u8])> {
        self.inner.get_borrowed(pack, offset)
    }
    fn put(&mut self, pack: u32, offset: u64, bytes: &[u8], kind: gix_object::Kind, compressed: usize) {
        self.inner.put(pack, offset, bytes, kind, compressed);
    }
    fn get(&mut self, pack: u32, offset: u64, out: &mut Vec<u8>) -> Option<(gix_object::Kind, usize)> {
        self.inner.get(pack, offset, out)
    }
    fn get_controlled(
        &mut self,
        pack: u32,
        offset: u64,
        out: &mut Vec<u8>,
        limit: usize,
        check: &dyn Fn() -> Result,
    ) -> Result<Option<(gix_object::Kind, usize)>> {
        self.inner.get_controlled(pack, offset, out, limit, check)
    }
    fn put_controlled(
        &mut self,
        pack: u32,
        offset: u64,
        bytes: &[u8],
        kind: gix_object::Kind,
        compressed: usize,
        check: &dyn Fn() -> Result,
    ) -> Result {
        self.storing.set(true);
        self.inner.put_controlled(pack, offset, bytes, kind, compressed, check)
    }
}

#[test]
fn cancellation_inside_one_large_delta_copy_with_a_borrowed_cached_base() -> Result {
    let (pack, offsets) = chain(LARGE, 2);
    for appended_output in [false, true] {
        let mut cache = RecordingCache {
            inner: cache::lru::MemoryCappedHashmap::with_options(
                16 * 1024 * 1024,
                cache::lru::Options {
                    intermediate_objects: true,
                    borrowed_objects: true,
                    reused_buffers: true,
                    appended_output,
                },
            ),
            storing: Cell::new(false),
        };
        let mut output = Vec::with_capacity(12 * 1024 * 1024);
        let mut inflate = gix_zlib::Inflate::default();
        pack.decode_entry_controlled(
            pack.entry(offsets[1])?,
            &mut output,
            &mut inflate,
            &|_, _| Ok(None),
            &mut cache,
            limits(),
            &|| Ok(()),
        )?;
        cache.storing.set(false);
        let polls = Cell::new(0);
        let flag = AtomicBool::new(false);
        // Appended output has no full-size zero-fill before delta application.
        // The slice-output path additionally polls during initialization.
        let budget = if appended_output { 32 } else { 32 + LARGE / (32 * 1024) };
        let err = pack
            .decode_entry_controlled(
                pack.entry(offsets[2])?,
                &mut output,
                &mut inflate,
                &|_, _| Ok(None),
                &mut cache,
                limits(),
                &cancellation(&polls, &flag, budget),
            )
            .unwrap_err();
        assert!(err.is_cancelled());
        assert!(output.is_empty());
        assert!(
            !cache.storing.get(),
            "stop must be acknowledged during delta work, before publishing it to cache"
        );
        let outcome = pack.decode_entry_controlled(
            pack.entry(offsets[2])?,
            &mut output,
            &mut inflate,
            &|_, _| Ok(None),
            &mut cache,
            limits(),
            &|| Ok(()),
        )?;
        assert_eq!(outcome.object_size, LARGE as u64);
        assert_eq!(output, vec![b'A'; LARGE]);
    }
    Ok(())
}

#[test]
fn cached_delta_reports_object_length_not_instruction_length() -> Result {
    let (pack, offsets) = chain(1024, 3);
    let mut cache = cache::lru::MemoryCappedHashmap::new(64 * 1024).with_intermediate_objects();
    let mut output = Vec::new();
    let mut inflate = gix_zlib::Inflate::default();
    for _ in 0..2 {
        let outcome = pack.decode_entry(
            pack.entry(offsets[3])?,
            &mut output,
            &mut inflate,
            &|_, _| Ok(None),
            &mut cache,
        )?;
        assert_eq!(
            outcome.object_size, 1024,
            "a cached resolved object is not its small delta stream"
        );
        assert_eq!(output, vec![b'A'; 1024]);
    }
    Ok(())
}

#[test]
fn delta_chain_limits_and_cross_pack_resolver_errors_are_not_success() -> Result {
    let (pack, offsets) = chain(1024, 32);
    let mut output = Vec::new();
    let mut inflate = gix_zlib::Inflate::default();
    let tiny = data::decode::Limits {
        max_delta_depth: 8,
        max_delta_bytes: 1024,
    };
    assert!(
        pack.decode_entry_controlled(
            pack.entry(offsets[32])?,
            &mut output,
            &mut inflate,
            &|_, _| Ok(None),
            &mut cache::Never,
            tiny,
            &|| Ok(())
        )
        .is_err()
    );
    assert!(output.is_empty());
    let mut entry = pack.entry(offsets[1])?;
    entry.header = Header::RefDelta {
        base_id: gix_hash::ObjectId::empty_blob(gix_hash::Kind::Sha1),
    };
    let err = pack
        .decode_entry_controlled(
            entry,
            &mut output,
            &mut inflate,
            &|_, _| Err(gix_error::cancelled("cross-pack base request cancelled").into()),
            &mut cache::Never,
            limits(),
            &|| Ok(()),
        )
        .unwrap_err();
    assert!(err.is_cancelled());
    assert!(err.to_string().contains("cross-pack base request"));
    assert!(output.is_empty());
    Ok(())
}

#[test]
fn controlled_cache_copy_stops_between_chunks_and_enforces_destination_limit() -> Result {
    let mut cache = cache::lru::MemoryCappedHashmap::new(8 * 1024 * 1024);
    cache.put(1, 12, &vec![b'A'; LARGE], gix_object::Kind::Blob, 77);
    let mut output = Vec::new();
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    assert!(
        cache
            .get_controlled(1, 12, &mut output, LARGE, &cancellation(&polls, &flag, 4))
            .unwrap_err()
            .is_cancelled()
    );
    assert!(
        output.len() <= 2 * 32 * 1024,
        "cache copying cannot finish before cancellation"
    );
    output.clear();
    assert!(cache.get_controlled(1, 12, &mut output, 1024, &|| Ok(())).is_err());
    assert!(output.is_empty());
    assert_eq!(
        cache.get_controlled(1, 12, &mut output, LARGE, &|| Ok(()))?,
        Some((gix_object::Kind::Blob, 77))
    );
    assert_eq!(output, vec![b'A'; LARGE]);
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    let err = cache
        .put_controlled(
            1,
            13,
            &vec![b'B'; LARGE],
            gix_object::Kind::Blob,
            88,
            &cancellation(&polls, &flag, 4),
        )
        .expect_err("cancelled store cannot publish a partial cache entry");
    assert!(err.is_cancelled());
    assert!(cache.get(1, 13, &mut Vec::new()).is_none());
    Ok(())
}

#[cfg(feature = "generate")]
#[test]
fn compression_and_output_cancel_without_a_success_digest() -> Result {
    use data::output::{self, count::PackLocation};
    let count = output::Count {
        id: gix_hash::ObjectId::empty_blob(gix_hash::Kind::Sha1),
        entry_pack_location: PackLocation::NotLookedUp,
    };
    let bytes = vec![b'A'; LARGE];
    let object = gix_object::Data {
        kind: gix_object::Kind::Blob,
        object_hash: gix_hash::Kind::Sha1,
        data: &bytes,
    };
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    assert!(
        output::Entry::from_data_controlled(
            &count,
            &object,
            gix_zlib::Compression::DEFAULT,
            LARGE + 4096,
            &cancellation(&polls, &flag, 8)
        )
        .unwrap_err()
        .is_cancelled()
    );
    assert!(
        output::Entry::from_data_controlled(&count, &object, gix_zlib::Compression::NONE, 1024, &|| Ok(())).is_err()
    );
    let entry =
        output::Entry::from_data_controlled(&count, &object, gix_zlib::Compression::NONE, LARGE + 4096, &|| Ok(()))?;
    let mut writer = output::bytes::FromEntriesIter::new(
        std::iter::once(Ok(vec![entry])),
        Vec::new(),
        1,
        data::Version::V2,
        gix_hash::Kind::Sha1,
    );
    let polls = Cell::new(0);
    let flag = AtomicBool::new(false);
    assert!(
        writer
            .next_controlled(&cancellation(&polls, &flag, 32))
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert!(writer.digest().is_none());
    assert!(
        writer.next_controlled(&|| Ok(())).is_none(),
        "failure is terminal even after the request is reset"
    );
    let emitted = writer.into_write().len();
    assert!(
        emitted > 32 * 1024,
        "the cancellation gate is inside payload copying, after the pack and entry headers"
    );
    assert!(
        emitted < LARGE / 2,
        "cancellation interrupts one large entry, not just the next entry"
    );
    Ok(())
}

#[cfg(feature = "generate")]
#[test]
fn interrupted_sink_retries_recheck_request_control_even_while_writing_the_header() -> Result {
    use data::output;
    struct Sink<'a> {
        flag: &'a AtomicBool,
        calls: &'a Cell<usize>,
    }
    impl Write for Sink<'_> {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.calls.set(self.calls.get() + 1);
            self.flag.store(true, Ordering::Release);
            if self.calls.get() > 4 {
                return Err(std::io::Error::other("uncontrolled write retry"));
            }
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let flag = AtomicBool::new(false);
    let calls = Cell::new(0);
    let check = || {
        if flag.load(Ordering::Acquire) {
            Err(gix_error::cancelled("sink request cancelled").into())
        } else {
            Ok(())
        }
    };
    let mut writer = output::bytes::FromEntriesIter::new_controlled(
        std::iter::empty::<Result<Vec<output::Entry>>>(),
        Sink {
            flag: &flag,
            calls: &calls,
        },
        0,
        data::Version::V2,
        gix_hash::Kind::Sha1,
        8,
        &check,
    )?;
    let err = writer
        .next_controlled(&check)
        .expect("header write attempts the sink")
        .expect_err("cancellation interrupts write_all retrying Interrupted");
    assert!(err.is_cancelled());
    assert_eq!(calls.get(), 1, "no uncontrolled retry may bypass the request callback");
    assert!(writer.digest().is_none());
    assert!(writer.next_controlled(&|| Ok(())).is_none());
    Ok(())
}

#[cfg(feature = "generate")]
#[test]
fn controlled_output_round_trips_and_checks_entry_counts() -> Result {
    use data::output::{self, count::PackLocation};
    let count = output::Count {
        id: gix_hash::ObjectId::empty_blob(gix_hash::Kind::Sha1),
        entry_pack_location: PackLocation::NotLookedUp,
    };
    let object = gix_object::Data {
        kind: gix_object::Kind::Blob,
        object_hash: gix_hash::Kind::Sha1,
        data: b"payload\0with bytes\xff",
    };
    let entry = output::Entry::from_data_controlled(&count, &object, gix_zlib::Compression::DEFAULT, 1024, &|| Ok(()))?;
    let mut writer = output::bytes::FromEntriesIter::new_controlled(
        std::iter::once(Ok(vec![entry])),
        Vec::new(),
        1,
        data::Version::V2,
        gix_hash::Kind::Sha1,
        8,
        &|| Ok(()),
    )?;
    while let Some(result) = writer.next_controlled(&|| Ok(())) {
        result?;
    }
    let digest = writer.digest().expect("complete output has a checksum");
    let bytes = writer.into_write();
    assert_eq!(&bytes[bytes.len() - 20..], digest.as_slice());
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(&bytes[..bytes.len() - 20]);
    assert_eq!(hasher.try_finalize()?, digest);
    let pack = data::File::from_data(bytes, PathBuf::from("output.pack"), gix_hash::Kind::Sha1)?
        .with_alloc_limit_bytes(Some(1024));
    let mut output = Vec::new();
    pack.decode_entry_controlled(
        pack.entry(12)?,
        &mut output,
        &mut gix_zlib::Inflate::default(),
        &|_, _| Ok(None),
        &mut cache::Never,
        limits(),
        &|| Ok(()),
    )?;
    assert_eq!(output, object.data);
    let mut short = output::bytes::FromEntriesIter::new_controlled(
        std::iter::empty::<Result<Vec<output::Entry>>>(),
        Vec::new(),
        1,
        data::Version::V2,
        gix_hash::Kind::Sha1,
        8,
        &|| Ok(()),
    )?;
    assert!(
        short
            .next_controlled(&|| Ok(()))
            .expect("short input produces an error")
            .is_err()
    );
    assert!(short.digest().is_none());
    Ok(())
}
