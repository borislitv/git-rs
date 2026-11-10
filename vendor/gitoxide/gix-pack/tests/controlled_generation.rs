#![cfg(all(feature = "generate", feature = "streaming-input"))]

use std::{cell::Cell, collections::BTreeMap, path::PathBuf};

use gix_error::Result;
use gix_pack::{
    cache,
    data::{self, output},
};

struct Provider {
    objects: BTreeMap<gix_hash::ObjectId, Vec<u8>>,
    calls: Cell<usize>,
    fail_at: Option<usize>,
}

impl gix_object::Find for Provider {
    fn try_find<'a>(&self, id: &gix_hash::oid, out: &'a mut Vec<u8>) -> Result<Option<gix_object::Data<'a>>> {
        let calls = self.calls.get() + 1;
        self.calls.set(calls);
        if self.fail_at == Some(calls) {
            return Err(gix_error::permission_denied("provider rejected object").into());
        }
        let Some(data) = self.objects.get(id) else {
            return Ok(None);
        };
        out.clear();
        out.extend_from_slice(data);
        Ok(Some(gix_object::Data {
            kind: gix_object::Kind::Blob,
            object_hash: id.kind(),
            data: out,
        }))
    }
}

fn fixture() -> (Provider, Vec<output::Count>) {
    let mut objects = BTreeMap::new();
    let mut counts = Vec::new();
    for data in [
        b"first\0blob".as_slice(),
        b"second\xffblob".as_slice(),
        b"third blob".as_slice(),
    ] {
        let id =
            gix_object::compute_hash(gix_hash::Kind::Sha1, gix_object::Kind::Blob, data).expect("small synthetic hash");
        objects.insert(id, data.to_vec());
        counts.push(output::Count {
            id,
            entry_pack_location: output::count::PackLocation::NotLookedUp,
        });
    }
    (
        Provider {
            objects,
            calls: Cell::new(0),
            fail_at: None,
        },
        counts,
    )
}

#[test]
fn controlled_generation_is_lazy_and_encodes_every_requested_object() -> Result {
    let (provider, counts) = fixture();
    let input = output::entry::iter_from_counts_controlled(
        counts.clone(),
        &provider,
        gix_zlib::Compression::DEFAULT,
        1024,
        &|| Ok(()),
    );
    assert_eq!(
        provider.calls.get(),
        0,
        "no lookup, sort or nested worker is started during construction"
    );
    let mut writer = output::bytes::FromEntriesIter::new_controlled(
        input,
        Vec::new(),
        counts.len() as u32,
        data::Version::V2,
        gix_hash::Kind::Sha1,
        8,
        &|| Ok(()),
    )?;
    while let Some(result) = writer.next_controlled(&|| Ok(())) {
        result?;
    }
    assert!(writer.digest().is_some());
    assert_eq!(provider.calls.get(), 3);
    let bytes = writer.into_write();
    let file = data::File::from_data(bytes.as_slice(), PathBuf::new(), gix_hash::Kind::Sha1)?
        .with_alloc_limit_bytes(Some(4096));
    let entries = data::input::BytesToEntriesIter::new_from_header(
        bytes.as_slice(),
        data::input::Mode::Verify,
        data::input::EntryDataMode::KeepAndCrc32,
        gix_hash::Kind::Sha1,
    )?;
    let mut decoded = Vec::new();
    let mut actual = Vec::new();
    for entry in entries {
        let entry = entry?;
        file.decode_entry_controlled(
            file.entry(entry.pack_offset)?,
            &mut decoded,
            &mut gix_zlib::Inflate::default(),
            &|_, _| Ok(None),
            &mut cache::Never,
            data::decode::Limits {
                max_delta_depth: 8,
                max_delta_bytes: 4096,
            },
            &|| Ok(()),
        )?;
        actual.push(decoded.clone());
    }
    assert_eq!(
        actual,
        vec![
            b"first\0blob".to_vec(),
            b"second\xffblob".to_vec(),
            b"third blob".to_vec()
        ]
    );
    Ok(())
}

#[test]
fn provider_failure_terminates_generation_without_a_success_digest() -> Result {
    let (mut provider, counts) = fixture();
    provider.fail_at = Some(2);
    let input =
        output::entry::iter_from_counts_controlled(counts, &provider, gix_zlib::Compression::DEFAULT, 1024, &|| Ok(()));
    let mut writer = output::bytes::FromEntriesIter::new_controlled(
        input,
        Vec::new(),
        3,
        data::Version::V2,
        gix_hash::Kind::Sha1,
        8,
        &|| Ok(()),
    )?;
    writer.next_controlled(&|| Ok(())).expect("first object exists")?;
    let err = writer
        .next_controlled(&|| Ok(()))
        .expect("second lookup returns a failure")
        .expect_err("failure is not treated as absence");
    assert!(err.is_permission_denied());
    assert!(writer.digest().is_none());
    assert!(writer.next_controlled(&|| Ok(())).is_none());
    assert_eq!(provider.calls.get(), 2, "the third object is never read after failure");
    Ok(())
}

#[test]
fn pre_cancelled_generation_has_no_lookup_and_output_limits_are_terminal() {
    let (provider, counts) = fixture();
    let check = || Err(gix_error::cancelled("request cancelled").into());
    let mut input = output::entry::iter_from_counts_controlled(
        counts.clone(),
        &provider,
        gix_zlib::Compression::DEFAULT,
        1024,
        &check,
    );
    assert!(
        input
            .next()
            .expect("cancelled iteration returns its error")
            .expect_err("cancelled request cannot yield an entry")
            .is_cancelled()
    );
    assert!(input.next().is_none());
    assert_eq!(provider.calls.get(), 0);
    let mut bounded =
        output::entry::iter_from_counts_controlled(counts, &provider, gix_zlib::Compression::DEFAULT, 1, &|| Ok(()));
    assert!(
        bounded
            .next()
            .expect("limited iteration attempts the first object")
            .is_err()
    );
    assert!(bounded.next().is_none());
    assert_eq!(provider.calls.get(), 1);
}

#[test]
fn missing_requested_object_is_an_error_not_a_silently_shortened_pack() {
    let (mut provider, counts) = fixture();
    provider.objects.remove(&counts[0].id);
    let mut input =
        output::entry::iter_from_counts_controlled(counts, &provider, gix_zlib::Compression::DEFAULT, 1024, &|| Ok(()));
    assert!(
        input
            .next()
            .expect("missing object yields its error")
            .expect_err("missing object cannot be omitted from a successful pack")
            .is_not_found()
    );
    assert!(input.next().is_none());
    assert_eq!(provider.calls.get(), 1);
}
