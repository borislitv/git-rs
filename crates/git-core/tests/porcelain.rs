#[path = "support/blame_fixture.rs"]
mod fixture;

use fixture::{Fixture, control, render};
use git_rs_core::{BlameError, BlameOptions, BlameRequest, CapacityBudget, Porcelain, StopReason};

#[test]
fn exact_group_headers_repeated_fields_and_signed_zero_timezone() {
    let mut fixture = Fixture::new();
    let commit = fixture.commit(
        &[(b"file", b"one\ntwo\n", "100644")],
        &[],
        1,
        b"Subject\n",
        None,
    );
    let metadata = "author Author\nauthor-mail <author@example.invalid>\nauthor-time 1\nauthor-tz -0000\ncommitter Committer\ncommitter-mail <committer@example.invalid>\ncommitter-time 1\ncommitter-tz +0530\nsummary Subject\nfilename file\n";
    let expected =
        format!("{commit} 1 1 2\n{metadata}\tone\n{commit} 2 2\n{metadata}\ttwo\n").into_bytes();
    assert_eq!(
        render(&fixture, commit, b"file", &BlameOptions::default()),
        expected
    );
    assert_eq!(fixture.native(commit, b"file"), expected);
}

#[test]
fn empty_binary_crlf_nul_and_unterminated_content_match_full_native_output() {
    let mut fixture = Fixture::new();
    for bytes in [
        b"".as_slice(),
        b"one",
        b"one\r\ntwo\r\nlast",
        b"nul\0inside\nnext\0",
        b"\n\n",
        b"\xff\x00\xfe",
    ] {
        let commit = fixture.commit(&[(b"file", bytes, "100644")], &[], 1, b"Content\n", None);
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file")
        );
    }
}

#[test]
fn summary_uses_native_blame_first_nonblank_line_not_pretty_paragraph_folding() {
    let mut fixture = Fixture::new();
    for message in [
        b"\n \t\r\n Subject  \r\ncontinuation\n\nbody\n".as_slice(),
        b"",
        b"\n\n",
        b"first\0hidden\n",
    ] {
        let commit = fixture.commit(&[(b"file", b"line\n", "100644")], &[], 3, message, None);
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file")
        );
    }
}

#[test]
fn supported_declared_encodings_are_lossless_against_native_bytes() {
    let mut fixture = Fixture::new();
    for (encoding, message) in [
        ("ISO-8859-1", b"caf\xe9 \x80\n".as_slice()),
        ("windows-1252", b"price \x80\n"),
        ("UTF-8", b"utf8 \xc3\xa9\n"),
        ("CP1252", b"alias \x80\n"),
        ("ISO_8859-1:1987", b"C1 identity \x80\xe9\n"),
        ("latin-1", b"fallback alias \xe9\n"),
        ("UTF-16LE-BOM", b"\xff\xfe\x61\x00\n\x00"),
        ("KOI8-R", b"\xf0\xd2\xc9\xd7\xc5\xd4\n"),
        ("Shift_JIS", b"\x82\xa0\n"),
        ("EUC-JP", b"\xa4\xa2\n"),
        ("BIG5", b"\xa4\xa4\n"),
        ("GB18030", b"\x90\x30\x81\x30\n"),
        ("ISO-2022-JP", b"\x1b$B$\x22\x1b(B\n"),
        ("TSCII", b"\x82\x82\x82\n"),
    ] {
        let commit = fixture.commit(
            &[(b"file", b"line\n", "100644")],
            &[],
            4,
            message,
            Some(encoding),
        );
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file")
        );
    }
}

#[test]
fn multibyte_and_stateful_sequences_split_at_native_call_boundaries() {
    let mut fixture = Fixture::new();
    let blob = fixture.insert(gix_object::Kind::Blob, b"line\n");
    let tree = fixture.tree(&[(b"file", "100644", blob)]);
    for (encoding, sequence) in [
        ("Shift_JIS", b"\x82\xa0".as_slice()),
        ("ISO-2022-JP", b"\x1b$B$\x22\x1b(B"),
        ("GB18030", b"\x90\x30\x81\x30"),
    ] {
        for split in 1..sequence.len() {
            let mut body = format!("tree {tree}\nauthor Author <a@example.invalid> 1 -0000\ncommitter Committer <c@example.invalid> 1 -0000\nencoding {encoding}\n\n").into_bytes();
            body.resize(8192 - split, b'x');
            body.extend_from_slice(sequence);
            body.extend_from_slice(b"\n");
            let commit = fixture.insert(gix_object::Kind::Commit, &body);
            assert_eq!(
                render(&fixture, commit, b"file", &BlameOptions::default()),
                fixture.native(commit, b"file"),
                "{encoding} split after {split} byte(s)"
            );
        }
    }
}

#[test]
fn decomposing_native_codec_growth_is_admitted_not_assumed_four_bytes_per_input() {
    let mut fixture = Fixture::new();
    let message = vec![0x82; 9000]; // TSCII SRI decomposes to four Tamil scalars.
    let commit = fixture.commit(
        &[(b"file", b"line\n", "100644")],
        &[],
        1,
        &message,
        Some("TSCII"),
    );
    let native = fixture.native(commit, b"file");
    assert!(
        native.len() > message.len() * 4,
        "fixture must exercise expansion beyond 4x"
    );
    assert_eq!(
        render(&fixture, commit, b"file", &BlameOptions::default()),
        native
    );
}

#[test]
fn codec_capacity_denial_is_typed_and_never_publishes_raw_fallback_as_success() {
    let mut fixture = Fixture::new();
    let message = vec![0x82; 9000];
    for encoding in ["UTF-8", "TSCII"] {
        let commit = fixture.commit(
            &[(b"file", b"line\n", "100644")],
            &[],
            1,
            &message,
            Some(encoding),
        );
        let budget = CapacityBudget::new(192 * 1024);
        let session = fixture.session(budget.clone(), control());
        let result = session.blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        );
        if encoding == "UTF-8" {
            assert!(
                result.is_ok(),
                "the unconverted fixture must fit this admission"
            );
        } else {
            assert_eq!(
                result.err(),
                Some(BlameError::Capacity(git_rs_core::CapacityError::Exhausted))
            );
            drop(session);
            assert_eq!(
                budget.used(),
                0,
                "failed conversion cannot retain partial buffers"
            );
            continue;
        }
        drop(result);
        drop(session);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn streaming_dependency_preserves_incomplete_input_and_flushes_shift_state() {
    use iconv_native::stream::{Converter, Status};
    let mut converter = Converter::new(c"UTF-8", c"ISO-2022-JP").unwrap();
    let mut output = [0u8; 16];
    let prefix = converter.convert(b"\xe3\x81", &mut output);
    assert_eq!(prefix.status, Status::Incomplete);
    assert_eq!((prefix.consumed, prefix.written), (0, 0));
    let character = converter.convert(b"\xe3\x81\x82", &mut output);
    assert_eq!(character.status, Status::Complete);
    assert_eq!(character.consumed, 3);
    assert_eq!(&output[..character.written], b"\x1b$B$\x22");
    let short = converter.flush(&mut output[..2]);
    assert_eq!(short.status, Status::OutputFull);
    assert_eq!((short.consumed, short.written), (0, 0));
    let flush = converter.flush(&mut output);
    assert_eq!(flush.status, Status::Complete);
    assert_eq!(&output[..flush.written], b"\x1b(B");
    assert_eq!(
        converter.flush(&mut output).written,
        0,
        "flush resets the descriptor"
    );
    assert!(Converter::new(c"unknown-synthetic-encoding", c"UTF-8").is_none());
}

#[test]
fn complete_artifact_owners_can_cross_executor_and_shared_waiter_boundaries() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<git_rs_core::CompleteBlame>();
    assert_send_sync::<git_rs_core::CompletePorcelain>();
}

#[test]
fn unknown_and_malformed_declared_codecs_preserve_original_atomically_like_native() {
    let mut fixture = Fixture::new();
    for (encoding, message) in [
        (
            "unknown-synthetic-encoding",
            b"original \xff\xe9\n".as_slice(),
        ),
        ("windows-1252", b"converted prefix \x80 then invalid \x81\n"),
        ("Shift_JIS", b"valid \x82\xa0 then incomplete \x82"),
        ("US-ASCII", b"invalid \xff\n"),
        ("ISO-2022-JP", b"valid \x1b$B$\x22 then invalid \xff"),
    ] {
        let commit = fixture.commit(
            &[(b"file", b"line\n", "100644")],
            &[],
            1,
            message,
            Some(encoding),
        );
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file"),
            "encoding {encoding} must fall back to the complete original"
        );
    }
}

#[test]
fn late_conversion_failure_discards_already_converted_output_and_capacity() {
    let mut fixture = Fixture::new();
    let mut message = vec![0x80; 20_000];
    message.push(0x81); // Invalid CP1252 only after multiple native/output windows.
    let commit = fixture.commit(
        &[(b"file", b"line\n", "100644")],
        &[],
        1,
        &message,
        Some("CP1252"),
    );
    let expected = fixture.native(commit, b"file");
    assert!(expected.windows(64).any(|window| window == [0x80; 64]));
    assert_eq!(
        render(&fixture, commit, b"file", &BlameOptions::default()),
        expected
    );
}

#[test]
fn native_legacy_single_byte_values_include_pending_and_decomposed_state() {
    let mut fixture = Fixture::new();
    for byte in 0x80..=0xff {
        let commit = fixture.commit(
            &[(b"file", b"line\n", "100644")],
            &[],
            1,
            &[byte],
            Some("TSCII"),
        );
        assert_eq!(
            render(&fixture, commit, b"file", &BlameOptions::default()),
            fixture.native(commit, b"file"),
            "TSCII byte {byte:02x}"
        );
    }
}

#[test]
fn declared_encoding_applies_to_author_and_committer_not_only_the_subject() {
    let mut fixture = Fixture::new();
    let blob = fixture.insert(gix_object::Kind::Blob, b"line\n");
    let tree = fixture.tree(&[(b"file", "100644", blob)]);
    let mut body = format!("tree {tree}\n").into_bytes();
    body.extend_from_slice(b"author J\xf6rg <author@example.invalid> 1 -0000\ncommitter Andr\xe9 <committer@example.invalid> 2 -0130\nencoding ISO-8859-1\n\nR\xe9sum\xe9\n");
    let commit = fixture.insert(gix_object::Kind::Commit, &body);
    assert_eq!(
        render(&fixture, commit, b"file", &BlameOptions::default()),
        fixture.native(commit, b"file")
    );
}

#[test]
fn serialization_cannot_switch_to_an_unrelated_budget_or_reset_job_deadline() {
    let mut fixture = Fixture::new();
    let commit = fixture.commit(&[(b"file", b"line\n", "100644")], &[], 1, b"Root\n", None);
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let ctl = control();
    let session = fixture.session(budget.clone(), ctl.clone());
    let blame = session
        .blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .unwrap();
    let unrelated = CapacityBudget::new(8 * 1024 * 1024);
    assert_eq!(
        Porcelain::encode_complete(&blame, &ctl, &unrelated, 1024).err(),
        Some(BlameError::InvalidRequest)
    );
    assert_eq!(unrelated.used(), 0);
    assert_eq!(
        Porcelain::encode_complete(&blame, &control(), &budget, 1024).err(),
        Some(BlameError::InvalidRequest)
    );
    drop(blame);
    drop(session);
    assert_eq!(budget.used(), 0);
}

#[test]
fn chunked_output_is_complete_admitted_and_rejected_at_the_exact_body_bound() {
    let mut fixture = Fixture::new();
    let content = vec![b'x'; 70_000];
    let commit = fixture.commit(
        &[(b"file", &content, "100644")],
        &[],
        1,
        b"Long single line\n",
        None,
    );
    let expected = fixture.native(commit, b"file");
    let budget = CapacityBudget::new(16 * 1024 * 1024);
    let ctl = control();
    let session = fixture.session(budget.clone(), ctl.clone());
    let blame = session
        .blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .unwrap();
    let baseline = budget.used();
    assert!(
        baseline > 0,
        "admitted vendor outcome and metadata must remain owned"
    );
    assert_eq!(
        Porcelain::encode_complete(&blame, &ctl, &budget, expected.len() - 1).err(),
        Some(BlameError::OutputTooLarge)
    );
    assert_eq!(
        budget.used(),
        baseline,
        "a failed private writer cannot retain partial body capacity"
    );
    let output = Porcelain::encode_complete(&blame, &ctl, &budget, expected.len()).unwrap();
    assert_eq!(output.len(), expected.len() as u64);
    assert!(output.chunks().count() >= 2);
    assert!(output.chunks().all(|chunk| chunk.len() <= 64 * 1024));
    assert_eq!(
        output.chunks().flatten().copied().collect::<Vec<_>>(),
        expected
    );
    drop(blame);
    drop(session);
    assert!(
        budget.used() >= output.len() as usize,
        "complete output independently pins backing capacity"
    );
    drop(output);
    assert_eq!(budget.used(), 0);
}

#[test]
fn cancelled_serializer_exposes_no_partial_body_and_keeps_input_owners() {
    let mut fixture = Fixture::new();
    let commit = fixture.commit(
        &[(b"file", b"one\ntwo\n", "100644")],
        &[],
        1,
        b"Root\n",
        None,
    );
    let budget = CapacityBudget::new(8 * 1024 * 1024);
    let ctl = control();
    let session = fixture.session(budget.clone(), ctl.clone());
    let blame = session
        .blame_complete(
            BlameRequest {
                commit: &commit,
                path: b"file",
            },
            &BlameOptions::default(),
        )
        .unwrap();
    let baseline = budget.used();
    ctl.cancel();
    assert_eq!(
        Porcelain::encode_complete(&blame, &ctl, &budget, 1024).err(),
        Some(BlameError::Stopped(StopReason::Cancelled))
    );
    assert_eq!(budget.used(), baseline);
    drop(blame);
    drop(session);
    assert_eq!(budget.used(), 0);
}
