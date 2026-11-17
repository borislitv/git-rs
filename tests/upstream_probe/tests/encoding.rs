use std::io::Write;
use std::process::{Command, Stdio};

use git_rs_upstream_probe::commit_text::{CommitTextCodec, Error, Limits, Outcome};
use tempfile::TempDir;

const LIMITS: Limits = Limits {
    input_bytes: 128 * 1024,
    steps: 128,
};

#[test]
fn skip_does_not_validate_or_stage_utf8() {
    let raw = b"bad UTF-8: \xff\0physical tail";
    for label in [None, Some(b"uTf-8".as_slice()), Some(b"Utf8")] {
        assert_eq!(
            CommitTextCodec::convert(raw, label, &mut [], Limits { steps: 0, ..LIMITS }),
            Ok(Outcome::Skipped(raw)),
        );
    }
}

#[test]
fn labels_and_c_string_input() {
    let mut staging = [0; 128];
    for label in ["ISO-8859-1", "latin-1", "ISO_8859-1:1987", "lAtIn1"] {
        assert_eq!(
            CommitTextCodec::convert(
                b"\x80\xe9\0ignored",
                Some(label.as_bytes()),
                &mut staging,
                LIMITS
            ),
            Ok(Outcome::Converted("\u{80}é".as_bytes())),
            "{label}",
        );
    }
    assert_eq!(
        CommitTextCodec::convert(b"\x80", Some(b"windows-1252"), &mut staging, LIMITS),
        Ok(Outcome::Converted("€".as_bytes())),
    );
    assert_eq!(
        CommitTextCodec::convert(b"\xff\xfeA\x01", Some(b"UTF16LE-BOM"), &mut staging, LIMITS),
        Ok(Outcome::Converted("Ł".as_bytes())),
    );
    assert_eq!(
        CommitTextCodec::convert(b"raw", Some(b"UTF-8\0suffix"), &mut staging, LIMITS),
        Err(Error::InvalidLabel),
    );
    for label in [b"unknown-synthetic-charset".as_slice(), b"bad\xff"] {
        assert_eq!(
            CommitTextCodec::convert(b"raw", Some(label), &mut staging, LIMITS),
            Ok(Outcome::RawFallback(b"raw")),
        );
    }
}

#[test]
fn limits_are_not_fallback_and_tamil_output_is_not_flushed() {
    let mut staging = [0; 128];
    assert_eq!(
        CommitTextCodec::convert(b"\x82", Some(b"TSCII"), &mut staging, LIMITS),
        Ok(Outcome::Converted("ஸ்ரீ".as_bytes())),
    );
    assert_eq!(
        CommitTextCodec::convert(b"\x8a", Some(b"TSCII"), &mut staging, LIMITS),
        Ok(Outcome::Converted("\u{bb8}".as_bytes())),
    );
    assert_eq!(
        CommitTextCodec::convert(b"\x82", Some(b"TSCII"), &mut staging[..1], LIMITS),
        Err(Error::OutputLimit),
    );
    assert_eq!(
        CommitTextCodec::convert(
            b"\x82",
            Some(b"TSCII"),
            &mut staging,
            Limits { steps: 1, ..LIMITS },
        ),
        Err(Error::StepLimit),
    );
    assert_eq!(
        CommitTextCodec::convert(
            b"a\0b",
            Some(b"latin-1"),
            &mut staging,
            Limits {
                input_bytes: 2,
                ..LIMITS
            },
        ),
        Err(Error::InputLimit),
    );
    for (label, raw) in [
        ("CP1252", b"valid \x80 then illegal \x81".as_slice()),
        ("Shift_JIS", b"valid \x82\xa0 then incomplete \x82"),
    ] {
        assert_eq!(
            CommitTextCodec::convert(raw, Some(label.as_bytes()), &mut staging, LIMITS),
            Ok(Outcome::RawFallback(raw)),
        );
    }
}

struct Oracle {
    dir: TempDir,
    tree: String,
}

impl Oracle {
    fn new() -> Self {
        let mut this = Self {
            dir: tempfile::tempdir().unwrap(),
            tree: String::new(),
        };
        this.run(&["init", "--quiet", "--template="], b"");
        this.tree = oid(this.run(&["mktree"], b""));
        this
    }

    fn run(&self, args: &[&str], input: &[u8]) -> Vec<u8> {
        let mut child = Command::new("git")
            .args(args)
            .current_dir(self.dir.path())
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.dir.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("LC_ALL", "C")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(input).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "git {args:?}: {:?}", out.stderr);
        out.stdout
    }

    fn check(
        &self,
        label: &str,
        author: &[u8],
        committer: &[u8],
        body: &[u8],
        raw_fallback: bool,
    ) -> Vec<u8> {
        let mut commit = format!("tree {}\nauthor ", self.tree).into_bytes();
        commit.extend_from_slice(author);
        commit.extend_from_slice(b" <author@example.invalid> 1 +0000\ncommitter ");
        commit.extend_from_slice(committer);
        commit.extend_from_slice(b" <committer@example.invalid> 2 -0130\nencoding ");
        commit.extend_from_slice(label.as_bytes());
        commit.extend_from_slice(b"\n\n");
        commit.extend_from_slice(body);

        let id = oid(self.run(&["hash-object", "-t", "commit", "-w", "--stdin"], &commit));
        let native = self.run(
            &[
                "log",
                "-1",
                "--encoding=UTF-8",
                "--no-use-mailmap",
                "--format=format:%an%x00%ae%x00%cn%x00%ce%x00%B",
                &id,
            ],
            b"",
        );
        let mut staging = vec![0; 512 * 1024];
        let actual =
            CommitTextCodec::convert(&commit, Some(label.as_bytes()), &mut staging, LIMITS)
                .unwrap_or_else(|err| panic!("{label}: {err:?}"));
        let bytes = match actual {
            Outcome::RawFallback(bytes) if raw_fallback => {
                assert_eq!(bytes, commit);
                bytes
            }
            Outcome::Skipped(bytes) if label.eq_ignore_ascii_case("utf8") => bytes,
            Outcome::Converted(bytes) if !raw_fallback => bytes,
            _ => panic!("unexpected conversion outcome for {label}"),
        };
        let actual = fields(bytes);
        let first_difference = actual.iter().zip(&native).position(|(a, b)| a != b);
        assert_eq!(
            first_difference, None,
            "native field bytes differ for {label}"
        );
        assert_eq!(
            actual.len(),
            native.len(),
            "native field lengths differ for {label}"
        );
        native
    }
}

fn oid(stdout: Vec<u8>) -> String {
    String::from_utf8(stdout).unwrap().trim_end().to_owned()
}

// Select the same fields as the native pretty-format command, without
// applying subject folding, whitespace normalization, or UTF-8 replacement.
fn fields(commit: &[u8]) -> Vec<u8> {
    let sep = commit.windows(2).position(|w| w == b"\n\n").unwrap();
    let headers = &commit[..sep];
    let mut result = Vec::new();
    for prefix in [b"author ".as_slice(), b"committer "] {
        let sig = headers
            .split(|&b| b == b'\n')
            .find_map(|line| line.strip_prefix(prefix))
            .unwrap();
        let name_end = sig.windows(2).position(|w| w == b" <").unwrap();
        let email = &sig[name_end + 2..];
        let email_end = email.iter().position(|&b| b == b'>').unwrap();
        result.extend_from_slice(&sig[..name_end]);
        result.push(0);
        result.extend_from_slice(&email[..email_end]);
        result.push(0);
    }
    result.extend_from_slice(&commit[sep + 2..]);
    result
}

#[test]
fn native_unknown_label_preserves_raw_fields_on_first_open() {
    let oracle = Oracle::new();
    let body = b"raw \x80\n\nbody \xff\n";
    let got = oracle.check("unknown-synthetic-charset", b"J\xe9", b"C\xfe", body, true);
    assert!(got.starts_with(b"J\xe9\0"));
    assert!(got.ends_with(body));
}

#[test]
fn native_whole_fields_aliases_and_late_failure() {
    let oracle = Oracle::new();
    for (label, decoded) in [
        ("latin-1", "\u{80}"),
        ("ISO_8859-1:1987", "\u{80}"),
        ("CP1252", "€"),
    ] {
        let got = oracle.check(
            label,
            b"J\xe9\x80",
            b"A\xe9\x80",
            b"R\xe9\x80\n\nbody \x80\n",
            false,
        );
        assert!(
            got.starts_with(format!("Jé{decoded}\0").as_bytes()),
            "{label}"
        );
    }
    oracle.check("uTf8", b"raw \xff", b"raw \xfe", b"invalid \xfd\n", false);
    let mut late = b"valid before failure\n\n".to_vec();
    late.extend(std::iter::repeat_n(0x80, 20_000));
    late.push(0x81);
    let got = oracle.check("CP1252", b"J\xe9\x80", b"A\xe9\x80", &late, true);
    assert!(got.starts_with(b"J\xe9\x80\0"));
    assert!(got.ends_with(&late));
    oracle.check(
        "Shift_JIS",
        b"A\x82\xa0",
        b"B\x82\xa2",
        b"incomplete \x82",
        true,
    );
}

#[test]
fn native_whole_fields_expansion_no_flush_and_stateful_resume() {
    let oracle = Oracle::new();
    let mut expanded = vec![0x82; 9000];
    expanded.extend_from_slice(b"\n\nfinal \x8a");
    let got = oracle.check("TSCII", b"A\x82", b"C\x82", &expanded, false);
    assert!(got.len() > expanded.len() * 4);
    assert!(got.ends_with("final \u{bb8}".as_bytes()));

    // Two input bytes per Japanese character become three output bytes, so
    // the first native output window ends with the converter in JIS state.
    let mut message = b"subject\n\n\x1b$B".to_vec();
    for _ in 0..5000 {
        message.extend_from_slice(b"$\x22$\x24");
    }
    message.extend_from_slice(b"\x1b(B\n");
    let got = oracle.check(
        "ISO-2022-JP",
        b"A\x1b$B$\x22\x1b(B",
        b"C\x1b$B$\x24\x1b(B",
        &message,
        false,
    );
    assert!(got.ends_with(format!("{}\n", "あい".repeat(5000)).as_bytes()));
}
