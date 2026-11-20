#![forbid(unsafe_code)]
//! Explicit operator policy and bounded target inputs.
use git_rs::blame::{Blame, Request, WarmPolicy};

#[test]
fn production_warming_is_disabled_until_explicit_operator_policy() {
    assert!(Blame::default().warming_statistics().is_none());
    let policy = WarmPolicy::one_path("literal%20file").unwrap();
    assert_eq!(policy.admitted(), 0);
    assert_eq!(policy.completed(), 0);
    assert_eq!(policy.failed(), 0);
    assert!(
        Blame::default().with_warming(policy).is_err(),
        "unsigned/unconfigured cache cannot accept warm policy"
    );
}

#[test]
fn query_and_warm_path_limits_are_checked_before_decoded_storage() {
    let commit = "1111111111111111111111111111111111111111";
    let maximum = "%61".repeat(4096);
    assert_eq!(
        Request::parse(commit, &format!("path={maximum}"))
            .unwrap()
            .path()
            .len(),
        4096
    );
    assert!(WarmPolicy::one_path(&maximum).is_ok());
    assert!(Request::parse(commit, &format!("path={maximum}%61")).is_err());
    assert!(WarmPolicy::one_path(&format!("{maximum}%61")).is_err());
    for path in ["", "%", "%GG", "../file", "a//b", "a%00b", "a&other=b"] {
        assert!(
            Request::parse(commit, &format!("path={path}")).is_err(),
            "accepted query path {path:?}"
        );
        assert!(
            WarmPolicy::one_path(path).is_err(),
            "accepted warm path {path:?}"
        );
    }
}
