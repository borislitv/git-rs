use git_rs::{blame::Blame, http::Request, receive::ResourceProfile};
use std::{ffi::OsStr, io::Cursor};

#[test]
fn receive_profile_is_opt_in_and_requires_exclusive_shared_admission() {
    assert_eq!(
        ResourceProfile::from_os_value(None).unwrap(),
        ResourceProfile::BaselineV1
    );
    assert!(ResourceProfile::from_os_value(Some(OsStr::new("large-v1"))).is_err());
    if usize::BITS < 64 {
        return;
    }
    let large = ResourceProfile::from_os_value(Some(OsStr::new("large-bootstrap-v1"))).unwrap();
    assert!(Blame::new_with_resource_profiles(2, 0, Default::default(), large).is_err());
    assert!(Blame::new_with_resource_profiles(1, 1, Default::default(), large).is_err());
    let blame = Blame::new_with_resource_profiles(1, 0, Default::default(), large).unwrap();
    assert_eq!(blame.cpu().receive_profile(), large);
    assert_eq!(
        blame.resource_profile(),
        git_rs::blame::ResourceProfile::BaselineV1
    );
    assert!(blame.with_capacity(1, 1).is_err());
}

#[test]
fn only_receive_post_has_a_finite_larger_http_length() {
    let request = |service: &str, length: u64| {
        Cursor::new(format!(
            "POST /team/project.git/git-{service}-pack HTTP/1.1\r\nContent-Type: application/x-git-{service}-pack-request\r\nContent-Length: {length}\r\n\r\n"
        ))
    };
    let large = ResourceProfile::LargeBootstrapV1;
    let limit = 4608u64 << 20;
    assert!(Request::parse(&mut request("receive", limit)).is_err());
    assert!(Request::parse_with_receive_profile(&mut request("receive", limit), large).is_ok());
    assert!(Request::parse_with_receive_profile(&mut request("upload", limit), large).is_err());
    assert!(
        Request::parse_with_receive_profile(&mut request("receive", limit + 1), large).is_err()
    );
}
