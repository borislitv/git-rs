#![forbid(unsafe_code)]

use git_rs::maintenance::leader::{Election, LeaderConfig};
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube_client::{
    Api, Client,
    api::{Patch, PatchParams},
};
use kubert::{
    LeaseManager,
    lease::{ClaimParams, Error},
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

fn config(lease: &str, namespace: &str, pod: &str) -> LeaderConfig {
    LeaderConfig {
        lease_name: lease.into(),
        namespace: namespace.into(),
        pod_uid: pod.into(),
        lease_seconds: 30,
        renew_grace_seconds: 15,
        safety_seconds: 2,
        pg_lease_seconds: 20,
        heartbeat_seconds: 5,
    }
}

#[test]
fn leader_config_rejects_invalid_identities_and_unsafe_or_overflowing_margins() {
    let valid = config("source", "maintenance", "synthetic-pod-1");
    valid.validate().unwrap();
    for lease in [
        "",
        "Source",
        "-source",
        "source-",
        "source..lease",
        "source/lease",
    ] {
        let mut invalid = valid.clone();
        invalid.lease_name = lease.into();
        assert!(invalid.validate().is_err(), "accepted Lease name {lease:?}");
    }
    for namespace in ["", "other.namespace", "../maintenance", "Maintenance"] {
        let mut invalid = valid.clone();
        invalid.namespace = namespace.into();
        assert!(
            invalid.validate().is_err(),
            "accepted namespace {namespace:?}"
        );
    }
    for pod in ["", "pod\nidentity", "pod/other"] {
        let mut invalid = valid.clone();
        invalid.pod_uid = pod.into();
        assert!(invalid.validate().is_err(), "accepted pod identity {pod:?}");
    }
    for (lease, grace, safety, pg, heartbeat) in [
        (30, 12, 2, 20, 5),
        (30, 18, 2, 20, 5),
        (30, u64::MAX, 2, 20, 5),
        (u64::MAX, 15, 2, 20, 5),
        (30, 15, u64::MAX, 20, 5),
        (30, 15, 2, u64::MAX, 5),
        (30, 15, 2, 20, u64::MAX),
        (30, 15, 2, 17, 5),
        (30, 15, 2, 20, 0),
    ] {
        let mut invalid = valid.clone();
        invalid.lease_seconds = lease;
        invalid.renew_grace_seconds = grace;
        invalid.safety_seconds = safety;
        invalid.pg_lease_seconds = pg;
        invalid.heartbeat_seconds = heartbeat;
        assert!(
            invalid.validate().is_err(),
            "accepted unsafe configuration: {invalid:?}"
        );
    }
}

fn fixture() -> (String, String) {
    assert!(
        std::env::var_os("KUBECONFIG").is_some(),
        "candidate fixture must supply isolated credentials"
    );
    let namespace = std::env::var("GIT_RS_TEST_LEASE_NAMESPACE").unwrap();
    let name = std::env::var("GIT_RS_TEST_LEASE_NAME").unwrap();
    assert!(namespace.starts_with("git-rs-candidate-"));
    assert!(name.starts_with("candidate-"));
    (namespace, name)
}

async fn annotation(api: &Api<Lease>, name: &str, value: &str) {
    // Force-apply a different field manager's metadata without claiming the live
    // ownership fields. The real API must retain those fields and change RV.
    let patch = Lease {
        metadata: ObjectMeta {
            name: Some(name.into()),
            annotations: Some(BTreeMap::from([(
                "git-rs.test/observation".into(),
                value.into(),
            )])),
            ..Default::default()
        },
        ..Default::default()
    };
    api.patch(
        name,
        &PatchParams {
            field_manager: Some("candidate-fixture".into()),
            force: true,
            ..Default::default()
        },
        &Patch::Apply(&patch),
    )
    .await
    .unwrap();
}

#[test]
#[ignore = "requires the exact-owned Kubernetes 1.37 candidate fixture and named-Lease GET/PATCH credentials"]
fn real_named_lease_cas_expiry_and_transition_contracts() {
    let (namespace, name) = fixture();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let client = Client::try_default().await.unwrap();
        let api = Api::<Lease>::namespaced(client, &namespace);
        let empty = api.get(&name).await.unwrap();
        assert!(
            empty
                .spec
                .as_ref()
                .is_none_or(|spec| spec.holder_identity.is_none())
        );
        let manager = LeaseManager::init(api.clone(), &name)
            .await
            .unwrap()
            .with_field_manager("git-rs-source-maintenance");
        let params = ClaimParams {
            lease_duration: Duration::from_secs(30),
            renew_grace_period: Duration::from_secs(15),
        };
        let first = manager
            .ensure_claimed("candidate-a", &params)
            .await
            .unwrap();
        assert!(first.is_current_for("candidate-a"));
        let acquired = api.get(&name).await.unwrap();
        assert_eq!(acquired.spec.as_ref().unwrap().lease_transitions, Some(1));

        let other = LeaseManager::init(api.clone(), &name).await.unwrap();
        assert!(
            other
                .ensure_claimed("candidate-b", &params)
                .await
                .unwrap()
                .is_current_for("candidate-a")
        );
        assert!(!other.vacate("candidate-b").await.unwrap());
        annotation(&api, &name, "metadata-only").await;
        let current = api.get(&name).await.unwrap();
        assert_ne!(
            current.metadata.resource_version,
            acquired.metadata.resource_version
        );
        assert_eq!(
            current.spec, acquired.spec,
            "force apply reset live Lease ownership"
        );

        let stale = Lease {
            metadata: ObjectMeta {
                resource_version: acquired.metadata.resource_version.clone(),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some("stale-owner".into()),
                ..Default::default()
            }),
        };
        let rejected = api
            .patch(&name, &PatchParams::default(), &Patch::Strategic(&stale))
            .await
            .unwrap_err();
        assert!(
            matches!(rejected, kube_client::Error::Api(error) if error.code==409),
            "stale resourceVersion must conflict"
        );
        assert_eq!(
            api.get(&name)
                .await
                .unwrap()
                .spec
                .unwrap()
                .holder_identity
                .as_deref(),
            Some("candidate-a")
        );

        first.expire_with_grace(params.renew_grace_period).await;
        let renewed = manager
            .ensure_claimed("candidate-a", &params)
            .await
            .unwrap();
        assert!(renewed.is_current_for("candidate-a"));
        assert_ne!(renewed.expiry, first.expiry);
        assert_eq!(
            api.get(&name)
                .await
                .unwrap()
                .spec
                .unwrap()
                .lease_transitions,
            Some(1)
        );

        // Advance only the local monotonic clock. The server's unchanged wall
        // expiry remains in the future, as after a wall-clock rollback. Neither
        // metadata-only RV changes nor vacate(no-op) may resurrect this claim.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(!renewed.is_current());
        tokio::time::resume();
        annotation(&api, &name, "after-local-expiry").await;
        assert!(!manager.sync().await.unwrap().unwrap().is_current());
        assert!(!manager.vacate("candidate-a").await.unwrap());
        assert!(!manager.sync().await.unwrap().unwrap().is_current());
        let replacement = manager
            .ensure_claimed("candidate-b", &params)
            .await
            .unwrap();
        assert!(replacement.is_current_for("candidate-b"));
        assert!(
            !renewed.is_current(),
            "new acquisition revived a prior claim"
        );
        assert_eq!(
            api.get(&name)
                .await
                .unwrap()
                .spec
                .unwrap()
                .lease_transitions,
            Some(2)
        );
        assert!(manager.vacate("candidate-b").await.unwrap());

        for (transitions, succeeds) in [(65535, true), (i32::MAX, false)] {
            let patch = Lease {
                metadata: ObjectMeta::default(),
                spec: Some(LeaseSpec {
                    lease_transitions: Some(transitions),
                    ..Default::default()
                }),
            };
            api.patch(&name, &PatchParams::default(), &Patch::Strategic(&patch))
                .await
                .unwrap();
            let candidate = LeaseManager::init(api.clone(), &name).await.unwrap();
            let result = candidate.ensure_claimed("counter-boundary", &params).await;
            if succeeds {
                assert!(result.unwrap().is_current_for("counter-boundary"));
                assert_eq!(
                    api.get(&name)
                        .await
                        .unwrap()
                        .spec
                        .unwrap()
                        .lease_transitions,
                    Some(65536)
                );
                assert!(candidate.vacate("counter-boundary").await.unwrap());
            } else {
                assert!(matches!(result, Err(Error::InvalidTransitions)));
                let spec = api.get(&name).await.unwrap().spec.unwrap();
                assert!(spec.holder_identity.is_none());
                assert_eq!(spec.lease_transitions, Some(i32::MAX));
            }
        }
    });
}

fn until(mut condition: impl FnMut() -> bool, seconds: u64) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "candidate state did not converge before its deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires the exact-owned Kubernetes 1.37 candidate fixture and named-Lease GET/PATCH credentials"]
fn real_adapter_has_eligible_standbys_and_releases_only_after_stop() {
    let (namespace, name) = fixture();
    let name = format!("{name}-adapter");
    // The client owns background I/O spawned on this runtime. Keep its driver
    // running while the separately owned Election threads use clones of it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let client = runtime.block_on(Client::try_default()).unwrap();
    let first = Election::with_client(config(&name, &namespace, "synthetic-pod-a"), client.clone())
        .unwrap();
    let second =
        Election::with_client(config(&name, &namespace, "synthetic-pod-b"), client).unwrap();
    until(
        || first.eligible() && second.eligible() && (first.is_candidate() != second.is_candidate()),
        20,
    );
    let (leader, standby) = if first.is_candidate() {
        (first, second)
    } else {
        (second, first)
    };
    assert!(leader.confirm().unwrap() > Instant::now());
    assert!(standby.confirm().is_err());
    leader.request_stop();
    assert!(leader.stopping());
    assert!(!leader.eligible());
    assert!(!leader.is_candidate());
    assert!(leader.confirm().is_err());
    drop(leader);
    until(|| standby.is_candidate(), 45);
    assert!(standby.confirm().unwrap() > Instant::now());
    drop(standby);
    let api =
        Api::<Lease>::namespaced(runtime.block_on(Client::try_default()).unwrap(), &namespace);
    let lease = runtime.block_on(api.get(&name)).unwrap();
    assert!(
        lease.spec.unwrap().holder_identity.is_none(),
        "graceful stop left an owned Lease"
    );
}
