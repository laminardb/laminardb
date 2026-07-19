#![cfg(feature = "cluster")]

use std::sync::Arc;

use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
use laminar_core::cluster::control::{
    set_cluster_tls, ClusterController, ClusterKv, ClusterTls, InMemoryKv, LeaderLease,
    LeaderLeaseOwner, LeaseDeadline, ProcessLease,
};
use laminar_core::cluster::discovery::NodeId;

#[tokio::test]
async fn barrier_proof_confirmation_uses_mutual_tls() {
    const SAN: &str = "laminar-cluster";

    let mut ca_params = rcgen::CertificateParams::new(vec!["laminar-test-ca".into()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf = rcgen::CertificateParams::new(vec![SAN.into()]).unwrap();
    leaf.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
    set_cluster_tls(ClusterTls::from_pem(
        leaf_cert.pem().as_bytes(),
        leaf_key.serialize_pem().as_bytes(),
        ca_cert.pem().as_bytes(),
        SAN,
    ))
    .unwrap();

    let peer = NodeId(7);
    let boot = uuid::Uuid::from_u128(77);
    let remote_kv = Arc::new(InMemoryKv::new(peer));
    let remote = Arc::new(ClusterController::new_with_recovery_incarnation(
        peer,
        remote_kv.clone(),
        remote_kv.clone(),
        None,
        tokio::sync::watch::channel(Vec::new()).1,
        boot,
    ));
    remote
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )))
        .unwrap();
    remote.set_active(true);

    let owner = LeaderLeaseOwner {
        node: peer,
        boot,
        process_term: 1,
    };
    let lease = LeaderLease {
        seq: 1,
        renewal_sequence: 1,
        token: 1,
        owner: owner.clone(),
        expires_at_ms: i64::MAX,
        catalog_manifest: None,
    };
    let proof = lease.proof();
    remote
        .set_leader_lease_watch(
            tokio::sync::watch::channel(Some(lease)).1,
            owner,
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    remote.install_local_leader_proof_provider();

    let process_lease = ProcessLease {
        node: peer,
        owner: boot,
        term: 1,
        seq: 1,
        expires_at_ms: i64::MAX,
    };
    let bound = remote
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();
    assert_ne!(bound.port(), 0);

    let caller_node = NodeId(1);
    let caller_kv = Arc::new(InMemoryKv::new(caller_node));
    caller_kv.seed(
        peer,
        BARRIER_ADDR_KEY,
        remote_kv.read_from(peer, BARRIER_ADDR_KEY).await.unwrap(),
    );
    let caller = ClusterController::new_with_recovery_incarnation(
        caller_node,
        caller_kv.clone(),
        caller_kv,
        None,
        tokio::sync::watch::channel(Vec::new()).1,
        uuid::Uuid::from_u128(1),
    );
    caller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )))
        .unwrap();
    caller
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            &ProcessLease {
                node: caller_node,
                owner: uuid::Uuid::from_u128(1),
                term: 1,
                seq: 1,
                expires_at_ms: i64::MAX,
            },
        )
        .await
        .unwrap();

    assert!(caller
        .confirm_remote_leader_proof(
            &proof,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap());
}
