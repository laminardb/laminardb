//! Legacy authority adoption in the existing stateful, real-process soak.
//!
//! This is an identical-inventory format upgrade, not an additive graph migration. The fixture
//! uses one verified executable for every process and a unique checkpoint namespace, satisfying
//! adoption's coordinated binary upgrade precondition without claiming mixed-version support.

use super::{has_full_membership, wait_for, Node};
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::{
    CatalogManifestStore, CheckpointDecisionStore, LeaderLeaseStore, LegacyTopologyBaseline,
    TopologyAdoptionOutcome, TopologyCatalogState,
};
use object_store::ObjectStoreExt;

pub(super) fn adopt_inventory(
    checkpoint_url: &str,
    nodes: &mut [Node],
    recovery_ceiling: Duration,
) -> LegacyTopologyBaseline {
    let objects = objects_for_namespace(checkpoint_url);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 30_000));
    let catalog = CatalogManifestStore::new(Arc::clone(&authority));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("soak authority runtime");
    let baseline = runtime.block_on(async {
        let TopologyCatalogState::LegacySealed { manifest } = catalog
            .topology_state()
            .await
            .expect("read original legacy topology")
        else {
            panic!("fresh soak must use a sealed, explicitly unversioned catalog");
        };
        let manifest_path = object_store::path::Path::from(format!(
            "control/catalog-manifest/v1/{}.json",
            manifest.sha256
        ));
        let original_bytes = objects
            .get(&manifest_path)
            .await
            .expect("read original inventory")
            .bytes()
            .await
            .expect("original inventory bytes");
        let deployment = CheckpointDecisionStore::new(Arc::clone(&objects))
            .load_deployment_id()
            .await
            .expect("read existing deployment")
            .expect("running cluster must already have a deployment identity");
        let proof = authority
            .load()
            .await
            .expect("read active lease")
            .expect("running cluster must have leader authority")
            .proof();
        let operation = uuid::Uuid::new_v4()
            .try_into()
            .expect("nonzero request UUID");
        let TopologyAdoptionOutcome::Created(baseline) = catalog
            .adopt_legacy_topology(&proof, operation, &manifest, &deployment)
            .await
            .expect("adopt exact running inventory")
        else {
            panic!("unique soak namespace must admit exactly one baseline");
        };
        assert_eq!(baseline.manifest, manifest);
        assert_eq!(baseline.deployment_id, deployment);
        assert_eq!(
            objects
                .get(&manifest_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            original_bytes,
            "adoption rewrote the original catalog blob"
        );
        assert_eq!(
            catalog
                .adopt_legacy_topology(&proof, operation, &manifest, &deployment)
                .await
                .expect("retry original adoption"),
            TopologyAdoptionOutcome::Existing(baseline.clone())
        );
        baseline
    });
    wait_for_status(nodes, &baseline, false, recovery_ceiling);
    eprintln!(
        "soak: adopted identical catalog as topology {} at authority sequence {} with operation {}",
        baseline.topology_version.get(),
        baseline.authority_sequence,
        baseline.operation_id.get()
    );
    baseline
}

pub(super) fn objects_for_namespace(checkpoint_url: &str) -> Arc<dyn object_store::ObjectStore> {
    let mut storage = std::collections::HashMap::new();
    for (environment, key) in [
        ("LAMINAR_SOAK_S3_ENDPOINT", "endpoint"),
        ("LAMINAR_SOAK_S3_ACCESS_KEY", "aws_access_key_id"),
        ("LAMINAR_SOAK_S3_SECRET_KEY", "aws_secret_access_key"),
        ("LAMINAR_SOAK_S3_REGION", "region"),
    ] {
        if let Ok(value) = std::env::var(environment) {
            storage.insert(key.to_owned(), value);
        }
    }
    if storage.contains_key("endpoint") {
        storage.insert("allow_http".to_owned(), "true".to_owned());
    }
    laminar_core::checkpoint::object_store_builder::build_object_store(checkpoint_url, &storage)
        .expect("build exact shared soak authority namespace")
}

fn wait_for_status(
    nodes: &mut [Node],
    baseline: &LegacyTopologyBaseline,
    require_active: bool,
    recovery_ceiling: Duration,
) {
    wait_for(
        "all processes to observe adopted topology",
        recovery_ceiling,
        || {
            nodes.iter_mut().all(|node| {
                node.assert_running();
                let Some(body) = node.http_get("/api/v1/cluster/topology") else {
                    return false;
                };
                let status: serde_json::Value = serde_json::from_str(&body)
                    .expect("topology status must be valid bounded JSON");
                let catalog: TopologyCatalogState =
                    serde_json::from_value(status["catalog"].clone())
                        .expect("explicit topology status encoding");
                assert_eq!(
                    catalog,
                    TopologyCatalogState::Versioned {
                        baseline: baseline.clone(),
                        committed: None
                    },
                    "node{} observed a different adopted inventory or operation",
                    node.id
                );
                assert_eq!(status["committed_version"].as_u64(), Some(1));
                if require_active {
                    status["locally_active_version"].as_u64() == Some(1) && node.is_ready()
                } else {
                    // A concurrent normal recovery may have replayed the unchanged inventory.
                    // Otherwise the process conservatively reports no locally active version.
                    if let Some(version) = status["locally_active_version"].as_u64() {
                        assert_eq!(version, 1);
                    }
                    true
                }
            })
        },
    );
}

pub(super) fn restart_all(
    nodes: &mut [Node],
    baseline: &LegacyTopologyBaseline,
    recovery_ceiling: Duration,
) {
    let started = Instant::now();
    for node in nodes.iter_mut() {
        node.disarm_checkpoint_kill();
        node.kill9();
    }
    for node in nodes.iter_mut() {
        let executable = node.verify_executable_for_spawn();
        node.spawn(executable);
    }
    wait_for_status(nodes, baseline, true, recovery_ceiling);
    wait_for(
        "full membership after adopted-catalog restart",
        recovery_ceiling,
        || has_full_membership(nodes),
    );
    eprintln!(
        "soak: all processes replayed and activated topology one after {:?}",
        started.elapsed()
    );
}
