//! In-process conformance cluster over shared in-memory control and checkpoint storage.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, CatalogManifestStore, LeaderLease,
};
use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};
use tokio::sync::watch;
use uuid::Uuid;

use super::source::{self, FixtureSource};
use super::DEADLINE;
use crate::cluster_test_support::{prepare_controls, ClusterControl};
use crate::{DbError, LaminarDB};

/// Running cluster nodes; node 1 holds the leader lease and bootstraps the catalog.
pub(super) struct Cluster {
    pub dbs: Vec<Arc<LaminarDB>>,
    /// Batches each node's sources delivered, by node.
    pub polled: Vec<Arc<AtomicU64>>,
    _controls: Vec<ClusterControl>,
    _leader_watch: watch::Sender<Option<LeaderLease>>,
}

pub(super) enum Startup {
    Running(Cluster),
    /// The bootstrap catalog was rejected before any node started.
    Rejected(DbError),
}

fn assignment(nodes: u64, vnodes: u32) -> AssignmentSnapshot {
    let participants = (1..=nodes)
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    let owners = (0..u64::from(vnodes))
        .map(|vnode| StateNodeId(vnode % nodes + 1))
        .collect::<Vec<_>>();
    AssignmentSnapshot::empty()
        .next_for_participants(AssignmentSnapshot::vnodes_from_vec(&owners), participants)
        .expect("conformance assignment is well formed")
}

impl Cluster {
    /// Build `nodes` databases over `vnodes` vnodes, bootstrap `ddl` on node 1, and fence every
    /// node for startup. [`Self::activate`] starts them.
    pub async fn start(
        nodes: u64,
        vnodes: u32,
        sources: &[FixtureSource],
        ddl: &[String],
    ) -> Startup {
        let objects: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let assignment = assignment(nodes, vnodes);
        let (controls, leader_watch) =
            prepare_controls(&assignment, vnodes, Arc::clone(&objects)).await;
        let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
        assignments
            .save_if_absent(&assignment)
            .await
            .unwrap()
            .unwrap();
        let owners = assignment.to_vnode_vec(vnodes).unwrap();
        let polled = (0..nodes)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect::<Vec<_>>();
        let mut dbs = Vec::new();
        for (index, control) in controls.iter().enumerate() {
            let registry = Arc::new(VnodeRegistry::new_unassigned(vnodes));
            registry.set_assignment_and_version(Arc::from(owners.clone()), assignment.version);
            let manifest = Arc::new(CatalogManifestStore::new(
                control.controller.checkpoint_authority().unwrap(),
            ));
            let db = LaminarDB::builder()
                .cluster_controller(Arc::clone(&control.controller))
                .cluster_checkpoint_object_store(Arc::clone(&objects))
                .assignment_snapshot_store(Arc::clone(&assignments))
                .catalog_manifest_store(manifest)
                .vnode_registry(registry)
                .shuffle_sender(Arc::clone(&control.sender))
                .shuffle_receiver(Arc::clone(&control.receiver))
                .delivery_guarantee(laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce)
                .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                    interval_ms: None,
                    ..Default::default()
                })
                .register_connector(source::register(
                    sources.to_vec(),
                    Arc::clone(&polled[index]),
                ))
                .build()
                .await
                .unwrap();
            if index == 0 {
                if let Err(error) = db.execute_cluster_bootstrap_batch(ddl).await {
                    let _ = db.shutdown().await;
                    return Startup::Rejected(error);
                }
            } else {
                db.restore_catalog_from_manifest()
                    .await
                    .unwrap()
                    .expect("bootstrap node sealed the catalog");
            }
            control
                .controller
                .publish_checkpoint_assignment_fence(Some(assignment.assignment_fence().unwrap()));
            db.fence_cluster_startup();
            db.prepare_cluster_startup_recovery_generation(tokio::time::Instant::now() + DEADLINE)
                .await
                .unwrap();
            dbs.push(db);
        }
        Startup::Running(Self {
            dbs,
            polled,
            _controls: controls,
            _leader_watch: leader_watch,
        })
    }

    /// Start every node; a node that fails to start is a runtime failure, not a rejection.
    pub async fn activate(&self) -> Result<(), DbError> {
        for result in futures::future::join_all(self.dbs.iter().map(LaminarDB::start)).await {
            result?;
        }
        for db in &self.dbs {
            db.finish_cluster_startup(tokio::time::Instant::now() + DEADLINE)
                .await?;
        }
        Ok(())
    }

    pub async fn shutdown(self) {
        for db in &self.dbs {
            let _ = tokio::time::timeout(DEADLINE, db.shutdown()).await;
        }
    }
}
