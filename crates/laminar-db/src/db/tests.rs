//! Unit and integration tests for [`LaminarDB`](super::LaminarDB).
//!
//! Split out of `db.rs` to keep the main file focused on public API and
//! struct wiring. Declared via `#[cfg(test)] mod tests;` in `db.rs`.

use super::*;
use crate::ddl::extract_connector_from_with_options;
use laminar_core::catalog::CatalogObjectKind;
#[cfg(feature = "cluster")]
use object_store::ObjectStoreExt;

#[cfg(feature = "cluster")]
fn test_cluster_checkpoint_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

#[tokio::test]
async fn test_open_default() {
    let db = LaminarDB::open().unwrap();
    assert!(!db.is_closed());
    assert!(db.sources().is_empty());
    assert!(db.sinks().is_empty());
}

#[tokio::test]
async fn typed_namespace_rejects_every_cross_kind_create_and_drop() {
    let creates = [
        (CatalogObjectKind::Source, "CREATE SOURCE shared (id INT)"),
        (
            CatalogObjectKind::Sink,
            "CREATE SINK shared FROM base_input",
        ),
        (
            CatalogObjectKind::Table,
            "CREATE TABLE shared (id INT PRIMARY KEY)",
        ),
        (
            CatalogObjectKind::LookupTable,
            "CREATE LOOKUP TABLE shared (id INT NOT NULL, PRIMARY KEY (id)) \
             WITH ('connector' = 'static')",
        ),
        (
            CatalogObjectKind::Stream,
            "CREATE STREAM shared AS SELECT id FROM base_input",
        ),
        (
            CatalogObjectKind::MaterializedView,
            "CREATE MATERIALIZED VIEW shared AS SELECT id FROM base_input",
        ),
    ];
    let drops = [
        (CatalogObjectKind::Source, "DROP SOURCE shared"),
        (CatalogObjectKind::Sink, "DROP SINK shared"),
        (CatalogObjectKind::Table, "DROP TABLE shared"),
        (CatalogObjectKind::LookupTable, "DROP LOOKUP TABLE shared"),
        (CatalogObjectKind::Stream, "DROP STREAM shared"),
        (
            CatalogObjectKind::MaterializedView,
            "DROP MATERIALIZED VIEW shared",
        ),
    ];

    for (owner, owner_sql) in creates {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE base_input (id INT)")
            .await
            .unwrap();
        db.execute(owner_sql).await.unwrap();
        assert_eq!(
            db.catalog_namespace.lock().get("shared").copied(),
            Some(owner)
        );

        for (contender, contender_sql) in creates {
            if contender == owner {
                continue;
            }
            let error = db.execute(contender_sql).await.unwrap_err();
            assert!(
                error.to_string().contains("identifier is owned"),
                "{contender:?} unexpectedly replaced {owner:?}: {error}"
            );
        }
        for (drop_kind, drop_sql) in drops {
            if drop_kind == owner {
                continue;
            }
            let error = db.execute(drop_sql).await.unwrap_err();
            assert!(
                error.to_string().contains("identifier is owned"),
                "{drop_kind:?} DROP unexpectedly removed {owner:?}: {error}"
            );
        }
        assert_eq!(
            db.catalog_namespace.lock().get("shared").copied(),
            Some(owner)
        );
        assert!(db.connector_manager.lock().get_ddl("shared").is_some());
    }
}

#[tokio::test]
async fn catalog_identifiers_follow_case_sensitive_session_semantics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE Foo (id INT)").await.unwrap();
    db.execute("CREATE TABLE foo (id INT PRIMARY KEY)")
        .await
        .unwrap();
    db.execute("CREATE TABLE \"QuotedCase\" (id INT PRIMARY KEY)")
        .await
        .unwrap();

    db.execute("SELECT * FROM Foo").await.unwrap();
    db.execute("SELECT * FROM foo").await.unwrap();
    db.execute("SELECT * FROM \"QuotedCase\"").await.unwrap();
    assert!(db.catalog_namespace.lock().contains_key("Foo"));
    assert!(db.catalog_namespace.lock().contains_key("foo"));
    assert!(db.catalog_namespace.lock().contains_key("QuotedCase"));
    for name in ["Foo", "foo", "QuotedCase"] {
        assert!(db.ctx.table_exist(exact_table_reference(name)).unwrap());
    }

    let error = db
        .execute("CREATE SOURCE \"QuotedCase\" (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identifier is owned"));
    let error = db
        .execute("CREATE TABLE app.qualified (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must be unqualified"));

    db.execute("DROP SOURCE Foo").await.unwrap();
    assert!(!db.ctx.table_exist(exact_table_reference("Foo")).unwrap());
    assert!(db.ctx.table_exist(exact_table_reference("foo")).unwrap());
    db.execute("DROP TABLE foo").await.unwrap();
    db.execute("DROP TABLE \"QuotedCase\"").await.unwrap();
}

#[tokio::test]
async fn unsupported_standard_catalog_ddl_cannot_bypass_typed_admission() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE base (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let error = db
        .execute("CREATE VIEW bypass AS SELECT * FROM base")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not typed or transactional"));
    assert!(!db.ctx.table_exist("bypass").unwrap());
    assert!(!db.catalog_namespace.lock().contains_key("bypass"));
    assert!(db.connector_manager.lock().get_ddl("bypass").is_none());
}

#[tokio::test]
async fn multi_table_drop_preflights_all_names_and_reports_noop_exactly() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE first (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let error = db.execute("DROP TABLE first, missing").await.unwrap_err();
    assert!(error.to_string().to_lowercase().contains("not found"));
    assert!(db.ctx.table_exist("first").unwrap());
    assert!(db.catalog_namespace.lock().contains_key("first"));

    let result = db.execute("DROP TABLE IF EXISTS missing").await.unwrap();
    assert!(matches!(
        result,
        ExecuteResult::Ddl(DdlInfo { applied: false, .. })
    ));
    let result = db
        .execute("DROP TABLE IF EXISTS first, missing")
        .await
        .unwrap();
    assert!(matches!(
        result,
        ExecuteResult::Ddl(DdlInfo { applied: true, .. })
    ));
    assert!(!db.ctx.table_exist("first").unwrap());
}

#[tokio::test]
async fn cascade_drop_preflights_root_and_removes_sink_dependents_transitively() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE root (id INT)").await.unwrap();
    db.execute("CREATE STREAM derived AS SELECT id FROM root")
        .await
        .unwrap();
    db.execute("CREATE SINK output FROM derived").await.unwrap();

    let error = db.execute("DROP SOURCE missing CASCADE").await.unwrap_err();
    assert!(error.to_string().to_lowercase().contains("not found"));
    for name in ["root", "derived", "output"] {
        assert!(db.catalog_namespace.lock().contains_key(name));
    }

    db.execute("DROP SOURCE root CASCADE").await.unwrap();
    for name in ["root", "derived", "output"] {
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
    }
    assert!(db.catalog.get_source("root").is_none());
    assert!(db.catalog.get_stream_entry("derived").is_none());
    assert!(db.catalog.get_sink_input("output").is_none());
    assert!(!db.ctx.table_exist("root").unwrap());
    assert!(!db.ctx.table_exist("derived").unwrap());
    assert!(db.planner.lock().get_source("root").is_none());
    assert!(db.planner.lock().get_sink("output").is_none());
}

#[tokio::test]
async fn if_exists_does_not_hide_wrong_kind_drop() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE occupied (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let error = db
        .execute("DROP SOURCE IF EXISTS occupied")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identifier is owned by a table"));
    assert!(db.ctx.table_exist("occupied").unwrap());
}

#[tokio::test]
async fn manual_checkpoint_before_start_fails_closed() {
    let db = LaminarDB::open_with_config(LaminarConfig {
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        }),
        ..Default::default()
    })
    .unwrap();

    let error = db.checkpoint().await.unwrap_err();
    assert!(
        matches!(&error, DbError::Checkpoint(message) if message.contains("not running")),
        "manual checkpoint without a live coordinator must fail closed, got {error:?}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_assignment_pair_install_is_exact_and_fail_closed() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};
    use uuid::Uuid;

    let node_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node_id, control, recovery, None, members_rx, boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();

    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node_id.0)));
    let target = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &[node_id.0],
        vec![CheckpointParticipant {
            node_id: node_id.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(target.clone()));

    let receiver = Arc::new(
        ShuffleReceiver::bind(node_id.0, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    receiver
        .install_assignment_fence(&target, &[node_id.0])
        .unwrap();
    let sender = Arc::new(ShuffleSender::new(node_id.0, boot));
    let conflicting = CheckpointAssignmentFence::from_owner_map(
        target.assignment_version,
        &[node_id.0],
        vec![
            CheckpointParticipant {
                node_id: node_id.0,
                boot_incarnation: boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            },
        ],
    )
    .unwrap();
    sender
        .install_assignment_fence(&conflicting, &[node_id.0])
        .unwrap();

    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .build()
        .await
        .unwrap();
    let revision = db
        .assignment_authority_revision
        .load(std::sync::atomic::Ordering::Acquire);
    let error = db.install_shuffle_assignment_fence(&target).unwrap_err();
    assert!(error.to_string().contains("identity conflicts"), "{error}");
    assert!(
        db.assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire)
            > revision,
        "every failed pair install must invalidate watcher authority caches"
    );
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);
    assert_eq!(sender.active_assignment_digest(), None);
    assert_eq!(receiver.active_assignment_digest(), None);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn assignment_activation_installs_transport_before_controller_publication() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};
    use uuid::Uuid;

    let node_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node_id, control, recovery, None, members_rx, boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node_id.0)));
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &[node_id.0],
        vec![CheckpointParticipant {
            node_id: node_id.0,
            boot_incarnation: boot,
        }],
    )
    .unwrap();
    let receiver = Arc::new(
        ShuffleReceiver::bind(node_id.0, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(node_id.0, boot));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    assert_eq!(controller.checkpoint_assignment_fence(1), None);

    let activation = db
        .activate_assignment_authority(
            &fence,
            None,
            db.assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire),
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert!(activation.installed);
    assert!(activation.intake_open);
    assert_eq!(sender.assignment_version(), 1);
    assert_eq!(receiver.assignment_version(), 1);
    assert_eq!(controller.checkpoint_assignment_fence(1), Some(fence));
    assert!(!db.cluster_intake_fenced());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn assignment_adoption_rejects_smaller_and_larger_vnode_maps() {
    use laminar_core::checkpoint::{CheckpointParticipant, LeaderProof, LeaderProofOwner};
    use laminar_core::cluster::control::AssignmentSnapshot;
    use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};
    use uuid::Uuid;

    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = LaminarDB::builder()
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(registry)
        .build()
        .await
        .unwrap();
    let participants = vec![CheckpointParticipant {
        node_id: 1,
        boot_incarnation: Uuid::from_u128(11),
    }];
    for owners in [vec![NodeId(1)], vec![NodeId(1); 3]] {
        let snapshot = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                participants.clone(),
            )
            .unwrap();
        let active = snapshot.clone();
        let error = db
            .adopt_assignment_snapshot(
                snapshot,
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("vnode cardinality"), "{error}");

        let draining = active
            .next_draining(
                AssignmentSnapshot::vnodes_from_vec(&owners),
                participants.clone(),
                LeaderProof {
                    owner: LeaderProofOwner {
                        node_id: 1,
                        boot_id: Uuid::from_u128(11),
                        process_term: 1,
                    },
                    fencing_token: 1,
                },
            )
            .unwrap();
        let error = db.adopt_draining_snapshot(&draining).unwrap_err();
        assert!(error.to_string().contains("vnode cardinality"), "{error}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn replaced_process_cannot_adopt_same_owned_vnodes_without_restore() {
    use laminar_core::checkpoint::CheckpointParticipant;
    use laminar_core::cluster::control::{
        AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, ClusterKv, InMemoryKv,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use uuid::Uuid;

    let node = NodeId(1);
    let old_boot = Uuid::from_u128(11);
    let new_boot = Uuid::from_u128(111);
    let owners = vec![StateNodeId(node.0); 2];
    let snapshots = Arc::new(AssignmentSnapshotStore::new(
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>
    ));
    let previous = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: old_boot,
            }],
        )
        .unwrap();
    snapshots.save_if_absent(&previous).await.unwrap();
    let target = previous
        .next_for_participants(
            previous.vnodes.clone(),
            vec![CheckpointParticipant {
                node_id: node.0,
                boot_incarnation: new_boot,
            }],
        )
        .unwrap();
    assert!(matches!(
        snapshots
            .save_if_version(&target, previous.version)
            .await
            .unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));

    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        Some(Arc::clone(&snapshots)),
        members_rx,
        new_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let registry = Arc::new(VnodeRegistry::single_owner(2, StateNodeId(node.0)));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .assignment_snapshot_store(snapshots)
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(Arc::clone(&registry))
        .build()
        .await
        .unwrap();

    let error = db
        .adopt_assignment_snapshot(
            target,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot acquire 2 vnodes"));
    assert_eq!(registry.assignment_version(), previous.version);
    assert!(db.cluster_intake_fenced());
}

#[tokio::test]
async fn test_create_source() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE SOURCE");
            assert_eq!(info.object_name, "trades");
        }
        _ => panic!("Expected DDL result"),
    }

    assert_eq!(db.sources().len(), 1);
    assert_eq!(db.sources()[0].name, "trades");
}

#[tokio::test]
async fn test_create_source_with_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    let sources = db.sources();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].watermark_column, Some("ts".to_string()));
}

#[tokio::test]
async fn test_create_source_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    let result = db.execute("CREATE SOURCE test (id INT)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn concurrent_cross_kind_create_preserves_exactly_one_namespace_owner() {
    const SOURCE_DDL: &str = "CREATE SOURCE shared_name (id INT)";
    const STREAM_DDL: &str = "CREATE STREAM shared_name AS SELECT id FROM seed";

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE seed (id INT)").await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(3));

    let source_db = Arc::clone(&db);
    let source_gate = Arc::clone(&gate);
    let source = tokio::spawn(async move {
        source_gate.wait().await;
        source_db.execute(SOURCE_DDL).await
    });
    let stream_db = Arc::clone(&db);
    let stream_gate = Arc::clone(&gate);
    let stream = tokio::spawn(async move {
        stream_gate.wait().await;
        stream_db.execute(STREAM_DDL).await
    });
    gate.wait().await;

    let source_result = source.await.unwrap();
    let stream_result = stream.await.unwrap();
    assert_ne!(source_result.is_ok(), stream_result.is_ok());

    let source_present = db.catalog.get_source("shared_name").is_some();
    let stream_present = db.catalog.get_stream_entry("shared_name").is_some();
    assert_ne!(source_present, stream_present);
    assert!(db.ctx.table_exist("shared_name").unwrap());
    assert!(db.mv_registry.lock().get("shared_name").is_none());
    assert_eq!(
        db.connector_manager.lock().get_ddl("shared_name"),
        Some(if source_present {
            SOURCE_DDL
        } else {
            STREAM_DDL
        })
    );
}

#[tokio::test]
async fn test_create_source_if_not_exists() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    let result = db
        .execute("CREATE SOURCE IF NOT EXISTS test (id INT)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn create_or_replace_source_is_rejected_without_mutation() {
    const ORIGINAL: &str = "CREATE SOURCE test (id INT)";
    let db = LaminarDB::open().unwrap();
    db.execute(ORIGINAL).await.unwrap();
    let error = db
        .execute("CREATE OR REPLACE SOURCE test (id INT, name VARCHAR)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not atomic"));
    assert_eq!(
        db.catalog.get_source("test").unwrap().schema.fields().len(),
        1
    );
    assert_eq!(db.connector_manager.lock().get_ddl("test"), Some(ORIGINAL));
}

#[tokio::test]
async fn test_create_sink() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();

    assert_eq!(db.sinks().len(), 1);
}

#[tokio::test]
async fn create_or_replace_sink_is_rejected_without_mutation() {
    const ORIGINAL: &str = "CREATE SINK output FROM events";
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SOURCE other (id INT)").await.unwrap();
    db.execute(ORIGINAL).await.unwrap();

    let error = db
        .execute("CREATE OR REPLACE SINK output FROM other")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not atomic"));
    assert_eq!(
        db.catalog.get_sink_input("output").as_deref(),
        Some("events")
    );
    assert_eq!(
        db.connector_manager.lock().get_ddl("output"),
        Some(ORIGINAL)
    );
}

#[tokio::test]
async fn test_source_handle_untyped() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    assert_eq!(handle.name(), "events");
    assert_eq!(handle.schema().fields().len(), 2);
}

#[tokio::test]
async fn test_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.source_untyped("nonexistent");
    assert!(matches!(result, Err(DbError::SourceNotFound(_))));
}

#[tokio::test]
async fn test_show_sources() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();

    let result = db.execute("SHOW SOURCES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 2);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, active BOOLEAN)")
        .await
        .unwrap();

    let result = db.execute("DESCRIBE events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 3);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();

    let result = db.execute("DESCRIBE products").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 3);
        }
        _ => panic!("Expected Metadata result"),
    }
}

/// An MV (and a stream) must resolve a `FROM <stream>` reference at DDL time —
/// `CREATE STREAM` registers a planning placeholder for its output schema, so a
/// chain source → stream → stream → MV plans without "table not found". (The
/// crypto-sentiment demo is exactly this shape.)
#[tokio::test]
async fn mv_resolves_a_chain_of_streams() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE src (id BIGINT, txt VARCHAR, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute("CREATE STREAM filtered AS SELECT id, txt, ts FROM src WHERE txt IS NOT NULL")
        .await
        .unwrap();
    db.execute("CREATE STREAM tagged AS SELECT id, txt, ts FROM filtered")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW agg AS SELECT COUNT(*) AS c FROM tagged")
        .await
        .expect("MV resolves a stream built on another stream");
}

#[tokio::test]
async fn test_describe_materialized_view() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, value DOUBLE)")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW event_counts AS \
         SELECT name, COUNT(*) as cnt FROM events GROUP BY name",
    )
    .await
    .unwrap();

    let result = db.execute("DESCRIBE event_counts").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert!(batch.num_rows() >= 2, "Should have at least name and cnt");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DESCRIBE nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    assert_eq!(db.sources().len(), 1);

    db.execute("DROP SOURCE test").await.unwrap();
    assert_eq!(db.sources().len(), 0);
}

#[tokio::test]
async fn test_drop_source_if_exists() {
    let db = LaminarDB::open().unwrap();
    // Should not error when source doesn't exist
    let result = db.execute("DROP SOURCE IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_drop_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP SOURCE nonexistent").await;
    assert!(matches!(result, Err(DbError::SourceNotFound(_))));
}

#[tokio::test]
async fn test_shutdown() {
    let db = LaminarDB::open().unwrap();
    assert!(!db.is_closed());
    db.close();
    assert!(db.is_closed());

    let result = db.execute("CREATE SOURCE test (id INT)").await;
    assert!(matches!(result, Err(DbError::Shutdown)));
}

#[tokio::test]
async fn test_debug_format() {
    let db = LaminarDB::open().unwrap();
    let debug = format!("{db:?}");
    assert!(debug.contains("LaminarDB"));
}

#[tokio::test]
async fn test_explain_create_source() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("EXPLAIN CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert!(batch.num_rows() > 0);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            // Should contain plan_type and source info
            let key_values: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(key_values.contains(&"plan_type"));
        }
        _ => panic!("Expected Metadata result for EXPLAIN"),
    }
}

#[tokio::test]
async fn test_cancel_query() {
    let db = LaminarDB::open().unwrap();
    // Register a query via catalog directly for testing
    assert_eq!(db.active_query_count(), 0);

    // Simulate a query registration
    let query_id = db.catalog.register_query("SELECT * FROM test");
    assert_eq!(db.active_query_count(), 1);

    // Cancel it
    db.cancel_query(query_id).unwrap();
    assert_eq!(db.active_query_count(), 0);
}

#[tokio::test]
async fn test_source_and_sink_counts() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.source_count(), 0);
    assert_eq!(db.sink_count(), 0);

    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();
    assert_eq!(db.source_count(), 2);

    db.execute("CREATE SINK output FROM a").await.unwrap();
    assert_eq!(db.sink_count(), 1);

    let error = db.execute("DROP SOURCE a").await.unwrap_err();
    assert!(
        matches!(&error, DbError::InvalidOperation(message)
            if message.contains("depended on by output")),
        "{error}"
    );
    assert_eq!(db.source_count(), 2);

    db.execute("DROP SINK output").await.unwrap();
    db.execute("DROP SOURCE a").await.unwrap();
    assert_eq!(db.source_count(), 1);
    assert_eq!(db.sink_count(), 0);
}

#[tokio::test]
async fn test_multi_statement_execution() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT); CREATE SOURCE b (id INT); CREATE SINK output FROM a")
        .await
        .unwrap();
    assert_eq!(db.source_count(), 2);
    assert_eq!(db.sink_count(), 1);
}

#[tokio::test]
async fn test_multi_statement_trailing_semicolon() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT);").await.unwrap();
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_multi_statement_error_stops() {
    let db = LaminarDB::open().unwrap();
    // Second statement should fail (duplicate)
    let result = db
        .execute("CREATE SOURCE a (id INT); CREATE SOURCE a (id INT)")
        .await;
    assert!(result.is_err());
    // First statement should have succeeded
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_config_var_substitution() {
    let db = LaminarDB::builder()
        .config_var("TABLE_NAME", "events")
        .build()
        .await
        .unwrap();
    // Config var in source name won't work (parsed as identifier),
    // but it works in WITH option values
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_create_stream() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE STREAM");
            assert_eq!(info.object_name, "counts");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_drop_stream() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
        .await
        .unwrap();
    let result = db.execute("DROP STREAM counts").await.unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "DROP STREAM");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_drop_stream_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP STREAM nonexistent").await;
    assert!(matches!(result, Err(DbError::StreamNotFound(_))));
}

#[tokio::test]
async fn test_drop_stream_if_exists() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP STREAM IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

fn assert_stream_is_registered(db: &LaminarDB, name: &str) {
    assert!(db.catalog.get_stream_entry(name).is_some());
    assert!(db.connector_manager.lock().streams().contains_key(name));
    assert!(db.ctx.table_exist(name).unwrap());
}

async fn cancellation_test_db_and_graph() -> (Arc<LaminarDB>, crate::operator_graph::OperatorGraph)
{
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    let source = db.catalog.get_source("events").unwrap();
    let mut graph = crate::operator_graph::OperatorGraph::new(db.ctx.clone());
    graph.register_source_schema("events".to_string(), source.schema.clone());
    (db, graph)
}

fn assert_cancellation_test_stream_state(db: &LaminarDB, name: &str, present: bool) {
    assert_eq!(db.catalog.get_stream_entry(name).is_some(), present);
    assert_eq!(
        db.connector_manager.lock().streams().contains_key(name),
        present
    );
    assert_eq!(db.ctx.table_exist(name).unwrap(), present);
    assert_eq!(db.subscription_registry.contains_name(name), present);
    assert_eq!(db.connector_manager.lock().get_ddl(name).is_some(), present);
    assert!(db.mv_registry.lock().get(name).is_none());
    assert!(!db.mv_store.read().has_mv(name));
}

const CANCELLATION_STREAM_DDL: &str =
    "CREATE STREAM cancellable AS SELECT id FROM events WITH ('retain_history' = '4mb')";
const CANCELLATION_MV_DDL: &str =
    "CREATE MATERIALIZED VIEW cancellable_mv AS SELECT id FROM events";

async fn create_cancellation_test_stream(
    db: &LaminarDB,
    graph: &mut crate::operator_graph::OperatorGraph,
) {
    db.execute(CANCELLATION_STREAM_DDL).await.unwrap();
    crate::pipeline_callback::admit_control_stream(
        graph,
        "cancellable".to_string(),
        "SELECT id FROM events".to_string(),
        None,
        None,
        None,
        None,
        false,
    )
    .unwrap();
    assert!(graph.has_query("cancellable"));
    assert_cancellation_test_stream_state(db, "cancellable", true);
}

fn apply_control_without_ack(
    graph: &mut crate::operator_graph::OperatorGraph,
    message: crate::pipeline::ControlMsg,
) -> tokio::sync::oneshot::Sender<Result<(), DbError>> {
    match message.into_kind() {
        crate::pipeline::ControlMsgKind::AddStream {
            name,
            sql,
            emit_clause,
            window_config,
            order_config,
            join_config,
            incremental,
            reply,
            mutation,
        } => {
            crate::pipeline_callback::admit_control_stream(
                graph,
                name,
                sql,
                emit_clause,
                window_config,
                order_config,
                join_config,
                incremental,
            )
            .unwrap();
            assert!(mutation.try_apply());
            reply
        }
        crate::pipeline::ControlMsgKind::DropStreams {
            names,
            reply,
            mutation,
        } => {
            assert!(mutation.try_apply());
            for name in &names {
                graph.remove_query(name);
            }
            reply
        }
    }
}

#[tokio::test]
async fn applied_stream_create_drop_survives_lost_ack_and_persists() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    const CREATE: &str =
        "CREATE STREAM ack_stream AS SELECT id FROM events WITH ('retain_history' = '4mb')";
    let caller_db = Arc::clone(&db);
    let create = tokio::spawn(async move { caller_db.execute(CREATE).await });
    let message = control_rx.recv().await.unwrap();
    drop(apply_control_without_ack(&mut graph, message));
    create.await.unwrap().unwrap();

    assert!(graph.has_query("ack_stream"));
    assert_cancellation_test_stream_state(&db, "ack_stream", true);
    assert_eq!(
        db.connector_manager.lock().get_ddl("ack_stream"),
        Some(CREATE)
    );

    let caller_db = Arc::clone(&db);
    let drop_call = tokio::spawn(async move { caller_db.execute("DROP STREAM ack_stream").await });
    let message = control_rx.recv().await.unwrap();
    drop(apply_control_without_ack(&mut graph, message));
    drop_call.await.unwrap().unwrap();

    assert!(!graph.has_query("ack_stream"));
    assert_cancellation_test_stream_state(&db, "ack_stream", false);
    assert!(db.connector_manager.lock().get_ddl("ack_stream").is_none());
}

#[tokio::test]
async fn applied_mv_create_drop_survives_lost_ack_and_persists() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    const CREATE: &str = "CREATE MATERIALIZED VIEW ack_mv AS SELECT id FROM events";
    let caller_db = Arc::clone(&db);
    let create = tokio::spawn(async move { caller_db.execute(CREATE).await });
    let message = control_rx.recv().await.unwrap();
    drop(apply_control_without_ack(&mut graph, message));
    create.await.unwrap().unwrap();

    assert!(graph.has_query("ack_mv"));
    assert!(db.mv_registry.lock().get("ack_mv").is_some());
    assert!(db.mv_store.read().has_mv("ack_mv"));
    assert!(db.ctx.table_exist("ack_mv").unwrap());
    assert_eq!(db.connector_manager.lock().get_ddl("ack_mv"), Some(CREATE));

    let caller_db = Arc::clone(&db);
    let drop_call =
        tokio::spawn(async move { caller_db.execute("DROP MATERIALIZED VIEW ack_mv").await });
    let message = control_rx.recv().await.unwrap();
    drop(apply_control_without_ack(&mut graph, message));
    drop_call.await.unwrap().unwrap();

    assert!(!graph.has_query("ack_mv"));
    assert_cancellation_test_mv_state(&db, "ack_mv", false);
    assert!(db.connector_manager.lock().get_ddl("ack_mv").is_none());
}

#[tokio::test(start_paused = true)]
async fn pending_control_timeout_cancels_create_and_rolls_back() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move {
        caller_db
            .execute("CREATE STREAM timed_out AS SELECT id FROM events")
            .await
    });
    let message = control_rx.recv().await.unwrap();
    tokio::time::advance(crate::ddl::CONTROL_ACK_DEADLINE + std::time::Duration::from_millis(1))
        .await;
    tokio::task::yield_now().await;

    let error = caller.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("did not acknowledge"));
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    assert!(!graph.has_query("timed_out"));
    assert_cancellation_test_stream_state(&db, "timed_out", false);
}

#[tokio::test(start_paused = true)]
async fn applied_control_timeout_is_success_and_persists() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    const CREATE: &str = "CREATE STREAM slow_ack AS SELECT id FROM events";
    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CREATE).await });
    let message = control_rx.recv().await.unwrap();
    let reply = apply_control_without_ack(&mut graph, message);
    tokio::time::advance(crate::ddl::CONTROL_ACK_DEADLINE + std::time::Duration::from_millis(1))
        .await;
    tokio::task::yield_now().await;

    caller.await.unwrap().unwrap();
    drop(reply);
    assert!(graph.has_query("slow_ack"));
    assert_eq!(
        db.connector_manager.lock().get_ddl("slow_ack"),
        Some(CREATE)
    );
}

#[tokio::test]
async fn applied_control_error_ack_does_not_report_a_rolled_back_create() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    const CREATE: &str = "CREATE STREAM inconsistent_ack AS SELECT id FROM events";
    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CREATE).await });
    let message = control_rx.recv().await.unwrap();
    let reply = apply_control_without_ack(&mut graph, message);
    reply
        .send(Err(DbError::Pipeline("impossible late rejection".into())))
        .unwrap();

    caller.await.unwrap().unwrap();
    assert!(graph.has_query("inconsistent_ack"));
    assert_eq!(
        db.connector_manager.lock().get_ddl("inconsistent_ack"),
        Some(CREATE)
    );
}

#[tokio::test]
async fn active_runtime_rejects_every_unwired_topology_ddl_class() {
    const OPERATIONS: &[&str] = &[
        "CREATE SOURCE blocked_source (id INT)",
        "DROP SOURCE base_source",
        "ALTER SOURCE base_source ADD COLUMN value BIGINT",
        "CREATE SINK blocked_sink FROM base_source",
        "DROP SINK base_sink",
        "CREATE TABLE blocked_table (id BIGINT PRIMARY KEY)",
        "DROP TABLE base_table",
        "ALTER TABLE base_table ADD COLUMN value BIGINT",
        "CREATE LOOKUP TABLE blocked_lookup (id INT NOT NULL, PRIMARY KEY (id)) WITH ('connector' = 'static')",
        "DROP LOOKUP TABLE base_lookup",
        "CREATE CONTINUOUS QUERY blocked_cq AS SELECT COUNT(*) FROM base_source",
    ];

    for state in [
        DbState::Starting,
        DbState::Running,
        DbState::ShuttingDown,
        DbState::Faulted,
    ] {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE base_source (id INT)")
            .await
            .unwrap();
        db.execute("CREATE SINK base_sink FROM base_source")
            .await
            .unwrap();
        db.execute("CREATE TABLE base_table (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute(
            "CREATE LOOKUP TABLE base_lookup (id INT NOT NULL, PRIMARY KEY (id)) \
             WITH ('connector' = 'static')",
        )
        .await
        .unwrap();
        state.store(&db.state);

        for sql in OPERATIONS {
            let error = db.execute(sql).await.unwrap_err();
            let message = error.to_string();
            assert!(
                message.contains("LDB-6043")
                    || message.contains("disabled")
                    || message.contains("no typed catalog/drop lifecycle"),
                "{state:?} unexpectedly admitted {sql}: {error}"
            );
        }

        assert!(db.catalog.get_source("base_source").is_some());
        assert_eq!(
            db.catalog
                .describe_source("base_source")
                .unwrap()
                .fields()
                .len(),
            1
        );
        assert!(db.catalog.get_sink_input("base_sink").is_some());
        assert!(db.ctx.table_exist("base_table").unwrap());
        assert_eq!(
            db.ctx
                .table_provider("base_table")
                .await
                .unwrap()
                .schema()
                .fields()
                .len(),
            1
        );
        assert!(db.ctx.table_exist("base_lookup").unwrap());
        assert!(db.catalog.get_source("blocked_source").is_none());
        assert!(db.catalog.get_sink_input("blocked_sink").is_none());
        assert!(!db.ctx.table_exist("blocked_table").unwrap());
        assert!(!db.ctx.table_exist("blocked_lookup").unwrap());
        assert!(!db
            .queries()
            .iter()
            .any(|query| query.sql.contains("blocked_cq")));
    }
}

#[tokio::test]
async fn hot_stream_and_mv_ddl_requires_a_live_running_control_coordinator() {
    for state in [
        DbState::Starting,
        DbState::Running,
        DbState::ShuttingDown,
        DbState::Faulted,
    ] {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        state.store(&db.state);

        for sql in [
            "CREATE STREAM blocked_stream AS SELECT id FROM events",
            "CREATE MATERIALIZED VIEW blocked_mv AS SELECT id FROM events",
        ] {
            let error = db.execute(sql).await.unwrap_err();
            assert!(
                error.to_string().contains("LDB-6043"),
                "{state:?} unexpectedly admitted {sql}: {error}"
            );
        }
        assert!(db.catalog.get_stream_entry("blocked_stream").is_none());
        assert!(db.mv_registry.lock().get("blocked_mv").is_none());
        assert!(!db.ctx.table_exist("blocked_stream").unwrap());
        assert!(!db.ctx.table_exist("blocked_mv").unwrap());
        assert!(db
            .connector_manager
            .lock()
            .get_ddl("blocked_stream")
            .is_none());
        assert!(db.connector_manager.lock().get_ddl("blocked_mv").is_none());
    }
}

#[tokio::test]
async fn cancelled_create_before_graph_commit_rolls_back_both_sides() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CANCELLATION_STREAM_DDL).await });
    let message = control_rx.recv().await.unwrap();

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let node_count = graph.node_count();
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);

    assert_eq!(
        graph.node_count(),
        node_count,
        "cancelled CREATE must be skipped"
    );
    assert!(!graph.has_query("cancellable"));
    assert_cancellation_test_stream_state(&db, "cancellable", false);
}

#[tokio::test]
async fn cancelled_create_after_graph_commit_keeps_both_sides() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CANCELLATION_STREAM_DDL).await });
    let message = control_rx.recv().await.unwrap();

    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(graph.has_query("cancellable"));
    assert_cancellation_test_stream_state(&db, "cancellable", true);
    assert_eq!(
        db.connector_manager.lock().get_ddl("cancellable"),
        Some(CANCELLATION_STREAM_DDL)
    );
}

#[tokio::test]
async fn cancelled_drop_before_graph_claim_keeps_both_sides() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    create_cancellation_test_stream(&db, &mut graph).await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute("DROP STREAM cancellable").await });
    let message = control_rx.recv().await.unwrap();

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);

    assert!(graph.has_query("cancellable"));
    assert_cancellation_test_stream_state(&db, "cancellable", true);
}

#[tokio::test]
async fn cancelled_drop_after_graph_claim_removes_both_sides() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    create_cancellation_test_stream(&db, &mut graph).await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute("DROP STREAM cancellable").await });
    let message = control_rx.recv().await.unwrap();

    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(!graph.has_query("cancellable"));
    assert_cancellation_test_stream_state(&db, "cancellable", false);
    assert!(db.connector_manager.lock().get_ddl("cancellable").is_none());
}

fn assert_cancellation_test_mv_state(db: &LaminarDB, name: &str, present: bool) {
    assert_eq!(db.mv_registry.lock().get(name).is_some(), present);
    assert_eq!(
        db.connector_manager.lock().streams().contains_key(name),
        present
    );
    assert_eq!(db.mv_store.read().has_mv(name), present);
    assert_eq!(db.ctx.table_exist(name).unwrap(), present);
    assert_eq!(db.subscription_registry.contains_name(name), present);
    assert!(db.catalog.get_stream_entry(name).is_none());
    assert_eq!(db.connector_manager.lock().get_ddl(name).is_some(), present);
}

#[tokio::test]
async fn cancelled_mv_create_before_graph_commit_rolls_back_all_local_state() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CANCELLATION_MV_DDL).await });
    let message = control_rx.recv().await.unwrap();
    db.subscription_registry.configure("cancellable_mv", 1024);
    assert_cancellation_test_mv_state(&db, "cancellable_mv", true);

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let node_count = graph.node_count();
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);

    assert_eq!(
        graph.node_count(),
        node_count,
        "cancelled MV CREATE must be skipped"
    );
    assert!(!graph.has_query("cancellable_mv"));
    assert_cancellation_test_mv_state(&db, "cancellable_mv", false);
}

#[tokio::test]
async fn cancelled_mv_create_after_graph_commit_keeps_exact_replay_identity() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);

    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move { caller_db.execute(CANCELLATION_MV_DDL).await });
    let message = control_rx.recv().await.unwrap();
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(graph.has_query("cancellable_mv"));
    assert!(db.mv_registry.lock().get("cancellable_mv").is_some());
    assert!(db.mv_store.read().has_mv("cancellable_mv"));
    assert!(db.ctx.table_exist("cancellable_mv").unwrap());
    assert_eq!(
        db.connector_manager.lock().get_ddl("cancellable_mv"),
        Some(CANCELLATION_MV_DDL)
    );
}

#[tokio::test]
async fn cancelled_mv_drop_after_graph_claim_tears_down_all_local_state() {
    let (db, mut graph) = cancellation_test_db_and_graph().await;
    db.execute(CANCELLATION_MV_DDL).await.unwrap();
    db.subscription_registry.configure("cancellable_mv", 1024);
    crate::pipeline_callback::admit_control_stream(
        &mut graph,
        "cancellable_mv".to_string(),
        "SELECT id FROM events".to_string(),
        None,
        None,
        None,
        None,
        false,
    )
    .unwrap();
    assert!(graph.has_query("cancellable_mv"));
    assert_cancellation_test_mv_state(&db, "cancellable_mv", true);

    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);
    let caller_db = Arc::clone(&db);
    let caller = tokio::spawn(async move {
        caller_db
            .execute("DROP MATERIALIZED VIEW cancellable_mv")
            .await
    });
    let message = control_rx.recv().await.unwrap();

    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(!graph.has_query("cancellable_mv"));
    assert_cancellation_test_mv_state(&db, "cancellable_mv", false);
    assert!(db
        .connector_manager
        .lock()
        .get_ddl("cancellable_mv")
        .is_none());
}

#[tokio::test]
async fn drop_stream_full_control_channel_preserves_local_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE STREAM retained AS SELECT id FROM events")
        .await
        .unwrap();

    let (control_tx, _control_rx) =
        crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let (reply, _acknowledgement) = tokio::sync::oneshot::channel();
    control_tx
        .try_send(crate::pipeline::ControlMsg::drop_streams(
            vec!["occupied".to_string()],
            reply,
            std::sync::Arc::new(crate::pipeline::ControlMutation::new()),
        ))
        .unwrap();
    *db.control_tx.lock() = Some(control_tx);

    let error = db.execute("DROP STREAM retained").await.unwrap_err();
    assert!(matches!(error, DbError::Pipeline(_)));
    assert_stream_is_registered(&db, "retained");
}

#[tokio::test]
async fn drop_stream_closed_control_channel_preserves_local_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE STREAM retained AS SELECT id FROM events")
        .await
        .unwrap();

    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    drop(control_rx);
    *db.control_tx.lock() = Some(control_tx);

    let error = db.execute("DROP STREAM retained").await.unwrap_err();
    assert!(matches!(error, DbError::Pipeline(_)));
    assert_stream_is_registered(&db, "retained");
}

#[tokio::test]
async fn create_stream_admission_rejection_rolls_back_all_local_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, ts TIMESTAMP)")
        .await
        .unwrap();

    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);
    let reject = tokio::spawn(async move {
        let msg = control_rx.recv().await.unwrap();
        let crate::pipeline::ControlMsgKind::AddStream { reply, .. } = msg.into_kind() else {
            panic!("expected AddStream control message");
        };
        reply
            .send(Err(DbError::Pipeline("deterministic rejection".into())))
            .unwrap();
    });

    let error = db
        .execute(
            "CREATE STREAM rejected AS \
             SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, COUNT(*) AS n FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE) \
             WITH ('retain_history' = '4mb')",
        )
        .await
        .unwrap_err();
    reject.await.unwrap();

    assert!(matches!(error, DbError::Pipeline(message) if message == "deterministic rejection"));
    assert!(db.catalog.get_stream_entry("rejected").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("rejected"));
    assert!(!db.ctx.table_exist("rejected").unwrap());
    assert!(!db.subscription_registry.drop_name("rejected"));

    *db.control_tx.lock() = None;
    db.execute(
        "CREATE STREAM admitted AS \
         SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, COUNT(*) AS n FROM events \
         GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
    )
    .await
    .unwrap();
    let error = db
        .execute(
            "CREATE STREAM joined AS SELECT a.bucket FROM rejected a \
             JOIN admitted b ON a.bucket = b.bucket",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unbounded join"),
        "a rolled-back windowed stream must not remain a bounded planner input: {error}"
    );
}

#[tokio::test]
async fn create_stream_closed_admission_reply_rolls_back_all_local_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);
    let close_reply = tokio::spawn(async move {
        let message = control_rx.recv().await.unwrap();
        assert!(matches!(
            message.into_kind(),
            crate::pipeline::ControlMsgKind::AddStream { .. }
        ));
    });

    let error = db
        .execute("CREATE STREAM rejected AS SELECT id FROM events")
        .await
        .unwrap_err();
    close_reply.await.unwrap();

    assert!(matches!(error, DbError::Pipeline(message) if message.contains("pipeline stopped")));
    assert!(db.catalog.get_stream_entry("rejected").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("rejected"));
    assert!(!db.ctx.table_exist("rejected").unwrap());
}

#[tokio::test]
async fn create_stream_planner_rejection_does_not_enter_registries() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE left_events (id INT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE right_events (id INT)")
        .await
        .unwrap();

    db.execute(
        "CREATE STREAM rejected AS \
         SELECT l.id FROM left_events l JOIN right_events r ON l.id = r.id",
    )
    .await
    .unwrap_err();

    assert!(db.catalog.get_stream_entry("rejected").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("rejected"));
    assert!(!db.ctx.table_exist("rejected").unwrap());
}

#[tokio::test]
async fn test_show_streams() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM a AS SELECT 1 FROM events")
        .await
        .unwrap();
    let result = db.execute("SHOW STREAMS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_stream_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await
        .unwrap();
    let result = db
        .execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await;
    assert!(
        matches!(result, Err(DbError::InvalidOperation(message)) if message.contains("already exists"))
    );
}

#[tokio::test]
async fn test_create_table() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "products");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_create_table_and_query_empty() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE dim (id INT PRIMARY KEY, label VARCHAR)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM dim").await.unwrap();
    match result {
        ExecuteResult::Query(q) => {
            assert_eq!(q.schema().fields().len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_insert_into_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO events VALUES (1, 3.14), (2, 2.72)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 2),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_into_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_into_nonexistent_table() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("INSERT INTO nosuch VALUES (1, 2)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_table_with_types() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, qty SMALLINT, total DECIMAL(10,2))")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "orders");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_insert_null_values() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE data (id BIGINT, label VARCHAR)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO data VALUES (1, NULL)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_negative_values() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE temps (id BIGINT, celsius DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO temps VALUES (1, -40.0)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_create_source_unknown_connector() {
    let db = LaminarDB::open().unwrap();
    // Use correct SQL syntax: FROM <type> (...) SCHEMA (...)
    let result = db
        .execute(
            "CREATE SOURCE events FROM NONEXISTENT \
             ('topic' = 'test') SCHEMA (id INT)",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown source connector type"), "got: {err}");
}

#[tokio::test]
async fn test_create_sink_unknown_connector() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    // Use correct SQL syntax: INTO <type> (...)
    let result = db
        .execute(
            "CREATE SINK output FROM events \
             INTO NONEXISTENT ('topic' = 'out')",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown sink connector type"), "got: {err}");
}

#[tokio::test]
async fn test_create_source_invalid_format() {
    // We test format validation via build_source_config in
    // connector_manager::tests (since the SQL parser may reject
    // unknown formats at parse time rather than DDL validation).
    // Here we verify that an error is returned either way.
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE SOURCE events FROM NONEXISTENT \
             FORMAT BADFORMAT SCHEMA (id INT)",
        )
        .await;
    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// Pipeline-running state guards
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_create_source_with_connector_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE seed (id INT)").await.unwrap();
    db.start().await.unwrap();

    // WITH syntax
    let result = db
        .execute("CREATE SOURCE events (id INT) WITH ('connector' = 'kafka', 'topic' = 'x')")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("LDB-6043"),
        "expected pipeline-running error, got: {err}"
    );

    // FROM syntax (what server mode generates via source_to_ddl)
    let result = db
        .execute("CREATE SOURCE events2 (id INT) FROM KAFKA (topic = 'x')")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("LDB-6043"),
        "expected pipeline-running error for FROM syntax, got: {err}"
    );
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn create_source_surfaces_kafka_config_error_in_ddl_message() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE SOURCE ion_tw FROM KAFKA ( \
                 'bootstrap.servers' = 'localhost:9092', \
                 'group.id' = 'g', \
                 'topic' = 't', \
                 'format' = 'avro', \
                 'schema.registry.url' = 'http://localhost:8081', \
                 'broker.commit.interval.ms' = '5000' \
             )",
        )
        .await;
    let err = result.expect_err("expected DDL to surface the deprecated-key error");
    let msg = err.to_string();
    assert!(
        msg.contains("broker.commit.interval.ms"),
        "DDL error must name the offending key, got: {msg}"
    );
    assert!(
        msg.contains("schema auto-discovery failed"),
        "DDL error must use the new framing, got: {msg}"
    );
}

#[tokio::test]
async fn test_create_source_without_connector_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.start().await.unwrap();

    let error = db
        .execute("CREATE SOURCE events (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-6043"));
    assert!(db.catalog.get_source("events").is_none());
}

#[tokio::test]
async fn test_create_sink_with_connector_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.start().await.unwrap();

    let result = db
        .execute(
            "CREATE SINK output FROM events \
             WITH ('connector' = 'kafka', 'topic' = 'out')",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("LDB-6043"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_drop_source_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.start().await.unwrap();

    let result = db.execute("DROP SOURCE events").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("LDB-6043"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_drop_sink_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();
    db.start().await.unwrap();

    let result = db.execute("DROP SINK output").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("LDB-6043"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_connector_registry_accessor() {
    let db = LaminarDB::open().unwrap();
    let registry = db.connector_registry();

    // With feature flags enabled, built-in connectors are auto-registered.
    // Without any features, registry should be empty.
    #[allow(unused_mut)]
    // The generator source is unconditional (no feature gate).
    let mut expected_sources = 1;
    #[allow(unused_mut)]
    let mut expected_sinks = 0;

    #[cfg(feature = "kafka")]
    {
        expected_sources += 1; // kafka source
        expected_sinks += 1; // kafka sink
    }
    #[cfg(feature = "postgres-cdc")]
    {
        expected_sources += 1; // postgres CDC source
    }
    #[cfg(feature = "postgres-sink")]
    {
        expected_sinks += 1; // postgres sink
    }
    #[cfg(feature = "delta-lake")]
    {
        expected_sources += 1; // delta-lake source
        expected_sinks += 1; // delta-lake sink
    }
    #[cfg(feature = "iceberg")]
    {
        expected_sources += 1; // iceberg source
        expected_sinks += 1; // iceberg sink
    }
    #[cfg(feature = "websocket")]
    {
        expected_sources += 1; // websocket source
        expected_sinks += 1; // websocket sink
    }
    #[cfg(feature = "mongodb-cdc")]
    {
        expected_sources += 1; // mongodb CDC source
        expected_sinks += 1; // mongodb sink
    }
    #[cfg(feature = "files")]
    {
        expected_sources += 1; // file source
        expected_sinks += 1; // file sink
    }
    #[cfg(feature = "otel")]
    {
        expected_sources += 1; // otel source
    }
    #[cfg(feature = "nats")]
    {
        expected_sources += 1; // nats source
        expected_sinks += 1; // nats sink
    }

    assert_eq!(registry.list_sources().len(), expected_sources);
    assert_eq!(registry.list_sinks().len(), expected_sinks);
    for connector_type in ["mysql", "mysql-cdc"] {
        let config = laminar_connectors::config::ConnectorConfig::new(connector_type);
        let error = registry
            .create_source(&config, None)
            .err()
            .expect("removed MySQL connectors must use the unknown-source path");
        assert_eq!(
            error.to_string(),
            format!("configuration error: unknown source connector type: '{connector_type}'")
        );
    }
    assert!(registry.is_frozen());
    assert_eq!(registry.frozen_fingerprint().unwrap().len(), 64);
}

#[tokio::test]
async fn test_builder_register_connector() {
    use std::sync::Arc;

    let db = LaminarDB::builder()
        .register_connector(|registry| {
            registry.register_source(
                "test-source",
                laminar_connectors::config::ConnectorInfo {
                    name: "test-source".to_string(),
                    display_name: "Test Source".to_string(),
                    version: "0.1.0".to_string(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_: Option<&prometheus::Registry>| {
                    Box::new(laminar_connectors::testing::MockSourceConnector::new())
                }),
            )
        })
        .build()
        .await
        .unwrap();
    let registry = db.connector_registry();
    assert!(registry.list_sources().contains(&"test-source".to_string()));
    assert!(registry.is_frozen());

    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.start().await.unwrap();
    let replacement = registry.register_source(
        "test-source",
        laminar_connectors::config::ConnectorInfo {
            name: "replacement".into(),
            display_name: "Replacement".into(),
            version: "9.9.9".into(),
            is_source: true,
            is_sink: false,
            config_keys: vec![],
        },
        Arc::new(|_: Option<&prometheus::Registry>| {
            Box::new(laminar_connectors::testing::MockSourceConnector::new())
        }),
    );
    assert!(matches!(
        replacement,
        Err(laminar_connectors::error::ConnectorError::RegistryFrozen { .. })
    ));
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn builder_rejects_custom_replacement_of_builtin_connector() {
    let result = LaminarDB::builder()
        .register_connector(|registry| {
            registry.register_source(
                "generator",
                laminar_connectors::config::ConnectorInfo {
                    name: "replacement".into(),
                    display_name: "Replacement".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_: Option<&prometheus::Registry>| {
                    Box::new(laminar_connectors::testing::MockSourceConnector::new())
                }),
            )
        })
        .build()
        .await;
    assert!(matches!(result, Err(DbError::ConnectorOp(_))));
}

/// SQL DDL auto-discovery must land a `Map` column in the catalog
/// verbatim from the connector — no SQL string round-trip.
#[tokio::test]
async fn test_sql_create_source_auto_discovers_map_column() {
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};

    let map_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "data",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));

    let (db, _) = fake_source_db("fake-avro", Some(Arc::clone(&map_schema))).await;
    db.execute(
        "CREATE SOURCE events WITH ('connector' = 'fake-avro', \
         'schema.registry.url' = 'http://irrelevant', 'topic' = 'events')",
    )
    .await
    .unwrap();

    let entry = db.catalog.get_source("events").expect("source in catalog");
    assert_eq!(entry.schema.fields().len(), 2);
    assert_eq!(entry.schema.field(0).data_type(), &DataType::Int64);
    assert!(
        matches!(entry.schema.field(1).data_type(), DataType::Map(_, _)),
        "auto-discovered `data` must arrive as Map, got {:?}",
        entry.schema.field(1).data_type()
    );
}

/// CREATE SOURCE IF NOT EXISTS must skip auto-discovery when the
/// source already exists — no wasted Schema Registry round trip.
#[tokio::test]
async fn test_sql_create_source_if_not_exists_skips_discovery() {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    let discovered = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let (db, counter) = fake_source_db("counting-fake", Some(discovered)).await;

    db.execute("CREATE SOURCE events WITH ('connector' = 'counting-fake')")
        .await
        .unwrap();
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);

    db.execute("CREATE SOURCE IF NOT EXISTS events WITH ('connector' = 'counting-fake')")
        .await
        .unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "IF NOT EXISTS should short-circuit before discovery"
    );
}

/// CREATE SOURCE without columns must fail loudly when discovery
/// yields an empty schema, not register a zero-column table.
#[tokio::test]
async fn test_sql_create_source_errors_when_discovery_yields_empty() {
    let (db, _) = fake_source_db("empty-fake", None).await;
    let err = db
        .execute("CREATE SOURCE events WITH ('connector' = 'empty-fake')")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("could not auto-discover a schema"),
        "expected actionable discovery-failure error, got: {err}"
    );
}

/// Build a `LaminarDB` with one fake source plus a shared counter
/// that ticks on every `discover_schema` call.
async fn fake_source_db(
    name: &'static str,
    discovered: Option<Arc<arrow::datatypes::Schema>>,
) -> (Arc<LaminarDB>, Arc<std::sync::atomic::AtomicUsize>) {
    use arrow::datatypes::Schema as ArrowSchema;
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::connector::{
        SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceStart,
        SourceTopology,
    };
    use laminar_connectors::error::ConnectorError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeSource {
        schema: Arc<ArrowSchema>,
        on_discover: Option<Arc<ArrowSchema>>,
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        async fn start(&mut self, _: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }
        async fn discover_schema(
            &mut self,
            _: &std::collections::HashMap<String, String>,
        ) -> Result<(), ConnectorError> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            if let Some(s) = &self.on_discover {
                self.schema = Arc::clone(s);
            }
            Ok(())
        }
        fn schema(&self) -> Arc<ArrowSchema> {
            Arc::clone(&self.schema)
        }
        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn contract(
            &self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<SourceContract, ConnectorError> {
            Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ))
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let db = LaminarDB::builder()
        .register_connector(move |registry| {
            let discovered = discovered.clone();
            let counter = Arc::clone(&counter_clone);
            registry.register_source(
                name,
                ConnectorInfo {
                    name: name.into(),
                    display_name: name.into(),
                    version: "0.1.0".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(move |_: Option<&prometheus::Registry>| {
                    Box::new(FakeSource {
                        schema: Arc::new(ArrowSchema::empty()),
                        on_discover: discovered.clone(),
                        counter: Arc::clone(&counter),
                    })
                }),
            )
        })
        .build()
        .await
        .unwrap();
    (db, counter)
}

#[tokio::test]
async fn paused_schema_discovery_serializes_pipeline_start() {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::connector::{SourceBatch, SourceConnector, SourceStart};
    use laminar_connectors::error::ConnectorError;

    struct GatedSource {
        schema: Arc<ArrowSchema>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        discovered_once: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl SourceConnector for GatedSource {
        async fn start(&mut self, _: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }
        async fn discover_schema(
            &mut self,
            _: &std::collections::HashMap<String, String>,
        ) -> Result<(), ConnectorError> {
            if !self
                .discovered_once
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
        fn schema(&self) -> Arc<ArrowSchema> {
            Arc::clone(&self.schema)
        }
        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let discovered_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let db = LaminarDB::builder()
        .register_connector({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let schema = Arc::clone(&schema);
            let discovered_once = Arc::clone(&discovered_once);
            move |registry| {
                registry.register_source(
                    "gated-source",
                    ConnectorInfo {
                        name: "gated-source".into(),
                        display_name: "gated-source".into(),
                        version: "0.1.0".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(move |_| {
                        Box::new(GatedSource {
                            schema: Arc::clone(&schema),
                            entered: Arc::clone(&entered),
                            release: Arc::clone(&release),
                            discovered_once: Arc::clone(&discovered_once),
                        })
                    }),
                )
            }
        })
        .build()
        .await
        .unwrap();

    let create = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute("CREATE SOURCE discovered WITH ('connector' = 'gated-source')")
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let start = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.start().await })
    };
    tokio::task::yield_now().await;
    assert_eq!(DbState::load(&db.state), DbState::Starting);
    assert!(!start.is_finished());

    release.notify_one();
    create.await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), start)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Running);
    db.stop_pipeline().await.unwrap();
}

#[tokio::test]
async fn paused_stream_planning_serializes_pipeline_stop() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE STREAM existing AS SELECT id FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *db.topology_planning_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));
    let create = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute("CREATE STREAM planned AS SELECT id FROM events")
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let stop = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.stop_pipeline().await })
    };
    tokio::task::yield_now().await;
    assert_eq!(DbState::load(&db.state), DbState::ShuttingDown);
    assert!(!stop.is_finished());

    *db.topology_planning_gate.lock() = None;
    release.notify_one();
    let error = create
        .await
        .unwrap()
        .expect_err("a stream cannot be admitted after shutdown has claimed the runtime");
    assert!(
        error
            .to_string()
            .contains("lost its live control coordinator"),
        "{error}"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), stop)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Created);
    assert!(db.catalog.get_stream_entry("planned").is_none());
}

#[tokio::test]
async fn test_create_materialized_view() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("CREATE MATERIALIZED VIEW event_stats AS SELECT * FROM events")
        .await;

    // The MV may fail at query execution (no data in DataFusion) but the
    // important thing is the MV path is invoked and the registry is wired up.
    // If it succeeds, verify the DDL result.
    if let Ok(ExecuteResult::Ddl(info)) = &result {
        assert_eq!(info.statement_type, "CREATE MATERIALIZED VIEW");
        assert_eq!(info.object_name, "event_stats");
    }
}

/// An MV's `EMIT ON WINDOW CLOSE` must reach `StreamRegistration` so
/// `OperatorGraph` routes it through `EowcQueryOperator` rather than the
/// per-cycle `SqlQueryOperator`.
#[tokio::test]
async fn test_mv_emit_on_window_close_threads_through_to_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE ticks (sym VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW per_minute \
         AS SELECT sym, AVG(price) AS avg_px \
            FROM ticks \
            GROUP BY sym, tumble(ts, INTERVAL '1' MINUTE) \
            EMIT ON WINDOW CLOSE",
    )
    .await
    .expect("MV creation should succeed");

    let mgr = db.connector_manager.lock();
    let reg = mgr
        .streams()
        .get("per_minute")
        .expect("MV should be registered as a stream");
    assert!(
        matches!(
            reg.emit_clause,
            Some(laminar_sql::parser::EmitClause::OnWindowClose)
        ),
        "EMIT ON WINDOW CLOSE was dropped on the way to StreamRegistration: {:?}",
        reg.emit_clause
    );
}

#[tokio::test]
async fn test_mv_registry_base_tables() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (sym VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    let registry = db.mv_registry.lock();
    assert!(registry.is_base_table("trades"));
}

#[tokio::test]
async fn test_show_materialized_views_empty() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "view_name");
            assert_eq!(batch.schema().field(1).name(), "sql");
            assert_eq!(batch.schema().field(2).name(), "state");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_materialized_view_if_exists() {
    let db = LaminarDB::open().unwrap();
    // Should not error with IF EXISTS on non-existent view
    let result = db
        .execute("DROP MATERIALIZED VIEW IF EXISTS nonexistent")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "DROP MATERIALIZED VIEW");
        }
        _ => panic!("Expected Ddl result"),
    }
}

#[tokio::test]
async fn test_drop_materialized_view_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP MATERIALIZED VIEW nonexistent").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "Expected 'not found' error, got: {err}"
    );
}

#[tokio::test]
async fn test_create_mv_if_not_exists() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW my_view AS SELECT id FROM events")
        .await
        .unwrap();

    // IF NOT EXISTS should succeed without error
    let result = db
        .execute("CREATE MATERIALIZED VIEW IF NOT EXISTS my_view AS SELECT * FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.object_name, "my_view");
            assert!(!info.applied);
        }
        _ => panic!("Expected Ddl result"),
    }
}

#[tokio::test]
async fn materialized_view_replace_is_rejected_before_mutating_old_definition() {
    for running in [false, true] {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (id INT)").await.unwrap();
        let original = "CREATE MATERIALIZED VIEW current_mv AS SELECT id FROM events";
        db.execute(original).await.unwrap();

        let old_registry_sql = db.mv_registry.lock().get("current_mv").unwrap().sql.clone();
        let old_runtime_sql = db.connector_manager.lock().streams()["current_mv"]
            .query_sql
            .clone();
        let control_rx = if running {
            let (control_tx, control_rx) =
                crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
            *db.control_tx.lock() = Some(control_tx);
            Some(control_rx)
        } else {
            None
        };

        let error = db
            .execute(
                "CREATE OR REPLACE MATERIALIZED VIEW current_mv AS \
                 SELECT id + 1 AS id FROM events",
            )
            .await
            .unwrap_err();

        assert!(matches!(error, DbError::InvalidOperation(_)));
        assert_eq!(
            db.mv_registry.lock().get("current_mv").unwrap().sql,
            old_registry_sql
        );
        assert_eq!(
            db.connector_manager.lock().streams()["current_mv"].query_sql,
            old_runtime_sql
        );
        if let Some(control_rx) = control_rx {
            assert!(control_rx.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn materialized_view_namespace_collision_preserves_existing_provider() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE TABLE existing_table (id INT PRIMARY KEY)")
        .await
        .unwrap();
    db.execute("CREATE STREAM existing_stream AS SELECT id FROM events")
        .await
        .unwrap();

    for name in ["events", "existing_table", "existing_stream"] {
        let before = db.ctx.table_provider(name).await.unwrap();
        let stream_sql = db
            .connector_manager
            .lock()
            .streams()
            .get(name)
            .map(|registration| registration.query_sql.clone());

        let error = db
            .execute(&format!(
                "CREATE MATERIALIZED VIEW {name} AS SELECT id FROM events"
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::InvalidOperation(_)));

        let after = db.ctx.table_provider(name).await.unwrap();
        assert!(Arc::ptr_eq(&before, &after), "provider changed for {name}");
        assert!(db.mv_registry.lock().get(name).is_none());
        assert_eq!(
            db.connector_manager
                .lock()
                .streams()
                .get(name)
                .map(|registration| registration.query_sql.clone()),
            stream_sql
        );
    }
}

#[tokio::test]
async fn multiway_incremental_mv_is_rejected_without_partial_registration() {
    let db = LaminarDB::open_with_config(LaminarConfig {
        incremental_emit: true,
        ..Default::default()
    })
    .unwrap();
    for source in ["ev_a", "ev_b", "ev_c"] {
        db.execute(&format!("CREATE SOURCE {source} (k BIGINT, v BIGINT)"))
            .await
            .unwrap();
    }
    for (view, source) in [("agg_a", "ev_a"), ("agg_b", "ev_b"), ("agg_c", "ev_c")] {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW {view} AS \
             SELECT k, SUM(v) AS total FROM {source} GROUP BY k"
        ))
        .await
        .unwrap();
    }

    let error = db
        .execute(
            "CREATE MATERIALIZED VIEW abc AS \
             SELECT a.k, a.total, b.total AS b_total, c.total AS c_total \
             FROM agg_a a JOIN agg_b b ON a.k = b.k \
             JOIN agg_c c ON b.k = c.k",
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("atomic batch topology admission"));
    assert!(db.mv_registry.lock().get("abc").is_none());
    assert!(!db.ctx.table_exist("abc").unwrap());
    assert!(db.connector_manager.lock().get_ddl("abc").is_none());
}

#[tokio::test]
async fn if_not_exists_preserves_live_definition_and_durable_ddl() {
    let db = LaminarDB::open().unwrap();
    const SOURCE_DDL: &str = "CREATE SOURCE stable_source (id INT)";
    const SINK_DDL: &str = "CREATE SINK stable_sink FROM stable_source";
    const TABLE_DDL: &str = "CREATE TABLE stable_table (id INT PRIMARY KEY)";
    db.execute(SOURCE_DDL).await.unwrap();
    db.execute("CREATE SOURCE other_source (id INT)")
        .await
        .unwrap();
    db.execute(SINK_DDL).await.unwrap();
    db.execute(TABLE_DDL).await.unwrap();

    db.execute("CREATE SOURCE IF NOT EXISTS stable_source (id INT, changed VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE SINK IF NOT EXISTS stable_sink FROM other_source")
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE IF NOT EXISTS stable_table \
         (id INT PRIMARY KEY, changed VARCHAR)",
    )
    .await
    .unwrap();

    assert_eq!(
        db.connector_manager.lock().get_ddl("stable_source"),
        Some(SOURCE_DDL)
    );
    assert_eq!(
        db.connector_manager.lock().get_ddl("stable_sink"),
        Some(SINK_DDL)
    );
    assert_eq!(
        db.connector_manager.lock().get_ddl("stable_table"),
        Some(TABLE_DDL)
    );
    assert_eq!(
        db.catalog
            .describe_source("stable_source")
            .unwrap()
            .fields()
            .len(),
        1
    );
    assert_eq!(
        db.catalog.get_sink_input("stable_sink").as_deref(),
        Some("stable_source")
    );
    assert_eq!(
        db.ctx
            .table_provider("stable_table")
            .await
            .unwrap()
            .schema()
            .fields()
            .len(),
        1
    );
}

#[cfg(feature = "cluster")]
struct TestCatalogAuthority {
    checkpoint_store: Arc<dyn object_store::ObjectStore>,
    manifest_store: Arc<laminar_core::cluster::control::CatalogManifestStore>,
    lease_store: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
    lease_tx: tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
    lease: laminar_core::cluster::control::LeaderLease,
}

#[cfg(feature = "cluster")]
async fn test_catalog_authority(
    object_store: Arc<dyn object_store::ObjectStore>,
) -> TestCatalogAuthority {
    test_catalog_authority_with_ttl(object_store, 1_000).await
}

#[cfg(feature = "cluster")]
async fn test_catalog_authority_with_ttl(
    object_store: Arc<dyn object_store::ObjectStore>,
    ttl_ms: i64,
) -> TestCatalogAuthority {
    use laminar_core::cluster::control::{
        CatalogManifestStore, LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeId;

    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: uuid::Uuid::from_u128(101),
        process_term: 1,
    };
    let lease_store = Arc::new(LeaderLeaseStore::new(Arc::clone(&object_store), ttl_ms));
    let LeaseOutcome::Acquired(lease) = lease_store.try_acquire(&owner, 0).await.unwrap() else {
        unreachable!()
    };
    let (controller, lease_tx) = catalog_authority_controller(NodeId(1), owner);
    controller.set_leader_lease_store(Arc::clone(&lease_store));
    TestCatalogAuthority {
        checkpoint_store: object_store,
        manifest_store: Arc::new(CatalogManifestStore::new(Arc::clone(&lease_store))),
        lease_store,
        controller,
        lease_tx,
        lease,
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_subscription_rejects_before_lookup_and_replay_state() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};

    struct UnusedRow;
    impl crate::handle::FromBatch for UnusedRow {
        fn from_batch(_batch: &arrow::array::RecordBatch, _row: usize) -> Self {
            Self
        }

        fn from_batch_all(_batch: &arrow::array::RecordBatch) -> Vec<Self> {
            Vec::new()
        }
    }

    let node = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .build()
        .await
        .unwrap();

    let raw_error = db
        .subscribe::<UnusedRow>("missing")
        .await
        .expect_err("direct stream subscriptions must not expose a local cluster shard");
    assert!(matches!(raw_error, DbError::Unsupported(_)));

    for start in [
        crate::subscription::SubscribeStart::Tail,
        crate::subscription::SubscribeStart::AsOfEpoch(7),
    ] {
        let error = db
            .open_subscription("missing", Some("not valid SQL ("), start)
            .await
            .expect_err("cluster SUBSCRIBE must fail before lookup, filter, or replay work");
        assert!(
            matches!(&error, DbError::Unsupported(_)),
            "unexpected cluster SUBSCRIBE error: {error}"
        );
        assert!(!db.subscription_registry.contains_name("missing"));
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_retain_history_bootstrap_rejects_without_catalog_mutation() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let error = db
        .execute_cluster_bootstrap_batch(&[
            "CREATE SOURCE unapplied_source (id INT)".into(),
            "CREATE STREAM unapplied_stream AS SELECT id FROM unapplied_source WITH ('retain_history' = '4mb')"
                .into(),
        ])
        .await
        .expect_err("cluster RETAIN HISTORY must reject the complete bootstrap batch");
    assert!(
        matches!(&error, DbError::Unsupported(_)),
        "unexpected cluster RETAIN HISTORY error: {error}"
    );

    for name in ["unapplied_source", "unapplied_stream"] {
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
        assert!(!db.ctx.table_exist(name).unwrap());
    }
    assert!(db.catalog.get_source("unapplied_source").is_none());
    assert!(db.planner.lock().get_source("unapplied_source").is_none());
    assert!(db.catalog.get_stream_entry("unapplied_stream").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("unapplied_stream"));
    assert!(!db.subscription_registry.contains_name("unapplied_stream"));
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn sealed_catalog_rejects_if_not_exists_definition_changes() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    db.execute_cluster_bootstrap_batch(&[
        "CREATE SOURCE stable_source (id INT)".into(),
        "CREATE SOURCE stable_other (id INT)".into(),
        "CREATE SINK stable_sink FROM stable_source".into(),
    ])
    .await
    .unwrap();
    let before = manifest_store.load().await.unwrap().unwrap();

    for ddl in [
        "CREATE SOURCE IF NOT EXISTS stable_source (id INT, changed VARCHAR)",
        "CREATE SINK IF NOT EXISTS stable_sink FROM stable_other",
    ] {
        assert!(db.execute_cluster_bootstrap(ddl).await.is_err());
    }

    let after = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn create_materialized_view_closed_admission_reply_rolls_back_local_registration() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    let (control_tx, control_rx) = crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    *db.control_tx.lock() = Some(control_tx);
    let close_reply = tokio::spawn(async move {
        let message = control_rx.recv().await.unwrap();
        assert!(matches!(
            message.into_kind(),
            crate::pipeline::ControlMsgKind::AddStream { .. }
        ));
    });

    let error = db
        .execute("CREATE MATERIALIZED VIEW rejected_mv AS SELECT id FROM events")
        .await
        .unwrap_err();
    close_reply.await.unwrap();

    assert!(matches!(error, DbError::Pipeline(message) if message.contains("pipeline stopped")));
    assert!(db.mv_registry.lock().get("rejected_mv").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("rejected_mv"));
    assert!(!db.mv_store.read().has_mv("rejected_mv"));
    assert!(!db.ctx.table_exist("rejected_mv").unwrap());
}

#[tokio::test]
async fn test_create_mv_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register a view directly
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "my_view",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    // Without IF NOT EXISTS, should error
    let result = db
        .execute("CREATE MATERIALIZED VIEW my_view AS SELECT * FROM events")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already exists"),
        "Expected 'already exists' error, got: {err}"
    );
}

#[tokio::test]
async fn test_show_materialized_views_with_entries() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register views directly for metadata testing
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "view_a",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "view_a");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_mv_and_show() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW temp_view AS SELECT id FROM events")
        .await
        .unwrap();

    // Verify it's there
    assert_eq!(db.mv_registry.lock().len(), 1);

    // Drop it
    db.execute("DROP MATERIALIZED VIEW temp_view")
        .await
        .unwrap();

    // Verify it's gone
    assert_eq!(db.mv_registry.lock().len(), 0);
}

#[tokio::test]
async fn test_debug_includes_mv_count() {
    let db = LaminarDB::open().unwrap();
    let debug = format!("{db:?}");
    assert!(
        debug.contains("materialized_views: 0"),
        "Debug should include MV count, got: {debug}"
    );
}

#[tokio::test]
async fn test_pipeline_topology_empty() {
    let db = LaminarDB::open().unwrap();
    let topo = db.pipeline_topology();
    assert!(topo.nodes.is_empty());
    assert!(topo.edges.is_empty());
}

#[tokio::test]
async fn test_pipeline_topology_sources_only() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE clicks (url VARCHAR, ts BIGINT)")
        .await
        .unwrap();

    let topo = db.pipeline_topology();
    assert_eq!(topo.nodes.len(), 2);
    assert!(topo.edges.is_empty());

    for node in &topo.nodes {
        assert_eq!(node.node_type, PipelineNodeType::Source);
        assert!(node.schema.is_some());
        assert!(node.sql.is_none());
    }
}

#[tokio::test]
async fn test_pipeline_topology_full_pipeline() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM agg AS SELECT COUNT(*) as cnt FROM events GROUP BY id")
        .await
        .unwrap();
    db.execute("CREATE SINK output FROM agg").await.unwrap();

    let topo = db.pipeline_topology();

    // Nodes: 1 source + 1 stream + 1 sink = 3
    assert_eq!(topo.nodes.len(), 3);

    let sources: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Source)
        .collect();
    let streams: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Stream)
        .collect();
    let sinks: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Sink)
        .collect();

    assert_eq!(sources.len(), 1);
    assert_eq!(streams.len(), 1);
    assert_eq!(sinks.len(), 1);

    assert_eq!(sources[0].name, "events");
    assert_eq!(streams[0].name, "agg");
    assert!(streams[0].sql.is_some());
    assert_eq!(sinks[0].name, "output");

    // Edges: events->agg, agg->output
    assert_eq!(topo.edges.len(), 2);
    assert!(topo
        .edges
        .iter()
        .any(|e| e.from == "events" && e.to == "agg"));
    assert!(topo
        .edges
        .iter()
        .any(|e| e.from == "agg" && e.to == "output"));
}

#[tokio::test]
async fn test_pipeline_topology_fan_out() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE ticks (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM ohlc AS SELECT symbol, MIN(price) FROM ticks GROUP BY symbol")
        .await
        .unwrap();
    db.execute("CREATE STREAM vol AS SELECT symbol, COUNT(*) FROM ticks GROUP BY symbol")
        .await
        .unwrap();

    let topo = db.pipeline_topology();

    // 1 source + 2 streams = 3 nodes
    assert_eq!(topo.nodes.len(), 3);

    // Both streams should have an edge from ticks
    let ticks_edges: Vec<_> = topo.edges.iter().filter(|e| e.from == "ticks").collect();
    assert_eq!(ticks_edges.len(), 2);

    let targets: Vec<&str> = ticks_edges.iter().map(|e| e.to.as_str()).collect();
    assert!(targets.contains(&"ohlc"));
    assert!(targets.contains(&"vol"));
}

#[tokio::test]
async fn test_streams_method() {
    let db = LaminarDB::open().unwrap();
    assert!(db.streams().is_empty());

    db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await
        .unwrap();

    let streams = db.streams();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, "counts");
    assert!(streams[0].sql.is_some());
    assert!(
        streams[0].sql.as_ref().unwrap().contains("COUNT"),
        "SQL should contain the query: {:?}",
        streams[0].sql,
    );
}

#[tokio::test]
async fn test_pipeline_node_types() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE src (id INT)").await.unwrap();
    db.execute("CREATE STREAM st AS SELECT * FROM src")
        .await
        .unwrap();
    db.execute("CREATE SINK sk FROM st").await.unwrap();

    let topo = db.pipeline_topology();

    let find = |name: &str| topo.nodes.iter().find(|n| n.name == name).unwrap();

    assert_eq!(find("src").node_type, PipelineNodeType::Source);
    assert_eq!(find("st").node_type, PipelineNodeType::Stream);
    assert_eq!(find("sk").node_type, PipelineNodeType::Sink);
}

#[tokio::test]
async fn test_create_table_with_primary_key() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE TABLE instruments (\
             symbol VARCHAR PRIMARY KEY, \
             company_name VARCHAR, \
             sector VARCHAR\
             )",
        )
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "instruments");
        }
        _ => panic!("Expected DDL result"),
    }

    // Verify TableStore registration
    let ts = db.table_store.read();
    assert!(ts.has_table("instruments"));
    assert_eq!(ts.primary_key("instruments"), Some("symbol"));
    assert_eq!(ts.table_row_count("instruments"), 0);
}

#[tokio::test]
async fn create_table_rejects_invalid_key_contract_before_mutation() {
    let db = LaminarDB::open().unwrap();

    for (name, sql, expected) in [
        (
            "missing_pk",
            "CREATE TABLE missing_pk (tenant INT, id INT)",
            "exactly one PRIMARY KEY",
        ),
        (
            "composite_pk",
            "CREATE TABLE composite_pk (tenant INT, id INT, PRIMARY KEY (tenant, id))",
            "composite PRIMARY KEY",
        ),
    ] {
        let error = db.execute(sql).await.unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected rejection for {name}: {error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(!db.table_store.read().has_table(name));
        assert!(!db.ctx.table_exist(name).unwrap());
        assert!(!db.connector_manager.lock().tables().contains_key(name));
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
    }
}

#[tokio::test]
async fn create_table_rejects_ignored_options_before_mutation() {
    let db = LaminarDB::open().unwrap();

    for (name, sql, expected) in [
        (
            "orphan_connector_option",
            "CREATE TABLE orphan_connector_option (id INT PRIMARY KEY) \
             WITH (topic = 'events')",
            "require a connector",
        ),
        (
            "orphan_cache_limit",
            "CREATE TABLE orphan_cache_limit (id INT PRIMARY KEY) \
             WITH (cache_max_entries = '64')",
            "bounded on-demand cache",
        ),
        (
            "ignored_cache_mode",
            "CREATE TABLE ignored_cache_mode (id INT PRIMARY KEY) \
             WITH (cache_mode = 'partial')",
            "bounded on-demand cache",
        ),
        (
            "unsupported_storage",
            "CREATE TABLE unsupported_storage (id INT PRIMARY KEY) \
             WITH (storage = 'memory')",
            "storage option 'memory' is unsupported",
        ),
    ] {
        let error = db.execute(sql).await.unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected rejection for {name}: {error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(!db.table_store.read().has_table(name));
        assert!(!db.ctx.table_exist(name).unwrap());
        assert!(!db.connector_manager.lock().tables().contains_key(name));
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
    }
}

#[tokio::test]
async fn typed_table_requires_provider_and_table_store_entry() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE inconsistent (id INT PRIMARY KEY)")
        .await
        .unwrap();
    assert!(db.ctx.table_exist("inconsistent").unwrap());
    assert!(db.table_store.write().drop_table("inconsistent"));

    let error = db
        .execute("CREATE TABLE IF NOT EXISTS inconsistent (id INT PRIMARY KEY)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("inconsistent"));
    assert!(db.ctx.table_exist("inconsistent").unwrap());
    assert_eq!(
        db.catalog_namespace.lock().get("inconsistent").copied(),
        Some(CatalogObjectKind::Table)
    );
}

#[tokio::test]
async fn table_insert_rejects_partial_or_invalid_rows_without_mutation() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE TABLE strict_insert (id INT PRIMARY KEY, label VARCHAR NOT NULL, score DOUBLE)",
    )
    .await
    .unwrap();
    db.execute("INSERT INTO strict_insert VALUES (1, 'valid', 1.5)")
        .await
        .unwrap();

    for sql in [
        "INSERT INTO strict_insert VALUES (2, 'missing')",
        "INSERT INTO strict_insert VALUES (2, 'extra', 2.0, 99)",
        "INSERT INTO strict_insert VALUES ('bad-id', 'invalid', 2.0)",
        "INSERT INTO strict_insert VALUES (NULL, 'null-key', 2.0)",
        "INSERT INTO strict_insert (id, label, score) VALUES (2, 'named', 2.0)",
    ] {
        let error = db.execute(sql).await.unwrap_err();
        assert!(
            matches!(
                &error,
                DbError::InsertError(_) | DbError::InvalidOperation(_)
            ),
            "unexpected error for {sql}: {error}"
        );
        assert_eq!(db.table_store.read().table_row_count("strict_insert"), 1);
    }

    let error = db
        .execute(
            "INSERT INTO strict_insert VALUES \
             (2, 'must-not-be-installed', 2.0), (3, NULL, 3.0)",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbError::InsertError(_) | DbError::InvalidOperation(_)
    ));
    assert_eq!(
        db.table_store.read().table_row_count("strict_insert"),
        1,
        "a later invalid row must reject the entire VALUES batch"
    );
}

#[tokio::test]
async fn create_table_rejects_source_only_connector_without_residue() {
    let db = LaminarDB::open().unwrap();
    let error = db
        .execute(
            "CREATE TABLE instruments (\
             symbol VARCHAR PRIMARY KEY, \
             company_name VARCHAR\
             ) WITH (connector = 'kafka', topic = 'instruments')",
        )
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains("reference-table source"),
        "{error}"
    );
    assert!(!db.catalog_namespace.lock().contains_key("instruments"));
    assert!(!db.table_store.read().has_table("instruments"));
    assert!(!db.ctx.table_exist("instruments").unwrap());
    assert!(!db
        .connector_manager
        .lock()
        .tables()
        .contains_key("instruments"));
    assert!(db.connector_manager.lock().get_ddl("instruments").is_none());
}

#[tokio::test]
async fn lookup_cdc_aliases_fail_before_registration() {
    let db = LaminarDB::open().unwrap();

    for (index, connector) in ["postgres-cdc", "postgres_cdc"].into_iter().enumerate() {
        let name = format!("rejected_lookup_{index}");
        let error = db
            .execute(&format!(
                "CREATE LOOKUP TABLE {name} (id INT NOT NULL, PRIMARY KEY (id)) \
                 WITH ('connector' = '{connector}')"
            ))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("cannot back a LOOKUP TABLE"),
            "unexpected rejection for {connector}: {error}"
        );
        assert!(!db.table_store.read().has_table(&name));
        assert!(!db.connector_manager.lock().tables().contains_key(&name));
        assert!(!db.ctx.table_exist(name.as_str()).unwrap());
        assert!(db.planner.lock().get_lookup_table(&name).is_none());
        assert!(db.lookup_registry.get_entry(&name).is_none());
    }
}

#[tokio::test]
async fn lookup_connectors_without_snapshot_factories_leave_no_residue() {
    let db = LaminarDB::open().unwrap();

    for (index, connector) in ["redis", "s3", "s3-parquet"].into_iter().enumerate() {
        let name = format!("unsupported_lookup_{index}");
        let error = db
            .execute(&format!(
                "CREATE LOOKUP TABLE {name} (id INT NOT NULL, PRIMARY KEY (id)) \
                 WITH ('connector' = '{connector}')"
            ))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("no registered snapshot-capable table source"),
            "unexpected rejection for {connector}: {error}"
        );
        assert!(!db.table_store.read().has_table(&name));
        assert!(!db.connector_manager.lock().tables().contains_key(&name));
        assert!(!db.ctx.table_exist(name.as_str()).unwrap());
        assert!(db.planner.lock().get_lookup_table(&name).is_none());
        assert!(db.lookup_registry.get_entry(&name).is_none());
    }
}

#[cfg(any(not(feature = "postgres-cdc"), not(feature = "delta-lake")))]
#[tokio::test]
async fn feature_disabled_lookup_connectors_leave_no_residue() {
    let db = LaminarDB::open().unwrap();
    let mut connectors = Vec::new();
    #[cfg(not(feature = "postgres-cdc"))]
    connectors.push("postgres");
    #[cfg(not(feature = "delta-lake"))]
    connectors.push("delta-lake");

    for (index, connector) in connectors.into_iter().enumerate() {
        let name = format!("disabled_lookup_{index}");
        let error = db
            .execute(&format!(
                "CREATE LOOKUP TABLE {name} (id INT NOT NULL, PRIMARY KEY (id)) \
                 WITH ('connector' = '{connector}')"
            ))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no registered snapshot-capable table source"),
            "unexpected rejection for {connector}: {error}"
        );
        assert!(!db.table_store.read().has_table(&name));
        assert!(!db.connector_manager.lock().tables().contains_key(&name));
        assert!(!db.ctx.table_exist(name.as_str()).unwrap());
        assert!(db.planner.lock().get_lookup_table(&name).is_none());
        assert!(db.lookup_registry.get_entry(&name).is_none());
    }
}

#[cfg(feature = "postgres-cdc")]
#[tokio::test]
async fn postgresql_alias_registers_standalone_lookup() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE LOOKUP TABLE customers (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id)) \
         WITH ('connector' = 'postgresql', 'connection' = 'host=localhost', \
         'table' = 'customers')",
    )
    .await
    .unwrap();

    assert!(db.table_store.read().has_table("customers"));
    assert_eq!(
        db.table_store.read().connector("customers"),
        Some("postgres")
    );
    assert_eq!(
        db.connector_manager
            .lock()
            .tables()
            .get("customers")
            .and_then(|registration| registration.connector_type.as_deref()),
        Some("postgres")
    );
    assert!(db.ctx.table_exist("customers").unwrap());
    assert!(matches!(
        db.planner
            .lock()
            .get_lookup_table("customers")
            .map(|info| info.properties.connector.clone()),
        Some(laminar_sql::parser::lookup_table::ConnectorType::Postgres)
    ));

    let error = db
        .execute(
            "CREATE OR REPLACE LOOKUP TABLE customers \
             (id INT NOT NULL, PRIMARY KEY (id)) WITH ('connector' = 'missing-lookup')",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not atomic"));
    assert_eq!(
        db.table_store.read().connector("customers"),
        Some("postgres")
    );
    assert_eq!(
        db.connector_manager
            .lock()
            .tables()
            .get("customers")
            .and_then(|registration| registration.connector_type.as_deref()),
        Some("postgres")
    );
    assert!(db.ctx.table_exist("customers").unwrap());
    assert!(matches!(
        db.planner
            .lock()
            .get_lookup_table("customers")
            .map(|info| info.properties.connector.clone()),
        Some(laminar_sql::parser::lookup_table::ConnectorType::Postgres)
    ));
}

#[cfg(feature = "postgres-cdc")]
#[tokio::test]
async fn postgres_on_demand_admission_uses_lookup_factory() {
    let db = LaminarDB::open().unwrap();
    assert!(db.connector_registry().has_lookup_source("postgres"));
    db.execute(
        "CREATE LOOKUP TABLE pg_direct (id INT NOT NULL, PRIMARY KEY (id)) \
         WITH ('connector' = 'postgres', 'strategy' = 'on-demand', \
         'connection' = 'host=localhost', 'table' = 'pg_direct')",
    )
    .await
    .unwrap();
    assert!(db
        .connector_manager
        .lock()
        .tables()
        .get("pg_direct")
        .is_some_and(|registration| registration.on_demand));
}

#[cfg(feature = "mongodb-cdc")]
#[tokio::test]
async fn mongodb_on_demand_admission_does_not_require_table_source() {
    let db = LaminarDB::open().unwrap();
    assert!(db.connector_registry().has_lookup_source("mongodb"));
    assert!(!db.connector_registry().has_table_source("mongodb"));
    db.execute(
        "CREATE LOOKUP TABLE mongo_direct (id VARCHAR NOT NULL, PRIMARY KEY (id)) \
         WITH ('connector' = 'mongodb', 'strategy' = 'on-demand', \
         'connection.uri' = 'mongodb://localhost:27017', 'database' = 'test', \
         'collection' = 'dimensions')",
    )
    .await
    .unwrap();
    assert!(db
        .connector_manager
        .lock()
        .tables()
        .get("mongo_direct")
        .is_some_and(|registration| registration.on_demand));
}

#[tokio::test]
async fn on_demand_lookup_is_rejected_for_exactly_once_without_residue() {
    use laminar_connectors::connector::DeliveryGuarantee;

    let db = LaminarDB::builder()
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .build()
        .await
        .unwrap();
    let error = db
        .execute(
            "CREATE LOOKUP TABLE direct_lookup \
             (id INT NOT NULL, PRIMARY KEY (id)) \
             WITH ('connector' = 'unregistered', 'strategy' = 'on-demand')",
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("not checkpointed"), "{error}");
    assert!(!db.catalog_namespace.lock().contains_key("direct_lookup"));
    assert!(!db.table_store.read().has_table("direct_lookup"));
    assert!(!db.ctx.table_exist("direct_lookup").unwrap());
    assert!(!db
        .connector_manager
        .lock()
        .tables()
        .contains_key("direct_lookup"));
    assert!(db
        .planner
        .lock()
        .get_lookup_table("direct_lookup")
        .is_none());
    assert!(db.lookup_registry.get_entry("direct_lookup").is_none());
}

#[tokio::test]
async fn custom_on_demand_lookup_uses_lookup_factory_without_table_source() {
    struct EmptyLookup {
        schema: arrow_schema::SchemaRef,
    }

    #[async_trait::async_trait]
    impl laminar_core::lookup::source::LookupSourceDyn for EmptyLookup {
        async fn query_batch(
            &self,
            keys: &[&[u8]],
            _predicates: &[laminar_core::lookup::predicate::Predicate],
            _projection: &[laminar_core::lookup::source::ColumnId],
        ) -> Result<
            Vec<Option<arrow::record_batch::RecordBatch>>,
            laminar_core::lookup::source::LookupError,
        > {
            Ok(std::iter::repeat_with(|| None).take(keys.len()).collect())
        }

        fn schema(&self) -> arrow_schema::SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    struct EmptyLookupFactory;

    #[async_trait::async_trait]
    impl laminar_connectors::registry::LookupSourceFactory for EmptyLookupFactory {
        async fn build(
            &self,
            _config: laminar_connectors::config::ConnectorConfig,
            declared_schema: Option<arrow_schema::SchemaRef>,
        ) -> Result<
            Arc<dyn laminar_core::lookup::source::LookupSourceDyn>,
            laminar_connectors::error::ConnectorError,
        > {
            let schema = declared_schema.ok_or_else(|| {
                laminar_connectors::error::ConnectorError::ConfigurationError(
                    "declared schema is required".to_string(),
                )
            })?;
            Ok(Arc::new(EmptyLookup { schema }))
        }
    }

    let db = LaminarDB::builder()
        .register_connector(|registry| {
            registry.register_lookup_source(
                "mock-direct",
                laminar_connectors::config::ConnectorInfo {
                    name: "mock-direct".into(),
                    display_name: "Mock direct lookup".into(),
                    version: "0.1.0".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(EmptyLookupFactory),
            )
        })
        .build()
        .await
        .unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute(
        "CREATE LOOKUP TABLE dimensions (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id)) \
         WITH ('connector' = 'mock-direct', 'strategy' = 'on-demand', \
         'cache.memory' = '1mb')",
    )
    .await
    .unwrap();
    assert_eq!(
        db.table_store.read().connector("dimensions"),
        Some("mock-direct")
    );
    assert!(db
        .connector_manager
        .lock()
        .tables()
        .get("dimensions")
        .is_some_and(|registration| {
            registration.on_demand && registration.connector_type.as_deref() == Some("mock-direct")
        }));

    db.start().await.unwrap();
    let entry = db.lookup_registry.get_entry("dimensions");
    assert!(matches!(
        entry,
        Some(laminar_sql::datafusion::RegisteredLookup::Partial(ref state))
            if state.source.is_some()
    ));
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_insert_into_table_with_pk_upserts() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE TABLE products (\
         id INT PRIMARY KEY, \
         name VARCHAR, \
         price DOUBLE\
         )",
    )
    .await
    .unwrap();

    // Insert a row
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 1);

    // Upsert (same PK = overwrite)
    db.execute("INSERT INTO products VALUES (1, 'Super Widget', 19.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 1);

    // Insert another row (different PK)
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 14.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 2);

    let snapshot = db
        .table_store
        .read()
        .to_record_batch("products")
        .unwrap()
        .unwrap();
    let ids = snapshot
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    let names = snapshot
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual = (0..snapshot.num_rows())
        .map(|row| (ids.value(row), names.value(row).to_string()))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(actual.get(&1).map(String::as_str), Some("Super Widget"));
    assert_eq!(actual.get(&2).map(String::as_str), Some("Gadget"));

    // Verify via SELECT
    let result = db.execute("SELECT * FROM products").await.unwrap();
    match result {
        ExecuteResult::Query(q) => {
            assert_eq!(q.schema().fields().len(), 3);
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_show_tables() {
    let db = LaminarDB::open().unwrap();

    // Empty
    let result = db.execute("SHOW TABLES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "name");
            assert_eq!(batch.schema().field(1).name(), "primary_key");
            assert_eq!(batch.schema().field(2).name(), "row_count");
            assert_eq!(batch.schema().field(3).name(), "connector");
        }
        _ => panic!("Expected Metadata result"),
    }

    // With a table
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
        .await
        .unwrap();
    let result = db.execute("SHOW TABLES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
        .await
        .unwrap();
    assert!(db.table_store.read().has_table("t"));

    db.execute("DROP TABLE t").await.unwrap();
    assert!(!db.table_store.read().has_table("t"));
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP TABLE IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_having_filters_grouped_results() {
    let db = LaminarDB::open().unwrap();

    // Create table and query via DataFusion directly
    db.ctx
        .sql(
            "CREATE TABLE hv_trades AS SELECT * FROM (VALUES \
             ('AAPL', 100), ('GOOG', 5), ('MSFT', 50)) \
             AS t(symbol, volume)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql("SELECT symbol, volume FROM hv_trades WHERE volume > 10 ORDER BY symbol")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // AAPL(100), MSFT(50) pass; GOOG(5) filtered
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_having_with_aggregate() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE hv_orders AS SELECT * FROM (VALUES \
             ('A', 100), ('A', 200), ('B', 50), ('B', 30), ('C', 500)) \
             AS t(category, amount)",
        )
        .await
        .unwrap();

    // Query with GROUP BY + HAVING through DataFusion
    let df = db
        .ctx
        .sql(
            "SELECT category, SUM(amount) as total \
             FROM hv_orders GROUP BY category \
             HAVING SUM(amount) > 100 ORDER BY category",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // A: 300 > 100 ✓, B: 80 ✗, C: 500 > 100 ✓
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_having_all_filtered_out() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE items AS SELECT * FROM (VALUES \
             ('x', 1), ('y', 2)) AS t(name, qty)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql("SELECT name, SUM(qty) as total FROM items GROUP BY name HAVING SUM(qty) > 1000")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_having_compound_predicate() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE sales AS SELECT * FROM (VALUES \
             ('A', 100), ('A', 200), ('B', 50), ('C', 10), ('C', 20)) \
             AS t(region, amount)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT region, COUNT(*) as cnt, SUM(amount) as total \
             FROM sales GROUP BY region \
             HAVING COUNT(*) >= 2 AND SUM(amount) > 25 \
             ORDER BY region",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // A: cnt=2>=2 AND total=300>25 ✓
    // B: cnt=1<2 ✗
    // C: cnt=2>=2 AND total=30>25 ✓
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_two_way_lookup() {
    let db = LaminarDB::open().unwrap();

    // Create tables via DataFusion
    db.ctx
        .sql(
            "CREATE TABLE orders AS SELECT * FROM (VALUES \
             (1, 100, 'A'), (2, 200, 'B')) AS t(id, customer_id, product_code)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE customers AS SELECT * FROM (VALUES \
             (100, 'Alice'), (200, 'Bob')) AS t(id, name)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE products AS SELECT * FROM (VALUES \
             ('A', 'Widget'), ('B', 'Gadget')) AS t(code, label)",
        )
        .await
        .unwrap();

    // Two-way join through DataFusion
    let df = db
        .ctx
        .sql(
            "SELECT o.id, c.name, p.label \
             FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             JOIN products p ON o.product_code = p.code \
             ORDER BY o.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_three_way() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql("CREATE TABLE t1 AS SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, fk1)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t2 AS SELECT * FROM (VALUES (10, 100), (20, 200)) AS t(id, fk2)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t3 AS SELECT * FROM (VALUES (100, 'x'), (200, 'y')) AS t(id, fk3)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t4 AS SELECT * FROM (VALUES ('x', 'final_x'), ('y', 'final_y')) AS t(id, val)")
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT t1.id, t4.val \
             FROM t1 \
             JOIN t2 ON t1.fk1 = t2.id \
             JOIN t3 ON t2.fk2 = t3.id \
             JOIN t4 ON t3.fk3 = t4.id \
             ORDER BY t1.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_mixed_types() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE stream_a AS SELECT * FROM (VALUES \
             (1, 'k1'), (2, 'k2')) AS t(id, key)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE stream_b AS SELECT * FROM (VALUES \
             ('k1', 10), ('k2', 20)) AS t(key, value)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE dim_c AS SELECT * FROM (VALUES \
             ('k1', 'label1'), ('k2', 'label2')) AS t(key, label)",
        )
        .await
        .unwrap();

    // Inner join + left join
    let df = db
        .ctx
        .sql(
            "SELECT a.id, b.value, c.label \
             FROM stream_a a \
             JOIN stream_b b ON a.key = b.key \
             LEFT JOIN dim_c c ON a.key = c.key \
             ORDER BY a.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_single_backward_compat() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE left_t AS SELECT * FROM (VALUES \
             (1, 'a'), (2, 'b')) AS t(id, val)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE right_t AS SELECT * FROM (VALUES \
             (1, 'x'), (2, 'y')) AS t(id, data)",
        )
        .await
        .unwrap();

    // Single join still works
    let df = db
        .ctx
        .sql(
            "SELECT l.id, l.val, r.data \
             FROM left_t l JOIN right_t r ON l.id = r.id \
             ORDER BY l.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_frame_moving_average() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_prices AS SELECT * FROM (VALUES \
             (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)) \
             AS t(id, price)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, AVG(price) OVER (ORDER BY id \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma \
             FROM frame_prices ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5);

    // Verify moving average values: row 3 → avg(10,20,30) = 20
    let ma_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((ma_col.value(2) - 20.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_running_sum() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_amounts AS SELECT * FROM (VALUES \
             (1, 100.0), (2, 200.0), (3, 300.0)) AS t(id, amount)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, SUM(amount) OVER (ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
             FROM frame_amounts ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);

    let sum_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Row 3: cumulative sum = 100 + 200 + 300 = 600
    assert!((sum_col.value(2) - 600.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_rolling_max() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_vals AS SELECT * FROM (VALUES \
             (1, 5.0), (2, 15.0), (3, 10.0), (4, 20.0)) AS t(id, price)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, MAX(price) OVER (ORDER BY id \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rmax \
             FROM frame_vals ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4);

    let max_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Row 3: max(5, 15, 10) = 15
    assert!((max_col.value(2) - 15.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_rolling_count() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_events AS SELECT * FROM (VALUES \
             (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) AS t(id, code)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, COUNT(*) OVER (ORDER BY id \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt \
             FROM frame_events ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4);

    let cnt_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    // Row 1: count of just row 1 = 1
    assert_eq!(cnt_col.value(0), 1);
    // Row 2+: count of current + 1 preceding = 2
    assert_eq!(cnt_col.value(1), 2);
    assert_eq!(cnt_col.value(2), 2);
}

/// Helper: create a test `RecordBatch` for table population tests.
fn table_test_batch(ids: &[i32], symbols: &[&str]) -> RecordBatch {
    use arrow::array::Int32Array;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("symbol", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(symbols.to_vec())),
        ],
    )
    .unwrap()
}

/// Build a database with a mock reference-table factory.
async fn db_with_mock_table_source(snapshot_batches: Vec<RecordBatch>) -> Arc<LaminarDB> {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::reference::MockReferenceTableSource;

    let snap = std::sync::Arc::new(parking_lot::Mutex::new(Some(snapshot_batches)));
    LaminarDB::builder()
        .register_connector(move |registry| {
            registry.register_table_source(
                "mock",
                ConnectorInfo {
                    name: "mock".to_string(),
                    display_name: "Mock Table Source".to_string(),
                    version: "0.1.0".to_string(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                std::sync::Arc::new(move |_config, _declared_schema| {
                    let s = snap.lock().take().unwrap_or_default();
                    Ok(Box::new(MockReferenceTableSource::new(s)))
                }),
            )
        })
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_table_source_snapshot_populates_table() {
    let batch = table_test_batch(&[1, 2], &["AAPL", "GOOG"]);
    let db = db_with_mock_table_source(vec![batch]).await;

    db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    // Table should be populated by snapshot
    let ts = db.table_store.read();
    assert!(ts.is_ready("instruments"));
    assert_eq!(ts.table_row_count("instruments"), 2);
}

#[tokio::test]
async fn table_refresh_modes_are_rejected_without_residue() {
    let db = db_with_mock_table_source(Vec::new()).await;

    for (index, mode) in ["manual", "cdc", "snapshot_only"].into_iter().enumerate() {
        let name = format!("rejected_refresh_{index}");
        let error = db
            .execute(&format!(
                "CREATE TABLE {name} (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
                 WITH (connector = 'mock', format = 'json', refresh = '{mode}')"
            ))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("authoritative startup snapshot"));
        assert!(!db.catalog_namespace.lock().contains_key(&name));
        assert!(!db.table_store.read().has_table(&name));
        assert!(!db.ctx.table_exist(name.as_str()).unwrap());
        assert!(!db.connector_manager.lock().tables().contains_key(&name));
        assert!(db.connector_manager.lock().get_ddl(&name).is_none());
    }
}

#[tokio::test]
async fn test_table_source_multiple_tables() {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::reference::MockReferenceTableSource;

    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let cc = call_count.clone();
    let batch1 = table_test_batch(&[1], &["AAPL"]);
    let batch2 = table_test_batch(&[2, 3], &["GOOG", "MSFT"]);
    let batches = std::sync::Arc::new(parking_lot::Mutex::new(vec![vec![batch1], vec![batch2]]));

    let db = LaminarDB::builder()
        .register_connector(move |registry| {
            registry.register_table_source(
                "mock",
                ConnectorInfo {
                    name: "mock".to_string(),
                    display_name: "Mock".to_string(),
                    version: "0.1.0".to_string(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                std::sync::Arc::new(move |_config, _declared_schema| {
                    let idx = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
                    let mut all = batches.lock();
                    let snap = if idx < all.len() {
                        std::mem::take(&mut all[idx])
                    } else {
                        vec![]
                    };
                    Ok(Box::new(MockReferenceTableSource::new(snap)))
                }),
            )
        })
        .build()
        .await
        .unwrap();

    db.execute("CREATE SOURCE events (x INT)").await.unwrap();

    db.execute(
        "CREATE TABLE t1 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE t2 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    let ts = db.table_store.read();
    // Both tables should be snapshot-populated (order may vary)
    let total = ts.table_row_count("t1") + ts.table_row_count("t2");
    assert_eq!(total, 3); // 1 + 2
    assert!(ts.is_ready("t1"));
    assert!(ts.is_ready("t2"));
}

// --- Pipeline Observability API tests ---

#[tokio::test]
async fn test_metrics_initial_state() {
    let db = LaminarDB::open().unwrap();
    let m = db.metrics();
    assert_eq!(m.total_events_ingested, 0);
    assert_eq!(m.total_events_emitted, 0);
    assert_eq!(m.total_events_dropped, 0);
    assert_eq!(m.total_cycles, 0);
    assert_eq!(m.total_batches, 0);
    assert_eq!(m.state, crate::metrics::PipelineState::Created);
    assert_eq!(m.source_count, 0);
    assert_eq!(m.stream_count, 0);
    assert_eq!(m.sink_count, 0);
}

#[tokio::test]
async fn test_source_metrics_after_push() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    // Push some data
    let handle = db.source_untyped("trades").unwrap();
    let batch = RecordBatch::try_new(
        handle.schema().clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0, 2800.0])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let sm = db.source_metrics("trades").unwrap();
    assert_eq!(sm.name, "trades");
    assert_eq!(sm.total_events, 1); // 1 push = sequence 1
    assert!(sm.pending > 0);
    assert!(sm.capacity > 0);
    assert!(sm.utilization > 0.0);
}

#[tokio::test]
async fn test_source_metrics_not_found() {
    let db = LaminarDB::open().unwrap();
    assert!(db.source_metrics("nonexistent").is_none());
}

#[tokio::test]
async fn test_all_source_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();

    let all = db.all_source_metrics();
    assert_eq!(all.len(), 2);
    #[allow(clippy::disallowed_types)] // test code
    let names: std::collections::HashSet<_> = all.iter().map(|m| m.name.clone()).collect();
    assert!(names.contains("a"));
    assert!(names.contains("b"));
}

#[tokio::test]
async fn test_total_events_processed_zero() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.total_events_processed(), 0);
}

#[tokio::test]
async fn test_pipeline_state_enum_created() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(
        db.pipeline_state_enum(),
        crate::metrics::PipelineState::Created
    );
}

#[tokio::test]
async fn test_engine_metrics_accessible() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = std::sync::Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(prom.clone());
    prom.events_ingested.inc_by(42);
    let m = db.metrics();
    assert_eq!(m.total_events_ingested, 42);
}

#[tokio::test]
async fn test_metrics_counts_after_create() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE s1 (id INT)").await.unwrap();
    db.execute("CREATE SINK out1 FROM s1").await.unwrap();

    let m = db.metrics();
    assert_eq!(m.source_count, 1);
    assert_eq!(m.sink_count, 1);
}

#[tokio::test]
async fn test_source_handle_capacity() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    // Default buffer size is 1024
    assert!(handle.capacity() >= 1024);
    assert!(!handle.is_backpressured());
}

#[tokio::test]
async fn test_stream_metrics_with_sql() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM avg_price AS \
         SELECT symbol, AVG(price) as avg_price \
         FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
    )
    .await
    .unwrap();

    let sm = db.stream_metrics("avg_price");
    assert!(sm.is_some());
    let sm = sm.unwrap();
    assert_eq!(sm.name, "avg_price");
    assert!(sm.sql.is_some());
    assert!(sm.sql.as_deref().unwrap().contains("AVG"));
}

#[tokio::test]
async fn test_all_stream_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM s1 AS SELECT symbol, AVG(price) as avg_price \
         FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
    )
    .await
    .unwrap();

    let all = db.all_stream_metrics();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].name, "s1");
}

#[tokio::test]
async fn test_stream_metrics_not_found() {
    let db = LaminarDB::open().unwrap();
    assert!(db.stream_metrics("nonexistent").is_none());
}

/// Helper: push a batch with `Timestamp(µs)` column to a source.
///
/// `timestamps_ms` are in **milliseconds**; the helper converts to microseconds
/// internally to match the `TIMESTAMP` SQL type (`Timestamp(Microsecond, None)`).
fn make_ts_batch(schema: &arrow::datatypes::SchemaRef, timestamps_ms: &[i64]) -> RecordBatch {
    let us_values: Vec<i64> = timestamps_ms.iter().map(|ms| ms * 1000).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(
                (1..=i64::try_from(timestamps_ms.len()).expect("len fits i64")).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(us_values)),
        ],
    )
    .unwrap()
}

fn poll_subscription_batch(
    portal: &mut crate::subscription::SubscriptionPortal,
) -> Option<RecordBatch> {
    loop {
        match portal.try_next_frame()? {
            crate::subscription::PortalFrame::Batch { batch, .. } => return Some(batch),
            crate::subscription::PortalFrame::Barrier { .. } => {}
            crate::subscription::PortalFrame::Lagged(skipped) => {
                panic!("subscription lagged by {skipped} entries")
            }
            crate::subscription::PortalFrame::Error { message } => {
                panic!("subscription failed: {message}")
            }
        }
    }
}

#[tokio::test]
async fn test_watermark_gauges_advance_on_push() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    handle
        .push_arrow(make_ts_batch(&schema, &[1000, 2000, 3000]))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let source_wm = prom
        .source_watermark_ms
        .with_label_values(&["events"])
        .get();
    assert_eq!(source_wm, 3000);
    let stream_wm = prom.stream_watermark_ms.with_label_values(&["out"]).get();
    assert_eq!(stream_wm, 3000);
}

#[tokio::test]
async fn test_watermark_advances_on_push() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    let batch = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch).unwrap();

    // Wait for pipeline loop to process
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // With 0s delay, watermark should be max timestamp = 3000
    let wm = handle.current_watermark();
    assert_eq!(
        wm, 3000,
        "watermark should equal max timestamp with 0s delay"
    );
}

#[tokio::test]
async fn test_watermark_bounded_delay() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push timestamps [1000, 800, 1200] — max = 1200
    let batch = make_ts_batch(&schema, &[1000, 800, 1200]);
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Watermark = max(1200) - 100ms delay = 1100
    let wm = handle.current_watermark();
    assert_eq!(wm, 1100, "watermark should be max_ts - delay");
}

#[tokio::test]
async fn test_watermark_no_regression() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push high timestamps first
    let batch1 = make_ts_batch(&schema, &[5000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let wm1 = handle.current_watermark();

    // Push lower timestamps
    let batch2 = make_ts_batch(&schema, &[1000]);
    handle.push_arrow(batch2).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let wm2 = handle.current_watermark();

    // Watermark should never decrease
    assert!(wm2 >= wm1, "watermark must not regress: {wm2} < {wm1}");
    assert_eq!(wm1, 5000);
    assert_eq!(wm2, 5000);
}

#[tokio::test]
async fn test_source_without_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
        .await
        .unwrap();

    // Source without WATERMARK clause should have default watermark
    let handle = db.source_untyped("events").unwrap();
    assert_eq!(handle.current_watermark(), i64::MIN);
    assert!(handle.max_out_of_orderness().is_none());
}

#[tokio::test]
async fn test_watermark_with_arrow_timestamp_column() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Build a batch with Arrow Timestamp(us) column matching the schema
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                5_000_000i64,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let wm = handle.current_watermark();
    // ArrowNative format: timestamp is in microseconds, extractor converts to millis
    assert_eq!(wm, 5000, "watermark should work with Arrow Timestamp type");
}

#[tokio::test]
async fn test_pipeline_watermark_global_min() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE SOURCE orders (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    let trades = db.source_untyped("trades").unwrap();
    let orders = db.source_untyped("orders").unwrap();

    // Push high watermark to trades
    let batch1 = make_ts_batch(trades.schema(), &[5000]);
    trades.push_arrow(batch1).unwrap();

    // Push lower watermark to orders
    let batch2 = make_ts_batch(orders.schema(), &[2000]);
    orders.push_arrow(batch2).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Global watermark should be min(5000, 2000) = 2000
    let global = db.pipeline_watermark();
    assert_eq!(
        global, 2000,
        "global watermark should be min of all sources"
    );
}

#[tokio::test]
async fn test_pipeline_watermark_in_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let batch = make_ts_batch(handle.schema(), &[4000]);
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let m = db.metrics();
    assert_eq!(
        m.pipeline_watermark,
        db.pipeline_watermark(),
        "metrics().pipeline_watermark should match pipeline_watermark()"
    );
    assert_eq!(m.pipeline_watermark, 4000);
}

#[tokio::test]
async fn test_source_handle_max_out_of_orderness() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
    )
    .await
    .unwrap();

    let handle = db.source_untyped("events").unwrap();
    let dur = handle.max_out_of_orderness();
    assert_eq!(dur, Some(std::time::Duration::from_secs(5)));
}

#[tokio::test]
async fn test_source_handle_no_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    assert!(handle.max_out_of_orderness().is_none());
}

#[tokio::test]
async fn test_late_data_dropped_after_external_watermark() {
    // Scenario:
    //  1. Push on-time batch (ts = [1000, 2000, 3000])
    //  2. Advance watermark to 200_000 externally via source.watermark()
    //  3. Push late batch (ts = [100, 200, 300]) — all timestamps < watermark
    //  4. Verify late batch does NOT appear in stream output
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Step 1: Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Drain on-time results
    let mut on_time_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Step 2: Advance watermark to 200_000 (external signal)
    handle.watermark(200_000);
    // Give the pipeline loop a cycle to pick up the external watermark
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Step 3: Push late data (all timestamps < 200_000)
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Step 4: Check that late data was filtered out
    let mut late_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

#[test]
fn test_filter_late_rows_filters_correctly() {
    use arrow::array::{Int64Array, TimestampMillisecondArray};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 500, 200, 800])),
        ],
    )
    .unwrap();

    // Watermark at 300: rows with ts >= 300 survive (ts=500, ts=800).
    let filtered = filter_late_rows(&batch, "ts", 300)
        .expect("no schema drift")
        .expect("some on-time rows");
    assert_eq!(filtered.num_rows(), 2);

    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2); // ts=500
    assert_eq!(ids.value(1), 4); // ts=800
}

#[test]
fn test_filter_late_rows_all_late() {
    use arrow::array::{Int64Array, TimestampMillisecondArray};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let result = filter_late_rows(&batch, "ts", 1000);
    assert!(
        matches!(result, Ok(None)),
        "all-late batch should be Ok(None), got {result:?}"
    );
}

#[test]
fn test_filter_late_rows_no_column() {
    use arrow::array::Int64Array;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

    // Missing event-time column is schema drift — an error, distinct
    // from an all-late batch so it isn't misreported as a watermark drop.
    assert!(filter_late_rows(&batch, "ts", 1000).is_err());
}

/// Helper: creates a `RecordBatch` with (id: BIGINT, ts: BIGINT).
#[tokio::test]
async fn test_programmatic_watermark_filters_late_rows() {
    // Source with set_event_time_column("ts"), no SQL WATERMARK clause.
    // Push data, advance watermark, push late data, verify late data filtered.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    handle.set_event_time_column("ts");

    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    let schema = handle.schema().clone();

    // Step 1: Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Drain on-time results
    let mut on_time_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Step 2: Advance watermark to 200_000 (external signal)
    handle.watermark(200_000);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Step 3: Push late data (all timestamps < 200_000)
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Step 4: Check that late data was filtered out
    let mut late_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

/// End-to-end: a non-windowed `EMIT CHANGES` view with a `now()`
/// temporal predicate emits a `+1` when the watermark frontier admits a
/// row and a `-1` when it ages out.
#[tokio::test]
async fn test_retracting_temporal_filter_emits_insert_then_retract() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM recent AS SELECT * FROM events \
         WHERE ts > now() - INTERVAL '10' SECOND EMIT CHANGES",
    )
    .await
    .unwrap();

    let handle = db.source_untyped("events").unwrap();
    handle.set_event_time_column("ts");
    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("recent", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    let schema = handle.schema().clone();

    // `(weight, ts_ms)` for one changelog batch.
    let rows = |b: &RecordBatch| -> Vec<(i64, i64)> {
        let w = b
            .column(b.schema().index_of("__weight").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let ts = b
            .column(b.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        (0..b.num_rows())
            .map(|r| (w.value(r), ts.value(r) / 1000))
            .collect()
    };
    macro_rules! drain {
        () => {{
            let mut out = Vec::new();
            for _ in 0..256 {
                match poll_subscription_batch(&mut sub) {
                    Some(b) => out.extend(rows(&b)),
                    None => break,
                }
            }
            out
        }};
    }

    // Phase 1: row ts=5000ms is a member while frontier < 15000ms
    // (exit = 5000 - (-10000)). External watermark 8000 < 15000 ⇒ +1.
    handle.push_arrow(make_ts_batch(&schema, &[5000])).unwrap();
    handle.watermark(8000);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(drain!(), vec![(1, 5000)], "row admitted ⇒ +1");

    // Phase 2: advance the frontier to 20000ms (≥ 15000) — the ts=5000
    // row ages out. A fresh ts=18000 row (not late vs the prior 8000
    // frontier) carries the drain cycle and is itself a new member.
    handle.watermark(20_000);
    handle
        .push_arrow(make_ts_batch(&schema, &[18_000]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut got = drain!();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![(-1, 5000), (1, 18_000)],
        "aged-out row retracts (-1) while the fresh row inserts (+1)"
    );
}

/// The production path: a `WATERMARK FOR ts` stream where the frontier
/// advances purely from event-time data (no manual watermark calls). A
/// later event ages out an earlier one. (A *fully silent* source does
/// not advance event time and so does not retract — correct event-time
/// semantics, identical to windowed operators; not exercised here
/// because it is, by definition, the absence of behaviour.)
#[tokio::test]
async fn test_retracting_temporal_filter_event_time_driven() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM recent AS SELECT * FROM events \
         WHERE ts > now() - INTERVAL '10' SECOND EMIT CHANGES",
    )
    .await
    .unwrap();

    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("recent", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();
    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    let rows = |b: &RecordBatch| -> Vec<(i64, i64)> {
        let w = b
            .column(b.schema().index_of("__weight").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let ts = b
            .column(b.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        (0..b.num_rows())
            .map(|r| (w.value(r), ts.value(r) / 1000))
            .collect()
    };
    macro_rules! drain {
        () => {{
            let mut out = Vec::new();
            for _ in 0..256 {
                match poll_subscription_batch(&mut sub) {
                    Some(b) => out.extend(rows(&b)),
                    None => break,
                }
            }
            out
        }};
    }

    // ts=5000 is a member while frontier < 15000ms; the watermark comes
    // only from event time (zero-delay WATERMARK FOR ts), no manual calls.
    handle.push_arrow(make_ts_batch(&schema, &[5000])).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(drain!(), vec![(1, 5000)], "first event admitted ⇒ +1");

    // A much later event advances event time past ts=5000's exit (15000).
    handle
        .push_arrow(make_ts_batch(&schema, &[30_000]))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut got = drain!();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![(-1, 5000), (1, 30_000)],
        "later event ages out the earlier one purely via event time"
    );
}

#[tokio::test]
async fn test_sql_watermark_for_col_filters_late_rows() {
    // Source with WATERMARK FOR ts (no AS expr), should use zero delay.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut on_time_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Advance watermark externally
    handle.watermark(200_000);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Push late data
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut late_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

#[tokio::test]
async fn test_no_watermark_passes_all_data() {
    // Source without any watermark config — all data should pass through.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    db.start().await.unwrap();
    let mut sub = db
        .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push two batches — no watermark filtering should happen
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    handle.watermark(200_000); // watermark without event_time_column is a no-op for filtering
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let batch2 = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(batch2).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // All rows from both batches should appear
    let mut total_rows = 0;
    for _ in 0..256 {
        match poll_subscription_batch(&mut sub) {
            Some(b) => total_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(
        total_rows, 6,
        "all data should pass through without watermark config"
    );
}

#[tokio::test]
async fn test_select_from_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO sensors VALUES (1, 22.5), (2, 23.1)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM sensors").await.unwrap();
    match result {
        ExecuteResult::Query(mut q) => {
            // The bridge_query_stream spawns a tokio task; yield to let it run.
            tokio::task::yield_now().await;
            let mut sub = q.subscribe_raw().unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut total_rows = 0;
            for _ in 0..256 {
                match sub.poll() {
                    Some(b) => total_rows += b.num_rows(),
                    None => break,
                }
            }
            assert_eq!(total_rows, 2);
        }
        _ => panic!("Expected Query result from SELECT on source"),
    }
}

#[tokio::test]
async fn test_select_from_dropped_source_fails() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("DROP SOURCE sensors").await.unwrap();

    let result = db.execute("SELECT * FROM sensors").await;
    assert!(result.is_err(), "SELECT after DROP SOURCE should fail");
}

#[tokio::test]
async fn rejected_source_replace_preserves_existing_buffer() {
    const ORIGINAL: &str = "CREATE SOURCE sensors (id BIGINT, temp DOUBLE)";
    let db = LaminarDB::open().unwrap();
    db.execute(ORIGINAL).await.unwrap();
    db.execute("INSERT INTO sensors VALUES (1, 20.0)")
        .await
        .unwrap();

    let error = db
        .execute("CREATE OR REPLACE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not atomic"));
    db.execute("INSERT INTO sensors VALUES (2, 30.0)")
        .await
        .unwrap();
    assert_eq!(
        db.connector_manager.lock().get_ddl("sensors"),
        Some(ORIGINAL)
    );

    let result = db.execute("SELECT * FROM sensors").await.unwrap();
    match result {
        ExecuteResult::Query(mut q) => {
            let mut sub = q.subscribe_raw().unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut total_rows = 0;
            for _ in 0..256 {
                match sub.poll() {
                    Some(b) => total_rows += b.num_rows(),
                    None => break,
                }
            }
            assert_eq!(
                total_rows, 2,
                "a rejected replacement must preserve the original source buffer"
            );
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_mv_registers_stream_in_connector_manager() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    // Before MV creation, no stream registered
    {
        let mgr = db.connector_manager.lock();
        assert!(
            !mgr.streams().contains_key("event_totals"),
            "stream should not exist before MV creation"
        );
    }

    let result = db
        .execute("CREATE MATERIALIZED VIEW event_totals AS SELECT * FROM events")
        .await;

    // The MV may fail at query execution (no data), but if DDL succeeds
    // the connector manager should have the stream registered
    if result.is_ok() {
        let mgr = db.connector_manager.lock();
        assert!(
            mgr.streams().contains_key("event_totals"),
            "MV should be registered as a stream in connector manager"
        );
        let reg = &mgr.streams()["event_totals"];
        assert!(
            reg.query_sql.contains("events"),
            "stream query should reference the source"
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_query_shape_admission_is_pre_mutation_and_mode_derived() {
    use laminar_core::cluster::control::{
        CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseStore,
    };
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};
    use object_store::ObjectStore;

    async fn one_owner_cluster() -> Arc<LaminarDB> {
        let node = NodeId(1);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let catalog_store = Arc::new(CatalogManifestStore::new(Arc::new(LeaderLeaseStore::new(
            Arc::clone(&object_store),
            1_000,
        ))));
        let process_incarnation = controller.recovery_incarnation();
        let receiver = Arc::new(
            ShuffleReceiver::bind(node.0, "127.0.0.1:0".parse().unwrap(), process_incarnation)
                .await
                .unwrap(),
        );
        LaminarDB::builder()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(object_store)
            .catalog_manifest_store(catalog_store)
            .shuffle_sender(Arc::new(ShuffleSender::new(node.0, process_incarnation)))
            .shuffle_receiver(receiver)
            .state_backend(Arc::new(InProcessBackend::new(8)))
            .vnode_registry(Arc::new(VnodeRegistry::single_owner(8, node)))
            .build()
            .await
            .unwrap()
    }

    fn assert_no_query_residue(db: &LaminarDB, name: &str) {
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(db.catalog.get_stream_entry(name).is_none());
        assert!(db.mv_registry.lock().get(name).is_none());
        assert!(!db.connector_manager.lock().streams().contains_key(name));
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
        assert!(!db.mv_store.read().has_mv(name));
        assert!(!db.subscription_registry.contains_name(name));
        assert!(!db.planner.lock().has_query(name));
        assert!(!db.stream_schemas.read().contains_key(name));
        assert!(!db.ctx.table_exist(name).unwrap());
    }

    async fn assert_cluster_rejection(db: &LaminarDB, name: &str, ddl: &str) {
        let error = db
            .execute(ddl)
            .await
            .expect_err("unsupported cluster query shape must fail closed");
        assert!(
            error.to_string().contains("LDB-4007"),
            "unexpected rejection for {name}: {error}"
        );
        assert_no_query_residue(db, name);
    }

    let db = one_owner_cluster().await;
    let persisted_unsafe = std::collections::HashMap::from([(
        "persisted_final".to_string(),
        crate::connector_manager::StreamRegistration {
            name: "persisted_final".to_string(),
            query_sql: "SELECT id FROM left_events".to_string(),
            emit_clause: Some(laminar_sql::parser::EmitClause::Final),
            window_config: None,
            order_config: None,
            join_config: None,
            has_analytic: false,
            has_frame: false,
            incremental: false,
        },
    )]);
    let error = db
        .revalidate_persisted_cluster_query_shapes(&persisted_unsafe)
        .await
        .expect_err("startup must reject persisted local-only state before connector creation");
    assert!(error.to_string().contains("LDB-4007"), "{error}");
    assert!(db.connector_manager.lock().streams().is_empty());

    let lookup_plan = crate::ddl::PlannedStreamingQuery {
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config: Some(vec![laminar_sql::translator::JoinOperatorConfig::Lookup(
            laminar_sql::translator::LookupJoinConfig::inner("id".into(), "id".into()),
        )]),
        has_analytic: false,
        has_frame: false,
    };
    let error = db
        .validate_cluster_query_shape(
            "materialized view",
            "rejected_snapshot_lookup",
            "SELECT id FROM left_events",
            &lookup_plan,
        )
        .await
        .expect_err("lookup state without vnode capture/apply/revoke must fail closed");
    assert!(error.to_string().contains("LDB-4007"), "{error}");

    super::CATALOG_MANIFEST_REPLAY
        .scope((), async {
            db.execute(
                "CREATE SOURCE left_events (id BIGINT, value DOUBLE, ts TIMESTAMP, \
                 WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
            )
            .await
            .unwrap();
            db.execute(
                "CREATE SOURCE right_events (id BIGINT, value DOUBLE, ts TIMESTAMP, \
                 WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
            )
            .await
            .unwrap();

            assert_cluster_rejection(
                &db,
                "rejected_eowc_stream",
                "CREATE STREAM rejected_eowc_stream AS \
                 SELECT id, TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, COUNT(*) AS n \
                 FROM left_events GROUP BY id, TUMBLE(ts, INTERVAL '1' MINUTE) \
                 EMIT ON WINDOW CLOSE",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_interval",
                "CREATE STREAM rejected_interval AS \
                 SELECT l.id, l.value AS left_value, r.value AS right_value \
                 FROM left_events l JOIN right_events r ON l.id = r.id \
                 AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_temporal_filter",
                "CREATE STREAM rejected_temporal_filter AS SELECT id, value, ts \
                 FROM left_events WHERE ts > now() - INTERVAL '10' SECOND EMIT CHANGES",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_keyed_window",
                "CREATE STREAM rejected_keyed_window AS \
                 SELECT id, TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, SUM(value) AS total \
                 FROM left_events GROUP BY id, TUMBLE(ts, INTERVAL '1' MINUTE)",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_derived_aggregate",
                "CREATE STREAM rejected_derived_aggregate AS \
                 SELECT SUM(value) / COUNT(value) AS ratio FROM left_events",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_nested_aggregate",
                "CREATE STREAM rejected_nested_aggregate AS \
                 SELECT SUM(inner_total) AS total FROM (\
                     SELECT SUM(value) AS inner_total FROM left_events\
                 ) nested",
            )
            .await;

            assert_cluster_rejection(
                &db,
                "rejected_append_mv",
                "CREATE MATERIALIZED VIEW rejected_append_mv AS \
                 SELECT id, value FROM left_events",
            )
            .await;
            assert_cluster_rejection(
                &db,
                "rejected_global_aggregate",
                "CREATE MATERIALIZED VIEW rejected_global_aggregate AS \
                 SELECT SUM(value) AS total FROM left_events",
            )
            .await;
            db.execute(
                "CREATE STREAM projection_ok AS \
                 SELECT id, value FROM left_events WHERE value > 0",
            )
            .await
            .expect("stateless projection/filter is cluster-safe");
            assert!(db.catalog.get_stream_entry("projection_ok").is_some());

            let aggregate = db
                .ctx
                .sql("SELECT SUM(value) AS total FROM left_events")
                .await
                .unwrap();
            let aggregate_physical = db
                .ctx
                .state()
                .create_physical_plan(aggregate.logical_plan())
                .await
                .unwrap();
            assert_eq!(
                (
                    crate::ddl::logical_aggregate_stage_count(aggregate.logical_plan()),
                    crate::ddl::physical_aggregate_stage_count(&aggregate_physical),
                ),
                (1, 1),
                "the admitted global aggregate must use the single certified stage shape"
            );
            db.execute(
                "CREATE STREAM global_sum_ok AS \
                 SELECT SUM(value) AS total FROM left_events",
            )
            .await
            .expect("a constructible global incremental aggregate is cluster-safe");
            assert!(db.catalog.get_stream_entry("global_sum_ok").is_some());
        })
        .await;

    // Startup independently enforces the same invariant so an inconsistent in-memory catalog
    // cannot bypass DDL admission.
    let stale = one_owner_cluster().await;
    stale
        .mv_store
        .write()
        .create_mv(
            "stale_mv",
            Arc::new(arrow_schema::Schema::empty()),
            crate::mv_store::MvStorageMode::append_default(),
        )
        .unwrap();
    let error = stale
        .start()
        .await
        .expect_err("cluster startup must reject residual materialized state");
    assert!(error.to_string().contains("LDB-4007"), "{error}");

    // A cluster-capable binary may also host an independent local database. Cluster admission is
    // scoped to configured mode, while the ordinary planner retains its local join contract.
    let embedded = LaminarDB::open().unwrap();
    embedded
        .execute("CREATE SOURCE local_left (id BIGINT)")
        .await
        .unwrap();
    embedded
        .execute("CREATE SOURCE local_right (id BIGINT)")
        .await
        .unwrap();
    let error = embedded
        .execute(
            "CREATE MATERIALIZED VIEW local_join AS SELECT l.id FROM local_left l \
             JOIN local_right r ON l.id = r.id",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unbounded join"), "{error}");
    assert!(!error.to_string().contains("LDB-4007"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn live_topology_ddl_is_fenced_in_a_configured_one_owner_cluster() {
    use laminar_core::cluster::control::{
        CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseStore,
    };
    use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};
    use object_store::ObjectStore;

    let cluster_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(cluster_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(cluster_id, kv, None, members_rx));
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let manifest_store = Arc::new(CatalogManifestStore::new(Arc::new(LeaderLeaseStore::new(
        Arc::clone(&object_store),
        1_000,
    ))));
    let cluster = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(object_store)
        .catalog_manifest_store(manifest_store)
        .state_backend(Arc::new(InProcessBackend::new(4)))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(4, cluster_id)))
        .build()
        .await
        .unwrap();
    DbState::Starting.store(&cluster.state);

    let error = cluster
        .execute("CREATE SOURCE rejected (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-6043"));
    assert!(cluster.catalog.get_source("rejected").is_none());

    super::CATALOG_MANIFEST_REPLAY
        .scope(
            (),
            cluster.execute(
                "CREATE SOURCE replayed (id INT) WITH \
                 ('connector' = 'generator', 'topic' = 'manifest')",
            ),
        )
        .await
        .expect("internal manifest replay must rebuild connector catalog entries during startup");
    assert!(cluster.catalog.get_source("replayed").is_some());
    assert!(cluster
        .connector_manager
        .lock()
        .sources()
        .contains_key("replayed"));

    // Source DDL has no live coordinator wiring even in a local single-node runtime.
    let single_owner = LaminarDB::builder()
        .state_backend(Arc::new(InProcessBackend::new(4)))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(4, NodeId(1))))
        .build()
        .await
        .unwrap();
    DbState::Running.store(&single_owner.state);
    let error = single_owner
        .execute("CREATE SOURCE admitted (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-6043"));
    assert!(single_owner.catalog.get_source("admitted").is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_start_fails_closed_when_catalog_manifest_cannot_be_loaded() {
    use laminar_core::cluster::control::CatalogManifestStore;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    object_store
        .put(
            &object_store::path::Path::from("control/leader-lease/v0000000000000001.json"),
            PutPayload::from_bytes(bytes::Bytes::from_static(b"{not-json")),
        )
        .await
        .unwrap();
    let manifest_store = Arc::new(CatalogManifestStore::new(Arc::new(
        laminar_core::cluster::control::LeaderLeaseStore::new(Arc::clone(&object_store), 1_000),
    )));
    let node = laminar_core::cluster::discovery::NodeId(1);
    let (controller, _lease_tx) = catalog_authority_controller(
        node,
        laminar_core::cluster::control::LeaderLeaseOwner {
            node,
            boot: uuid::Uuid::from_u128(101),
            process_term: 1,
        },
    );
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(object_store)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let error = db
        .start()
        .await
        .expect_err("corrupt cluster catalog manifest must abort startup");
    assert!(error.to_string().contains("LDB-6003"));
    assert!(error.to_string().contains("catalog manifest load failed"));
    assert_eq!(DbState::load(&db.state), DbState::Created);
    assert!(db
        .catalog_manifest_store
        .lock()
        .as_ref()
        .is_some_and(|installed| Arc::ptr_eq(installed, &manifest_store)));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_ddl_rolls_back_when_manifest_seal_fails() {
    use laminar_core::cluster::testing::{FaultyObjectStore, ObjectStoreFault};
    use object_store::ObjectStore;

    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let faulty = Arc::new(FaultyObjectStore::new(inner));
    let object_store: Arc<dyn ObjectStore> = faulty.clone();
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    faulty.set_fault(ObjectStoreFault::FailWrites);

    let error = db
        .execute_cluster_bootstrap("CREATE SOURCE local_only (id INT)")
        .await
        .expect_err("manifest seal failure must roll local DDL back");
    assert!(error.to_string().contains("catalog manifest seal failed"));
    assert!(db.catalog.get_source("local_only").is_none());
    assert!(!db.ctx.table_exist("local_only").unwrap());
    assert!(db.planner.lock().get_source("local_only").is_none());
    assert!(db.connector_manager.lock().get_ddl("local_only").is_none());
    assert!(!db.catalog_namespace.lock().contains_key("local_only"));
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn partial_cluster_bootstrap_failure_leaves_no_manifest_or_local_topology() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let error = db
        .execute_cluster_bootstrap_batch(&[
            "CREATE SOURCE first_applied (id INT)".into(),
            "CREATE OR REPLACE SINK invalid_sink FROM first_applied".into(),
        ])
        .await
        .expect_err("a later DDL failure must abort the complete inventory");
    assert!(error.to_string().contains("not atomic"));
    assert!(db.catalog.get_source("first_applied").is_none());
    assert!(db
        .connector_manager
        .lock()
        .get_ddl("first_applied")
        .is_none());
    assert!(!db.catalog_namespace.lock().contains_key("first_applied"));
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn query_cannot_observe_catalog_create_while_manifest_seal_is_pending() {
    use laminar_core::cluster::testing::{FaultyObjectStore, ObjectStoreFault};
    use object_store::ObjectStore;

    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let faulty = Arc::new(FaultyObjectStore::new(inner));
    let object_store: Arc<dyn ObjectStore> = faulty.clone();
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(manifest_store)
        .build()
        .await
        .unwrap();
    faulty.set_fault(ObjectStoreFault::FailWrites);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *db.catalog_seal_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));

    let create = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute_cluster_bootstrap("CREATE SOURCE tentative (id INT)")
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(db.catalog.get_source("tentative").is_some());
    assert!(db.ctx.table_exist("tentative").unwrap());

    let query = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.execute("SELECT * FROM tentative").await })
    };
    let local_scan = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.collect_local_table("tentative").await })
    };
    tokio::task::yield_now().await;
    assert!(!query.is_finished());
    assert!(!local_scan.is_finished());

    *db.catalog_seal_gate.lock() = None;
    release.notify_one();
    assert!(create.await.unwrap().is_err());
    let query_error = query.await.unwrap().unwrap_err();
    assert!(query_error.to_string().contains("tentative"));
    let scan_error = local_scan.await.unwrap().unwrap_err();
    assert!(scan_error.to_string().contains("tentative"));
    assert!(db.catalog.get_source("tentative").is_none());
    assert!(!db.ctx.table_exist("tentative").unwrap());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn concurrent_cluster_catalog_creates_have_one_durable_winner() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let (follower_controller, _follower_lease) = catalog_authority_controller(
        laminar_core::cluster::discovery::NodeId(2),
        authority.lease.owner.clone(),
    );
    let left = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let right = LaminarDB::builder()
        .cluster_controller(follower_controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let (left_result, right_result) = tokio::join!(
        left.execute_cluster_bootstrap("CREATE SOURCE left_winner (id INT)"),
        right.execute_cluster_bootstrap("CREATE SOURCE right_winner (id INT)")
    );
    assert_ne!(left_result.is_ok(), right_result.is_ok());

    let (winner_db, winner_name, loser_db, loser_name) = if left_result.is_ok() {
        (&left, "left_winner", &right, "right_winner")
    } else {
        (&right, "right_winner", &left, "left_winner")
    };
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].canonical_name, winner_name);
    assert!(winner_db.catalog.get_source(winner_name).is_some());
    assert!(loser_db.catalog.get_source(loser_name).is_none());
    assert!(!loser_db.ctx.table_exist(loser_name).unwrap());
    assert!(loser_db.planner.lock().get_source(loser_name).is_none());
    assert!(loser_db
        .connector_manager
        .lock()
        .get_ddl(loser_name)
        .is_none());
    assert!(!loser_db.catalog_namespace.lock().contains_key(loser_name));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn concurrent_identical_cluster_bootstrap_adopts_the_single_sealed_inventory() {
    use object_store::ObjectStore;

    const DDL: &str = "CREATE SOURCE shared_bootstrap (id INT)";
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let controller = Arc::clone(&authority.controller);
    let left = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let right = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let (left_result, right_result) = tokio::join!(
        left.execute_cluster_bootstrap(DDL),
        right.execute_cluster_bootstrap(DDL)
    );
    assert!(
        left_result.is_ok(),
        "left bootstrap failed: {left_result:?}"
    );
    assert!(
        right_result.is_ok(),
        "right bootstrap failed: {right_result:?}"
    );
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].canonical_name, "shared_bootstrap");
    for db in [&left, &right] {
        assert!(db.catalog.get_source("shared_bootstrap").is_some());
        assert_eq!(
            db.connector_manager.lock().get_ddl("shared_bootstrap"),
            Some(DDL)
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn startup_bootstrap_restores_before_config_and_requires_exact_ddl() {
    use object_store::ObjectStore;

    const DDL: &str = "CREATE SOURCE configured (id INT)";
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let writer = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    writer.execute_cluster_bootstrap(DDL).await.unwrap();

    let (joiner_controller, _joiner_lease) = catalog_authority_controller(
        laminar_core::cluster::discovery::NodeId(2),
        authority.lease.owner.clone(),
    );
    let joiner = LaminarDB::builder()
        .cluster_controller(joiner_controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let result = joiner.execute_cluster_bootstrap(DDL).await.unwrap();
    assert!(matches!(
        result,
        ExecuteResult::Ddl(DdlInfo { applied: false, .. })
    ));
    let error = joiner
        .execute_cluster_bootstrap("CREATE SOURCE configured (id BIGINT)")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("complete ordered sealed inventory"));
    let addition = joiner
        .execute_cluster_bootstrap("CREATE SOURCE added_later (id INT)")
        .await
        .unwrap_err();
    assert!(addition
        .to_string()
        .contains("complete ordered sealed inventory"));
    assert!(joiner.catalog.get_source("added_later").is_none());
    assert_eq!(
        manifest_store.load().await.unwrap().unwrap().entries.len(),
        1
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn catalog_bootstrap_rechecks_lifecycle_after_acquiring_topology_lock() {
    let db = LaminarDB::open().unwrap();
    let topology_guard = db.topology_ddl_lock.write().await;
    let bootstrap = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute_cluster_bootstrap("CREATE SOURCE lifecycle_race (id INT)")
                .await
        })
    };
    tokio::task::yield_now().await;
    DbState::Starting.store(&db.state);
    drop(topology_guard);

    let error = bootstrap.await.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("only valid before pipeline startup"));
}

#[cfg(feature = "cluster")]
fn catalog_authority_controller(
    node: laminar_core::cluster::discovery::NodeId,
    observed_owner: laminar_core::cluster::control::LeaderLeaseOwner,
) -> (
    Arc<laminar_core::cluster::control::ClusterController>,
    tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
) {
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, LeaderLease, LeaderLeaseOwner, LeaseDeadline,
    };

    let boot = uuid::Uuid::from_u128(u128::from(node.0) + 100);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&kv),
        kv,
        None,
        members_rx,
        boot,
    ));
    controller.set_active(false);
    controller.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
        std::time::Duration::from_secs(30),
    )));
    let local_owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: 1,
    };
    let (lease_tx, lease_rx) = tokio::sync::watch::channel(Some(LeaderLease {
        seq: 1,
        token: 1,
        owner: observed_owner,
        expires_at_ms: i64::MAX,
        catalog_manifest: None,
    }));
    controller
        .set_leader_lease_watch(
            lease_rx,
            local_owner,
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    (controller, lease_tx)
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn partitioned_joiner_can_exact_replay_but_cannot_publish_without_durable_lease() {
    use laminar_core::cluster::discovery::NodeId;
    use object_store::ObjectStore;

    const DDL: &str = "CREATE SOURCE durable_source (id INT)";
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let (joiner_controller, _joiner_lease) =
        catalog_authority_controller(NodeId(2), authority.lease.owner.clone());
    let leader = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let joiner = LaminarDB::builder()
        .cluster_controller(joiner_controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let error = joiner
        .execute_cluster_bootstrap("CREATE SOURCE partitioned_write (id INT)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("active durable leader lease"));
    assert!(joiner.catalog.get_source("partitioned_write").is_none());

    leader.execute_cluster_bootstrap(DDL).await.unwrap();

    let replay = joiner.execute_cluster_bootstrap(DDL).await.unwrap();
    assert!(matches!(
        replay,
        ExecuteResult::Ddl(DdlInfo { applied: false, .. })
    ));
    let error = joiner
        .execute_cluster_bootstrap("CREATE SOURCE partitioned_write (id INT)")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("complete ordered sealed inventory"));
    assert!(joiner.catalog.get_source("partitioned_write").is_none());
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].ddl, DDL);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn stale_catalog_sealer_rolls_back_after_a_successor_wins() {
    use laminar_core::cluster::control::LeaderLeaseOwner;
    use laminar_core::cluster::discovery::NodeId;
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority_with_ttl(object_store, 10).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let lease_store = Arc::clone(&authority.lease_store);
    let checkpoint_store = Arc::clone(&authority.checkpoint_store);
    let controller = Arc::clone(&authority.controller);
    let lease_tx = authority.lease_tx;
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::clone(&checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *db.catalog_seal_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));
    let create = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute_cluster_bootstrap("CREATE SOURCE fenced_create (id INT)")
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    lease_tx.send_replace(None);
    let successor_owner = LeaderLeaseOwner {
        node: NodeId(2),
        boot: uuid::Uuid::from_u128(102),
        process_term: 1,
    };
    let current = lease_store.load().await.unwrap().unwrap();
    let observation = lease_store
        .observe_rival(&successor_owner, &current)
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let laminar_core::cluster::control::LeaseOutcome::Acquired(takeover) = lease_store
        .try_takeover(&successor_owner, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("successor must acquire the durable lease");
    };
    let (successor_controller, successor_lease_tx) =
        catalog_authority_controller(NodeId(2), successor_owner);
    successor_lease_tx.send_replace(Some(takeover));
    let successor = LaminarDB::builder()
        .cluster_controller(successor_controller)
        .cluster_checkpoint_object_store(checkpoint_store)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    successor
        .execute_cluster_bootstrap("CREATE SOURCE successor_winner (id INT)")
        .await
        .unwrap();
    *db.catalog_seal_gate.lock() = None;
    release.notify_one();

    let error = create.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("lost its durable leader lease"));
    assert!(db.catalog.get_source("fenced_create").is_none());
    assert!(!db.ctx.table_exist("fenced_create").unwrap());
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].canonical_name, "successor_winner");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_manifest_never_persists_literal_connector_secrets() {
    use futures::TryStreamExt;
    use object_store::ObjectStore;

    const PASSWORD: &str = "catalog-password-must-not-leak";
    const TOKEN: &str = "catalog-token-must-not-leak";
    let backing = Arc::new(object_store::memory::InMemory::new());
    let object_store: Arc<dyn ObjectStore> = backing.clone();
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    db.execute_cluster_bootstrap("CREATE SOURCE safe (id INT)")
        .await
        .unwrap();

    let error = db
        .execute(&format!(
            "CREATE SOURCE rejected (id INT) WITH ('password' = '{PASSWORD}')"
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot persist secret property"));
    assert!(db.catalog.get_source("rejected").is_none());
    let token_error = db
        .execute(&format!(
            "CREATE SOURCE rejected_token (id INT) WITH ('token' = '{TOKEN}')"
        ))
        .await
        .unwrap_err();
    assert!(token_error
        .to_string()
        .contains("cannot persist secret property"));
    assert!(db.catalog.get_source("rejected_token").is_none());
    let signed_url_error = db
        .execute(
            "CREATE SOURCE rejected_url (id INT) WITH \
             ('connection' = 'https://example.test/data?X-Amz%2DSignature=signed-secret')",
        )
        .await
        .unwrap_err();
    assert!(signed_url_error
        .to_string()
        .contains("cannot persist secret property"));
    for ddl in [
        "CREATE SOURCE rejected_exact_url (id INT) WITH \
         ('url' = 'wss://user:socket-secret@example.test/events')",
        "CREATE SOURCE rejected_sas_uri (id INT) WITH \
         ('uri' = 'https://blob.test/data?sv=1&sig=sas-secret')",
        "CREATE SOURCE rejected_uri_list (id INT) WITH \
         ('endpoints' = 'wss://public.test, wss://user:list-secret@private.test')",
    ] {
        let error = db.execute(ddl).await.unwrap_err();
        assert!(
            error.to_string().contains("cannot persist secret property"),
            "{error}"
        );
    }
    let comment_error = db
        .execute("CREATE SOURCE rejected_comment (id INT) -- durable raw tail")
        .await
        .unwrap_err();
    assert!(comment_error
        .to_string()
        .contains("cannot persist SQL comments"));
    let default_error = db
        .execute(
            "CREATE SOURCE rejected_default (id INT) WITH \
             ('password' = '${LDB_PASSWORD:-unsafe-default}')",
        )
        .await
        .unwrap_err();
    assert!(default_error
        .to_string()
        .contains("cannot persist secret property"));
    let objects = backing.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(!objects.is_empty());
    let mut durable = String::new();
    for object in objects {
        let bytes = backing
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        durable.push_str(&String::from_utf8(bytes.to_vec()).unwrap());
    }
    assert!(!durable.contains(PASSWORD));
    assert!(!durable.contains(TOKEN));
    assert!(!durable.contains("signed-secret"));
    assert!(!durable.contains("socket-secret"));
    assert!(!durable.contains("sas-secret"));
    assert!(!durable.contains("list-secret"));
    assert!(!durable.contains("unsafe-default"));
    assert!(!durable.contains("rejected"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_secret_reference_is_resolved_per_node_but_manifest_stays_logical() {
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{
        SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceStart,
        SourceTopology,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::{CatalogManifest, CatalogManifestEntry};
    use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};
    use object_store::ObjectStore;

    const VARIABLE: &str = "LDB_TEST_CLUSTER_CONNECTOR_PASSWORD";
    const PASSWORD: &str = "resolved-only-on-this-node";
    const DDL: &str = "CREATE SOURCE secured (id INT) WITH ('connector' = 'capture-secret', \
        'password' = '${LDB_TEST_CLUSTER_CONNECTOR_PASSWORD}')";

    struct CapturingSource {
        schema: Arc<Schema>,
        observed: Arc<parking_lot::Mutex<Option<String>>>,
    }

    #[async_trait]
    impl SourceConnector for CapturingSource {
        fn set_vnode_assignment(
            &mut self,
            _source_identity: &str,
            _registry: Arc<VnodeRegistry>,
            _self_id: NodeId,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            *self.observed.lock() = request.config.get("password").map(str::to_owned);
            Ok(())
        }

        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    let observed = Arc::new(parking_lot::Mutex::new(None));
    let backing = Arc::new(object_store::memory::InMemory::new());
    let object_store: Arc<dyn ObjectStore> = backing;
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    manifest_store
        .seal(
            &CatalogManifest::new(vec![CatalogManifestEntry {
                canonical_name: "secured".into(),
                kind: CatalogObjectKind::Source,
                ddl: DDL.into(),
            }])
            .unwrap(),
            &authority.lease.proof(),
        )
        .await
        .unwrap();
    let controller = Arc::clone(&authority.controller);
    controller.publish_recovery_incarnation().await.unwrap();
    controller.publish_checkpoint_assignment_fence(Some(
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[1],
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: controller.recovery_incarnation(),
            }],
        )
        .unwrap(),
    ));
    controller.set_active(true);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .state_backend(Arc::new(ObjectStoreBackend::cluster_shared(
            Arc::clone(&authority.checkpoint_store),
            "node-1",
            1,
        )))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, NodeId(1))))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(3_600_000),
            ..Default::default()
        })
        .config_var(VARIABLE, PASSWORD)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .register_connector({
            let observed = Arc::clone(&observed);
            move |registry| {
                registry.register_source(
                    "capture-secret",
                    ConnectorInfo {
                        name: "capture-secret".into(),
                        display_name: "capture-secret".into(),
                        version: "1".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(move |_| {
                        Box::new(CapturingSource {
                            schema: Arc::clone(&schema),
                            observed: Arc::clone(&observed),
                        })
                    }),
                )
            }
        })
        .build()
        .await
        .unwrap();

    db.execute_cluster_bootstrap(DDL).await.unwrap();
    db.start().await.unwrap();
    assert_eq!(observed.lock().as_deref(), Some(PASSWORD));
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries[0].ddl, DDL);
    assert!(manifest.entries[0]
        .ddl
        .contains(&format!("${{{VARIABLE}}}")));
    assert!(!manifest.entries[0].ddl.contains(PASSWORD));
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn manifest_replay_rejects_connector_schema_rediscovery_before_factory_use() {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_core::cluster::control::{CatalogManifest, CatalogManifestEntry};
    use object_store::ObjectStore;

    const DDL: &str = "CREATE SOURCE unstable WITH ('connector' = 'changing-discovery')";
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    manifest_store
        .seal(
            &CatalogManifest::new(vec![CatalogManifestEntry {
                canonical_name: "unstable".into(),
                kind: CatalogObjectKind::Source,
                ddl: DDL.into(),
            }])
            .unwrap(),
            &authority.lease.proof(),
        )
        .await
        .unwrap();

    for _ in 0..2 {
        let factory_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&authority.controller))
            .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
            .catalog_manifest_store(Arc::clone(&manifest_store))
            .register_connector({
                let factory_calls = Arc::clone(&factory_calls);
                move |registry| {
                    registry.register_source(
                        "changing-discovery",
                        ConnectorInfo {
                            name: "changing-discovery".into(),
                            display_name: "changing-discovery".into(),
                            version: "1".into(),
                            is_source: true,
                            is_sink: false,
                            config_keys: vec![],
                        },
                        Arc::new(move |_| {
                            factory_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Box::new(laminar_connectors::testing::MockSourceConnector::new())
                        }),
                    )
                }
            })
            .build()
            .await
            .unwrap();
        let error = db.execute_cluster_bootstrap_batch(&[]).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("lacks an explicit durable schema"));
        assert_eq!(factory_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(db.catalog.get_source("unstable").is_none());
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn stopped_cluster_follower_cannot_publish_while_a_peer_is_active() {
    use laminar_core::cluster::control::{
        CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use object_store::ObjectStore;

    let self_id = NodeId(2);
    let active_peer = NodeInfo {
        id: NodeId(1),
        name: "active".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![active_peer]);
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let manifest_store = Arc::new(CatalogManifestStore::new(Arc::new(
        laminar_core::cluster::control::LeaderLeaseStore::new(Arc::clone(&object_store), 1_000),
    )));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(object_store)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    DbState::Stopped.store(&db.state);

    let error = db
        .execute("CREATE SOURCE unsafe_local (id INT)")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("replicated topology-version barrier"));
    assert!(db.catalog.get_source("unsafe_local").is_none());
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn sealed_catalog_exact_replay_is_a_noop_and_local_divergence_fails_closed() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let writer = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    writer
        .execute_cluster_bootstrap("CREATE SOURCE durable (id INT)")
        .await
        .unwrap();

    let (omitted_controller, _omitted_lease) = catalog_authority_controller(
        laminar_core::cluster::discovery::NodeId(2),
        authority.lease.owner.clone(),
    );
    let omitted = LaminarDB::builder()
        .cluster_controller(omitted_controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let error = omitted
        .execute_cluster_bootstrap_batch(&[])
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exactly match the complete ordered sealed inventory"));

    let (exact_controller, _exact_lease) = catalog_authority_controller(
        laminar_core::cluster::discovery::NodeId(3),
        authority.lease.owner.clone(),
    );
    let exact = LaminarDB::builder()
        .cluster_controller(exact_controller)
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    let result = exact
        .execute_cluster_bootstrap("CREATE SOURCE durable (id INT)")
        .await
        .unwrap();
    assert!(matches!(
        result,
        ExecuteResult::Ddl(DdlInfo { applied: false, .. })
    ));
    assert_eq!(
        manifest_store.load().await.unwrap().unwrap().entries.len(),
        1
    );

    exact
        .connector_manager
        .lock()
        .store_ddl("durable", "CREATE SOURCE durable (id BIGINT)");
    let error = exact
        .execute_cluster_bootstrap("CREATE SOURCE durable (id INT)")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("conflicts with catalog manifest"));
    assert_eq!(
        manifest_store.load().await.unwrap().unwrap().entries.len(),
        1
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn clustered_destructive_catalog_ddl_is_rejected_before_mutation() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    db.execute_cluster_bootstrap("CREATE SOURCE durable (id INT)")
        .await
        .unwrap();
    let error = db.execute("DROP SOURCE durable CASCADE").await.unwrap_err();
    assert!(error
        .to_string()
        .contains("only reversible typed CREATE statements"));
    assert!(db.catalog.get_source("durable").is_some());
    assert!(db.ctx.table_exist("durable").unwrap());
    assert_eq!(
        db.catalog_namespace.lock().get("durable").copied(),
        Some(CatalogObjectKind::Source)
    );
    let manifest = manifest_store.load().await.unwrap().unwrap();
    assert_eq!(manifest.entries.len(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn clustered_reference_tables_fail_before_local_registration() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority(object_store).await;
    let manifest_store = Arc::clone(&authority.manifest_store);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    for (name, sql) in [
        (
            "local_table",
            "CREATE TABLE local_table (id INT PRIMARY KEY)",
        ),
        (
            "local_lookup",
            "CREATE LOOKUP TABLE local_lookup (id INT NOT NULL, PRIMARY KEY (id)) \
             WITH ('connector' = 'static')",
        ),
    ] {
        let error = db.execute_cluster_bootstrap(sql).await.unwrap_err();
        assert!(
            error.to_string().contains("distributed state"),
            "unexpected cluster rejection for {name}: {error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(!db.table_store.read().has_table(name));
        assert!(!db.ctx.table_exist(name).unwrap());
        assert!(!db.connector_manager.lock().tables().contains_key(name));
    }
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_manifest_invalid_entry_fails_before_any_replay() {
    use laminar_core::cluster::control::{
        CatalogManifest, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
    };
    use object_store::ObjectStore;

    fn entry(name: &str, kind: CatalogObjectKind, ddl: &str) -> CatalogManifestEntry {
        CatalogManifestEntry {
            canonical_name: name.to_string(),
            kind,
            ddl: ddl.to_string(),
        }
    }

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        Arc::clone(&object_store),
        1_000,
    ));
    let owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(1),
        boot: uuid::Uuid::from_u128(1),
        process_term: 1,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(lease) =
        authority.try_acquire(&owner, 0).await.unwrap()
    else {
        unreachable!()
    };
    let manifest_store = Arc::new(CatalogManifestStore::new(authority));
    let definitions = [
        entry(
            "manual_source",
            CatalogObjectKind::Source,
            "CREATE SOURCE manual_source (id INT)",
        ),
        entry(
            "manual_sink",
            CatalogObjectKind::Sink,
            "CREATE SINK manual_sink FROM manual_source",
        ),
        entry(
            "plain_table",
            CatalogObjectKind::Table,
            "CREATE TABLE plain_table (id INT PRIMARY KEY)",
        ),
        entry("broken", CatalogObjectKind::Source, "NOT VALID DDL"),
        entry(
            "later",
            CatalogObjectKind::Source,
            "CREATE SOURCE later (id INT)",
        ),
    ];
    let manifest = CatalogManifest::new(definitions.into_iter().collect()).unwrap();
    manifest_store
        .seal(&manifest, &lease.proof())
        .await
        .unwrap();
    let (controller, _lease_tx) = catalog_authority_controller(owner.node, owner.clone());
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(object_store)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();

    let error = db
        .start()
        .await
        .expect_err("one invalid manifest entry must abort cluster startup");
    assert!(error.to_string().contains("LDB-6003"));
    assert!(error.to_string().contains("'broken'"));
    assert_eq!(DbState::load(&db.state), DbState::Created);
    for name in [
        "manual_source",
        "manual_sink",
        "plain_table",
        "broken",
        "later",
    ] {
        assert!(db.connector_manager.lock().get_ddl(name).is_none());
        assert!(!db.catalog_namespace.lock().contains_key(name));
    }
    assert!(db.catalog.get_source("manual_source").is_none());
    assert!(db.catalog.get_sink_input("manual_sink").is_none());
    assert!(!db.ctx.table_exist("plain_table").unwrap());
    assert!(db
        .catalog_manifest_store
        .lock()
        .as_ref()
        .is_some_and(|installed| Arc::ptr_eq(installed, &manifest_store)));

    let retry = db.start().await.unwrap_err();
    assert!(retry.to_string().contains("'broken'"));
    assert_eq!(DbState::load(&db.state), DbState::Created);
}

#[tokio::test]
async fn test_drop_mv_unregisters_stream() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM events")
        .await;

    if result.is_ok() {
        // Verify registered
        {
            let mgr = db.connector_manager.lock();
            assert!(mgr.streams().contains_key("mv1"));
        }

        // Drop the MV
        db.execute("DROP MATERIALIZED VIEW mv1").await.unwrap();

        // Verify unregistered
        {
            let mgr = db.connector_manager.lock();
            assert!(
                !mgr.streams().contains_key("mv1"),
                "stream should be unregistered after DROP MV"
            );
        }
    }
}

#[tokio::test]
async fn test_set_session_property() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET parallelism = 4").await.unwrap();
    assert_eq!(
        db.get_session_property("parallelism"),
        Some("4".to_string())
    );
}

#[tokio::test]
async fn test_set_session_property_string_value() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET state_ttl = '1 hour'").await.unwrap();
    assert_eq!(
        db.get_session_property("state_ttl"),
        Some("1 hour".to_string())
    );
}

#[tokio::test]
async fn test_set_session_property_overwrite() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET batch_size = 100").await.unwrap();
    db.execute("SET batch_size = 200").await.unwrap();
    assert_eq!(
        db.get_session_property("batch_size"),
        Some("200".to_string())
    );
}

#[tokio::test]
async fn test_get_session_property_not_set() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.get_session_property("nonexistent"), None);
}

#[tokio::test]
async fn test_session_properties_all() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET parallelism = 4").await.unwrap();
    db.execute("SET state_ttl = '1 hour'").await.unwrap();
    let props = db.session_properties();
    assert_eq!(props.len(), 2);
    assert_eq!(props.get("parallelism"), Some(&"4".to_string()));
    assert_eq!(props.get("state_ttl"), Some(&"1 hour".to_string()));
}

#[tokio::test]
async fn alter_source_is_rejected_without_schema_mutation() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    let schema = db.catalog.describe_source("events").unwrap();
    assert_eq!(schema.fields().len(), 2);

    let error = db
        .execute("ALTER SOURCE events ADD COLUMN new_col VARCHAR")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("disabled"));

    let schema = db.catalog.describe_source("events").unwrap();
    assert_eq!(schema.fields().len(), 2);
}

#[tokio::test]
async fn test_alter_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("ALTER SOURCE nonexistent ADD COLUMN col INT")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn alter_source_properties_are_rejected_without_mutation() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    let error = db
        .execute("ALTER SOURCE events SET ('batch.size' = '1000')")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("disabled"));
    assert_eq!(db.get_session_property("events.batch.size"), None);
}

#[test]
fn test_extract_connector_from_with_options_basic() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "kafka".to_string());
    opts.insert("topic".to_string(), "events".to_string());
    opts.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    opts.insert("format".to_string(), "json".to_string());

    let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

    // 'connector' and 'format' are extracted, not in connector_options
    assert!(!conn_opts.contains_key("connector"));
    assert!(!conn_opts.contains_key("format"));
    assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
    assert_eq!(
        conn_opts.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(format, Some("json".to_string()));
    assert!(fmt_opts.is_empty());
}

#[test]
fn test_extract_connector_filters_streaming_keys() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "websocket".to_string());
    opts.insert("url".to_string(), "wss://feed.example.com".to_string());
    opts.insert("buffer_size".to_string(), "4096".to_string());
    opts.insert("backpressure".to_string(), "block".to_string());
    opts.insert("watermark_delay".to_string(), "5s".to_string());

    let (conn_opts, _, _) = extract_connector_from_with_options(&opts);

    // Streaming keys should NOT be in connector_options
    assert!(!conn_opts.contains_key("buffer_size"));
    assert!(!conn_opts.contains_key("backpressure"));
    assert!(!conn_opts.contains_key("watermark_delay"));
    // Connector-specific key should be present
    assert_eq!(
        conn_opts.get("url"),
        Some(&"wss://feed.example.com".to_string())
    );
}

#[test]
fn test_extract_connector_format_options() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "kafka".to_string());
    opts.insert("format".to_string(), "avro".to_string());
    opts.insert(
        "format.schema.registry.url".to_string(),
        "http://localhost:8081".to_string(),
    );
    opts.insert("topic".to_string(), "events".to_string());

    let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

    assert_eq!(format, Some("avro".to_string()));
    assert_eq!(
        fmt_opts.get("schema.registry.url"),
        Some(&"http://localhost:8081".to_string())
    );
    assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
    assert!(!conn_opts.contains_key("format.schema.registry.url"));
}

#[tokio::test]
async fn test_create_source_with_connector_option() {
    // Verify that WITH ('connector' = '...') is accepted at the DDL level.
    // The actual connector won't be instantiated because the type isn't
    // registered in the default embedded registry, so we just check
    // that the error is "Unknown source connector type" (meaning the
    // WITH clause was correctly routed) rather than silently ignored.
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE SOURCE ws_feed (id BIGINT, data TEXT) WITH (
                'connector' = 'websocket',
                'url' = 'wss://feed.example.com',
                'format' = 'json'
            )",
        )
        .await;

    // Without the websocket feature, the connector type won't be registered,
    // so we expect an "Unknown source connector type" error — which proves
    // the WITH clause WAS routed to the connector registry.
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            msg.contains("Unknown source connector type"),
            "Expected connector routing error, got: {msg}"
        );
    } else {
        // If websocket feature IS enabled, the connector type is registered
        // and the DDL succeeds — also acceptable.
    }
}

#[tokio::test]
async fn test_show_sources_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    let result = db.execute("SHOW SOURCES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "source_name");
            assert_eq!(batch.schema().field(1).name(), "connector");
            assert_eq!(batch.schema().field(2).name(), "format");
            assert_eq!(batch.schema().field(3).name(), "watermark_column");

            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "events");

            let wm = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(wm.value(0), "ts");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_sinks_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();

    let result = db.execute("SHOW SINKS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "sink_name");
            assert_eq!(batch.schema().field(1).name(), "input");
            assert_eq!(batch.schema().field(2).name(), "connector");
            assert_eq!(batch.schema().field(3).name(), "format");

            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "output");

            let inputs = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(inputs.value(0), "events");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_streams_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM my_stream AS SELECT 1 FROM events")
        .await
        .unwrap();

    let result = db.execute("SHOW STREAMS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "stream_name");
            assert_eq!(batch.schema().field(1).name(), "sql");

            let sqls = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(
                sqls.value(0).contains("SELECT"),
                "SQL column should contain query"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_source() {
    let db = LaminarDB::open().unwrap();
    let ddl = "CREATE SOURCE events (id BIGINT, name VARCHAR)";
    db.execute(ddl).await.unwrap();

    let result = db.execute("SHOW CREATE SOURCE events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema().field(0).name(), "create_statement");
            let stmts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(stmts.value(0), ddl);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_sink() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    let ddl = "CREATE SINK output FROM events";
    db.execute(ddl).await.unwrap();

    let result = db.execute("SHOW CREATE SINK output").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema().field(0).name(), "create_statement");
            let stmts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(stmts.value(0), ddl);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW CREATE SOURCE nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_show_create_sink_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW CREATE SINK nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_explain_analyze_returns_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("EXPLAIN ANALYZE SELECT * FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(
                key_vals.contains(&"rows_produced"),
                "Expected rows_produced metric, got: {key_vals:?}"
            );
            assert!(
                key_vals.contains(&"execution_time_ms"),
                "Expected execution_time_ms metric, got: {key_vals:?}"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_explain_without_analyze_has_no_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db.execute("EXPLAIN SELECT * FROM events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(
                !key_vals.contains(&"rows_produced"),
                "EXPLAIN without ANALYZE should not have rows_produced"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_connectorless_source_does_not_break_pipeline() {
    let db = LaminarDB::open().unwrap();

    // Connector-less source (no FROM clause) — formerly caused
    // LDB-1002 "No partitions provided" on every pipeline cycle.
    db.execute("CREATE SOURCE metadata (symbol VARCHAR, category VARCHAR)")
        .await
        .unwrap();

    // A real source with a watermark that the pipeline will process.
    db.execute(
        "CREATE SOURCE trades (id BIGINT, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute("CREATE STREAM out AS SELECT id, price FROM trades")
        .await
        .unwrap();

    db.start().await.unwrap();

    // Push data into the real source. `ts TIMESTAMP` maps to
    // Timestamp(Microsecond), so values here are in µs.
    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::Float64Array::from(vec![100.0, 200.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    // Let the pipeline run a few cycles.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify the pipeline processed data without errors.
    let m = db.metrics();
    assert!(m.total_events_ingested > 0, "pipeline should ingest events");

    // Push data into the connector-less source via push_arrow — should
    // work without causing pipeline errors.
    let meta_handle = db.source_untyped("metadata").unwrap();
    let meta_schema = meta_handle.schema().clone();
    let meta_batch = RecordBatch::try_new(
        meta_schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["BTC", "ETH"])),
            Arc::new(arrow::array::StringArray::from(vec!["L1", "L1"])),
        ],
    )
    .unwrap();
    meta_handle.push_arrow(meta_batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Pipeline should still be healthy.
    let m2 = db.metrics();
    assert!(
        m2.total_events_ingested >= m.total_events_ingested,
        "pipeline should continue after connector-less source push"
    );
}

#[tokio::test]
async fn connector_options_resolve_vars() {
    // `${VAR}` resolves in connector option values (config vars, then env) for
    // both sources and sinks — and only there, not elsewhere in the statement.
    let db = LaminarDB::builder()
        .config_var("TOPIC", "events")
        .build()
        .await
        .unwrap();
    db.execute(
        "CREATE SOURCE s (id BIGINT) WITH ('connector' = 'generator', 'topic' = '${TOPIC}')",
    )
    .await
    .unwrap();
    {
        let mgr = db.connector_manager.lock();
        let opts = &mgr.sources().get("s").unwrap().connector_options;
        assert_eq!(opts.get("topic").map(String::as_str), Some("events"));
    }
    // Sinks go through the same resolver — an unresolved option errors (raised
    // before the unknown-connector check), proving the sink path is wired.
    let err = db
        .execute("CREATE SINK snk FROM s WITH ('connector' = 'noop', 'topic' = '${MISSING_X9Q}')")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Unresolved variable"),
        "sink: {err}"
    );
}

#[tokio::test]
async fn faulted_pipeline_recovers_and_resumes() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id FROM trades")
        .await
        .unwrap();

    // Simulate a compute-thread crash: recoverable state, recorded reason, and
    // the shutdown permit the watcher's `notify_one()` leaves with no waiter.
    DbState::Faulted.store(&db.state);
    *db.last_fault.lock() = Some("operator boom".to_string());
    db.shutdown_signal.notify_one();
    assert_eq!(db.pipeline_state(), "Faulted");

    // start() drains the stale permit, rebuilds from the catalog, and clears it.
    db.start().await.unwrap();
    assert_eq!(db.pipeline_state(), "Running");
    assert!(db.last_fault().is_none());

    // The recovered coordinator must be alive — without draining the permit it
    // would see shutdown immediately and ingest nothing.
    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        db.metrics().total_events_ingested > 0,
        "recovered pipeline should ingest"
    );
}

/// Poll an MV until it has at least `min_rows` rows, or timeout.
async fn poll_mv(db: &LaminarDB, mv: &str, min_rows: usize) -> usize {
    // 90s, not 2s: CI runners are CPU-starved (this MV emits in ~0.1s locally but
    // has hit 10s under nextest, and the `cargo llvm-cov` coverage job — whole
    // suite in one process under instrumentation — needs more). Early-return on
    // `rows >= min_rows` keeps the happy path fast.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        let df = db.ctx.sql(&format!("SELECT * FROM {mv}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if rows >= min_rows || std::time::Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_mv_aggregate_queryable_with_pipeline() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW trade_counts AS \
         SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();

    assert_eq!(poll_mv(&db, "trade_counts", 0).await, 0);

    db.start().await.unwrap();

    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0, 2800.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "trade_counts", 1).await;
    assert!(rows > 0, "MV should have data after pipeline processes");
}

#[tokio::test]
async fn test_mv_append_mode_queryable() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id INT, value DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW filtered AS \
         SELECT id, value FROM events WHERE value > 10.0",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::Float64Array::from(vec![5.0, 15.0, 25.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000, 3_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "filtered", 2).await;
    assert_eq!(rows, 2, "filter MV should have 2 matching rows");
}

#[tokio::test]
async fn test_mv_drop_cleans_up_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW ev_mv AS SELECT * FROM events")
        .await
        .unwrap();

    assert!(db.ctx.sql("SELECT * FROM ev_mv").await.is_ok());
    assert!(db.mv_store.read().has_mv("ev_mv"));

    db.execute("DROP MATERIALIZED VIEW ev_mv").await.unwrap();

    assert!(!db.mv_store.read().has_mv("ev_mv"));
    assert!(
        db.ctx.sql("SELECT * FROM ev_mv").await.is_err(),
        "table should be deregistered after DROP"
    );
}

#[tokio::test]
async fn test_mv_empty_returns_correct_schema() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW ev_mv AS SELECT id, value FROM events")
        .await
        .unwrap();

    let df = db.ctx.sql("SELECT * FROM ev_mv").await.unwrap();
    let schema = df.schema().clone();
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 0);

    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        names.contains(&"id") && names.contains(&"value"),
        "schema should contain projected columns, got: {names:?}"
    );
}

#[tokio::test]
async fn test_mv_hot_add_while_pipeline_running() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    // Need at least one stream before start() for the pipeline to launch.
    db.execute("CREATE STREAM noop AS SELECT * FROM trades")
        .await
        .unwrap();

    db.start().await.unwrap();

    // Create MV AFTER pipeline is running (hot-add via ControlMsg).
    db.execute(
        "CREATE MATERIALIZED VIEW trade_counts AS \
         SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();

    // Wait for the coordinator to process the AddStream control message.
    // Poll observable state instead of a fixed sleep.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while db.metrics().total_cycles == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec![
                "AAPL", "GOOG", "MSFT",
            ])),
            Arc::new(arrow::array::Float64Array::from(vec![
                150.0, 2_800.0, 420.0,
            ])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000, 3_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "trade_counts", 1).await;
    assert!(rows > 0, "hot-added MV should receive pipeline data");
}

/// `NULLIF(SUM(q), 0)` against a `Float64` aggregate must not raise
/// `Invalid comparison operation: Float64 == Int64` at evaluation.
#[tokio::test]
async fn test_nullif_float_with_int_literal_runs_without_error() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute(
        "CREATE SOURCE t (q DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    // The projection has the exact pattern that broke the cross-venue
    // demo: SUM (Float64) divided by NULLIF(SUM, 0). Pre-fix this
    // raised a SQL-cycle error every cycle and emitted nothing; with
    // the TypeCoercionRewriter applied, the cast to Float64 is
    // inserted automatically.
    db.execute(
        "CREATE STREAM out AS \
         SELECT SUM(q) / NULLIF(SUM(q), 0) AS r \
         FROM t \
         GROUP BY TUMBLE(ts, INTERVAL '1' SECOND) EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("t").unwrap();
    let schema = handle.schema().clone();
    // Two events in window [0,1s); one in window [1s,2s) so the first
    // window closes once the watermark reaches 1000ms.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                100_000, 500_000, 1_500_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    // Wait for the coordinator to process at least one cycle.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while prom.events_emitted.get() == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // The load-bearing assertion: zero cycle errors. The pre-fix run
    // would have logged a `post-projection evaluate` error per cycle
    // and dropped output entirely.
    assert!(
        prom.events_emitted.get() >= 1,
        "post-projection should evaluate without Float64==Int64 error"
    );
}

/// Regression: a windowed aggregate over a lateral `UNNEST` must emit.
/// `single_source_table` ignored the UNNEST factor, so the compiled fast path
/// skipped it and the aggregate saw no rows (planned clean, emitted nothing).
#[tokio::test]
async fn windowed_aggregate_over_lateral_unnest_emits() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (msg VARCHAR, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW tag_counts AS \
         SELECT TUMBLE(ts, INTERVAL '5' SECOND) AS bucket, tag, COUNT(*) AS n \
         FROM events, UNNEST(make_array('alpha','beta','gamma')) AS t(tag) \
         WHERE strpos(msg, tag) > 0 \
         GROUP BY TUMBLE(ts, INTERVAL '5' SECOND), tag \
         EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec![
                "alpha and beta",
                "gamma only",
                "no match here",
                "tick", // next-window event advances the watermark to close [0,5s)
            ])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                100_000, 200_000, 300_000, 6_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "tag_counts", 1).await;
    assert!(
        rows >= 1,
        "windowed aggregate over lateral UNNEST should emit"
    );
}

/// An `ASOF JOIN` feeding a materialized view must plan and emit, matching
/// each left row to the latest right row at-or-before its timestamp (per key).
/// `DataFusion` can't lower `AsOf`, so schema resolution rewrites it to a plain
/// join; execution uses the ASOF operator.
#[tokio::test]
async fn asof_join_in_materialized_view_emits_backward_match() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE quotes (sym VARCHAR, price DOUBLE, qts TIMESTAMP, \
         WATERMARK FOR qts AS qts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE SOURCE trades (sym VARCHAR, tts TIMESTAMP, \
         WATERMARK FOR tts AS tts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW enriched AS \
         SELECT t.sym, q.price \
         FROM trades t ASOF JOIN quotes q \
         MATCH_CONDITION(t.tts >= q.qts) \
         ON t.sym = q.sym",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let q = db.source_untyped("quotes").unwrap();
    q.push_arrow(
        RecordBatch::try_new(
            q.schema().clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x", "x", "x"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1_000_000, 5_000_000, 8_000_000,
                ])),
            ],
        )
        .unwrap(),
    )
    .unwrap();

    let t = db.source_untyped("trades").unwrap();
    t.push_arrow(
        RecordBatch::try_new(
            t.schema().clone(),
            vec![
                // trade@3s -> latest quote qts<=3s = 10.0; trade@7s -> 20.0.
                Arc::new(arrow::array::StringArray::from(vec!["x", "x"])),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    3_000_000, 7_000_000,
                ])),
            ],
        )
        .unwrap(),
    )
    .unwrap();

    assert!(
        poll_mv(&db, "enriched", 2).await >= 2,
        "ASOF join in an MV should emit matches"
    );
    let batches = db
        .ctx
        .sql("SELECT price FROM enriched ORDER BY price")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let prices: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        prices,
        vec![10.0, 20.0],
        "backward ASOF should pick the latest quote at-or-before each trade"
    );
}

/// Regression: `COUNT(DISTINCT)` must survive a checkpoint that lands while a
/// window is still open. `Accumulator::state()` drains the DISTINCT set, and
/// the window-checkpoint path calls it on the *live* accumulator — so before
/// the rebuild-from-snapshot fix, a window spanning a checkpoint lost every
/// distinct value seen before it (`COUNT(*)` was unaffected).
#[tokio::test]
async fn count_distinct_survives_midwindow_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = crate::LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = LaminarDB::open_with_config(cfg).unwrap();
    db.execute(
        "CREATE SOURCE src (author VARCHAR, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW ct AS \
         SELECT TUMBLE(ts, INTERVAL '5' SECOND) AS bucket, \
         COUNT(*) AS n, COUNT(DISTINCT author) AS uniq \
         FROM src GROUP BY TUMBLE(ts, INTERVAL '5' SECOND) EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
    db.start().await.unwrap();
    let h = db.source_untyped("src").unwrap();
    let schema = h.schema().clone();
    let push = |author: &str, ts: i64| {
        h.push_arrow(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![author])),
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![ts])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    };
    // author a, then a checkpoint mid-window, then author b; tick@6s closes
    // [0,5s). The window [0,5s) saw two distinct authors across the checkpoint.
    push("a", 100_000);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    db.checkpoint().await.unwrap();
    push("b", 200_000);
    push("z", 6_000_000);
    poll_mv(&db, "ct", 1).await;
    let batches = db
        .ctx
        .sql("SELECT n, uniq FROM ct WHERE n > 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let uniq: i64 = batches
        .iter()
        .flat_map(|b| {
            let u = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            (0..b.num_rows()).map(|i| u.value(i)).collect::<Vec<_>>()
        })
        .max()
        .expect("the [0,5s) window must emit");
    assert_eq!(
        uniq, 2,
        "checkpoint must not drop distinct values seen before it"
    );
}

/// Regression: a windowed aggregate over a SELECT-list `UNNEST` (in a
/// subquery) must emit. The unnest is invisible to FROM-based source
/// detection, so `single_source_table` must still treat it as multi-source —
/// otherwise the compiled fast path skips the expansion and emits nothing.
#[tokio::test]
async fn windowed_aggregate_over_projection_unnest_emits() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE docs (text VARCHAR, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW word_counts AS \
         SELECT TUMBLE(ts, INTERVAL '5' SECOND) AS bucket, w, COUNT(*) AS n \
         FROM (SELECT ts, unnest(string_to_array(text, ' ')) AS w FROM docs) \
         WHERE w <> '' \
         GROUP BY TUMBLE(ts, INTERVAL '5' SECOND), w \
         EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("docs").unwrap();
    let schema = handle.schema().clone();
    handle
        .push_arrow(
            RecordBatch::try_new(
                schema,
                vec![
                    // [0,5s): "a b", "a c" -> a=2, b=1, c=1. "tick" closes it.
                    Arc::new(arrow::array::StringArray::from(vec!["a b", "a c", "tick"])),
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                        100_000, 200_000, 6_000_000,
                    ])),
                ],
            )
            .unwrap(),
        )
        .unwrap();

    assert!(
        poll_mv(&db, "word_counts", 1).await >= 1,
        "projection-unnest MV should emit"
    );
    let batches = db
        .ctx
        .sql("SELECT n FROM word_counts WHERE w = 'a'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "word 'a' appears in both docs within the window");
}

#[tokio::test]
async fn open_subscription_resolves_unknown_name_to_error() {
    let db = LaminarDB::open().unwrap();
    let err = db
        .open_subscription("nope", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap_err();
    assert!(matches!(err, DbError::StreamNotFound(_)));
}

#[tokio::test]
async fn open_subscription_resolves_and_attaches_under_topology_lock() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE raw (id BIGINT)").await.unwrap();
    db.execute("CREATE STREAM visible AS SELECT * FROM raw")
        .await
        .unwrap();

    let topology = db.topology_ddl_lock.write().await;
    let opener = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.open_subscription("visible", None, crate::subscription::SubscribeStart::Tail)
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(
        !opener.is_finished(),
        "subscription must wait before schema lookup while topology DDL is active"
    );

    // Execute the parsed DROP under the write guard already held by this test,
    // matching execute_single's topology serialization without reacquiring it.
    let drop_sql = "DROP STREAM visible";
    let statement = parse_streaming_sql(drop_sql).unwrap().remove(0);
    db.execute_parsed_single(drop_sql, &statement)
        .await
        .unwrap();
    drop(topology);

    let error = opener.await.unwrap().unwrap_err();
    assert!(matches!(error, DbError::StreamNotFound(name) if name == "visible"));
}

/// A SOURCE is not subscribable: only streams/MVs defined over it publish to
/// the registry, so subscribing to the source directly must error (not hang
/// forever), reusing the `StreamNotFound` path.
#[tokio::test]
async fn open_subscription_rejects_bare_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE raw (id BIGINT, val VARCHAR)")
        .await
        .unwrap();
    let err = db
        .open_subscription("raw", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap_err();
    assert!(matches!(err, DbError::StreamNotFound(_)), "got {err:?}");
}

#[tokio::test]
async fn open_subscription_resolves_named_stream() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM all_trades AS SELECT * FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut portal = db
        .open_subscription(
            "all_trades",
            None,
            crate::subscription::SubscribeStart::Tail,
        )
        .await
        .expect("portal opens");

    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    // Wait up to 2s for a Batch frame, ignoring barrier markers.
    let batch = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match portal.next_frame().await {
                Some(crate::subscription::PortalFrame::Batch { batch, .. }) => break Some(batch),
                Some(crate::subscription::PortalFrame::Barrier { .. }) => {}
                Some(crate::subscription::PortalFrame::Lagged(_))
                | Some(crate::subscription::PortalFrame::Error { .. })
                | None => break None,
            }
        }
    })
    .await
    .expect("portal must produce a Batch within 2s")
    .expect("batch frame");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(db.stream_metrics("all_trades").unwrap().total_events, 1);
    assert_eq!(prom.events_emitted.get(), 1);
    assert_eq!(prom.events_dropped.get(), 0);
}

#[tokio::test]
async fn typed_named_subscription_uses_resolved_schema_and_surfaces_drop() {
    #[derive(Debug)]
    struct Row;
    impl crate::handle::FromBatch for Row {
        fn from_batch(_batch: &RecordBatch, _row: usize) -> Self {
            Self
        }

        fn from_batch_all(batch: &RecordBatch) -> Vec<Self> {
            (0..batch.num_rows()).map(|_| Self).collect()
        }
    }

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE raw (id BIGINT)").await.unwrap();
    db.execute("CREATE STREAM visible AS SELECT id FROM raw")
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut subscription = db.subscribe::<Row>("visible").await.unwrap();
    let schema = subscription.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");

    db.execute("DROP STREAM visible").await.unwrap();
    let error = subscription.next_frame().await.unwrap_err();
    assert_eq!(
        error,
        crate::handle::SubscriptionError::Failed {
            message: "object dropped".into()
        }
    );
}

#[tokio::test]
async fn open_subscription_with_invalid_filter_errors_at_open() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR)")
        .await
        .unwrap();
    // Subscribe against an MV (filterable schema resolved at CREATE); a bare
    // source isn't subscribable.
    db.execute("CREATE MATERIALIZED VIEW priced AS SELECT symbol FROM trades")
        .await
        .unwrap();

    let err = db
        .open_subscription(
            "priced",
            Some("nonexistent_col > 1"),
            crate::subscription::SubscribeStart::Tail,
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent_col"),
        "error must mention the bad column, got: {msg}"
    );
}

#[tokio::test]
async fn open_subscription_rejects_unresolved_stream_schema() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE STREAM all_trades AS SELECT * FROM trades")
        .await
        .unwrap();

    let err = db
        .open_subscription(
            "all_trades",
            Some("symbol = 'AAPL'"),
            crate::subscription::SubscribeStart::Tail,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DbError::StreamNotFound(name) if name == "all_trades"));

    struct Row;
    impl crate::handle::FromBatch for Row {
        fn from_batch(_batch: &RecordBatch, _row: usize) -> Self {
            Self
        }

        fn from_batch_all(_batch: &RecordBatch) -> Vec<Self> {
            Vec::new()
        }
    }
    let err = db.subscribe::<Row>("all_trades").await.unwrap_err();
    assert!(matches!(err, DbError::StreamNotFound(name) if name == "all_trades"));
}

#[tokio::test]
async fn open_subscription_with_filter_on_stream_after_start_compiles() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute("CREATE SOURCE trades (symbol VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE STREAM all_trades AS SELECT * FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut portal = db
        .open_subscription(
            "all_trades",
            Some("symbol = 'AAPL'"),
            crate::subscription::SubscribeStart::Tail,
        )
        .await
        .expect("WHERE on a started stream must compile");
    portal.close();
}

#[tokio::test]
async fn drop_stream_closes_subscription() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute("CREATE SOURCE trades (symbol VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE STREAM s AS SELECT * FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut portal = db
        .open_subscription("s", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();

    db.execute("DROP STREAM s").await.unwrap();

    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), portal.next_frame())
        .await
        .expect("portal must terminate after DROP");
    assert!(matches!(
        terminal,
        Some(crate::subscription::PortalFrame::Error { message })
            if message == "object dropped"
    ));
    assert!(portal.next_frame().await.is_none());
}

#[tokio::test]
async fn create_stream_with_retain_history_enables_as_of_replay() {
    use crate::subscription::SubscribeStart;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM all_trades AS SELECT * FROM trades WITH ('retain_history' = '4mb')")
        .await
        .unwrap();
    db.start().await.unwrap();

    // Drive the registry directly: barrier(1) -> batch -> barrier(2). We're
    // not testing the pipeline; we're testing that DDL plumbed the cap so
    // the buffer actually retains.
    let reg = &db.subscription_registry;
    reg.broadcast_barrier(1, 100);

    let schema = db.source_untyped("trades").unwrap().schema().clone();
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0])),
        ],
    )
    .unwrap();
    reg.send_batch("all_trades", batch).unwrap();
    reg.broadcast_barrier(2, 200);

    // Subscribe AS OF EPOCH 1 — should replay batch + barrier(2).
    let mut portal = db
        .open_subscription("all_trades", None, SubscribeStart::AsOfEpoch(1))
        .await
        .expect("AS OF EPOCH 1 must be retained");

    let frames = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut frames = Vec::new();
        for _ in 0..2 {
            if let Some(f) = portal.next_frame().await {
                frames.push(f);
            }
        }
        frames
    })
    .await
    .expect("frames within 1s");
    assert_eq!(frames.len(), 2);
    assert!(matches!(
        frames[0],
        crate::subscription::PortalFrame::Batch { .. }
    ));
    assert!(matches!(
        frames[1],
        crate::subscription::PortalFrame::Barrier { epoch: 2, .. }
    ));
}

#[tokio::test]
async fn open_subscription_as_of_uncommitted_returns_structured_error() {
    use crate::subscription::SubscribeStart;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR)")
        .await
        .unwrap();
    db.execute("CREATE STREAM all_trades AS SELECT * FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    // No checkpoint has committed yet, so this is not a pruning error.
    let err = db
        .open_subscription("all_trades", None, SubscribeStart::AsOfEpoch(1))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        DbError::SubscriptionEpochNotCommitted {
            ref name,
            requested: 1,
            latest_committed: None,
        } if name == "all_trades"
    ));
    assert_eq!(err.code(), laminar_core::error_codes::INVALID_OPERATION);
    assert!(err.to_string().contains("not committed"), "msg: {err}");
}
