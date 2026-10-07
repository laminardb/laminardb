//! The public route uses production connector metadata and exact durable parent replay.

use futures::TryStreamExt;
use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{
    prove_shared_object_store_namespaces, AssignmentSnapshotStore, CatalogManifest,
    CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind, CheckpointDecisionStore,
    ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline,
    LeaseOutcome,
};
use laminar_core::cluster::discovery::NodeId;
use laminar_core::state::VnodeRegistry;
use object_store::ObjectStore;

use super::*;

async fn validate(
    app: Router,
    token: &str,
    request: serde_json::Value,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/v1/cluster/topology/validate")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(request.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn topology_validation_http_is_local_bounded_authenticated_and_does_not_admit_a_target() {
    let node = NodeId(52);
    let boot = uuid::Uuid::from_u128(52);
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 60_000));
    let owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("test namespace must admit its leader");
    };
    let catalog = Arc::new(CatalogManifestStore::new(Arc::clone(&authority)));
    // Deliberately unreachable Kafka address: metadata compilation must not open clients.
    let statements = vec![
        "CREATE SOURCE trades (id BIGINT NOT NULL, value BIGINT NOT NULL) FROM KAFKA ('bootstrap.servers' = '127.0.0.1:1', 'group.id' = 'planning', 'topic' = 'parent')".to_owned(),
        "CREATE STREAM totals AS SELECT id, SUM(value) AS total FROM trades GROUP BY id WITH ('retain_history' = '4mb')".to_owned(),
        "CREATE SINK totals_sink FROM totals INTO KAFKA ('bootstrap.servers' = '127.0.0.1:1', 'topic' = 'old-output')".to_owned(),
    ];
    let manifest = CatalogManifest::new(vec![
        CatalogManifestEntry {
            schema_binding: None,
            canonical_name: "trades".into(),
            kind: CatalogObjectKind::Source,
            catalog_generation: 1,
            ddl: statements[0].clone(),
        },
        CatalogManifestEntry {
            schema_binding: None,
            canonical_name: "totals".into(),
            kind: CatalogObjectKind::Stream,
            catalog_generation: 1,
            ddl: statements[1].clone(),
        },
        CatalogManifestEntry {
            schema_binding: None,
            canonical_name: "totals_sink".into(),
            kind: CatalogObjectKind::Sink,
            catalog_generation: 1,
            ddl: statements[2].clone(),
        },
    ])
    .unwrap();
    catalog.seal(&manifest, &lease.proof()).await.unwrap();
    let reference = manifest.reference().unwrap();
    let deployment = CheckpointDecisionStore::new(Arc::clone(&objects))
        .load_or_create_deployment_id()
        .await
        .unwrap();
    catalog
        .adopt_legacy_topology(
            &lease.proof(),
            uuid::Uuid::from_u128(53).try_into().unwrap(),
            &reference,
            &deployment,
        )
        .await
        .unwrap();
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: boot,
    };
    let namespaces = prove_shared_object_store_namespaces(
        participant,
        &[participant],
        Arc::clone(&control),
        Arc::clone(&objects),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let snapshots = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control),
        control,
        Some(Arc::clone(&snapshots)),
        members_rx.clone(),
        boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    let receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(node.0, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .verified_cluster_namespaces(namespaces)
        .catalog_manifest_store(Arc::clone(&catalog))
        .shuffle_sender(Arc::new(laminar_core::shuffle::ShuffleSender::new(
            node.0, boot,
        )))
        .shuffle_receiver(receiver)
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(
            8,
            laminar_core::state::NodeId(node.0),
        )))
        .delivery_guarantee(laminar_db::DeliveryGuarantee::AtLeastOnce)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    db.execute_cluster_bootstrap_batch(&statements)
        .await
        .unwrap();
    let token = canonical_auth_token(52);
    let mut state = test_state_with_token(&token);
    let mutable = Arc::get_mut(&mut state).unwrap();
    mutable.db = db;
    mutable.cluster = Some(ClusterComponents {
        controller,
        snapshot_store: snapshots,
        membership_rx: members_rx,
    });
    let before_authority = authority.load().await.unwrap();
    let before_objects = objects.list(None).try_collect::<Vec<_>>().await.unwrap();
    let app = build_router(Arc::clone(&state));
    let additions = serde_json::json!([
        "CREATE STREAM probe AS SELECT id, value FROM trades WHERE value > 0",
        "CREATE SINK probe_sink FROM probe INTO KAFKA ('bootstrap.servers' = '127.0.0.1:1', 'topic' = 'future-output')",
    ]);
    let request = serde_json::json!({"expected_parent_version": 1, "statements": additions});
    let response = validate(app.clone(), "wrong-token", request.clone()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = validate(app.clone(), &token, request.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let plan: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(plan["scope"], "local_candidate_plan");
    assert_eq!(plan["parent_version"], 1);
    assert_eq!(plan["target_version"], 2);
    assert_eq!(
        plan["parent_manifest"],
        serde_json::to_value(&reference).unwrap()
    );
    assert_ne!(plan["parent_pipeline"], plan["target_pipeline"]);
    assert_eq!(plan["objects"].as_array().unwrap().len(), 5);
    assert!(plan["objects"]
        .as_array()
        .unwrap()
        .iter()
        .any(|object| object["name"] == "totals"
            && object["managed_state_contract"] == "sql_aggregate_v1"));
    assert_eq!(
        plan["required_before_activation"].as_array().unwrap().len(),
        6
    );
    let repeated = validate(app.clone(), &token, request).await;
    let bytes = axum::body::to_bytes(repeated.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        plan,
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
    );
    let response = validate(
        app.clone(),
        &token,
        serde_json::json!({"expected_parent_version": 1, "statements": ["CREATE STREAM new_state AS SELECT id, SUM(value) FROM trades GROUP BY id"]}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let managed_plan: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let added = managed_plan["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|object| object["name"] == "new_state")
        .unwrap();
    assert_eq!(added["transition"], "add_future_only");
    assert_eq!(added["initialization"], "empty_managed_state_at_cut");
    assert_eq!(added["managed_state_contract"], "sql_aggregate_v1");
    for statements in [
        vec!["DROP SINK totals_sink"],
        vec!["DROP SINK totals_sink", "DROP STREAM totals"],
        vec![
            "DROP SINK totals_sink",
            "DROP STREAM totals",
            "DROP SOURCE trades",
        ],
        vec![
            "DROP SINK totals_sink",
            "DROP STREAM totals",
            "CREATE STREAM totals AS SELECT value, SUM(id) AS total FROM trades GROUP BY value WITH ('retain_history' = '4mb')",
            "CREATE SINK totals_sink FROM totals INTO KAFKA ('bootstrap.servers' = '127.0.0.1:1', 'topic' = 'changed-output')",
        ],
    ] {
        let response = validate(
            app.clone(),
            &token,
            serde_json::json!({"expected_parent_version": 1, "statements": statements}),
        )
        .await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK, "statements: {statements:?}; response: {}", String::from_utf8_lossy(&bytes));
        let removal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(removal["statements"], serde_json::json!(statements));
        let removed = removal["objects"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|object| object["transition"] == "remove")
            .collect::<Vec<_>>();
        assert_eq!(removed.len(), statements.iter().filter(|sql| sql.starts_with("DROP")).count());
        assert!(removed
            .iter()
            .all(|object| object["initialization"] == "retire_at_cut"));
    }
    for (request, expected) in [
        (
            serde_json::json!({"expected_parent_version": 2, "statements": additions}),
            StatusCode::CONFLICT,
        ),
        (
            serde_json::json!({"expected_parent_version": 0, "statements": additions}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"expected_parent_version": 1, "statements": []}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"expected_parent_version": 1, "statements": ["CREATE STREAM unsupported AS SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY value) AS rn FROM trades"]}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            serde_json::json!({"expected_parent_version": 1, "statements": ["DROP STREAM totals"]}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            serde_json::json!({"expected_parent_version": 1, "statements": additions, "activate": true}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            serde_json::json!({"expected_parent_version": 1, "statements": ["x".repeat(512 * 1024)]}),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        let response = validate(app.clone(), &token, request).await;
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    for (identity, bearer, expected) in [
        (
            "00000000-0000-0000-0000-000000000058",
            "wrong-token",
            StatusCode::UNAUTHORIZED,
        ),
        ("not-a-uuid", token.as_str(), StatusCode::BAD_REQUEST),
        (
            "00000000-0000-0000-0000-000000000058",
            token.as_str(),
            StatusCode::CONFLICT,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/api/v1/cluster/topology/operations/{identity}/prepare"
                    ))
                    .header("authorization", format!("Bearer {bearer}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if expected != StatusCode::UNAUTHORIZED {
            assert_eq!(response.headers()["cache-control"], "no-store");
        }
    }
    assert_eq!(authority.load().await.unwrap(), before_authority);
    assert_eq!(
        objects.list(None).try_collect::<Vec<_>>().await.unwrap(),
        before_objects
    );
    assert_eq!(catalog.load().await.unwrap().unwrap(), manifest);
    assert_eq!(state.db.pipeline_state(), "Created");
    assert!(state
        .db
        .execute("CREATE STREAM forbidden AS SELECT * FROM trades")
        .await
        .unwrap_err()
        .to_string()
        .contains("LDB-6043"));
}
