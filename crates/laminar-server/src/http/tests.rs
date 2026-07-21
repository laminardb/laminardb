use super::*;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

#[test]
fn cap_result_trims_and_flags() {
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
    let batch = |n: i32| {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from((0..n).collect::<Vec<_>>()))],
        )
        .unwrap()
    };
    let rows = |bs: &[RecordBatch]| bs.iter().map(RecordBatch::num_rows).sum::<usize>();

    // Under the cap: unchanged, not truncated.
    let (b, t) = cap_result(vec![batch(3)], 5);
    assert_eq!((rows(&b), t), (3, false));
    // Exactly at the cap across batches: complete, not truncated.
    let (b, t) = cap_result(vec![batch(3), batch(2)], 5);
    assert_eq!((rows(&b), t), (5, false));
    // Over the cap: trimmed to the cap, truncated.
    let (b, t) = cap_result(vec![batch(3), batch(4)], 5);
    assert_eq!((rows(&b), t), (5, true));
}

fn ready_serving_gate() -> Arc<ServingGate> {
    let gate = Arc::new(ServingGate::starting());
    assert!(gate.open());
    gate
}

fn test_state_with_db_and_gate(
    db: Arc<LaminarDB>,
    serving_gate: Arc<ServingGate>,
) -> Arc<AppState> {
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    let server_metrics = crate::metrics::ServerMetrics::new(&registry);
    Arc::new(AppState {
        db,
        config_path: PathBuf::from("test.toml"),
        current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
            server: crate::config::ServerSection::default(),
            state: laminar_core::state::StateBackendConfig::default(),
            checkpoint: crate::config::CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            node_id: None,
            sql: None,
            ai: Default::default(),
            models: Default::default(),
        }),
        reload_guard: ReloadGuard::new(),

        registry,
        server_metrics,
        ws_slots: ws_connection_slots(),
        serving_gate,
        #[cfg(feature = "cluster")]
        cluster: None,
    })
}

fn test_state_with_db(db: Arc<LaminarDB>) -> Arc<AppState> {
    test_state_with_db_and_gate(db, ready_serving_gate())
}

fn test_state_with_gate(serving_gate: Arc<ServingGate>) -> Arc<AppState> {
    test_state_with_db_and_gate(LaminarDB::open().unwrap(), serving_gate)
}

fn test_state() -> Arc<AppState> {
    test_state_with_db(LaminarDB::open().unwrap())
}

/// Like [`test_state`] but with a console bearer token configured, so the
/// auth middleware is active on protected routes.
fn test_state_with_token_and_gate(token: &str, serving_gate: Arc<ServingGate>) -> Arc<AppState> {
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    let db = LaminarDB::open().unwrap();
    db.set_engine_metrics(engine_metrics);
    let server_metrics = crate::metrics::ServerMetrics::new(&registry);
    let server = crate::config::ServerSection {
        console_token: Some(crate::config::Secret::new(token)),
        ..Default::default()
    };
    Arc::new(AppState {
        db,
        config_path: PathBuf::from("test.toml"),
        current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
            server,
            state: laminar_core::state::StateBackendConfig::default(),
            checkpoint: crate::config::CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            node_id: None,
            sql: None,
            ai: Default::default(),
            models: Default::default(),
        }),
        reload_guard: ReloadGuard::new(),
        registry,
        server_metrics,
        ws_slots: ws_connection_slots(),
        serving_gate,
        #[cfg(feature = "cluster")]
        cluster: None,
    })
}

fn test_state_with_token(token: &str) -> Arc<AppState> {
    test_state_with_token_and_gate(token, ready_serving_gate())
}

#[test]
fn terminal_serving_fence_wins_a_concurrent_startup_open() {
    for _ in 0..64 {
        let gate = Arc::new(ServingGate::starting());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            let opener_gate = Arc::clone(&gate);
            let opener_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                opener_barrier.wait();
                opener_gate.open();
            });
            let fencer_gate = Arc::clone(&gate);
            let fencer_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                fencer_barrier.wait();
                fencer_gate.fence();
            });
            barrier.wait();
        });

        assert_eq!(
            gate.rejection_message(),
            Some("server serving authority is fenced")
        );
        assert!(!gate.open(), "a terminal fence must be irreversible");
    }
}

#[tokio::test]
async fn test_auth_required_without_token_returns_401() {
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/sources")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_with_valid_bearer_returns_200() {
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/sources")
        .header("authorization", "Bearer supersecret-token")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_auth_with_wrong_bearer_returns_401() {
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/sources")
        .header("authorization", "Bearer not-the-token")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_with_query_token_returns_200() {
    // WebSocket clients can't set the Authorization header, so the token is
    // accepted from the query string — but only on `/ws/` routes. A plain
    // (non-upgrade) GET to a WS route passes auth and is then rejected by
    // the WebSocket upgrade extractor, so the meaningful assertion is that
    // auth did not reject it with 401.
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/ws/events?token=supersecret-token")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_query_token_on_http_returns_401() {
    // The `?token=` query parameter is honored only on WS upgrade routes.
    // On a normal HTTP control-plane route it is ignored, so a request
    // without a bearer header is unauthorized.
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/sources?token=supersecret-token")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_public_health_bypasses_auth() {
    // /health is public even when a console token is configured.
    let state = test_state_with_token("supersecret-token");
    let app = build_router(state);

    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn startup_gate_preserves_probes_and_rejects_every_other_route() {
    let state = test_state_with_gate(Arc::new(ServingGate::starting()));
    state
        .current_config
        .write()
        .server
        .console_cors_allowed_origins = Some(vec!["https://console.example".into()]);
    let app = build_router(state);

    for path in ["/health", "/metrics"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "probe {path}");
    }

    let readiness = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(readiness.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("server startup is not complete"));

    for path in ["/api/v1/sources", "/not-a-route"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(axum::http::header::ORIGIN, "https://console.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "non-probe {path}"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("https://console.example"),
            "closed startup response must retain CORS headers for {path}"
        );
    }
}

#[tokio::test]
async fn startup_gate_completes_closed_requests_instead_of_replaying_them() {
    let state =
        test_state_with_token_and_gate("supersecret-token", Arc::new(ServingGate::starting()));
    let app = build_router(Arc::clone(&state));
    let request = || {
        Request::builder()
            .uri("/api/v1/sources")
            .body(Body::empty())
            .unwrap()
    };

    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        app.clone().oneshot(request()),
    )
    .await
    .expect("a closed gate must answer immediately")
    .unwrap();
    assert_eq!(closed.status(), StatusCode::SERVICE_UNAVAILABLE);

    assert!(state.open_startup_gate());
    assert_eq!(closed.status(), StatusCode::SERVICE_UNAVAILABLE);
    let after_open = app.oneshot(request()).await.unwrap();
    assert_eq!(after_open.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn terminal_serving_fence_keeps_liveness_public_and_rejects_serving() {
    let gate = ready_serving_gate();
    let state = test_state_with_gate(Arc::clone(&gate));
    gate.fence();
    assert!(!state.open_startup_gate());
    let app = build_router(state);

    for path in ["/health", "/metrics"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "public path {path}");
    }

    for path in ["/ready", "/api/v1/sources"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("server serving authority is fenced"),
            "terminal rejection for {path}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn expired_process_deadline_rejects_serving_before_async_gate_fence() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let node = laminar_core::cluster::discovery::NodeId(41);
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let assignment_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        node,
        control,
        Some(Arc::clone(&snapshot_store)),
        members_rx.clone(),
    ));
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::fenced(),
        ))
        .unwrap();

    let mut state = test_state_with_gate(ready_serving_gate());
    Arc::get_mut(&mut state).unwrap().cluster = Some(ClusterComponents {
        controller,
        snapshot_store,
        membership_rx: members_rx,
    });
    let app = build_router(state);

    for path in ["/ready", "/api/v1/sources"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("server process lease is no longer live"),
            "deadline rejection for {path}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn recovering_cluster_rejects_readiness_and_serving_routes() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let node = laminar_core::cluster::discovery::NodeId(43);
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let assignment_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        node,
        control,
        Some(Arc::clone(&snapshot_store)),
        members_rx.clone(),
    ));
    controller.set_recovering(true);

    let mut state = test_state_with_gate(ready_serving_gate());
    Arc::get_mut(&mut state).unwrap().cluster = Some(ClusterComponents {
        controller,
        snapshot_store,
        membership_rx: members_rx,
    });
    let app = build_router(state);

    for path in ["/ready", "/api/v1/sources"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("server is completing coordinated recovery"),
            "recovery rejection for {path}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_deadline_loss_wakes_the_serving_gate() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let gate = Arc::new(ServingGate::starting());
    assert!(gate.open());
    let deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    let node = laminar_core::cluster::discovery::NodeId(42);
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = ClusterController::new(node, control, None, members_rx);
    controller
        .set_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    gate.install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();

    controller.fence_process_lease();

    tokio::time::timeout(std::time::Duration::from_secs(1), gate.wait_fenced())
        .await
        .expect("process lease loss did not wake HTTP/WS serving authority");
    assert_eq!(
        gate.rejection_message(),
        Some("server serving authority is fenced")
    );
}

async fn tcp_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test]
async fn live_listener_rejects_closed_gate_then_serves_after_open() {
    let state = test_state_with_gate(Arc::new(ServingGate::starting()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (server, started) = serve_listener(build_router(Arc::clone(&state)), listener);
    tokio::time::timeout(std::time::Duration::from_secs(1), started)
        .await
        .expect("HTTP accept loop must start promptly")
        .expect("HTTP serve task must remain live after its first poll");

    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        tcp_get(addr, "/api/v1/sources"),
    )
    .await
    .expect("a live listener must answer a closed gate immediately");
    assert!(
        closed.starts_with("HTTP/1.1 503 "),
        "closed gate response: {closed}"
    );
    assert!(closed.contains("server startup is not complete"));

    assert!(state.open_startup_gate());
    let open = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        tcp_get(addr, "/api/v1/sources"),
    )
    .await
    .expect("the open gate must serve the next request promptly");
    assert!(
        open.starts_with("HTTP/1.1 200 "),
        "open gate response: {open}"
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn test_health_check() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "healthy");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn test_readiness_not_running() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/ready")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Pipeline is in Created state, not Running
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_metrics() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.contains("text/plain"),
        "expected text/plain content-type, got: {ct}"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("laminardb_events_ingested_total"),
        "missing events_ingested_total"
    );
    assert!(
        text.contains("laminardb_cycles_total"),
        "missing cycles_total"
    );
    assert!(
        text.contains("laminardb_checkpoints_completed_total"),
        "missing checkpoints_completed_total"
    );
    // Prometheus text format includes HELP and TYPE annotations.
    assert!(text.contains("# HELP"), "missing # HELP annotation");
    assert!(text.contains("# TYPE"), "missing # TYPE annotation");
}

#[tokio::test]
async fn test_list_sources_empty() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/sources")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_get_stream_not_found() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/streams/nonexistent")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_execute_sql_create_source() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&serde_json::json!({
                "sql": "CREATE SOURCE test_src (id BIGINT, name VARCHAR)"
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["result_type"], "CREATE SOURCE");
}

#[tokio::test]
async fn test_execute_sql_metadata_returns_rows() {
    let state = test_state();
    let app = build_router(state);

    // Create a source so SHOW SOURCES has a row to return.
    let create = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&serde_json::json!({
                "sql": "CREATE SOURCE meta_src (id BIGINT, name VARCHAR)"
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // SHOW SOURCES yields an ExecuteResult::Metadata batch — the handler
    // must serialize it into the `data` field.
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&serde_json::json!({ "sql": "SHOW SOURCES" })).unwrap(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["result_type"], "metadata");
    let data = json["data"]
        .as_array()
        .expect("data should be a JSON array");
    assert_eq!(data.len(), 1, "expected the one created source");
    assert_eq!(data[0]["source_name"], "meta_src");
}

#[tokio::test]
async fn test_execute_sql_invalid() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&serde_json::json!({
                "sql": "NOT VALID SQL AT ALL BLAH"
            }))
            .unwrap(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_reload_invalid_config_path() {
    // test_state has config_path = "test.toml" which doesn't exist → 400
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/reload")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_reload_concurrent_returns_conflict() {
    let state = test_state();
    // Hold the guard before making the request
    let _guard = state.reload_guard.try_acquire().unwrap();

    let app = build_router(state);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/reload")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_reload_with_valid_config() {
    use std::io::Write;

    // Create a real temp config file
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    writeln!(tmpfile, "[server]").unwrap();
    let path = tmpfile.path().to_path_buf();

    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let db = LaminarDB::open().unwrap();
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    let server_metrics = crate::metrics::ServerMetrics::new(&registry);
    let state = Arc::new(AppState {
        db,
        config_path: path,
        current_config: parking_lot::RwLock::new(crate::config::ServerConfig {
            server: crate::config::ServerSection::default(),
            state: laminar_core::state::StateBackendConfig::default(),
            checkpoint: crate::config::CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            node_id: None,
            sql: None,
            ai: Default::default(),
            models: Default::default(),
        }),
        reload_guard: ReloadGuard::new(),

        registry,
        server_metrics,
        ws_slots: ws_connection_slots(),
        serving_gate: ready_serving_gate(),
        #[cfg(feature = "cluster")]
        cluster: None,
    });

    let app = build_router(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/reload")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], true);
}

/// POST a SQL statement to `/api/v1/sql`, asserting it succeeds.
async fn exec_sql(app: &Router, sql: &str) {
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&serde_json::json!({ "sql": sql })).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "exec failed: {sql}");
}

#[tokio::test]
async fn test_list_mvs_empty() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/mvs")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_mvs_after_create() {
    let state = test_state();
    let app = build_router(state);

    exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;
    // Registers the MV in the registry (see ddl.rs); query execution is not
    // required for it to be listed.
    exec_sql(
        &app,
        "CREATE MATERIALIZED VIEW event_stats AS SELECT * FROM events",
    )
    .await;

    let req = Request::builder()
        .uri("/api/v1/mvs")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let mvs = json.as_array().expect("mvs should be an array");
    let found = mvs
        .iter()
        .find(|m| m["name"] == "event_stats")
        .expect("event_stats should be listed");
    assert_eq!(found["state"], "Running");
    assert!(
        found["sql"].as_str().unwrap().contains("event_stats"),
        "sql should be the full CREATE statement: {found:?}"
    );
}

#[tokio::test]
async fn test_list_connectors() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/connectors")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Shape is `{sources: [...], sinks: [...]}`; the exact connectors depend
    // on enabled features, so only assert the structure here.
    assert!(json["sources"].is_array(), "sources should be an array");
    assert!(json["sinks"].is_array(), "sinks should be an array");
}

#[test]
fn test_ws_terminal_frames_expose_error_and_gap_details() {
    let error: serde_json::Value = serde_json::from_str(&ws_error_json(
        "orders",
        "subscription_failed",
        "bad filter",
        7,
    ))
    .unwrap();
    assert_eq!(error["type"], "error");
    assert_eq!(error["subscription_id"], "orders");
    assert_eq!(error["code"], "subscription_failed");
    assert_eq!(error["message"], "bad filter");
    assert_eq!(error["sequence"], "7");

    let gap: serde_json::Value = serde_json::from_str(&ws_gap_json("orders", 12, 8)).unwrap();
    assert_eq!(gap["type"], "gap");
    assert_eq!(gap["code"], "subscription_lagged");
    assert_eq!(gap["skipped_messages"], "12");
    assert_eq!(gap["sequence"], "8");

    let progress: serde_json::Value =
        serde_json::from_str(&ws_progress_json("orders", 9, 42, 8, 6, 10)).unwrap();
    assert_eq!(progress["type"], "progress");
    assert_eq!(progress["epoch"], "9");
    assert_eq!(progress["checkpoint_id"], "42");
    assert_eq!(progress["log_sequence"], "8");
    assert_eq!(progress["through_log_sequence"], "6");
    assert_eq!(progress["sequence"], "10");
}

#[test]
fn ws_terminal_frames_bound_untrusted_text() {
    let text = "\u{10ffff}".repeat(MAX_WS_CONTROL_FIELD_BYTES + 1);
    let frame = ws_error_json(&text, &text, &text, 1);
    assert!(frame.len() <= MAX_WS_FRAME_BYTES);
    let parsed: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert!(parsed["message"].as_str().unwrap().len() <= MAX_WS_CONTROL_FIELD_BYTES);
}

#[test]
fn ws_data_frames_split_before_the_wire_limit() {
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let value = "x".repeat(MAX_WS_FRAME_BYTES / 2);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![
                value.as_str(),
                value.as_str(),
                value.as_str(),
            ])),
        ],
    )
    .unwrap();

    let mut state = WsBatchFrameState::default();
    let mut sequence = 0;
    let mut ids = Vec::new();
    while state.offset < batch.num_rows() {
        let expected_offset = state.offset;
        let frame = next_ws_data_frame("large", &batch, &mut state, sequence, u64::MAX)
            .unwrap()
            .unwrap();
        let consumed = state.offset - expected_offset;
        assert!(frame.len() <= MAX_WS_FRAME_BYTES);
        let json: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(json["sequence"], sequence.to_string());
        assert_eq!(json["log_sequence"], u64::MAX.to_string());
        assert_eq!(json["row_offset"], expected_offset.to_string());
        assert_eq!(json["row_count"], consumed.to_string());
        assert_eq!(json["data"].as_array().unwrap().len(), consumed);
        ids.extend(
            json["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["id"].as_i64().unwrap()),
        );
        sequence += 1;
    }
    assert!(sequence > 1, "oversized batches must be split");
    assert_eq!(state.offset, batch.num_rows());
    assert_eq!(ids, vec![0, 1, 2], "rows must not be duplicated or skipped");
    assert!(state.pending_row.is_none());
    assert_eq!(
        next_ws_data_frame("large", &batch, &mut state, sequence, 99).unwrap(),
        None
    );
}

#[test]
fn ws_data_frame_rejects_a_single_oversized_row() {
    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let value = "x".repeat(MAX_WS_FRAME_BYTES);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![value.as_str()]))],
    )
    .unwrap();
    let mut state = WsBatchFrameState::default();
    assert_eq!(
        next_ws_data_frame("large", &batch, &mut state, 0, 0),
        Err(WsFrameBuildError::TooLarge)
    );
    assert_eq!(state.offset, 0);
}

#[test]
fn http_and_ws_json_preserve_exact_nested_values_and_nulls() {
    use arrow_array::builder::{Float64Builder, ListBuilder};
    use arrow_array::types::Int8Type;
    use arrow_array::{
        Array, Decimal128Array, DictionaryArray, Float32Array, Int64Array, Int8Array, RecordBatch,
        StructArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};

    let nested_decimal = Decimal128Array::from(vec![Some(12_345_i128), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let nested_fields = Fields::from(vec![
        Arc::new(Field::new("large", DataType::Int64, true)),
        Arc::new(Field::new(
            "amount",
            nested_decimal.data_type().clone(),
            true,
        )),
    ]);
    let nested = StructArray::try_new(
        nested_fields,
        vec![
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
            Arc::new(nested_decimal),
        ],
        None,
    )
    .unwrap();

    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), None]),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
    )
    .unwrap();

    let mut floats = ListBuilder::new(Float64Builder::new());
    floats.values().append_value(1.25);
    floats.values().append_value(f64::NAN);
    floats.values().append_value(f64::INFINITY);
    floats.values().append_value(f64::NEG_INFINITY);
    floats.values().append_null();
    floats.append(true);
    floats.append_null();
    let floats = floats.finish();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("unsigned", DataType::UInt64, false),
            Field::new("nested", nested.data_type().clone(), false),
            Field::new("dictionary", dictionary.data_type().clone(), true),
            Field::new("floats", floats.data_type().clone(), true),
            Field::new("float32", DataType::Float32, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
            Arc::new(nested),
            Arc::new(dictionary),
            Arc::new(floats),
            Arc::new(Float32Array::from(vec![1.234_567_f32, f32::INFINITY])),
        ],
    )
    .unwrap();

    let rows: serde_json::Value =
        serde_json::from_str(&batches_to_json_string(std::slice::from_ref(&batch)).unwrap())
            .unwrap();
    let first = &rows[0];
    assert_eq!(first["unsigned"], u64::MAX.to_string());
    assert_eq!(first["nested"]["large"], i64::MIN.to_string());
    assert_eq!(first["nested"]["amount"], "123.45");
    assert_eq!(first["dictionary"], u64::MAX.to_string());
    assert!(first["floats"][0].is_number());
    assert_eq!(first["floats"][0].as_f64(), Some(1.25));
    assert_eq!(first["floats"][1], "NaN");
    assert_eq!(first["floats"][2], "Infinity");
    assert_eq!(first["floats"][3], "-Infinity");
    assert!(first["floats"][4].is_null());
    assert!(first["float32"].is_number());

    let second = &rows[1];
    assert!(second["nested"]["large"].is_null());
    assert!(second["nested"]["amount"].is_null());
    assert!(second["dictionary"].is_null());
    assert!(second["floats"].is_null());
    assert_eq!(second["float32"], "Infinity");

    let mut state = WsBatchFrameState::default();
    let frame = next_ws_data_frame("exact", &batch, &mut state, 0, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(state.offset, 2);
    let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(frame["data"], rows);
    assert_eq!(frame["log_sequence"], u64::MAX.to_string());
}

#[test]
fn ws_slot_admission_is_atomic() {
    const CAPACITY: usize = 4;
    const CONTENDERS: usize = 32;

    let slots = Arc::new(tokio::sync::Semaphore::new(CAPACITY));
    let start = Arc::new(std::sync::Barrier::new(CONTENDERS + 1));
    let release = Arc::new(std::sync::Barrier::new(CONTENDERS + 1));
    let (tx, rx) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    for _ in 0..CONTENDERS {
        let slots = Arc::clone(&slots);
        let start = Arc::clone(&start);
        let release = Arc::clone(&release);
        let tx = tx.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            let permit = try_acquire_ws_slot(&slots);
            tx.send(permit.is_some()).unwrap();
            release.wait();
            drop(permit);
        }));
    }
    drop(tx);
    start.wait();
    let admitted = (0..CONTENDERS)
        .map(|_| rx.recv().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted, CAPACITY);
    release.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(slots.available_permits(), CAPACITY);
}

#[test]
fn ws_liveness_expires_without_pongs_and_recovers_on_pong() {
    let mut deadline = WsPongDeadline::default();
    assert!(deadline.before_ping());
    assert!(deadline.before_ping());
    assert!(!deadline.before_ping());
    deadline.on_pong();
    assert!(deadline.before_ping());
}

/// Bind a real ephemeral-port server so the WebSocket upgrade runs over a
/// genuine hyper connection (the `tower::oneshot` harness can't upgrade —
/// the request has no `OnUpgrade` extension, so axum rejects with 426).
async fn spawn_test_server(state: Arc<AppState>) -> std::net::SocketAddr {
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

/// Send a raw WebSocket upgrade request for `path` and return the first
/// chunk of the HTTP response (enough to read the status line).
async fn ws_handshake(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         \r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn test_ws_upgrade_switching_protocols() {
    let state = test_state();
    state
        .db
        .execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    state
        .db
        .execute("CREATE STREAM visible AS SELECT * FROM events")
        .await
        .unwrap();
    state.db.start().await.unwrap();

    let addr = spawn_test_server(state).await;
    let resp = ws_handshake(addr, "/ws/visible").await;
    assert!(
        resp.starts_with("HTTP/1.1 101"),
        "expected 101 Switching Protocols, got: {resp}"
    );
}

#[tokio::test]
async fn test_ws_upgrade_unknown_stream_returns_404() {
    let state = test_state();
    let addr = spawn_test_server(state).await;
    let resp = ws_handshake(addr, "/ws/does_not_exist").await;
    assert!(
        resp.starts_with("HTTP/1.1 404"),
        "expected 404 Not Found for unknown stream, got: {resp}"
    );
}

#[tokio::test]
async fn ws_emits_committed_checkpoint_progress() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(laminar_db::LaminarConfig {
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            data_dir: Some(checkpoint_dir.path().to_path_buf()),
            ..Default::default()
        }),
        ..Default::default()
    })
    .unwrap();
    let state = test_state_with_db(db);
    state
        .db
        .execute("CREATE SOURCE events (id BIGINT)")
        .await
        .unwrap();
    state
        .db
        .execute("CREATE MATERIALIZED VIEW visible AS SELECT id FROM events")
        .await
        .unwrap();
    state.db.start().await.unwrap();
    let addr = spawn_test_server(Arc::clone(&state)).await;

    let (attached_tx, attached_rx) = tokio::sync::oneshot::channel();
    let (data_tx, data_rx) = tokio::sync::oneshot::channel();
    let reader = tokio::task::spawn_blocking(move || {
        let (mut socket, _) =
            tungstenite::connect(format!("ws://{addr}/ws/visible")).expect("WS connect");
        let _ = attached_tx.send(());
        let mut data_tx = Some(data_tx);
        let mut frames = Vec::new();
        loop {
            match socket.read().expect("WS frame") {
                tungstenite::Message::Text(text) => {
                    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                    frames.push(json.clone());
                    if json["type"] == "data" {
                        if let Some(data_tx) = data_tx.take() {
                            let _ = data_tx.send(());
                        }
                    }
                    if json["type"] == "progress" {
                        return frames;
                    }
                }
                tungstenite::Message::Ping(data) => {
                    socket.send(tungstenite::Message::Pong(data)).expect("pong");
                }
                tungstenite::Message::Close(_) => panic!("WS closed before progress"),
                _ => {}
            }
        }
    });
    attached_rx.await.expect("reader attached");
    let source = state.db.source_untyped("events").unwrap();
    source
        .push_arrow(
            arrow_array::RecordBatch::try_new(
                source.schema().clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
            )
            .unwrap(),
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), data_rx)
        .await
        .expect("the input must reach the WebSocket before checkpointing")
        .expect("WebSocket reader must remain attached");
    let committed = state.db.checkpoint().await.expect("checkpoint");
    assert!(committed.success);
    let frames = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
        .await
        .expect("progress frame arrives")
        .expect("reader task");
    assert_eq!(frames.len(), 2, "data must precede its progress cut");
    assert_eq!(frames[0]["type"], "data");
    assert_eq!(frames[0]["sequence"], "0");
    assert_eq!(frames[0]["log_sequence"], "0");
    let progress = &frames[1];
    assert_eq!(progress["epoch"], committed.epoch.to_string());
    assert_eq!(
        progress["checkpoint_id"],
        committed.checkpoint_id.to_string()
    );
    assert_eq!(progress["log_sequence"], "1");
    assert_eq!(progress["through_log_sequence"], "1");
    assert_eq!(progress["sequence"], "1");
}

#[tokio::test]
async fn test_get_graph_returns_nodes_and_edges() {
    let state = test_state();
    let app = build_router(state);

    exec_sql(&app, "CREATE SOURCE events (id INT, value DOUBLE)").await;
    exec_sql(&app, "CREATE STREAM s1 AS SELECT * FROM events").await;

    let req = Request::builder()
        .uri("/api/v1/graph")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let nodes = json["nodes"].as_array().expect("nodes should be an array");
    let edges = json["edges"].as_array().expect("edges should be an array");

    let source = nodes
        .iter()
        .find(|n| n["name"] == "events")
        .expect("events source node should be present");
    assert_eq!(source["node_type"], "Source");

    let stream = nodes
        .iter()
        .find(|n| n["name"] == "s1")
        .expect("s1 stream node should be present");
    assert_eq!(stream["node_type"], "Stream");
    assert!(
        stream["sql"].as_str().unwrap().contains("events"),
        "stream node should carry its defining SQL: {stream:?}"
    );

    assert!(
        edges
            .iter()
            .any(|e| e["from"] == "events" && e["to"] == "s1"),
        "expected an edge events -> s1, got: {edges:?}"
    );
}

#[tokio::test]
async fn test_get_graph_empty() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/graph")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["nodes"].as_array().unwrap().is_empty());
    assert!(json["edges"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_cluster_nodes_404_when_not_cluster() {
    // test_state() leaves `cluster` as None, so the cluster endpoints 404
    // even when compiled with the `cluster` feature.
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/cluster/nodes")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_cluster_vnodes_404_when_not_cluster() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/cluster/vnodes")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn test_cluster_vnodes_fails_when_durable_snapshot_is_missing() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};

    let node = laminar_core::cluster::discovery::NodeId(51);
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let assignment_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        node,
        control,
        Some(Arc::clone(&snapshot_store)),
        members_rx.clone(),
    ));
    let mut state = test_state();
    Arc::get_mut(&mut state).unwrap().cluster = Some(ClusterComponents {
        controller,
        snapshot_store,
        membership_rx: members_rx,
    });
    let app = build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/vnodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("durable assignment snapshot is missing"));
}

#[tokio::test]
async fn test_cluster_leader_404_when_not_cluster() {
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/cluster/leader")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_cluster_checkpoints_returns_metadata() {
    // Available in both single-node and cluster mode. With no checkpoint
    // taken yet it still returns a single metadata row of zeros.
    let state = test_state();
    let app = build_router(state);

    let req = Request::builder()
        .uri("/api/v1/cluster/checkpoints")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json
        .as_array()
        .expect("checkpoint status should be an array");
    assert_eq!(rows.len(), 1, "expected one checkpoint-status row");
    let row = &rows[0];
    assert!(
        row.get("checkpoint_id").is_some(),
        "row should carry checkpoint_id: {row:?}"
    );
    assert!(
        row.get("total_checkpoints").is_some(),
        "row should carry total_checkpoints: {row:?}"
    );
}
