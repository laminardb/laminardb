use super::*;
use axum::body::Body;
use axum::http::Request;
#[cfg(feature = "cluster")]
use base64::Engine as _;
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

#[cfg(feature = "cluster")]
#[test]
fn diagnostic_read_gate_is_single_flight_and_rate_bounded() {
    let gate = DiagnosticReadGate::new();
    let permit = gate.permit.try_acquire().unwrap();
    assert!(gate.permit.try_acquire().is_err());
    drop(permit);
    assert!(gate.permit.try_acquire().is_ok());

    let now = Instant::now();
    let mut rate = DiagnosticRateWindow::default();
    for _ in 0..DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW {
        assert!(rate.try_start(now));
    }
    assert!(!rate.try_start(now));
    assert!(rate.try_start(now + DIAGNOSTIC_READ_RATE_WINDOW));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_read_deadline_releases_the_single_flight_permit() {
    let (state, diagnostic) = diagnostic_middleware_state();
    let app = Router::new()
        .route(
            "/api/v1/cluster/local-evidence",
            get(|| async {
                tokio::time::sleep(DIAGNOSTIC_READ_DEADLINE + std::time::Duration::from_secs(1))
                    .await;
                StatusCode::OK
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_bounds_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_auth_middleware,
        ));
    let request = Request::builder()
        .uri("/api/v1/cluster/local-evidence")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {diagnostic}"),
        )
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cancelling_a_diagnostic_read_releases_the_single_flight_permit() {
    let (state, diagnostic) = diagnostic_middleware_state();
    let started = Arc::new(tokio::sync::Notify::new());
    let blocked = Arc::new(tokio::sync::Notify::new());
    let handler_started = started.notified();
    let app = Router::new()
        .route(
            "/api/v1/cluster/local-evidence",
            get({
                let started = Arc::clone(&started);
                let blocked = Arc::clone(&blocked);
                move || {
                    let started = Arc::clone(&started);
                    let blocked = Arc::clone(&blocked);
                    async move {
                        started.notify_one();
                        blocked.notified().await;
                        StatusCode::OK
                    }
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_bounds_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_auth_middleware,
        ));
    let request = Request::builder()
        .uri("/api/v1/cluster/local-evidence")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {diagnostic}"),
        )
        .body(Body::empty())
        .unwrap();

    let task = tokio::spawn(app.oneshot(request));
    handler_started.await;
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 0);
    task.abort();
    let _ = task.await;
    tokio::task::yield_now().await;
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_auth_precedes_accounting_and_both_routes_share_one_permit() {
    let (state, diagnostic) = diagnostic_middleware_state();
    let first_started = Arc::new(tokio::sync::Notify::new());
    let first_blocked = Arc::new(tokio::sync::Notify::new());
    let second_entries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/api/v1/cluster/local-evidence",
            get({
                let first_started = Arc::clone(&first_started);
                let first_blocked = Arc::clone(&first_blocked);
                move || {
                    let first_started = Arc::clone(&first_started);
                    let first_blocked = Arc::clone(&first_blocked);
                    async move {
                        first_started.notify_one();
                        first_blocked.notified().await;
                        StatusCode::OK
                    }
                }
            }),
        )
        .route(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
            get({
                let second_entries = Arc::clone(&second_entries);
                move || {
                    let second_entries = Arc::clone(&second_entries);
                    async move {
                        second_entries.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        StatusCode::OK
                    }
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_bounds_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_auth_middleware,
        ));

    let authorized = |path: &'static str| {
        Request::builder()
            .uri(path)
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {diagnostic}"),
            )
            .body(Body::empty())
            .unwrap()
    };
    let first = tokio::spawn(
        app.clone()
            .oneshot(authorized("/api/v1/cluster/local-evidence")),
    );
    first_started.notified().await;
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 0);

    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-checkpoint-barrier-timings")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let contended = app
        .clone()
        .oneshot(authorized(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
        ))
        .await
        .unwrap();
    assert_eq!(contended.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(second_entries.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        state
            .diagnostic_reads
            .rate
            .lock()
            .starts
            .iter()
            .flatten()
            .count(),
        1
    );

    first.abort();
    let _ = first.await;
    tokio::task::yield_now().await;
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 1);

    let success = app
        .oneshot(authorized(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
        ))
        .await
        .unwrap();
    assert_eq!(success.status(), StatusCode::OK);
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 1);
    assert_eq!(second_entries.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_rolling_window_admits_eight_starts_then_rejects_without_handler_entry() {
    let (state, diagnostic) = diagnostic_middleware_state();
    let entries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler = {
        let entries = Arc::clone(&entries);
        move || {
            let entries = Arc::clone(&entries);
            async move {
                entries.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                StatusCode::OK
            }
        }
    };
    let app = Router::new()
        .route("/api/v1/cluster/local-evidence", get(handler.clone()))
        .route(
            "/api/v1/cluster/local-checkpoint-barrier-timings",
            get(handler),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_bounds_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            diagnostic_auth_middleware,
        ));
    let request = |path: &'static str, token: &str| {
        Request::builder()
            .uri(path)
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };

    for index in 0..DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW {
        let path = if index % 2 == 0 {
            "/api/v1/cluster/local-evidence"
        } else {
            "/api/v1/cluster/local-checkpoint-barrier-timings"
        };
        let response = app
            .clone()
            .oneshot(request(path, &diagnostic))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "start {index}");
    }

    let excess = app
        .clone()
        .oneshot(request("/api/v1/cluster/local-evidence", &diagnostic))
        .await
        .unwrap();
    assert_eq!(excess.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        entries.load(std::sync::atomic::Ordering::SeqCst),
        DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW
    );
    assert_eq!(state.diagnostic_reads.permit.available_permits(), 1);

    let unauthorized = app
        .oneshot(request("/api/v1/cluster/local-evidence", "wrong-token"))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        entries.load(std::sync::atomic::Ordering::SeqCst),
        DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW
    );
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
    let server = crate::config::ServerSection::default();
    let auth_policy = HttpAuthPolicy::from_server(&server);
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
        auth_policy,
        #[cfg(feature = "cluster")]
        diagnostic_reads: DiagnosticReadGate::new(),
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

fn test_state_with_config_path(
    config_path: PathBuf,
    current_config: crate::config::ServerConfig,
) -> Arc<AppState> {
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let db = LaminarDB::open().unwrap();
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    let server_metrics = crate::metrics::ServerMetrics::new(&registry);
    let auth_policy = HttpAuthPolicy::from_server(&current_config.server);
    Arc::new(AppState {
        db,
        config_path,
        current_config: parking_lot::RwLock::new(current_config),
        reload_guard: ReloadGuard::new(),
        registry,
        server_metrics,
        auth_policy,
        #[cfg(feature = "cluster")]
        diagnostic_reads: DiagnosticReadGate::new(),
        ws_slots: ws_connection_slots(),
        serving_gate: ready_serving_gate(),
        #[cfg(feature = "cluster")]
        cluster: None,
    })
}

fn test_state_with_auth_and_gate(
    console_token: Option<&str>,
    diagnostic_read_token: Option<&str>,
    serving_gate: Arc<ServingGate>,
) -> Arc<AppState> {
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    let db = LaminarDB::open().unwrap();
    db.set_engine_metrics(engine_metrics);
    let server_metrics = crate::metrics::ServerMetrics::new(&registry);
    let server = crate::config::ServerSection {
        console_token: console_token.map(crate::config::Secret::new),
        diagnostic_read_token: diagnostic_read_token.map(crate::config::Secret::new),
        ..Default::default()
    };
    let auth_policy = HttpAuthPolicy::from_server(&server);
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
        auth_policy,
        #[cfg(feature = "cluster")]
        diagnostic_reads: DiagnosticReadGate::new(),
        ws_slots: ws_connection_slots(),
        serving_gate,
        #[cfg(feature = "cluster")]
        cluster: None,
    })
}

/// Like [`test_state`] but with a console bearer token configured, so the
/// auth middleware is active on protected routes.
fn test_state_with_token_and_gate(token: &str, serving_gate: Arc<ServingGate>) -> Arc<AppState> {
    test_state_with_auth_and_gate(Some(token), None, serving_gate)
}

fn test_state_with_token(token: &str) -> Arc<AppState> {
    test_state_with_token_and_gate(token, ready_serving_gate())
}

#[cfg(feature = "cluster")]
fn canonical_auth_token(byte: u8) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([byte; 32])
}

#[cfg(feature = "cluster")]
fn diagnostic_middleware_state() -> (Arc<AppState>, String) {
    let console = canonical_auth_token(249);
    let diagnostic = canonical_auth_token(250);
    let mut state =
        test_state_with_auth_and_gate(Some(&console), Some(&diagnostic), ready_serving_gate());
    let auth_policy = {
        let mut current = state.current_config.write();
        current.server.mode = crate::config::ServerMode::Cluster;
        HttpAuthPolicy::from_server(&current.server)
    };
    Arc::get_mut(&mut state).unwrap().auth_policy = auth_policy;
    (state, diagnostic)
}

#[cfg(feature = "cluster")]
struct LocalEvidenceFixture {
    state: Arc<AppState>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
    control: Arc<laminar_core::cluster::control::InMemoryKv>,
    adoption: laminar_core::cluster::control::CheckpointAssignmentAdoption,
    process_term: u64,
}

#[cfg(feature = "cluster")]
async fn local_evidence_fixture_with_auth(
    console_token: Option<&str>,
    diagnostic_read_token: Option<&str>,
    serving_gate: Arc<ServingGate>,
    publish_adoption: bool,
) -> LocalEvidenceFixture {
    use laminar_core::cluster::control::{
        CheckpointAssignmentAdoption, CheckpointAssignmentFence, ClusterController, ClusterKv,
        InMemoryKv, LeaseDeadline, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };

    let node = laminar_core::cluster::discovery::NodeId(61);
    let boot = uuid::Uuid::from_u128(610);
    let control = Arc::new(InMemoryKv::new(node));
    let control_kv: Arc<dyn ClusterKv> = control.clone();
    let assignment_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let snapshot_store =
        Arc::new(laminar_core::cluster::control::AssignmentSnapshotStore::new(assignment_store));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&control_kv),
        control_kv,
        Some(Arc::clone(&snapshot_store)),
        members_rx.clone(),
        boot,
    ));

    let process_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(process_store, std::time::Duration::from_secs(60)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(node)
        .try_acquire(boot, 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must be acquired");
    };
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();

    let owners = [node.0, node.0, node.0];
    let adoption = CheckpointAssignmentAdoption {
        participant: laminar_core::checkpoint::CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        },
        assignment_version: 7,
        partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
        vnode_count: u32::try_from(owners.len()).unwrap(),
        assignment_digest: CheckpointAssignmentFence::owner_map_digest(3, &owners),
    };
    if publish_adoption {
        controller
            .announce_adopted_assignment(&adoption)
            .await
            .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(
            CheckpointAssignmentFence::from_owner_map(
                adoption.assignment_version,
                &owners,
                vec![adoption.participant],
            )
            .unwrap(),
        ));
    }

    let mut state =
        test_state_with_auth_and_gate(console_token, diagnostic_read_token, serving_gate);
    let auth_policy = {
        let mut current = state.current_config.write();
        current.server.mode = crate::config::ServerMode::Cluster;
        HttpAuthPolicy::from_server(&current.server)
    };
    let mutable_state = Arc::get_mut(&mut state).unwrap();
    mutable_state.auth_policy = auth_policy;
    mutable_state.cluster = Some(ClusterComponents {
        controller: Arc::clone(&controller),
        snapshot_store,
        membership_rx: members_rx,
    });

    LocalEvidenceFixture {
        state,
        controller,
        control,
        adoption,
        process_term: process_lease.term,
    }
}

#[cfg(feature = "cluster")]
async fn local_evidence_fixture(
    token: Option<&str>,
    serving_gate: Arc<ServingGate>,
    publish_adoption: bool,
) -> LocalEvidenceFixture {
    local_evidence_fixture_with_auth(token, None, serving_gate, publish_adoption).await
}

#[cfg(feature = "cluster")]
async fn local_checkpoint_barrier_timings_fixture_with_auth(
    console_token: Option<&str>,
    diagnostic_read_token: Option<&str>,
    serving_gate: Arc<ServingGate>,
) -> LocalEvidenceFixture {
    use laminar_core::cluster::control::{
        prove_shared_object_store_namespaces, ClusterKv, InMemoryKv,
    };

    let mut fixture =
        local_evidence_fixture_with_auth(console_token, diagnostic_read_token, serving_gate, false)
            .await;
    let participant = fixture.adoption.participant;
    let proof_control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(
        laminar_core::cluster::discovery::NodeId(participant.node_id),
    ));
    let checkpoint_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let state_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let namespaces = prove_shared_object_store_namespaces(
        participant,
        &[participant],
        proof_control,
        checkpoint_store,
        state_store,
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let vnode_count = u32::from(laminar_core::state::DEFAULT_CLUSTER_KEY_GROUP_COUNT);
    let state_backend: Arc<dyn laminar_core::state::StateBackend> =
        Arc::new(laminar_core::state::ObjectStoreBackend::cluster_shared(
            namespaces.state_store(),
            "http-timing-test",
            vnode_count,
        ));
    let db = laminar_db::LaminarDbBuilder::new()
        .profile(laminar_db::Profile::Cluster)
        .cluster_controller(Arc::clone(&fixture.controller))
        .verified_cluster_namespaces(namespaces)
        .state_backend(state_backend)
        .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::new(
            vnode_count,
        )))
        .build()
        .await
        .unwrap();
    Arc::get_mut(&mut fixture.state)
        .expect("the fixture must retain the only app-state reference")
        .db = db;
    fixture
}

#[cfg(feature = "cluster")]
async fn local_checkpoint_barrier_timings_fixture(
    token: Option<&str>,
    serving_gate: Arc<ServingGate>,
) -> LocalEvidenceFixture {
    local_checkpoint_barrier_timings_fixture_with_auth(token, None, serving_gate).await
}

#[cfg(feature = "cluster")]
fn local_checkpoint_barrier_timings_uri(
    fixture: &LocalEvidenceFixture,
    after_sequence: u64,
) -> String {
    format!(
        "/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence={after_sequence}&expected_node_id={}&expected_boot_incarnation={}&expected_process_term={}",
        fixture.adoption.participant.node_id,
        fixture.adoption.participant.boot_incarnation,
        fixture.process_term,
    )
}

#[cfg(feature = "cluster")]
fn local_checkpoint_barrier_timings_request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(
            axum::http::header::AUTHORIZATION,
            "Bearer supersecret-token",
        )
        .body(Body::empty())
        .unwrap()
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
        let expected_cors = (path == "/api/v1/sources").then_some("https://console.example");
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            expected_cors,
            "only a matched console route may receive CORS headers while startup is closed: {path}"
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
async fn explicit_reload_retains_pure_restart_only_configuration_and_auth_policy() {
    use std::io::Write;

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "[server]\nbind = \"127.0.0.1:9191\"\nconsole_token = \"replacement-console-token\""
    )
    .unwrap();

    let mut current: crate::config::ServerConfig = toml::from_str("[server]\n").unwrap();
    current.server.console_token = Some(crate::config::Secret::new("original-console-token"));
    let original_server = current.server.clone();
    let state = test_state_with_config_path(tmpfile.path().to_path_buf(), current);

    let app = build_router(state.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/reload")
        .header(
            axum::http::header::AUTHORIZATION,
            "Bearer original-console-token",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["success"], true);
    assert!(json["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning.as_str().unwrap().contains("[server]")));
    assert_eq!(state.current_config.read().server, original_server);

    let replacement = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/sources")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer replacement-console-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::UNAUTHORIZED);
    let original = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/sources")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer original-console-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(original.status(), StatusCode::OK);
}

#[tokio::test]
async fn explicit_reload_commits_live_sections_but_retains_mixed_restart_only_changes() {
    use std::io::Write;

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "[server]\nbind = \"127.0.0.1:9292\"\nconsole_token = \"replacement-console-token\""
    )
    .unwrap();

    let mut current: crate::config::ServerConfig = toml::from_str("[server]\n").unwrap();
    current.server.console_token = Some(crate::config::Secret::new("original-console-token"));
    current.sources.push(crate::config::SourceConfig {
        name: "removed_source".to_string(),
        connector: "kafka".to_string(),
        format: "json".to_string(),
        properties: toml::Table::new(),
        schema: vec![],
        watermark: None,
    });
    let original_server = current.server.clone();
    let state = test_state_with_config_path(tmpfile.path().to_path_buf(), current);
    let response = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reload")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer original-console-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let current = state.current_config.read();
    assert!(current.sources.is_empty());
    assert_eq!(current.server, original_server);
}

#[tokio::test]
async fn explicit_reload_failure_commits_neither_live_nor_restart_only_configuration() {
    use std::io::Write;

    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "[server]\nbind = \"127.0.0.1:9393\"\nconsole_token = \"replacement-console-token\"\n\n[[pipeline]]\nname = \"bad_reload\"\nsql = \"NOT VALID SQL AT ALL\""
    )
    .unwrap();

    let mut current: crate::config::ServerConfig = toml::from_str("[server]\n").unwrap();
    current.server.console_token = Some(crate::config::Secret::new("original-console-token"));
    let original = current.clone();
    let state = test_state_with_config_path(tmpfile.path().to_path_buf(), current);
    let response = build_router(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reload")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer original-console-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::MULTI_STATUS);
    assert_eq!(*state.current_config.read(), original);
}

#[tokio::test]
async fn explicit_reload_parse_error_body_does_not_disclose_substituted_secret() {
    use std::io::Write;

    const SENTINEL: &str = "LDB_RELOAD_SECRET_SENTINEL_0d927841";
    let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        tmpfile,
        "[server]\nconsole_token = ${{LDB_RELOAD_REDACTION_TOKEN:-{SENTINEL}}}"
    )
    .unwrap();
    let current: crate::config::ServerConfig = toml::from_str("[server]\n").unwrap();
    let state = test_state_with_config_path(tmpfile.path().to_path_buf(), current);
    let response = build_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/reload")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&body).contains(SENTINEL));
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
async fn test_cluster_local_evidence_404_when_not_cluster() {
    for state in [test_state(), test_state_with_token("supersecret-token")] {
        let response = build_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cluster/local-evidence")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer supersecret-token",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_requires_a_configured_token() {
    let fixture = local_evidence_fixture(None, ready_serving_gate(), true).await;
    let response = build_router(fixture.state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG })
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_requires_the_current_bearer() {
    let fixture =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), true).await;
    let app = build_router(fixture.state);

    for authorization in [None, Some("Bearer wrong-token")] {
        let mut request = Request::builder().uri("/api/v1/cluster/local-evidence");
        if let Some(authorization) = authorization {
            request = request.header(axum::http::header::AUTHORIZATION, authorization);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let query_token = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence?token=supersecret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query_token.status(), StatusCode::UNAUTHORIZED);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_and_console_bearers_can_read_local_evidence() {
    let console = canonical_auth_token(1);
    let diagnostic = canonical_auth_token(2);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        true,
    )
    .await;
    let app = build_router(fixture.state);

    for token in [&console, &diagnostic] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cluster/local-evidence")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(axum::http::header::ORIGIN, "https://observer.invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_auth_policy_is_immutable_after_startup() {
    let console = canonical_auth_token(11);
    let diagnostic = canonical_auth_token(12);
    let replacement_console = canonical_auth_token(13);
    let replacement_diagnostic = canonical_auth_token(14);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        true,
    )
    .await;
    {
        let mut current = fixture.state.current_config.write();
        current.server.console_token = Some(crate::config::Secret::new(&replacement_console));
        current.server.diagnostic_read_token =
            Some(crate::config::Secret::new(&replacement_diagnostic));
        current.server.mode = crate::config::ServerMode::Single;
    }
    let app = build_router(fixture.state);

    let replacement = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {replacement_diagnostic}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::UNAUTHORIZED);

    let original = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {diagnostic}"),
                )
                .header(axum::http::header::ORIGIN, "https://observer.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(original.status(), StatusCode::OK);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_bearer_is_rejected_before_every_console_handler() {
    let console = canonical_auth_token(3);
    let diagnostic = canonical_auth_token(4);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        true,
    )
    .await;
    let state = Arc::clone(&fixture.state);
    let before_pipeline = state.db.pipeline_state();
    let before_reloads = state.server_metrics.reload_total.get();
    let app = build_router(fixture.state);

    for (method, path) in [
        ("GET", "/api/v1/sources"),
        ("GET", "/api/v1/sinks"),
        ("GET", "/api/v1/streams"),
        ("GET", "/api/v1/streams/example"),
        ("GET", "/api/v1/mvs"),
        ("GET", "/api/v1/connectors"),
        ("GET", "/api/v1/graph"),
        ("GET", "/api/v1/cluster"),
        ("GET", "/api/v1/cluster/nodes"),
        ("GET", "/api/v1/cluster/vnodes"),
        ("GET", "/api/v1/cluster/leader"),
        ("GET", "/api/v1/cluster/checkpoints"),
        ("GET", "/api/v1/pipeline/status"),
        ("GET", "/ws/events"),
        ("POST", "/api/v1/checkpoint"),
        ("POST", "/api/v1/sql"),
        ("POST", "/api/v1/reload"),
        ("POST", "/api/v1/pipeline/stop"),
        ("POST", "/api/v1/pipeline/start"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {diagnostic}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
    }

    let websocket_query = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/ws/events?token={diagnostic}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(websocket_query.status(), StatusCode::UNAUTHORIZED);

    for path in ["/health", "/ready", "/metrics"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{path}?token={diagnostic}"))
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {diagnostic}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains(&diagnostic), "{path}");
        assert!(!body.contains("adopted_assignment"), "{path}");
    }

    assert_eq!(state.db.pipeline_state(), before_pipeline);
    assert_eq!(state.server_metrics.reload_total.get(), before_reloads);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_routes_reject_method_substitution_and_cors() {
    let console = canonical_auth_token(5);
    let diagnostic = canonical_auth_token(6);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        true,
    )
    .await;
    let app = build_router(fixture.state);

    for path in [
        "/api/v1/cluster/local-evidence",
        "/api/v1/cluster/local-checkpoint-barrier-timings",
    ] {
        for method in [
            "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(
                            axum::http::header::AUTHORIZATION,
                            format!("Bearer {diagnostic}"),
                        )
                        .header(axum::http::header::ORIGIN, "https://observer.invalid")
                        .header(axum::http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path}"
            );
            assert!(
                response
                    .headers()
                    .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                    .is_none(),
                "diagnostic route unexpectedly received CORS for {method} {path}"
            );
        }
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_routes_reject_credential_and_request_target_aliases() {
    let console = canonical_auth_token(7);
    let diagnostic = canonical_auth_token(8);
    let fixture = local_evidence_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
        true,
    )
    .await;
    let app = build_router(fixture.state);
    let path = "/api/v1/cluster/local-evidence";

    for request in [
        Request::builder().uri(path).body(Body::empty()).unwrap(),
        Request::builder()
            .uri(format!("{path}?token={diagnostic}"))
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri(path)
            .header(axum::http::header::COOKIE, format!("token={diagnostic}"))
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri(path)
            .header(axum::http::header::AUTHORIZATION, "Bearer short")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri(path)
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {diagnostic},Bearer {diagnostic}"),
            )
            .body(Body::empty())
            .unwrap(),
    ] {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let oversized = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {}", "A".repeat(4_096)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::UNAUTHORIZED);

    let mut duplicate = Request::builder()
        .uri(path)
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {diagnostic}"),
        )
        .body(Body::empty())
        .unwrap();
    duplicate.headers_mut().append(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {diagnostic}").parse().unwrap(),
    );
    let response = app.clone().oneshot(duplicate).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let query = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("{path}?unexpected=true"))
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {diagnostic}"),
                )
                .header(axum::http::header::ORIGIN, "https://observer.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::BAD_REQUEST);

    let absolute = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("http://observer.invalid/api/v1/cluster/local-evidence")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {diagnostic}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(absolute.status(), StatusCode::BAD_REQUEST);

    for alias in [
        "/api/v1/cluster/local-evidence/",
        "/api/v1/cluster//local-evidence",
        "/api/v1/cluster/LOCAL-EVIDENCE",
        "/api/v1/cluster/local%2Fevidence",
        "/api/v1/cluster/local%5Cevidence",
        "/api/v1/cluster/%2e/local-evidence",
        "/api/v1/cluster/not-local-evidence",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(alias)
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {diagnostic}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{alias}");
    }

    let timings_path = "/api/v1/cluster/local-checkpoint-barrier-timings";
    let absolute_timings = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("http://observer.invalid{timings_path}"))
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {diagnostic}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(absolute_timings.status(), StatusCode::BAD_REQUEST);

    for alias in [
        "/api/v1/cluster/local-checkpoint-barrier-timings/",
        "/api/v1/cluster//local-checkpoint-barrier-timings",
        "/api/v1/cluster/LOCAL-CHECKPOINT-BARRIER-TIMINGS",
        "/api/v1/cluster/local-checkpoint%2Fbarrier-timings",
        "/api/v1/cluster/local-checkpoint%5Cbarrier-timings",
        "/api/v1/cluster/%2e/local-checkpoint-barrier-timings",
        "/api/v1/cluster/not-local-checkpoint-barrier-timings",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(alias)
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {diagnostic}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{alias}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_returns_exact_bounded_no_store_envelope() {
    let fixture =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), true).await;
    let expected_adoption = fixture.adoption.clone();
    let expected_process_term = fixture.process_term;
    let response = build_router(fixture.state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer supersecret-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    let body = axum::body::to_bytes(response.into_body(), MAX_LOCAL_EVIDENCE_RESPONSE_BYTES + 1)
        .await
        .unwrap();
    assert!(body.len() <= MAX_LOCAL_EVIDENCE_RESPONSE_BYTES);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let envelope_fields = envelope.as_object().unwrap();
    assert_eq!(envelope_fields.len(), 2);
    assert_eq!(
        envelope["schema_version"],
        serde_json::Value::String(LOCAL_EVIDENCE_SCHEMA_VERSION.into())
    );
    let evidence = envelope["evidence"].as_object().unwrap();
    assert_eq!(evidence.len(), 3);
    assert_eq!(evidence["participant"]["node_id"], 61);
    assert_eq!(
        evidence["participant"]["boot_incarnation"],
        uuid::Uuid::from_u128(610).to_string()
    );
    assert_eq!(evidence["process_term"], expected_process_term);
    assert_eq!(
        serde_json::from_value::<laminar_core::cluster::control::CheckpointAssignmentAdoption>(
            evidence["adopted_assignment"].clone()
        )
        .unwrap(),
        expected_adoption
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_requires_the_live_audited_assignment_fence() {
    let fixture =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), true).await;

    // SnapshotWatcher clears this projection when it suspends local checkpoint authority. The
    // durable current-boot adoption remains retained, but it must no longer authorize HTTP 200.
    fixture.controller.publish_checkpoint_assignment_fence(None);
    let response = build_router(fixture.state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-evidence")
                .header(
                    axum::http::header::AUTHORIZATION,
                    "Bearer supersecret-token",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": LOCAL_EVIDENCE_UNAVAILABLE_MSG })
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_fails_closed_for_missing_stale_or_malformed_logical_adoption() {
    let missing =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), false).await;
    let request = || {
        Request::builder()
            .uri("/api/v1/cluster/local-evidence")
            .header(
                axum::http::header::AUTHORIZATION,
                "Bearer supersecret-token",
            )
            .body(Body::empty())
            .unwrap()
    };
    let missing_response = build_router(missing.state)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(missing_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let stale =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), false).await;
    stale.controller.publish_checkpoint_assignment_fence(Some(
        laminar_core::cluster::control::CheckpointAssignmentFence::from_owner_map(
            stale.adoption.assignment_version,
            &[61, 61, 61],
            vec![stale.adoption.participant],
        )
        .unwrap(),
    ));
    let mut stale_adoption = stale.adoption;
    stale_adoption.participant.boot_incarnation = uuid::Uuid::from_u128(611);
    stale.control.seed(
        laminar_core::cluster::discovery::NodeId(61),
        "control:adopted-assignment",
        serde_json::to_string(&stale_adoption).unwrap(),
    );
    let stale_response = build_router(stale.state).oneshot(request()).await.unwrap();
    assert_eq!(stale_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let malformed_logical =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), false).await;
    // The KV read succeeds and returns malformed logical payload bytes. Object-store/control
    // envelope failures remain storage uncertainty and are mapped to 503 by the core API.
    malformed_logical.control.seed(
        laminar_core::cluster::discovery::NodeId(61),
        "control:adopted-assignment",
        "not-json".into(),
    );
    let malformed_response = build_router(malformed_logical.state)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(
        malformed_response.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let body = axum::body::to_bytes(malformed_response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": LOCAL_EVIDENCE_INVALID_MSG })
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_evidence_preserves_startup_recovery_and_terminal_gates() {
    let startup = local_evidence_fixture(
        Some("supersecret-token"),
        Arc::new(ServingGate::starting()),
        true,
    )
    .await;
    let request = || {
        Request::builder()
            .uri("/api/v1/cluster/local-evidence")
            .header(
                axum::http::header::AUTHORIZATION,
                "Bearer supersecret-token",
            )
            .body(Body::empty())
            .unwrap()
    };
    let startup_response = build_router(startup.state)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(startup_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let recovering =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), true).await;
    recovering.controller.set_recovering(true);
    let recovery_response = build_router(recovering.state)
        .oneshot(request())
        .await
        .unwrap();
    assert_eq!(recovery_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let fenced =
        local_evidence_fixture(Some("supersecret-token"), ready_serving_gate(), true).await;
    fenced.controller.fence_process_lease();
    let fenced_response = build_router(fenced.state).oneshot(request()).await.unwrap();
    assert_eq!(fenced_response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_cluster_local_checkpoint_barrier_timings_404_when_not_cluster() {
    for state in [test_state(), test_state_with_token("supersecret-token")] {
        let response = build_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cluster/local-checkpoint-barrier-timings")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer supersecret-token",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_checkpoint_barrier_timings_requires_a_configured_bearer() {
    let unconfigured = local_checkpoint_barrier_timings_fixture(None, ready_serving_gate()).await;
    let uri = local_checkpoint_barrier_timings_uri(&unconfigured, 0);
    let response = build_router(unconfigured.state)
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let configured =
        local_checkpoint_barrier_timings_fixture(Some("supersecret-token"), ready_serving_gate())
            .await;
    let uri = local_checkpoint_barrier_timings_uri(&configured, 0);
    let app = build_router(configured.state);
    for authorization in [None, Some("Bearer wrong-token")] {
        let mut request = Request::builder().uri(&uri);
        if let Some(authorization) = authorization {
            request = request.header(axum::http::header::AUTHORIZATION, authorization);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let query_token = app
        .oneshot(
            Request::builder()
                .uri(format!("{uri}&token=supersecret-token"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query_token.status(), StatusCode::UNAUTHORIZED);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn diagnostic_bearer_can_read_local_checkpoint_barrier_timings() {
    let console = canonical_auth_token(9);
    let diagnostic = canonical_auth_token(10);
    let fixture = local_checkpoint_barrier_timings_fixture_with_auth(
        Some(&console),
        Some(&diagnostic),
        ready_serving_gate(),
    )
    .await;
    let response = build_router(fixture.state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence=0")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {diagnostic}"),
                )
                .header(axum::http::header::ORIGIN, "https://observer.invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_checkpoint_barrier_timings_rejects_closed_or_noncanonical_queries() {
    let fixture =
        local_checkpoint_barrier_timings_fixture(Some("supersecret-token"), ready_serving_gate())
            .await;
    let valid = local_checkpoint_barrier_timings_uri(&fixture, 0);
    let app = build_router(fixture.state);
    for uri in [
        "/api/v1/cluster/local-checkpoint-barrier-timings".to_string(),
        "/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence=1".to_string(),
        "/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence=0&expected_node_id=61"
            .to_string(),
        format!("{valid}&unexpected=true"),
        format!("{valid}&after_sequence=0"),
        format!("{valid}&expected_node_id=61"),
        valid.replace("expected_node_id=61", "expected_node_id=0"),
        valid.replace(&uuid::Uuid::from_u128(610).to_string(), "not-a-uuid"),
    ] {
        let response = app
            .clone()
            .oneshot(local_checkpoint_barrier_timings_request(&uri))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_checkpoint_barrier_timings_returns_exact_empty_no_store_envelope() {
    let fixture =
        local_checkpoint_barrier_timings_fixture(Some("supersecret-token"), ready_serving_gate())
            .await;
    let uri = "/api/v1/cluster/local-checkpoint-barrier-timings?after_sequence=0";
    let response = build_router(fixture.state)
        .oneshot(local_checkpoint_barrier_timings_request(uri))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let body = axum::body::to_bytes(
        response.into_body(),
        MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES + 1,
    )
    .await
    .unwrap();
    assert!(body.len() <= MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.as_object().unwrap().len(), 4);
    assert_eq!(
        envelope["schema_version"],
        LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA_VERSION
    );
    assert_eq!(envelope["after_sequence"], 0);
    assert_eq!(envelope["process_identity"]["participant"]["node_id"], 61);
    assert_eq!(
        envelope["process_identity"]["participant"]["boot_incarnation"],
        uuid::Uuid::from_u128(610).to_string()
    );
    assert_eq!(
        envelope["process_identity"]["process_term"],
        fixture.process_term
    );
    assert_eq!(
        envelope["page"],
        serde_json::json!({
            "capacity": laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY,
            "oldest_retained_sequence": null,
            "next_sequence": 1,
            "overwritten_record_count": 0,
            "recording_loss_count": 0,
            "metadata_exhausted": false,
            "has_more": false,
            "records": [],
        })
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_checkpoint_barrier_timings_rejects_stale_or_ahead_cursors() {
    let fixture =
        local_checkpoint_barrier_timings_fixture(Some("supersecret-token"), ready_serving_gate())
            .await;
    let current = local_checkpoint_barrier_timings_uri(&fixture, 0);
    let stale = current.replace(
        &format!("expected_process_term={}", fixture.process_term),
        &format!("expected_process_term={}", fixture.process_term + 1),
    );
    let ahead = local_checkpoint_barrier_timings_uri(&fixture, 1);
    let app = build_router(fixture.state);

    for uri in [stale, ahead] {
        let response = app
            .clone()
            .oneshot(local_checkpoint_barrier_timings_request(&uri))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT, "{uri}");
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_local_checkpoint_barrier_timings_preserves_all_serving_gates() {
    let fixture = local_checkpoint_barrier_timings_fixture(
        Some("supersecret-token"),
        Arc::new(ServingGate::starting()),
    )
    .await;
    let uri = local_checkpoint_barrier_timings_uri(&fixture, 0);
    let state = Arc::clone(&fixture.state);
    let app = build_router(Arc::clone(&state));

    let startup = app
        .clone()
        .oneshot(local_checkpoint_barrier_timings_request(&uri))
        .await
        .unwrap();
    assert_eq!(startup.status(), StatusCode::SERVICE_UNAVAILABLE);

    assert!(state.open_startup_gate());
    fixture.controller.set_recovering(true);
    let recovering = app
        .clone()
        .oneshot(local_checkpoint_barrier_timings_request(&uri))
        .await
        .unwrap();
    assert_eq!(recovering.status(), StatusCode::SERVICE_UNAVAILABLE);

    fixture.controller.set_recovering(false);
    fixture.controller.fence_process_lease();
    let fenced = app
        .oneshot(local_checkpoint_barrier_timings_request(&uri))
        .await
        .unwrap();
    assert_eq!(fenced.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[cfg(feature = "cluster")]
#[test]
fn local_checkpoint_barrier_timing_errors_have_closed_status_mapping() {
    use laminar_db::checkpoint_timing::{
        CheckpointBarrierTimingReadError as ReadError,
        CheckpointBarrierTimingSnapshotError as SnapshotError,
    };

    let process = laminar_core::cluster::control::LocalProcessAuthorityIdentity {
        participant: laminar_core::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        },
        process_term: 1,
    };
    let mut other = process;
    other.process_term = 2;
    let cases = [
        (
            ReadError::ProcessIdentityMismatch {
                expected: process,
                actual: other,
            },
            StatusCode::CONFLICT,
        ),
        (
            ReadError::Snapshot(SnapshotError::CursorAhead {
                after_sequence: 2,
                next_sequence: 2,
            }),
            StatusCode::CONFLICT,
        ),
        (
            ReadError::Snapshot(SnapshotError::CursorOverwritten {
                after_sequence: 1,
                oldest_retained_sequence: 3,
            }),
            StatusCode::GONE,
        ),
        (
            ReadError::Snapshot(SnapshotError::Busy),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            ReadError::ProcessIdentityChanged {
                before: process,
                after: other,
            },
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            ReadError::Snapshot(SnapshotError::InvalidLimit { limit: 0 }),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            ReadError::LedgerProcessMismatch {
                expected: process,
                actual: other,
            },
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(
            local_checkpoint_barrier_timing_error_response(&error).0,
            expected
        );
    }
}

#[cfg(feature = "cluster")]
#[test]
fn maximum_checkpoint_barrier_timing_envelope_fits_the_response_cap() {
    use laminar_db::checkpoint_timing::{
        CheckpointBarrierRole, CheckpointBarrierTimingPage, CheckpointBarrierTimingRecord,
        CheckpointBarrierTimingSnapshot, MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS,
    };

    let process = laminar_core::cluster::control::LocalProcessAuthorityIdentity {
        participant: laminar_core::checkpoint::CheckpointParticipant {
            node_id: u64::MAX,
            boot_incarnation: uuid::Uuid::from_u128(u128::MAX),
        },
        process_term: u64::MAX,
    };
    let records = (1..=MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS)
        .map(|sequence| CheckpointBarrierTimingRecord {
            sequence: u64::try_from(sequence).unwrap(),
            process,
            attempt: laminar_core::state::CheckpointAttempt::canonical(u64::MAX),
            role: CheckpointBarrierRole::Follower,
            assignment_version: u64::MAX,
            assignment_digest: [u8::MAX; 32],
            pipeline_stall_ns: u64::MAX,
            local_barrier_ns: u64::MAX / 2,
            aligned_resume_ns: Some(u64::MAX / 2),
            durable_tail_handoff: true,
            deadline_exhausted: true,
        })
        .collect();
    let timing_page = CheckpointBarrierTimingPage {
        process,
        snapshot: CheckpointBarrierTimingSnapshot {
            process: Some(process),
            capacity: laminar_db::checkpoint_timing::CHECKPOINT_BARRIER_TIMING_CAPACITY,
            oldest_retained_sequence: Some(u64::MAX),
            next_sequence: u64::MAX,
            overwritten_record_count: u64::MAX,
            recording_loss_count: u64::MAX,
            metadata_exhausted: true,
            has_more: true,
            records,
        },
    };
    let public_page = serde_json::to_value(&timing_page).unwrap();
    assert_eq!(public_page["process"]["process_term"], u64::MAX);
    assert_eq!(public_page["snapshot"]["process"]["process_term"], u64::MAX);
    let envelope = LocalCheckpointBarrierTimingsResponse::new(u64::MAX, &timing_page);
    let encoded = serde_json::to_vec(&envelope).unwrap();

    assert!(
        encoded.len() <= MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES,
        "maximum legal response is {} bytes",
        encoded.len()
    );
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
