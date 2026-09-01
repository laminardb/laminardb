use super::*;

use crate::config::*;

#[test]
fn checkpoint_config_rejects_relative_file_urls() {
    let result = apply_local_checkpoint_config(
        LaminarDB::builder(),
        "file://./relative",
        &CheckpointSection::default(),
    );
    let Err(error) = result else {
        panic!("relative checkpoint URL was admitted");
    };
    assert!(error.to_string().contains("absolute local path"), "{error}");
}

#[test]
fn checkpoint_state_budget_has_one_default_and_honours_an_override() {
    let mut checkpoint = CheckpointSection::default();
    assert_eq!(
        resolved_checkpoint_node_data_bytes(&checkpoint).unwrap(),
        laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES
    );

    checkpoint.max_node_data_bytes = Some(8 * 1024 * 1024);
    assert_eq!(
        resolved_checkpoint_node_data_bytes(&checkpoint).unwrap(),
        8 * 1024 * 1024
    );
}

#[test]
fn checkpoint_state_budget_rejects_zero_and_unaddressable_limits() {
    let mut checkpoint = CheckpointSection {
        max_node_data_bytes: Some(0),
        ..CheckpointSection::default()
    };
    assert!(resolved_checkpoint_node_data_bytes(&checkpoint).is_err());

    checkpoint.max_node_data_bytes = Some((isize::MAX as u64) + 1);
    let error = resolved_checkpoint_node_data_bytes(&checkpoint).unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeds this process address space"));
}

#[tokio::test]
async fn server_entry_rejects_invalid_budget_before_runtime_mode_routing() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.checkpoint.max_node_data_bytes = Some(0);

        let result = run_server(config, PathBuf::from("unused.toml")).await;
        let Err(error) = result else {
            panic!("invalid checkpoint state budget was admitted in {mode:?} mode");
        };
        assert!(
            error.to_string().contains("checkpoint.max_node_data_bytes"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn server_entry_rejects_invalid_temporal_retention_in_both_modes() {
    for mode in [ServerMode::Single, ServerMode::Cluster] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.mode = mode;
        config.server.temporal_join_idle_history_retention =
            Some(std::time::Duration::from_nanos(999_999));

        let result = run_server(config, PathBuf::from("unused.toml")).await;
        let Err(error) = result else {
            panic!("invalid temporal retention was admitted in {mode:?} mode");
        };
        assert!(
            error
                .to_string()
                .contains("temporal_join_idle_history_retention must be at least 1ms"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn server_entry_rejects_programmatic_diagnostic_auth_before_other_startup_work() {
    let mut config: ServerConfig = toml::from_str("").unwrap();
    config.server.diagnostic_read_token = Some(Secret::new("invalid"));
    // This second invalid value makes the test terminate safely even if authentication
    // validation is accidentally moved later; authentication must still win.
    config.checkpoint.max_node_data_bytes = Some(0);

    let result = run_server(config, PathBuf::from("unused.toml")).await;
    let Err(error) = result else {
        panic!("invalid programmatic diagnostic authentication was admitted");
    };
    let message = error.to_string();
    assert!(message.contains("HTTP authentication"), "{message}");
    assert!(message.contains("diagnostic_read_token"), "{message}");
    assert!(
        !message.contains("checkpoint.max_node_data_bytes"),
        "{message}"
    );
}

#[tokio::test]
async fn cancelling_http_start_does_not_detach_the_listener() {
    let server = ServerSection {
        bind: "127.0.0.1:0".into(),
        ..ServerSection::default()
    };
    let config = ServerConfig {
        server,
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), "test".into()),
        ("pipeline".into(), "test".into()),
    ]));
    let prepared = prepare_http_api(
        LaminarDB::open().unwrap(),
        registry,
        PathBuf::from("unused.toml"),
        config,
        Arc::new(http::ServingGate::starting()),
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .unwrap();
    let address = prepared.listener.local_addr().unwrap();

    {
        let start = prepared.start();
        tokio::pin!(start);
        assert!(futures::poll!(start.as_mut()).is_pending());
    }

    let rebound = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let Ok(listener) = tokio::net::TcpListener::bind(address).await {
                return listener;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling HTTP startup must release its listener");
    drop(rebound);
}

#[tokio::test]
async fn aborted_server_task_is_joined_before_cleanup_returns() {
    let mut task = tokio::spawn(std::future::pending::<()>());
    let observer = task.abort_handle();

    assert!(abort_and_join_server_task(&mut task, "test task").await);

    assert!(observer.is_finished());
}

#[tokio::test]
async fn dropping_single_server_handle_fences_and_aborts_owned_tasks() {
    let serving_gate = Arc::new(http::ServingGate::starting());
    assert!(serving_gate.open());
    let api_handle = tokio::spawn(std::future::pending::<()>());
    let api_abort = api_handle.abort_handle();
    let pgwire_handle = tokio::spawn(std::future::pending::<()>());
    let pgwire_abort = pgwire_handle.abort_handle();
    let watcher_handle = tokio::spawn(std::future::pending::<()>());
    let watcher_abort = watcher_handle.abort_handle();
    let db = LaminarDB::open().unwrap();
    let handle = ServerHandle {
        runtime: ServerRuntime::Single(SingleServerRuntime {
            db: Arc::clone(&db),
            db_shutdown_complete: false,
            serving_gate: Arc::clone(&serving_gate),
            api_handle,
            pgwire_handle: Some(pgwire_handle),
            watcher_handle: Some(watcher_handle),
        }),
    };

    drop(handle);

    assert_eq!(
        serving_gate.rejection_message(),
        Some("server serving authority is fenced")
    );
    assert!(db.is_closed());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !(api_abort.is_finished()
            && pgwire_abort.is_finished()
            && watcher_abort.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped server handle left an owned task running");
    db.shutdown().await.unwrap();
}

fn make_source(name: &str, connector: &str) -> SourceConfig {
    SourceConfig {
        name: name.to_string(),
        connector: connector.to_string(),
        format: "json".to_string(),
        properties: toml::Table::new(),
        schema: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
            },
        ],
        primary_key: vec![],
        watermark: None,
    }
}

#[cfg(feature = "cluster")]
async fn catalog_test_db(
    object_store: Arc<dyn object_store::ObjectStore>,
) -> (
    Arc<LaminarDB>,
    Arc<laminar_core::cluster::control::CatalogManifestStore>,
) {
    use laminar_core::cluster::control::{
        CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeId;

    let node = NodeId(1);
    let boot = uuid::Uuid::from_u128(101);
    let owner = LeaderLeaseOwner {
        node,
        boot,
        process_term: 1,
    };
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&object_store), 1_000));
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!()
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        Arc::clone(&kv),
        Arc::clone(&kv),
        None,
        members_rx,
        boot,
    ));
    controller.set_active(false);
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )))
        .unwrap();
    let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    let manifest_store = Arc::new(CatalogManifestStore::new(authority));
    let participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: boot,
    };
    let verified_namespaces = laminar_core::cluster::control::prove_shared_object_store_namespaces(
        participant,
        &[participant],
        kv,
        Arc::clone(&object_store),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::new(1));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .verified_cluster_namespaces(verified_namespaces)
        .vnode_registry(vnode_registry)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .build()
        .await
        .unwrap();
    (db, manifest_store)
}

#[test]
fn test_source_to_ddl_basic() {
    let mut source = make_source("events", "kafka");
    source.primary_key = vec!["id".to_string()];
    let ddl = source_to_ddl(&source);
    assert!(ddl.starts_with("CREATE SOURCE events"));
    assert!(ddl.contains("id BIGINT NOT NULL"));
    assert!(ddl.contains("name VARCHAR"));
    assert!(ddl.contains("PRIMARY KEY (id)"));
    assert!(ddl.contains("FROM KAFKA FORMAT JSON"));
    assert!(!ddl.contains("format ="));
}

/// Columnless OTel source + WATERMARK FOR must compose: the OTel
/// connector implements `discover_schema` so the DDL layer can
/// resolve columns before validating the watermark.
#[cfg(feature = "otel")]
#[tokio::test]
async fn execute_config_ddl_columnless_otel_with_watermark_succeeds() {
    let mut source = SourceConfig {
        name: "otel_events".to_string(),
        connector: "otel".to_string(),
        format: "json".to_string(),
        properties: toml::Table::new(),
        schema: vec![],
        primary_key: vec![],
        watermark: Some(WatermarkConfig {
            column: "_laminar_received_at".to_string(),
            max_out_of_orderness: std::time::Duration::from_secs(10),
        }),
    };
    // Bind to an ephemeral port so the test doesn't clash with 4317.
    source
        .properties
        .insert("port".to_string(), toml::Value::String("0".to_string()));
    source.properties.insert(
        "signals".to_string(),
        toml::Value::String("logs".to_string()),
    );

    let db = laminar_db::LaminarDB::open().unwrap();
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    execute_config_ddl(&db, &config, false)
        .await
        .expect("columnless OTel + WATERMARK FOR should compose");
}

/// Columnless Kafka source + WATERMARK FOR: the Kafka connector can't
/// discover a schema without `bootstrap.servers` configured, so the DDL
/// layer surfaces a "schema auto-discovery failed: …" error (or, when
/// the connector returns no schema, "could not auto-discover a schema").
/// The server no longer pre-empts this — we just check the error bubbles
/// up clearly. Requires the kafka connector to be registered.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn execute_config_ddl_columnless_kafka_surfaces_discovery_error() {
    let mut source = make_source("events", "kafka");
    source.schema.clear();
    source.watermark = Some(WatermarkConfig {
        column: "ts".to_string(),
        max_out_of_orderness: std::time::Duration::from_secs(5),
    });

    let db = laminar_db::LaminarDB::open().unwrap();
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let err = execute_config_ddl(&db, &config, false).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("schema auto-discovery failed")
            || msg.contains("could not auto-discover a schema")
            || msg.contains("no columns declared"),
        "expected schema-discovery error from the DDL layer, got: {msg}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_config_rejects_expanded_connector_secret_before_manifest_write() {
    use object_store::ObjectStore;

    let mut source = make_source("secured", "generator");
    source.properties.insert(
        "password".to_string(),
        toml::Value::String("expanded-password-must-not-persist".to_string()),
    );
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![source],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (db, manifest_store) = catalog_test_db(object_store).await;

    let error = execute_config_ddl(&db, &config, true).await.unwrap_err();
    assert!(error.to_string().contains("cannot persist secret property"));
    assert!(manifest_store.load().await.unwrap().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn empty_cluster_config_still_seals_an_empty_inventory() {
    use object_store::ObjectStore;

    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        sql: None,
        discovery: None,
        node_id: None,
        ai: Default::default(),
        models: Default::default(),
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (db, manifest_store) = catalog_test_db(object_store).await;

    execute_config_ddl(&db, &config, true).await.unwrap();
    assert_eq!(
        manifest_store.load().await.unwrap().unwrap().entries,
        Vec::new()
    );
}

#[test]
fn test_source_to_ddl_with_watermark() {
    let mut source = make_source("events", "kafka");
    source.watermark = Some(WatermarkConfig {
        column: "ts".to_string(),
        max_out_of_orderness: std::time::Duration::from_secs(5),
    });
    let ddl = source_to_ddl(&source);
    assert!(ddl.contains("WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"));
}

#[test]
fn connector_identifiers_preserve_provider_punctuation() {
    let hyphenated = source_to_ddl(&make_source("events", "postgres-cdc"));
    assert!(hyphenated.contains("FROM \"postgres-cdc\""));

    let underscored = source_to_ddl(&make_source("events", "vendor_v2"));
    assert!(underscored.contains("FROM VENDOR_V2"));
}

#[test]
fn test_source_to_ddl_with_properties() {
    let mut source = make_source("events", "kafka");
    source.properties.insert(
        "bootstrap.servers".to_string(),
        toml::Value::String("localhost:9092".to_string()),
    );
    source.properties.insert(
        "topic".to_string(),
        toml::Value::String("events".to_string()),
    );
    source.properties.insert(
        "client-id".to_string(),
        toml::Value::String("source-client".to_string()),
    );
    source.properties.insert(
        "vendor\"option".to_string(),
        toml::Value::String("quoted-key".to_string()),
    );
    let ddl = source_to_ddl(&source);
    assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
    assert!(ddl.contains("\"topic\" = 'events'"));
    assert!(ddl.contains("\"client-id\" = 'source-client'"));
    assert!(ddl.contains("\"vendor\"\"option\" = 'quoted-key'"));
    assert!(ddl.ends_with(") FORMAT JSON"));

    let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
    let laminar_sql::parser::StreamingStatement::CreateSource(parsed) = &statements[0] else {
        panic!("expected CREATE SOURCE")
    };
    assert_eq!(
        parsed
            .connector_options
            .get("client-id")
            .map(String::as_str),
        Some("source-client")
    );
    assert_eq!(
        parsed
            .connector_options
            .get("vendor\"option")
            .map(String::as_str),
        Some("quoted-key")
    );
}

#[test]
fn test_pipeline_to_ddl() {
    let pipeline = PipelineConfig {
        name: "vwap".to_string(),
        sql: "SELECT symbol, SUM(price) FROM trades GROUP BY symbol".to_string(),
    };
    let ddl = pipeline_to_ddl(&pipeline);
    assert_eq!(
        ddl,
        "CREATE STREAM vwap AS SELECT symbol, SUM(price) FROM trades GROUP BY symbol"
    );
}

#[test]
fn test_sink_to_ddl() {
    let mut props = toml::Table::new();
    props.insert(
        "topic".to_string(),
        toml::Value::String("output".to_string()),
    );
    props.insert(
        "bootstrap.servers".to_string(),
        toml::Value::String("localhost:9092".to_string()),
    );
    props.insert(
        "oauthbearer-token".to_string(),
        toml::Value::String("token".to_string()),
    );
    let sink = SinkConfig {
        name: "output_sink".to_string(),
        pipeline: "vwap".to_string(),
        connector: "kafka".to_string(),
        format: Some("json".to_string()),
        properties: props,
    };
    let ddl = sink_to_ddl(&sink);
    assert!(ddl.starts_with("CREATE SINK output_sink FROM vwap INTO KAFKA"));
    assert!(ddl.contains("\"topic\" = 'output'"));
    assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
    assert!(ddl.contains("\"oauthbearer-token\" = 'token'"));
    assert!(ddl.ends_with(") FORMAT JSON"));
    assert!(!ddl.contains("format ="));
    // Delivery is injected from the pipeline-wide engine contract at connector build time.
    assert!(!ddl.contains("delivery"));

    let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
    let laminar_sql::parser::StreamingStatement::CreateSink(parsed) = &statements[0] else {
        panic!("expected CREATE SINK")
    };
    assert_eq!(
        parsed
            .connector_options
            .get("oauthbearer-token")
            .map(String::as_str),
        Some("token")
    );
}

#[test]
fn test_sink_to_ddl_has_no_per_sink_delivery_dimension() {
    let sink = SinkConfig {
        name: "out".to_string(),
        pipeline: "p".to_string(),
        connector: "kafka".to_string(),
        format: None,
        properties: toml::Table::new(),
    };
    let ddl = sink_to_ddl(&sink);
    assert!(!ddl.contains("delivery"));
}

#[test]
fn test_lookup_to_ddl() {
    let lookup = LookupConfig {
        name: "instruments".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: {
            let mut t = toml::Table::new();
            t.insert(
                "connection".to_string(),
                toml::Value::String("postgresql://localhost/db".to_string()),
            );
            t
        },
        primary_key: vec!["symbol".to_string()],
        schema: vec![ColumnDef {
            name: "symbol".to_string(),
            data_type: "VARCHAR".to_string(),
            nullable: false,
        }],
    };
    let ddl = lookup_to_ddl(&lookup).unwrap();
    assert!(ddl.starts_with("CREATE LOOKUP TABLE instruments"));
    assert!(ddl.contains("symbol VARCHAR NOT NULL"));
    assert!(ddl.contains("PRIMARY KEY (symbol)"));
    assert!(ddl.contains("'connector' = 'postgres'"));
    assert!(ddl.contains("'strategy' = 'poll'"));
    assert!(ddl.contains("'connection' = 'postgresql://localhost/db'"));
}

#[test]
fn test_lookup_to_ddl_no_primary_key() {
    let lookup = LookupConfig {
        name: "t".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: toml::Table::new(),
        primary_key: vec![],
        schema: vec![ColumnDef {
            name: "id".to_string(),
            data_type: "INT".to_string(),
            nullable: false,
        }],
    };
    let ddl = lookup_to_ddl(&lookup).unwrap();
    assert!(!ddl.contains("PRIMARY KEY"));
}

#[test]
fn test_lookup_to_ddl_empty_schema_rejected() {
    let lookup = LookupConfig {
        name: "bad".to_string(),
        connector: "postgres".to_string(),
        strategy: "poll".to_string(),
        cache: LookupCacheConfig::default(),
        properties: toml::Table::new(),
        primary_key: vec![],
        schema: vec![],
    };
    assert!(lookup_to_ddl(&lookup).is_err());
}

#[test]
fn test_toml_value_to_sql() {
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("hello".to_string())),
        "hello"
    );
    assert_eq!(toml_value_to_sql(&toml::Value::Integer(42)), "42");
    assert_eq!(toml_value_to_sql(&toml::Value::Boolean(true)), "true");
    assert_eq!(toml_value_to_sql(&toml::Value::Float(3.25)), "3.25");
}

#[test]
fn test_toml_value_to_sql_escapes_single_quotes() {
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("it's a test".to_string())),
        "it''s a test"
    );
    assert_eq!(
        toml_value_to_sql(&toml::Value::String("a''b".to_string())),
        "a''''b"
    );
}
