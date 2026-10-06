use super::*;

fn canonical_source_digest(
    input_mode: SourceInputMode,
    row_positions: SourceRowPositionCapability,
) -> [u8; 32] {
    let payload = CanonicalPipeline {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        state_abi_version: STATE_ABI_VERSION,
        partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
        state_layout: STATE_LAYOUT,
        vnode_count: 1,
        delivery_guarantee: "at-least-once".into(),
        source_idle_timeout_ms: None,
        event_time_max_future_skew_ms: laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        sources: vec![CanonicalSource {
            catalog_generation: 1,
            name: "events".into(),
            connector_type: "test".into(),
            options: BTreeMap::new(),
            input_mode: canonical_source_input_mode(input_mode),
            row_positions: canonical_source_row_positions(row_positions),
            replay_order: None,
            schema: None,
            primary_key: Vec::new(),
            watermark_column: None,
            max_out_of_orderness_ms: None,
            processing_time: false,
        }],
        streams: Vec::new(),
        process_functions: Vec::new(),
        tables: Vec::new(),
        sinks: Vec::new(),
    };
    Sha256::digest(serde_json::to_vec(&payload).unwrap()).into()
}

#[test]
fn canonical_identity_digest_changes_with_root_execution_config() {
    let payload = |state_abi_version,
                   partitioning_abi_version,
                   source_idle_timeout_ms,
                   event_time_max_future_skew_ms| CanonicalPipeline {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        state_abi_version,
        partitioning_abi_version,
        state_layout: STATE_LAYOUT,
        vnode_count: 1,
        delivery_guarantee: "best_effort".into(),
        source_idle_timeout_ms,
        event_time_max_future_skew_ms,
        sources: Vec::new(),
        streams: Vec::new(),
        process_functions: Vec::new(),
        tables: Vec::new(),
        sinks: Vec::new(),
    };

    let current = Sha256::digest(
        serde_json::to_vec(&payload(
            STATE_ABI_VERSION,
            laminar_core::state::PARTITIONING_ABI_VERSION,
            None,
            laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        ))
        .unwrap(),
    );
    assert_eq!(STATE_ABI_VERSION, 6);
    let prior_state_abi = Sha256::digest(
        serde_json::to_vec(&payload(
            STATE_ABI_VERSION - 1,
            laminar_core::state::PARTITIONING_ABI_VERSION,
            None,
            laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        ))
        .unwrap(),
    );
    assert_ne!(current, prior_state_abi);
    let changed = Sha256::digest(
        serde_json::to_vec(&payload(
            STATE_ABI_VERSION,
            laminar_core::state::PARTITIONING_ABI_VERSION + 1,
            None,
            laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        ))
        .unwrap(),
    );
    assert_ne!(current, changed);
    let idle_timeout = Sha256::digest(
        serde_json::to_vec(&payload(
            STATE_ABI_VERSION,
            laminar_core::state::PARTITIONING_ABI_VERSION,
            Some(5_000),
            laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        ))
        .unwrap(),
    );
    assert_ne!(current, idle_timeout);
    let future_skew = Sha256::digest(
        serde_json::to_vec(&payload(
            STATE_ABI_VERSION,
            laminar_core::state::PARTITIONING_ABI_VERSION,
            None,
            30_000,
        ))
        .unwrap(),
    );
    assert_ne!(current, future_skew);
}

#[test]
fn source_input_mode_changes_canonical_identity() {
    let digest =
        |input_mode| canonical_source_digest(input_mode, SourceRowPositionCapability::Unavailable);
    let append = digest(SourceInputMode::AppendOnly);
    let upsert = digest(SourceInputMode::KeyedUpsert);
    let changelog = digest(SourceInputMode::FullChangelog);
    assert_ne!(append, upsert);
    assert_ne!(append, changelog);
    assert_ne!(upsert, changelog);
}

#[test]
fn source_row_position_capability_changes_canonical_identity() {
    assert_ne!(
        canonical_source_digest(
            SourceInputMode::AppendOnly,
            SourceRowPositionCapability::Unavailable,
        ),
        canonical_source_digest(
            SourceInputMode::AppendOnly,
            SourceRowPositionCapability::OrderedDeterministic,
        )
    );
}

#[test]
fn source_replay_order_binds_identity_without_changing_undeclared_sources() {
    let contract = SourceContract::new(
        laminar_connectors::connector::SourceConsistency::Replayable,
        laminar_connectors::connector::SourceTopology::Singleton,
        SourceInputMode::AppendOnly,
    );
    let encode = |contract| {
        serde_json::to_vec(&canonical_source(
            "events".into(),
            1,
            "test".into(),
            BTreeMap::new(),
            contract,
            None,
        ))
        .unwrap()
    };
    let legacy = encode(contract);
    assert_eq!(
        legacy,
        br#"{"name":"events","catalog_generation":1,"connector_type":"test","options":{},"input_mode":"append_only","row_positions":"unavailable","schema":null,"primary_key":[],"watermark_column":null,"max_out_of_orderness_ms":null,"processing_time":false}"#
    );
    let ordered = encode(contract.with_replay_order(SourceReplayOrder::SingleChannel));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ordered).unwrap()["replay_order"],
        "single_channel"
    );
    assert_ne!(Sha256::digest(&legacy), Sha256::digest(&ordered));
    let cut = encode(contract.with_replay_order(SourceReplayOrder::SingleChannelFixedBatches));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&cut).unwrap()["replay_order"],
        "single_channel_fixed_batches"
    );
    assert_ne!(Sha256::digest(&ordered), Sha256::digest(&cut));
}

#[test]
fn source_order_identity_inspects_the_schema_supplied_at_startup() {
    use std::sync::Arc;

    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::connector::{SourceBatch, SourceConnector, SourceStart};
    use laminar_connectors::error::ConnectorError;
    use laminar_connectors::generator::GeneratorSource;

    struct SchemaOrderedGenerator(GeneratorSource);

    #[async_trait::async_trait]
    impl SourceConnector for SchemaOrderedGenerator {
        fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            let contract = self.0.contract(config)?;
            Ok(if config.arrow_schema().is_some() {
                contract.with_replay_order(SourceReplayOrder::SingleChannel)
            } else {
                contract
            })
        }

        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            self.0.start(request).await
        }

        async fn poll_batch(
            &mut self,
            limit: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.0.poll_batch(limit).await
        }

        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            self.0.checkpoint()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.0.close().await
        }
    }

    let registry = ConnectorRegistry::new();
    registry
        .register_source(
            "schema-order-test",
            ConnectorInfo {
                name: "schema-order-test".into(),
                display_name: "Schema-dependent order declaration".into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(|_| Ok(Box::new(SchemaOrderedGenerator(GeneratorSource::default())))),
        )
        .unwrap();
    let config = ConnectorConfig::new("schema-order-test");
    let schema = GeneratorSource::default().schema();
    let (_, original_options, undeclared) =
        canonical_source_connector(&config, &registry, None).unwrap();
    let (_, options, declared) =
        canonical_source_connector(&config, &registry, Some(&schema)).unwrap();
    assert_eq!(undeclared.replay_order, SourceReplayOrder::Unspecified);
    assert_eq!(declared.replay_order, SourceReplayOrder::SingleChannel);
    assert_eq!(options, original_options);
    assert!(!options.contains_key("_arrow_schema"));
}

#[test]
fn temporal_retention_changes_only_temporal_stream_identity() {
    let stream = |join_config| StreamRegistration {
        name: "joined".into(),
        query_sql: "SELECT * FROM trades".into(),
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config,
        has_analytic: false,
        has_frame: false,
        incremental: false,
        subscription_output: None,
        subscription_retention_bytes: 0,
        catalog_generation: 1,
        subscription_certificate: None,
    };
    let temporal = stream(Some(vec![
        laminar_sql::translator::JoinOperatorConfig::Temporal(
            laminar_sql::translator::TemporalJoinTranslatorConfig {
                left_table: "trades".into(),
                right_table: "quotes".into(),
                left_key_columns: vec!["symbol".into()],
                right_key_columns: vec!["symbol".into()],
                left_time_column: "trade_time".into(),
                right_time_column: "quote_time".into(),
                join_kind: laminar_sql::temporal::TemporalJoinKind::Left,
                probe_schedule: laminar_sql::temporal::TemporalProbeSchedule::as_of(),
                probe_alias: None,
            },
        ),
    ]));
    let ordinary = stream(None);
    let identity_for = |registration: &StreamRegistration, retention: std::time::Duration| {
        let config = LaminarConfig {
            temporal_join_idle_history_retention: Some(retention),
            ..LaminarConfig::default()
        };
        let catalog = SourceCatalog::new(8, laminar_core::streaming::BackpressureStrategy::Block);
        let connector_registry = ConnectorRegistry::new();
        let registrations = PipelineRegistrations::new(
            std::iter::empty::<&SourceRegistration>(),
            std::iter::empty::<&SinkRegistration>(),
            std::iter::once(registration),
            std::iter::empty::<&TableRegistration>(),
        );
        compute(&PipelineIdentityContext::new(
            &config,
            &catalog,
            &connector_registry,
            registrations,
            1,
        ))
        .unwrap()
    };

    assert_ne!(
        identity_for(&temporal, std::time::Duration::from_secs(60)),
        identity_for(&temporal, std::time::Duration::from_secs(120))
    );
    assert_eq!(
        identity_for(&ordinary, std::time::Duration::from_secs(60)),
        identity_for(&ordinary, std::time::Duration::from_secs(120))
    );
}

#[test]
fn connector_property_order_is_canonical_and_credentials_are_ignored() {
    let mut left = ConnectorConfig::new("kafka");
    left.set("topic", "trades");
    left.set("password", "first");
    let mut right = ConnectorConfig::new("kafka");
    right.set("password", "rotated");
    right.set("topic", "trades");
    assert_eq!(canonical_connector(&left), canonical_connector(&right));
}

#[test]
fn connector_uri_credentials_are_absent_from_durable_identity() {
    let mut config = ConnectorConfig::new("mongodb");
    config.set(
        "connection.uri",
        "mongodb://alice:catalog-secret@db.test/app?token=query-secret",
    );
    let (_, properties) = canonical_connector(&config);
    let identity = properties.get("connection.uri").unwrap();
    assert_eq!(
        identity,
        "mongodb://<redacted>@db.test/app?token=<redacted>"
    );
    let serialized = serde_json::to_string(&properties).unwrap();
    assert!(!serialized.contains("alice"));
    assert!(!serialized.contains("catalog-secret"));
    assert!(!serialized.contains("query-secret"));

    let mut rotated = ConnectorConfig::new("mongodb");
    rotated.set(
        "connection.uri",
        "mongodb://bob:rotated@db.test/app?token=rotated-query",
    );
    assert_eq!(canonical_connector(&config), canonical_connector(&rotated));
}

#[test]
fn schema_metadata_order_is_canonical() {
    let left = Schema::new_with_metadata(
        vec![Field::new("id", arrow_schema::DataType::Int64, false)],
        [("b".into(), "2".into()), ("a".into(), "1".into())]
            .into_iter()
            .collect(),
    );
    let right = Schema::new_with_metadata(
        vec![Field::new("id", arrow_schema::DataType::Int64, false)],
        [("a".into(), "1".into()), ("b".into(), "2".into())]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        serde_json::to_vec(&canonical_schema(&left)).unwrap(),
        serde_json::to_vec(&canonical_schema(&right)).unwrap()
    );
}

#[tokio::test]
async fn source_primary_key_order_changes_pipeline_identity() {
    let identity_for = |primary_key: &[&str]| {
        let catalog = SourceCatalog::new(8, laminar_core::streaming::BackpressureStrategy::Block);
        catalog
            .register_source(
                "events",
                std::sync::Arc::new(Schema::new(vec![
                    Field::new("tenant", arrow_schema::DataType::Utf8, false),
                    Field::new("event_id", arrow_schema::DataType::Int64, false),
                ])),
                primary_key.iter().map(|column| (*column).into()).collect(),
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let config = LaminarConfig::default();
        let connector_registry = ConnectorRegistry::new();
        let registrations = PipelineRegistrations::new(
            std::iter::empty::<&SourceRegistration>(),
            std::iter::empty::<&SinkRegistration>(),
            std::iter::empty::<&StreamRegistration>(),
            std::iter::empty::<&TableRegistration>(),
        );
        compute(&PipelineIdentityContext::new(
            &config,
            &catalog,
            &connector_registry,
            registrations,
            1,
        ))
        .unwrap()
    };

    assert_ne!(
        identity_for(&["tenant", "event_id"]),
        identity_for(&["event_id", "tenant"])
    );
}
