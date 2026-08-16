use super::*;
use crate::reference::MockReferenceTableSource;
use crate::testing::*;

fn mock_info(name: &str, is_source: bool, is_sink: bool) -> ConnectorInfo {
    ConnectorInfo {
        name: name.to_string(),
        display_name: name.to_string(),
        version: "0.1.0".to_string(),
        is_source,
        is_sink,
        config_keys: vec![],
    }
}

fn declared_schema() -> SchemaRef {
    Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]))
}

#[test]
fn test_register_and_create_source() {
    let registry = ConnectorRegistry::new();
    registry
        .register_source(
            "mock",
            mock_info("mock", true, false),
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()))
            }),
        )
        .unwrap();

    let config = ConnectorConfig::new("mock");
    let connector = registry.create_source(&config, None);
    assert!(connector.is_ok());
    assert_eq!(
        registry.source_recovery_identity_options(&config).unwrap(),
        None
    );
}

#[test]
fn test_register_and_create_sink() {
    let registry = ConnectorRegistry::new();
    registry
        .register_sink(
            "mock",
            mock_info("mock", false, true),
            Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new()))),
        )
        .unwrap();

    let config = ConnectorConfig::new("mock");
    let connector = registry.create_sink(&config, None);
    assert!(connector.is_ok());
}

#[test]
fn factories_receive_shared_registry_identity() {
    let connectors = ConnectorRegistry::new();
    let metrics = Arc::new(prometheus::Registry::new());

    let source_metrics = Arc::clone(&metrics);
    connectors
            .register_source(
                "identity-source",
                mock_info("identity-source", true, false),
                Arc::new(move |registry: Option<&Arc<prometheus::Registry>>| {
                    assert!(
                        registry.is_some_and(|registry| { Arc::ptr_eq(registry, &source_metrics) })
                    );
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();

    let sink_metrics = Arc::clone(&metrics);
    connectors
        .register_sink(
            "identity-sink",
            mock_info("identity-sink", false, true),
            Arc::new(
                move |_config, registry: Option<&Arc<prometheus::Registry>>| {
                    assert!(registry.is_some_and(|registry| Arc::ptr_eq(registry, &sink_metrics)));
                    Ok(Box::new(MockSinkConnector::new()))
                },
            ),
        )
        .unwrap();

    connectors
        .create_source(&ConnectorConfig::new("identity-source"), Some(&metrics))
        .expect("source factory must receive the shared registry Arc");
    connectors
        .create_sink(&ConnectorConfig::new("identity-sink"), Some(&metrics))
        .expect("sink factory must receive the shared registry Arc");
}

struct RejectLookupFactory;

#[async_trait]
impl LookupSourceFactory for RejectLookupFactory {
    async fn build(
        &self,
        _config: ConnectorConfig,
        _declared_schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "test lookup factory has no backing source".into(),
        ))
    }
}

#[test]
fn sink_factory_receives_config_and_propagates_validation_errors() {
    let registry = ConnectorRegistry::new();
    registry
        .register_sink(
            "validated",
            mock_info("validated", false, true),
            Arc::new(|config, _registry| {
                if config.get("enabled") == Some("true") {
                    Ok(Box::new(MockSinkConnector::new()))
                } else {
                    Err(ConnectorError::ConfigurationError(
                        "validated sink requires enabled = true".into(),
                    ))
                }
            }),
        )
        .unwrap();

    let mut config = ConnectorConfig::new("validated");
    let error = match registry.create_sink(&config, None) {
        Ok(_) => panic!("expected factory validation error"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("enabled = true"));

    config.set("enabled", "true");
    assert!(registry.create_sink(&config, None).is_ok());
}

#[test]
fn test_create_unknown_connector() {
    let registry = ConnectorRegistry::new();
    let config = ConnectorConfig::new("nonexistent");

    assert!(registry.create_source(&config, None).is_err());
    assert!(registry.source_recovery_identity_options(&config).is_err());
    assert!(registry.create_sink(&config, None).is_err());
}

#[test]
fn test_list_connectors() {
    let registry = ConnectorRegistry::new();
    registry
        .register_source(
            "kafka",
            mock_info("kafka", true, false),
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()))
            }),
        )
        .unwrap();
    registry
        .register_sink(
            "delta",
            mock_info("delta", false, true),
            Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new()))),
        )
        .unwrap();

    let sources = registry.list_sources();
    assert_eq!(sources.len(), 1);
    assert!(sources.contains(&"kafka".to_string()));

    let sinks = registry.list_sinks();
    assert_eq!(sinks.len(), 1);
    assert!(sinks.contains(&"delta".to_string()));
}

#[test]
fn test_connector_info() {
    let registry = ConnectorRegistry::new();
    registry
        .register_source(
            "kafka",
            mock_info("kafka", true, false),
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()))
            }),
        )
        .unwrap();

    let info = registry.source_info("kafka");
    assert!(info.is_some());
    assert_eq!(info.unwrap().name, "kafka");

    assert!(registry.source_info("nonexistent").is_none());
}

#[test]
fn test_format_registry() {
    let registry = ConnectorRegistry::new();

    assert!(registry.create_deserializer("json").is_ok());
    assert!(registry.create_serializer("csv").is_ok());
    assert!(registry.create_deserializer("unknown").is_err());
}

#[tokio::test]
async fn default_source_schema_some_when_discovered() {
    let registry = ConnectorRegistry::new();
    registry
        .register_source(
            "mock",
            mock_info("mock", true, false),
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()))
            }),
        )
        .unwrap();
    let schema = registry
        .default_source_schema("mock", &std::collections::HashMap::new())
        .await
        .expect("discovery must not fail");
    assert!(schema.is_some_and(|s| !s.fields().is_empty()));
}

#[tokio::test]
async fn default_source_schema_none_for_unknown_connector() {
    let registry = ConnectorRegistry::new();
    assert!(registry
        .default_source_schema("nope", &std::collections::HashMap::new())
        .await
        .expect("unknown connector is Ok(None), not Err")
        .is_none());
}

#[tokio::test]
async fn source_factory_errors_propagate_from_creation_and_discovery() {
    let registry = ConnectorRegistry::new();
    registry
            .register_source(
                "failing",
                mock_info("failing", true, false),
                Arc::new(
                    |_: Option<&Arc<prometheus::Registry>>| -> Result<
                        Box<dyn SourceConnector>,
                        ConnectorError,
                    > {
                        Err(ConnectorError::Internal(
                            "source construction failed".into(),
                        ))
                    },
                ),
            )
            .unwrap();

    let config = ConnectorConfig::new("failing");
    let Err(create_error) = registry.create_source(&config, None) else {
        panic!("source construction must fail");
    };
    assert!(create_error
        .to_string()
        .contains("source construction failed"));

    let discovery_error = registry
        .default_source_schema("failing", &std::collections::HashMap::new())
        .await
        .expect_err("schema discovery must propagate source construction failure");
    assert!(discovery_error
        .to_string()
        .contains("source construction failed"));
}

// ── Table source factory tests ──

#[test]
fn test_register_and_create_table_source() {
    use crate::reference::MockReferenceTableSource;

    let registry = ConnectorRegistry::new();
    let observed_schema = Arc::new(parking_lot::Mutex::new(None));
    let factory_schema = Arc::clone(&observed_schema);
    registry
        .register_table_source(
            "mock",
            mock_info("mock", true, false),
            Arc::new(move |_config, declared_schema| {
                *factory_schema.lock() = Some(declared_schema);
                Ok(Box::new(MockReferenceTableSource::empty()))
            }),
        )
        .unwrap();

    let config = ConnectorConfig::new("mock");
    let declared_schema = declared_schema();
    let source = registry.create_table_source(&config, Arc::clone(&declared_schema));
    assert!(source.is_ok());
    assert_eq!(observed_schema.lock().as_ref(), Some(&declared_schema));
    assert!(registry.has_table_source("mock"));
    assert!(!registry.has_table_source("missing"));
}

#[test]
fn test_create_unknown_table_source() {
    let registry = ConnectorRegistry::new();
    let config = ConnectorConfig::new("nonexistent");
    let result = registry.create_table_source(&config, declared_schema());
    match result {
        Err(e) => assert!(
            e.to_string().contains("snapshot-capable table source"),
            "got: {e}"
        ),
        Ok(_) => panic!("Expected error for unknown table source"),
    }
}

#[test]
fn test_list_table_sources() {
    let registry = ConnectorRegistry::new();
    assert!(registry.list_table_sources().is_empty());

    registry
        .register_table_source(
            "mock-table",
            mock_info("mock-table", true, false),
            Arc::new(|_config, _declared_schema| Ok(Box::new(MockReferenceTableSource::empty()))),
        )
        .unwrap();

    let names = registry.list_table_sources();
    assert_eq!(names.len(), 1);
    assert!(names.contains(&"mock-table".to_string()));
}

#[test]
fn duplicate_registration_is_rejected_in_every_category() {
    let registry = ConnectorRegistry::new();
    let source = || {
        Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(MockSourceConnector::new()) as Box<dyn SourceConnector>)
        }) as SourceFactory
    };
    let sink = || {
        Arc::new(
            |_config: &ConnectorConfig, _registry: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSinkConnector::new()) as Box<dyn SinkConnector>)
            },
        ) as SinkFactory
    };
    let table = || {
        Arc::new(|_config: &ConnectorConfig, _declared_schema: SchemaRef| {
            Ok(Box::new(MockReferenceTableSource::empty()) as Box<dyn ReferenceTableSource>)
        }) as TableSourceFactory
    };

    registry
        .register_source("same", mock_info("same", true, false), source())
        .unwrap();
    assert!(matches!(
        registry.register_source("same", mock_info("same", true, false), source()),
        Err(ConnectorError::FactoryAlreadyRegistered { kind: "source", .. })
    ));

    registry
        .register_sink("same", mock_info("same", false, true), sink())
        .unwrap();
    assert!(matches!(
        registry.register_sink("same", mock_info("same", false, true), sink()),
        Err(ConnectorError::FactoryAlreadyRegistered { kind: "sink", .. })
    ));

    registry
        .register_table_source("same", mock_info("same", true, false), table())
        .unwrap();
    assert!(matches!(
        registry.register_table_source("same", mock_info("same", true, false), table()),
        Err(ConnectorError::FactoryAlreadyRegistered {
            kind: "table source",
            ..
        })
    ));

    registry
        .register_lookup_source(
            "same",
            mock_info("same", true, false),
            Arc::new(RejectLookupFactory),
        )
        .unwrap();
    assert!(matches!(
        registry.register_lookup_source(
            "same",
            mock_info("same", true, false),
            Arc::new(RejectLookupFactory)
        ),
        Err(ConnectorError::FactoryAlreadyRegistered {
            kind: "lookup source",
            ..
        })
    ));
}

#[test]
fn freeze_rejects_every_registration_category() {
    let registry = ConnectorRegistry::new();
    registry.freeze();
    registry.freeze();
    assert!(registry.is_frozen());

    assert!(matches!(
        registry.register_source(
            "late-source",
            mock_info("late-source", true, false),
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()))
            })
        ),
        Err(ConnectorError::RegistryFrozen { kind: "source", .. })
    ));
    assert!(matches!(
        registry.register_sink(
            "late-sink",
            mock_info("late-sink", false, true),
            Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new())))
        ),
        Err(ConnectorError::RegistryFrozen { kind: "sink", .. })
    ));
    assert!(matches!(
        registry.register_table_source(
            "late-table",
            mock_info("late-table", true, false),
            Arc::new(|_config, _declared_schema| {
                Ok(Box::new(MockReferenceTableSource::empty()))
            })
        ),
        Err(ConnectorError::RegistryFrozen {
            kind: "table source",
            ..
        })
    ));
    assert!(matches!(
        registry.register_lookup_source(
            "late-lookup",
            mock_info("late-lookup", true, false),
            Arc::new(RejectLookupFactory)
        ),
        Err(ConnectorError::RegistryFrozen {
            kind: "lookup source",
            ..
        })
    ));
}
