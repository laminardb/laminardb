use super::*;

#[test]
fn test_register_file_source() {
    let registry = ConnectorRegistry::new();
    register_file_source(&registry).unwrap();

    let sources = registry.list_sources();
    assert!(sources.contains(&"files".to_string()));

    let info = registry.source_info("files").unwrap();
    assert_eq!(info.name, "files");
    assert!(info.is_source);
    assert!(!info.is_sink);
}

#[test]
fn test_register_file_sink() {
    let registry = ConnectorRegistry::new();
    register_file_sink(&registry).unwrap();

    let sinks = registry.list_sinks();
    assert!(sinks.contains(&"files".to_string()));

    let info = registry.sink_info("files").unwrap();
    assert_eq!(info.name, "files");
    assert!(!info.is_source);
    assert!(info.is_sink);
}

#[test]
fn test_create_source_from_registry() {
    let registry = ConnectorRegistry::new();
    register_file_source(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("files");
    let source = registry.create_source(&config, None);
    assert!(source.is_ok());
}

#[test]
fn test_create_sink_from_registry() {
    let registry = ConnectorRegistry::new();
    register_file_sink(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("files");
    let sink = registry.create_sink(&config, None);
    assert!(sink.is_ok());
}
