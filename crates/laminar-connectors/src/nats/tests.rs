use super::*;

#[test]
fn register_source_appears_in_registry() {
    let registry = ConnectorRegistry::new();
    register_nats_source(&registry).unwrap();
    assert!(registry.list_sources().contains(&"nats".to_string()));
}

#[test]
fn register_sink_appears_in_registry() {
    let registry = ConnectorRegistry::new();
    register_nats_sink(&registry).unwrap();
    assert!(registry.list_sinks().contains(&"nats".to_string()));
}
