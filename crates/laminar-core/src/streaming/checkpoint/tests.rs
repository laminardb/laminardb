use super::*;

#[test]
fn test_default_config() {
    let config = StreamCheckpointConfig::default();
    assert!(config.interval_ms.is_none());
    assert!(config.timeout_ms.is_none());
    assert!(config.data_dir.is_none());
    assert!(config.max_node_data_bytes.is_none());
}
