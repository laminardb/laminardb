use super::*;

#[test]
fn test_default_is_block() {
    let strategy = WsBackpressure::default();
    assert!(matches!(strategy, WsBackpressure::Block));
}
