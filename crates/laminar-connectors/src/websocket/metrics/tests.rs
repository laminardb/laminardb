use super::*;

#[test]
fn test_initial_zeros() {
    let m = WebSocketSourceMetrics::local();
    assert_eq!(m.messages_received.get(), 0);
    assert_eq!(m.bytes_received.get(), 0);
    assert_eq!(m.parse_errors.get(), 0);
    assert_eq!(m.backpressure_drops.get(), 0);
}

#[test]
fn test_record_message() {
    let m = WebSocketSourceMetrics::local();
    m.record_message(1024);
    m.record_message(2048);

    assert_eq!(m.messages_received.get(), 2);
    assert_eq!(m.bytes_received.get(), 3072);
}

#[test]
fn test_record_reconnect() {
    let m = WebSocketSourceMetrics::local();
    m.record_reconnect();
    m.record_reconnect();

    assert_eq!(m.reconnect_count.get(), 2);
}

#[test]
fn test_record_parse_error() {
    let m = WebSocketSourceMetrics::local();
    m.record_parse_error();

    assert_eq!(m.parse_errors.get(), 1);
}

#[test]
fn test_record_backpressure_drop() {
    let m = WebSocketSourceMetrics::local();
    m.record_backpressure_drop();
    assert_eq!(m.backpressure_drops.get(), 1);
}

#[test]
fn test_combined_operations() {
    let m = WebSocketSourceMetrics::local();
    m.record_message(100);
    m.record_message(200);
    m.record_reconnect();
    m.record_parse_error();

    assert_eq!(m.messages_received.get(), 2);
    assert_eq!(m.bytes_received.get(), 300);
    assert_eq!(m.parse_errors.get(), 1);
}

#[test]
fn duplicate_family_registration_fails_visibly() {
    let registry = Registry::new();
    WebSocketSourceMetrics::register(&registry).unwrap();

    let error = WebSocketSourceMetrics::register(&registry)
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("register WebSocket source metrics"),
        "{error}"
    );
}
