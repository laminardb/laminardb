use super::*;

#[test]
fn test_initial_zeros() {
    let m = WebSocketSinkMetrics::local();
    assert_eq!(m.messages_sent.get(), 0);
    assert_eq!(m.bytes_sent.get(), 0);
    assert_eq!(m.messages_dropped_slow_client.get(), 0);
    assert_eq!(m.delivery_failures.get(), 0);
}

#[test]
fn test_record_send() {
    let m = WebSocketSinkMetrics::local();
    m.record_send(512);
    m.record_send(1024);

    assert_eq!(m.messages_sent.get(), 2);
    assert_eq!(m.bytes_sent.get(), 1536);
}

#[test]
fn test_record_drop() {
    let m = WebSocketSinkMetrics::local();
    m.record_drops(1);
    m.record_drops(1);
    m.record_delivery_failure(3);

    assert_eq!(m.messages_dropped_slow_client.get(), 2);
    assert_eq!(m.delivery_failures.get(), 3);
}

#[test]
fn test_record_connect_disconnect() {
    let m = WebSocketSinkMetrics::local();
    m.record_connect();
    m.record_connect();
    m.record_connect();
    m.record_disconnect();

    assert_eq!(m.connected_clients.get(), 2);
    assert_eq!(m.client_disconnects.get(), 1);
}

#[test]
fn test_disconnect_saturates_at_zero() {
    let m = WebSocketSinkMetrics::local();
    // Disconnect without any connect should not underflow
    m.record_disconnect();

    assert_eq!(m.connected_clients.get(), 0);
    assert_eq!(m.client_disconnects.get(), 1);
}

#[test]
fn test_combined_operations() {
    let m = WebSocketSinkMetrics::local();
    m.record_send(100);
    m.record_send(200);
    m.record_send(300);
    m.record_drops(1);
    m.record_connect();
    m.record_connect();
    m.record_disconnect();
    assert_eq!(m.messages_sent.get(), 3);
    assert_eq!(m.bytes_sent.get(), 600);
    assert_eq!(m.messages_dropped_slow_client.get(), 1);
    assert_eq!(m.connected_clients.get(), 1);
}

#[test]
fn connection_guard_balances_shared_gauge() {
    let metrics = WebSocketSinkMetrics::local();
    let first = metrics.connection_guard();
    let second = metrics.connection_guard();
    assert_eq!(metrics.connected_clients.get(), 2);

    drop(first);
    assert_eq!(metrics.connected_clients.get(), 1);
    drop(second);
    assert_eq!(metrics.connected_clients.get(), 0);
    assert_eq!(metrics.client_disconnects.get(), 2);
}

#[test]
fn duplicate_family_registration_fails_visibly() {
    let registry = Registry::new();
    WebSocketSinkMetrics::register(&registry).unwrap();

    let error = WebSocketSinkMetrics::register(&registry)
        .unwrap_err()
        .to_string();

    assert!(error.contains("register WebSocket sink metrics"), "{error}");
}
