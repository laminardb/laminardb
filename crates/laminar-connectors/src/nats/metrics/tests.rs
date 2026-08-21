use super::*;

#[test]
fn source_initial_zero() {
    let m = NatsSourceMetrics::new(None);
    assert_eq!(m.records_total.get(), 0);
    assert_eq!(m.bytes_total.get(), 0);
    assert_eq!(m.fetch_errors_total.get(), 0);
    assert_eq!(m.consumer_lag.get(), 0);
}

#[test]
fn source_records_increment() {
    let m = NatsSourceMetrics::new(None);
    m.record_poll(100, 4096);
    m.record_poll(50, 1024);
    m.record_ack_enqueued();
    m.record_ack();
    m.record_ack_enqueued();
    m.record_ack_error();
    m.record_ack_enqueued();
    m.record_ack_enqueued();
    m.record_ack_abandoned(2);
    m.record_fetch_error();

    m.set_consumer_lag(42);

    assert_eq!(m.records_total.get(), 150);
    assert_eq!(m.bytes_total.get(), 5120);
    assert_eq!(m.fetch_errors_total.get(), 1);
    assert_eq!(m.consumer_lag.get(), 42);
    assert_eq!(m.acks_total.get(), 1);
    assert_eq!(m.ack_errors_total.get(), 3);
    assert_eq!(m.pending_acks.get(), 0);
}

#[test]
fn sink_records_and_dedup() {
    let m = NatsSinkMetrics::new(None);
    for _ in 0..10 {
        m.record_published_row(200);
    }
    m.record_dedup();
    m.record_dedup();
    m.set_pending_futures(3);

    assert_eq!(m.records_total.get(), 10);
    assert_eq!(m.bytes_total.get(), 2000);
    assert_eq!(m.pending_futures.get(), 3);
    assert_eq!(m.dedup_total.get(), 2);
}

#[test]
fn metrics_register_on_shared_registry() {
    let reg = Registry::new();
    let _s = NatsSourceMetrics::new(Some(&reg));
    let _k = NatsSinkMetrics::new(Some(&reg));
    let names: Vec<String> = reg.gather().iter().map(|f| f.name().to_string()).collect();
    assert!(names.contains(&"nats_source_records_total".to_string()));
    assert!(names.contains(&"nats_sink_records_total".to_string()));
    assert!(names.contains(&"nats_sink_dedup_total".to_string()));
}
