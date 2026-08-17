use super::*;

#[test]
fn test_source_metrics_record_events() {
    let m = MongoDbCdcMetrics::new(None);
    m.record_event("I");
    m.record_event("I");
    m.record_event("U");
    m.record_event("D");
    m.record_event("DROP");
    m.record_bytes(1024);
    m.record_error();
    m.record_batch();
    m.record_reconnect();

    assert_eq!(m.events_received.get(), 5);
    assert_eq!(m.inserts.get(), 2);
    assert_eq!(m.updates.get(), 1);
    assert_eq!(m.deletes.get(), 1);
    assert_eq!(m.lifecycle_events.get(), 1);
    assert_eq!(m.bytes_received.get(), 1024);
    assert_eq!(m.errors.get(), 1);
    assert_eq!(m.batches_produced.get(), 1);
    assert_eq!(m.reconnects.get(), 1);
}

#[test]
fn test_sink_metrics_record_flush() {
    let m = MongoDbSinkMetrics::new(None);
    m.record_flush(100, 5000);
    m.record_bulk_write();
    m.record_inserts(80);
    m.record_upserts(15);
    m.record_deletes(5);
    m.record_error();

    assert_eq!(m.records_written.get(), 100);
    assert_eq!(m.bytes_written.get(), 5000);
    assert_eq!(m.batches_flushed.get(), 1);
    assert_eq!(m.bulk_writes.get(), 1);
    assert_eq!(m.inserts.get(), 80);
    assert_eq!(m.upserts.get(), 15);
    assert_eq!(m.deletes.get(), 5);
    assert_eq!(m.errors.get(), 1);
}
