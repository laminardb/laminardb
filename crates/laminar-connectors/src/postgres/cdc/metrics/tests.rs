use super::*;

#[test]
fn test_record_operations() {
    let m = PostgresCdcMetrics::new(None);
    m.record_insert();
    m.record_insert();
    m.record_update();
    m.record_delete();
    m.record_transaction();
    m.record_bytes(1024);
    m.record_batch();

    assert_eq!(m.events_received.get(), 4);
    assert_eq!(m.inserts.get(), 2);
    assert_eq!(m.updates.get(), 1);
    assert_eq!(m.deletes.get(), 1);
    assert_eq!(m.transactions.get(), 1);
    assert_eq!(m.bytes_received.get(), 1024);
    assert_eq!(m.batches_produced.get(), 1);
}

#[test]
fn test_lsn_and_lag_tracking() {
    let m = PostgresCdcMetrics::new(None);
    m.set_confirmed_flush_lsn(0x1234_ABCD);
    m.set_replication_lag_bytes(4096);

    assert_eq!(m.confirmed_flush_lsn.get(), 0x1234_ABCD_i64);
    assert_eq!(m.replication_lag_bytes.get(), 4096);
}
