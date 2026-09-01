use super::*;

#[test]
fn test_initial_zeros() {
    let m = PostgresSinkMetrics::new(None);
    assert_eq!(m.records_written.get(), 0);
    assert_eq!(m.bytes_written.get(), 0);
    assert_eq!(m.errors_total.get(), 0);
}

#[test]
fn test_record_write() {
    let m = PostgresSinkMetrics::new(None);
    m.record_write(100, 5000);
    m.record_write(200, 10_000);

    assert_eq!(m.records_written.get(), 300);
    assert_eq!(m.bytes_written.get(), 15_000);
}

#[test]
fn test_flush_and_copy_metrics() {
    let m = PostgresSinkMetrics::new(None);
    m.record_flush();
    m.record_flush();
    m.record_copy();

    assert_eq!(m.batches_flushed.get(), 2);
    assert_eq!(m.copy_operations.get(), 1);
}

#[test]
fn test_changelog_deletes() {
    let m = PostgresSinkMetrics::new(None);
    m.record_deletes(50);
    m.record_deletes(30);

    assert_eq!(m.changelog_deletes.get(), 80);
}

#[test]
fn test_error_counting() {
    let m = PostgresSinkMetrics::new(None);
    m.record_error();
    m.record_error();
    m.record_error();

    assert_eq!(m.errors_total.get(), 3);
}

#[test]
fn test_upsert_metric() {
    let m = PostgresSinkMetrics::new(None);
    m.record_upsert();

    assert_eq!(m.upsert_operations.get(), 1);
}
