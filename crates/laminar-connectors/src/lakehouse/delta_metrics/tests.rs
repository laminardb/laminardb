use super::*;

#[test]
fn test_initial_zeros() {
    let m = DeltaLakeSinkMetrics::new(None);
    assert_eq!(m.common.rows_flushed.get(), 0);
    assert_eq!(m.common.bytes_written.get(), 0);
    assert_eq!(m.common.errors_total.get(), 0);
}

#[test]
fn test_record_flush() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_flush(100, 5000);
    m.record_flush(200, 10_000);

    assert_eq!(m.common.rows_flushed.get(), 300);
    assert_eq!(m.common.bytes_written.get(), 15_000);
    assert_eq!(m.common.flush_count.get(), 2);
}

#[test]
fn test_record_commit() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_commit(1);
    m.record_commit(5);

    assert_eq!(m.common.commits.get(), 2);
    assert_eq!(m.last_delta_version.get(), 5);
}

#[test]
fn test_error_counting() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_error();
    m.record_error();
    m.record_error();

    assert_eq!(m.common.errors_total.get(), 3);
}

#[test]
fn test_rollback_counting() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_rollback();
    m.record_rollback();

    assert_eq!(m.common.epochs_rolled_back.get(), 2);
}

#[test]
fn test_merge_operations() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_merge();

    assert_eq!(m.merge_operations.get(), 1);
}

#[test]
fn test_changelog_deletes() {
    let m = DeltaLakeSinkMetrics::new(None);
    m.record_deletes(50);
    m.record_deletes(30);

    assert_eq!(m.common.changelog_deletes.get(), 80);
}
