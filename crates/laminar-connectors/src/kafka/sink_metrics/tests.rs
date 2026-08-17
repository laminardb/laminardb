use super::*;

#[test]
fn test_initial_zeros() {
    let m = KafkaSinkMetrics::new(None);
    assert_eq!(m.records_written.get(), 0);
    assert_eq!(m.bytes_written.get(), 0);
    assert_eq!(m.errors_total.get(), 0);
}

#[test]
fn test_record_write() {
    let m = KafkaSinkMetrics::new(None);
    m.record_write(100, 5000);
    m.record_write(200, 10000);
    assert_eq!(m.records_written.get(), 300);
    assert_eq!(m.bytes_written.get(), 15000);
}

#[test]
fn test_produce_latency() {
    let m = KafkaSinkMetrics::new(None);
    m.record_produce_latency(100);
    m.record_produce_latency(300);
    m.record_produce_latency(50);

    assert_eq!(m.produce_latency_count.get(), 3);
    assert_eq!(m.produce_latency_sum_us.get(), 450);
    assert_eq!(m.produce_latency_max_us.get(), 300);
}
