use std::time::Instant;

use laminar_db::LaminarDB;
use laminar_derive::Record;

#[derive(Clone, Debug, Record)]
struct BenchEvent {
    id: i64,
    value: f64,
    #[event_time]
    timestamp: i64,
}

#[tokio::test]
async fn bench_push_batch_performance() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE bench_events (id BIGINT, value DOUBLE, timestamp BIGINT)")
        .await
        .unwrap();

    let source = db.source::<BenchEvent>("bench_events").unwrap();

    let count = 1_000;
    let events: Vec<BenchEvent> = (0..count)
        .map(|i| BenchEvent {
            id: i,
            value: i as f64 * 1.5,
            timestamp: i * 1000,
        })
        .collect();

    println!("Starting push_batch of {} records...", count);
    let start = Instant::now();
    source.push_batch(events);
    let duration = start.elapsed();

    println!("Pushed {} records in {:?}", count, duration);
}
