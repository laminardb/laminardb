//! Standalone Kafka producer for the Market Data demo.
//!
//! Generates synthetic market data and produces to Redpanda/Kafka topics.
//!
//! # Running
//! ```bash
//! cargo run -p laminardb-demo --bin producer --features kafka
//! ```

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};

use laminardb_demo::generator::{self, MarketGenerator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let rate: usize = std::env::var("PRODUCER_RATE")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .unwrap_or(10);

    eprintln!("=== LaminarDB Market Data Producer ===");
    eprintln!("  Brokers: {brokers}");
    eprintln!("  Rate: {rate} ticks/symbol per batch");
    eprintln!("  Press Ctrl+C to stop");
    eprintln!();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let mut gen = MarketGenerator::new();
    let mut cycle: u64 = 0;

    loop {
        cycle += 1;
        let ts = chrono::Utc::now().timestamp_millis();

        let ticks = gen.generate_kafka_ticks(rate, ts);
        let orders = gen.generate_kafka_orders(rate / 2, ts);
        let book_updates = gen.generate_kafka_book_updates(ts);

        let tick_count =
            generator::produce_to_kafka_keyed(&producer, "market-ticks", &ticks, |t| {
                Some(&t.symbol)
            })
            .await?;
        let order_count =
            generator::produce_to_kafka_keyed(&producer, "order-events", &orders, |o| {
                Some(&o.symbol)
            })
            .await?;
        let book_count =
            generator::produce_to_kafka_keyed(&producer, "book-updates", &book_updates, |b| {
                Some(&b.symbol)
            })
            .await?;

        eprintln!(
            "[cycle {}] Produced {} ticks, {} orders, {} book updates",
            cycle, tick_count, order_count, book_count
        );

        // Wait for next cycle or shutdown signal
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nShutting down producer...");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }

    // Flush in-flight messages before exit
    producer.flush(Duration::from_secs(5))?;
    eprintln!("Producer stopped. All in-flight messages flushed.");

    Ok(())
}
