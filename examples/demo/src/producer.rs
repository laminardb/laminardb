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
use rdkafka::producer::FutureProducer;

use laminardb_demo::generator::{self, MarketGenerator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let rate: usize = std::env::var("PRODUCER_RATE")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .unwrap_or(10);

    eprintln!("=== LaminarDB Market Data Producer ===");
    eprintln!("  Brokers: {brokers}");
    eprintln!("  Rate: {rate} ticks/symbol per batch");
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

        let tick_count = generator::produce_to_kafka(&producer, "market-ticks", &ticks).await?;
        let order_count = generator::produce_to_kafka(&producer, "order-events", &orders).await?;
        let book_count =
            generator::produce_to_kafka(&producer, "book-updates", &book_updates).await?;

        eprintln!(
            "[cycle {}] Produced {} ticks, {} orders, {} book updates",
            cycle, tick_count, order_count, book_count
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
