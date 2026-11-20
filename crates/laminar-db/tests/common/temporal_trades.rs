//! Kafka trades for the CDC tests that price them through a temporal join on a rates source.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

/// The watermarked `trades` source: JSON objects `{"id", "key", "at"}` on `topic`, `at` in epoch
/// milliseconds.
pub fn trades_source(brokers: &str, topic: &str) -> String {
    format!(
        "CREATE SOURCE trades (id BIGINT NOT NULL, key BIGINT NOT NULL, at TIMESTAMP NOT NULL, \
         WATERMARK FOR at AS at - INTERVAL '1' SECOND) FROM KAFKA (\
         'bootstrap.servers' = '{brokers}', 'topic' = '{topic}', 'group.id' = '{topic}', \
         'startup.mode' = 'earliest', 'json.column.at.epoch_unit' = 'millis') FORMAT JSON"
    )
}

/// Produce `(id, key, at)` trades to `topic`.
pub async fn produce_trades(brokers: &str, topic: &str, trades: &[(i64, i64, i64)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");
    for (id, key, at) in trades {
        let payload = format!(r#"{{"id": {id}, "key": {key}, "at": {at}}}"#);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&id.to_string()),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

/// A wall-clock instant, in epoch milliseconds, at least 1.5 s after the last change and before
/// the next one; the fixtures' servers share this host's clock.
pub async fn instant_between_changes() -> i64 {
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let at = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    at
}
