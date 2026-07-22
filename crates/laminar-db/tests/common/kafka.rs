use std::time::{Duration, Instant};

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    Message,
};

/// Address of the Kafka broker started by `tests/docker/compose.yml`.
pub const KAFKA_BROKERS: &str = "127.0.0.1:19092";

/// Returns the broker address when it answers a Kafka metadata request.
pub fn kafka_brokers() -> Option<&'static str> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKERS)
        .set("socket.timeout.ms", "500")
        .create()
        .ok()?;
    let metadata = consumer
        .fetch_metadata(None, Duration::from_millis(500))
        .ok()?;
    (!metadata.brokers().is_empty()).then_some(KAFKA_BROKERS)
}

pub async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics(&[topic], &AdminOptions::new())
        .await
        .expect("create_topics");
    assert_eq!(
        results.len(),
        1,
        "unexpected Kafka create-topic result count"
    );
    for result in results {
        result.unwrap_or_else(|(topic, error)| {
            panic!("failed to create Kafka topic {topic}: {error:?}")
        });
    }
}

pub async fn delete_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let results = admin
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("delete_topics");
    assert_eq!(
        results.len(),
        1,
        "unexpected Kafka delete-topic result count"
    );
    for result in results {
        result.unwrap_or_else(|(topic, error)| {
            panic!("failed to delete Kafka topic {topic}: {error:?}")
        });
    }
}

pub async fn produce_json_seq(brokers: &str, topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");
    for i in 0..count {
        let payload = format!(r#"{{"id": {i}, "value": {}}}"#, i * 10);
        let key = i.to_string();
        producer
            .send(
                FutureRecord::to(topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

pub async fn consume_keyed(
    brokers: &str,
    topic: &str,
    group: &str,
    expected: usize,
    deadline: Duration,
) -> Vec<(Option<String>, Option<String>)> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");

    let mut out = Vec::new();
    let start = Instant::now();
    while out.len() < expected && start.elapsed() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let key = msg
                    .key()
                    .map(|key| String::from_utf8_lossy(key).into_owned());
                let value = msg
                    .payload()
                    .map(|payload| String::from_utf8_lossy(payload).into_owned());
                out.push((key, value));
            }
            Ok(Err(error)) => eprintln!("consumer error: {error}"),
            Err(_) => continue,
        }
    }
    out
}

pub fn json_i64(payload: &str, field: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|json| json.get(field).and_then(serde_json::Value::as_i64))
        .unwrap_or_else(|| panic!("payload missing integer field '{field}': {payload}"))
}

pub async fn wait_for_broker(deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if kafka_brokers().is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

pub async fn wait_for_broker_unavailable(deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if kafka_brokers().is_none() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}
