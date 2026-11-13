//! RabbitMQ: every node publishes persistent messages to one durable queue; the oracle consumes
//! the queue and acknowledges each delivery after counting it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::uri::AMQPUri;
use lapin::{Connection, ConnectionProperties};

use super::{block_on, setting, Destination, Observed, Oracle};

const PASSWORD: &str = "LAMINAR_SOAK_RABBITMQ_PASSWORD";

pub(super) struct RabbitMq {
    url: String,
    username: String,
}

impl RabbitMq {
    pub(super) fn from_environment() -> Self {
        setting(PASSWORD, None);
        Self {
            url: setting(
                "LAMINAR_SOAK_RABBITMQ_URL",
                Some("amqp://127.0.0.1:5672/%2f"),
            ),
            username: setting("LAMINAR_SOAK_RABBITMQ_USERNAME", Some("guest")),
        }
    }

    fn uri(&self) -> AMQPUri {
        let mut uri: AMQPUri = self
            .url
            .parse()
            .unwrap_or_else(|error| panic!("LAMINAR_SOAK_RABBITMQ_URL is invalid: {error}"));
        uri.authority.userinfo = lapin::uri::AMQPUserInfo {
            username: self.username.clone(),
            password: setting(PASSWORD, None),
        };
        uri
    }
}

impl Destination for RabbitMq {
    fn connector(&self) -> &'static str {
        "rabbitmq"
    }

    fn written_metric(&self) -> &'static str {
        "laminardb_rabbitmq_sink_published_total"
    }

    fn format(&self) -> Option<&'static str> {
        Some("json")
    }

    fn name(&self, run_id: &str) -> String {
        format!("soak.sink.{run_id}")
    }

    fn sink_properties(&self, queue: &str) -> String {
        format!(
            r#"url = "{url}"
username = "{username}"
password = "$${{{PASSWORD}}}"
"routing.key" = "{queue}"
"#,
            url = self.url,
            username = self.username,
        )
    }

    fn create(&self, queue: &str) {
        block_on(declare(self.uri(), queue));
    }

    fn delete(&self, queue: &str) {
        block_on(delete(self.uri(), queue));
    }

    fn spawn_oracle(&self, queue: &str) -> Oracle {
        let (uri, queue) = (self.uri(), queue.to_owned());
        Oracle::spawn(move |observed, stop| block_on(consume(uri, queue, observed, stop)))
    }
}

async fn declare(uri: AMQPUri, queue: &str) {
    let connection = Connection::connect_uri(uri, ConnectionProperties::default())
        .await
        .expect("connect to RabbitMQ");
    let channel = connection.create_channel().await.expect("channel");
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare the output queue");
    let _ = connection.close(200, "declared".into()).await;
}

async fn delete(uri: AMQPUri, queue: &str) {
    let Ok(connection) = Connection::connect_uri(uri, ConnectionProperties::default()).await else {
        return;
    };
    if let Ok(channel) = connection.create_channel().await {
        let _ = channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
    }
    let _ = connection.close(200, "deleted".into()).await;
}

async fn consume(uri: AMQPUri, queue: String, observed: Observed, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        let Ok(connection) =
            Connection::connect_uri(uri.clone(), ConnectionProperties::default()).await
        else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        if let Ok(channel) = connection.create_channel().await {
            if let Ok(mut consumer) = channel
                .basic_consume(
                    queue.as_str().into(),
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                while !stop.load(Ordering::Acquire) {
                    let Ok(next) =
                        tokio::time::timeout(Duration::from_millis(250), consumer.next()).await
                    else {
                        continue;
                    };
                    let Some(Ok(delivery)) = next else {
                        break;
                    };
                    observed.add(&delivery.data);
                    if delivery.ack(BasicAckOptions::default()).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = connection.close(200, "oracle".into()).await;
    }
}
