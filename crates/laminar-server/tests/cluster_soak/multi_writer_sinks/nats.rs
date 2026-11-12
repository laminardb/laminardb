//! NATS JetStream: every node publishes to one subject captured by a file-backed stream with three
//! replicas, the sink's durability minimum, and waits for each publish acknowledgement. The oracle
//! drains the stream through a durable consumer and acknowledges each message after counting it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{self, consumer, stream};
use futures_util::StreamExt as _;

use super::{block_on, setting, Destination, Observed, Oracle};

const ORACLE_CONSUMER: &str = "soak-oracle";

#[derive(Clone)]
pub(super) struct Nats {
    url: String,
}

impl Nats {
    pub(super) fn from_environment() -> Self {
        Self {
            url: setting("LAMINAR_SOAK_NATS_URL", Some("nats://127.0.0.1:14222")),
        }
    }

    async fn context(&self) -> Result<jetstream::Context, async_nats::ConnectError> {
        Ok(jetstream::new(async_nats::connect(&self.url).await?))
    }
}

fn subject(stream: &str) -> String {
    format!("soak.sink.{stream}")
}

impl Destination for Nats {
    fn connector(&self) -> &'static str {
        "nats"
    }

    fn written_metric(&self) -> &'static str {
        "laminardb_nats_sink_records_total"
    }

    fn format(&self) -> Option<&'static str> {
        Some("json")
    }

    fn name(&self, run_id: &str) -> String {
        format!("SOAK_SINK_{}", run_id.replace('-', "_"))
    }

    fn sink_properties(&self, stream: &str) -> String {
        format!(
            r#"servers = "{url}"
mode = "jetstream"
stream = "{stream}"
subject = "{subject}"
"#,
            url = self.url,
            subject = subject(stream),
        )
    }

    fn create(&self, stream: &str) {
        block_on(async {
            self.context()
                .await
                .expect("connect to NATS")
                .create_stream(stream::Config {
                    name: stream.to_owned(),
                    subjects: vec![subject(stream)],
                    storage: stream::StorageType::File,
                    num_replicas: 3,
                    ..stream::Config::default()
                })
                .await
                .expect("create the output stream");
        });
    }

    fn delete(&self, stream: &str) {
        block_on(async {
            if let Ok(context) = self.context().await {
                let _ = context.delete_stream(stream).await;
            }
        });
    }

    fn spawn_oracle(&self, stream: &str) -> Oracle {
        let (nats, stream) = (self.clone(), stream.to_owned());
        Oracle::spawn(move |observed, stop| block_on(consume(nats, stream, observed, stop)))
    }
}

/// A reconnect redelivers the messages read but not yet acknowledged, which can add duplicates
/// but never hide a loss.
async fn consume(nats: Nats, stream: String, observed: Observed, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        let Ok(mut messages) = open_consumer(&nats, &stream).await else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        while !stop.load(Ordering::Acquire) {
            let Ok(next) = tokio::time::timeout(Duration::from_millis(250), messages.next()).await
            else {
                continue;
            };
            let Some(Ok(message)) = next else {
                break;
            };
            observed.add(&message.payload);
            if message.ack().await.is_err() {
                break;
            }
        }
    }
}

async fn open_consumer(
    nats: &Nats,
    stream: &str,
) -> Result<consumer::pull::Stream, Box<dyn std::error::Error + Send + Sync>> {
    let consumer = nats
        .context()
        .await?
        .get_stream(stream)
        .await?
        .get_or_create_consumer(
            ORACLE_CONSUMER,
            consumer::pull::Config {
                durable_name: Some(ORACLE_CONSUMER.to_owned()),
                ack_policy: consumer::AckPolicy::Explicit,
                ..consumer::pull::Config::default()
            },
        )
        .await?;
    Ok(consumer.messages().await?)
}
