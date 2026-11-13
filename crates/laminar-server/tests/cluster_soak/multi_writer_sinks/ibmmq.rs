//! IBM MQ, against the `tests/docker/compose.yml` queue manager `QM1`: every node puts persistent
//! messages on one local queue under syncpoint; the oracle reads the queue through the IBM MQ
//! source and commits each batch after counting it.

use std::io::Write as _;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray as _;
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector as _, SourcePosition, SourceStart,
};
use laminar_connectors::ibmmq::IbmMqSource;

use super::{block_on, setting, Destination, Observed, Oracle};

const PASSWORD: &str = "LAMINAR_SOAK_IBMMQ_PASSWORD";

pub(super) struct IbmMq {
    library: String,
    container: String,
    connection: String,
}

impl IbmMq {
    pub(super) fn from_environment() -> Self {
        setting(PASSWORD, None);
        Self {
            library: setting("LAMINAR_SOAK_IBMMQ_LIBRARY", None),
            container: setting("LAMINAR_SOAK_IBMMQ_CONTAINER", None),
            connection: setting("LAMINAR_SOAK_IBMMQ_CONNECTION", Some("127.0.0.1(1414)")),
        }
    }
}

impl Destination for IbmMq {
    fn connector(&self) -> &'static str {
        "ibmmq"
    }

    fn written_metric(&self) -> &'static str {
        "laminardb_ibmmq_sink_put_total"
    }

    fn format(&self) -> Option<&'static str> {
        Some("json")
    }

    // The developer image authorizes `app` for `DEV.**`; MQ names allow no hyphen.
    fn name(&self, run_id: &str) -> String {
        format!("DEV.SOAK.SINK.{}", run_id.replace('-', "."))
    }

    fn sink_properties(&self, queue: &str) -> String {
        format!(
            r#""client.library" = "{library}"
"queue.manager" = "QM1"
channel = "DEV.APP.SVRCONN"
"connection.name" = "{connection}"
username = "app"
password = "$${{{PASSWORD}}}"
queue = "{queue}"
"#,
            library = self.library,
            connection = self.connection,
        )
    }

    fn create(&self, queue: &str) {
        mqsc(
            &self.container,
            &format!("DEFINE QLOCAL('{queue}') MAXDEPTH(999999999) REPLACE\n"),
        );
    }

    fn delete(&self, queue: &str) {
        mqsc(
            &self.container,
            &format!("DELETE QLOCAL('{queue}') PURGE\n"),
        );
    }

    fn spawn_oracle(&self, queue: &str) -> Oracle {
        let config = oracle_config(&self.library, &self.connection, queue);
        Oracle::spawn(move |observed, stop| block_on(consume(config, observed, stop)))
    }
}

fn mqsc(container: &str, commands: &str) {
    let mut child = Command::new("docker")
        .args(["exec", "-i", container, "runmqsc", "QM1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("run runmqsc in the IBM MQ container");
    child
        .stdin
        .take()
        .expect("runmqsc stdin")
        .write_all(commands.as_bytes())
        .expect("write MQSC commands");
    let output = child.wait_with_output().expect("runmqsc output");
    assert!(
        output.status.success(),
        "runmqsc failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn oracle_config(library: &str, connection: &str, queue: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("ibmmq");
    for (key, value) in [
        ("client.library", library),
        ("queue.manager", "QM1"),
        ("channel", "DEV.APP.SVRCONN"),
        ("connection.name", connection),
        ("username", "app"),
        ("queue", queue),
        ("format", "raw"),
    ] {
        config.set(key, value);
    }
    config.set("password", setting(PASSWORD, None));
    config
}

/// Commit each polled batch after recording it; a failed generation restarts the source, and the
/// queue manager returns its uncommitted messages.
async fn consume(config: ConnectorConfig, observed: Observed, stop: Arc<AtomicBool>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let mut epoch = 0;
    while !stop.load(Ordering::Acquire) {
        let mut source = IbmMqSource::new(Arc::clone(&schema), None);
        let start = SourceStart::new(
            config.clone(),
            SourcePosition::Initial,
            DeliveryGuarantee::AtLeastOnce,
        )
        .expect("oracle start request");
        if source.start(start).await.is_err() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        while !stop.load(Ordering::Acquire) {
            let batch = match source.poll_batch(1_000).await {
                Ok(Some(batch)) => batch,
                Ok(None) => continue,
                Err(_) => break,
            };
            for text in batch.records.column(0).as_string::<i32>().iter().flatten() {
                observed.add(text.as_bytes());
            }
            epoch += 1;
            let cut = source.checkpoint();
            if source.notify_epoch_committed(epoch, &cut).await.is_err() {
                break;
            }
        }
        let _ = source.close().await;
    }
}
