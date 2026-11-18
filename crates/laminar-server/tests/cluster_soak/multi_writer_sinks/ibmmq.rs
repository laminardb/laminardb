//! IBM MQ, against the `tests/docker/compose.yml` queue manager `QM1`: every node puts persistent
//! messages on one local queue under syncpoint; the oracle reads the queue through the IBM MQ
//! source and commits each batch after counting it. The IBM MQ source soak reads its input queue
//! with the same settings.

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

use super::{block_on, setting, wait_until, Destination, Observed, Oracle};

const PASSWORD: &str = "LAMINAR_SOAK_IBMMQ_PASSWORD";
/// runmqsc's message for an object another connection holds open.
const OBJECT_IN_USE: &str = "AMQ8148E";
const IN_USE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct IbmMq {
    library: String,
    container: String,
    connection: String,
}

impl IbmMq {
    pub(crate) fn from_environment() -> Self {
        setting(PASSWORD, None);
        Self {
            library: setting("LAMINAR_SOAK_IBMMQ_LIBRARY", None),
            container: setting("LAMINAR_SOAK_IBMMQ_CONTAINER", None),
            connection: setting("LAMINAR_SOAK_IBMMQ_CONNECTION", Some("127.0.0.1(1414)")),
        }
    }

    /// The run's input queue for the IBM MQ source soak.
    pub(crate) fn source_queue(run_id: &str) -> String {
        format!("DEV.SOAK.SOURCE.{}", run_id.replace('-', "."))
    }

    /// Server properties that connect to `queue` as the developer image's `app` user.
    pub(crate) fn queue_properties(&self, queue: &str) -> String {
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

    /// A test client's connector config for `queue`.
    pub(crate) fn client_config(&self, queue: &str, format: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("ibmmq");
        for (key, value) in [
            ("client.library", self.library.as_str()),
            ("queue.manager", "QM1"),
            ("channel", "DEV.APP.SVRCONN"),
            ("connection.name", self.connection.as_str()),
            ("username", "app"),
            ("queue", queue),
            ("format", format),
        ] {
            config.set(key, value);
        }
        config.set("password", setting(PASSWORD, None));
        config
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
        self.queue_properties(queue)
    }

    fn create(&self, queue: &str) {
        mqsc(
            &self.container,
            &format!("DEFINE QLOCAL('{queue}') MAXDEPTH(999999999) REPLACE\n"),
        )
        .unwrap_or_else(|output| panic!("runmqsc failed: {output}"));
    }

    /// A stopped node's connection may keep the queue open until the queue manager reaps it.
    fn delete(&self, queue: &str) {
        let command = format!("DELETE QLOCAL('{queue}') PURGE\n");
        let deleted = wait_until(IN_USE_TIMEOUT, || match mqsc(&self.container, &command) {
            Ok(()) => true,
            Err(output) if output.contains(OBJECT_IN_USE) => false,
            Err(output) => panic!("runmqsc failed: {output}"),
        });
        assert!(
            deleted,
            "queue {queue} stayed in use for {IN_USE_TIMEOUT:?}"
        );
    }

    fn spawn_oracle(&self, queue: &str) -> Oracle {
        let config = self.client_config(queue, "raw");
        Oracle::spawn(move |observed, stop| block_on(consume(config, observed, stop)))
    }
}

/// Run MQSC commands on `QM1`; a failure returns runmqsc's output.
fn mqsc(container: &str, commands: &str) -> Result<(), String> {
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
    if output.status.success() {
        return Ok(());
    }
    Err(String::from_utf8_lossy(&output.stdout).into_owned())
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
