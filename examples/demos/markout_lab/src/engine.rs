//! Embedded LaminarDB startup, live WebSocket source, and bounded fill input.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow::array::{BinaryBuilder, RecordBatch, UInt32Array};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::registry::ConnectorRegistry;
use laminar_db::{LaminarDB, TypedSubscription};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::{mpsc, Notify};

use crate::config::DEFAULT_FEED_URL;
use crate::live_feed::register_live_feed;
use crate::types::{
    fill_schema, fills_to_batch, CurveEvent, FillEvent, MarketEvent, MarkoutEvent, SimulatedFill,
    SummaryEvent,
};

/// The single SQL asset executed by the app and exposed by the HTTP API.
pub const PIPELINE_SQL: &str = include_str!("../pipeline.sql");

const FILL_CONNECTOR: &str = "markout-fills";
const SOURCE_QUEUE_CAPACITY: usize = 64;

fn render_pipeline_sql(
    feed_url: &str,
    connect_timeout: Duration,
    read_timeout: Duration,
) -> Result<String> {
    if feed_url
        .chars()
        .any(|character| matches!(character, '\'' | '\n' | '\r'))
    {
        bail!("feed URL contains characters that cannot be embedded safely in SQL");
    }
    if !PIPELINE_SQL.contains(DEFAULT_FEED_URL) {
        bail!("pipeline.sql does not contain the expected default feed URL");
    }
    let connect_timeout_ms = u64::try_from(connect_timeout.as_millis())
        .context("feed connect timeout exceeds u64 milliseconds")?;
    let read_timeout_ms = u64::try_from(read_timeout.as_millis())
        .context("feed read timeout exceeds u64 milliseconds")?;
    Ok(PIPELINE_SQL
        .replacen(DEFAULT_FEED_URL, feed_url, 1)
        .replacen(
            "'connect.timeout.ms' = '15000'",
            &format!("'connect.timeout.ms' = '{connect_timeout_ms}'"),
            1,
        )
        .replacen(
            "'read.timeout.ms' = '5000'",
            &format!("'read.timeout.ms' = '{read_timeout_ms}'"),
            1,
        ))
}

#[derive(Clone)]
struct BatchInput {
    sender: mpsc::Sender<RecordBatch>,
    notify: Arc<Notify>,
}

impl BatchInput {
    async fn push(&self, batch: RecordBatch) -> Result<()> {
        self.sender
            .send(batch)
            .await
            .context("LaminarDB input channel closed")?;
        self.notify.notify_one();
        Ok(())
    }
}

#[derive(Clone)]
/// Bounded input handle for visitor-requested simulated fills.
pub struct EngineInputs {
    fills: BatchInput,
}

impl EngineInputs {
    /// Push a non-empty simulated-fill slice into the configured fill connector.
    ///
    /// # Errors
    /// Returns an error when Arrow conversion fails or the engine input is closed.
    pub async fn push_fills(&self, fills: &[SimulatedFill]) -> Result<()> {
        if fills.is_empty() {
            return Ok(());
        }
        self.fills.push(fills_to_batch(fills)?).await
    }
}

/// Running embedded pipeline plus its input and subscription entry points.
pub struct PipelineHarness {
    db: Arc<LaminarDB>,
    inputs: EngineInputs,
    pipeline_sql: Arc<str>,
}

#[derive(Clone)]
/// Read-only health handle suitable for the HTTP bridge.
pub struct EngineMonitor {
    db: Arc<LaminarDB>,
}

#[derive(Debug, Serialize)]
/// Compact embedded-engine health snapshot.
pub struct EngineHealth {
    /// LaminarDB lifecycle state.
    pub state: &'static str,
    /// Last terminal engine fault, when present.
    pub fault: Option<String>,
    /// Current global event-time watermark in milliseconds.
    pub pipeline_watermark_ms: Option<i64>,
    /// Total source rows ingested by the engine.
    pub events_ingested: u64,
    /// Total rows emitted across engine streams.
    pub events_emitted: u64,
}

impl EngineMonitor {
    /// Capture a point-in-time engine health snapshot.
    #[must_use]
    pub fn snapshot(&self) -> EngineHealth {
        let metrics = self.db.metrics();
        let watermark = self.db.pipeline_watermark();
        EngineHealth {
            state: self.db.pipeline_state(),
            fault: self.db.last_fault(),
            pipeline_watermark_ms: (watermark != i64::MIN).then_some(watermark),
            events_ingested: metrics.total_events_ingested,
            events_emitted: metrics.total_events_emitted,
        }
    }
}

impl PipelineHarness {
    /// Build LaminarDB, connect the live quote source, and start `pipeline.sql`.
    ///
    /// # Errors
    /// Returns startup, connector-admission, SQL planning, or runtime-launch failures.
    pub async fn start(
        feed_url: &str,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self> {
        let pipeline_sql = render_pipeline_sql(feed_url, connect_timeout, read_timeout)?;
        let fill_channel = ChannelRegistration::new(FILL_CONNECTOR, fill_schema());
        let inputs = EngineInputs {
            fills: fill_channel.input.clone(),
        };
        let db = LaminarDB::builder()
            .buffer_size(8_192)
            .temporal_join_idle_history_retention(Duration::from_secs(180))
            .register_connector(move |registry| {
                register_live_feed(registry)?;
                fill_channel.register(registry)
            })
            .build()
            .await
            .context("build embedded LaminarDB")?;
        db.execute(&pipeline_sql)
            .await
            .context("execute Markout Lab pipeline.sql")?;
        db.start().await.context("start Markout Lab pipeline")?;
        Ok(Self {
            db,
            inputs,
            pipeline_sql: Arc::from(pipeline_sql),
        })
    }

    /// Clone the bounded simulated-fill input handle.
    #[must_use]
    pub fn inputs(&self) -> EngineInputs {
        self.inputs.clone()
    }

    /// Return the exact rendered SQL executed by this engine instance.
    #[must_use]
    pub fn pipeline_sql(&self) -> Arc<str> {
        Arc::clone(&self.pipeline_sql)
    }

    /// Return a cloneable read-only engine monitor.
    #[must_use]
    pub fn monitor(&self) -> EngineMonitor {
        EngineMonitor {
            db: Arc::clone(&self.db),
        }
    }

    /// Subscribe to SQL-produced market presentation rows.
    ///
    /// # Errors
    /// Returns an error when the named engine stream cannot be subscribed.
    pub async fn market_subscription(&self) -> Result<TypedSubscription<MarketEvent>> {
        self.db
            .subscribe("market_events")
            .await
            .context("subscribe to market_events")
    }

    /// Subscribe to SQL-produced fill presentation rows.
    ///
    /// # Errors
    /// Returns an error when the named engine stream cannot be subscribed.
    pub async fn fill_subscription(&self) -> Result<TypedSubscription<FillEvent>> {
        self.db
            .subscribe("fill_output")
            .await
            .context("subscribe to fill_output")
    }

    /// Subscribe to long-form temporal markout rows.
    ///
    /// # Errors
    /// Returns an error when the named engine stream cannot be subscribed.
    pub async fn markout_subscription(&self) -> Result<TypedSubscription<MarkoutEvent>> {
        self.db
            .subscribe("markout_events")
            .await
            .context("subscribe to markout_events")
    }

    /// Subscribe to engine-produced strategy/horizon curve rows.
    ///
    /// # Errors
    /// Returns an error when the named engine stream cannot be subscribed.
    pub async fn curve_subscription(&self) -> Result<TypedSubscription<CurveEvent>> {
        self.db
            .subscribe("strategy_curve")
            .await
            .context("subscribe to strategy_curve")
    }

    /// Subscribe to engine-produced dashboard KPI rows.
    ///
    /// # Errors
    /// Returns an error when the named engine stream cannot be subscribed.
    pub async fn summary_subscription(&self) -> Result<TypedSubscription<SummaryEvent>> {
        self.db
            .subscribe("dashboard_summary")
            .await
            .context("subscribe to dashboard_summary")
    }

    /// Shut down the embedded pipeline and its connector tasks.
    ///
    /// # Errors
    /// Returns an error when LaminarDB cannot complete shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.db.shutdown().await.context("shut down LaminarDB")
    }
}

struct ChannelRegistration {
    name: &'static str,
    schema: SchemaRef,
    input: BatchInput,
    receiver: Arc<Mutex<Option<mpsc::Receiver<RecordBatch>>>>,
}

impl ChannelRegistration {
    fn new(name: &'static str, schema: SchemaRef) -> Self {
        let (sender, receiver) = mpsc::channel(SOURCE_QUEUE_CAPACITY);
        let notify = Arc::new(Notify::new());
        Self {
            name,
            schema,
            input: BatchInput {
                sender,
                notify: Arc::clone(&notify),
            },
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }

    fn register(self, registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
        let name = self.name;
        let schema = Arc::clone(&self.schema);
        let receiver = Arc::clone(&self.receiver);
        let notify = Arc::clone(&self.input.notify);
        registry.register_source(
            name,
            ConnectorInfo {
                name: name.to_string(),
                display_name: format!("Markout Lab {name} input"),
                version: env!("CARGO_PKG_VERSION").to_string(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_| {
                Ok(Box::new(PositionedChannelSource {
                    name,
                    schema: Arc::clone(&schema),
                    receiver_slot: Arc::clone(&receiver),
                    receiver: None,
                    notify: Arc::clone(&notify),
                    next_position: 0,
                }))
            }),
        )
    }
}

struct PositionedChannelSource {
    name: &'static str,
    schema: SchemaRef,
    receiver_slot: Arc<Mutex<Option<mpsc::Receiver<RecordBatch>>>>,
    receiver: Option<mpsc::Receiver<RecordBatch>>,
    notify: Arc<Notify>,
    next_position: u64,
}

impl PositionedChannelSource {
    fn row_positions(&self, rows: usize) -> Result<SourceRowPositions, ConnectorError> {
        let mut partitions = BinaryBuilder::with_capacity(rows, self.name.len() * rows);
        let mut order_keys = BinaryBuilder::with_capacity(rows, 8 * rows);
        for row in 0..rows {
            let row = u64::try_from(row)
                .map_err(|_| ConnectorError::Internal("batch row exceeds u64".into()))?;
            let position = self
                .next_position
                .checked_add(row)
                .ok_or_else(|| ConnectorError::Internal("source position overflow".into()))?;
            partitions.append_value(self.name.as_bytes());
            order_keys.append_value(position.to_be_bytes());
        }
        SourceRowPositions::try_new(
            partitions.finish(),
            order_keys.finish(),
            UInt32Array::from(vec![0; rows]),
        )
    }

    fn source_checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("position", self.next_position.to_string());
        checkpoint.set_metadata("connector", self.name);
        checkpoint
            .set_input_channels(vec![self.name.as_bytes().to_vec()])
            .expect("the static Markout Lab input channel is valid");
        checkpoint
    }
}

#[async_trait]
impl SourceConnector for PositionedChannelSource {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (_config, position, _delivery) = request.into_parts();
        if !matches!(position, SourcePosition::Initial) {
            return Err(ConnectorError::ConfigurationError(format!(
                "ephemeral Markout Lab source '{}' cannot resume",
                self.name
            )));
        }
        let receiver =
            self.receiver_slot
                .lock()
                .take()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "one active Markout Lab source generation".into(),
                    actual: format!("duplicate '{}' generation", self.name),
                })?;
        self.receiver = Some(receiver);
        self.next_position = 0;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "started Markout Lab source".into(),
                actual: "source was polled before start".into(),
            })?;
        let batch = match receiver.try_recv() {
            Ok(batch) => batch,
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                return Ok(None);
            }
        };
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "{} input batch does not match its declared schema",
                self.name
            )));
        }
        if batch.num_rows() > max_records {
            return Err(ConnectorError::ReadError(format!(
                "{} input batch has {} rows, above the runtime target of {max_records}",
                self.name,
                batch.num_rows()
            )));
        }
        let rows = batch.num_rows();
        let positions = self.row_positions(rows)?;
        self.next_position = self
            .next_position
            .checked_add(
                u64::try_from(rows)
                    .map_err(|_| ConnectorError::Internal("batch size exceeds u64".into()))?,
            )
            .ok_or_else(|| ConnectorError::Internal("source position overflow".into()))?;
        Ok(Some(SourceBatch::positioned(batch, positions)?))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.source_checkpoint()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.receiver = None;
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.notify))
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_url_rendering_changes_only_the_configured_endpoint() {
        let rendered = render_pipeline_sql(
            "ws://127.0.0.1:9000/feed",
            Duration::from_secs(2),
            Duration::from_secs(3),
        )
        .unwrap();
        assert!(rendered.contains("url = 'ws://127.0.0.1:9000/feed'"));
        assert!(rendered.contains("'connect.timeout.ms' = '2000'"));
        assert!(rendered.contains("'read.timeout.ms' = '3000'"));
        assert!(!rendered.contains(DEFAULT_FEED_URL));
    }

    #[test]
    fn feed_url_rendering_rejects_sql_delimiters() {
        assert!(render_pipeline_sql(
            "ws://example.test/'bad",
            Duration::from_secs(2),
            Duration::from_secs(3),
        )
        .is_err());
    }
}
