use std::sync::Arc;

use arrow_array::{BinaryArray, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConfigKeySpec, ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::registry::ConnectorRegistry;

/// Connector name shared by temporal runtime tests.
pub(crate) const CONNECTOR_NAME: &str = "temporal-positioned-test";

/// Release handle that keeps test sources idle until observers are attached.
#[derive(Clone)]
pub(crate) struct TemporalTestSourceControl {
    ready: tokio::sync::watch::Sender<bool>,
}

impl TemporalTestSourceControl {
    /// Create a closed source gate.
    pub(crate) fn new() -> Self {
        let (ready, _) = tokio::sync::watch::channel(false);
        Self { ready }
    }

    /// Allow every connector instance to emit its deterministic batch.
    pub(crate) fn release(&self) {
        let _ = self.ready.send(true);
    }
}

/// Fixed schema emitted by the connector and declared in its DDL fixtures.
pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Register the deterministic source with a shared release control.
pub(crate) fn register(
    registry: &ConnectorRegistry,
    control: &TemporalTestSourceControl,
) -> Result<(), ConnectorError> {
    let ready = control.ready.subscribe();
    registry.register_source(
        CONNECTOR_NAME,
        ConnectorInfo {
            name: CONNECTOR_NAME.into(),
            display_name: "Temporal positioned test source".into(),
            version: "1".into(),
            is_source: true,
            is_sink: false,
            config_keys: vec![ConfigKeySpec::optional(
                "mode",
                "append or keyed-upsert test input",
                "append",
            )],
        },
        Arc::new(move |_| Ok(Box::new(TemporalTestSource::new(ready.clone())))),
    )
}

#[derive(Clone, Copy)]
enum TestInputMode {
    Append,
    Upsert,
}

impl TestInputMode {
    fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        match config.get("mode").unwrap_or("append") {
            "append" => Ok(Self::Append),
            "upsert" => Ok(Self::Upsert),
            mode => Err(ConnectorError::ConfigurationError(format!(
                "temporal test source mode must be 'append' or 'upsert', found '{mode}'"
            ))),
        }
    }

    const fn contract(self) -> SourceInputMode {
        match self {
            Self::Append => SourceInputMode::AppendOnly,
            Self::Upsert => SourceInputMode::KeyedUpsert,
        }
    }
}

struct TemporalTestSource {
    schema: SchemaRef,
    mode: TestInputMode,
    source_identity: Vec<u8>,
    emitted: bool,
    ready: tokio::sync::watch::Receiver<bool>,
}

impl TemporalTestSource {
    fn new(ready: tokio::sync::watch::Receiver<bool>) -> Self {
        Self {
            schema: schema(),
            mode: TestInputMode::Append,
            source_identity: CONNECTOR_NAME.as_bytes().to_vec(),
            emitted: false,
            ready,
        }
    }

    fn batch(&self) -> Result<SourceBatch, ConnectorError> {
        let (ids, times, values) = match self.mode {
            TestInputMode::Append => (vec![1], vec![2_000], vec![10]),
            TestInputMode::Upsert => (vec![1, 1], vec![1_000, 3_000], vec![7, 9]),
        };
        let rows = ids.len();
        let records = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(TimestampMicrosecondArray::from(times)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .map_err(|error| ConnectorError::ReadError(error.to_string()))?;
        let order_values = (0..rows)
            .map(|row| u64::try_from(row).unwrap().to_be_bytes())
            .collect::<Vec<_>>();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from(vec![self.source_identity.as_slice(); rows]),
            BinaryArray::from(
                order_values
                    .iter()
                    .map(<[u8; 8]>::as_slice)
                    .collect::<Vec<_>>(),
            ),
            UInt32Array::from(vec![0; rows]),
        )?;
        SourceBatch::positioned(records, positions)
    }
}

#[async_trait]
impl SourceConnector for TemporalTestSource {
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            TestInputMode::from_config(config)?.contract(),
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        self.mode = TestInputMode::from_config(&config)?;
        self.schema = config.arrow_schema().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "temporal test source requires the admitted Arrow schema".into(),
            )
        })?;
        if self.schema.as_ref() != schema().as_ref() {
            return Err(ConnectorError::SchemaMismatch(
                "temporal test source schema differs from its fixed test schema".into(),
            ));
        }
        self.source_identity = config
            .get("laminar.source.name")
            .unwrap_or(CONNECTOR_NAME)
            .as_bytes()
            .to_vec();
        self.emitted = match position {
            SourcePosition::Initial => false,
            SourcePosition::Resume { checkpoint, .. } => match checkpoint.get_offset("emitted") {
                Some("0") => false,
                Some("1") => true,
                _ => {
                    return Err(ConnectorError::ConfigurationError(
                        "temporal test checkpoint has no valid emitted cursor".into(),
                    ));
                }
            },
        };
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.emitted {
            return Ok(None);
        }
        while !*self.ready.borrow() {
            self.ready
                .changed()
                .await
                .map_err(|_| ConnectorError::InvalidState {
                    expected: "a live temporal test release control".into(),
                    actual: "the release control was dropped".into(),
                })?;
        }
        let batch = self.batch()?;
        self.emitted = true;
        Ok(Some(batch))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("emitted", if self.emitted { "1" } else { "0" });
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
