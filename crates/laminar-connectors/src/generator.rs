//! Synthetic data generator source — no external infrastructure.
//!
//! Emits a deterministic sequence at a configured rate: row `i` is
//! always `(seq = i, ts_ms = i * 1000 / rows_per_second, value = "v{i}")`,
//! so output is a pure function of the offset. That makes the source
//! fully replayable (exactly-once capable) and lets harnesses verify
//! sink completeness by recomputing the expected rows — its primary
//! consumer is the cluster soak test, but it works anywhere a
//! self-driving source is needed (demos, benchmarks).

use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConfigKeySpec, ConnectorConfig, ConnectorInfo};
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;
use crate::registry::ConnectorRegistry;

const GENERATOR_CHECKPOINT_CONNECTOR: &str = "generator";
const CHECKPOINT_VERSION_METADATA: &str = "checkpoint.version";
const GENERATOR_CHECKPOINT_VERSION: &str = "2";

fn generator_input_channel(source_name: &str) -> Result<Vec<u8>, ConnectorError> {
    let source_len = u32::try_from(source_name.len()).map_err(|_| {
        ConnectorError::ConfigurationError(
            "generator source identity exceeds the input-channel encoding limit".into(),
        )
    })?;
    let mut channel = Vec::with_capacity(source_name.len() + 8);
    channel.extend_from_slice(&source_len.to_be_bytes());
    channel.extend_from_slice(source_name.as_bytes());
    channel.extend_from_slice(&0_u32.to_be_bytes());
    Ok(channel)
}

fn validate_generator_checkpoint(checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
    match checkpoint.get_metadata("connector") {
        Some(GENERATOR_CHECKPOINT_CONNECTOR) => {}
        Some(connector) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "generator checkpoint belongs to connector '{connector}'"
            )));
        }
        None => {
            return Err(ConnectorError::ConfigurationError(
                "generator checkpoint is missing connector identity".into(),
            ));
        }
    }
    if checkpoint.get_metadata(CHECKPOINT_VERSION_METADATA) != Some(GENERATOR_CHECKPOINT_VERSION) {
        return Err(ConnectorError::ConfigurationError(format!(
            "generator checkpoint requires {CHECKPOINT_VERSION_METADATA}={GENERATOR_CHECKPOINT_VERSION}"
        )));
    }
    Ok(())
}

/// Deterministic rate-limited source. See module docs.
pub struct GeneratorSource {
    schema: SchemaRef,
    rows_per_second: u64,
    batch_max: usize,
    max_rows: Option<u64>,
    input_channel: Vec<u8>,
    /// Next sequence number to emit (== rows emitted so far across
    /// restarts; restored from the checkpoint).
    next_seq: u64,
    /// Rate-limit anchor: emission is allowed up to
    /// `anchor_seq + elapsed_since(anchor) * rows_per_second`.
    /// Created only after `start` installs the initial or recovered sequence,
    /// so a restart cannot burst or stall against a stale anchor.
    anchor: Option<(Instant, u64)>,
}

impl GeneratorSource {
    fn generator_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("ts_ms", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    /// Build rows `[start, start + n)` — a pure function of `start`,
    /// which is what makes the source replayable.
    // Cast lints: seq is a monotonic generator counter and rates are
    // operator-supplied config — both far below the 2^52/2^63 edges.
    #[allow(
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss
    )]
    fn build_batch(&self, start: u64, n: usize) -> Result<RecordBatch, ConnectorError> {
        let seqs: Vec<i64> = (0..n as u64).map(|i| (start + i) as i64).collect();
        let ts: Vec<i64> = seqs
            .iter()
            .map(|&s| {
                (s as u64)
                    .saturating_mul(1000)
                    .wrapping_div(self.rows_per_second) as i64
            })
            .collect();
        let values: Vec<String> = seqs.iter().map(|s| format!("v{s}")).collect();
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(seqs)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .map_err(|e| ConnectorError::ReadError(e.to_string()))
    }

    fn build_row_positions(
        &self,
        start: u64,
        n: usize,
    ) -> Result<SourceRowPositions, ConnectorError> {
        let mut partitions =
            BinaryBuilder::with_capacity(n, self.input_channel.len().saturating_mul(n));
        let mut order_keys = BinaryBuilder::with_capacity(n, 8_usize.saturating_mul(n));
        for row in 0..n {
            let sequence = start
                .checked_add(u64::try_from(row).map_err(|_| {
                    ConnectorError::Internal("generator batch row index exceeds u64".into())
                })?)
                .ok_or_else(|| ConnectorError::Internal("generator sequence overflow".into()))?;
            partitions.append_value(&self.input_channel);
            order_keys.append_value(sequence.to_be_bytes());
        }
        SourceRowPositions::try_new(
            partitions.finish(),
            order_keys.finish(),
            UInt32Array::from(vec![0; n]),
        )
    }
}

impl Default for GeneratorSource {
    fn default() -> Self {
        let input_channel = generator_input_channel("generator")
            .expect("the built-in generator source identity is valid");
        Self {
            schema: Self::generator_schema(),
            rows_per_second: 1000,
            batch_max: 1024,
            max_rows: None,
            input_channel,
            next_seq: 0,
            anchor: None,
        }
    }
}

#[async_trait]
impl SourceConnector for GeneratorSource {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_exact_delivery_certification())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        let source_name = config
            .get("laminar.source.name")
            .filter(|name| !name.is_empty())
            .unwrap_or("generator");
        self.input_channel = generator_input_channel(source_name)?;
        if let Some(rps) = config.get_parsed::<u64>("rows.per.second")? {
            if rps == 0 {
                return Err(ConnectorError::ConfigurationError(
                    "rows.per.second must be > 0".into(),
                ));
            }
            self.rows_per_second = rps;
        }
        if let Some(n) = config.get_parsed::<usize>("batch.max.size")? {
            self.batch_max = n.max(1);
        }
        self.max_rows = config.get_parsed::<u64>("max.rows")?;

        self.next_seq = match position {
            SourcePosition::Initial => 0,
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                validate_generator_checkpoint(&checkpoint)?;
                if checkpoint.input_channels() != Some(std::slice::from_ref(&self.input_channel)) {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "generator checkpoint {attempt:?} has the wrong input-channel inventory"
                    )));
                }
                let seq = checkpoint.get_offset("seq").ok_or_else(|| {
                    ConnectorError::ConfigurationError(format!(
                        "generator checkpoint {attempt:?} is missing required 'seq' offset"
                    ))
                })?;
                seq.parse().map_err(|e| {
                    ConnectorError::ConfigurationError(format!(
                        "bad generator offset '{seq}' in checkpoint {attempt:?}: {e}"
                    ))
                })?
            }
        };
        // Position must be installed before the rate anchor is created. Otherwise a
        // resumed source can accrue tokens against the pre-resume sequence and burst.
        self.anchor = None;
        Ok(())
    }

    // Cast lints: see build_batch.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let (anchored_at, anchor_seq) = *self
            .anchor
            .get_or_insert_with(|| (Instant::now(), self.next_seq));
        let mut allowed = anchor_seq.saturating_add(
            (anchored_at.elapsed().as_secs_f64() * self.rows_per_second as f64) as u64,
        );
        if let Some(max) = self.max_rows {
            allowed = allowed.min(max);
        }
        let pending = allowed.saturating_sub(self.next_seq);
        let n = (pending as usize).min(max_records).min(self.batch_max);
        if n == 0 {
            return Ok(None);
        }
        let batch = self.build_batch(self.next_seq, n)?;
        let row_positions = self.build_row_positions(self.next_seq, n)?;
        self.next_seq =
            self.next_seq
                .checked_add(u64::try_from(n).map_err(|_| {
                    ConnectorError::Internal("generator batch size exceeds u64".into())
                })?)
                .ok_or_else(|| ConnectorError::Internal("generator sequence overflow".into()))?;
        Ok(Some(SourceBatch::positioned(batch, row_positions)?))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("seq", self.next_seq.to_string());
        cp.set_metadata("connector", GENERATOR_CHECKPOINT_CONNECTOR);
        cp.set_metadata(CHECKPOINT_VERSION_METADATA, GENERATOR_CHECKPOINT_VERSION);
        cp.set_input_channels(vec![self.input_channel.clone()])
            .expect("the cached generator input-channel identity is valid");
        cp
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Registers the generator source so `CREATE SOURCE ... FROM GENERATOR (...)` resolves.
///
/// # Errors
///
/// Returns the registry error when the name is already registered or the registry is frozen.
pub fn register_generator_source(registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
    let info = ConnectorInfo {
        name: "generator".to_string(),
        display_name: "Synthetic Data Generator".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: vec![
            ConfigKeySpec::optional("rows.per.second", "Emission rate", "1000"),
            ConfigKeySpec::optional("batch.max.size", "Max rows per batch", "1024"),
            ConfigKeySpec::optional(
                "max.rows",
                "Stop after this many rows (unbounded if unset)",
                "",
            ),
        ],
    };
    registry.register_source(
        "generator",
        info,
        Arc::new(|_registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(GeneratorSource::default()))
        }),
    )
}

#[cfg(test)]
mod tests;
