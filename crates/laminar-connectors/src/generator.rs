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

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConfigKeySpec, ConnectorConfig, ConnectorInfo};
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::ConnectorError;
use crate::registry::ConnectorRegistry;

/// Deterministic rate-limited source. See module docs.
pub struct GeneratorSource {
    schema: SchemaRef,
    rows_per_second: u64,
    batch_max: usize,
    max_rows: Option<u64>,
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
}

impl Default for GeneratorSource {
    fn default() -> Self {
        Self {
            schema: Self::generator_schema(),
            rows_per_second: 1000,
            batch_max: 1024,
            max_rows: None,
            next_seq: 0,
            anchor: None,
        }
    }
}

#[async_trait]
impl SourceConnector for GeneratorSource {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(
            SourceContract::new(SourceConsistency::Replayable, SourceTopology::Singleton)
                .with_exact_delivery_certification(),
        )
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let SourceStart {
            config, position, ..
        } = request;
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
        self.next_seq += n as u64;
        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("seq", self.next_seq.to_string());
        cp
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Registers the generator source so
/// `CREATE SOURCE ... WITH (connector = 'generator')` resolves.
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
mod tests {
    use super::*;
    use crate::connector::{DeliveryGuarantee, SourcePosition, SourceStart};
    use laminar_core::state::CheckpointAttempt;

    fn start_request(config: ConnectorConfig, position: SourcePosition) -> SourceStart {
        SourceStart {
            config,
            position,
            delivery: DeliveryGuarantee::AtLeastOnce,
        }
    }

    #[test]
    fn source_contract_is_replayable_singleton() {
        let source = GeneratorSource::default();
        let contract = source
            .contract(&ConnectorConfig::new("generator"))
            .expect("static generator contract");
        assert_eq!(contract.consistency, SourceConsistency::Replayable);
        assert_eq!(contract.topology, SourceTopology::Singleton);
        assert!(contract.is_exact_delivery_certified());
    }

    #[tokio::test]
    async fn deterministic_and_replayable_across_resume() {
        let mut config = ConnectorConfig::new("generator");
        config.set("rows.per.second", "1000000"); // effectively unthrottled
        let mut a = GeneratorSource::default();
        a.start(start_request(config.clone(), SourcePosition::Initial))
            .await
            .unwrap();
        // First poll anchors the rate limit (no tokens accrue before
        // polling starts); rows become available after it.
        let _ = a.poll_batch(8).await.unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let first = a.poll_batch(8).await.unwrap().expect("rows");
        assert_eq!(first.num_rows(), 8);

        // Restore a fresh instance from a's checkpoint at seq=8 and
        // verify the next rows equal what `a` produces next.
        let cp = a.checkpoint();
        let mut b = GeneratorSource::default();
        b.start(start_request(
            config,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(1, 1),
                checkpoint: cp,
            },
        ))
        .await
        .unwrap();
        let _ = b.poll_batch(4).await.unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let from_a = a.poll_batch(4).await.unwrap().expect("rows").records;
        let from_b = b.poll_batch(4).await.unwrap().expect("rows").records;
        assert_eq!(from_a, from_b, "replay from offset must be byte-identical");
    }

    #[tokio::test]
    async fn rate_limit_and_max_rows_bound_emission() {
        let mut config = ConnectorConfig::new("generator");
        config.set("rows.per.second", "1000");
        config.set("max.rows", "3");
        let mut g = GeneratorSource::default();
        g.start(start_request(config, SourcePosition::Initial))
            .await
            .unwrap();
        let _ = g.poll_batch(100).await.unwrap(); // anchor
        std::thread::sleep(std::time::Duration::from_millis(50)); // ~50 tokens
        let batch = g.poll_batch(100).await.unwrap().expect("rows");
        assert_eq!(batch.num_rows(), 3, "max.rows caps emission");
        assert!(g.poll_batch(100).await.unwrap().is_none(), "exhausted");
    }

    #[tokio::test]
    async fn malformed_resume_fails_before_rate_anchor() {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("seq", "not-a-sequence");
        let mut source = GeneratorSource::default();
        let error = source
            .start(start_request(
                ConnectorConfig::new("generator"),
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::new(1, 1),
                    checkpoint,
                },
            ))
            .await
            .expect_err("malformed durable cursor must fail closed");
        assert!(error.to_string().contains("bad generator offset"));
        assert!(source.anchor.is_none());
    }
}
