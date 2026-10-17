//! FpML messages: one complete document per Kafka message, validated and projected on the FpML
//! decoder's worker thread behind a tracked blocking task, never on the async runtime.
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use arrow_array::RecordBatch;

use super::{
    terminalize_guaranteed_poll_error, warn, ConnectorError, DeliveryGuarantee, KafkaSource,
    SerdeError,
};
use crate::config::ConnectorConfig;
use crate::fpml::{message, FpmlDecoder};
use crate::schema::resolution::SchemaBinding;
use crate::schema::traits::FormatDecoder;

impl KafkaSource {
    /// Start the decoder for the closure and profile the committed binding names.
    pub(super) async fn start_fpml(
        &mut self,
        config: &ConnectorConfig,
        binding: Option<&SchemaBinding>,
    ) -> Result<(), ConnectorError> {
        if self.vnode_assignment.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "format 'fpml' is not qualified for cluster Kafka sources".into(),
            ));
        }
        let guard = self
            .task_owner
            .track()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "live Kafka task owner".into(),
                actual: "retired".into(),
            })?;
        let decoder = message::start_decoder(
            config,
            binding,
            "Kafka",
            self.metrics_registry.as_ref(),
            guard,
        )
        .await?;
        self.fpml = Some(Arc::new(decoder));
        Ok(())
    }

    pub(super) async fn decode_fpml_payloads(
        &mut self,
        decoder: Arc<FpmlDecoder>,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ConnectorError> {
        let payloads: Vec<Vec<u8>> = self
            .poll_payload_offsets
            .iter()
            .map(|&(start, len)| self.poll_payload_buf[start..start + len].to_vec())
            .collect();
        let total = payloads.len();
        let schema = decoder.output_schema();
        let results = self
            .blocking_tasks
            .run(move || {
                payloads
                    .iter()
                    .map(|payload| decoder.decode(payload))
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| ConnectorError::Internal(format!("FpML decode task: {error}")))?;
        let mut batches = Vec::with_capacity(total);
        let mut good = Vec::with_capacity(total);
        let mut errors = Vec::new();
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(batch) if batch.num_rows() == 1 => {
                    batches.push(batch);
                    good.push(index);
                }
                Ok(batch) => errors.push(SerdeError::RecordCountMismatch {
                    expected: 1,
                    got: batch.num_rows(),
                }),
                Err(rejection) => errors.push(SerdeError::MalformedInput(rejection.to_string())),
            }
        }
        let error_count = errors.len();
        let Some(first) = errors.into_iter().next() else {
            let batch =
                arrow_select::concat::concat_batches(&schema, &batches).map_err(|error| {
                    ConnectorError::Internal(format!("failed to concat FpML rows: {error}"))
                })?;
            return Ok((batch, None));
        };
        if self.delivery != DeliveryGuarantee::BestEffort {
            return Err(terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                ConnectorError::Serde(first),
            ));
        }
        for _ in 0..error_count {
            self.metrics.record_error();
        }
        warn!(
            rejected = error_count,
            total, "skipping rejected FpML messages"
        );
        self.isolate_poison_pills(&batches, good, error_count, total, first)
    }
}
