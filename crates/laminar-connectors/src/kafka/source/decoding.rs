//! Payload decoding, Debezium normalization, and metadata column assembly.

use super::{
    info, kafka_output_schema, terminalize_guaranteed_poll_error, warn, Arc, AvroDeserializer,
    CompatibilityMode, ConnectorError, DeliveryGuarantee, EvolutionVerdict, KafkaSource,
    SchemaEvolution, SchemaEvolutionStrategy, SerdeError,
};

impl KafkaSource {
    pub(super) async fn decode_polled_payloads(
        &mut self,
    ) -> Result<(arrow_array::RecordBatch, Option<Vec<usize>>), ConnectorError> {
        self.resolve_polled_avro_schemas().await?;
        self.deserialize_polled_payloads()
    }

    async fn resolve_polled_avro_schemas(&mut self) -> Result<(), ConnectorError> {
        // Resolve Avro schemas from Schema Registry before deserialization.
        // Also detect schema evolution when new schema IDs appear.
        if let Some(avro_deser) = self
            .deserializer
            .as_any_mut()
            .and_then(|any| any.downcast_mut::<AvroDeserializer>())
        {
            let mut new_schema_ids = Vec::new();
            for &(start, len) in &self.poll_payload_offsets {
                if let Some(schema_id) = AvroDeserializer::extract_confluent_id(
                    &self.poll_payload_buf[start..start + len],
                ) {
                    let is_new = avro_deser
                        .ensure_schema_registered(schema_id)
                        .await
                        .map_err(|error| {
                            terminalize_guaranteed_poll_error(
                                self.delivery,
                                &mut self.state,
                                &self.metrics,
                                self.reader_shutdown.as_ref(),
                                error,
                            )
                        })?;
                    if is_new {
                        new_schema_ids.push(schema_id);
                    }
                }
            }

            // Detect schema evolution by diffing successive writer schemas.
            if !new_schema_ids.is_empty()
                && self.config.schema_evolution_strategy != SchemaEvolutionStrategy::Ignore
            {
                if let Some(ref sr) = self.schema_registry {
                    let compat = self
                        .config
                        .schema_compatibility
                        .map_or(CompatibilityMode::Backward, CompatibilityMode::from);
                    let evolver = SchemaEvolution::new(compat);

                    for id in new_schema_ids {
                        let cached = sr.resolve_confluent_id(id).await.map_err(|error| {
                            terminalize_guaranteed_poll_error(
                                self.delivery,
                                &mut self.state,
                                &self.metrics,
                                self.reader_shutdown.as_ref(),
                                error,
                            )
                        })?;

                        let Some(ref prev) = self.last_avro_schema else {
                            // First schema — establish baseline, nothing to diff.
                            info!(schema_id = id, "initial Avro schema registered");
                            self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));
                            continue;
                        };

                        let changes = evolver.diff_schemas(prev, &cached.arrow_schema);
                        self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));

                        if changes.is_empty() {
                            info!(
                                schema_id = id,
                                "new Avro schema ID registered, no field changes"
                            );
                            continue;
                        }
                        let verdict = evolver.evaluate_evolution(&changes);
                        match &verdict {
                            EvolutionVerdict::Compatible => {
                                info!(schema_id = id, ?changes, "schema evolved (compatible)");
                            }
                            EvolutionVerdict::RequiresMigration => {
                                warn!(
                                    schema_id = id,
                                    ?changes,
                                    "schema evolved (requires migration)"
                                );
                            }
                            EvolutionVerdict::Incompatible(reason) => {
                                if self.config.schema_evolution_strategy
                                    == SchemaEvolutionStrategy::Reject
                                {
                                    let error = ConnectorError::SchemaMismatch(format!(
                                        "incompatible schema evolution for ID {id}: {reason}"
                                    ));
                                    return Err(terminalize_guaranteed_poll_error(
                                        self.delivery,
                                        &mut self.state,
                                        &self.metrics,
                                        self.reader_shutdown.as_ref(),
                                        error,
                                    ));
                                }
                                warn!(
                                    schema_id = id, %reason, ?changes,
                                    "incompatible schema evolution detected"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn deserialize_polled_payloads(
        &mut self,
    ) -> Result<(arrow_array::RecordBatch, Option<Vec<usize>>), ConnectorError> {
        let refs: Vec<&[u8]> = self
            .poll_payload_offsets
            .iter()
            .map(|&(start, len)| &self.poll_payload_buf[start..start + len])
            .collect();

        // Try batch deserialization first (fast path). If it fails, fall back
        // to per-record deserialization to isolate poison pills.
        let (batch, good_indices) = match self.deserializer.deserialize_batch(&refs, &self.schema) {
            Ok(batch) => (batch, None),
            Err(batch_err) if self.delivery != DeliveryGuarantee::BestEffort => {
                // Without a checkpoint-coupled dead-letter path, skipping even one input would
                // let a later checkpoint seal a cursor beyond data that was never emitted. Stop
                // this connector generation so recovery must restart from its durable cursor.
                return Err(terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    ConnectorError::Serde(batch_err),
                ));
            }
            Err(batch_err) => {
                // Best-effort-only fallback: deserialize one at a time, collect successful
                // batches directly (avoids double-deserialization).
                // Track indices of successful records so metadata vectors can
                // be filtered to match the reduced row count.
                let mut good_batches = Vec::with_capacity(refs.len());
                let mut good_idx = Vec::with_capacity(refs.len());
                let mut error_count = 0usize;
                for (i, r) in refs.iter().enumerate() {
                    match self
                        .deserializer
                        .deserialize_batch(std::slice::from_ref(r), &self.schema)
                    {
                        Ok(batch) => {
                            good_batches.push(batch);
                            good_idx.push(i);
                        }
                        Err(e) => {
                            error_count += 1;
                            self.metrics.record_error();
                            warn!(error = %e, "skipping poison pill record");
                        }
                    }
                }
                if good_batches.is_empty() {
                    return Err(ConnectorError::Serde(batch_err));
                }
                // Escalate if the error rate exceeds the configured threshold.
                if error_count > 0 {
                    let error_count = u32::try_from(error_count).map_err(|_| {
                        ConnectorError::Internal(
                            "Kafka deserialization error count exceeds u32".into(),
                        )
                    })?;
                    let record_count = u32::try_from(refs.len()).map_err(|_| {
                        ConnectorError::Internal("Kafka batch record count exceeds u32".into())
                    })?;
                    let error_rate = f64::from(error_count) / f64::from(record_count);
                    if error_rate > self.config.max_deser_error_rate {
                        return Err(ConnectorError::Serde(batch_err));
                    }
                    warn!(
                        skipped = error_count,
                        total = refs.len(),
                        error_rate = %format_args!("{error_rate:.1}"),
                        "deserialized batch with poison pill isolation"
                    );
                }
                let concat_schema = good_batches[0].schema();
                let batch = arrow_select::concat::concat_batches(&concat_schema, &good_batches)
                    .map_err(|e| {
                        ConnectorError::Internal(format!("failed to concat batches: {e}"))
                    })?;
                (batch, Some(good_idx))
            }
        };

        // Kafka source formats map one broker message to one row. A short successful decode is a
        // silent drop unless it is rejected before the message offsets become checkpointable.
        let expected_rows = good_indices.as_ref().map_or(refs.len(), Vec::len);
        if batch.num_rows() != expected_rows {
            let error = ConnectorError::Serde(SerdeError::RecordCountMismatch {
                expected: expected_rows,
                got: batch.num_rows(),
            });
            return Err(terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            ));
        }

        Ok((batch, good_indices))
    }
}

impl KafkaSource {
    pub(super) fn append_metadata_columns(
        &mut self,
        batch: arrow_array::RecordBatch,
        good_indices: Option<&[usize]>,
        include_metadata: bool,
        include_headers: bool,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        // If poison pill fallback filtered records, also filter metadata
        // vectors so their lengths match the deserialized batch row count.
        if let Some(idx) = good_indices {
            if include_metadata {
                self.poll_meta_partitions =
                    idx.iter().map(|&i| self.poll_meta_partitions[i]).collect();
                self.poll_meta_offsets = idx.iter().map(|&i| self.poll_meta_offsets[i]).collect();
                self.poll_meta_timestamps =
                    idx.iter().map(|&i| self.poll_meta_timestamps[i]).collect();
            }
            if include_headers {
                self.poll_meta_headers = idx
                    .iter()
                    .map(|&i| std::mem::take(&mut self.poll_meta_headers[i]))
                    .collect();
            }
        }

        let rows = batch.num_rows();
        let metadata_aligned = !include_metadata
            || (self.poll_meta_partitions.len() == rows
                && self.poll_meta_offsets.len() == rows
                && self.poll_meta_timestamps.len() == rows);
        let headers_aligned = !include_headers || self.poll_meta_headers.len() == rows;
        if !metadata_aligned || !headers_aligned {
            return Err(terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                ConnectorError::Internal(
                    "Kafka connector metadata is not aligned with the decoded rows".into(),
                ),
            ));
        }

        let batch = if include_metadata || include_headers {
            let output_schema =
                kafka_output_schema(&batch.schema(), include_metadata, include_headers);
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();

            if include_metadata {
                use arrow_array::{Int32Array, Int64Array, TimestampMillisecondArray};
                columns.push(Arc::new(Int32Array::from(std::mem::take(
                    &mut self.poll_meta_partitions,
                ))));
                columns.push(Arc::new(Int64Array::from(std::mem::take(
                    &mut self.poll_meta_offsets,
                ))));
                columns.push(Arc::new(TimestampMillisecondArray::from(std::mem::take(
                    &mut self.poll_meta_timestamps,
                ))));
            }
            if include_headers {
                columns.push(Arc::new(arrow_array::StringArray::from(std::mem::take(
                    &mut self.poll_meta_headers,
                ))));
            }

            arrow_array::RecordBatch::try_new(output_schema, columns).map_err(|e| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    ConnectorError::Internal(format!("failed to append metadata columns: {e}")),
                )
            })?
        } else {
            batch
        };

        Ok(batch)
    }
}
