//! Runtime sink lifecycle and coordinated-committer protocol implementations.

use super::{
    async_trait, debug, info, warn, Arc, ConnectorConfig, ConnectorError, ConnectorState,
    ConnectorTaskTracker, DeliveryGuarantee, DeltaLakeSink, DeltaLakeSinkConfig, DeltaWriteMode,
    Duration, Instant, RecordBatch, SchemaRef, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, StorageProvider, WriteResult,
};
#[cfg(feature = "delta-lake")]
use super::{
    publish_coordinated_delta_batch, retry_delta_metadata_until, UnresolvedDeltaPublication,
    MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};

#[async_trait]
impl SinkConnector for DeltaLakeSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            DeltaLakeSinkConfig::from_config(config)?
        };
        #[cfg(feature = "delta-lake")]
        if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            super::super::delta_io::validate_coordinated_storage_preflight(
                &cfg.table_path,
                &cfg.storage_options,
            )?;
        }
        if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && cfg.write_mode != DeltaWriteMode::Append
        {
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once requires coordinated append mode".into(),
            ));
        }
        let consistency = if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && cfg.write_mode == DeltaWriteMode::Append
        {
            // Append mode emits durable data-file descriptors for one
            // designated catalog commit. Certification still requires an
            // exact external namespace/cursor, enforced by runtime admission.
            SinkConsistency::CheckpointCommittable
        } else {
            SinkConsistency::DurableAtLeastOnce
        };
        let unity_target = cfg
            .table_path
            .split_once("://")
            .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("uc"));
        let shared_target = StorageProvider::is_shared_uri(&cfg.table_path) || unity_target;
        let topology = if cfg.write_mode == DeltaWriteMode::Append && shared_target {
            SinkTopology::MultiWriter
        } else {
            // Node-local files plus overwrite/MERGE lack fenced distributed ownership.
            SinkTopology::Singleton
        };
        let input_mode = match cfg.write_mode {
            DeltaWriteMode::Append | DeltaWriteMode::Upsert => SinkInputMode::FullChangelog,
            DeltaWriteMode::Overwrite => SinkInputMode::AppendOnly,
        };
        let contract = SinkContract::new(consistency, topology, input_mode);
        let cluster_exact_target = StorageProvider::is_direct_s3_uri(&cfg.table_path);
        Ok(
            if consistency == SinkConsistency::CheckpointCommittable && cluster_exact_target {
                contract.with_cluster_exact_delivery_certification()
            } else {
                contract
            },
        )
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = DeltaLakeSinkConfig::from_config(config)?;
        }
        if config.get("_arrow_schema").is_some() {
            let schema = config.arrow_schema().ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "invalid Delta sink '_arrow_schema' encoding".into(),
                )
            })?;
            self.schema = Some(Self::target_schema(&schema, self.config.write_mode));
        }
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            super::super::delta_io::validate_coordinated_storage_preflight(
                &self.config.table_path,
                &self.config.storage_options,
            )?;
        }
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && !self.is_coordinated()
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once requires coordinated append mode".into(),
            ));
        }

        info!(
            table_path = %self.config.table_path,
            mode = %self.config.write_mode,
            guarantee = %self.config.delivery_guarantee,
            "opening Delta Lake sink connector"
        );

        // When delta-lake feature is enabled, open/create the actual table.
        // If Unity Catalog auto-create is configured but no schema is available
        // yet, defer initialization to the first write_batch() call.
        #[cfg(feature = "delta-lake")]
        {
            let should_defer = matches!(
                self.config.catalog_type,
                super::super::delta_config::DeltaCatalogType::Unity { .. }
            ) && self.config.catalog_storage_location.is_some()
                && self.schema.is_none();

            if should_defer {
                info!(
                    "Unity Catalog auto-create configured but pipeline schema not yet \
                     available — deferring Delta table init to first begin_epoch"
                );
                self.needs_deferred_delta_init = true;
                self.state = ConnectorState::Initializing;
                return Ok(());
            }

            let deadline = self.operation_deadline();
            self.init_delta_table(deadline).await?;

            // If table still has no version after init (new table, no schema yet),
            // defer full creation to the first write_batch() when schema is available.
            if self.table.as_ref().is_some_and(|t| t.version().is_none()) && self.schema.is_none() {
                self.needs_deferred_delta_init = true;
                self.state = ConnectorState::Initializing;
                return Ok(());
            }
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ConfigurationError(
                "Delta Lake sink requires the 'delta-lake' feature to be enabled. \
                 Build with: cargo build --features delta-lake"
                    .into(),
            ));
        }

        #[cfg(feature = "delta-lake")]
        {
            self.state = ConnectorState::Running;
            info!("Delta Lake sink connector opened successfully");
            Ok(())
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        // Accept both Running and Initializing (deferred init in progress).
        if self.state != ConnectorState::Running && self.state != ConnectorState::Initializing {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        // Handle schema on first write. In upsert mode, strip metadata columns
        // (_op, _ts_ms) so the Delta table isn't created with changelog columns.
        if self.schema.is_none() {
            self.schema = Some(Self::target_schema(&batch.schema(), self.config.write_mode));
        }

        // Fallback for deferred init: if begin_epoch() couldn't complete
        // init (schema was still None), complete it now that the first
        // batch provides a schema.
        #[cfg(feature = "delta-lake")]
        if self.needs_deferred_delta_init {
            info!("schema now available from first batch — completing deferred Delta table init");
            match self.init_delta_table(deadline).await {
                Ok(()) => {
                    self.needs_deferred_delta_init = false;
                    self.state = ConnectorState::Running;
                    info!("Delta Lake sink connector opened successfully (deferred)");
                }
                Err(e) => {
                    self.state = ConnectorState::Failed;
                    return Err(e);
                }
            }
        }

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // At-least-once retries retain failed staged data, so cap the combined
        // in-memory backlog. Coordinated append instead drains full buffers to
        // invisible Parquet files below and bounds only their retained Adds.
        if !self.is_coordinated() {
            let pending_rows = self.buffered_rows + self.staged_rows + num_rows;
            let pending_bytes = self.buffered_bytes + self.staged_bytes + estimated_bytes;
            let row_cap = self
                .config
                .max_buffer_records
                .saturating_mul(4)
                .max(num_rows);
            let byte_cap = (self.config.target_file_size as u64)
                .saturating_mul(4)
                .max(estimated_bytes);
            if pending_rows > row_cap || pending_bytes > byte_cap {
                return Err(ConnectorError::WriteError(format!(
                    "delta sink buffer full ({pending_rows} rows, \
                     {pending_bytes} bytes pending; cap {row_cap} rows, \
                     {byte_cap} bytes) — backpressure until next flush/commit"
                )));
            }
        }

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        #[cfg(feature = "delta-lake")]
        if self.is_coordinated() && self.should_flush() {
            self.stage_coordinated_buffer(deadline).await?;
        } else if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce
            && self.should_flush()
        {
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta(deadline).await?;
            }
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
            self.flush_staged_to_delta(deadline).await?;
        }

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn checkpoint_artifact_intent(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, ConnectorError> {
        // Delta retains unpublished files for retention-safe vacuum because a cancelled catalog
        // commit may still publish them. Abort cleanup therefore has no exact deletion payload.
        Ok(None)
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        // Complete deferred Delta table init on the first epoch.
        #[cfg(feature = "delta-lake")]
        if self.needs_deferred_delta_init {
            // Schema may not be available yet on the very first epoch.
            // If so, buffer the epoch — the write_batch will provide it.
            // But if the pipeline provided a schema via with_schema() or a
            // previous epoch's write_batch set it, complete init now.
            if self.schema.is_some() {
                info!("schema available — completing deferred Delta table init");
                match self.init_delta_table(deadline).await {
                    Ok(()) => {
                        self.needs_deferred_delta_init = false;
                        self.state = ConnectorState::Running;
                        info!("Delta Lake sink connector opened successfully (deferred)");
                    }
                    Err(e) => {
                        self.state = ConnectorState::Failed;
                        return Err(e);
                    }
                }
            }
        }

        #[cfg(feature = "delta-lake")]
        if self.is_coordinated()
            && (!self.buffer.is_empty()
                || !self.staged_batches.is_empty()
                || !self.coordinated_adds.is_empty())
        {
            return Err(ConnectorError::InvalidState {
                expected: "the previous coordinated epoch to be prepared or rolled back".into(),
                actual: "unresolved Delta staging data remains".into(),
            });
        }

        self.current_epoch = epoch;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        debug!(epoch, "Delta Lake: began epoch");
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        // Coordinated append may already have materialized several invisible
        // Parquet chunks during writes. Flush only the remainder, then return
        // one bounded descriptor for the entire checkpoint cut.
        #[cfg(feature = "delta-lake")]
        if self.is_coordinated() {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
            let deadline = self.operation_deadline();
            self.stage_coordinated_buffer(deadline).await?;
            if self.coordinated_adds.is_empty() {
                if self.coordinated_binding.is_some() {
                    return Err(ConnectorError::Internal(
                        "empty Delta coordinated cut retained a table binding".into(),
                    ));
                }
                return Ok(None);
            }

            let binding = self.coordinated_binding.as_ref().ok_or_else(|| {
                ConnectorError::Internal(
                    "non-empty Delta coordinated cut has no table binding".into(),
                )
            })?;
            let descriptor =
                super::super::delta_io::encode_commit_descriptor(binding, &self.coordinated_adds)?;
            if descriptor.len() != self.coordinated_descriptor_bytes
                || descriptor.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
            {
                return Err(ConnectorError::Internal(format!(
                    "Delta coordinated descriptor accounting mismatch: tracked {}, encoded {}",
                    self.coordinated_descriptor_bytes,
                    descriptor.len()
                )));
            }
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
            return Ok(Some(descriptor));
        }

        // Non-coordinated callers retain the existing in-memory phase-one
        // behavior; the checkpoint runtime only invokes this path for sinks
        // whose contract admits it.
        if !self.buffer.is_empty() {
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
        }

        debug!(epoch, "Delta Lake: pre-committed (batches staged)");
        Ok(None)
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered/staged data. A coordinated attempt may already have
        // materialized an unreferenced immutable Parquet file; it must be reclaimed only by a
        // retention-safe vacuum after ambiguous catalog commits are impossible, never here.
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;
        self.staged_batches.clear();
        self.staged_rows = 0;
        self.staged_bytes = 0;
        #[cfg(feature = "delta-lake")]
        {
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
        }

        self.metrics.record_rollback();
        warn!(epoch, "Delta Lake: rolled back epoch");
        Ok(())
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config.write_timeout
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        // For at-least-once delivery, flush() is the commit trigger. Write
        // directly to Delta without entering the coordinated protocol.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            // Retry any orphaned staged data from a prior failed flush
            // before moving new data in, to prevent silent data loss.
            // flush_staged_to_delta handles table == None via reopen_table().
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta(deadline).await?;
            }

            // Stage new buffered data and flush to Delta.
            if !self.buffer.is_empty() {
                self.staged_batches = std::mem::take(&mut self.buffer);
                self.staged_rows = self.buffered_rows;
                self.staged_bytes = self.buffered_bytes;
                self.buffered_rows = 0;
                self.buffered_bytes = 0;
                self.buffer_start_time = None;

                self.flush_staged_to_delta(deadline).await?;
            }
            return Ok(());
        }

        #[cfg(feature = "delta-lake")]
        return self.stage_coordinated_buffer(deadline).await;

        #[cfg(not(feature = "delta-lake"))]
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake sink connector");

        // Only at-least-once may land buffered data during close. Exactly-once output without a
        // matching durable checkpoint must be discarded and replayed after recovery.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            || self.unresolved_delta_write
        {
            self.buffer.clear();
            self.staged_batches.clear();
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.staged_rows = 0;
            self.staged_bytes = 0;
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
        } else {
            self.flush().await?;
        }

        // Drop the table handle when closing.
        #[cfg(feature = "delta-lake")]
        {
            self.table = None;
        }

        self.state = ConnectorState::Closed;

        info!(
            table_path = %self.config.table_path,
            delta_version = self.delta_version,
            "Delta Lake sink connector closed"
        );

        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    fn as_coordinated_committer(&self) -> Option<&dyn crate::connector::CoordinatedCommitter> {
        self.is_coordinated()
            .then_some(self as &dyn crate::connector::CoordinatedCommitter)
    }

    fn coordinated_abort_cleaner(
        &self,
    ) -> Option<Arc<dyn crate::connector::CoordinatedAbortCleaner>> {
        None
    }
}

#[cfg(feature = "delta-lake")]
#[async_trait]
impl crate::connector::CoordinatedCommitter for DeltaLakeSink {
    async fn commit_aggregated(
        &self,
        batch: crate::connector::CoordinatedCommitBatch,
        context: crate::connector::CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        let deadline = context.deadline();
        let publication_budget = context.remaining();
        if publication_budget.is_zero() {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated publication deadline elapsed before I/O".into(),
            ));
        }

        batch.validate_shape().map_err(|error| {
            ConnectorError::TransactionError(format!(
                "Delta coordinated batch validation failed: {error}"
            ))
        })?;
        let external_key = batch.namespace.external_key();
        let exact_batch_fingerprint = batch.exact_fingerprint();
        let target_cursor = crate::connector::CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        };
        let pending = UnresolvedDeltaPublication {
            external_key,
            target: target_cursor,
            exact_batch_fingerprint,
        };
        {
            let mut unresolved = self.coordinated_unresolved_publication.lock();
            if unresolved
                .as_ref()
                .is_some_and(|unresolved| unresolved != &pending)
            {
                return Err(ConnectorError::TransactionError(
                    "Delta has a different unresolved coordinated publication; only that exact cut may be retried"
                        .into(),
                ));
            }
            *unresolved = Some(pending.clone());
        }

        let operation = publish_coordinated_delta_batch(
            self.resolved_table_path.clone(),
            self.resolved_storage_options.clone(),
            Arc::clone(&self.coordinated_unresolved_publication),
            pending.clone(),
            batch,
            deadline,
            publication_budget,
        );
        // Tokio task-local state is intentionally propagated only for the
        // deterministic delayed-catalog test hook.
        #[cfg(test)]
        let operation = {
            let delay = super::super::delta_io::DELAY_COORDINATED_CATALOG_COMMIT
                .try_with(Clone::clone)
                .ok();
            async move {
                if let Some(delay) = delay {
                    super::super::delta_io::DELAY_COORDINATED_CATALOG_COMMIT
                        .scope(delay, operation)
                        .await
                } else {
                    operation.await
                }
            }
        };
        let task = match self.spawn_tracked_delta_task(operation) {
            Ok(task) => task,
            Err(error) => {
                let mut unresolved = self.coordinated_unresolved_publication.lock();
                if unresolved.as_ref() == Some(&pending) {
                    *unresolved = None;
                }
                return Err(error);
            }
        };

        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ConnectorError::outcome_unknown(
                format!("Delta coordinated publication task terminated unexpectedly: {error}"),
                true,
            )),
            Err(_) => Err(ConnectorError::outcome_unknown(
                format!(
                    "Delta coordinated publication exceeded its {publication_budget:?} remaining \
                     budget; reconcile the exact cursor before replay"
                ),
                true,
            )),
        }
    }

    async fn committed_cursor(
        &self,
        namespace: &crate::connector::CoordinatedCommitNamespace,
    ) -> Result<Option<crate::connector::CoordinatedCommitCursor>, ConnectorError> {
        namespace.validate()?;
        let external_key = namespace.external_key();
        let deadline = self.operation_deadline();
        // RECOVERY: both operations are metadata reads. Retrying typed transient failures cannot
        // duplicate publication; the sink-task command still clamps this future to its caller's
        // earlier checkpoint deadline.
        let observed = retry_delta_metadata_until(deadline, "coordinated cursor read", || async {
            let table = super::super::delta_io::open_or_create_table(
                &self.resolved_table_path,
                self.resolved_storage_options.clone(),
                None,
            )
            .await?;
            super::super::delta_io::get_coordinated_cursor(&table, &external_key).await
        })
        .await?;
        let mut unresolved = self.coordinated_unresolved_publication.lock();
        if unresolved.as_ref().is_some_and(|pending| {
            pending.external_key == external_key && pending.reconciled_by(observed)
        }) {
            *unresolved = None;
        }
        Ok(observed)
    }
}
