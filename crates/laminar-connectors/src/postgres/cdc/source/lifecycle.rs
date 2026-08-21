//! Source startup, polling, durable feedback, and shutdown ownership.

#[cfg(not(test))]
use super::startup::{prepare_reader_runtime, PreparedReaderRuntime};
use super::{
    async_trait, reap_postgres_reader, validate_checkpoint_identity, validate_live_binding,
    write_checkpoint_binding, Arc, BTreeMap, ConnectorConfig, ConnectorError, ConnectorState,
    ConnectorTaskTracker, Lsn, Notify, PostgresCdcConfig, PostgresCdcSource,
    PostgresCheckpointBinding, SchemaRef, SourceBatch, SourceCheckpoint, SourceConnector,
    SourceContract, SourcePosition, SourceStart, INITIAL_BOOTSTRAP_NOT_ADMITTED,
};

struct PreparedSourceStart {
    config: PostgresCdcConfig,
    start_lsn: Lsn,
    checkpoint_binding: PostgresCheckpointBinding,
}

fn prepare_source_start(
    current_config: &PostgresCdcConfig,
    config: &ConnectorConfig,
    position: SourcePosition,
) -> Result<PreparedSourceStart, ConnectorError> {
    let mut config = if config.properties().is_empty() {
        current_config.clone()
    } else {
        PostgresCdcConfig::from_config(config)?
    };
    config.normalize_table_filters();
    config.validate()?;

    let SourcePosition::Resume {
        attempt,
        checkpoint,
    } = position
    else {
        return Err(ConnectorError::ConfigurationError(
            INITIAL_BOOTSTRAP_NOT_ADMITTED.into(),
        ));
    };
    let context = format!("checkpoint {attempt:?}");
    let checkpoint_binding = validate_checkpoint_identity(&checkpoint, &config, &context)?;
    let lsn_str = checkpoint.get_offset("lsn").ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC checkpoint {attempt:?} is missing required 'lsn' offset"
        ))
    })?;
    let start_lsn = lsn_str.parse::<Lsn>().map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid LSN '{lsn_str}' in PostgreSQL CDC checkpoint {attempt:?}: {error}"
        ))
    })?;
    Ok(PreparedSourceStart {
        config,
        start_lsn,
        checkpoint_binding,
    })
}

#[cfg(not(test))]
fn install_reader_runtime(source: &mut PostgresCdcSource, runtime: PreparedReaderRuntime) {
    source.wal_rx = Some(runtime.wal_rx);
    source.wal_byte_budget = Some(runtime.wal_byte_budget);
    source.wal_terminal_error = Some(runtime.terminal_error);
    source.reader_handle = Some(runtime.reader_handle);
    source.reader_shutdown = Some(runtime.shutdown_tx);
    source.confirmed_lsn_tx = Some(runtime.confirmed_lsn_tx);
}

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        let mut parsed = if config.properties().is_empty() {
            self.config.clone()
        } else {
            PostgresCdcConfig::from_config(config)?
        };
        parsed.normalize_table_filters();
        parsed.validate()?;

        Ok(Some(BTreeMap::from([
            ("database".into(), parsed.database),
            ("publication".into(), parsed.publication),
            ("slot.name".into(), parsed.slot_name),
            ("table.exclude".into(), parsed.table_exclude.join(",")),
            ("table.include".into(), parsed.table_include.join(",")),
            ("wire.protocol".into(), "pgoutput-v1".into()),
        ])))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let (config, position, _) = request.into_parts();
        let prepared = prepare_source_start(&self.config, &config, position)?;

        #[cfg(not(test))]
        {
            let runtime = prepare_reader_runtime(
                self,
                &prepared.config,
                &prepared.checkpoint_binding,
                prepared.start_lsn,
            )
            .await?;
            install_reader_runtime(self, runtime);
        }

        // Publish the new runtime only after all fallible startup work has
        // succeeded. A failed start remains a clean Created connector.
        self.config = prepared.config;
        self.confirmed_flush_lsn = prepared.start_lsn;
        self.write_lsn = prepared.start_lsn;
        self.polled_lsn = prepared.start_lsn;
        self.checkpoint_binding = Some(prepared.checkpoint_binding);
        self.metrics
            .set_confirmed_flush_lsn(prepared.start_lsn.as_u64());
        self.state = ConnectorState::Running;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".to_string(),
                actual: self.state.to_string(),
            });
        }

        // Backpressure: stop draining raw WAL before the decoded-stage hard limit. The raw byte
        // budget then propagates pressure to the replication reader and PostgreSQL.
        {
            self.fail_on_terminal_wal_error()?;
            let high_watermark = self.config.decoded_high_watermark_bytes();
            let decoded_retained_bytes = self.decoded_retained_bytes().inspect_err(|_error| {
                self.state = ConnectorState::Failed;
            })?;
            let mut reader_closed = false;
            let must_finish_transaction = self.current_txn.is_some();
            let payload_budget = max_records.max(1);
            let drain_reader = must_finish_transaction || decoded_retained_bytes < high_watermark;
            if !drain_reader && self.pending_payloads.is_empty() {
                tracing::debug!(
                    retained_bytes = decoded_retained_bytes,
                    high_watermark,
                    "CDC backpressure active — pausing WAL reader drain"
                );
            }

            let mut processed_payloads = 0_usize;
            while processed_payloads < payload_budget {
                let payload = if let Some(payload) = self.pending_payloads.pop_front() {
                    Some(payload)
                } else if drain_reader {
                    match self.wal_rx.as_mut().map(|receiver| receiver.try_recv()) {
                        Some(Ok(payload)) => Some(payload),
                        Some(Err(crossfire::TryRecvError::Empty)) | None => None,
                        Some(Err(crossfire::TryRecvError::Disconnected)) => {
                            reader_closed = true;
                            None
                        }
                    }
                } else {
                    None
                };
                let Some(payload) = payload else {
                    break;
                };
                if let Err(e) = self.process_owned_wal_payload(payload) {
                    self.state = ConnectorState::Failed;
                    return Err(e);
                }
                processed_payloads = processed_payloads.checked_add(1).ok_or_else(|| {
                    self.state = ConnectorState::Failed;
                    ConnectorError::Internal(
                        "PostgreSQL CDC poll payload-count accounting overflow".into(),
                    )
                })?;
            }

            // Notify ourselves only when a bounded drain demonstrably left work queued.
            // Retaining one item avoids both a lost coalesced notification and an
            // open-transaction busy loop while the server is genuinely idle.
            let reached_payload_budget = processed_payloads == payload_budget;
            let may_drain_more = if reached_payload_budget && self.current_txn.is_none() {
                self.decoded_retained_bytes().inspect_err(|_error| {
                    self.state = ConnectorState::Failed;
                })? < high_watermark
            } else {
                reached_payload_budget
            };
            if may_drain_more && !reader_closed {
                if let Some(ref mut rx) = self.wal_rx {
                    match rx.try_recv() {
                        Ok(payload) => self.pending_payloads.push_back(payload),
                        Err(crossfire::TryRecvError::Empty) => {}
                        Err(crossfire::TryRecvError::Disconnected) => reader_closed = true,
                    }
                }
            }
            if !self.pending_payloads.is_empty() {
                self.data_ready.notify_one();
            }
            self.fail_on_terminal_wal_error()?;
            if reader_closed && self.committed_transactions.is_empty() {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ReadError(
                    "WAL reader task terminated unexpectedly — replication stream lost".to_string(),
                ));
            }
        }

        #[cfg(test)]
        self.process_pending_messages()?;

        // Drain buffered events into a RecordBatch.
        // Configured Arrow-column extractors derive event-time watermarks from `_ts_ms`.
        let result = match self.drain_events(max_records)? {
            Some(batch) => {
                self.metrics
                    .set_confirmed_flush_lsn(self.confirmed_flush_lsn.as_u64());
                self.metrics
                    .set_replication_lag_bytes(self.replication_lag_bytes());

                Ok(Some(SourceBatch::new(batch)))
            }
            None => Ok(None),
        };
        let emitted_batch = matches!(&result, Ok(Some(_)));
        if max_records > 0 && (emitted_batch || !self.committed_transactions.is_empty()) {
            self.data_ready.notify_one();
        }
        result
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        // polled_lsn = latest position drained into a batch — the resumable point recorded in the
        // manifest. The PG slot is NOT advanced here: doing so per poll lets PG reclaim WAL for
        // data that is only in-pipeline, so a crash loses an LSN range recovery still needs.
        // Slot feedback is deferred to notify_epoch_committed (durable-commit only).
        cp.set_offset("lsn", self.polled_lsn.to_string());
        cp.set_metadata("slot_name", &self.config.slot_name);
        cp.set_metadata("publication", &self.config.publication);
        cp.set_metadata("database", &self.config.database);
        if let Some(binding) = &self.checkpoint_binding {
            write_checkpoint_binding(&mut cp, binding);
        }
        cp
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        // Advance the PG replication slot only after the epoch is durably committed (manifest
        // persisted + sinks committed), so PG never reclaims WAL for data still in-pipeline.
        // The checkpoint carries the exact LSN persisted for this epoch; a timer-driven empty
        // checkpoint has no "lsn" offset and is a no-op.
        let Some(lsn_str) = checkpoint.get_offset("lsn") else {
            return Ok(());
        };
        let lsn = lsn_str.parse::<Lsn>().map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "committed PostgreSQL CDC epoch {epoch} contains invalid LSN '{lsn_str}': {error}"
            ))
        })?;
        let context = format!("committed epoch {epoch} checkpoint");
        let committed_binding = validate_checkpoint_identity(checkpoint, &self.config, &context)?;
        let active_binding =
            self.checkpoint_binding
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "running PostgreSQL CDC checkpoint binding".into(),
                    actual: "checkpoint binding is missing".into(),
                })?;
        validate_live_binding(&committed_binding, active_binding, &context)?;
        if lsn.as_u64() > self.polled_lsn.as_u64() {
            return Err(ConnectorError::ConfigurationError(format!(
                "committed PostgreSQL CDC epoch {epoch} LSN {lsn} is ahead of the source's polled LSN {}; refusing irreversible slot feedback",
                self.polled_lsn
            )));
        }
        // A strictly stale notification is already satisfied and must never regress either cursor.
        // An equal notification is handed off again: that is idempotent and repairs feedback after
        // a reader restart whose local cursor was restored before its channel was created.
        if lsn.as_u64() < self.confirmed_flush_lsn.as_u64() {
            return Ok(());
        }

        let tx = self
            .confirmed_lsn_tx
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "running PostgreSQL CDC confirmed-LSN feedback channel".into(),
                actual: "feedback channel is missing".into(),
            })?;
        tx.send(lsn.as_u64()).map_err(|_| {
            ConnectorError::ConnectionFailed(
                "PostgreSQL CDC confirmed-LSN feedback channel is closed".into(),
            )
        })?;
        // The local cursor is authoritative only after the reader accepted the handoff.
        self.confirmed_flush_lsn = lsn;
        self.metrics.set_confirmed_flush_lsn(lsn.as_u64());
        Ok(())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        // The replication slot's WAL is reclaimed only as the confirmed-flush LSN advances, which
        // happens on durable commit. Without checkpointing the slot never advances and the source
        // database's WAL fills without bound, so this source is commit-coupled.
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            PostgresCdcConfig::from_config(config)?.validate()?;
        }
        Err(ConnectorError::ConfigurationError(
            "PostgreSQL CDC emits a raw JSON change envelope; canonical primary-keyed row/delete records are required"
                .into(),
        ))
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Keep both fields installed while awaiting. If this close future is
        // cancelled, the same instance still owns the reader and can retry.
        if let Some(tx) = self.reader_shutdown.as_ref() {
            tx.send_replace(true);
        }
        let detach_reader = if let Some(handle) = self.reader_handle.as_mut() {
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut *handle)
                .await
                .is_err()
        } else {
            false
        };
        if detach_reader {
            tracing::warn!(
                "PostgreSQL CDC reader did not stop before the close deadline; its tracked reaper retains shutdown ownership"
            );
            let handle = self
                .reader_handle
                .take()
                .expect("reader handle was present while awaiting it");
            reap_postgres_reader(handle, &self.task_owner);
        }
        self.reader_handle = None;
        self.reader_shutdown = None;
        self.wal_rx = None;
        self.confirmed_lsn_tx = None;
        self.pending_payloads.clear();
        self.wal_byte_budget = None;
        self.wal_terminal_error = None;

        self.state = ConnectorState::Closed;
        self.committed_transactions.clear();
        self.current_txn = None;
        self.relation_cache.clear();
        self.buffered_event_count = 0;
        self.buffered_event_bytes = 0;
        #[cfg(test)]
        self.pending_messages.clear();
        Ok(())
    }
}
