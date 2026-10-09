//! Source contract, startup, polling, durable feedback, and shutdown ownership.

use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskTracker, SourceBatch, SourceCheckpointUnavailablePolicy, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition,
    SourceRowPositionCapability, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;

use super::super::config::{OutputMode, PostgresCdcConfig};
use super::super::schema_resolution::{committed_relation, declared_primary_key, declared_schema};
use super::super::typed_rows::RowBuilder;
use super::checkpoint::{parse_resumable, validate_live_binding, write_cursor, CursorPhase};
use super::decoding::Decoded;
use super::startup::{prepare, ReaderRuntime, StartInputs, StartPhase, StartPlan};
use super::{reap_postgres_reader, Arc, Lsn, Notify, Phase, PostgresCdcSource};

/// Minimum spacing of live publication/table revalidation at checkpoint commits.
const CONTRACT_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

fn parsed_config(
    current: &PostgresCdcConfig,
    config: &ConnectorConfig,
) -> Result<PostgresCdcConfig, ConnectorError> {
    if config.properties().is_empty() {
        current.validate()?;
        Ok(current.clone())
    } else {
        PostgresCdcConfig::from_config(config)
    }
}

impl PostgresCdcSource {
    /// Decode queued WAL within one bounded work quantum and emit whole committed transactions.
    fn poll_streaming(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.fail_on_terminal_wal_error()?;
        let payload_budget = max_records.max(1);
        let mut processed = 0_usize;
        let mut reader_closed = false;
        while processed < payload_budget {
            let payload = if let Some(payload) = self.pending_payloads.pop_front() {
                payload
            } else {
                match self.wal_rx.as_ref().map(|receiver| receiver.try_recv()) {
                    Some(Ok(payload)) => payload,
                    Some(Err(crossfire::TryRecvError::Empty)) | None => break,
                    Some(Err(crossfire::TryRecvError::Disconnected)) => {
                        reader_closed = true;
                        break;
                    }
                }
            };
            match self.process_owned_wal_payload(payload) {
                Ok(Decoded::Applied) => processed += 1,
                Ok(Decoded::Deferred(payload)) => {
                    self.pending_payloads.push_front(payload);
                    break;
                }
                Err(error) => return Err(self.fail(error)),
            }
        }
        // A full quantum may hide queued work behind a coalesced notification; keep one
        // payload so the next poll is self-notified instead of waiting on the reader.
        if processed == payload_budget && self.pending_payloads.is_empty() {
            match self.wal_rx.as_ref().map(|receiver| receiver.try_recv()) {
                Some(Ok(payload)) => self.pending_payloads.push_back(payload),
                Some(Err(crossfire::TryRecvError::Disconnected)) => reader_closed = true,
                Some(Err(crossfire::TryRecvError::Empty)) | None => {}
            }
        }
        #[cfg(test)]
        self.process_pending_messages()?;
        self.fail_on_terminal_wal_error()?;
        if reader_closed && self.committed.is_empty() && self.pending_payloads.is_empty() {
            return Err(self.fail(ConnectorError::ReadError(
                "WAL reader task terminated unexpectedly — replication stream lost".to_string(),
            )));
        }
        let batch = self.drain_committed(max_records)?;
        if max_records > 0 && (!self.pending_payloads.is_empty() || !self.committed.is_empty()) {
            self.data_ready.notify_one();
        }
        self.metrics
            .set_replication_lag_bytes(self.replication_lag_bytes());
        Ok(batch)
    }

    /// Launch the replication reader at the finished snapshot's consistent point.
    pub(super) async fn begin_streaming(&mut self, start_lsn: Lsn) -> Result<(), ConnectorError> {
        let binding = self.checkpoint_binding.clone().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC streams without a binding".into())
        })?;
        let launched = super::startup::launch_reader(
            &self.task_owner,
            Arc::clone(&self.data_ready),
            &self.config,
            &binding,
            start_lsn,
        )
        .await;
        match launched {
            Ok(runtime) => {
                self.enter_streaming(start_lsn, runtime);
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn enter_streaming(&mut self, start_lsn: Lsn, runtime: ReaderRuntime) {
        self.wal_rx = Some(runtime.wal_rx);
        self.wal_byte_budget = Some(runtime.wal_byte_budget);
        self.wal_terminal_error = Some(runtime.terminal_error);
        self.reader_handle = Some(runtime.reader_handle);
        self.reader_shutdown = Some(runtime.shutdown_tx);
        self.applied_lsn = Some(runtime.applied_lsn);
        self.phase = Phase::Streaming;
        self.polled_lsn = start_lsn;
        self.write_lsn = self.write_lsn.max(start_lsn);
        self.confirmed_flush_lsn = start_lsn;
        self.metrics.set_confirmed_flush_lsn(start_lsn.as_u64());
        self.data_ready.notify_one();
    }

    fn current_cursor(&self) -> CursorPhase {
        match self.phase {
            Phase::Snapshot(_) => CursorPhase::Snapshot,
            Phase::Idle | Phase::Streaming => CursorPhase::Streaming(self.polled_lsn),
        }
    }

    /// Re-read the live contract at most every [`CONTRACT_CHECK_INTERVAL`].
    async fn revalidate_contract(&mut self) -> Result<(), ConnectorError> {
        let now = tokio::time::Instant::now();
        if self.next_contract_check.is_some_and(|due| now < due) {
            return Ok(());
        }
        let (Some(binding), Some(relation)) = (&self.checkpoint_binding, &self.relation) else {
            return Ok(());
        };
        super::startup::revalidate(&self.task_owner, &self.config, binding, relation).await?;
        self.next_contract_check = Some(now + CONTRACT_CHECK_INTERVAL);
        Ok(())
    }
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
        let parsed = parsed_config(&self.config, config)?;
        Ok(Some(BTreeMap::from([
            ("database".into(), parsed.database),
            ("publication".into(), parsed.publication),
            ("slot.name".into(), parsed.slot_name),
            ("table".into(), parsed.table.to_string()),
            ("output.mode".into(), parsed.output_mode.to_string()),
            ("wire.protocol".into(), "pgoutput-v1-typed".into()),
        ])))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        let guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("PostgreSQL CDC schema owner is retired".into())
        })?;
        super::super::schema_resolution::resolve(config, explicit, guard).await
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let (config, position, _) = request.into_parts();
        let parsed = parsed_config(&self.config, &config)?;
        let declared = declared_schema(&config, None)?;
        let primary_key = declared_primary_key(&config);
        let committed = committed_relation(config.schema_binding(), &parsed)?;
        let plan = match position {
            SourcePosition::Initial => None,
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "PostgreSQL CDC has no sealed topology startup contract".into(),
                ));
            }
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => Some(parse_resumable(
                &checkpoint,
                &parsed,
                &format!("checkpoint {attempt:?}"),
            )?),
        };

        let prepared = prepare(
            &self.task_owner,
            StartInputs {
                data_ready: &self.data_ready,
                config: &parsed,
                declared: &declared,
                primary_key: &primary_key,
                committed_relation: committed.as_ref(),
            },
            match plan {
                None => StartPlan::Fresh,
                Some((lsn, binding)) => StartPlan::Resume { lsn, binding },
            },
        )
        .await?;
        // Publish the runtime only after all fallible network preparation succeeded.
        self.open_rows = Some(RowBuilder::new(&prepared.layout));
        self.layout = Some(prepared.layout);
        self.relation = Some(prepared.relation);
        self.checkpoint_binding = Some(prepared.binding);
        self.config = parsed;
        self.schema = declared;
        self.state = ConnectorState::Running;
        match prepared.phase {
            StartPhase::Snapshot(reader) => {
                self.phase = Phase::Snapshot(Box::new(reader));
                self.data_ready.notify_one();
            }
            StartPhase::Stream(lsn, runtime) => self.enter_streaming(lsn, runtime),
        }
        tracing::info!(
            table = %self.config.table,
            slot = %self.config.slot_name,
            output_mode = %self.config.output_mode,
            snapshot_mode = ?self.config.snapshot_mode,
            "PostgreSQL CDC source opened"
        );
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
        match self.phase {
            Phase::Snapshot(_) => self.poll_snapshot(max_records).await,
            Phase::Streaming => self.poll_streaming(max_records),
            Phase::Idle => Err(ConnectorError::Internal(
                "PostgreSQL CDC is running without a read phase".into(),
            )),
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        write_cursor(
            &self.config,
            self.checkpoint_binding.as_ref(),
            self.current_cursor(),
        )
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        Ok(match self.current_cursor() {
            CursorPhase::Snapshot => None,
            CursorPhase::Streaming(_) => Some(self.checkpoint()),
        })
    }

    fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
        // The initial snapshot is one replay unit: no checkpoint barrier may cut it.
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        // Advance the slot only after the epoch is durably committed (manifest persisted and
        // sinks committed), so PostgreSQL never reclaims WAL for rows still in the pipeline.
        // Snapshot-phase and empty cursors carry no LSN and are not feedback.
        if checkpoint.get_offset("lsn").is_none() {
            return Ok(());
        }
        let context = format!("committed epoch {epoch} checkpoint");
        let (lsn, committed_binding) = parse_resumable(checkpoint, &self.config, &context)?;
        let active =
            self.checkpoint_binding
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "running PostgreSQL CDC checkpoint binding".into(),
                    actual: "checkpoint binding is missing".into(),
                })?;
        validate_live_binding(&committed_binding, active, &context)?;
        if lsn > self.polled_lsn {
            return Err(ConnectorError::ConfigurationError(format!(
                "committed PostgreSQL CDC epoch {epoch} LSN {lsn} is ahead of the source's polled LSN {}; refusing irreversible slot feedback",
                self.polled_lsn
            )));
        }
        if lsn < self.confirmed_flush_lsn {
            return Ok(());
        }
        self.revalidate_contract().await?;
        let applied = self
            .applied_lsn
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "running PostgreSQL CDC replication feedback".into(),
                actual: "replication feedback handle is missing".into(),
            })?;
        applied.update(pgwire_replication::Lsn::from_u64(lsn.as_u64()));
        self.confirmed_flush_lsn = lsn;
        self.metrics.set_confirmed_flush_lsn(lsn.as_u64());
        Ok(())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let parsed = parsed_config(&self.config, config)?;
        // Slot WAL is reclaimed only as durable commits advance the confirmed-flush LSN, so the
        // source is commit-coupled. Exact delivery is not certified: feedback and sink commits
        // are not one atomic protocol.
        let input_mode = match parsed.output_mode {
            OutputMode::Upsert => SourceInputMode::KeyedUpsert,
            OutputMode::Changelog => SourceInputMode::FullChangelog,
        };
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            input_mode,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
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
            if let Some(handle) = self.reader_handle.take() {
                reap_postgres_reader(handle, &self.task_owner);
            }
        }
        if let Phase::Snapshot(reader) = std::mem::replace(&mut self.phase, Phase::Idle) {
            drop(reader);
        }
        self.reader_handle = None;
        self.reader_shutdown = None;
        self.wal_rx = None;
        self.applied_lsn = None;
        self.pending_payloads.clear();
        self.wal_byte_budget = None;
        self.wal_terminal_error = None;
        self.state = ConnectorState::Closed;
        self.committed.clear();
        self.committed_bytes = 0;
        self.open_transaction = None;
        self.open_mutations.clear();
        if let (Some(rows), Some(layout)) = (self.open_rows.as_mut(), self.layout.as_ref()) {
            drop(rows.finish(layout));
        }
        #[cfg(test)]
        self.pending_messages.clear();
        Ok(())
    }
}
