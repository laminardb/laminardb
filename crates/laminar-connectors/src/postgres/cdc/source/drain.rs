//! Transaction-atomic extraction from decoded events into Arrow batches.

use super::super::changelog::ArrowBatchPlan;
use super::{
    events_to_record_batch, plan_record_batch, retained_event_bytes, ChangeEvent, ConnectorError,
    ConnectorState, Lsn, PostgresCdcSource, RecordBatch, VecDeque,
};

#[derive(Clone, Copy)]
struct DrainSelection {
    transaction_count: usize,
    event_count: usize,
    resumable_lsn: Lsn,
}

struct ExtractedDrain {
    event_groups: Vec<VecDeque<ChangeEvent>>,
    plan: ArrowBatchPlan,
    event_count: usize,
    retained_bytes: usize,
    resumable_lsn: Lsn,
}

impl PostgresCdcSource {
    pub(super) fn fail_on_terminal_wal_error(&mut self) -> Result<(), ConnectorError> {
        let message = self.wal_terminal_error.as_ref().and_then(|error| {
            error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        if let Some(message) = message {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(message));
        }
        Ok(())
    }

    fn select_drain_transactions(&mut self, max: usize) -> Result<DrainSelection, ConnectorError> {
        let mut transaction_count = 0_usize;
        let mut event_count = 0_usize;
        let mut resumable_lsn = self.polled_lsn;
        for transaction in &self.committed_transactions {
            let candidate_events = event_count
                .checked_add(transaction.events.len())
                .ok_or_else(|| {
                    self.state = ConnectorState::Failed;
                    ConnectorError::Internal(
                        "PostgreSQL CDC drain event-count accounting overflow".into(),
                    )
                })?;
            // Once the row target is full, still absorb immediately-following
            // filtered transactions so the durable cursor advances in WAL order.
            if event_count != 0 && !transaction.events.is_empty() && candidate_events > max {
                break;
            }
            event_count = candidate_events;
            transaction_count = transaction_count.checked_add(1).ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC drain transaction-count accounting overflow".into(),
                )
            })?;
            resumable_lsn = transaction.end_lsn;
        }
        Ok(DrainSelection {
            transaction_count,
            event_count,
            resumable_lsn,
        })
    }

    fn discard_filtered_transactions(&mut self, selection: DrainSelection) {
        for _ in 0..selection.transaction_count {
            self.committed_transactions.pop_front();
        }
        if self.committed_transactions.is_empty() {
            self.committed_transactions = VecDeque::new();
        }
        self.polled_lsn = selection.resumable_lsn;
    }

    fn selected_retained_bytes(
        &mut self,
        selection: &DrainSelection,
    ) -> Result<usize, ConnectorError> {
        let selected = self
            .committed_transactions
            .iter()
            .take(selection.transaction_count);
        let retained_bytes = selected
            .flat_map(|transaction| transaction.events.iter())
            .try_fold(0_usize, |bytes, event| {
                bytes
                    .checked_add(retained_event_bytes(event)?)
                    .ok_or_else(|| {
                        ConnectorError::Internal(
                            "PostgreSQL CDC drained-event retained-byte accounting overflow".into(),
                        )
                    })
            })
            .inspect_err(|_error| self.state = ConnectorState::Failed)?;
        if selection.event_count > self.buffered_event_count
            || retained_bytes > self.buffered_event_bytes
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC retained-buffer accounting invariant failed".into(),
            ));
        }
        Ok(retained_bytes)
    }

    fn validate_arrow_extraction_budget(
        &mut self,
        plan: &ArrowBatchPlan,
        extraction_capacity: usize,
    ) -> Result<(), ConnectorError> {
        let extraction_bytes = extraction_capacity
            .checked_mul(std::mem::size_of::<VecDeque<ChangeEvent>>())
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow extraction-container size overflow".into(),
                )
            })?;
        let planned_arrow_bytes = plan
            .retained_bytes
            .checked_add(extraction_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC Arrow build retained-byte accounting overflow".into(),
                )
            })?;
        let arrow_byte_limit = self.config.arrow_build_bytes();
        if planned_arrow_bytes > arrow_byte_limit {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC Arrow batch exceeds the hard build-buffer limit (retained bytes: {planned_arrow_bytes}/{arrow_byte_limit})"
            )));
        }
        Ok(())
    }

    fn preflight_and_extract(
        &mut self,
        selection: DrainSelection,
    ) -> Result<ExtractedDrain, ConnectorError> {
        let retained_bytes = self.selected_retained_bytes(&selection)?;
        let selected = self
            .committed_transactions
            .iter()
            .take(selection.transaction_count);
        let plan = plan_record_batch(selected.flat_map(|transaction| transaction.events.iter()))
            .inspect_err(|_error| self.state = ConnectorState::Failed)?;
        self.validate_arrow_extraction_budget(&plan, selection.transaction_count)?;

        let mut event_groups = Vec::new();
        if let Err(error) = event_groups.try_reserve_exact(selection.transaction_count) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve Arrow extraction storage: {error}"
            )));
        }
        let extraction_capacity = event_groups.capacity();
        self.validate_arrow_extraction_budget(&plan, extraction_capacity)?;
        for _ in 0..selection.transaction_count {
            let Some(mut transaction) = self.committed_transactions.pop_front() else {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::Internal(
                    "PostgreSQL CDC committed transaction disappeared after drain preflight".into(),
                ));
            };
            event_groups.push(std::mem::take(&mut transaction.events));
        }
        let extracted_events = event_groups.iter().try_fold(0_usize, |count, events| {
            count.checked_add(events.len()).ok_or_else(|| {
                ConnectorError::Internal(
                    "PostgreSQL CDC Arrow extraction row-count overflow".into(),
                )
            })
        });
        let extracted_events = extracted_events.inspect_err(|_error| {
            self.state = ConnectorState::Failed;
        })?;
        if extracted_events != selection.event_count
            || event_groups.capacity() != extraction_capacity
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC Arrow extraction changed after capacity preflight".into(),
            ));
        }
        if self.committed_transactions.is_empty() {
            self.committed_transactions = VecDeque::new();
        }
        Ok(ExtractedDrain {
            event_groups,
            plan,
            event_count: selection.event_count,
            retained_bytes,
            resumable_lsn: selection.resumable_lsn,
        })
    }

    /// Drains committed transactions without exposing a cursor inside a transaction.
    ///
    /// `max` is a batching target, not permission to split a `PostgreSQL` transaction. Logical
    /// replication can resume only at a WAL position, so a checkpoint between two fragments of
    /// one transaction would restore before rows already included in the checkpoint. When the
    /// first queued transaction is larger than `max`, emit it whole; the configured hard event
    /// and byte limits remain the memory bound.
    pub(super) fn drain_events(
        &mut self,
        max: usize,
    ) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.committed_transactions.is_empty() || max == 0 {
            return Ok(None);
        }

        let selection = self.select_drain_transactions(max)?;

        if selection.event_count == 0 {
            self.discard_filtered_transactions(selection);
            return Ok(None);
        }

        let extracted = self.preflight_and_extract(selection)?;
        let ExtractedDrain {
            event_groups,
            plan,
            event_count: drained_count,
            retained_bytes: drained_bytes,
            resumable_lsn,
        } = extracted;

        let batch = match events_to_record_batch(event_groups.into_iter().flatten(), &plan) {
            Ok(batch) => batch,
            Err(error) => {
                self.buffered_event_count -= drained_count;
                self.buffered_event_bytes -= drained_bytes;
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        };

        self.buffered_event_count -= drained_count;
        self.buffered_event_bytes -= drained_bytes;
        self.polled_lsn = resumable_lsn;
        self.metrics.record_batch();
        Ok(Some(batch))
    }
}
