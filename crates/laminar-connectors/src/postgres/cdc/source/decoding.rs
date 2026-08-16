//! WAL decoding, relation admission, and decoded-stage resource accounting.

#[cfg(test)]
use super::Bytes;
use super::{
    conservative_deque_growth_bytes, decode_message, logical_wal_payload_bytes,
    old_tuple_json_encoded_len, old_tuple_to_json, planned_event_bytes, retained_event_bytes,
    tuple_json_encoded_len, tuple_to_json, CdcOperation, ChangeEvent, CommittedTransaction,
    ConnectorError, ConnectorState, Lsn, OldTuple, OwnedWalPayload, PostgresCdcSource,
    RelationInfo, TransactionState, VecDeque, WalMessage, WalPayload,
};

impl PostgresCdcSource {
    #[cfg(test)]
    pub(super) fn enqueue_wal_data(&mut self, data: Vec<u8>) {
        self.pending_messages.push_back(data);
    }

    #[cfg(test)]
    pub(super) fn process_pending_messages(&mut self) -> Result<(), ConnectorError> {
        while let Some(data) = self.pending_messages.pop_front() {
            let result = decode_message(Bytes::from(data))
                .map_err(|error| ConnectorError::ReadError(format!("pgoutput decode: {error}")))
                .and_then(|message| self.process_wal_message(message));
            if let Err(error) = result {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Processes a single decoded WAL message.
    pub(super) fn process_wal_message(&mut self, msg: WalMessage) -> Result<(), ConnectorError> {
        match msg {
            WalMessage::Begin(begin) => {
                if self.current_txn.is_some() {
                    return Err(ConnectorError::ReadError(
                        "PostgreSQL CDC received BEGIN before the current transaction committed"
                            .into(),
                    ));
                }
                self.current_txn = Some(TransactionState {
                    final_lsn: begin.final_lsn,
                    commit_ts_ms: begin.commit_ts_ms,
                    events: VecDeque::new(),
                });
                self.reserve_committed_transaction_slot()?;
            }
            WalMessage::Commit(commit) => {
                if let Err(error) = self.validate_commit_boundary(&commit) {
                    self.state = ConnectorState::Failed;
                    return Err(error);
                }
                self.reserve_committed_transaction_slot()?;
                let txn = self.current_txn.take().ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC received COMMIT without an open transaction".into(),
                    )
                })?;
                self.committed_transactions.push_back(CommittedTransaction {
                    end_lsn: commit.end_lsn,
                    events: txn.events,
                });
                self.write_lsn = self.write_lsn.max(commit.end_lsn);
                self.metrics.record_transaction();
                self.metrics
                    .set_replication_lag_bytes(self.replication_lag_bytes());
            }
            WalMessage::Relation(rel) => {
                let info = RelationInfo {
                    relation_id: rel.relation_id,
                    namespace: rel.namespace,
                    name: rel.name,
                    replica_identity: rel.replica_identity as char,
                    columns: rel.columns,
                };
                self.admit_relation(info)?;
            }
            WalMessage::Insert(ins) => {
                self.process_insert(ins.relation_id, &ins.new_tuple)?;
            }
            WalMessage::Update(upd) => {
                self.process_update(upd.relation_id, upd.old_tuple.as_ref(), &upd.new_tuple)?;
            }
            WalMessage::Delete(del) => {
                self.process_delete(del.relation_id, &del.old_tuple)?;
            }
            WalMessage::Truncate(trunc) => {
                let table_names: Vec<String> = trunc
                    .relation_ids
                    .iter()
                    .map(|id| {
                        self.relation_cache
                            .get(*id)
                            .map_or_else(|| Ok(format!("oid:{id}")), RelationInfo::full_name)
                    })
                    .collect::<Result<_, ConnectorError>>()?;
                return Err(ConnectorError::ReadError(format!(
                    "TRUNCATE received on table(s): {}. \
                     Cannot produce retraction events — restart the pipeline with a fresh snapshot.",
                    table_names.join(", ")
                )));
            }
            WalMessage::Origin(_) | WalMessage::Type(_) => {
                // Origin and Type messages are noted but don't
                // produce change events in the current implementation.
            }
        }
        Ok(())
    }

    pub(super) fn process_insert(
        &mut self,
        relation_id: u32,
        new_tuple: &super::super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, after_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let after_len = tuple_json_encoded_len(new_tuple, relation)?;
            (table, after_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), None, Some(after_len))?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let after_json = tuple_to_json(new_tuple, self.require_relation(relation_id)?, after_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Insert,
            lsn,
            ts_ms,
            before: None,
            after: Some(after_json),
        };

        self.push_event(event, event_bytes)?;
        self.metrics.record_insert();
        Ok(())
    }

    pub(super) fn process_update(
        &mut self,
        relation_id: u32,
        old_tuple: Option<&OldTuple>,
        new_tuple: &super::super::decoder::TupleData,
    ) -> Result<(), ConnectorError> {
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, before_len, after_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let before_len = old_tuple
                .map(|tuple| old_tuple_json_encoded_len(tuple, relation))
                .transpose()?;
            let after_len = tuple_json_encoded_len(new_tuple, relation)?;
            (table, before_len, after_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), before_len, Some(after_len))?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let relation = self.require_relation(relation_id)?;
        let before_json = old_tuple
            .zip(before_len)
            .map(|(tuple, length)| old_tuple_to_json(tuple, relation, length))
            .transpose()?;
        let after_json = tuple_to_json(new_tuple, relation, after_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Update,
            lsn,
            ts_ms,
            before: before_json,
            after: Some(after_json),
        };

        self.push_event(event, event_bytes)?;
        self.metrics.record_update();
        Ok(())
    }

    pub(super) fn process_delete(
        &mut self,
        relation_id: u32,
        old_tuple: &OldTuple,
    ) -> Result<(), ConnectorError> {
        let (lsn, ts_ms) = self.require_current_txn_context()?;
        let (table, before_len) = {
            let relation = self.require_relation(relation_id)?;
            let table = relation.full_name()?;

            if !self.config.should_include_table(&table) {
                return Ok(());
            }

            let before_len = old_tuple_json_encoded_len(old_tuple, relation)?;
            (table, before_len)
        };
        let event_bytes = planned_event_bytes(table.capacity(), Some(before_len), None)?;
        self.reserve_current_event_slot()?;
        self.ensure_event_capacity(event_bytes)?;
        let before_json =
            old_tuple_to_json(old_tuple, self.require_relation(relation_id)?, before_len)?;

        let event = ChangeEvent {
            table,
            op: CdcOperation::Delete,
            lsn,
            ts_ms,
            before: Some(before_json),
            after: None,
        };

        self.push_event(event, event_bytes)?;
        self.metrics.record_delete();
        Ok(())
    }

    /// Looks up a relation by ID, returning a reference (no clone).
    ///
    /// The caller must extract all needed data (table name, JSON) from
    /// the reference before calling `push_event` or other `&mut self`
    /// methods (Rust's borrow rules require disjoint access).
    pub(super) fn require_relation(
        &self,
        relation_id: u32,
    ) -> Result<&RelationInfo, ConnectorError> {
        self.relation_cache.get(relation_id).ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "unknown relation ID {relation_id} (no Relation message received yet)"
            ))
        })
    }

    pub(super) fn require_current_txn_context(&mut self) -> Result<(Lsn, i64), ConnectorError> {
        if let Some(txn) = &self.current_txn {
            return Ok((txn.final_lsn, txn.commit_ts_ms));
        }
        self.state = ConnectorState::Failed;
        Err(ConnectorError::ReadError(
            "PostgreSQL CDC received a row change outside a transaction".into(),
        ))
    }

    pub(super) fn validate_commit_boundary(
        &self,
        commit: &super::super::decoder::CommitMessage,
    ) -> Result<(), ConnectorError> {
        let transaction = self.current_txn.as_ref().ok_or_else(|| {
            ConnectorError::ReadError(
                "PostgreSQL CDC received COMMIT without an open transaction".into(),
            )
        })?;
        if commit.commit_lsn != transaction.final_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT LSN {} does not match BEGIN final LSN {}",
                commit.commit_lsn, transaction.final_lsn
            )));
        }
        if commit.commit_ts_ms != transaction.commit_ts_ms {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT timestamp {} does not match BEGIN timestamp {}",
                commit.commit_ts_ms, transaction.commit_ts_ms
            )));
        }
        if commit.end_lsn < commit.commit_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT end LSN {} is before commit LSN {}",
                commit.end_lsn, commit.commit_lsn
            )));
        }
        let last_resumable_lsn = self
            .committed_transactions
            .back()
            .map_or(self.polled_lsn, |transaction| {
                transaction.end_lsn.max(self.polled_lsn)
            });
        if commit.end_lsn < last_resumable_lsn {
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC COMMIT end LSN {} is behind the last emitted or queued LSN {last_resumable_lsn}",
                commit.end_lsn
            )));
        }
        Ok(())
    }

    pub(super) fn event_container_retained_bytes(&self) -> Result<usize, ConnectorError> {
        let event_size = std::mem::size_of::<ChangeEvent>();
        let mut retained = self
            .committed_transactions
            .capacity()
            .checked_mul(std::mem::size_of::<CommittedTransaction>())
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC committed-transaction container size overflow".into(),
                )
            })?;
        if let Some(transaction) = &self.current_txn {
            retained = retained
                .checked_add(
                    transaction
                        .events
                        .capacity()
                        .checked_mul(event_size)
                        .ok_or_else(|| {
                            ConnectorError::ReadError(
                                "PostgreSQL CDC open-transaction container size overflow".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC event-container retained-byte overflow".into(),
                    )
                })?;
        }
        for transaction in &self.committed_transactions {
            retained = retained
                .checked_add(
                    transaction
                        .events
                        .capacity()
                        .checked_mul(event_size)
                        .ok_or_else(|| {
                            ConnectorError::ReadError(
                                "PostgreSQL CDC committed-event container size overflow".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC event-container retained-byte overflow".into(),
                    )
                })?;
        }
        Ok(retained)
    }

    pub(super) fn decoded_retained_bytes(&self) -> Result<usize, ConnectorError> {
        let container_bytes = self.event_container_retained_bytes()?;
        let relation_bytes = self.relation_cache.retained_bytes()?;
        self.buffered_event_bytes
            .checked_add(container_bytes)
            .and_then(|bytes| bytes.checked_add(relation_bytes))
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC decoded-stage retained-byte accounting overflow".into(),
                )
            })
    }

    pub(super) fn ensure_decoded_byte_limit(
        &mut self,
        additional_bytes: usize,
        context: &str,
    ) -> Result<usize, ConnectorError> {
        let retained_bytes = self
            .decoded_retained_bytes()
            .and_then(|bytes| {
                bytes.checked_add(additional_bytes).ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC decoded-stage retained-byte accounting overflow".into(),
                    )
                })
            })
            .inspect_err(|_error| {
                self.state = ConnectorState::Failed;
            })?;
        let max_bytes = self.config.decoded_event_bytes();
        if retained_bytes > max_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC {context} exceeds the hard decoded-stage buffer limit (retained bytes: {retained_bytes}/{max_bytes})"
            )));
        }
        Ok(retained_bytes)
    }

    pub(super) fn reserve_current_event_slot(&mut self) -> Result<(), ConnectorError> {
        let Some(transaction) = self.current_txn.as_ref() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received a row change outside a transaction".into(),
            ));
        };
        let old_capacity = transaction.events.capacity();
        let growth_bytes = conservative_deque_growth_bytes(
            transaction.events.len(),
            old_capacity,
            std::mem::size_of::<ChangeEvent>(),
        )?;
        self.ensure_decoded_byte_limit(growth_bytes, "event-container growth")?;
        let Some(transaction) = self.current_txn.as_mut() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC open transaction disappeared during container preflight".into(),
            ));
        };
        if let Err(error) = transaction.events.try_reserve_exact(1) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve decoded-event storage: {error}"
            )));
        }
        let Some(actual_growth) = transaction
            .events
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<ChangeEvent>()))
        else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC event-container growth accounting failed".into(),
            ));
        };
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC event-container growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(0, "event-container growth")?;
        Ok(())
    }

    pub(super) fn reserve_committed_transaction_slot(&mut self) -> Result<(), ConnectorError> {
        if self.current_txn.is_none() {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received COMMIT without an open transaction".into(),
            ));
        }
        self.committed_transactions
            .len()
            .checked_add(1)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError(
                    "PostgreSQL CDC committed-transaction count overflow".into(),
                )
            })?;
        let old_capacity = self.committed_transactions.capacity();
        let growth_bytes = conservative_deque_growth_bytes(
            self.committed_transactions.len(),
            old_capacity,
            std::mem::size_of::<CommittedTransaction>(),
        )?;
        self.ensure_decoded_byte_limit(growth_bytes, "committed-transaction container growth")?;
        if let Err(error) = self.committed_transactions.try_reserve_exact(1) {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve committed-transaction storage: {error}"
            )));
        }
        let actual_growth = self
            .committed_transactions
            .capacity()
            .checked_sub(old_capacity)
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<CommittedTransaction>()))
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC committed-transaction growth accounting failed".into(),
                )
            })?;
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC committed-transaction growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(0, "committed-transaction container growth")?;
        Ok(())
    }

    pub(super) fn admit_relation(&mut self, info: RelationInfo) -> Result<(), ConnectorError> {
        let existing_bytes = self
            .relation_cache
            .get(info.relation_id)
            .map(RelationInfo::variable_retained_bytes)
            .transpose()
            .inspect_err(|_error| {
                self.state = ConnectorState::Failed;
            })?;
        let new_relation = usize::from(existing_bytes.is_none());
        let existing_bytes = existing_bytes.unwrap_or(0);
        self.relation_cache
            .len()
            .checked_add(new_relation)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::ReadError("PostgreSQL CDC relation-cache count overflow".into())
            })?;
        let incoming_bytes = info.variable_retained_bytes().inspect_err(|_error| {
            self.state = ConnectorState::Failed;
        })?;
        let retained_growth = incoming_bytes.saturating_sub(existing_bytes);
        let growth_bytes = self
            .relation_cache
            .reservation_growth_bytes(info.relation_id)
            .inspect_err(|_error| {
                self.state = ConnectorState::Failed;
            })?;
        let admission_bytes = retained_growth.checked_add(growth_bytes).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::ReadError(
                "PostgreSQL CDC relation-cache admission size overflow".into(),
            )
        })?;
        self.ensure_decoded_byte_limit(admission_bytes, "relation-cache admission")?;
        let old_cache_bytes = self.relation_cache.retained_bytes()?;
        self.relation_cache
            .try_reserve_for(info.relation_id)
            .inspect_err(|_error| {
                self.state = ConnectorState::Failed;
            })?;
        let actual_growth = self
            .relation_cache
            .retained_bytes()?
            .checked_sub(old_cache_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC relation-cache growth accounting failed".into(),
                )
            })?;
        if actual_growth > growth_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC relation-cache growth exceeded its conservative preflight: actual={actual_growth}, planned={growth_bytes}"
            )));
        }
        self.ensure_decoded_byte_limit(retained_growth, "relation-cache admission")?;
        self.relation_cache.insert(info).inspect_err(|_error| {
            self.state = ConnectorState::Failed;
        })?;
        self.ensure_decoded_byte_limit(0, "relation-cache retention")?;
        Ok(())
    }

    pub(super) fn ensure_event_capacity(
        &mut self,
        event_bytes: usize,
    ) -> Result<(), ConnectorError> {
        if self.current_txn.is_none() {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC received a row change outside a transaction".into(),
            ));
        }

        self.buffered_event_count.checked_add(1).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::ReadError(
                "PostgreSQL CDC decoded-event count accounting overflow".into(),
            )
        })?;
        self.ensure_decoded_byte_limit(event_bytes, "transaction")?;
        Ok(())
    }

    pub(super) fn push_event(
        &mut self,
        event: ChangeEvent,
        preflight_bytes: usize,
    ) -> Result<(), ConnectorError> {
        let event_bytes = retained_event_bytes(&event)?;
        if event_bytes > preflight_bytes {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(format!(
                "PostgreSQL CDC decoded event exceeded its retained-byte preflight: actual={event_bytes}, planned={preflight_bytes}"
            )));
        }
        self.ensure_event_capacity(event_bytes)?;
        let next_event_count = self.buffered_event_count.checked_add(1).ok_or_else(|| {
            self.state = ConnectorState::Failed;
            ConnectorError::Internal(
                "PostgreSQL CDC preflighted event-count accounting overflow".into(),
            )
        })?;
        let next_event_bytes = self
            .buffered_event_bytes
            .checked_add(event_bytes)
            .ok_or_else(|| {
                self.state = ConnectorState::Failed;
                ConnectorError::Internal(
                    "PostgreSQL CDC preflighted retained-byte accounting overflow".into(),
                )
            })?;

        let Some(txn) = self.current_txn.as_mut() else {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC open transaction disappeared after event preflight".into(),
            ));
        };
        txn.events.push_back(event);
        self.buffered_event_count = next_event_count;
        self.buffered_event_bytes = next_event_bytes;
        Ok(())
    }

    /// Processes a [`WalPayload`] received from the background reader task.
    pub(super) fn process_wal_payload(
        &mut self,
        payload: WalPayload,
    ) -> Result<(), ConnectorError> {
        use super::super::decoder::pg_timestamp_to_unix_ms;

        match payload {
            WalPayload::Begin {
                final_lsn,
                commit_ts_us,
                xid,
            } => {
                let begin = super::super::decoder::BeginMessage {
                    final_lsn: Lsn::new(final_lsn),
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us).map_err(|error| {
                        ConnectorError::ReadError(format!(
                            "pgoutput BEGIN timestamp decode: {error}"
                        ))
                    })?,
                    xid,
                };
                self.process_wal_message(WalMessage::Begin(begin))
            }
            WalPayload::Commit {
                end_lsn,
                commit_ts_us,
                lsn,
            } => {
                let commit = super::super::decoder::CommitMessage {
                    flags: 0,
                    commit_lsn: Lsn::new(lsn),
                    end_lsn: Lsn::new(end_lsn),
                    commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us).map_err(|error| {
                        ConnectorError::ReadError(format!(
                            "pgoutput COMMIT timestamp decode: {error}"
                        ))
                    })?,
                };
                self.process_wal_message(WalMessage::Commit(commit))
            }
            WalPayload::XLogData { wal_end, data } => {
                let msg = decode_message(data)
                    .map_err(|e| ConnectorError::ReadError(format!("pgoutput decode: {e}")))?;
                self.process_wal_message(msg)?;
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
                Ok(())
            }
            WalPayload::KeepAlive { wal_end } => {
                self.write_lsn = self.write_lsn.max(Lsn::new(wal_end));
                Ok(())
            }
        }
    }

    pub(super) fn process_owned_wal_payload(
        &mut self,
        payload: OwnedWalPayload,
    ) -> Result<(), ConnectorError> {
        let received_bytes = u64::try_from(logical_wal_payload_bytes(&payload.payload))
            .map_err(|_| ConnectorError::Internal("PostgreSQL CDC byte metric overflow".into()))?;
        self.metrics.record_bytes(received_bytes);
        let OwnedWalPayload {
            payload,
            _byte_permit,
            wire_bytes,
        } = payload;
        let result = self.process_wal_payload(payload);
        drop(wire_bytes);
        result
    }
}
