use std::collections::BTreeMap;

use futures::{stream::FuturesUnordered, StreamExt};
use laminar_connectors::connector::{
    MAX_COORDINATED_COMMIT_BATCH_BYTES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};

use super::{
    checked_successor_epoch, CheckpointAttempt, CheckpointCoordinator, DbError, RegisteredSink,
    SinkEpochPublication,
};

const MAX_SINK_PHASE_ONE_CONCURRENCY: usize = 8;

impl CheckpointCoordinator {
    #[cfg(test)]
    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
    ) {
        self.register_sink_with_abort_cleaner(name, handle, None);
    }

    pub(crate) fn register_sink_with_abort_cleaner(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
        abort_cleaner: Option<
            std::sync::Arc<dyn laminar_connectors::connector::CoordinatedAbortCleaner>,
        >,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
            abort_cleaner,
            abort_cleaner_retired: std::sync::atomic::AtomicBool::new(false),
        });
    }

    pub(crate) fn committable_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint(
                "checkpoint-committable sink names must be unique".into(),
            ));
        }
        Ok(names)
    }

    pub(super) fn sorted_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint("sink names must be unique".into()));
        }
        Ok(names)
    }

    pub(super) fn has_checkpoint_committable_sinks(&self) -> bool {
        self.sinks
            .iter()
            .any(|sink| sink.handle.checkpoint_committable())
    }

    pub(crate) fn clear_sinks(&mut self) -> Result<(), DbError> {
        if self.active_sink_witness.is_some()
            || self.allocator.sink_epoch_reservation.lock().is_some()
        {
            return Err(DbError::Checkpoint(
                "cannot clear sinks while a sink epoch remains open".into(),
            ));
        }
        self.sinks.clear();
        Ok(())
    }

    async fn create_sink_witness_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>, DbError> {
        let names = self.committable_sink_names()?;
        if names.is_empty() {
            return Ok(None);
        }
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(None);
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committable sinks require a decision store".into())
        })?;
        tokio::time::timeout_at(
            deadline,
            store.create_sink_open_witness(
                self.expected_pipeline_identity()?,
                self.store.participant_id(),
                attempt,
                names,
            ),
        )
        .await
        .map_err(|_| DbError::Checkpoint("sink-open witness create timed out".into()))?
        .map(Some)
        .map_err(|error| DbError::Checkpoint(format!("sink-open witness create: {error}")))
    }

    pub(super) async fn clear_sink_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let Some(witness) = self.active_sink_witness.clone() else {
            return Ok(());
        };
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("sink-open witness cleanup requires a decision store".into())
        })?;
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("sink-open witness cleanup failed: {error}"))
            })?;
        self.active_sink_witness = None;
        Ok(())
    }

    pub(super) async fn begin_sink_epoch_until(
        &mut self,
        deadline: tokio::time::Instant,
        publication: SinkEpochPublication,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        let inventory = self.reserve_sink_epoch_for_runtime_until(deadline).await?;
        let attempt = inventory.attempt;
        if let Err(error) = self
            .persist_sink_artifact_intents_until(&inventory, deadline)
            .await
        {
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(error);
        }
        let witness = match self.create_sink_witness_until(attempt, deadline).await {
            Ok(witness) => witness,
            Err(error) => {
                self.allocator.mark_sink_epoch_in_doubt(attempt);
                self.failure_requires_recovery = true;
                return Err(error);
            }
        };
        self.active_sink_witness = witness;

        let failures = self
            .begin_committable_sink_gates_until(attempt, deadline)
            .await;
        if !failures.is_empty() {
            return self
                .resolve_sink_begin_failure_until(attempt, failures, deadline)
                .await;
        }

        let admission = self.validate_begun_sink_epoch(attempt)?;
        if publication == SinkEpochPublication::Immediate {
            self.publish_begun_sink_epoch(attempt, admission)?;
        }
        Ok(())
    }

    async fn begin_committable_sink_gates_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Vec<String> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move {
                        (
                            name,
                            handle.begin_epoch_until(attempt.epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect()
    }

    fn validate_begun_sink_epoch(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<crate::sink_task::SinkEpochAdmission, DbError> {
        let admissions = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| {
                (
                    sink.name.as_str(),
                    sink.handle.begun_epoch_admission(attempt.epoch),
                )
            })
            .collect::<Vec<_>>();
        let expected = admissions.first().and_then(|(_, admission)| *admission);
        let invalid = admissions
            .iter()
            .filter_map(|(name, admission)| {
                (*admission != expected || admission.is_none()).then_some(*name)
            })
            .collect::<Vec<_>>();
        if !invalid.is_empty() {
            self.fail_committable_sink_gates();
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} begin acknowledgement did not leave every gate Begun: {}",
                attempt.epoch,
                invalid.join(", ")
            )));
        }

        let admission = expected.ok_or_else(|| {
            DbError::Checkpoint(format!(
                "sink epoch {} has no committable gate admission",
                attempt.epoch
            ))
        })?;
        if let Err(error) = self.allocator.mark_sink_epoch_ready(attempt) {
            self.fail_committable_sink_gates();
            self.failure_requires_recovery = true;
            return Err(error);
        }
        Ok(admission)
    }

    fn publish_begun_sink_epoch(
        &mut self,
        attempt: CheckpointAttempt,
        admission: crate::sink_task::SinkEpochAdmission,
    ) -> Result<(), DbError> {
        let mut publication_error = None;
        for sink in self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
        {
            if let Err(error) = sink.handle.publish_open_epoch(admission) {
                publication_error = Some(DbError::Checkpoint(format!(
                    "sink '{}' epoch {} publication failed: {error}",
                    sink.name, attempt.epoch
                )));
                break;
            }
        }
        if let Some(error) = publication_error {
            self.fail_committable_sink_gates();
            self.failure_requires_recovery = true;
            return Err(error);
        }
        Ok(())
    }

    fn fail_committable_sink_gates(&self) {
        for sink in self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
        {
            sink.handle.fail_epoch_gate();
        }
    }

    async fn resolve_sink_begin_failure_until(
        &mut self,
        attempt: CheckpointAttempt,
        failures: Vec<String>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.fail_committable_sink_gates();
        if let Err(rollback) = self.rollback_sinks_until(attempt.epoch, deadline).await {
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} failed to open ({}) and rollback failed ({rollback})",
                attempt.epoch,
                failures.join("; ")
            )));
        }
        self.allocator.mark_sink_epoch_in_doubt(attempt);
        self.failure_requires_recovery = true;
        Err(DbError::Checkpoint(format!(
            "sink epoch {} failed to open and requires durable artifact settlement: {}",
            attempt.epoch,
            failures.join("; ")
        )))
    }
    pub async fn begin_initial_epoch(&mut self) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if !self.initial_sink_epoch_required()? {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        self.begin_sink_epoch_until(deadline, SinkEpochPublication::Immediate)
            .await
    }

    pub(super) async fn rollback_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move { (name, handle.rollback_epoch_until(epoch, deadline).await) }
                }),
        )
        .await;
        let failures = results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink rollback failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(super) async fn seal_sink_epoch_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move {
                        (
                            name,
                            handle.seal_epoch_for_protocol_until(epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        let mut admission = None;
        let mut failures = Vec::new();
        for (name, result) in results {
            match result {
                Ok(Some(current)) if admission.is_none_or(|expected| expected == current) => {
                    admission = Some(current);
                }
                Ok(Some(current)) => failures.push(format!(
                    "{name}: mismatched sink transition admission {current:?}"
                )),
                Ok(None) => {}
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink epoch {epoch} seal failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) async fn reconcile_sink_open_witness(&mut self) -> Result<(), DbError> {
        self.reconcile_sink_open_witness_until(
            tokio::time::Instant::now() + self.config.cleanup_timeout,
        )
        .await
    }

    pub(crate) async fn reconcile_sink_open_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(());
        }
        let Some(store) = self.decision_store.clone() else {
            return Ok(());
        };
        let Some(witness) = tokio::time::timeout_at(deadline, store.sink_open_witness())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness read: {error}")))?
        else {
            return Ok(());
        };
        if witness.pipeline_identity != self.expected_pipeline_identity()?
            || witness.deployment_id != self.expected_deployment_id()?
            || witness.participant_id != self.store.participant_id()
            || witness.committable_sinks != self.committable_sink_names()?
        {
            return Err(DbError::Checkpoint(
                "sink-open witness does not match the running checkpoint namespace".into(),
            ));
        }
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open outcome read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open outcome read: {error}")))?;
        match head.and_then(|head| head.latest_terminal) {
            Some(outcome) if outcome.epoch > witness.attempt.epoch => {
                return Err(DbError::Checkpoint(
                    "sink-open witness remained open past a newer terminal outcome".into(),
                ));
            }
            Some(outcome) if outcome.epoch == witness.attempt.epoch => {
                if outcome.checkpoint_id != witness.attempt.checkpoint_id {
                    return Err(DbError::Checkpoint(
                        "sink-open witness conflicts with its terminal outcome".into(),
                    ));
                }
                if !outcome.is_commit() {
                    self.rollback_sinks_until(witness.attempt.epoch, deadline)
                        .await?;
                }
            }
            Some(_) | None => {
                self.rollback_sinks_until(witness.attempt.epoch, deadline)
                    .await?;
            }
        }
        self.clear_sink_artifact_intents(witness.attempt);
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness cleanup: {error}")))?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            witness.attempt.epoch,
            "reconciling sink-open ownership",
        )?);
        self.failure_requires_recovery = false;
        Ok(())
    }

    pub(super) async fn pre_commit_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, Option<Vec<u8>>>, DbError> {
        // Phase one is a group boundary: no connector may enter PreCommit while a peer can still
        // admit an epoch write. Each handle repeats this seal idempotently at command admission.
        self.seal_sink_epoch_until(epoch, deadline).await?;
        let mut pending = self.sinks.iter();
        let mut active = FuturesUnordered::new();
        for sink in pending.by_ref().take(MAX_SINK_PHASE_ONE_CONCURRENCY) {
            active.push(Self::sink_phase_one(sink, epoch, deadline));
        }
        let mut descriptors = BTreeMap::new();
        let mut descriptor_bytes = 0usize;
        let mut first_error = None;
        while let Some(result) = active.next().await {
            match result {
                Ok(Some((name, payload))) if first_error.is_none() => {
                    descriptor_bytes = descriptor_bytes
                        .checked_add(payload.as_ref().map_or(0, Vec::len))
                        .ok_or_else(|| {
                            DbError::Checkpoint("sink descriptor byte count overflow".into())
                        })?;
                    if descriptor_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                        first_error = Some(DbError::Checkpoint(format!(
                            "sink descriptors exceed {MAX_COORDINATED_COMMIT_BATCH_BYTES} bytes"
                        )));
                    } else {
                        descriptors.insert(name, payload);
                        if let Some(sink) = pending.next() {
                            active.push(Self::sink_phase_one(sink, epoch, deadline));
                        }
                    }
                }
                Ok(None) if first_error.is_none() => {
                    if let Some(sink) = pending.next() {
                        active.push(Self::sink_phase_one(sink, epoch, deadline));
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(descriptors), Err)
    }

    async fn sink_phase_one(
        sink: &RegisteredSink,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<(String, Option<Vec<u8>>)>, DbError> {
        if sink.handle.checkpoint_committable() {
            let payload = sink
                .handle
                .pre_commit_until(epoch, deadline)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!("sink '{}' pre-commit failed: {error}", sink.name))
                })?;
            if payload
                .as_ref()
                .is_some_and(|payload| payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES)
            {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' descriptor exceeds {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} bytes",
                    sink.name
                )));
            }
            Ok(Some((sink.name.clone(), payload)))
        } else {
            sink.handle.flush_until(deadline).await.map_err(|error| {
                DbError::Checkpoint(format!("sink '{}' flush failed: {error}", sink.name))
            })?;
            Ok(None)
        }
    }
}
