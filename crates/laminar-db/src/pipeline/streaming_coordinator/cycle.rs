//! Mechanically extracted coordinator responsibility.

use super::{
    strip_source_row_positions, AlignedCheckpointContext, Arc, BarrierOutcome, CheckpointAttempt,
    CheckpointBarrier, CheckpointCleanupOwner, CycleError, CycleOutcome, CyclePublicationDurations,
    FxHashMap, FxHashSet, Instant, PendingWatermarkBatch, PipelineCallback, RecordBatch,
    SourceBatchCursor, SourceCheckpoint, SourceInputMode, SourceMsg, StreamingCoordinator,
    SOURCE_MUTATION_COLUMN,
};

impl StreamingCoordinator {
    pub(super) fn stage_batch(
        &mut self,
        source_idx: usize,
        batch: &RecordBatch,
        cursor: SourceBatchCursor,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) -> Result<(), CycleError> {
        let name = self.source_names.get(source_idx).cloned().ok_or_else(|| {
            CycleError::Recovery(format!(
                "source batch referenced unknown runtime index {source_idx}"
            ))
        })?;
        let has_mutations = batch.column_by_name(SOURCE_MUTATION_COLUMN).is_some();
        let input_mode = self
            .source_input_modes
            .get(source_idx)
            .copied()
            .ok_or_else(|| {
                CycleError::Recovery(format!(
                    "source '{name}' has no input-mode admission slot at runtime index {source_idx}"
                ))
            })?;
        let visible = strip_source_row_positions(batch).map_err(|error| {
            CycleError::Recovery(format!(
                "source '{name}' emitted invalid hidden metadata: {error}"
            ))
        })?;
        if has_mutations && input_mode != Some(SourceInputMode::KeyedUpsert) {
            return Err(CycleError::Recovery(format!(
                "source '{name}' emitted keyed mutation metadata outside a keyed-upsert route"
            )));
        }

        // Filter against the pre-drain watermark. Extraction is deferred until after all batches
        // are filtered so one batch cannot make the next batch appear late.
        let admission_floor = callback.current_watermark();
        let filtered = if input_mode.is_some() {
            Some(batch.clone())
        } else {
            callback.filter_late_rows(&name, batch)?
        };
        if source_idx >= self.pending_offsets.len() || source_idx >= self.committed_offsets.len() {
            return Err(CycleError::Recovery(format!(
                "source '{name}' has no runtime offset slot at index {source_idx}"
            )));
        }
        let input_channels = match cursor {
            SourceBatchCursor::Complete(checkpoint) => {
                let input_channels = checkpoint.input_channels_arc().cloned();
                self.pending_offsets[source_idx] = Some(SourceBatchCursor::Complete(checkpoint));
                input_channels
            }
            SourceBatchCursor::Incremental(delta) => {
                let input_channels = Some(Arc::clone(delta.input_channels_arc()));
                match self.pending_offsets[source_idx].as_mut() {
                    Some(SourceBatchCursor::Complete(checkpoint)) => {
                        checkpoint.apply_delta(delta).map_err(|error| {
                            CycleError::Recovery(format!(
                                "source '{name}' emitted an invalid incremental cursor: {error}"
                            ))
                        })?;
                    }
                    Some(SourceBatchCursor::Incremental(pending)) => {
                        pending.merge(delta).map_err(|error| {
                            CycleError::Recovery(format!(
                                "source '{name}' emitted an invalid incremental cursor: {error}"
                            ))
                        })?;
                    }
                    None => {
                        let committed = self
                            .committed_offsets
                            .get(source_idx)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| {
                                CycleError::Recovery(format!(
                                    "source '{name}' emitted an incremental cursor before its complete assignment cursor"
                                ))
                            })?;
                        delta.validate_base(committed).map_err(|error| {
                            CycleError::Recovery(format!(
                                "source '{name}' emitted an invalid incremental cursor: {error}"
                            ))
                        })?;
                        self.pending_offsets[source_idx] =
                            Some(SourceBatchCursor::Incremental(delta));
                    }
                }
                input_channels
            }
        };
        *cycle_events += visible.num_rows() as u64;
        if let Some(filtered) = filtered {
            self.source_batches_buf
                .entry(Arc::clone(&name))
                .or_default()
                .push(filtered);
        }
        self.pending_watermark_batches.push(PendingWatermarkBatch {
            source_name: name,
            batch: batch.clone(),
            admission_floor,
            input_channels,
        });
        Ok(())
    }

    /// Process one source message under the exact source-barrier ordering invariant.
    pub(super) fn process_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        barriers: &mut Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
        cycle_events: &mut u64,
    ) -> Result<(), CycleError> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                cursor,
            } => {
                if self.barrier_seen.contains(&source_idx) {
                    return Err(CycleError::Recovery(format!(
                        "source {} emitted data after its checkpoint barrier without an exact release",
                        self.source_names
                            .get(source_idx)
                            .map_or("<unknown>", AsRef::as_ref)
                    )));
                }
                self.stage_batch(source_idx, &batch, cursor, callback, cycle_events)?;
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                checkpoint,
            } => {
                let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
                if !self.pending_barrier.active || self.pending_barrier.attempt != Some(attempt) {
                    tracing::debug!(
                        source_idx,
                        checkpoint_id = barrier.checkpoint_id,
                        epoch = barrier.epoch,
                        "ignoring stale or cancelled source barrier"
                    );
                    self.release_source_barrier_for(source_idx, attempt);
                    return Ok(());
                }
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    "coordinator received source barrier"
                );
                self.barrier_seen.insert(source_idx);
                barriers.push((source_idx, barrier, checkpoint));
            }
        }
        Ok(())
    }

    /// Process a message after checkpoint admission has closed.
    ///
    /// No shutdown checkpoint exists, so every remaining batch belongs to an uncommitted open
    /// epoch. Barriers are control records for attempts that have already been cancelled and are
    /// ignored; they must never affect later open-epoch data.
    pub(super) fn process_shutdown_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) -> Option<String> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                cursor,
            } => self
                .stage_batch(source_idx, &batch, cursor, callback, cycle_events)
                .err()
                .map(|error| error.to_string()),
            SourceMsg::Barrier {
                source_idx,
                barrier,
                ..
            } => {
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    epoch = barrier.epoch,
                    "ignoring checkpoint barrier during shutdown drain"
                );
                None
            }
        }
    }

    /// Per-source committed offsets keyed by source name, reflecting the last successful cycle.
    /// Follower control uses this stable cut rather than advancing without source positions.
    pub(super) fn current_source_offsets(&self) -> FxHashMap<String, SourceCheckpoint> {
        self.committed_offsets
            .iter()
            .enumerate()
            .filter_map(|(idx, cp)| {
                if !self
                    .source_handles
                    .get(idx)
                    .is_none_or(|handle| handle.recovery_cursor)
                {
                    return None;
                }
                cp.as_ref().and_then(|c| {
                    self.source_names
                        .get(idx)
                        .map(|name| (name.to_string(), c.clone()))
                })
            })
            .collect()
    }

    /// Commit one staged complete cursor or assignment-local delta.
    pub(super) fn commit_pending_offset(&mut self, source_idx: usize) -> Result<(), CycleError> {
        let name = self
            .source_names
            .get(source_idx)
            .map_or("<unknown>", AsRef::as_ref);
        let pending = self.pending_offsets.get(source_idx).ok_or_else(|| {
            CycleError::Recovery(format!(
                "source '{name}' has no runtime offset slot at index {source_idx}"
            ))
        })?;
        if source_idx >= self.committed_offsets.len() {
            return Err(CycleError::Recovery(format!(
                "source '{name}' has no committed offset slot at index {source_idx}"
            )));
        }
        if let Some(SourceBatchCursor::Incremental(delta)) = pending.as_ref() {
            let committed = self
                .committed_offsets
                .get(source_idx)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    CycleError::Recovery(format!(
                        "source '{name}' cannot commit an incremental cursor without a complete assignment cursor"
                    ))
                })?;
            delta.validate_base(committed).map_err(|error| {
                CycleError::Recovery(format!(
                    "source '{name}' cannot commit its incremental cursor: {error}"
                ))
            })?;
        }

        match self.pending_offsets[source_idx].take() {
            Some(SourceBatchCursor::Complete(checkpoint)) => {
                self.committed_offsets[source_idx] = Some(checkpoint);
            }
            Some(SourceBatchCursor::Incremental(delta)) => {
                self.committed_offsets[source_idx]
                    .as_mut()
                    .ok_or_else(|| {
                        CycleError::Recovery(format!(
                            "source '{name}' lost its complete assignment cursor before delta commit"
                        ))
                    })?
                    .apply_delta(delta)
                    .map_err(|error| {
                        CycleError::Recovery(format!(
                            "source '{name}' cannot commit its incremental cursor: {error}"
                        ))
                    })?;
            }
            None => {}
        }
        Ok(())
    }

    /// Merge staged offsets into `committed_offsets` after successful cycle publication.
    pub(super) fn commit_pending_offsets(&mut self) -> Result<(), CycleError> {
        for source_idx in 0..self.pending_offsets.len() {
            self.commit_pending_offset(source_idx)?;
        }
        Ok(())
    }

    /// Publish materialized views, streams, and sink work before advancing source cursors.
    /// Publication failure is a shared-runtime consistency fault, so every mode recovers.
    pub(super) async fn publish_cycle_outputs(
        &mut self,
        callback: &mut impl PipelineCallback,
        outcome: &CycleOutcome,
    ) -> Result<CyclePublicationDurations, CycleError> {
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("materialized-view publication") {
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        let output_store_started = Instant::now();
        if let Err(error) = callback.update_mv_stores(&outcome.results) {
            #[cfg(feature = "cluster")]
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "materialized-view publication failed: {error}"
            )));
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("stream publication") {
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        if let Err(error) = callback.push_to_streams(&outcome.results) {
            #[cfg(feature = "cluster")]
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "stream publication failed: {error}"
            )));
        }
        let output_store_ns =
            u64::try_from(output_store_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("sink publication") {
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        let sink_enqueue_started = Instant::now();
        if let Err(error) = callback.write_to_sinks(&outcome.results, None).await {
            #[cfg(feature = "cluster")]
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        let sink_enqueue_ns =
            u64::try_from(sink_enqueue_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

        // A sink command admitted under authority may still be queued when the lease is fenced.
        // Recheck before advancing the in-memory source cursor; the checkpoint path separately
        // FIFO-fences every sink before persisting it.
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source cursor advancement") {
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        if let Err(error) =
            self.settle_pending_offsets(&outcome.failed_sources, &outcome.deferred_sources)
        {
            #[cfg(feature = "cluster")]
            callback.abort_subscription_output();
            self.discard_pending_offsets();
            return Err(error);
        }
        #[cfg(feature = "cluster")]
        callback.commit_subscription_output();
        self.replay_pending = outcome.any_deferred;
        Ok(CyclePublicationDurations {
            output_store_ns,
            sink_enqueue_ns,
        })
    }

    /// Settle one graph cycle without allowing a cursor to overtake graph-retained input.
    /// Failure takes precedence if a source appears in both sets: best-effort isolation drops the
    /// failed cycle, whereas a pure deferral must retain the exact staged cursor for its retry.
    pub(super) fn settle_pending_offsets(
        &mut self,
        failed: &FxHashSet<Arc<str>>,
        deferred: &FxHashSet<Arc<str>>,
    ) -> Result<(), CycleError> {
        if self.committed_offsets.len() != self.pending_offsets.len() {
            return Err(CycleError::Recovery(
                "source cursor slot counts diverged at cycle settlement".into(),
            ));
        }
        if failed.is_empty() && deferred.is_empty() {
            return self.commit_pending_offsets();
        }
        for i in 0..self.pending_offsets.len() {
            let in_failed_domain = self
                .source_names
                .get(i)
                .is_some_and(|name| failed.contains(name));
            if in_failed_domain {
                self.pending_offsets[i] = None;
            } else if self
                .source_names
                .get(i)
                .is_some_and(|name| deferred.contains(name))
            {
                // Retain the cursor alongside the graph's buffered input.
            } else {
                self.commit_pending_offset(i)?;
            }
        }
        Ok(())
    }

    /// Discard staged offsets when cycle execution or publication fails.
    pub(super) fn discard_pending_offsets(&mut self) {
        for slot in &mut self.pending_offsets {
            *slot = None;
        }
    }

    /// Reset per-cycle barrier tracking at cycle start. While a multi-source barrier is still
    /// aligning, retain sources already held at the cut so any protocol violation fails closed.
    pub(super) fn reset_barrier_seen_for_cycle(&mut self) {
        self.barrier_seen.clear();
        if self.pending_barrier.active {
            self.barrier_seen
                .extend(self.pending_barrier.sources_aligned.iter().copied());
        }
    }

    pub(super) fn capture_replayable_barrier_cursor(
        &mut self,
        source_idx: usize,
        checkpoint: &SourceCheckpoint,
    ) {
        if self
            .source_handles
            .get(source_idx)
            .is_some_and(|handle| !handle.recovery_cursor)
        {
            return;
        }
        if let Some(name) = self.source_names.get(source_idx) {
            self.pending_barrier
                .source_checkpoints
                .insert(name.to_string(), checkpoint.clone());
        }
    }

    pub(super) async fn handle_aligned_checkpoint_outcome(
        &mut self,
        callback: &mut impl PipelineCallback,
        outcome: BarrierOutcome,
        context: AlignedCheckpointContext,
        source_checkpoints: &FxHashMap<String, SourceCheckpoint>,
    ) -> Result<(), String> {
        let AlignedCheckpointContext {
            cleanup_owner,
            attempt,
            started_at,
            flags,
            assignment_fence,
        } = context;
        let authoritative_abort = matches!(&outcome, BarrierOutcome::Aborted);
        let (cleanup_reason, manual_reason, record_failure) = match outcome {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority("aligned checkpoint publication")
                {
                    let reason = error.to_string();
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    return Err(reason);
                }
                let publication_error = callback.publish_barrier(attempt).err();
                self.broadcast_epoch_committed(epoch, source_checkpoints);
                self.finish_manual_success(
                    attempt,
                    &crate::checkpoint_coordinator::CheckpointResult {
                        success: true,
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        duration: started_at.elapsed(),
                        error: None,
                        failure_disposition: None,
                    },
                );
                return publication_error.map_or(Ok(()), Err);
            }
            BarrierOutcome::Async => {
                return Ok(());
            }
            BarrierOutcome::Committed(epoch) => {
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                (reason.clone(), reason, true)
            }
            BarrierOutcome::Skipped(reason) => {
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "barrier checkpoint skipped"
                );
                (
                    reason.to_string(),
                    format!("manual checkpoint skipped: {reason}"),
                    false,
                )
            }
            BarrierOutcome::CancelledBeforeCapture => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint topology closed before state capture"
                );
                let reason = "checkpoint topology closed before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Aborted => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint was authoritatively aborted"
                );
                let reason =
                    "checkpoint was aborted by authoritative cluster control before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Failed => {
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint failed"
                );
                (
                    "barrier-aligned checkpoint failed before durable tail".into(),
                    "manual barrier-aligned checkpoint failed before the durable tail".into(),
                    true,
                )
            }
        };
        callback.abort_subscription_cut(attempt);
        if authoritative_abort && cleanup_owner == CheckpointCleanupOwner::Follower {
            callback.resolve_authoritative_follower_abort(attempt)?;
        } else {
            Self::cleanup_checkpoint_attempt(
                callback,
                cleanup_owner,
                attempt,
                &cleanup_reason,
                flags,
                assignment_fence,
            )
            .await?;
        }
        if record_failure {
            callback.record_checkpoint_failure(attempt.checkpoint_id, &cleanup_reason);
        }
        self.fail_manual_attempt(attempt, manual_reason);
        Ok(())
    }
}
