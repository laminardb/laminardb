//! Mechanically extracted coordinator responsibility.

use super::{
    CycleError, ExitReason, Instant, PipelineCallback, SourceHandle, SourceTaskLease,
    StreamingCoordinator, SHUTDOWN_COMPLETION_TICK, SHUTDOWN_JOIN_TIMEOUT,
};

impl StreamingCoordinator {
    pub(super) async fn finish_run<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        mut fault: Option<String>,
        mut halted: bool,
        mut halt_reason: Option<String>,
    ) -> ExitReason {
        if let Some(reason) = callback.take_pipeline_halt() {
            tracing::warn!(%reason, "[LDB-3022] pipeline stopped after a permanent error");
            halted = true;
            halt_reason.get_or_insert(reason);
        }
        if halted {
            halt_reason.get_or_insert_with(|| "pipeline halted after a permanent error".into());
            fault = None;
        }
        // Every exit below is coordinator-owned. Mark it before tail settlement so source-task
        // guards cannot turn an intentional teardown into a second runtime fault.
        for handle in &self.source_handles {
            handle.task.mark_expected_shutdown();
        }

        // Stop is an admission fence. Cancel alignment before waiting for captured tails: an
        // unaligned attempt has no tail and therefore cannot make the in-flight counter progress.
        self.drain_manual_requests();
        self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");
        let interrupted_reason = if halted {
            "permanent pipeline halt interrupted source barrier alignment"
        } else if fault.is_some() {
            "pipeline fault interrupted source barrier alignment"
        } else {
            "pipeline shutdown interrupted source barrier alignment"
        };
        if let Err(error) = self
            .cancel_pending_barrier_for_stop(callback, interrupted_reason, false)
            .await
        {
            if halted {
                tracing::warn!(%error, "checkpoint cleanup also failed after a permanent halt");
            } else {
                fault.get_or_insert(error);
            }
        }

        // Captured tails own durable state and may still need to publish source acknowledgements.
        // Settling them while sources and sinks remain open prevents close from racing commit.
        if let Some(error) = self.settle_checkpoint_tails(callback).await {
            if halted {
                tracing::warn!(%error, "checkpoint tail also failed after a permanent halt");
            } else {
                fault.get_or_insert(error);
            }
        }
        if let Some(reason) = callback.take_pipeline_halt() {
            tracing::warn!(%reason, "[LDB-3022] checkpoint cleanup retained a permanent halt");
            halted = true;
            halt_reason.get_or_insert(reason);
            fault = None;
        }
        if !halted {
            if let Some(reason) = callback.take_pipeline_fault() {
                fault.get_or_insert(reason);
            }
        }

        self.stop_source_barrier_holds();
        for handle in &self.source_handles {
            handle.task.notify_shutdown();
        }

        let intake_fenced = callback.intake_paused();
        self.source_batches_buf.clear();
        self.pending_watermark_batches.clear();
        self.barrier_seen.clear();
        if intake_fenced || !self.replay_pending {
            self.discard_pending_offsets();
        }
        let mut drain_events = 0_u64;

        // A message parked by the intake-gate race is open-epoch data.
        if let Some(msg) = self.parked_source_msg.take() {
            if !halted && fault.is_none() && !intake_fenced {
                if let Some(reason) = self.process_shutdown_msg(msg, callback, &mut drain_events) {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        // Closing the watch senders releases source tasks after they consume any exact commit
        // broadcast settled above. Keep draining their data channel while they finish so a task
        // blocked on a full channel cannot deadlock shutdown.
        let mut stopping_sources = Vec::with_capacity(self.source_handles.len());
        for handle in std::mem::take(&mut self.source_handles) {
            let SourceHandle {
                task,
                epoch_committed_tx,
                ..
            } = handle;
            drop(epoch_committed_tx);
            stopping_sources.push(task);
        }

        let source_deadline = tokio::time::Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
        let mut source_channel_closed = false;
        while stopping_sources.iter().any(|task| !task.is_finished())
            && tokio::time::Instant::now() < source_deadline
        {
            while let Ok(msg) = self.rx.try_recv() {
                if !halted && fault.is_none() && !intake_fenced {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
            }

            if stopping_sources.iter().all(SourceTaskLease::is_finished) {
                break;
            }

            let tick = SHUTDOWN_COMPLETION_TICK
                .min(source_deadline.saturating_duration_since(tokio::time::Instant::now()));
            if source_channel_closed {
                // A disconnected receive is immediately ready. Keep yielding so the stable task
                // actor and tracker proofs can publish terminal completion on a current-thread
                // runtime.
                tokio::time::sleep(tick).await;
                continue;
            }
            match tokio::time::timeout(tick, self.rx.recv()).await {
                Ok(Ok(msg)) if !halted && fault.is_none() && !intake_fenced => {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
                Ok(Err(_)) => source_channel_closed = true,
                Ok(Ok(_)) | Err(_) => {}
            }
        }

        for source in stopping_sources {
            Self::reap_source_task(source);
        }

        // Capture messages enqueued immediately before the last source task exited.
        while let Ok(msg) = self.rx.try_recv() {
            if !halted && fault.is_none() && !intake_fenced {
                if let Some(reason) = self.process_shutdown_msg(msg, callback, &mut drain_events) {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        #[cfg(feature = "cluster")]
        if !halted && fault.is_none() && !intake_fenced {
            if let Err(error) = self.require_process_authority("folding the shutdown source drain")
            {
                self.source_batches_buf.clear();
                self.pending_watermark_batches.clear();
                self.discard_pending_offsets();
                fault = Some(error.to_string());
            }
        }

        if !halted && fault.is_none() && !intake_fenced {
            let staged_source_progress = !self.pending_watermark_batches.is_empty();
            let watermark_result =
                self.pending_watermark_batches
                    .drain(..)
                    .try_for_each(|pending| {
                        callback.reconcile_source_input_channels(
                            &pending.source_name,
                            pending.input_channels,
                        )?;
                        callback.extract_watermark(
                            &pending.source_name,
                            &pending.batch,
                            pending.admission_floor,
                        )
                    });
            let watermarks_valid = match watermark_result {
                Ok(()) => true,
                Err(error) => {
                    self.discard_pending_offsets();
                    fault = Some(error.to_string());
                    false
                }
            };
            if watermarks_valid {
                callback.tick_idle_watermark();
            }
            if watermarks_valid
                && (!self.source_batches_buf.is_empty()
                    || staged_source_progress
                    || self.replay_pending
                    || callback.has_deferred_input())
            {
                let cycle_start = Instant::now();
                let wm = callback.current_watermark();
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority(
                    "operator execution during the shutdown source drain",
                ) {
                    self.discard_pending_offsets();
                    fault = Some(error.to_string());
                }
                if fault.is_none() {
                    let execute_started = Instant::now();
                    let execute_result = callback.execute_cycle(&self.source_batches_buf, wm).await;
                    let execute_ns =
                        u64::try_from(execute_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    match execute_result {
                        Ok(out) if out.any_failed && callback.fault_on_cycle_error() => {
                            self.discard_pending_offsets();
                            fault = Some(
                        "isolated domain fault during shutdown drain under replay guarantee"
                            .to_string(),
                    );
                        }
                        Ok(out) => match self.publish_cycle_outputs(callback, &out).await {
                            Ok(publication) => {
                                callback.record_cycle_phases(
                                    execute_ns,
                                    publication.output_store_ns,
                                    publication.sink_enqueue_ns,
                                );
                                if out.any_failed {
                                    callback.note_cycle_error();
                                }
                            }
                            Err(CycleError::Halt(reason)) => {
                                self.discard_pending_offsets();
                                halted = true;
                                halt_reason.get_or_insert(reason.clone());
                                fault = None;
                                tracing::warn!(
                                    %reason,
                                    "[LDB-3022] output publication halted during shutdown drain"
                                );
                            }
                            Err(CycleError::Recovery(reason) | CycleError::Fatal(reason)) => {
                                fault = Some(format!(
                            "cycle output publication failed during shutdown drain: {reason}"
                        ));
                            }
                        },
                        Err(CycleError::Halt(reason)) => {
                            self.discard_pending_offsets();
                            halted = true;
                            halt_reason.get_or_insert(reason.clone());
                            fault = None;
                            tracing::warn!(%reason, "[LDB-3022] cycle halted during shutdown drain");
                        }
                        Err(CycleError::Recovery(reason)) => {
                            self.discard_pending_offsets();
                            fault = Some(format!(
                        "shared pipeline infrastructure failed during shutdown drain: {reason}"
                    ));
                        }
                        Err(CycleError::Fatal(reason)) if callback.fault_on_cycle_error() => {
                            self.discard_pending_offsets();
                            fault = Some(format!(
                                "fatal SQL cycle error during shutdown drain: {reason}"
                            ));
                        }
                        Err(CycleError::Fatal(reason)) => {
                            self.discard_pending_offsets();
                            callback.note_cycle_error();
                            tracing::warn!(%reason, "[LDB-3020] SQL cycle error during shutdown drain");
                        }
                    }
                }
                let elapsed_ns =
                    u64::try_from(cycle_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                callback.record_cycle(drain_events, 0, elapsed_ns);
            }
        }

        // Resolve the durable open-epoch witness before close terminates the actor that owns its
        // rollback. On failure, keep actors live: lifecycle teardown retains their stable handles
        // and retries settlement before issuing close.
        let sink_epoch_settled = match callback.settle_sink_epoch_for_shutdown().await {
            Ok(()) => true,
            Err(error) => {
                if halted {
                    tracing::warn!(
                        %error,
                        "sink epoch settlement also failed after a permanent halt"
                    );
                } else {
                    match fault.as_mut() {
                        Some(existing) => {
                            existing.push_str("; sink epoch settlement also failed: ");
                            existing.push_str(&error);
                        }
                        None => fault = Some(format!("sink epoch settlement failed: {error}")),
                    }
                }
                false
            }
        };

        // No final snapshot is synthesized: open-epoch rows deliberately replay from the last
        // committed cut. Sink close confirms queued writes and releases connector resources only
        // after durable epoch ownership is settled.
        if sink_epoch_settled {
            if let Err(close_error) = callback.close_sinks().await {
                if halted {
                    tracing::warn!(
                        error = %close_error,
                        "sink shutdown also failed after a permanent halt"
                    );
                } else if callback.fault_on_cycle_error() {
                    match fault.as_mut() {
                        Some(existing) => {
                            existing.push_str("; sink shutdown also failed: ");
                            existing.push_str(&close_error);
                        }
                        None => fault = Some(format!("sink shutdown failed: {close_error}")),
                    }
                } else {
                    callback.note_cycle_error();
                    tracing::warn!(
                        error = %close_error,
                        "sink shutdown failed under best-effort delivery"
                    );
                }
            }
        }

        if let Some(reason) = callback.take_pipeline_halt() {
            tracing::warn!(%reason, "[LDB-3022] shutdown retained a permanent pipeline halt");
            halted = true;
            halt_reason.get_or_insert(reason);
            fault = None;
        }
        if halted {
            halt_reason.get_or_insert_with(|| "pipeline halted after a permanent error".into());
            fault = None;
        }
        let exit = if let Some(reason) = halt_reason {
            ExitReason::Halt(reason)
        } else {
            fault.map_or(ExitReason::Shutdown, ExitReason::Fault)
        };
        let reason = match &exit {
            ExitReason::Shutdown => {
                "pipeline stopped; discard subscription rows after the last committed progress frontier"
            }
            ExitReason::Halt(error) | ExitReason::Fault(error) => error,
        };
        callback.invalidate_subscriptions(reason);
        exit
    }
}
