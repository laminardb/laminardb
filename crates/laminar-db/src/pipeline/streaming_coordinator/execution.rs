//! Mechanically extracted coordinator responsibility.

use super::{
    wait_coordinator_delay, CheckpointCompletion, CheckpointControlWake, CoordinatorGates,
    CoordinatorRunState, CoordinatorWait, CoordinatorWaitAction, CoordinatorWake, CycleError,
    Duration, ExitReason, ForceCheckpointRequest, Instant, PipelineCallback, SourceHandle,
    StreamingCoordinator, IDLE_TIMEOUT,
};

impl StreamingCoordinator {
    /// Run the coordinator loop until shutdown or a fatal cycle fault.
    ///
    /// Cycle priority: (1) shutdown, (2) drain + SQL, (3) barrier alignment,
    /// (4) checkpointing, (5) control, (6) barrier timeout.
    pub async fn run<C: PipelineCallback>(self, callback: C) -> ExitReason {
        self.run_inner(callback, None).await
    }

    /// Run the coordinator and report when its control loop is ready.
    ///
    /// Pipeline startup and coordinated recovery use this stronger boundary: constructing the
    /// compute runtime is not enough, because recovery must not acknowledge a node before barrier
    /// injection and manual-checkpoint control are installed on the live loop.
    pub(crate) async fn run_with_ready<C: PipelineCallback>(
        self,
        callback: C,
        ready: crossfire::oneshot::TxOneshot<Result<(), String>>,
    ) -> ExitReason {
        self.run_inner(callback, Some(ready)).await
    }

    pub(super) async fn wait_for_cycle<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        state: &mut CoordinatorRunState,
    ) -> CoordinatorWait {
        if let Err(error) = callback.prepare_source_intake() {
            state.fault = Some(format!(
                "source recovery handoff could not be installed before intake: {error}"
            ));
            return CoordinatorWait::stop();
        }
        let gates = CoordinatorGates::capture(callback, self.replay_pending);
        let replay_ready = self.replay_pending
            && gates.compute_admitted()
            && (self.manual_handoff_required || callback.has_runnable_deferred_input());
        let parked_ready =
            !self.replay_pending && gates.compute_admitted() && self.parked_source_msg.is_some();
        let mut retrying_replay = false;
        let mut checkpoint_control_due = false;

        let message = tokio::select! {
            biased;
            () = self.terminal_shutdown.cancelled() => return CoordinatorWait::stop(),
            () = self.shutdown.notified() => return CoordinatorWait::stop(),
            Some(source_fault) = self.source_fault_rx.recv() => {
                state.fault = Some(format!(
                    "source '{}' fault: {}",
                    source_fault.source, source_fault.error
                ));
                return CoordinatorWait::stop();
            }
            Some(completion) = async {
                if let Some(ref mut rx) = self.checkpoint_complete_rx {
                    rx.recv().await.ok()
                } else {
                    futures::future::pending::<Option<CheckpointCompletion>>().await
                }
            } => {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    state.fault = Some(error);
                    return CoordinatorWait::stop();
                }
                if !state.checkpoint_control_pending {
                    return CoordinatorWait::continue_loop();
                }
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
            }
            Some(reply) = async {
                if let Some(ref mut rx) = self.force_ckpt_rx {
                    rx.recv().await.ok()
                } else {
                    futures::future::pending::<Option<ForceCheckpointRequest>>().await
                }
            } => {
                self.manual_waiting.push(reply);
                None
            },
            () = async {
                match state.checkpoint_control_wake.as_mut() {
                    Some(wake) => wake.wait_until(state.checkpoint_control_poll_at).await,
                    None => std::future::pending().await,
                }
            }, if !state.checkpoint_control_pending && !callback.is_leader() => {
                state.checkpoint_control_pending = true;
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
            },
            () = tokio::time::sleep_until(state.checkpoint_control_poll_at),
                if state.checkpoint_control_pending && !callback.is_leader() =>
            {
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
            },
            () = std::future::ready(()), if replay_ready => {
                retrying_replay = true;
                None
            },
            () = std::future::ready(()), if parked_ready => self.parked_source_msg.take(),
            msg = self.rx.recv(),
                if state.source_channel_expected
                    && gates.compute_admitted() =>
            {
                if let Ok(message) = msg {
                    if !state.batch_window.is_zero() {
                        let authority_lost = wait_coordinator_delay(
                            state.batch_window,
                            #[cfg(feature = "cluster")]
                            self.process_authority.as_deref(),
                        )
                        .await;
                        if authority_lost {
                            state.fault = Some(
                                "cluster process lease expired during source batch window".into(),
                            );
                            return CoordinatorWait::stop();
                        }
                    }
                    Some(message)
                } else {
                    state.fault = Some("all configured source tasks exited unexpectedly".into());
                    return CoordinatorWait::stop();
                }
            }
            () = async {
                match state.shuffle_work_wake.as_ref() {
                    Some(wake) => wake.notified().await,
                    None => std::future::pending().await,
                }
            }, if !gates.external_commit_backpressured => None,
            authority_lost = wait_coordinator_delay(
                IDLE_TIMEOUT,
                #[cfg(feature = "cluster")]
                self.process_authority.as_deref(),
            ) => {
                if authority_lost {
                    state.fault =
                        Some("cluster process lease expired while coordinator was idle".into());
                    return CoordinatorWait::stop();
                }
                None
            },
        };

        CoordinatorWait::cycle(CoordinatorWake {
            message,
            retrying_replay,
            checkpoint_control_due,
            gates,
        })
    }

    pub(super) async fn service_background_work<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        state: &mut CoordinatorRunState,
        checkpoint_control_due: bool,
    ) -> bool {
        if self.replay_pending && self.pending_barrier.active {
            let deadline = self
                .pending_barrier
                .attempt_deadline(self.config.checkpoint_timeout);
            if let Err(error) = callback.drain_checkpoint_edges_until(deadline).await {
                let reason = error.to_string();
                let terminal = matches!(&error, CycleError::Halt(_));
                if terminal {
                    state.halt(reason.clone());
                }
                tracing::error!(%error, "checkpoint replay drain failed");
                if let Err(cleanup) = self
                    .cancel_pending_barrier_for_stop(callback, &reason, true)
                    .await
                {
                    if terminal {
                        tracing::warn!(
                            %cleanup,
                            "checkpoint cleanup also failed after a permanent replay-drain halt"
                        );
                    } else {
                        state.fault = Some(format!(
                            "{reason}; checkpoint cleanup failed after replay drain: {cleanup}"
                        ));
                    }
                    return false;
                }
                match error {
                    CycleError::Halt(_) => {}
                    CycleError::Fatal(_) | CycleError::Recovery(_) => state.fault = Some(reason),
                }
                return false;
            }
            if let Err(error) = self.commit_pending_offsets() {
                state.fault = Some(error.to_string());
                return false;
            }
            self.replay_pending = false;
        }

        for (source_idx, barrier, checkpoint) in &state.barriers {
            match self
                .handle_barrier(*source_idx, barrier, checkpoint, callback)
                .await
            {
                Ok(()) => {}
                Err(CycleError::Halt(reason)) => {
                    tracing::warn!(%reason, "[LDB-3022] checkpoint drain halted the pipeline");
                    state.halt(reason);
                    return false;
                }
                Err(CycleError::Fatal(reason) | CycleError::Recovery(reason)) => {
                    state.fault = Some(reason);
                    return false;
                }
            }
        }
        if self.terminal_shutdown.is_cancelled() {
            return false;
        }
        if let Some(reason) = callback.take_pipeline_halt() {
            self.discard_pending_offsets();
            tracing::warn!(
                reason = %reason,
                "[LDB-3022] permanent pipeline error; stopping without recovery"
            );
            state.halt(reason);
            return false;
        }
        if let Some(reason) = callback.take_pipeline_fault() {
            self.discard_pending_offsets();
            tracing::error!(
                reason = %reason,
                "[LDB-3024] pipeline consistency fault; stopping for recovery"
            );
            state.fault = Some(reason);
            return false;
        }

        let follower_control_ready =
            state.checkpoint_control_pending && checkpoint_control_due && !callback.is_leader();
        let checkpoint_work_due =
            callback.is_leader() || follower_control_ready || !self.manual_waiting.is_empty();
        if checkpoint_work_due {
            // Admission can already have reserved an exact attempt and begun its durable artifact
            // inventory before Prepare publication blocks on external control-plane I/O. A
            // recovery stop must still reach `finish_run`: already-spawned tails are settled
            // there, while an interrupted pre-tail attempt remains visible for coordinated
            // recovery's authoritative artifact reconciliation. Do not synthesize a local
            // completion for that ambiguous publication.
            let terminal_shutdown = self.terminal_shutdown.clone();
            let control_serviced = tokio::select! {
                biased;
                () = terminal_shutdown.cancelled() => return false,
                serviced = self.maybe_checkpoint(callback) => serviced,
            };
            if follower_control_ready {
                #[cfg(feature = "cluster")]
                if let Some(wake) = state.checkpoint_control_wake.as_ref() {
                    if control_serviced {
                        state.checkpoint_control_pending = false;
                        state.checkpoint_control_poll_at =
                            tokio::time::Instant::now() + wake.fallback();
                    } else {
                        state.checkpoint_control_poll_at =
                            tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                    }
                }
                #[cfg(not(feature = "cluster"))]
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at = tokio::time::Instant::now()
                        + if control_serviced {
                            CheckpointControlWake::fallback()
                        } else {
                            CheckpointControlWake::capacity_retry()
                        };
                    state.checkpoint_control_pending &= !control_serviced;
                }
            }
            if let Some(reason) = callback.take_pipeline_halt() {
                self.discard_pending_offsets();
                tracing::warn!(
                    reason = %reason,
                    "[LDB-3022] checkpoint control observed a permanent pipeline error"
                );
                state.halt(reason);
                return false;
            }
            if let Some(reason) = callback.take_pipeline_fault() {
                self.discard_pending_offsets();
                tracing::error!(
                    reason = %reason,
                    "[LDB-3024] checkpoint control fault; stopping for recovery"
                );
                state.fault = Some(reason);
                return false;
            }
        }

        while let Ok(message) = self.control_rx.try_recv() {
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("pipeline control mutation") {
                state.fault = Some(error.to_string());
                return false;
            }
            callback.apply_control(message);
        }

        if self.pending_barrier.active
            && tokio::time::Instant::now()
                >= self
                    .pending_barrier
                    .attempt_deadline(self.config.checkpoint_timeout)
        {
            if let Err(error) = self
                .cancel_pending_barrier_for_stop(callback, "source barrier alignment timeout", true)
                .await
            {
                state.fault = Some(error);
                return false;
            }
        }
        true
    }

    pub(super) async fn run_inner<C: PipelineCallback>(
        mut self,
        mut callback: C,
        ready: Option<crossfire::oneshot::TxOneshot<Result<(), String>>>,
    ) -> ExitReason {
        /// Maximum messages to drain per cycle before yielding for background work.
        const MAX_DRAIN_PER_CYCLE: usize = 10_000;

        let injectors = self
            .source_handles
            .iter()
            .map(SourceHandle::barrier_control)
            .collect();
        callback.set_barrier_injectors(injectors);
        let cancelled_before_ready = self.terminal_shutdown.is_cancelled();
        if let Some(ready) = ready {
            if cancelled_before_ready {
                ready.send(Err(
                    "pipeline runtime generation was cancelled before readiness".into(),
                ));
            } else {
                ready.send(Ok(()));
            }
        }
        if !cancelled_before_ready && !self.terminal_shutdown.is_cancelled() {
            // Readiness is the linearization point: source tasks are released only after it is
            // published, so none can enqueue a batch, barrier, or fault on the pre-ready side.
            for handle in &mut self.source_handles {
                if let Some(activation) = handle.startup_activation.take() {
                    activation.send(());
                }
            }
        }

        let intake_was_paused = callback.intake_paused();
        let mut state = CoordinatorRunState {
            batch_window: self.config.batch_window,
            checkpoint_control_wake: callback.checkpoint_control_wake(),
            shuffle_work_wake: callback.shuffle_work_wake(),
            checkpoint_control_poll_at: tokio::time::Instant::now(),
            checkpoint_control_pending: false,
            intake_was_paused,
            barriers: Vec::new(),
            fault: None,
            halted: false,
            halt_reason: None,
            source_channel_expected: !self.source_names.is_empty(),
        };

        loop {
            if self.terminal_shutdown.is_cancelled() {
                break;
            }
            let wait = self.wait_for_cycle(&mut callback, &mut state).await;
            match wait.action {
                CoordinatorWaitAction::Cycle => {}
                CoordinatorWaitAction::Continue => continue,
                CoordinatorWaitAction::Stop => break,
            }
            let CoordinatorWake {
                message: msg,
                retrying_replay,
                checkpoint_control_due,
                gates,
            } = wait.wake;
            // Recheck after the await: recovery may have closed the gate after this loop removed
            // a message from the source FIFO. Keep that message ahead of later FIFO entries so a
            // transient close/reopen cannot silently lose it. A fenced shutdown still discards
            // all open-epoch data below, where recovery owns the rewind.
            let intake_blocked = gates.intake_paused || callback.intake_paused();
            self.observe_intake_gate_for_checkpoint_cadence(&mut state, intake_blocked);
            if intake_blocked {
                if !self
                    .service_paused_intake(&mut callback, &mut state, msg)
                    .await
                {
                    break;
                }
                continue;
            }
            if gates.external_commit_backpressured {
                if !self
                    .service_external_commit_backpressure(
                        &mut callback,
                        &mut state,
                        checkpoint_control_due,
                    )
                    .await
                {
                    break;
                }
                continue;
            }

            if !self.replay_pending {
                if let Err(error) = callback.pin_source_frontiers_for_new_cycle() {
                    state.fault = Some(format!(
                        "decision-bound source frontiers could not be pinned before intake: {error}"
                    ));
                    break;
                }
            }

            self.source_batches_buf.clear();
            self.reset_barrier_seen_for_cycle();
            if !retrying_replay && !self.replay_pending {
                self.discard_pending_offsets();
            }
            state.barriers.clear();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            let had_data = msg.is_some();
            if let Some(first_msg) = msg {
                if let Err(error) = self.process_msg(
                    first_msg,
                    &mut callback,
                    &mut state.barriers,
                    &mut cycle_events,
                ) {
                    match error {
                        CycleError::Halt(reason) => {
                            tracing::warn!(%reason, "[LDB-3022] source staging halted");
                            state.halt(reason);
                        }
                        CycleError::Fatal(reason) | CycleError::Recovery(reason) => {
                            state.fault = Some(reason);
                        }
                    }
                }
            }
            if state.halted || state.fault.is_some() {
                self.discard_pending_offsets();
                break;
            }

            // Coalesce additional buffered messages; stop at count, time budget, or backpressure.
            let mut drain_count = 0;
            let drain_budget = Duration::from_nanos(self.config.drain_budget_ns);
            // `is_backpressured()` bumps a counter, so call it only on active wakeups rather than
            // idle timeouts.
            let backpressured = had_data && callback.is_backpressured();
            if backpressured {
                tracing::debug!("operator graph backpressured — skipping drain");
            }
            while !backpressured
                && drain_count < MAX_DRAIN_PER_CYCLE
                && cycle_start.elapsed() < drain_budget
            {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        if let Err(error) = self.process_msg(
                            msg,
                            &mut callback,
                            &mut state.barriers,
                            &mut cycle_events,
                        ) {
                            match error {
                                CycleError::Halt(reason) => {
                                    tracing::warn!(%reason, "[LDB-3022] source staging halted");
                                    state.halt(reason);
                                }
                                CycleError::Fatal(reason) | CycleError::Recovery(reason) => {
                                    state.fault = Some(reason);
                                }
                            }
                            break;
                        }
                        drain_count += 1;
                    }
                    Err(_) => break,
                }
            }
            if let Ok(source_fault) = self.source_fault_rx.try_recv() {
                state.fault = Some(format!(
                    "source '{}' fault: {}",
                    source_fault.source, source_fault.error
                ));
            }
            if state.halted || state.fault.is_some() {
                self.discard_pending_offsets();
                break;
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("folding a drained source cycle") {
                self.discard_pending_offsets();
                state.fault = Some(error.to_string());
                break;
            }

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
            if let Err(error) = watermark_result {
                self.discard_pending_offsets();
                match error {
                    CycleError::Halt(reason) => {
                        tracing::warn!(%reason, "[LDB-3022] watermark processing halted");
                        state.halt(reason);
                    }
                    CycleError::Fatal(reason) | CycleError::Recovery(reason) => {
                        state.fault = Some(reason);
                    }
                }
                break;
            }

            if !self.replay_pending {
                callback.tick_idle_watermark();
            }

            // Run empty cycles for filtered source progress and deferred operator work so cursors,
            // watermarks, and retained data do not stall when a source goes quiet.
            if !self.source_batches_buf.is_empty()
                || staged_source_progress
                || self.replay_pending
                || callback.has_deferred_input()
            {
                let wm = callback.current_watermark();
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority("operator execution") {
                    self.discard_pending_offsets();
                    state.fault = Some(error.to_string());
                    break;
                }
                let execute_started = Instant::now();
                let execute_result = callback.execute_cycle(&self.source_batches_buf, wm).await;
                let execute_ns =
                    u64::try_from(execute_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                match execute_result {
                    Ok(out) => {
                        // Durable delivery rewinds the whole pipeline, so don't partial-commit
                        // siblings — recover instead.
                        if out.any_failed && callback.fault_on_cycle_error() {
                            self.discard_pending_offsets();
                            tracing::error!(
                                "[LDB-3021] failure domain faulted; faulting for recovery"
                            );
                            state.fault =
                                Some("isolated domain fault (durable delivery)".to_string());
                            break;
                        }
                        let publication =
                            match self.publish_cycle_outputs(&mut callback, &out).await {
                                Ok(publication) => publication,
                                Err(CycleError::Halt(reason)) => {
                                    tracing::warn!(
                                        %reason,
                                        "[LDB-3022] cycle output publication halted"
                                    );
                                    state.halt(reason);
                                    break;
                                }
                                Err(CycleError::Recovery(reason) | CycleError::Fatal(reason)) => {
                                    tracing::error!(
                                        error = %reason,
                                        "cycle output publication failed; faulting for recovery"
                                    );
                                    state.fault = Some(reason);
                                    break;
                                }
                            };
                        callback.record_cycle_phases(
                            execute_ns,
                            publication.output_store_ns,
                            publication.sink_enqueue_ns,
                        );
                        if out.any_failed {
                            callback.note_cycle_error();
                            tracing::warn!(
                                "[LDB-3020] failure domain dropped (best-effort: continuing)"
                            );
                        }
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        match e {
                            CycleError::Recovery(msg) => {
                                tracing::error!(
                                    error = %msg,
                                    "shared pipeline infrastructure failed; faulting for recovery"
                                );
                                state.fault = Some(msg);
                                break;
                            }
                            // Shutdown already signaled; restarting would just re-trip it.
                            CycleError::Halt(msg) => {
                                tracing::warn!(reason = %msg, "[LDB-3022] cycle halted");
                                state.halt(msg);
                                break;
                            }
                            // Continuing would drop drained rows, so durable delivery faults for
                            // recovery.
                            CycleError::Fatal(msg) if callback.fault_on_cycle_error() => {
                                tracing::error!(
                                    error = %msg,
                                    "[LDB-3021] fatal SQL cycle error; faulting for recovery"
                                );
                                state.fault = Some(msg);
                                break;
                            }
                            // Best effort: drop the bad cycle and continue.
                            CycleError::Fatal(msg) => {
                                callback.note_cycle_error();
                                tracing::warn!(
                                    error = %msg,
                                    "[LDB-3020] SQL cycle error (best-effort: continuing)"
                                );
                            }
                        }
                    }
                }
                let elapsed_ns =
                    u64::try_from(cycle_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                callback.record_cycle(cycle_events, 0, elapsed_ns);

                if elapsed_ns >= self.config.cycle_budget_ns {
                    tracing::debug!(
                        elapsed_ms = elapsed_ns / 1_000_000,
                        budget_ms = self.config.cycle_budget_ns / 1_000_000,
                        "cycle budget exceeded — proceeding to background work"
                    );
                }
            }

            if !self
                .service_background_work(&mut callback, &mut state, checkpoint_control_due)
                .await
            {
                break;
            }
        }

        self.finish_run(&mut callback, state.fault, state.halted, state.halt_reason)
            .await
    }
}
