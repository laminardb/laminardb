//! `RingHandler` implementation for core threads.
//!
//! Bridges `ThreeRingReactor` ↔ SPSC inbox/outbox/Reactor, providing
//! concrete behavior for every handler method.
//!
//! - **Ring 0 (latency)**: Wakeup eventfd completions (SPSC inbox has data).
//! - **Ring 1 (main)**: WAL write/sync completions.
//! - **Ring 2 (control)**: Not wired in this phase.

use std::sync::atomic::Ordering;

use crate::budget::TaskBudget;
use crate::io_uring::three_ring::{RingHandler, RoutedCompletion};
use crate::operator::{CheckpointCompleteData, Output};
use crate::reactor::Reactor;

use super::core_handle::{CoreMessage, CoreThreadContext, TaggedOutput};

/// `RingHandler` for core threads.
///
/// Processes SPSC inbox messages via the `Reactor` operator chain and
/// routes outputs to the SPSC outbox. Handles WAL write completions
/// from the main ring and wakeup eventfd completions from the latency ring.
pub(super) struct CoreRingHandler<'a> {
    /// Shared core thread context (inbox, outbox, shutdown flag, etc.).
    ctx: &'a CoreThreadContext,
    /// The reactor operator chain.
    reactor: &'a mut Reactor,
    /// Reusable buffer for reactor poll outputs.
    poll_buffer: &'a mut Vec<Output>,
    /// Batch budget for Ring 0 processing.
    batch_budget: TaskBudget,
    /// Pending WAL write `user_data` IDs for durability tracking.
    pending_wal_writes: &'a mut Vec<u64>,
    /// Whether the wakeup eventfd needs re-arming after a latency completion.
    wakeup_needs_rearm: bool,
    /// Last source index seen (for tagging reactor outputs).
    last_source_idx: usize,
}

impl<'a> CoreRingHandler<'a> {
    /// Create a new handler.
    pub(super) fn new(
        ctx: &'a CoreThreadContext,
        reactor: &'a mut Reactor,
        poll_buffer: &'a mut Vec<Output>,
        pending_wal_writes: &'a mut Vec<u64>,
    ) -> Self {
        Self {
            ctx,
            reactor,
            poll_buffer,
            batch_budget: TaskBudget::ring0_batch(),
            pending_wal_writes,
            wakeup_needs_rearm: false,
            last_source_idx: 0,
        }
    }

    /// Push a tagged output to the outbox, incrementing the drop counter on failure.
    fn push_output(&self, tagged: TaggedOutput) {
        if self.ctx.outbox.push(tagged).is_err() {
            self.ctx.outputs_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl RingHandler for CoreRingHandler<'_> {
    fn handle_latency_completion(&mut self, _completion: RoutedCompletion) {
        // Latency ring completion: the wakeup eventfd poll fired.
        // New data is in the SPSC inbox — `process_ring0_events` will drain it.
        // Mark for re-arm so the next iteration can detect more wakeups.
        self.wakeup_needs_rearm = true;
    }

    fn handle_main_completion(&mut self, completion: RoutedCompletion) {
        // Main ring completion: WAL write or WAL sync finished.
        // Remove from pending tracking to signal durability.
        if let Some(pos) = self
            .pending_wal_writes
            .iter()
            .position(|&id| id == completion.user_data)
        {
            self.pending_wal_writes.swap_remove(pos);
        }
        if completion.result < 0 {
            tracing::error!(
                core_id = self.ctx.core_id,
                user_data = completion.user_data,
                errno = -completion.result,
                "WAL write failed"
            );
        }
    }

    fn handle_poll_completion(&mut self, completion: RoutedCompletion) {
        // Poll ring completion: storage read (NVMe).
        // Used for state store reads in future phases.
        if completion.result < 0 {
            tracing::error!(
                core_id = self.ctx.core_id,
                user_data = completion.user_data,
                errno = -completion.result,
                "Storage read failed"
            );
        }
    }

    fn process_ring0_events(&mut self) {
        // Drain inbox and process events through the reactor.
        // This is the core thread's hot path — equivalent to core_thread_main()'s inner loop.
        let mut messages_processed = 0usize;

        while let Some(message) = self.ctx.inbox.pop() {
            match message {
                CoreMessage::Event { source_idx, event } => {
                    self.last_source_idx = source_idx;
                    if let Err(e) = self.reactor.submit(event) {
                        tracing::error!("Core {}: reactor submit: {e}", self.ctx.core_id);
                    }
                    messages_processed += 1;
                }
                CoreMessage::Watermark(timestamp) => {
                    self.reactor.advance_watermark(timestamp);
                    messages_processed += 1;
                }
                CoreMessage::CheckpointRequest(checkpoint_id) => {
                    let operator_states = self.reactor.trigger_checkpoint();
                    self.push_output(TaggedOutput {
                        source_idx: 0,
                        output: Output::CheckpointComplete(Box::new(CheckpointCompleteData {
                            checkpoint_id,
                            operator_states,
                        })),
                    });
                    messages_processed += 1;
                }
                CoreMessage::Barrier {
                    source_idx,
                    barrier,
                } => {
                    // Flush reactor before forwarding barrier
                    self.flush_reactor_outputs(source_idx);
                    // Forward barrier to coordinator
                    self.push_output(TaggedOutput {
                        source_idx,
                        output: Output::Barrier(Box::new(barrier)),
                    });
                    messages_processed += 1;
                }
                CoreMessage::Shutdown => {
                    if messages_processed > 0 {
                        self.ctx.credit_gate.release(messages_processed);
                    }
                    return;
                }
            }

            if self.batch_budget.almost_exceeded() {
                break;
            }
        }

        if messages_processed > 0 {
            self.ctx.credit_gate.release(messages_processed);
        }

        // Process reactor outputs
        self.flush_reactor_outputs(self.last_source_idx);
    }

    fn ring0_idle(&self) -> bool {
        self.ctx.inbox.is_empty()
    }

    fn process_ring1_chunk(&mut self) {
        // Ring 1: WAL group commit.
        // If there are pending WAL writes and enough time has passed,
        // a sync should be submitted to the main ring.
        // Currently no-op until IoUringWalShared is wired to the core thread.
    }

    fn has_control_message(&self) -> bool {
        false
    }

    fn process_ring2(&mut self) {
        // Ring 2: control plane (reconfiguration, etc.).
        // Not wired in this phase.
    }

    fn should_sleep(&self) -> bool {
        // Sleep when inbox is empty AND no pending io_uring ops.
        // The main ring's `submit_and_wait(1)` will wake when:
        //   - A WAL write/sync completes (main ring CQE)
        //   - The wakeup eventfd fires (latency ring → cross-ring eventfd)
        self.ctx.inbox.is_empty() && self.pending_wal_writes.is_empty()
    }

    fn should_shutdown(&self) -> bool {
        self.ctx.shutdown.load(Ordering::Acquire)
    }
}

impl CoreRingHandler<'_> {
    /// Drain reactor outputs into the outbox tagged with the given source index.
    fn flush_reactor_outputs(&mut self, source_idx: usize) {
        self.poll_buffer.clear();
        self.reactor.poll_into(self.poll_buffer);
        #[allow(clippy::cast_possible_truncation)]
        self.ctx
            .events_processed
            .fetch_add(self.poll_buffer.len() as u64, Ordering::Relaxed);
        let outputs: Vec<_> = self.poll_buffer.drain(..).collect();
        for output in outputs {
            self.push_output(TaggedOutput { source_idx, output });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointBarrier;
    use crate::operator::{Event, Operator, OperatorState, OutputVec, Timer};
    use crate::reactor::ReactorConfig;
    use crate::tpc::backpressure::BackpressureConfig;
    use crate::tpc::spsc::SpscQueue;
    use crate::tpc::CreditGate;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;

    struct PassthroughOp;

    impl Operator for PassthroughOp {
        fn process(
            &mut self,
            event: &Event,
            _ctx: &mut crate::operator::OperatorContext,
        ) -> OutputVec {
            let mut out = OutputVec::new();
            out.push(Output::Event(event.clone()));
            out
        }

        fn on_timer(
            &mut self,
            _timer: Timer,
            _ctx: &mut crate::operator::OperatorContext,
        ) -> OutputVec {
            OutputVec::new()
        }

        fn checkpoint(&self) -> OperatorState {
            OperatorState {
                operator_id: "pt".to_string(),
                data: vec![],
            }
        }

        fn restore(&mut self, _state: OperatorState) -> Result<(), crate::operator::OperatorError> {
            Ok(())
        }
    }

    fn make_ctx() -> CoreThreadContext {
        CoreThreadContext {
            core_id: 0,
            cpu_affinity: None,
            reactor_config: ReactorConfig::default(),
            numa_aware: false,
            numa_node: 0,
            inbox: Arc::new(SpscQueue::new(1024)),
            outbox: Arc::new(SpscQueue::new(1024)),
            credit_gate: Arc::new(CreditGate::new(BackpressureConfig::default())),
            shutdown: Arc::new(AtomicBool::new(false)),
            events_processed: Arc::new(AtomicU64::new(0)),
            outputs_dropped: Arc::new(AtomicU64::new(0)),
            is_running: Arc::new(AtomicBool::new(true)),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            io_uring_config: None,
        }
    }

    fn make_event(value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(value, batch)
    }

    #[test]
    fn test_process_ring0_events() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        reactor.add_operator(Box::new(PassthroughOp));
        let mut poll_buffer = Vec::new();
        let mut pending_wal = Vec::new();

        // Push an event to inbox
        ctx.inbox
            .push(CoreMessage::Event {
                source_idx: 0,
                event: make_event(42),
            })
            .unwrap();

        let mut handler =
            CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);
        handler.process_ring0_events();

        // Should have produced output in outbox
        let output = ctx.outbox.pop();
        assert!(output.is_some());
        assert_eq!(output.unwrap().source_idx, 0);
    }

    #[test]
    fn test_should_sleep_idle() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        let mut poll_buffer = Vec::new();
        let mut pending_wal = Vec::new();

        let handler = CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);
        assert!(handler.should_sleep());
    }

    #[test]
    fn test_should_sleep_with_pending_wal() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        let mut poll_buffer = Vec::new();
        let mut pending_wal = vec![42u64];

        let handler = CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);
        assert!(!handler.should_sleep());
    }

    #[test]
    fn test_should_shutdown() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        let mut poll_buffer = Vec::new();
        let mut pending_wal = Vec::new();

        let handler = CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);
        assert!(!handler.should_shutdown());

        ctx.shutdown.store(true, Ordering::Release);
        assert!(handler.should_shutdown());
    }

    #[test]
    fn test_handle_main_completion_removes_pending() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        let mut poll_buffer = Vec::new();
        let mut pending_wal = vec![10, 20, 30];

        let mut handler =
            CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);

        let completion = RoutedCompletion {
            user_data: 20,
            result: 0,
            flags: 0,
            affinity: crate::io_uring::three_ring::RingAffinity::Main,
            submitted_at: Some(std::time::Instant::now()),
            op_type: None,
        };
        handler.handle_main_completion(completion);

        assert_eq!(handler.pending_wal_writes.len(), 2);
        assert!(!handler.pending_wal_writes.contains(&20));
    }

    #[test]
    fn test_barrier_flushes_reactor() {
        let ctx = make_ctx();
        let mut reactor = Reactor::new(ReactorConfig::default()).unwrap();
        reactor.add_operator(Box::new(PassthroughOp));
        let mut poll_buffer = Vec::new();
        let mut pending_wal = Vec::new();

        // Push event + barrier
        ctx.inbox
            .push(CoreMessage::Event {
                source_idx: 1,
                event: make_event(10),
            })
            .unwrap();
        ctx.inbox
            .push(CoreMessage::Barrier {
                source_idx: 1,
                barrier: CheckpointBarrier::new(99, 1),
            })
            .unwrap();

        let mut handler =
            CoreRingHandler::new(&ctx, &mut reactor, &mut poll_buffer, &mut pending_wal);
        handler.process_ring0_events();

        // Collect all outputs
        let mut outputs = Vec::new();
        while let Some(o) = ctx.outbox.pop() {
            outputs.push(o);
        }

        let has_barrier = outputs
            .iter()
            .any(|o| matches!(o.output, Output::Barrier(_)));
        assert!(has_barrier, "Expected barrier in outputs");

        // Barrier source_idx should be preserved
        let barrier = outputs
            .iter()
            .find(|o| matches!(o.output, Output::Barrier(_)))
            .unwrap();
        assert_eq!(barrier.source_idx, 1);
    }
}
