//! Barrier alignment for multi-input operators.
//!
//! Operators with multiple inputs must align checkpoint barriers before
//! snapshotting. When a barrier arrives on one input, events from that
//! input are buffered until the same barrier arrives on all inputs.
//!
//! ## Algorithm
//!
//! 1. Barrier arrives on input `i` → mark bit `i` in the bitset
//! 2. Buffer all subsequent events from input `i`
//! 3. Once all bits are set → emit `AlignmentAction::Aligned`
//! 4. Drain buffered events from all inputs
//!
//! ## Capacity
//!
//! Uses a `u128` bitset supporting up to 128 inputs. This is sufficient
//! for even large fan-in operators (e.g., repartition after shuffle).

use super::barrier::{CheckpointBarrier, StreamMessage};

/// Action returned by [`BarrierAligner::process`].
#[derive(Debug, PartialEq)]
pub enum AlignmentAction<T> {
    /// Forward this event downstream (no barrier pending on this input).
    Forward(T),
    /// Buffer this event (barrier pending on this input, waiting for others).
    Buffer,
    /// All inputs have received the barrier — time to snapshot.
    Aligned(CheckpointBarrier),
    /// A buffered event to drain after alignment completes.
    Drain(T),
    /// A watermark that should be forwarded regardless of alignment state.
    WatermarkPassThrough(i64),
}

/// Aligns checkpoint barriers across multiple inputs.
///
/// Each input is identified by an index `0..num_inputs`. The aligner
/// tracks which inputs have received the current barrier using a `u128`
/// bitset, and buffers events from aligned inputs until all inputs
/// have reported.
pub struct BarrierAligner<T> {
    /// Number of inputs to align.
    num_inputs: u8,
    /// Bitset of inputs that have received the current barrier.
    received: u128,
    /// Per-input event buffers (reused across alignments).
    buffers: Vec<Vec<T>>,
    /// The barrier we're currently aligning on (if any).
    pending_barrier: Option<CheckpointBarrier>,
    /// Drain queue — flattened from per-input buffers after alignment.
    /// Stored in reverse order so `pop()` returns FIFO.
    drain_queue: Vec<T>,
}

impl<T> BarrierAligner<T> {
    /// Create a new aligner for the given number of inputs.
    ///
    /// # Panics
    ///
    /// Panics if `num_inputs` is 0 or exceeds 128.
    #[must_use]
    pub fn new(num_inputs: u8) -> Self {
        assert!(num_inputs > 0, "num_inputs must be > 0");
        assert!(num_inputs <= 128, "num_inputs must be <= 128");

        Self {
            num_inputs,
            received: 0,
            buffers: (0..num_inputs).map(|_| Vec::new()).collect(),
            pending_barrier: None,
            drain_queue: Vec::new(),
        }
    }

    /// Process a message from the given input.
    ///
    /// Returns an [`AlignmentAction`] telling the operator what to do.
    ///
    /// # Panics
    ///
    /// Panics if `input` is >= the configured number of inputs.
    pub fn process(&mut self, input: u8, message: StreamMessage<T>) -> AlignmentAction<T> {
        assert!(
            input < self.num_inputs,
            "input index {input} >= num_inputs {}",
            self.num_inputs
        );

        match message {
            StreamMessage::Watermark(ts) => AlignmentAction::WatermarkPassThrough(ts),

            StreamMessage::Barrier(barrier) => {
                // Mark this input as having received the barrier
                self.received |= 1u128 << input;
                self.pending_barrier = Some(barrier);

                // Check if all inputs have aligned
                let all_mask = if self.num_inputs == 128 {
                    u128::MAX
                } else {
                    (1u128 << self.num_inputs) - 1
                };

                if self.received & all_mask == all_mask {
                    // All inputs aligned — prepare drain
                    self.prepare_drain();
                    let b = self.pending_barrier.take().unwrap();
                    self.received = 0;
                    AlignmentAction::Aligned(b)
                } else {
                    AlignmentAction::Buffer
                }
            }

            StreamMessage::Event(event) => {
                let input_aligned = self.received & (1u128 << input) != 0;

                if input_aligned {
                    // This input already sent a barrier — buffer
                    self.buffers[input as usize].push(event);
                    AlignmentAction::Buffer
                } else {
                    // No barrier yet on this input — forward
                    AlignmentAction::Forward(event)
                }
            }
        }
    }

    /// Drain the next buffered event after alignment.
    ///
    /// Call this repeatedly after receiving `AlignmentAction::Aligned`
    /// to get all buffered events. Returns `None` when the drain queue
    /// is empty. Events are returned in FIFO order (the queue is
    /// reversed internally so `pop()` returns the oldest event).
    pub fn drain_next(&mut self) -> Option<AlignmentAction<T>> {
        self.drain_queue.pop().map(AlignmentAction::Drain)
    }

    /// Whether there are buffered events waiting to be drained.
    #[must_use]
    pub fn has_pending_drain(&self) -> bool {
        !self.drain_queue.is_empty()
    }

    /// Whether we're currently waiting for barriers to align.
    #[must_use]
    pub fn is_aligning(&self) -> bool {
        self.pending_barrier.is_some()
    }

    /// Number of inputs that have sent the current barrier.
    #[must_use]
    pub fn aligned_count(&self) -> u32 {
        self.received.count_ones()
    }

    /// Flatten per-input buffers into the drain queue (reversed for FIFO pop).
    fn prepare_drain(&mut self) {
        self.drain_queue.clear();

        for buf in &mut self.buffers {
            self.drain_queue.append(buf);
        }
        // Reverse so pop() returns oldest event first
        self.drain_queue.reverse();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_input_immediate_alignment() {
        let mut aligner = BarrierAligner::<String>::new(1);
        let barrier = CheckpointBarrier::new(1, 1);

        let action = aligner.process(0, StreamMessage::Barrier(barrier));
        assert!(matches!(action, AlignmentAction::Aligned(b) if b.checkpoint_id == 1));
    }

    #[test]
    fn test_two_inputs_alignment() {
        let mut aligner = BarrierAligner::<String>::new(2);
        let barrier = CheckpointBarrier::new(1, 1);

        // Event on input 0 — forward (no barrier yet)
        let action = aligner.process(0, StreamMessage::Event("e1".into()));
        assert!(matches!(action, AlignmentAction::Forward(ref s) if s == "e1"));

        // Barrier on input 0
        let action = aligner.process(0, StreamMessage::Barrier(barrier));
        assert!(matches!(action, AlignmentAction::Buffer));
        assert!(aligner.is_aligning());

        // Event on input 0 after barrier — buffered
        let action = aligner.process(0, StreamMessage::Event("e2".into()));
        assert!(matches!(action, AlignmentAction::Buffer));

        // Event on input 1 — forward (no barrier yet on this input)
        let action = aligner.process(1, StreamMessage::Event("e3".into()));
        assert!(matches!(action, AlignmentAction::Forward(ref s) if s == "e3"));

        // Barrier on input 1 — aligned!
        let action = aligner.process(1, StreamMessage::Barrier(barrier));
        assert!(matches!(action, AlignmentAction::Aligned(b) if b.checkpoint_id == 1));

        // Drain buffered events
        let drain = aligner.drain_next();
        assert!(matches!(drain, Some(AlignmentAction::Drain(ref s)) if s == "e2"));

        // No more
        assert!(aligner.drain_next().is_none());
    }

    #[test]
    fn test_watermark_passthrough() {
        let mut aligner = BarrierAligner::<String>::new(2);
        let barrier = CheckpointBarrier::new(1, 1);

        // Start aligning
        aligner.process(0, StreamMessage::Barrier(barrier));

        // Watermark passes through even during alignment
        let action = aligner.process(1, StreamMessage::Watermark(5000));
        assert!(matches!(
            action,
            AlignmentAction::WatermarkPassThrough(5000)
        ));
    }

    #[test]
    fn test_multiple_checkpoints() {
        let mut aligner = BarrierAligner::<u32>::new(2);

        // First checkpoint
        aligner.process(0, StreamMessage::Barrier(CheckpointBarrier::new(1, 1)));
        aligner.process(0, StreamMessage::Event(10));
        let action = aligner.process(1, StreamMessage::Barrier(CheckpointBarrier::new(1, 1)));
        assert!(matches!(action, AlignmentAction::Aligned(b) if b.checkpoint_id == 1));

        // Drain
        assert!(aligner.drain_next().is_some());
        assert!(aligner.drain_next().is_none());

        // Second checkpoint
        aligner.process(0, StreamMessage::Event(20));
        aligner.process(1, StreamMessage::Barrier(CheckpointBarrier::new(2, 2)));
        aligner.process(1, StreamMessage::Event(30));
        let action = aligner.process(0, StreamMessage::Barrier(CheckpointBarrier::new(2, 2)));
        assert!(matches!(action, AlignmentAction::Aligned(b) if b.checkpoint_id == 2));

        // Drain second checkpoint's buffered events
        let d = aligner.drain_next();
        assert!(matches!(d, Some(AlignmentAction::Drain(30))));
        assert!(aligner.drain_next().is_none());
    }

    #[test]
    fn test_aligned_count() {
        let mut aligner = BarrierAligner::<()>::new(4);
        assert_eq!(aligner.aligned_count(), 0);

        aligner.process(0, StreamMessage::Barrier(CheckpointBarrier::new(1, 1)));
        assert_eq!(aligner.aligned_count(), 1);

        aligner.process(2, StreamMessage::Barrier(CheckpointBarrier::new(1, 1)));
        assert_eq!(aligner.aligned_count(), 2);
    }

    #[test]
    #[should_panic(expected = "num_inputs must be > 0")]
    fn test_zero_inputs_panics() {
        let _ = BarrierAligner::<()>::new(0);
    }
}
