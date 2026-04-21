//! Chandy–Lamport barrier alignment. One `BarrierTracker` per sharded
//! operator; `observe(input, barrier)` returns `Some` once every input
//! has seen the same checkpoint id.

use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::checkpoint::barrier::CheckpointBarrier;

/// State of a single checkpoint's alignment across inputs.
#[derive(Debug)]
struct Pending {
    barrier: CheckpointBarrier,
    seen: FxHashSet<usize>,
}

/// Per-operator alignment state. Cheap to construct; one instance per
/// sharded operator.
pub struct BarrierTracker {
    inputs: usize,
    state: Mutex<FxHashMap<u64, Pending>>,
}

impl std::fmt::Debug for BarrierTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierTracker")
            .field("inputs", &self.inputs)
            .field("pending_epochs", &self.state.lock().len())
            .finish()
    }
}

impl BarrierTracker {
    /// Construct a tracker for an operator with `inputs` shuffle
    /// inputs. `inputs` must be > 0; zero-input operators have nothing
    /// to align.
    ///
    /// # Panics
    /// Panics if `inputs == 0`.
    #[must_use]
    pub fn new(inputs: usize) -> Self {
        assert!(inputs > 0, "BarrierTracker needs at least one input");
        Self {
            inputs,
            state: Mutex::new(FxHashMap::default()),
        }
    }

    /// Record that `from_input` observed `barrier`. Returns `Some` when
    /// every input has now seen a barrier for the same `checkpoint_id`
    /// — the operator should snapshot state and emit a downstream
    /// barrier. Returns `None` when alignment is still pending.
    ///
    /// Repeat observations for the same input / checkpoint are
    /// idempotent (they don't double-count).
    ///
    /// # Panics
    /// Panics if `from_input >= self.inputs`.
    pub fn observe(
        &self,
        from_input: usize,
        barrier: CheckpointBarrier,
    ) -> Option<CheckpointBarrier> {
        assert!(
            from_input < self.inputs,
            "input {from_input} >= inputs {}",
            self.inputs,
        );
        let mut state = self.state.lock();
        let entry = state
            .entry(barrier.checkpoint_id)
            .or_insert_with(|| Pending {
                barrier,
                seen: FxHashSet::default(),
            });
        entry.seen.insert(from_input);
        if entry.seen.len() == self.inputs {
            let out = entry.barrier;
            state.remove(&barrier.checkpoint_id);
            Some(out)
        } else {
            None
        }
    }

    /// Number of checkpoints currently tracked (for observability).
    #[must_use]
    pub fn pending(&self) -> usize {
        self.state.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::barrier::flags;

    fn b(cp: u64, epoch: u64) -> CheckpointBarrier {
        CheckpointBarrier {
            checkpoint_id: cp,
            epoch,
            flags: flags::FULL_SNAPSHOT,
        }
    }

    #[test]
    fn aligns_when_every_input_observed() {
        let t = BarrierTracker::new(3);
        assert!(t.observe(0, b(1, 1)).is_none());
        assert!(t.observe(1, b(1, 1)).is_none());
        let fired = t.observe(2, b(1, 1)).expect("aligned");
        assert_eq!(fired.checkpoint_id, 1);
        assert_eq!(t.pending(), 0, "state cleaned up post-alignment");
    }

    #[test]
    fn duplicate_observation_is_idempotent() {
        let t = BarrierTracker::new(2);
        assert!(t.observe(0, b(5, 2)).is_none());
        assert!(t.observe(0, b(5, 2)).is_none(), "repeat for input 0 no-op");
        let fired = t.observe(1, b(5, 2)).expect("aligned");
        assert_eq!(fired.checkpoint_id, 5);
    }

    #[test]
    fn independent_checkpoints_align_independently() {
        let t = BarrierTracker::new(2);
        // Interleave two checkpoints; neither fires until both inputs seen.
        assert!(t.observe(0, b(10, 4)).is_none());
        assert!(t.observe(0, b(11, 5)).is_none());
        assert_eq!(t.pending(), 2);
        assert_eq!(t.observe(1, b(10, 4)).unwrap().checkpoint_id, 10);
        assert_eq!(t.observe(1, b(11, 5)).unwrap().checkpoint_id, 11);
        assert_eq!(t.pending(), 0);
    }

    #[test]
    #[should_panic(expected = "input 9 >= inputs 2")]
    fn observe_rejects_out_of_range_input() {
        let t = BarrierTracker::new(2);
        let _ = t.observe(9, b(1, 1));
    }

    #[test]
    #[should_panic(expected = "at least one input")]
    fn zero_inputs_rejected() {
        let _ = BarrierTracker::new(0);
    }
}
