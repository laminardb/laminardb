//! Committed cuts as in-process readers see them, and the pins that hold a cut against local
//! checkpoint retention while a read loads its artifacts.

use std::sync::Arc;
use std::time::Instant;

use laminar_core::checkpoint::{
    CheckpointManifest, CommittedCheckpointIndex, CommittedCheckpointRef,
};

use super::CheckpointCoordinator;

/// Most reads one process may pin at once.
const MAX_READ_PINS: usize = 1_024;

/// One committed cut: its index, the reference that names it, and when it was taken.
#[derive(Debug)]
pub(crate) struct CommittedCut {
    pub(crate) index: CommittedCheckpointIndex,
    pub(crate) reference: CommittedCheckpointRef,
    /// Earliest participant capture time of the cut, in Unix milliseconds.
    pub(crate) taken_at_ms: u64,
}

impl CommittedCut {
    pub(crate) fn new<'a>(
        index: CommittedCheckpointIndex,
        reference: CommittedCheckpointRef,
        manifests: impl IntoIterator<Item = &'a CheckpointManifest>,
    ) -> Self {
        let taken_at_ms = manifests
            .into_iter()
            .map(|manifest| manifest.timestamp_ms)
            .min()
            .unwrap_or(0);
        Self {
            index,
            reference,
            taken_at_ms,
        }
    }
}

/// The latest committed cut, replaced as each checkpoint commits or recovery installs one.
pub(crate) type CommittedCuts = tokio::sync::watch::Receiver<Option<Arc<CommittedCut>>>;

/// Committed cuts that in-process reads hold against local retention. Retention protects the
/// oldest unexpired pin older than the cut it was asked to keep, and raises a floor below which no
/// read may pin, so a pinned cut is never retired before its deadline.
#[derive(Debug, Default)]
pub(crate) struct ReadPins {
    state: parking_lot::Mutex<PinState>,
}

#[derive(Debug, Default)]
struct PinState {
    /// Epochs below this floor may already be retired.
    floor: u64,
    next_id: u64,
    pins: Vec<PinSlot>,
}

#[derive(Debug)]
struct PinSlot {
    id: u64,
    cut: CommittedCheckpointRef,
    expires_at: Instant,
}

impl PinState {
    fn drop_expired(&mut self, now: Instant) {
        self.pins.retain(|pin| pin.expires_at > now);
    }
}

/// Outcome of pinning a committed cut.
#[derive(Debug)]
pub(crate) enum PinAcquire {
    Acquired(ReadPin),
    /// Retention may already have retired the cut.
    Pruned,
    /// Every pin slot is held.
    Capacity,
}

/// One read's hold on a committed cut. Dropping it releases the hold; a hold also lapses at its
/// deadline, after which retention may retire the cut.
#[derive(Debug)]
pub(crate) struct ReadPin {
    pins: Arc<ReadPins>,
    id: u64,
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        self.pins.state.lock().pins.retain(|pin| pin.id != self.id);
    }
}

impl ReadPins {
    /// Hold `cut` against retention until `expires_at`.
    pub(crate) fn acquire(
        self: &Arc<Self>,
        cut: &CommittedCheckpointRef,
        expires_at: Instant,
    ) -> PinAcquire {
        let mut state = self.state.lock();
        if cut.epoch < state.floor {
            return PinAcquire::Pruned;
        }
        state.drop_expired(Instant::now());
        if state.pins.len() >= MAX_READ_PINS {
            return PinAcquire::Capacity;
        }
        state.next_id += 1;
        let id = state.next_id;
        state.pins.push(PinSlot {
            id,
            cut: cut.clone(),
            expires_at,
        });
        PinAcquire::Acquired(ReadPin {
            pins: Arc::clone(self),
            id,
        })
    }

    /// The cut retention protects when asked to keep `requested`: the oldest unexpired pin when
    /// it is older. No read can pin below the returned cut afterwards.
    pub(super) fn reserve_floor(
        &self,
        requested: CommittedCheckpointRef,
    ) -> CommittedCheckpointRef {
        let mut state = self.state.lock();
        state.drop_expired(Instant::now());
        let selected = state
            .pins
            .iter()
            .map(|pin| &pin.cut)
            .filter(|cut| cut.epoch < requested.epoch)
            .min_by_key(|cut| cut.epoch)
            .cloned()
            .unwrap_or(requested);
        state.floor = state.floor.max(selected.epoch);
        selected
    }
}

#[cfg(feature = "cluster")]
impl ReadPins {
    /// Refuse every pin until [`Self::open_at`].
    pub(super) fn close(&self) {
        self.state.lock().floor = u64::MAX;
    }

    /// Admit pins at or after `epoch`, which a durable pin now protects.
    pub(super) fn open_at(&self, epoch: u64) {
        self.state.lock().floor = epoch;
    }
}

impl CheckpointCoordinator {
    /// Readers of committed cuts: the latest cut and the pins retention honours.
    pub(crate) fn committed_reads(&self) -> (CommittedCuts, Arc<ReadPins>) {
        (self.committed_cut.subscribe(), Arc::clone(&self.read_pins))
    }

    pub(super) fn publish_committed_cut(&self, cut: Option<CommittedCut>) {
        self.committed_cut.send_replace(cut.map(Arc::new));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn cut(epoch: u64) -> CommittedCheckpointRef {
        CommittedCheckpointRef {
            epoch,
            checkpoint_id: epoch,
            len: 1,
            sha256: "0".repeat(64),
        }
    }

    fn later(seconds: u64) -> Instant {
        Instant::now() + Duration::from_secs(seconds)
    }

    #[test]
    fn retention_keeps_the_oldest_pinned_cut_and_refuses_pins_below_it() {
        let pins = Arc::new(ReadPins::default());
        let PinAcquire::Acquired(held) = pins.acquire(&cut(3), later(60)) else {
            panic!("an unretired cut can be pinned");
        };
        assert_eq!(pins.reserve_floor(cut(5)).epoch, 3);
        assert!(matches!(
            pins.acquire(&cut(2), later(60)),
            PinAcquire::Pruned
        ));
        assert!(matches!(
            pins.acquire(&cut(4), later(60)),
            PinAcquire::Acquired(_)
        ));

        drop(held);
        assert_eq!(pins.reserve_floor(cut(5)).epoch, 5);
        assert!(matches!(
            pins.acquire(&cut(4), later(60)),
            PinAcquire::Pruned
        ));
    }

    #[test]
    fn an_expired_pin_no_longer_holds_retention() {
        let pins = Arc::new(ReadPins::default());
        let PinAcquire::Acquired(_held) = pins.acquire(&cut(3), Instant::now()) else {
            panic!("an unretired cut can be pinned");
        };
        assert_eq!(pins.reserve_floor(cut(5)).epoch, 5);
    }

    #[test]
    fn pins_are_bounded() {
        let pins = Arc::new(ReadPins::default());
        let held: Vec<_> = (0..MAX_READ_PINS)
            .map(|_| pins.acquire(&cut(1), later(60)))
            .collect();
        assert!(held
            .iter()
            .all(|pin| matches!(pin, PinAcquire::Acquired(_))));
        assert!(matches!(
            pins.acquire(&cut(1), later(60)),
            PinAcquire::Capacity
        ));
    }
}
