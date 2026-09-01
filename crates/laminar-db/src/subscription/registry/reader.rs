//! Cursor ownership and progress through a shared subscription log.

use std::sync::Arc;

use tokio::sync::watch;

use super::{head_sequence, reclaim_consumed_prefix, ChargedUpdate, MvUpdate, StreamLog};

pub(in crate::subscription) enum SubscriptionRead {
    Update {
        sequence: u64,
        update: ChargedUpdate,
    },
    Lagged(u64),
    Terminal(String),
}

pub(super) enum TryRead {
    Ready(SubscriptionRead),
    Pending,
}

pub(crate) struct SubscriptionReader {
    log: Arc<StreamLog>,
    reader_id: u64,
    cursor: u64,
    /// `(epoch, physical sequence)` of the retained progress marker that an
    /// AS-OF reader must skip while still replaying post-cut rows sequenced before it.
    skip_barrier: Option<(u64, u64)>,
    wake: watch::Receiver<()>,
    registered: bool,
}

impl std::fmt::Debug for SubscriptionReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubscriptionReader")
            .field("reader_id", &self.reader_id)
            .field("cursor", &self.cursor)
            .field("skip_barrier", &self.skip_barrier)
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}

impl SubscriptionReader {
    pub(super) fn attached(
        log: Arc<StreamLog>,
        reader_id: u64,
        cursor: u64,
        skip_barrier: Option<(u64, u64)>,
        wake: watch::Receiver<()>,
    ) -> Self {
        Self {
            log,
            reader_id,
            cursor,
            skip_barrier,
            wake,
            registered: true,
        }
    }

    pub(in crate::subscription) async fn next(&mut self) -> SubscriptionRead {
        loop {
            match self.try_read() {
                TryRead::Ready(read) => return read,
                TryRead::Pending => {
                    if self.wake.changed().await.is_err() {
                        return SubscriptionRead::Terminal(
                            "subscription shared log closed unexpectedly".into(),
                        );
                    }
                }
            }
        }
    }

    pub(super) fn try_read(&mut self) -> TryRead {
        let mut inner = self.log.inner.lock();
        if let Some(message) = &inner.terminal_error {
            return TryRead::Ready(SubscriptionRead::Terminal(message.clone()));
        }
        if inner.readers.get(&self.reader_id).copied() != Some(self.cursor) {
            return TryRead::Ready(SubscriptionRead::Terminal(
                "subscription reader cursor registration invariant failed".into(),
            ));
        }
        let head = head_sequence(&inner);
        if self.cursor < head {
            let mut skipped = head.saturating_sub(self.cursor);
            if self
                .skip_barrier
                .is_some_and(|(_, sequence)| sequence >= self.cursor && sequence < head)
            {
                skipped = skipped.saturating_sub(1);
                self.skip_barrier = None;
                if skipped == 0 {
                    self.cursor = head;
                    inner.readers.insert(self.reader_id, self.cursor);
                    reclaim_consumed_prefix(&mut inner);
                    drop(inner);
                    return self.try_read();
                }
            }
            return TryRead::Ready(SubscriptionRead::Lagged(skipped));
        }

        if self.cursor < inner.next_sequence {
            let Ok(index) = usize::try_from(self.cursor.saturating_sub(head)) else {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log index exceeds addressable memory".into(),
                ));
            };
            let Some(entry) = inner.entries.get(index) else {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log sequence invariant failed".into(),
                ));
            };
            if entry.sequence != self.cursor {
                return TryRead::Ready(SubscriptionRead::Terminal(
                    "subscription shared log is not contiguous".into(),
                ));
            }
            let sequence = entry.sequence;
            let update = entry.update.clone();
            let skip = self.skip_barrier.is_some_and(|(epoch, sequence)| {
                sequence == entry.sequence
                    &&
                matches!(update.as_ref(), MvUpdate::Barrier { epoch: seen, .. } if *seen == epoch)
            });
            self.cursor = self.cursor.saturating_add(1);
            inner.readers.insert(self.reader_id, self.cursor);
            reclaim_consumed_prefix(&mut inner);
            drop(inner);
            if skip {
                self.skip_barrier = None;
                return self.try_read();
            }
            return TryRead::Ready(SubscriptionRead::Update { sequence, update });
        }

        TryRead::Pending
    }

    fn release(&mut self) {
        if !self.registered {
            return;
        }
        let mut inner = self.log.inner.lock();
        inner.readers.remove(&self.reader_id);
        reclaim_consumed_prefix(&mut inner);
        drop(inner);
        self.registered = false;
    }
}

impl Drop for SubscriptionReader {
    fn drop(&mut self) {
        self.release();
    }
}
