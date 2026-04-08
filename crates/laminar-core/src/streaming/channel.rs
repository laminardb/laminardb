//! Bounded MPSC channel backed by crossfire.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crossfire::mpsc;

use super::config::ChannelConfig;
use super::error::{RecvError, TryPushError};

type Flavor<T> = mpsc::Array<T>;

/// Sender half of a bounded channel. Cloneable for multi-producer use.
pub struct Producer<T: Send + 'static> {
    tx: crossfire::MTx<Flavor<T>>,
    watch_tx: Arc<tokio::sync::watch::Sender<()>>,
}

/// Receiver half of a bounded channel.
pub struct Consumer<T: Send + 'static> {
    rx: crossfire::Rx<Flavor<T>>,
    watch_tx: Arc<tokio::sync::watch::Sender<()>>,
}

/// Creates a bounded channel with the given buffer size.
#[must_use]
pub fn channel<T: Send + 'static>(buffer_size: usize) -> (Producer<T>, Consumer<T>) {
    let cap = buffer_size.max(2);
    let (tx, rx) = mpsc::bounded_blocking::<T>(cap);
    let (watch_tx, _) = tokio::sync::watch::channel(());
    let watch_tx = Arc::new(watch_tx);
    (
        Producer {
            tx,
            watch_tx: Arc::clone(&watch_tx),
        },
        Consumer { rx, watch_tx },
    )
}

/// Creates a bounded channel from a [`ChannelConfig`].
#[must_use]
pub(crate) fn channel_with_config<T: Send + 'static>(
    config: &ChannelConfig,
) -> (Producer<T>, Consumer<T>) {
    channel(config.buffer_size)
}

// -- Producer -----------------------------------------------------------------

impl<T: Send + 'static> Producer<T> {
    /// Non-blocking send. Returns immediately if the channel is full.
    ///
    /// # Errors
    ///
    /// Returns the item if the channel is full or the receiver was dropped.
    pub fn push(&self, item: T) -> Result<(), T> {
        self.tx
            .try_send(item)
            .map_err(crossfire::TrySendError::into_inner)?;
        self.watch_tx.send_modify(|()| {});
        Ok(())
    }

    /// Non-blocking send with typed error.
    ///
    /// # Errors
    ///
    /// Returns `TryPushError` containing the item if the channel is full.
    pub fn try_push(&self, item: T) -> Result<(), TryPushError<T>> {
        self.tx
            .try_send(item)
            .map_err(|e| TryPushError::full(e.into_inner()))?;
        self.watch_tx.send_modify(|()| {});
        Ok(())
    }

    /// Returns `true` if the receiver has been dropped.
    pub fn is_closed(&self) -> bool {
        self.tx.is_disconnected()
    }

    /// Number of items currently buffered.
    pub fn len(&self) -> usize {
        self.tx.deref().len()
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.tx.deref().capacity().unwrap_or(0)
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.tx.deref().is_empty()
    }
}

impl<T: Send + 'static> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            watch_tx: Arc::clone(&self.watch_tx),
        }
    }
}

impl<T: Send + 'static> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

// -- Consumer -----------------------------------------------------------------

impl<T: Send + 'static> Consumer<T> {
    /// Non-blocking receive.
    pub fn poll(&self) -> Option<T> {
        self.rx.try_recv().ok()
    }

    /// Blocking receive.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the sender has been dropped.
    pub fn recv(&self) -> Result<T, RecvError> {
        self.rx.recv().map_err(|_| RecvError::Disconnected)
    }

    /// Blocking receive with timeout.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` or `RecvError::Disconnected`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        self.rx.recv_timeout(timeout).map_err(|e| match e {
            crossfire::RecvTimeoutError::Timeout => RecvError::Timeout,
            crossfire::RecvTimeoutError::Disconnected => RecvError::Disconnected,
        })
    }

    /// Returns `true` if the sender has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.rx.is_disconnected()
    }

    /// Number of items currently buffered.
    pub fn len(&self) -> usize {
        self.rx.deref().len()
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.rx.deref().capacity().unwrap_or(0)
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.rx.deref().is_empty()
    }

    /// Returns a watch receiver that fires when new data may be available.
    pub fn async_watch(&self) -> tokio::sync::watch::Receiver<()> {
        self.watch_tx.subscribe()
    }
}

// SAFETY: Consumer wraps crossfire::Rx which is !Sync (contains Cell for waker cache).
// Our architecture guarantees single-consumer access: either via Direct subscription
// or Sink::poll_and_distribute, never concurrently. The Arc<SinkInner> sharing is for
// lifetime management, not concurrent access to the receiver.
unsafe impl<T: Send + 'static> Sync for Consumer<T> {}

impl<T: Send + 'static> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("len", &self.len())
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_recv() {
        let (tx, rx) = channel::<i32>(16);
        tx.push(42).unwrap();
        assert_eq!(rx.poll(), Some(42));
        assert_eq!(rx.poll(), None);
    }

    #[test]
    fn test_try_push_full() {
        let (tx, rx) = channel::<i32>(2);
        assert!(tx.try_push(1).is_ok());
        assert!(tx.try_push(2).is_ok());
        let err = tx.try_push(3);
        assert!(err.is_err());
        assert_eq!(err.unwrap_err().into_inner(), 3);
        assert_eq!(rx.poll(), Some(1));
        assert!(tx.try_push(3).is_ok());
    }

    #[test]
    fn test_disconnected_on_drop() {
        let (tx, rx) = channel::<i32>(16);
        assert!(!rx.is_disconnected());
        drop(tx);
        assert!(rx.is_disconnected());
    }

    #[test]
    fn test_closed_on_drop() {
        let (tx, rx) = channel::<i32>(16);
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
    }

    #[test]
    fn test_clone_multi_producer() {
        let (tx, rx) = channel::<i32>(16);
        let tx2 = tx.clone();
        tx.push(1).unwrap();
        tx2.push(2).unwrap();
        let mut items = vec![rx.poll().unwrap(), rx.poll().unwrap()];
        items.sort();
        assert_eq!(items, vec![1, 2]);
    }

    #[test]
    fn test_recv_timeout() {
        let (_tx, rx) = channel::<i32>(16);
        let result = rx.recv_timeout(Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::Timeout)));
    }

    #[test]
    fn test_len_capacity() {
        let (tx, rx) = channel::<i32>(8);
        assert_eq!(tx.len(), 0);
        assert!(tx.is_empty());

        tx.push(1).unwrap();
        tx.push(2).unwrap();
        assert_eq!(rx.len(), 2);
        assert!(!rx.is_empty());
    }

    #[test]
    fn test_concurrent_mpsc() {
        let (tx, rx) = channel::<i32>(1024);
        let n_producers = 4;
        let n_items = 250;

        let handles: Vec<_> = (0..n_producers)
            .map(|p| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for i in 0..n_items {
                        tx.push(p * 1000 + i).unwrap();
                    }
                })
            })
            .collect();

        drop(tx);
        for h in handles {
            h.join().unwrap();
        }

        let mut count = 0;
        while rx.poll().is_some() {
            count += 1;
        }
        assert_eq!(count, n_producers * n_items);
    }

    #[tokio::test]
    async fn test_async_watch() {
        let (tx, rx) = channel::<i32>(64);
        let mut watch = rx.async_watch();

        tx.push(42).unwrap();
        let _ = watch.changed().await;
        assert_eq!(rx.poll(), Some(42));
    }
}
