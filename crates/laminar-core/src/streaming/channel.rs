//! Bounded MPSC channel backed by crossfire.

use std::ops::Deref;

use crossfire::mpsc;

use super::config::ChannelConfig;
use super::error::TryPushError;

type Flavor<T> = mpsc::Array<T>;

/// Sender half. Cloneable for multi-producer use.
pub struct Producer<T: Send + 'static> {
    tx: crossfire::MTx<Flavor<T>>,
}

/// Async receiver half.
pub struct AsyncConsumer<T: Send + 'static> {
    rx: crossfire::AsyncRx<Flavor<T>>,
}

/// Creates a bounded channel with blocking sender and async receiver.
#[must_use]
pub fn channel<T: Send + 'static>(buffer_size: usize) -> (Producer<T>, AsyncConsumer<T>) {
    let cap = buffer_size.max(2);
    let (tx, rx) = mpsc::bounded_blocking_async::<T>(cap);
    (Producer { tx }, AsyncConsumer { rx })
}

/// Creates a bounded channel from a [`ChannelConfig`].
#[must_use]
pub(crate) fn channel_with_config<T: Send + 'static>(
    config: &ChannelConfig,
) -> (Producer<T>, AsyncConsumer<T>) {
    channel(config.buffer_size)
}

// -- Producer -----------------------------------------------------------------

impl<T: Send + 'static> Producer<T> {
    /// Non-blocking send.
    ///
    /// # Errors
    ///
    /// Returns the item if the channel is full or the receiver was dropped.
    pub fn push(&self, item: T) -> Result<(), T> {
        self.tx
            .try_send(item)
            .map_err(crossfire::TrySendError::into_inner)
    }

    /// Non-blocking send with typed error.
    ///
    /// # Errors
    ///
    /// Returns `TryPushError` containing the item if the channel is full.
    pub fn try_push(&self, item: T) -> Result<(), TryPushError<T>> {
        self.tx.try_send(item).map_err(|e| match e {
            crossfire::TrySendError::Full(v) => TryPushError::full(v),
            crossfire::TrySendError::Disconnected(v) => TryPushError::disconnected(v),
        })
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

// -- AsyncConsumer ------------------------------------------------------------

impl<T: Send + 'static> AsyncConsumer<T> {
    /// Async receive. Suspends until a message arrives or the sender disconnects.
    ///
    /// # Errors
    ///
    /// Returns `crossfire::RecvError` if the sender was dropped.
    pub async fn recv(&mut self) -> Result<T, crossfire::RecvError> {
        self.rx.recv().await
    }

    /// Non-blocking receive. Returns `Err` immediately if the channel is
    /// empty or the senders have all disconnected.
    ///
    /// # Errors
    ///
    /// Returns `crossfire::TryRecvError::Empty` when no items are buffered,
    /// `crossfire::TryRecvError::Disconnected` after all senders are dropped
    /// and the buffer is drained.
    pub fn try_recv(&self) -> Result<T, crossfire::TryRecvError> {
        self.rx.try_recv()
    }

    /// Returns `true` if the sender has been dropped.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.rx.is_disconnected()
    }
}

impl<T: Send + 'static> std::fmt::Debug for AsyncConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConsumer")
            .field("disconnected", &self.is_disconnected())
            .finish()
    }
}

#[cfg(test)]
mod tests;
