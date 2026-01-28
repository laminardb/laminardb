//! Channel-based push-to-pull bridge for `DataFusion` integration
//!
//! This module provides the `StreamBridge` which connects `LaminarDB`'s
//! push-based event processing model (where the Reactor pushes events)
//! with `DataFusion`'s pull-based query execution (where consumers pull
//! `RecordBatch` instances).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐         ┌──────────────────┐
//! │  `LaminarDB`      │         │   `DataFusion`     │
//! │  Reactor (push) │──send──▶│  Query (pull)    │
//! │                 │         │                  │
//! └─────────────────┘         └──────────────────┘
//!         │                           ▲
//!         │                           │
//!         └───────── channel ─────────┘
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::DataFusionError;
use futures::Stream;
use tokio::sync::mpsc;

/// Default channel capacity for the bridge.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// A bridge that connects push-based data producers with pull-based consumers.
///
/// The bridge creates a channel pair: a sender for pushing `RecordBatch`
/// instances from the producer side, and a stream for pulling batches
/// from the consumer side.
///
/// # Usage
///
/// ```rust,ignore
/// let schema = Arc::new(Schema::new(vec![...]));
/// let bridge = StreamBridge::new(schema, 100);
/// let sender = bridge.sender();
///
/// // Producer side (`LaminarDB` Reactor)
/// sender.send(batch).await?;
///
/// // Consumer side (`DataFusion` query)
/// let stream = bridge.into_stream();
/// while let Some(batch) = stream.next().await { ... }
/// ```
#[derive(Debug)]
pub struct StreamBridge {
    /// Schema of the record batches flowing through the bridge
    schema: SchemaRef,
    /// Sender side of the channel
    sender: BridgeSender,
    /// Receiver side of the channel
    receiver: Option<mpsc::Receiver<Result<RecordBatch, DataFusionError>>>,
}

impl StreamBridge {
    /// Creates a new bridge with the given schema and channel capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of `RecordBatch` instances that will flow through
    /// * `capacity` - Maximum number of batches that can be buffered
    #[must_use]
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            schema,
            sender: BridgeSender { tx },
            receiver: Some(rx),
        }
    }

    /// Creates a new bridge with default capacity.
    #[must_use]
    pub fn with_default_capacity(schema: SchemaRef) -> Self {
        Self::new(schema, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Returns the schema for this bridge.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Returns a cloneable sender for pushing batches into the bridge.
    ///
    /// Multiple senders can be created by cloning the returned sender.
    #[must_use]
    pub fn sender(&self) -> BridgeSender {
        self.sender.clone()
    }

    /// Converts this bridge into a `RecordBatchStream` for `DataFusion`.
    ///
    /// This consumes the bridge, taking ownership of the receiver.
    /// After calling this, you can still use senders obtained from `sender()`.
    ///
    /// # Panics
    ///
    /// Panics if called more than once (the receiver can only be taken once).
    #[must_use]
    pub fn into_stream(mut self) -> BridgeStream {
        BridgeStream {
            schema: self.schema,
            receiver: self.receiver.take().expect("receiver already taken"),
        }
    }

    /// Creates a stream without consuming the bridge.
    ///
    /// This takes ownership of the receiver, so subsequent calls will return `None`.
    #[must_use]
    pub fn take_stream(&mut self) -> Option<BridgeStream> {
        self.receiver.take().map(|receiver| BridgeStream {
            schema: Arc::clone(&self.schema),
            receiver,
        })
    }
}

/// A cloneable sender for pushing `RecordBatch` instances into a bridge.
///
/// Multiple producers can share senders by cloning this type.
#[derive(Debug, Clone)]
pub struct BridgeSender {
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
}

impl BridgeSender {
    /// Sends a batch to the bridge.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver has been dropped.
    pub async fn send(&self, batch: RecordBatch) -> Result<(), BridgeSendError> {
        self.tx
            .send(Ok(batch))
            .await
            .map_err(|_| BridgeSendError::ReceiverDropped)
    }

    /// Sends an error to the bridge.
    ///
    /// This allows the producer to signal errors to the consumer.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver has been dropped.
    pub async fn send_error(&self, error: DataFusionError) -> Result<(), BridgeSendError> {
        self.tx
            .send(Err(error))
            .await
            .map_err(|_| BridgeSendError::ReceiverDropped)
    }

    /// Attempts to send a batch without waiting.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is full or the receiver is dropped.
    pub fn try_send(&self, batch: RecordBatch) -> Result<(), BridgeTrySendError> {
        self.tx.try_send(Ok(batch)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => BridgeTrySendError::Full,
            mpsc::error::TrySendError::Closed(_) => BridgeTrySendError::ReceiverDropped,
        })
    }

    /// Returns true if the receiver has been dropped.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// A stream that pulls `RecordBatch` instances from the bridge.
///
/// This implements both `Stream` and `DataFusion`'s `RecordBatchStream`
/// so it can be used directly in `DataFusion` query execution.
pub struct BridgeStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
}

impl std::fmt::Debug for BridgeStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl Stream for BridgeStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl RecordBatchStream for BridgeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Error when sending a batch to the bridge.
#[derive(Debug, thiserror::Error)]
pub enum BridgeSendError {
    /// The receiver has been dropped.
    #[error("bridge receiver has been dropped")]
    ReceiverDropped,
}

/// Error when trying to send a batch without blocking.
#[derive(Debug, thiserror::Error)]
pub enum BridgeTrySendError {
    /// The channel is full.
    #[error("bridge channel is full")]
    Full,
    /// The receiver has been dropped.
    #[error("bridge receiver has been dropped")]
    ReceiverDropped,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(schema: &SchemaRef, values: Vec<i64>) -> RecordBatch {
        let array = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(Arc::clone(schema), vec![array]).unwrap()
    }

    #[tokio::test]
    async fn test_bridge_send_receive() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let sender = bridge.sender();
        let mut stream = bridge.into_stream();

        // Send a batch
        let batch = test_batch(&schema, vec![1, 2, 3]);
        sender.send(batch.clone()).await.unwrap();
        drop(sender); // Close the channel

        // Receive the batch
        let received = stream.next().await.unwrap().unwrap();
        assert_eq!(received.num_rows(), 3);

        // Stream should end
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_bridge_multiple_batches() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let sender = bridge.sender();
        let mut stream = bridge.into_stream();

        // Send multiple batches
        for i in 0..5 {
            let batch = test_batch(&schema, vec![i64::from(i)]);
            sender.send(batch).await.unwrap();
        }
        drop(sender);

        // Receive all batches
        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_bridge_sender_clone() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let sender1 = bridge.sender();
        let sender2 = sender1.clone();
        let mut stream = bridge.into_stream();

        // Send from both senders
        sender1.send(test_batch(&schema, vec![1])).await.unwrap();
        sender2.send(test_batch(&schema, vec![2])).await.unwrap();
        drop(sender1);
        drop(sender2);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_bridge_send_error() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let sender = bridge.sender();
        let mut stream = bridge.into_stream();

        // Send an error
        sender
            .send_error(DataFusionError::Plan("test error".to_string()))
            .await
            .unwrap();
        drop(sender);

        let result = stream.next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bridge_try_send() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 2);
        let sender = bridge.sender();
        // Keep the stream alive to prevent channel close
        let _stream = bridge.into_stream();

        // Fill the channel
        sender.try_send(test_batch(&schema, vec![1])).unwrap();
        sender.try_send(test_batch(&schema, vec![2])).unwrap();

        // Should fail when full
        let result = sender.try_send(test_batch(&schema, vec![3]));
        assert!(matches!(result, Err(BridgeTrySendError::Full)));
    }

    #[tokio::test]
    async fn test_bridge_receiver_dropped() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let sender = bridge.sender();
        let stream = bridge.into_stream();
        drop(stream);

        // Should detect closed channel
        assert!(sender.is_closed());

        let result = sender.send(test_batch(&schema, vec![1])).await;
        assert!(matches!(result, Err(BridgeSendError::ReceiverDropped)));
    }

    #[test]
    fn test_bridge_stream_schema() {
        let schema = test_schema();
        let bridge = StreamBridge::new(Arc::clone(&schema), 10);
        let stream = bridge.into_stream();

        assert_eq!(RecordBatchStream::schema(&stream), schema);
    }
}
