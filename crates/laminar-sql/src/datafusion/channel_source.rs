//! Channel-based streaming source implementation
//!
//! This module provides `ChannelStreamSource`, the primary integration point
//! between `LaminarDB`'s Reactor and `DataFusion`'s query engine.

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_expr::Expr;
use futures::Stream;
use parking_lot::Mutex;

use super::bridge::{BridgeSender, StreamBridge};
use super::source::{SortColumn, StreamSource};

/// Default channel capacity for the stream source.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Maximum number of partitions allowed, to prevent accidental
/// misconfiguration from allocating thousands of channels.
const MAX_PARTITIONS: usize = 256;

/// A streaming source that receives data through a channel.
///
/// This is the primary integration point between `LaminarDB`'s push-based
/// Reactor and `DataFusion`'s pull-based query execution. Data is pushed
/// into the source via `BridgeSender`, and `DataFusion` pulls it through
/// the stream.
///
/// # Multi-Partition Support
///
/// A source can be created with multiple partitions via
/// [`with_partitions`](Self::with_partitions) to enable parallel query
/// execution. Each partition has its own bridge and sender.
///
/// # Important Usage Pattern
///
/// The sender must be taken (not cloned) to ensure proper channel closure:
///
/// ```rust,ignore
/// // Create the source and take the sender
/// let source = ChannelStreamSource::new(schema);
/// let sender = source.take_sender(0).expect("sender available");
///
/// // Register with `DataFusion`
/// let provider = StreamingTableProvider::new("events", Arc::new(source));
/// ctx.register_table("events", Arc::new(provider))?;
///
/// // Push data from Reactor
/// sender.send(batch).await?;
///
/// // IMPORTANT: Drop the sender to close the channel before querying
/// drop(sender);
///
/// // Execute query
/// let df = ctx.sql("SELECT * FROM events").await?;
/// let results = df.collect().await?;
/// ```
///
/// # Thread Safety
///
/// The source is thread-safe and can be shared across threads. The sender
/// can be cloned after being taken to allow multiple producers.
pub struct ChannelStreamSource {
    /// Schema of the data
    schema: SchemaRef,
    /// Per-partition bridges connecting senders and receivers
    bridges: Mutex<Vec<Option<StreamBridge>>>,
    /// Per-partition senders for pushing data
    senders: Mutex<Vec<Option<BridgeSender>>>,
    /// Number of partitions
    num_partitions: usize,
    /// Channel capacity per partition
    capacity: usize,
    /// Declared output ordering (for ORDER BY elision)
    ordering: Option<Vec<SortColumn>>,
}

impl ChannelStreamSource {
    /// Creates a new single-partition channel stream source with default capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the `RecordBatch` instances that will be pushed
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_partitions_and_capacity(schema, 1, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Creates a new single-partition channel stream source with specified capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the `RecordBatch` instances that will be pushed
    /// * `capacity` - Maximum number of batches that can be buffered per partition
    #[must_use]
    pub fn with_capacity(schema: SchemaRef, capacity: usize) -> Self {
        Self::with_partitions_and_capacity(schema, 1, capacity)
    }

    /// Creates a new multi-partition channel stream source with default capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the `RecordBatch` instances that will be pushed
    /// * `num_partitions` - Number of partitions (must be >= 1)
    #[must_use]
    pub fn with_partitions(schema: SchemaRef, num_partitions: usize) -> Self {
        Self::with_partitions_and_capacity(schema, num_partitions, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Creates a new multi-partition channel stream source with specified capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the `RecordBatch` instances that will be pushed
    /// * `num_partitions` - Number of partitions (must be >= 1)
    /// * `capacity` - Maximum number of batches that can be buffered per partition
    #[must_use]
    pub fn with_partitions_and_capacity(
        schema: SchemaRef,
        num_partitions: usize,
        capacity: usize,
    ) -> Self {
        let num_partitions = num_partitions.clamp(1, MAX_PARTITIONS);
        let mut bridges = Vec::with_capacity(num_partitions);
        let mut senders = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let bridge = StreamBridge::new(Arc::clone(&schema), capacity);
            senders.push(Some(bridge.sender()));
            bridges.push(Some(bridge));
        }
        Self {
            schema,
            bridges: Mutex::new(bridges),
            senders: Mutex::new(senders),
            num_partitions,
            capacity,
            ordering: None,
        }
    }

    /// Declares that this source produces data in the given sort order.
    ///
    /// When set, `DataFusion` can elide `SortExec` for ORDER BY queries
    /// that match the declared ordering.
    ///
    /// # Arguments
    ///
    /// * `ordering` - The columns that the output is sorted by
    #[must_use]
    pub fn with_ordering(mut self, ordering: Vec<SortColumn>) -> Self {
        self.ordering = Some(ordering);
        self
    }

    /// Takes the sender for pushing batches into the given partition.
    ///
    /// This method can only be called once per partition. The sender is
    /// moved out of the source to ensure the caller has full ownership
    /// and can close the channel by dropping the sender.
    ///
    /// Returns `None` if the sender was already taken or `partition` is
    /// out of range.
    #[must_use]
    pub fn take_sender(&self, partition: usize) -> Option<BridgeSender> {
        let mut guard = self.senders.lock();
        guard.get_mut(partition)?.take()
    }

    /// Takes all senders at once, one per partition.
    ///
    /// Each element is `Some` if the sender hasn't been taken yet for
    /// that partition, or `None` if it was already taken.
    #[must_use]
    pub fn take_senders(&self) -> Vec<Option<BridgeSender>> {
        let mut guard = self.senders.lock();
        guard.iter_mut().map(Option::take).collect()
    }

    /// Resets the source, recreating all bridges and senders.
    ///
    /// This is useful when you need to reuse the source after the previous
    /// streams have been consumed. Any data sent before the reset but not
    /// yet consumed will be lost.
    ///
    /// Returns the new senders, one per partition.
    pub fn reset(&self) -> Vec<BridgeSender> {
        let mut bridges_guard = self.bridges.lock();
        let mut senders_guard = self.senders.lock();
        bridges_guard.clear();
        senders_guard.clear();
        let mut result = Vec::with_capacity(self.num_partitions);
        for _ in 0..self.num_partitions {
            let bridge = StreamBridge::new(Arc::clone(&self.schema), self.capacity);
            result.push(bridge.sender());
            senders_guard.push(Some(bridge.sender()));
            bridges_guard.push(Some(bridge));
        }
        result
    }
}

impl Debug for ChannelStreamSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelStreamSource")
            .field("schema", &self.schema)
            .field("num_partitions", &self.num_partitions)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl StreamSource for ChannelStreamSource {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    fn output_ordering(&self) -> Option<Vec<SortColumn>> {
        self.ordering.clone()
    }

    fn stream(
        &self,
        partition: usize,
        projection: Option<Vec<usize>>,
        _filters: Vec<Expr>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, DataFusionError> {
        if partition >= self.num_partitions {
            return Err(DataFusionError::Plan(format!(
                "Partition {partition} out of range (num_partitions={})",
                self.num_partitions,
            )));
        }

        let mut bridges_guard = self.bridges.lock();
        let bridge = bridges_guard
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Stream for partition {partition} already taken; call reset() to recreate",
                ))
            })?;

        let inner_stream = bridge.into_stream();

        // Apply projection if specified
        let stream: datafusion::physical_plan::SendableRecordBatchStream =
            if let Some(indices) = projection {
                let projected_schema = {
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| self.schema.field(i).clone())
                        .collect();
                    Arc::new(arrow_schema::Schema::new(fields))
                };
                Box::pin(ProjectingStream::new(
                    inner_stream,
                    projected_schema,
                    indices,
                ))
            } else {
                Box::pin(inner_stream)
            };

        Ok(stream)
    }
}

/// A stream that applies column projection to record batches.
struct ProjectingStream<S> {
    inner: S,
    schema: SchemaRef,
    indices: Vec<usize>,
}

impl<S> ProjectingStream<S> {
    fn new(inner: S, schema: SchemaRef, indices: Vec<usize>) -> Self {
        Self {
            inner,
            schema,
            indices,
        }
    }
}

impl<S> Debug for ProjectingStream<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectingStream")
            .field("schema", &self.schema)
            .field("indices", &self.indices)
            .finish_non_exhaustive()
    }
}

impl<S> Stream for ProjectingStream<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Project columns
                let columns: Vec<_> = self
                    .indices
                    .iter()
                    .map(|&i| Arc::clone(batch.column(i)))
                    .collect();
                let projected =
                    RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|e| {
                        DataFusionError::ArrowError(
                            Box::new(e),
                            Some("projection failed".to_string()),
                        )
                    });
                Poll::Ready(Some(projected))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> RecordBatchStream for ProjectingStream<S>
where
    S: Stream<Item = Result<RecordBatch, DataFusionError>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef, ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_channel_source_schema() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        assert_eq!(source.schema(), schema);
    }

    #[test]
    fn test_channel_source_default_single_partition() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        assert_eq!(source.num_partitions(), 1);
    }

    #[tokio::test]
    async fn test_channel_source_stream() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));
        let sender = source.take_sender(0).unwrap();

        let mut stream = source.stream(0, None, vec![]).unwrap();

        // Send data
        sender
            .send(test_batch(&schema, vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        drop(sender);

        // Receive data
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_channel_source_projection() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));
        let sender = source.take_sender(0).unwrap();

        // Project only the "value" column (index 1)
        let mut stream = source.stream(0, Some(vec![1]), vec![]).unwrap();

        sender
            .send(test_batch(&schema, vec![1, 2], vec![100, 200]))
            .await
            .unwrap();
        drop(sender);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "value");

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 100);
        assert_eq!(values.value(1), 200);
    }

    #[tokio::test]
    async fn test_channel_source_stream_already_taken() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        // First stream takes ownership
        let _stream = source.stream(0, None, vec![]).unwrap();

        // Second stream should fail
        let result = source.stream(0, None, vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_channel_source_multiple_batches() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));
        let sender = source.take_sender(0).unwrap();
        let mut stream = source.stream(0, None, vec![]).unwrap();

        // Send multiple batches
        for i in 0..5i64 {
            sender
                .send(test_batch(&schema, vec![i], vec![i * 10]))
                .await
                .unwrap();
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
    async fn test_channel_source_take_sender_once() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        // First take succeeds
        let sender = source.take_sender(0);
        assert!(sender.is_some());

        // Second take returns None
        let sender2 = source.take_sender(0);
        assert!(sender2.is_none());
    }

    #[tokio::test]
    async fn test_channel_source_reset() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        // Take sender and stream
        let _sender = source.take_sender(0).unwrap();
        let _stream = source.stream(0, None, vec![]).unwrap();

        // Reset creates new bridges and senders
        let new_senders = source.reset();
        assert_eq!(new_senders.len(), 1);
        let mut new_stream = source.stream(0, None, vec![]).unwrap();

        // Can use the new sender and stream
        new_senders[0]
            .send(test_batch(&schema, vec![1], vec![10]))
            .await
            .unwrap();
        drop(new_senders);

        let batch = new_stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_channel_source_debug() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        let debug_str = format!("{source:?}");
        assert!(debug_str.contains("ChannelStreamSource"));
        assert!(debug_str.contains("capacity"));
    }

    #[test]
    fn test_channel_source_default_no_ordering() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        assert!(source.output_ordering().is_none());
    }

    #[test]
    fn test_channel_source_with_ordering() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema))
            .with_ordering(vec![SortColumn::ascending("id")]);

        let ordering = source.output_ordering();
        assert!(ordering.is_some());
        let cols = ordering.unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
        assert!(!cols[0].descending);
    }

    // --- Multi-partition tests ---

    #[test]
    fn test_channel_source_multi_partition_count() {
        let schema = test_schema();
        let source = ChannelStreamSource::with_partitions(Arc::clone(&schema), 4);

        assert_eq!(source.num_partitions(), 4);
    }

    #[tokio::test]
    async fn test_channel_source_multi_partition_senders_and_streams() {
        let schema = test_schema();
        let num_partitions = 3;
        let source = ChannelStreamSource::with_partitions(Arc::clone(&schema), num_partitions);

        // Take all senders
        let senders = source.take_senders();
        assert_eq!(senders.len(), num_partitions);

        // Open all streams
        let mut streams: Vec<_> = (0..num_partitions)
            .map(|p| source.stream(p, None, vec![]).unwrap())
            .collect();

        // Send distinct data to each partition
        for (i, sender) in senders.into_iter().enumerate() {
            let sender = sender.unwrap();
            let id = i as i64;
            sender
                .send(test_batch(&schema, vec![id], vec![id * 100]))
                .await
                .unwrap();
            drop(sender);
        }

        // Each partition should yield its own data
        let mut total_rows = 0;
        for stream in &mut streams {
            while let Some(batch) = stream.next().await {
                total_rows += batch.unwrap().num_rows();
            }
        }
        assert_eq!(total_rows, num_partitions);
    }

    #[test]
    fn test_channel_source_partition_out_of_range() {
        let schema = test_schema();
        let source = ChannelStreamSource::with_partitions(Arc::clone(&schema), 2);

        let result = source.stream(2, None, vec![]);
        match result {
            Err(e) => assert!(e.to_string().contains("out of range")),
            Ok(_) => panic!("expected error for out-of-range partition"),
        }
    }

    #[test]
    fn test_channel_source_take_sender_out_of_range() {
        let schema = test_schema();
        let source = ChannelStreamSource::new(Arc::clone(&schema));

        assert!(source.take_sender(1).is_none());
    }
}
