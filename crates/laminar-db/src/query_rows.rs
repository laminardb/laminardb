//! The rows of an ad-hoc query, delivered in order through a bounded queue. The query runs only as
//! fast as its reader reads, and a result that ends early ends with an error, never quietly.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::DbError;

/// Batches a result holds ahead of its reader.
const QUEUED_BATCHES: usize = 8;

enum Delivery {
    Batch(RecordBatch),
    Failed(DbError),
    End,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch(batch) => write!(f, "Batch({} rows)", batch.num_rows()),
            Self::Failed(error) => write!(f, "Failed({error})"),
            Self::End => f.write_str("End"),
        }
    }
}

/// The rows of an ad-hoc query, in order. Reading them to the end returns every row the query
/// produced; a query that fails, or is cancelled before its last row, ends them with an error.
#[derive(Debug)]
pub struct QueryRows {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Delivery>,
    ended: bool,
}

impl QueryRows {
    /// Run `stream` on a task that queues its batches for the returned rows. The task stops at the
    /// stream's end or first error, on `cancel`, or when the rows are dropped, and then calls
    /// `finished`.
    pub(crate) fn spawn(
        mut stream: SendableRecordBatchStream,
        cancel: CancellationToken,
        finished: impl FnOnce() + Send + 'static,
    ) -> Self {
        let schema = stream.schema();
        let (sender, receiver) = mpsc::channel(QUEUED_BATCHES);
        tokio::spawn(async move {
            loop {
                let delivery = tokio::select! {
                    () = cancel.cancelled() => break,
                    next = stream.next() => match next {
                        Some(Ok(batch)) => Delivery::Batch(batch),
                        Some(Err(error)) => Delivery::Failed(execution_error(error)),
                        None => Delivery::End,
                    },
                };
                let last = !matches!(delivery, Delivery::Batch(_));
                let sent = tokio::select! {
                    () = cancel.cancelled() => break,
                    sent = sender.send(delivery) => sent,
                };
                if last || sent.is_err() {
                    break;
                }
            }
            finished();
        });
        Self {
            schema,
            receiver,
            ended: false,
        }
    }

    /// Rows that were already read to the end.
    #[cfg(feature = "api")]
    pub(crate) fn ready(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let (sender, receiver) = mpsc::channel(batches.len() + 1);
        for delivery in batches
            .into_iter()
            .map(Delivery::Batch)
            .chain(std::iter::once(Delivery::End))
        {
            // The channel holds every delivery and its receiver is alive.
            let _ = sender.try_send(delivery);
        }
        Self {
            schema,
            receiver,
            ended: false,
        }
    }

    /// Schema of every batch.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        SchemaRef::clone(&self.schema)
    }

    /// The next batch, waiting for it; `None` after the last one.
    ///
    /// # Errors
    /// The query's error if it failed, or [`DbError::InvalidOperation`] if it was cancelled
    /// before its last row.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DbError> {
        if self.ended {
            return Ok(None);
        }
        let delivery = self.receiver.recv().await;
        self.deliver(delivery)
    }

    /// The next batch if one is ready; `None` when none is ready yet or after the last one.
    ///
    /// # Errors
    /// As [`Self::next_batch`].
    pub fn try_next_batch(&mut self) -> Result<Option<RecordBatch>, DbError> {
        if self.ended {
            return Ok(None);
        }
        match self.receiver.try_recv() {
            Ok(delivery) => self.deliver(Some(delivery)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => self.deliver(None),
        }
    }

    /// The next batch, blocking the thread until it is ready; for callers outside an async
    /// runtime.
    ///
    /// # Errors
    /// As [`Self::next_batch`].
    ///
    /// # Panics
    /// When called from within an async runtime.
    pub fn blocking_next_batch(&mut self) -> Result<Option<RecordBatch>, DbError> {
        if self.ended {
            return Ok(None);
        }
        let delivery = self.receiver.blocking_recv();
        self.deliver(delivery)
    }

    /// Every remaining batch.
    ///
    /// # Errors
    /// As [`Self::next_batch`].
    pub async fn read_all(mut self) -> Result<Vec<RecordBatch>, DbError> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    fn deliver(&mut self, delivery: Option<Delivery>) -> Result<Option<RecordBatch>, DbError> {
        match delivery {
            Some(Delivery::Batch(batch)) => Ok(Some(batch)),
            Some(Delivery::End) => {
                self.ended = true;
                Ok(None)
            }
            Some(Delivery::Failed(error)) => {
                self.ended = true;
                Err(error)
            }
            None => {
                self.ended = true;
                Err(DbError::InvalidOperation(
                    "the query was cancelled before its last row".into(),
                ))
            }
        }
    }
}

/// A query fails with the [`DbError`] a table scan raised, which `DataFusion` carries as an
/// external error, or with `DataFusion`'s own error.
pub(crate) fn execution_error(error: datafusion_common::DataFusionError) -> DbError {
    match error {
        datafusion_common::DataFusionError::External(inner) => match inner.downcast::<DbError>() {
            Ok(error) => *error,
            Err(inner) => datafusion_common::DataFusionError::External(inner).into(),
        },
        error => error.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    fn batches(count: i64) -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batches = (0..count)
            .map(|n| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![n]))],
                )
                .unwrap()
            })
            .collect();
        (schema, batches)
    }

    fn rows_of(
        schema: SchemaRef,
        items: Vec<Result<RecordBatch, datafusion_common::DataFusionError>>,
        cancel: CancellationToken,
    ) -> QueryRows {
        let stream = RecordBatchStreamAdapter::new(schema, futures::stream::iter(items));
        QueryRows::spawn(Box::pin(stream), cancel, || {})
    }

    /// A reader that falls far behind still receives every batch: the query waits for it.
    #[tokio::test]
    async fn a_slow_reader_receives_every_batch() {
        let (schema, batches) = batches(5_000);
        let rows = rows_of(
            schema,
            batches.into_iter().map(Ok).collect(),
            CancellationToken::new(),
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let read = rows.read_all().await.unwrap();
        let values: Vec<i64> = read
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();
        assert_eq!(values, (0..5_000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_failure_or_a_cancellation_ends_the_rows_with_an_error() {
        let (schema, mut batches) = batches(2);
        let mut failed = rows_of(
            Arc::clone(&schema),
            vec![
                Ok(batches.remove(0)),
                Err(datafusion_common::DataFusionError::External(Box::new(
                    DbError::MaterializedView("scan failed".into()),
                ))),
                Ok(batches.remove(0)),
            ],
            CancellationToken::new(),
        );
        assert!(failed.next_batch().await.unwrap().is_some());
        let error = failed.next_batch().await.unwrap_err();
        assert!(matches!(error, DbError::MaterializedView(_)), "{error}");
        assert!(failed.next_batch().await.unwrap().is_none());

        let cancel = CancellationToken::new();
        let pending = RecordBatchStreamAdapter::new(schema, futures::stream::pending());
        let mut cancelled = QueryRows::spawn(Box::pin(pending), cancel.clone(), || {});
        cancel.cancel();
        let error = cancelled.next_batch().await.unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
    }
}
