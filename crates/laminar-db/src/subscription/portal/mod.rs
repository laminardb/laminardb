//! Per-subscriber cursor over the shared subscription log.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use futures::FutureExt;
use laminar_core::checkpoint::{OutputPartitionId, PartitionSequence, StreamGeneration};

#[cfg(feature = "cluster")]
use super::cluster::{ClusterReaderFrame, ClusterReaderRead, ClusterSubscriptionReader};
use super::registry::{ChargedUpdate, MvUpdate, SubscriptionRead, SubscriptionReader};

#[derive(Debug)]
enum PortalReader {
    Local(SubscriptionReader),
    #[cfg(feature = "cluster")]
    Cluster(ClusterSubscriptionReader),
}

impl PortalReader {
    async fn next(&mut self) -> PortalRead {
        match self {
            Self::Local(reader) => PortalRead::Local(reader.next().await),
            #[cfg(feature = "cluster")]
            Self::Cluster(reader) => PortalRead::Cluster(reader.next().await),
        }
    }

    fn try_next(&mut self) -> Option<PortalRead> {
        match self {
            Self::Local(reader) => reader.next().now_or_never().map(PortalRead::Local),
            #[cfg(feature = "cluster")]
            Self::Cluster(reader) => reader.try_next().map(PortalRead::Cluster),
        }
    }
}

enum PortalRead {
    Local(SubscriptionRead),
    #[cfg(feature = "cluster")]
    Cluster(ClusterReaderRead),
}

/// Keeps the process-wide subscription charge alive with an emitted batch.
#[doc(hidden)]
#[derive(Clone)]
pub struct SubscriptionFrameLease {
    _local_owner: Option<ChargedUpdate>,
    #[cfg(feature = "cluster")]
    _cluster_owner: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl std::fmt::Debug for SubscriptionFrameLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SubscriptionFrameLease")
    }
}

/// Optional durable identity carried alongside a compatibility [`PortalFrame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSubscriptionFrameMetadata {
    /// Partition-local identity of one committed data frame.
    Data {
        /// Durable stream incarnation.
        stream_generation: StreamGeneration,
        /// Stable vnode output partition.
        partition: OutputPartitionId,
        /// Monotonic sequence within this generation and partition.
        partition_sequence: PartitionSequence,
        /// Whole-cluster checkpoint that first exposed this frame.
        committed_epoch: u64,
    },
    /// Whole-cluster committed progress after every partition interval was delivered.
    Progress {
        /// Durable stream incarnation.
        stream_generation: StreamGeneration,
        /// Committed checkpoint epoch.
        epoch: u64,
        /// Committed checkpoint identifier.
        checkpoint_id: u64,
    },
}

/// Backward-compatible frame plus optional cluster delivery metadata.
#[derive(Debug, Clone)]
pub struct SubscriptionEnvelope {
    /// Existing local/standalone-compatible frame.
    pub frame: PortalFrame,
    /// Present only for committed cluster frames.
    pub cluster: Option<ClusterSubscriptionFrameMetadata>,
    /// Stable terminal error code when `frame` is [`PortalFrame::Error`].
    pub error_code: Option<&'static str>,
}

impl SubscriptionEnvelope {
    fn local(frame: PortalFrame) -> Self {
        Self {
            frame,
            cluster: None,
            error_code: None,
        }
    }
}

/// One frame emitted toward the wire.
#[derive(Debug, Clone)]
pub enum PortalFrame {
    /// Rows produced in a cycle.
    Batch {
        /// Arrow rows in the shared-log entry.
        batch: RecordBatch,
        /// Portal-local delivery sequence. In cluster mode this is neither durable nor global;
        /// use [`ClusterSubscriptionFrameMetadata::Data`] for partition identity.
        sequence: u64,
        /// Internal process-memory ownership token.
        #[doc(hidden)]
        lease: SubscriptionFrameLease,
    },
    /// Progress frontier for a durably committed checkpoint.
    Barrier {
        /// Portal-local delivery sequence; not a cluster-wide ordering position.
        sequence: u64,
        /// Engine checkpoint epoch.
        epoch: u64,
        /// Engine checkpoint id.
        checkpoint_id: u64,
        /// Local-log cut, or the gateway-local delivery cut in cluster mode. The durable cluster
        /// position is the checkpoint's partition-frontier vector, addressed by `epoch`.
        through_sequence: u64,
    },
    /// Consumer fell behind by exactly `skipped` shared-log entries. This is
    /// terminal because continuing would hide rows or checkpoint markers.
    Lagged(u64),
    /// The subscription cannot continue without returning an invalid result.
    Error {
        /// Human-readable failure detail.
        message: String,
    },
}

/// One `SUBSCRIBE` consumer.
#[derive(Debug)]
pub struct SubscriptionPortal {
    name: String,
    schema: SchemaRef,
    reader: Option<PortalReader>,
    closed: bool,
    filter: Option<Arc<dyn PhysicalExpr>>,
}

impl SubscriptionPortal {
    pub(crate) fn open(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
    ) -> Self {
        Self::new_inner(name, schema, reader, None)
    }

    pub(crate) fn open_with_filter(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
        filter: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self::new_inner(name, schema, reader, Some(filter))
    }

    fn new_inner(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: SubscriptionReader,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            reader: Some(PortalReader::Local(reader)),
            closed: false,
            filter,
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn open_cluster(
        name: impl Into<String>,
        schema: SchemaRef,
        reader: ClusterSubscriptionReader,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            reader: Some(PortalReader::Cluster(reader)),
            closed: false,
            filter,
        }
    }

    /// Schema of the subscribed object.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Next frame, or `None` after a terminal frame or explicit close.
    pub async fn next_frame(&mut self) -> Option<PortalFrame> {
        self.next_envelope().await.map(|envelope| envelope.frame)
    }

    /// Next frame with optional partition/checkpoint metadata for cluster-aware clients.
    pub async fn next_envelope(&mut self) -> Option<SubscriptionEnvelope> {
        if self.closed {
            return None;
        }

        loop {
            let read = self.reader.as_mut()?.next().await;
            if let Some(frame) = self.process_read(read) {
                return Some(frame);
            }
        }
    }

    /// Return the next immediately available frame without waiting.
    pub fn try_next_frame(&mut self) -> Option<PortalFrame> {
        self.try_next_envelope().map(|envelope| envelope.frame)
    }

    /// Return the next immediately available cluster-aware envelope without waiting.
    pub fn try_next_envelope(&mut self) -> Option<SubscriptionEnvelope> {
        if self.closed {
            return None;
        }

        loop {
            let read = self.reader.as_mut()?.try_next()?;
            if let Some(frame) = self.process_read(read) {
                return Some(frame);
            }
        }
    }

    fn process_read(&mut self, read: PortalRead) -> Option<SubscriptionEnvelope> {
        match read {
            PortalRead::Local(read) => self.process_local_read(read),
            #[cfg(feature = "cluster")]
            PortalRead::Cluster(read) => self.process_cluster_read(read),
        }
    }

    fn process_local_read(&mut self, read: SubscriptionRead) -> Option<SubscriptionEnvelope> {
        let frame = match read {
            SubscriptionRead::Update { sequence, update } => translate(sequence, update),
            SubscriptionRead::Lagged(skipped) => {
                tracing::warn!(
                    subscription = %self.name,
                    skipped,
                    "subscription cursor was evicted; closing"
                );
                self.close();
                return Some(SubscriptionEnvelope::local(PortalFrame::Lagged(skipped)));
            }
            SubscriptionRead::Terminal(message) => {
                tracing::warn!(
                    subscription = %self.name,
                    %message,
                    "subscription log terminated; closing"
                );
                self.close();
                return Some(SubscriptionEnvelope::local(PortalFrame::Error { message }));
            }
        };

        self.process_envelope(SubscriptionEnvelope::local(frame))
    }

    #[cfg(feature = "cluster")]
    fn process_cluster_read(&mut self, read: ClusterReaderRead) -> Option<SubscriptionEnvelope> {
        let envelope = match read {
            ClusterReaderRead::Frame(ClusterReaderFrame::Batch {
                batch,
                delivery_sequence,
                stream_generation,
                partition,
                partition_sequence,
                committed_epoch,
                permit,
            }) => SubscriptionEnvelope {
                frame: PortalFrame::Batch {
                    batch,
                    sequence: delivery_sequence,
                    lease: SubscriptionFrameLease {
                        _local_owner: None,
                        _cluster_owner: Some(permit),
                    },
                },
                cluster: Some(ClusterSubscriptionFrameMetadata::Data {
                    stream_generation,
                    partition,
                    partition_sequence,
                    committed_epoch,
                }),
                error_code: None,
            },
            ClusterReaderRead::Frame(ClusterReaderFrame::Progress {
                delivery_sequence,
                through_sequence,
                stream_generation,
                epoch,
                checkpoint_id,
            }) => SubscriptionEnvelope {
                frame: PortalFrame::Barrier {
                    sequence: delivery_sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                },
                cluster: Some(ClusterSubscriptionFrameMetadata::Progress {
                    stream_generation,
                    epoch,
                    checkpoint_id,
                }),
                error_code: None,
            },
            ClusterReaderRead::Terminal(error) => {
                let code = error.code();
                tracing::warn!(
                    subscription = %self.name,
                    code,
                    error = %error,
                    "committed cluster subscription terminated"
                );
                self.close();
                return Some(SubscriptionEnvelope {
                    frame: PortalFrame::Error {
                        message: format!("[{code}] {error}"),
                    },
                    cluster: None,
                    error_code: Some(code),
                });
            }
        };
        self.process_envelope(envelope)
    }

    fn process_envelope(&mut self, envelope: SubscriptionEnvelope) -> Option<SubscriptionEnvelope> {
        let SubscriptionEnvelope {
            frame,
            cluster,
            error_code,
        } = envelope;

        let PortalFrame::Batch {
            batch,
            sequence,
            lease,
        } = frame
        else {
            if matches!(&frame, PortalFrame::Error { .. }) {
                self.close();
            }
            return Some(SubscriptionEnvelope {
                frame,
                cluster,
                error_code,
            });
        };
        let Some(filter) = self.filter.as_ref() else {
            return Some(SubscriptionEnvelope {
                frame: PortalFrame::Batch {
                    batch,
                    sequence,
                    lease,
                },
                cluster,
                error_code,
            });
        };
        match crate::filter_compile::apply(&batch, filter.as_ref()) {
            Ok(Some(filtered)) => Some(SubscriptionEnvelope {
                frame: PortalFrame::Batch {
                    batch: filtered,
                    sequence,
                    lease,
                },
                cluster,
                error_code,
            }),
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    subscription = %self.name,
                    %error,
                    "subscription filter failed; closing"
                );
                let message = error.to_string();
                self.close();
                Some(SubscriptionEnvelope {
                    frame: PortalFrame::Error { message },
                    cluster: None,
                    error_code: None,
                })
            }
        }
    }

    /// Stop reading and release the subscriber registration. Idempotent.
    pub fn close(&mut self) {
        self.closed = true;
        self.reader = None;
    }

    /// True after `close()` has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl Drop for SubscriptionPortal {
    fn drop(&mut self) {
        self.close();
    }
}

fn translate(sequence: u64, update: ChargedUpdate) -> PortalFrame {
    match update.as_ref() {
        MvUpdate::Batch(batch) => {
            let batch = batch.clone();
            PortalFrame::Batch {
                batch,
                sequence,
                lease: SubscriptionFrameLease {
                    _local_owner: Some(update),
                    #[cfg(feature = "cluster")]
                    _cluster_owner: None,
                },
            }
        }
        MvUpdate::Barrier {
            epoch,
            checkpoint_id,
            through_sequence,
        } => PortalFrame::Barrier {
            sequence,
            epoch: *epoch,
            checkpoint_id: *checkpoint_id,
            through_sequence: *through_sequence,
        },
        MvUpdate::Error(message) => PortalFrame::Error {
            message: message.clone(),
        },
    }
}

#[cfg(test)]
mod tests;
