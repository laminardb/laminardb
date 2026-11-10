//! Packs every append-only view's new segments into this checkpoint's view-log object and
//! carries retained segments forward from the committed predecessor by reference.

use laminar_core::checkpoint::{NodeDataObject, ViewLog, ViewLogSegment};

use super::{
    checkpoint_sha256, BTreeMap, ByteRange, Bytes, CapturedViewLog, CapturedViewLogSegment,
    CheckpointAttempt, CheckpointCoordinator, CheckpointManifest, DbError, Digest, NonZeroU32,
    ReferencedStateChunk, Sha256, StateChunkId,
};

/// The view-log section of one participant manifest, with the object bytes it writes.
pub(super) struct PackedViewLogs {
    pub(super) payload: Vec<Bytes>,
    pub(super) object: Option<NodeDataObject>,
    pub(super) logs: Vec<ViewLog>,
    pub(super) referenced: Vec<ReferencedStateChunk>,
}

impl CheckpointCoordinator {
    pub(super) async fn pack_view_logs_until(
        &self,
        attempt: CheckpointAttempt,
        mut captured: Vec<CapturedViewLog>,
        deadline: tokio::time::Instant,
    ) -> Result<PackedViewLogs, DbError> {
        captured.sort_unstable_by(|left, right| left.view.cmp(&right.view));
        if captured.windows(2).any(|pair| pair[0].view == pair[1].view) {
            return Err(DbError::Checkpoint(
                "checkpoint capture repeats a view log".into(),
            ));
        }
        let current = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: attempt.checkpoint_id,
        };
        let prior = self.view_log_predecessor.as_deref();
        let mut packer = ViewLogPacker::new(current);
        let mut logs = Vec::with_capacity(captured.len());
        for log in captured {
            logs.push(packer.pack_log(prior, log)?);
        }
        if packer.object_length > self.config.max_node_data_bytes {
            return Err(DbError::Checkpoint(format!(
                "checkpoint view log is {} bytes; limit is {}",
                packer.object_length, self.config.max_node_data_bytes
            )));
        }
        let (object_sha256, digests) = digest_until(packer.payload.clone(), deadline).await?;
        for ((log, segment), digest) in packer.current.iter().zip(digests) {
            logs[*log].segments[*segment].sha256 = digest;
        }
        let object = (!packer.payload.is_empty()).then_some(NodeDataObject {
            chunk: current,
            object_length: packer.object_length,
            sha256: object_sha256,
        });
        let referenced = packer
            .referenced
            .into_iter()
            .map(|(chunk, (object_length, sha256, count))| {
                Ok(ReferencedStateChunk {
                    chunk,
                    object_length,
                    sha256,
                    ref_count: NonZeroU32::new(count).ok_or_else(|| {
                        DbError::Checkpoint("referenced view log has zero segments".into())
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(PackedViewLogs {
            payload: packer.payload,
            object,
            logs,
            referenced,
        })
    }
}

struct ViewLogPacker {
    chunk: StateChunkId,
    payload: Vec<Bytes>,
    object_length: u64,
    /// `(log, segment)` positions of the segments whose bytes this object holds, in byte order.
    current: Vec<(usize, usize)>,
    referenced: BTreeMap<StateChunkId, (u64, String, u32)>,
    logs: usize,
}

impl ViewLogPacker {
    const fn new(chunk: StateChunkId) -> Self {
        Self {
            chunk,
            payload: Vec::new(),
            object_length: 0,
            current: Vec::new(),
            referenced: BTreeMap::new(),
            logs: 0,
        }
    }

    fn pack_log(
        &mut self,
        prior: Option<&CheckpointManifest>,
        captured: CapturedViewLog,
    ) -> Result<ViewLog, DbError> {
        let CapturedViewLog {
            view,
            next_sequence,
            frontier,
            idle,
            mut segments,
        } = captured;
        segments.sort_unstable_by_key(CapturedViewLogSegment::position);
        if segments
            .windows(2)
            .any(|pair| pair[0].position() == pair[1].position())
        {
            return Err(DbError::Checkpoint(format!(
                "view log '{view}' repeats a segment"
            )));
        }
        let prior_log = prior.and_then(|manifest| {
            manifest
                .view_logs
                .binary_search_by(|log| log.view.as_str().cmp(&view))
                .ok()
                .map(|index| &manifest.view_logs[index])
        });
        let mut packed = Vec::with_capacity(segments.len());
        for segment in segments {
            let segment = match segment {
                CapturedViewLogSegment::New {
                    partition,
                    sequence,
                    rows,
                    event_time,
                    payload,
                } => {
                    self.current.push((self.logs, packed.len()));
                    ViewLogSegment {
                        partition,
                        sequence,
                        chunk: self.chunk,
                        range: self.append(payload)?,
                        sha256: String::new(),
                        rows,
                        event_time,
                    }
                }
                CapturedViewLogSegment::Retained {
                    partition,
                    sequence,
                } => self.retain(prior, prior_log, &view, (partition, sequence))?,
                CapturedViewLogSegment::Adopted {
                    partition,
                    sequence,
                    adopted,
                } => {
                    self.reference(
                        adopted.segment.chunk,
                        adopted.object_length,
                        &adopted.object_sha256,
                    )?;
                    ViewLogSegment {
                        partition,
                        sequence,
                        ..adopted.segment
                    }
                }
            };
            packed.push(segment);
        }
        self.logs += 1;
        Ok(ViewLog {
            view,
            next_sequence,
            frontier,
            idle,
            segments: packed,
        })
    }

    fn append(&mut self, payload: Bytes) -> Result<ByteRange, DbError> {
        let length = u64::try_from(payload.len())
            .map_err(|_| DbError::Checkpoint("view-log segment length exceeds u64".into()))?;
        if length == 0 {
            return Err(DbError::Checkpoint(
                "view-log segment has an empty payload".into(),
            ));
        }
        let range = ByteRange {
            offset: self.object_length,
            length,
        };
        self.object_length = range
            .end()
            .ok_or_else(|| DbError::Checkpoint("view-log object length overflow".into()))?;
        self.payload.push(payload);
        Ok(range)
    }

    fn retain(
        &mut self,
        prior: Option<&CheckpointManifest>,
        prior_log: Option<&ViewLog>,
        view: &str,
        position: (u16, u64),
    ) -> Result<ViewLogSegment, DbError> {
        let missing = || {
            DbError::Checkpoint(format!(
                "view log '{view}' retains partition {} segment {} that its committed \
                 predecessor does not hold",
                position.0, position.1
            ))
        };
        let (prior, prior_log) = prior.zip(prior_log).ok_or_else(missing)?;
        let segment = prior_log
            .segments
            .binary_search_by(|segment| (segment.partition, segment.sequence).cmp(&position))
            .ok()
            .map(|index| prior_log.segments[index].clone())
            .ok_or_else(missing)?;
        let (length, digest) = prior_object(prior, segment.chunk).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "view log '{view}' segment references untracked object {:?}",
                segment.chunk
            ))
        })?;
        self.reference(segment.chunk, length, &digest)?;
        Ok(segment)
    }

    /// Count one more segment of this manifest in the older object `chunk`.
    fn reference(&mut self, chunk: StateChunkId, length: u64, digest: &str) -> Result<(), DbError> {
        let entry = self
            .referenced
            .entry(chunk)
            .or_insert_with(|| (length, digest.to_owned(), 0));
        if entry.0 != length || entry.1 != digest {
            return Err(DbError::Checkpoint(format!(
                "conflicting metadata for referenced view log {chunk:?}"
            )));
        }
        entry.2 = entry
            .2
            .checked_add(1)
            .ok_or_else(|| DbError::Checkpoint("referenced segment count overflow".into()))?;
        Ok(())
    }
}

fn prior_object(prior: &CheckpointManifest, chunk: StateChunkId) -> Option<(u64, String)> {
    if let Some(object) = prior
        .view_log
        .as_ref()
        .filter(|object| object.chunk == chunk)
    {
        return Some((object.object_length, object.sha256.clone()));
    }
    prior
        .referenced_view_logs
        .binary_search_by(|reference| reference.chunk.cmp(&chunk))
        .ok()
        .map(|index| {
            let reference = &prior.referenced_view_logs[index];
            (reference.object_length, reference.sha256.clone())
        })
}

async fn digest_until(
    payload: Vec<Bytes>,
    deadline: tokio::time::Instant,
) -> Result<(String, Vec<String>), DbError> {
    if payload.is_empty() {
        return Ok((String::new(), Vec::new()));
    }
    let task = tokio::task::spawn_blocking(move || {
        let mut object = Sha256::new();
        let segments = payload
            .iter()
            .map(|bytes| {
                object.update(bytes);
                checkpoint_sha256(bytes)
            })
            .collect::<Vec<_>>();
        (format!("{:x}", object.finalize()), segments)
    });
    tokio::time::timeout_at(deadline, task)
        .await
        .map_err(|_| {
            DbError::Checkpoint("view-log digest task exceeded its end-to-end deadline".into())
        })?
        .map_err(|error| DbError::Checkpoint(format!("view-log digest task failed: {error}")))
}

#[cfg(test)]
mod tests {
    use laminar_core::checkpoint::{AdoptedSegment, EventTimeBounds};

    use super::*;

    fn adopted(sequence: u64, object_length: u64) -> CapturedViewLogSegment {
        CapturedViewLogSegment::Adopted {
            partition: 0,
            sequence,
            adopted: AdoptedSegment {
                segment: ViewLogSegment {
                    partition: 2,
                    sequence: 40 + sequence,
                    chunk: StateChunkId {
                        participant_id: 9,
                        checkpoint_id: 4,
                    },
                    range: ByteRange {
                        offset: sequence * 10,
                        length: 10,
                    },
                    sha256: "a".repeat(64),
                    rows: 3,
                    event_time: Some(EventTimeBounds { min: 1, max: 2 }),
                },
                object_length,
                object_sha256: "b".repeat(64),
            },
        }
    }

    fn captured(segments: Vec<CapturedViewLogSegment>) -> CapturedViewLog {
        CapturedViewLog {
            view: "events".into(),
            next_sequence: 8,
            frontier: None,
            idle: false,
            segments,
        }
    }

    #[test]
    fn an_adopted_segment_references_the_departed_participants_object() {
        let mut packer = ViewLogPacker::new(StateChunkId {
            participant_id: 1,
            checkpoint_id: 5,
        });
        let log = packer
            .pack_log(None, captured(vec![adopted(6, 100), adopted(7, 100)]))
            .unwrap();
        let positions: Vec<_> = log
            .segments
            .iter()
            .map(|segment| (segment.partition, segment.sequence, segment.range.offset))
            .collect();
        assert_eq!(positions, [(0, 6, 60), (0, 7, 70)]);
        assert_eq!(log.segments[0].rows, 3);
        let reference = &packer.referenced[&StateChunkId {
            participant_id: 9,
            checkpoint_id: 4,
        }];
        assert_eq!((reference.0, reference.2), (100, 2));

        let error = packer
            .pack_log(None, captured(vec![adopted(6, 101)]))
            .unwrap_err();
        assert!(
            error.to_string().contains("conflicting metadata"),
            "{error}"
        );
    }
}
