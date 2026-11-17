//! Object-store fixtures and polling helpers shared by the files source tests.

use std::time::Duration;

use laminar_core::checkpoint::CheckpointAttempt;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt as _, PutPayload};

use super::source::FileSource;
use crate::checkpoint::SourceCheckpoint;
use crate::connector::{SourceBatch, SourceConnector, SourcePosition};

pub(super) async fn put_object(store: &InMemory, key: &str, body: &[u8]) {
    store
        .put(
            &object_store::path::Path::from(key),
            PutPayload::from(body.to_vec()),
        )
        .await
        .unwrap();
}

pub(super) fn resume_from(checkpoint: SourceCheckpoint) -> SourcePosition {
    SourcePosition::Resume {
        attempt: CheckpointAttempt::canonical(3),
        checkpoint,
    }
}

/// The first column of a text source's batch.
pub(super) fn batch_lines(batch: &SourceBatch) -> Vec<String> {
    let column = batch
        .records
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    column.iter().flatten().map(str::to_owned).collect()
}

/// The first batch of at most `max_records` lines, following ownership as the engine does.
pub(super) async fn next_lines(source: &mut FileSource, max_records: usize) -> Vec<String> {
    for _ in 0..200 {
        source.drive_control_plane();
        if let Some(batch) = source.poll_batch(max_records).await.unwrap() {
            return batch_lines(&batch);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("no batch within a second");
}

/// Lines read until the source stays empty, in read order.
pub(super) async fn read_lines(source: &mut FileSource) -> Vec<String> {
    let mut lines = Vec::new();
    let mut idle = 0;
    while idle < 20 {
        source.drive_control_plane();
        let Some(batch) = source.poll_batch(64).await.unwrap() else {
            idle += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        idle = 0;
        lines.extend(batch_lines(&batch));
    }
    lines
}
