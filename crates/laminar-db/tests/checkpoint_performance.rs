#![allow(clippy::disallowed_types)]
use async_trait::async_trait;
use laminar_db::checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
};
use laminar_storage::checkpoint_manifest::CheckpointManifest;
use laminar_storage::checkpoint_store::{CheckpointStore, CheckpointStoreError};
use std::time::{Duration, Instant};

/// Simulates a slow I/O store. With the async trait this is a
/// cooperative delay via `tokio::time::sleep`, not a thread-block — the
/// runtime stays responsive while the checkpoint is in flight.
struct SlowCheckpointStore {
    delay: Duration,
}

impl SlowCheckpointStore {
    fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

#[async_trait]
impl CheckpointStore for SlowCheckpointStore {
    async fn save(&self, _manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        tokio::time::sleep(self.delay).await;
        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Ok(None)
    }

    async fn load_by_id(
        &self,
        _id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Ok(None)
    }

    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        Ok(vec![])
    }

    async fn prune(&self, _keep_count: usize) -> Result<usize, CheckpointStoreError> {
        Ok(0)
    }

    async fn save_state_data(
        &self,
        _id: u64,
        _data: &[u8],
    ) -> Result<(), CheckpointStoreError> {
        Ok(())
    }

    async fn load_state_data(
        &self,
        _id: u64,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        Ok(None)
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn test_checkpoint_non_blocking() {
    let delay = Duration::from_millis(200);
    // Use the slow store to simulate slow I/O
    let store = Box::new(SlowCheckpointStore::new(delay));
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), store).await;

    // Spawn a background task that measures tick intervals
    // If the runtime is blocked, these intervals will spike
    let (tx, rx) = crossfire::mpsc::bounded_async::<Duration>(100);

    let ticker = tokio::spawn(async move {
        let mut last_tick = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = Instant::now();
            let elapsed = now.duration_since(last_tick);
            if tx.send(elapsed).await.is_err() {
                break;
            }
            last_tick = now;
        }
    });

    // Run a checkpoint
    // This should take ~200ms total
    let start = Instant::now();
    let result = coordinator
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    let duration = start.elapsed();

    assert!(result.success);
    assert!(
        duration >= delay,
        "Checkpoint duration ({:?}) should be at least delay ({:?})",
        duration,
        delay
    );

    // Stop ticker and wait for it to finish
    ticker.abort();

    // Collect stats
    let mut max_interval = Duration::ZERO;
    let mut count = 0;
    while let Ok(interval) = rx.try_recv() {
        if interval > max_interval {
            max_interval = interval;
        }
        count += 1;
    }

    println!("Ticks collected: {}", count);
    println!("Max tick interval: {:?}", max_interval);
    println!("Checkpoint duration: {:?}", duration);

    // If blocking, max_interval ~= 200ms + 10ms
    // If non-blocking, max_interval ~= 10ms + overhead

    // Assert that we did NOT block the runtime
    // We allow some margin (e.g., 50ms) but it should be far less than 200ms
    assert!(
        max_interval < Duration::from_millis(100),
        "Checkpoint blocked the runtime! Max interval: {:?} (expected < 100ms)",
        max_interval
    );
}
