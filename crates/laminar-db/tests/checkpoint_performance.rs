use laminar_db::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use laminar_storage::checkpoint_manifest::CheckpointManifest;
use laminar_storage::checkpoint_store::{CheckpointStore, CheckpointStoreError};
use std::time::{Duration, Instant};

struct BlockingCheckpointStore {
    delay: Duration,
}

impl BlockingCheckpointStore {
    fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

impl CheckpointStore for BlockingCheckpointStore {
    fn save(&self, _manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        // Block the thread to simulate synchronous I/O
        std::thread::sleep(self.delay);
        Ok(())
    }

    fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Ok(None)
    }

    fn load_by_id(&self, _id: u64) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Ok(None)
    }

    fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        Ok(vec![])
    }

    fn prune(&self, _keep_count: usize) -> Result<usize, CheckpointStoreError> {
        Ok(0)
    }

    fn save_state_data(&self, _id: u64, _data: &[u8]) -> Result<(), CheckpointStoreError> {
        Ok(())
    }

    fn load_state_data(&self, _id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        Ok(None)
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn test_checkpoint_non_blocking() {
    let delay = Duration::from_millis(200);
    // Use the blocking store to simulate slow I/O
    let store = Box::new(BlockingCheckpointStore::new(delay));
    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store);

    // Spawn a background task that measures tick intervals
    // If the runtime is blocked, these intervals will spike
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

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
        .checkpoint(
            std::collections::HashMap::new(),
            None,
            0,
            vec![],
            None,
            std::collections::HashMap::new(),
            None,
        )
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
