//! Queries that read every materialized view at one committed checkpoint.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::LaminarDB;
use crate::error::DbError;
use crate::mv_store::{CommittedRead, CommittedReadError, VIEW_READ_DEADLINE};

/// Rows of a committed read, and the committed cut every view in it was read at.
#[derive(Debug, Clone)]
pub struct CommittedQuery {
    /// Epoch of the cut.
    pub epoch: u64,
    /// Checkpoint that committed the cut.
    pub checkpoint_id: u64,
    /// When the cut was taken, in Unix milliseconds; the result is as old as now minus this.
    pub taken_at_ms: u64,
    /// Schema of the result.
    pub schema: SchemaRef,
    /// Result rows.
    pub batches: Vec<RecordBatch>,
}

impl LaminarDB {
    /// Run the query `sql` with every materialized view it scans read at the one committed
    /// checkpoint `at` resolves to.
    ///
    /// The result never mixes checkpoints and is never rolled back, and a later
    /// [`CommittedRead::Latest`] read on this database never sees an earlier epoch. It is as fresh
    /// as the checkpoint cadence. Pass a returned epoch back as [`CommittedRead::AtLeast`] to read
    /// no earlier than a previous result. An append view with a row-count retention is not exact:
    /// which rows a cap keeps depends on arrival order.
    ///
    /// The statement may scan only materialized views. The checkpoint is held against retention
    /// for at most 30 seconds, which bounds the read, including any wait for
    /// [`CommittedRead::AtLeast`]. A cluster node reads every participant's committed state, so
    /// any node serves any view; it reads only cuts at or after the durable pin it holds. A node
    /// outside the checkpoint participants (draining, or not yet assigned) learns of each cut of
    /// its own topology from the cluster authority within about a second.
    ///
    /// # Errors
    /// [`DbError::CommittedRead`] when checkpointing is disabled, the requested epoch has not
    /// committed in time or is no longer retained, the statement scans live state, or the read
    /// passes its deadline; any planning or execution error of `sql`.
    pub async fn query_committed(
        &self,
        sql: &str,
        at: CommittedRead,
    ) -> Result<CommittedQuery, DbError> {
        self.query_committed_until(sql, at, Instant::now() + VIEW_READ_DEADLINE)
            .await
    }

    pub(crate) async fn query_committed_until(
        &self,
        sql: &str,
        at: CommittedRead,
        deadline: Instant,
    ) -> Result<CommittedQuery, DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }
        let (frame, cut) =
            crate::mv_store::plan_committed_statement(&self.ctx, &self.mv_store, sql, at, deadline)
                .await?;
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), frame.collect())
                .await
                .map_err(|_| CommittedReadError::DeadlineExceeded { epoch: cut.epoch })?
                .map_err(crate::query_rows::execution_error)?;
        Ok(CommittedQuery {
            epoch: cut.epoch,
            checkpoint_id: cut.checkpoint_id,
            taken_at_ms: cut.taken_at_ms,
            schema,
            batches,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use laminar_core::streaming::StreamCheckpointConfig;

    use super::*;
    use crate::config::LaminarConfig;

    async fn pipeline(dir: &std::path::Path) -> Arc<LaminarDB> {
        let db = LaminarDB::open_with_config(LaminarConfig {
            storage_dir: Some(dir.to_path_buf()),
            checkpoint: Some(StreamCheckpointConfig {
                interval_ms: None,
                ..StreamCheckpointConfig::default()
            }),
            ..LaminarConfig::default()
        })
        .unwrap();
        for sql in [
            "CREATE SOURCE events (k BIGINT)",
            "CREATE MATERIALIZED VIEW tail AS SELECT k FROM events WITH (retention = '10 rows')",
            "CREATE MATERIALIZED VIEW counts AS SELECT k, COUNT(*) AS n FROM events GROUP BY k",
        ] {
            db.execute(sql).await.unwrap();
        }
        db.start().await.unwrap();
        db
    }

    async fn push_and_checkpoint(db: &LaminarDB, k: i64) -> u64 {
        let batch = RecordBatch::try_from_iter_with_nullable(vec![(
            "k",
            Arc::new(Int64Array::from(vec![k])) as arrow::array::ArrayRef,
            true,
        )])
        .unwrap();
        db.source_untyped("events")
            .unwrap()
            .push_arrow(batch)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let result = db.checkpoint().await.unwrap();
        assert!(result.success, "{:?}", result.error);
        result.epoch
    }

    fn manifest_exists(dir: &std::path::Path, checkpoint_id: u64) -> bool {
        let suffix = std::path::Path::new(&format!("{checkpoint_id:020}")).join("manifest.json");
        let mut pending = vec![dir.to_path_buf()];
        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.ends_with(&suffix) {
                    return true;
                }
                if path.is_dir() {
                    pending.push(path);
                }
            }
        }
        false
    }

    fn keys(batches: &[RecordBatch]) -> Vec<i64> {
        let mut keys: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..column.len())
                    .map(|row| column.value(row))
                    .collect::<Vec<_>>()
            })
            .collect();
        keys.sort_unstable();
        keys
    }

    #[tokio::test]
    async fn a_pinned_cut_outlives_later_checkpoints_until_it_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let db = pipeline(dir.path()).await;
        let first = push_and_checkpoint(&db, 1).await;
        let views = db.mv_store.read().committed_views().unwrap();
        let held = views
            .pin(
                CommittedRead::Exact(first),
                Instant::now() + VIEW_READ_DEADLINE,
            )
            .await
            .unwrap();
        // Each checkpoint rewrites key 1's frame, so nothing later references the first one.
        push_and_checkpoint(&db, 1).await;
        push_and_checkpoint(&db, 1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let pinned = held.checkpoint_id;
        assert!(
            manifest_exists(dir.path(), pinned),
            "retention keeps the pinned cut"
        );
        let shape = db.mv_store.read().view_shape("counts").unwrap();
        let rows = held.read_view(&views, &shape, &[]).await.unwrap();
        let counts = rows[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values().to_vec(), [1]);

        drop(held);
        push_and_checkpoint(&db, 1).await;
        for _ in 0..50 {
            if !manifest_exists(dir.path(), pinned) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            !manifest_exists(dir.path(), pinned),
            "a released cut is retired"
        );
        let mut error = None;
        for _ in 0..50 {
            match db
                .query_committed("SELECT n FROM counts", CommittedRead::Exact(first))
                .await
            {
                Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(other) => {
                    error = Some(other);
                    break;
                }
            }
        }
        assert!(
            matches!(
                error,
                Some(DbError::CommittedRead(CommittedReadError::Retired { requested }))
                    if requested == first
            ),
            "{error:?}"
        );
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_declared_freshness_survives_a_pipeline_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(LaminarConfig {
            storage_dir: Some(dir.path().to_path_buf()),
            checkpoint: Some(StreamCheckpointConfig {
                interval_ms: Some(1_000),
                ..StreamCheckpointConfig::default()
            }),
            ..LaminarConfig::default()
        })
        .unwrap();
        db.execute("CREATE SOURCE events (k BIGINT)").await.unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW counts AS SELECT k, COUNT(*) AS n FROM events GROUP BY k \
             WITH (freshness = '5 seconds')",
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        db.stop_pipeline().await.unwrap();
        db.start().await.unwrap();
        let contracts = db.mv_store.read().freshness_contracts();
        assert_eq!(
            contracts.declared_freshness("counts"),
            Some(Duration::from_secs(5))
        );
        db.stop_pipeline().await.unwrap();
        db.execute("DROP MATERIALIZED VIEW counts").await.unwrap();
        assert_eq!(contracts.declared_freshness("counts"), None);
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_read_waits_for_its_epoch_until_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let db = pipeline(dir.path()).await;
        let first = push_and_checkpoint(&db, 1).await;
        let error = db
            .query_committed_until(
                "SELECT k FROM tail",
                CommittedRead::AtLeast(first + 100),
                Instant::now() + Duration::from_millis(200),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                DbError::CommittedRead(CommittedReadError::NotCommitted {
                    requested: Some(requested),
                    latest: Some(latest),
                }) if requested == first + 100 && latest == first
            ),
            "{error:?}"
        );

        let writer = Arc::clone(&db);
        let next = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            push_and_checkpoint(&writer, 2).await
        });
        let read = db
            .query_committed("SELECT k FROM tail", CommittedRead::AtLeast(first + 1))
            .await
            .unwrap();
        let second = next.await.unwrap();
        assert_eq!(read.epoch, second);
        assert_eq!(keys(&read.batches), [1, 2]);
        db.shutdown().await.unwrap();
    }
}
