//! View conformance: once every fixture batch is delivered and a checkpoint commits, each read of
//! an embedded materialized view equals batch SQL over the complete input. Each case also states
//! the cluster verdict on the same catalog; a case that must change verdict changes here in the
//! same commit as the runtime change that moves it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;

use super::cluster::{Cluster, Startup};
use super::oracle::{self, OutputForm};
use super::source::FixtureSource;
use super::tests::{assert_rejected, events, CLOSED_TUMBLE_TRUTH, INTERVAL_JOIN};
use super::{embedded_db, wait_until, Verdict, DEADLINE, NODES, VNODES};
use crate::{ExecuteResult, LaminarDB};

struct ViewCase {
    name: &'static str,
    sources: Vec<FixtureSource>,
    views: Vec<String>,
    /// Each read and the batch SQL over the complete input whose rows it must equal.
    reads: Vec<(&'static str, &'static str)>,
}

impl ViewCase {
    fn ddl(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(FixtureSource::ddl)
            .chain(self.views.iter().cloned())
            .collect()
    }

    /// Check every embedded read, then classify the catalog in cluster mode.
    async fn run(&self) -> Result<Verdict, String> {
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        let polled = Arc::new(AtomicU64::new(0));
        let db = embedded_db(&self.sources, &polled, directory.path()).await?;
        let checked = self.check_reads_after_drain(&db, &polled).await;
        let _ = tokio::time::timeout(DEADLINE, db.shutdown()).await;
        checked?;
        match Cluster::start(NODES, VNODES, &self.sources, &self.ddl()).await {
            Startup::Rejected(error) => Ok(Verdict::Rejected(error.to_string())),
            Startup::Running(cluster) => {
                cluster.shutdown().await;
                Err(format!(
                    "{}: cluster admitted the views but has no committed view read to compare",
                    self.name
                ))
            }
        }
    }

    async fn check_reads_after_drain(
        &self,
        db: &Arc<LaminarDB>,
        polled: &AtomicU64,
    ) -> Result<(), String> {
        for statement in self.ddl() {
            db.execute(&statement)
                .await
                .map_err(|e| format!("{}: embedded `{statement}`: {e}", self.name))?;
        }
        db.start()
            .await
            .map_err(|e| format!("{}: embedded start: {e}", self.name))?;
        let total: u64 = self.sources.iter().map(FixtureSource::batch_count).sum();
        wait_until(DEADLINE, || polled.load(Ordering::Acquire) >= total)
            .await
            .map_err(|()| format!("{}: sources did not deliver {total} batches", self.name))?;
        // The cut drains the graph and applies cycle output to every view before capture.
        let checkpoint = db
            .checkpoint_with_timeout(DEADLINE)
            .await
            .map_err(|e| format!("{}: checkpoint: {e}", self.name))?;
        if !checkpoint.success {
            return Err(format!("{}: checkpoint failed: {checkpoint:?}", self.name));
        }
        for (read, truth) in &self.reads {
            let expected = oracle::ground_truth(&self.sources, truth).await?;
            if expected.is_empty() {
                return Err(format!("{}: empty ground truth for `{read}`", self.name));
            }
            let observed = oracle::consolidate(&read_all(db, read).await?, OutputForm::Rows)?;
            if observed != expected {
                return Err(format!(
                    "{}: `{read}` diverges from batch ground truth; {}",
                    self.name,
                    oracle::diff(&expected, &observed)
                ));
            }
        }
        Ok(())
    }
}

/// Every batch of one query; its result stream closes after the last batch.
async fn read_all(db: &LaminarDB, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = db.execute(sql).await.map_err(|e| format!("`{sql}`: {e}"))?;
    let ExecuteResult::Query(mut query) = result else {
        return Err(format!("`{sql}` did not return a query result"));
    };
    let mut subscription = query.subscribe_raw().map_err(|e| e.to_string())?;
    let mut batches = Vec::new();
    loop {
        match tokio::time::timeout(DEADLINE, subscription.recv_async()).await {
            Ok(Ok(batch)) => batches.push(batch),
            Ok(Err(_closed)) => return Ok(batches),
            Err(_) => return Err(format!("`{sql}` did not close within {DEADLINE:?}")),
        }
    }
}

async fn verdict(case: ViewCase) -> Verdict {
    match case.run().await {
        Ok(verdict) => verdict,
        Err(bug) => panic!("view conformance bug: {bug}"),
    }
}

fn views(statements: &[&str]) -> Vec<String> {
    statements.iter().map(|s| (*s).to_owned()).collect()
}

const KEYED_TRUTH: &str = "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k";

/// Keyed upsert, global replace-every-cycle, and the multiset and keyed views chained over an
/// incremental view, plus one statement that reads two views.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_and_chained_views_are_rejected_in_cluster() {
    let case = ViewCase {
        name: "keyed_and_chained_views",
        sources: vec![events("events", 0)],
        views: views(&[
            "CREATE MATERIALIZED VIEW counts AS \
             SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k",
            "CREATE MATERIALIZED VIEW totals AS SELECT COUNT(*) AS n, SUM(v) AS s FROM events",
            "CREATE MATERIALIZED VIEW busy AS SELECT k, n FROM counts WHERE n > 12",
            "CREATE MATERIALIZED VIEW histogram AS \
             SELECT n, COUNT(*) AS key_count FROM counts GROUP BY n",
        ]),
        reads: vec![
            ("SELECT * FROM counts", KEYED_TRUTH),
            (
                "SELECT * FROM totals",
                "SELECT COUNT(*) AS n, SUM(v) AS s FROM events",
            ),
            (
                "SELECT * FROM busy",
                "SELECT k, COUNT(*) AS n FROM events GROUP BY k HAVING COUNT(*) > 12",
            ),
            (
                "SELECT * FROM histogram",
                "SELECT n, COUNT(*) AS key_count FROM \
                 (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n",
            ),
            (
                "SELECT k, n FROM busy WHERE k IN (SELECT k FROM counts WHERE s % 2 = 0)",
                "SELECT k, COUNT(*) AS n FROM events GROUP BY k \
                 HAVING COUNT(*) > 12 AND SUM(v) % 2 = 0",
            ),
        ],
    };
    assert_rejected(verdict(case).await, "materialized view");
}

/// A projection over a source and a closed tumbling window both append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_views_are_rejected_in_cluster() {
    let case = ViewCase {
        name: "append_views",
        sources: vec![events("events", 0)],
        views: views(&[
            "CREATE MATERIALIZED VIEW multiples AS \
             SELECT k, v * 2 AS v2 FROM events WHERE v % 3 = 0",
            "CREATE MATERIALIZED VIEW windows AS \
             SELECT k % 4 AS g, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n, SUM(v) AS s FROM events \
             GROUP BY k % 4, tumble(ts, INTERVAL '100' MILLISECOND) EMIT ON WINDOW CLOSE",
        ]),
        reads: vec![
            (
                "SELECT * FROM multiples",
                "SELECT k, v * 2 AS v2 FROM events WHERE v % 3 = 0",
            ),
            ("SELECT * FROM windows", CLOSED_TUMBLE_TRUTH),
        ],
    };
    assert_rejected(verdict(case).await, "materialized view");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_view_is_rejected_in_cluster() {
    let case = ViewCase {
        name: "interval_join_view",
        sources: vec![events("orders", 0), events("payments", 3)],
        views: vec![format!(
            "CREATE MATERIALIZED VIEW joined AS {INTERVAL_JOIN}"
        )],
        reads: vec![("SELECT * FROM joined", INTERVAL_JOIN)],
    };
    assert_rejected(verdict(case).await, "materialized view");
}
