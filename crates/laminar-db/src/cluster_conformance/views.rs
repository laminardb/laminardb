//! View conformance: once every fixture batch is delivered and a checkpoint commits, each read of
//! an embedded materialized view equals batch SQL over the complete input, both at the last cycle
//! and at the committed checkpoint. Each case also states
//! the cluster verdict on the same catalog; a case that must change verdict changes here in the
//! same commit as the runtime change that moves it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;

use super::cluster::{Cluster, Startup};
use super::oracle::{self, OutputForm};
use super::source::FixtureSource;
use super::tests::{events, events_in, events_with, CLOSED_TUMBLE_TRUTH, INTERVAL_JOIN};
use super::{embedded_db, Verdict, DEADLINE, NODES, VNODES};
use crate::subscription::{
    ClusterSubscriptionFrameMetadata, PortalFrame, SubscribeStart, SubscriptionPortal,
};
use crate::{CommittedRead, ExecuteResult, LaminarDB};

struct ViewCase {
    name: &'static str,
    sources: Vec<FixtureSource>,
    views: Vec<String>,
    /// Each read and the batch SQL over the complete input whose rows it must equal.
    reads: Vec<(&'static str, &'static str)>,
    /// Reads that return no rows whenever every view they scan comes from one cycle. They run
    /// repeatedly while the sources deliver.
    same_cycle_invariants: Vec<&'static str>,
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
        let cluster = match Cluster::start(NODES, VNODES, &self.sources, &self.ddl()).await {
            Startup::Rejected(error) => return Ok(Verdict::Rejected(error.to_string())),
            Startup::Running(cluster) => cluster,
        };
        let checked = self.check_cluster_reads(&cluster).await;
        cluster.shutdown().await;
        checked.map(|()| Verdict::Equal)
    }

    /// Every node reads every view at the cut committed after the sources delivered, each read
    /// equals batch SQL over the complete input, and a live read of a view is refused.
    async fn check_cluster_reads(&self, cluster: &Cluster) -> Result<(), String> {
        cluster.allowance.store(0, Ordering::Release);
        cluster
            .activate()
            .await
            .map_err(|e| format!("{}: cluster start: {e}", self.name))?;
        let total: u64 = self.sources.iter().map(FixtureSource::batch_count).sum();
        let delivered = || -> u64 {
            cluster
                .polled
                .iter()
                .map(|polled| polled.load(Ordering::Acquire))
                .sum()
        };
        // Subscribers attach before any input, a second cut holds part of it, and the last all.
        self.cluster_checkpoint(cluster).await?;
        let mut subscriptions = self.subscribe(cluster, SubscribeStart::Tail).await?;
        cluster.allowance.store(1, Ordering::Release);
        super::wait_until(DEADLINE, || delivered() >= 1)
            .await
            .map_err(|()| format!("{}: cluster sources did not deliver", self.name))?;
        let partial = self.cluster_checkpoint(cluster).await?;
        cluster.allowance.store(u64::MAX, Ordering::Release);
        super::wait_until(DEADLINE, || delivered() >= total)
            .await
            .map_err(|()| format!("{}: cluster sources did not deliver", self.name))?;
        let checkpoint = self.cluster_checkpoint(cluster).await?;
        subscriptions.extend(
            self.subscribe(cluster, SubscribeStart::AsOfEpoch(partial.epoch))
                .await?
                .into_iter()
                .take(1),
        );
        for (truth, portal) in &mut subscriptions {
            self.check_subscription(portal, truth, checkpoint.epoch)
                .await?;
        }
        for (read, truth) in &self.reads {
            // A node holds only its part of each view, so it refuses a live read.
            let live = cluster.dbs[1].execute(read).await;
            if !live
                .as_ref()
                .is_err_and(|error| error.to_string().contains("committed cut"))
            {
                return Err(format!(
                    "{}: node 2 answered live `{read}` from its part",
                    self.name
                ));
            }
            let expected = oracle::ground_truth(&self.sources, truth).await?;
            for (node, db) in cluster.dbs.iter().enumerate() {
                let committed = db
                    .query_committed(read, CommittedRead::Exact(checkpoint.epoch))
                    .await
                    .map_err(|e| {
                        format!("{}: node {} committed `{read}`: {e}", self.name, node + 1)
                    })?;
                let observed = oracle::consolidate(&committed.batches, OutputForm::Rows)?;
                if observed != expected {
                    return Err(format!(
                        "{}: node {} committed `{read}` diverges from batch ground truth; {}",
                        self.name,
                        node + 1,
                        oracle::diff(&expected, &observed)
                    ));
                }
            }
            // A session that chose no level reads a cluster node at its newest committed cut.
            let node = &cluster.dbs[1];
            let read_at_default = node
                .read(read, node.default_read_level())
                .await
                .map_err(|e| format!("{}: node 2 read `{read}`: {e}", self.name))?;
            if read_at_default
                .cut
                .is_none_or(|cut| cut.epoch < checkpoint.epoch)
            {
                return Err(format!(
                    "{}: node 2 read `{read}` at {:?}, not a committed cut at or after {}",
                    self.name, read_at_default.cut, checkpoint.epoch
                ));
            }
            let rows = read_at_default
                .rows
                .read_all()
                .await
                .map_err(|e| format!("{}: node 2 read `{read}`: {e}", self.name))?;
            let observed = oracle::consolidate(&rows, OutputForm::Rows)?;
            if observed != expected {
                return Err(format!(
                    "{}: node 2 read `{read}` diverges from batch ground truth; {}",
                    self.name,
                    oracle::diff(&expected, &observed)
                ));
            }
            // The replayed catalog leads a recent read to its sources, which report no upstream.
            let recent = node
                .query_committed(read, CommittedRead::Recent { at_least: 0 })
                .await;
            let reached_source = matches!(
                &recent,
                Err(crate::DbError::CommittedRead(
                    crate::CommittedReadError::RecencyUnsupported { input }
                )) if self.sources.iter().any(|source| source.name == input)
            );
            if !reached_source {
                return Err(format!(
                    "{}: node 2 recent `{read}` did not reach its sources: {recent:?}",
                    self.name
                ));
            }
        }
        Ok(())
    }

    async fn cluster_checkpoint(
        &self,
        cluster: &Cluster,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, String> {
        let checkpoint = cluster.dbs[0]
            .checkpoint_with_timeout(DEADLINE)
            .await
            .map_err(|e| format!("{}: cluster checkpoint: {e}", self.name))?;
        if !checkpoint.success {
            return Err(format!(
                "{}: cluster checkpoint failed: {checkpoint:?}",
                self.name
            ));
        }
        Ok(checkpoint)
    }

    /// A subscription to each view a read scans whole, spread over the nodes, with the batch SQL
    /// its rows must equal.
    async fn subscribe(
        &self,
        cluster: &Cluster,
        start: SubscribeStart,
    ) -> Result<Vec<(&'static str, SubscriptionPortal)>, String> {
        let mut subscriptions = Vec::new();
        for (index, (read, truth)) in self.reads.iter().enumerate() {
            let Some(view) = read.strip_prefix("SELECT * FROM ") else {
                continue;
            };
            let portal = cluster.dbs[index % cluster.dbs.len()]
                .open_subscription(view, None, start)
                .await
                .map_err(|e| format!("{}: subscribe to {view} at {start:?}: {e}", self.name))?;
            subscriptions.push((*truth, portal));
        }
        Ok(subscriptions)
    }

    /// Fold everything `portal` delivers through committed epoch `through`: a snapshot, then
    /// changes. It must equal `truth` over the complete input.
    async fn check_subscription(
        &self,
        portal: &mut SubscriptionPortal,
        truth: &str,
        through: u64,
    ) -> Result<(), String> {
        let mut batches = Vec::new();
        let mut snapshot = true;
        loop {
            let envelope = tokio::time::timeout(DEADLINE, portal.next_envelope())
                .await
                .map_err(|_| format!("{}: subscription stalled before {through}", self.name))?
                .ok_or_else(|| format!("{}: subscription closed", self.name))?;
            match (envelope.frame, envelope.cluster) {
                (
                    PortalFrame::Batch { batch, .. },
                    Some(ClusterSubscriptionFrameMetadata::ViewRows { snapshot: rows, .. }),
                ) => {
                    if rows != snapshot {
                        return Err(format!("{}: snapshot rows after a change", self.name));
                    }
                    batches.push(batch);
                }
                (PortalFrame::Barrier { epoch, .. }, _) if epoch >= through => break,
                (PortalFrame::Barrier { .. }, _) => snapshot = false,
                (frame, _) => return Err(format!("{}: unexpected frame {frame:?}", self.name)),
            }
        }
        let observed = oracle::consolidate(&batches, OutputForm::Changelog)?;
        let expected = oracle::ground_truth(&self.sources, truth).await?;
        if observed != expected {
            return Err(format!(
                "{}: subscription through {through} diverges from batch ground truth; {}",
                self.name,
                oracle::diff(&expected, &observed)
            ));
        }
        Ok(())
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
        let delivered = || polled.load(Ordering::Acquire) >= total;
        tokio::time::timeout(DEADLINE, self.check_invariants_until(db, delivered))
            .await
            .map_err(|_| format!("{}: sources did not deliver {total} batches", self.name))??;
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
            let committed = db
                .query_committed(read, CommittedRead::Exact(checkpoint.epoch))
                .await
                .map_err(|e| format!("{}: committed `{read}`: {e}", self.name))?;
            for (level, batches) in [
                ("latest", read_all(db, read).await?),
                ("committed", committed.batches),
            ] {
                let observed = oracle::consolidate(&batches, OutputForm::Rows)?;
                if observed != expected {
                    return Err(format!(
                        "{}: {level} `{read}` diverges from batch ground truth; {}",
                        self.name,
                        oracle::diff(&expected, &observed)
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ViewCase {
    async fn check_invariants_until(
        &self,
        db: &LaminarDB,
        done: impl Fn() -> bool,
    ) -> Result<(), String> {
        while !done() {
            for read in &self.same_cycle_invariants {
                let batches = read_all(db, read).await?;
                if batches.iter().any(|batch| batch.num_rows() > 0) {
                    return Err(format!("{}: `{read}` saw views from two cycles", self.name));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
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
    let rows = query.subscribe_raw().map_err(|e| e.to_string())?;
    tokio::time::timeout(DEADLINE, rows.read_all())
        .await
        .map_err(|_| format!("`{sql}` did not close within {DEADLINE:?}"))?
        .map_err(|e| format!("`{sql}`: {e}"))
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
/// incremental view, plus statements that read two views; the invariant runs while 240 batches
/// are still being delivered. Every cluster node reads the same rows at the committed cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_and_chained_views_match_batch_sql_in_cluster() {
    let case = ViewCase {
        name: "keyed_and_chained_views",
        sources: vec![events_with("events", 0, 40)],
        views: views(&[
            "CREATE MATERIALIZED VIEW counts AS \
             SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k",
            "CREATE MATERIALIZED VIEW totals AS SELECT COUNT(*) AS n, SUM(v) AS s FROM events",
            "CREATE MATERIALIZED VIEW busy AS SELECT k, n FROM counts WHERE n > 160",
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
                "SELECT k, COUNT(*) AS n FROM events GROUP BY k HAVING COUNT(*) > 160",
            ),
            (
                "SELECT * FROM histogram",
                "SELECT n, COUNT(*) AS key_count FROM \
                 (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n",
            ),
            (
                "SELECT k, n FROM busy WHERE k IN (SELECT k FROM counts WHERE s % 2 = 0)",
                "SELECT k, COUNT(*) AS n FROM events GROUP BY k \
                 HAVING COUNT(*) > 160 AND SUM(v) % 2 = 0",
            ),
        ],
        same_cycle_invariants: vec![
            "SELECT k FROM busy b WHERE NOT EXISTS \
             (SELECT 1 FROM counts c WHERE c.k = b.k AND c.n = b.n)",
        ],
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Views that drop their upstream's key are placed by their full row: each node routes a row to
/// the owner of its vnode, and every node reads all of them at the committed cut. Rows repeat
/// across keys, so the owners keep multiplicities from several producers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_placed_views_match_batch_sql_in_cluster() {
    let case = ViewCase {
        name: "row_placed_views",
        sources: vec![events_with("events", 0, 40)],
        views: views(&[
            "CREATE MATERIALIZED VIEW counts AS \
             SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k",
            "CREATE MATERIALIZED VIEW sizes AS SELECT n FROM counts",
            "CREATE MATERIALIZED VIEW sums AS SELECT n, s FROM counts WHERE s % 2 = 0",
        ]),
        reads: vec![
            (
                "SELECT * FROM sizes",
                "SELECT COUNT(*) AS n FROM events GROUP BY k",
            ),
            (
                "SELECT * FROM sums",
                "SELECT COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k HAVING SUM(v) % 2 = 0",
            ),
        ],
        same_cycle_invariants: vec![],
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A projection over a source and a closed tumbling window both append; each node keeps its
/// own part of their logs, and every node reads all of it at the committed cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_views_match_batch_sql_in_cluster() {
    let case = ViewCase {
        name: "append_views",
        sources: vec![events("events", 0)],
        views: views(&[
            "CREATE MATERIALIZED VIEW multiples AS \
             SELECT k, v * 2 AS v2 FROM events WHERE v % 3 = 0 \
             WITH (retention = '100000 rows')",
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
        same_cycle_invariants: vec![],
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// An interval join's output appends, so its view keeps a log on each node like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interval_join_view_matches_batch_sql_in_cluster() {
    let case = ViewCase {
        name: "interval_join_view",
        sources: vec![events("orders", 0), events("payments", 3)],
        views: vec![format!(
            "CREATE MATERIALIZED VIEW joined AS {INTERVAL_JOIN} WITH (retention = '100000 rows')"
        )],
        reads: vec![("SELECT * FROM joined", INTERVAL_JOIN)],
        same_cycle_invariants: vec![],
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Views that aggregate over an aggregating subquery or over an interval join split into internal
/// stages that execute as streams; each view keeps only its outer aggregate's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_views_match_batch_sql_in_cluster() {
    const HISTOGRAM: &str = "SELECT n, COUNT(*) AS key_count FROM \
         (SELECT k, COUNT(*) AS n FROM orders GROUP BY k) GROUP BY n";
    const JOINED_TOTALS: &str = "SELECT o.k AS k, COUNT(*) AS n, SUM(p.v) AS s FROM orders o \
         INNER JOIN payments p ON o.k = p.k \
         AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND GROUP BY o.k";
    let case = ViewCase {
        name: "split_views",
        sources: vec![events("orders", 0), events("payments", 3)],
        views: vec![
            format!("CREATE MATERIALIZED VIEW histogram AS {HISTOGRAM}"),
            format!("CREATE MATERIALIZED VIEW joined_totals AS {JOINED_TOTALS}"),
        ],
        reads: vec![
            ("SELECT * FROM histogram", HISTOGRAM),
            ("SELECT * FROM joined_totals", JOINED_TOTALS),
        ],
        same_cycle_invariants: vec![],
    };
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Two partitions over three nodes leave node 3 without input, so its view is idle and takes no
/// part in the view's committed frontier. Every node keeps exactly the rows at or after that one
/// frontier minus the interval, wherever they were written, and refuses a live read of its part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_view_retention_follows_one_committed_frontier_on_every_node() {
    let sources = vec![events_in("events", 0, 2, 3)];
    let ddl: Vec<String> = sources
        .iter()
        .map(FixtureSource::ddl)
        .chain(views(&[
            "CREATE MATERIALIZED VIEW recent AS \
             SELECT k, v, ts FROM events WITH (retention = '100 milliseconds')",
        ]))
        .collect();
    let Startup::Running(cluster) = Cluster::start(NODES, VNODES, &sources, &ddl).await else {
        panic!("append views are admitted in cluster");
    };
    cluster.activate().await.unwrap();
    let total: u64 = sources.iter().map(FixtureSource::batch_count).sum();
    super::wait_until(DEADLINE, || {
        cluster
            .polled
            .iter()
            .map(|polled| polled.load(Ordering::Acquire))
            .sum::<u64>()
            >= total
    })
    .await
    .expect("sources deliver");
    // A participant's frontier follows the committed cluster frontier, so the view's committed
    // frontier reaches the input's within a few cuts.
    for _ in 0..3 {
        let checkpoint = cluster.dbs[0]
            .checkpoint_with_timeout(DEADLINE)
            .await
            .unwrap();
        assert!(checkpoint.success, "{checkpoint:?}");
    }
    let views = cluster.dbs[0].mv_store.read().committed_views().unwrap();
    let cut = views.latest().unwrap();
    let frontier = *cut
        .index
        .view_frontiers
        .get("recent")
        .expect("the idle node does not hold back the committed frontier");
    let expected = oracle::ground_truth(
        &sources,
        &format!(
            "SELECT k, v, ts FROM events WHERE ts >= to_timestamp_millis({})",
            frontier - 100
        ),
    )
    .await
    .unwrap();
    let all = oracle::ground_truth(&sources, "SELECT k, v, ts FROM events")
        .await
        .unwrap();
    assert!(
        !expected.is_empty() && expected.len() < all.len(),
        "retention must keep some rows and drop others"
    );
    for (node, db) in cluster.dbs.iter().enumerate() {
        let read = db
            .query_committed(
                "SELECT * FROM recent",
                CommittedRead::Exact(cut.index.epoch),
            )
            .await
            .unwrap();
        let observed = oracle::consolidate(&read.batches, OutputForm::Rows).unwrap();
        assert_eq!(
            observed,
            expected,
            "node {} diverges; {}",
            node + 1,
            oracle::diff(&expected, &observed)
        );
    }
    let live = cluster.dbs[1].execute("SELECT * FROM recent").await;
    assert!(
        live.as_ref()
            .is_err_and(|error| error.to_string().contains("committed cut")),
        "a cluster node must refuse a live read of its part"
    );
    cluster.shutdown().await;
}

/// A read on a follower holds its cut against cluster cleanup while later checkpoints commit;
/// once it ends, the node's pin moves on and cleanup retires the cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_cluster_read_keeps_its_cut_from_cleanup() {
    let sources = vec![events_with("events", 0, 4)];
    let ddl: Vec<String> = sources
        .iter()
        .map(FixtureSource::ddl)
        .chain(views(&["CREATE MATERIALIZED VIEW counts AS \
             SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k"]))
        .collect();
    let Startup::Running(cluster) = Cluster::start(NODES, VNODES, &sources, &ddl).await else {
        panic!("keyed views are admitted in cluster");
    };
    cluster.activate().await.unwrap();
    let checkpoint = |db: &Arc<LaminarDB>| {
        let db = Arc::clone(db);
        async move {
            let checkpoint = db.checkpoint_with_timeout(DEADLINE).await.unwrap();
            assert!(checkpoint.success, "{checkpoint:?}");
            checkpoint.epoch
        }
    };
    let held_epoch = checkpoint(&cluster.dbs[0]).await;
    let follower = &cluster.dbs[1];
    let views = follower.mv_store.read().committed_views().unwrap();
    let shape = follower.mv_store.read().view_shape("counts").unwrap();
    let deadline = std::time::Instant::now() + DEADLINE;
    let held = views
        .pin(CommittedRead::Exact(held_epoch), deadline)
        .await
        .unwrap();
    // Longer than the pin's advance interval, so the node pin is asked to move past the cut.
    for _ in 0..3 {
        tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;
        checkpoint(&cluster.dbs[0]).await;
    }
    assert!(!held
        .read_view(&views, &shape, &[])
        .await
        .unwrap()
        .is_empty());
    // Cleanup retires a cut's committed index with it, so an exact read proves it was kept.
    follower
        .query_committed("SELECT * FROM counts", CommittedRead::Exact(held_epoch))
        .await
        .unwrap();
    drop(held);

    let retired = async {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            checkpoint(&cluster.dbs[0]).await;
            let read = follower
                .query_committed("SELECT * FROM counts", CommittedRead::Exact(held_epoch))
                .await;
            if let Err(crate::DbError::CommittedRead(crate::CommittedReadError::Retired {
                ..
            })) = read
            {
                return;
            }
        }
    };
    tokio::time::timeout(DEADLINE, retired)
        .await
        .expect("cleanup retires the cut once no read holds it");
    cluster.shutdown().await;
}

/// Each node's append log compacts while a subscriber follows the view: a checkpoint that
/// writes a merged segment in place of its inputs adds no rows, so each appended row is received
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_view_subscription_receives_each_row_once_across_merges() {
    const TRUTH: &str = "SELECT k, v FROM events";
    let case = ViewCase {
        name: "append_view_merges",
        sources: vec![events_with("events", 0, 40)],
        views: views(&[
            "CREATE MATERIALIZED VIEW appended AS SELECT k, v FROM events \
             WITH (retention = '100000 rows')",
        ]),
        reads: vec![("SELECT * FROM appended", TRUTH)],
        same_cycle_invariants: vec![],
    };
    let Startup::Running(cluster) = Cluster::start(NODES, VNODES, &case.sources, &case.ddl()).await
    else {
        panic!("the append view is admitted in cluster");
    };
    cluster.allowance.store(0, Ordering::Release);
    cluster.activate().await.unwrap();
    case.cluster_checkpoint(&cluster).await.unwrap();
    let mut subscriptions = case
        .subscribe(&cluster, SubscribeStart::Tail)
        .await
        .unwrap();
    let delivered = || -> u64 {
        cluster
            .polled
            .iter()
            .map(|polled| polled.load(Ordering::Acquire))
            .sum()
    };
    // More checkpoints with new rows than one compaction block holds segments.
    for step in 1..=12 {
        cluster.allowance.store(step, Ordering::Release);
        super::wait_until(DEADLINE, || delivered() >= step * NODES)
            .await
            .unwrap();
        case.cluster_checkpoint(&cluster).await.unwrap();
    }
    cluster.allowance.store(u64::MAX, Ordering::Release);
    let total: u64 = case.sources.iter().map(FixtureSource::batch_count).sum();
    super::wait_until(DEADLINE, || delivered() >= total)
        .await
        .unwrap();
    let last = case.cluster_checkpoint(&cluster).await.unwrap();
    for (truth, portal) in &mut subscriptions {
        case.check_subscription(portal, truth, last.epoch)
            .await
            .unwrap();
    }
    cluster.shutdown().await;
}
