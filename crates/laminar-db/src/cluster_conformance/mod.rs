//! Cluster SQL conformance: one query either produces the embedded result on three in-process
//! cluster nodes or is rejected at `CREATE`. Embedded output is also checked against batch SQL
//! over the complete input, so a defect shared by both modes cannot pass as agreement.

mod cluster;
mod oracle;
mod source;
mod tests;
mod views;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;

use crate::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal, SubscriptionReader};
use crate::LaminarDB;
use cluster::{Cluster, Startup};
use oracle::{Consolidated, OutputForm};
use source::FixtureSource;

pub(super) const DEADLINE: Duration = Duration::from_secs(40);
const NODES: u64 = 3;
const VNODES: u32 = 12;

struct Case {
    name: &'static str,
    sources: Vec<FixtureSource>,
    streams: Vec<String>,
    observe: &'static str,
    form: OutputForm,
    /// Batch SQL over the complete input whose rows equal the consolidated stream output.
    ground_truth: Option<&'static str>,
    /// Checkpoint repeatedly while the sources deliver; in cluster mode some checkpoint flush
    /// must move rows across an exchange.
    checkpoint_under_load: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// Cluster output equals embedded output.
    Equal,
    /// Cluster admission rejected the catalog before any node started.
    Rejected(String),
    /// Embedded rejects it too, so it is not a cluster gap.
    UnsupportedEverywhere(String),
}

impl Case {
    fn ddl(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(FixtureSource::ddl)
            .chain(self.streams.iter().cloned())
            .collect()
    }

    fn batch_count(&self) -> u64 {
        self.sources.iter().map(FixtureSource::batch_count).sum()
    }

    /// Classify the case; `Err` is a conformance bug with its evidence.
    async fn run(&self) -> Result<Verdict, String> {
        let embedded = match self.run_embedded().await? {
            Ok(output) => output,
            Err(reason) => return Ok(Verdict::UnsupportedEverywhere(reason)),
        };
        if embedded.is_empty() {
            return Err(format!(
                "{}: empty embedded output proves nothing",
                self.name
            ));
        }
        if let Some(sql) = self.ground_truth {
            let truth = oracle::ground_truth(&self.sources, sql).await?;
            if truth != embedded {
                return Err(format!(
                    "{}: embedded output diverges from batch ground truth; {}",
                    self.name,
                    oracle::diff(&truth, &embedded)
                ));
            }
        }
        let cluster = match Cluster::start(NODES, VNODES, &self.sources, &self.ddl()).await {
            Startup::Running(cluster) => cluster,
            Startup::Rejected(error) => return Ok(Verdict::Rejected(error.to_string())),
        };
        let observed = self.observe_cluster(&cluster).await;
        cluster.shutdown().await;
        let observed = observed?;
        if observed != embedded {
            return Err(format!(
                "{}: cluster output diverges from embedded; {}",
                self.name,
                oracle::diff(&embedded, &observed)
            ));
        }
        Ok(Verdict::Equal)
    }

    /// `Ok(Err(reason))` when embedded DDL rejects the catalog.
    async fn run_embedded(&self) -> Result<Result<Consolidated, String>, String> {
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        let polled = Arc::new(AtomicU64::new(0));
        let db = embedded_db(&self.sources, &polled, directory.path()).await?;
        for statement in self.ddl() {
            if let Err(error) = db.execute(&statement).await {
                let _ = db.shutdown().await;
                return Ok(Err(error.to_string()));
            }
        }
        let reader = self.subscribe(&db)?;
        let started = db.start().await.map_err(|e| format!("embedded start: {e}"));
        let output = match started.and_then(|()| self.portal(&db, reader)) {
            Ok(portal) => {
                self.drain(&[Arc::clone(&db)], &mut [portal], &[polled])
                    .await
            }
            Err(error) => Err(error),
        };
        let _ = tokio::time::timeout(DEADLINE, db.shutdown()).await;
        Ok(Ok(oracle::consolidate(&output?, self.form)
            .map_err(|e| format!("{}: embedded {e}", self.name))?))
    }

    async fn observe_cluster(&self, cluster: &Cluster) -> Result<Consolidated, String> {
        let readers = cluster
            .dbs
            .iter()
            .map(|db| self.subscribe(db))
            .collect::<Result<Vec<_>, _>>()?;
        cluster
            .activate()
            .await
            .map_err(|e| format!("{}: cluster runtime failure at start: {e}", self.name))?;
        let mut portals = cluster
            .dbs
            .iter()
            .zip(readers)
            .map(|(db, reader)| self.portal(db, reader))
            .collect::<Result<Vec<_>, _>>()?;
        let output = self
            .drain(&cluster.dbs, &mut portals, &cluster.polled)
            .await?;
        if self.checkpoint_under_load && !cluster.dbs.iter().any(|db| flush_crossed_exchange(db)) {
            return Err(format!(
                "{}: no checkpoint flush ran past its first wave and idle confirmation",
                self.name
            ));
        }
        if let Some(idle) = cluster
            .polled
            .iter()
            .position(|polled| polled.load(Ordering::Acquire) == 0)
        {
            return Err(format!("{}: node {} read no input", self.name, idle + 1));
        }
        oracle::consolidate(&output, self.form).map_err(|e| format!("{}: cluster {e}", self.name))
    }

    /// Owner-local observation of the stream log, attached before start so no output is missed;
    /// it makes no distributed delivery claim.
    fn subscribe(&self, db: &LaminarDB) -> Result<SubscriptionReader, String> {
        db.subscription_registry
            .subscribe(self.observe, SubscribeStart::Tail)
            .map_err(|e| format!("{}: subscribe: {e:?}", self.name))
    }

    /// Stream schemas resolve at launch, so the portal wraps the reader after start.
    fn portal(
        &self,
        db: &LaminarDB,
        reader: SubscriptionReader,
    ) -> Result<SubscriptionPortal, String> {
        let schema = db
            .lookup_subscription_schema(self.observe)
            .ok_or_else(|| format!("{}: stream '{}' has no schema", self.name, self.observe))?;
        Ok(SubscriptionPortal::open(self.observe, schema, reader))
    }

    /// Wait until every fixture batch is delivered, commit a checkpoint on the first (leader)
    /// node, and read everything each portal published before it.
    async fn drain(
        &self,
        dbs: &[Arc<LaminarDB>],
        portals: &mut [SubscriptionPortal],
        polled: &[Arc<AtomicU64>],
    ) -> Result<Vec<RecordBatch>, String> {
        let total = self.batch_count();
        let delivered = || -> u64 { polled.iter().map(|p| p.load(Ordering::Acquire)).sum() };
        if self.checkpoint_under_load {
            let under_load = async {
                while delivered() < total {
                    self.checkpoint(dbs).await?;
                }
                Ok::<_, String>(())
            };
            tokio::time::timeout(DEADLINE, under_load)
                .await
                .map_err(|_| {
                    format!("{}: sources did not finish under checkpoints", self.name)
                })??;
        }
        wait_until(DEADLINE, || delivered() >= total)
            .await
            .map_err(|()| {
                format!(
                    "{}: sources delivered {} of {total} batches",
                    self.name,
                    delivered()
                )
            })?;
        self.checkpoint(dbs).await?;
        let mut batches = Vec::new();
        for portal in portals.iter_mut() {
            read_published(portal, &mut batches).map_err(|e| format!("{}: {e}", self.name))?;
        }
        Ok(batches)
    }

    /// Commit a checkpoint from the first (leader) node.
    async fn checkpoint(&self, dbs: &[Arc<LaminarDB>]) -> Result<(), String> {
        let checkpoint = dbs[0]
            .checkpoint_with_timeout(DEADLINE)
            .await
            .map_err(|e| format!("{}: checkpoint on {} node(s): {e}", self.name, dbs.len()))?;
        if !checkpoint.success {
            return Err(format!("{}: checkpoint failed: {checkpoint:?}", self.name));
        }
        Ok(())
    }
}

/// An embedded database that reads every fixture partition and checkpoints into `directory`.
async fn embedded_db(
    sources: &[FixtureSource],
    polled: &Arc<AtomicU64>,
    directory: &std::path::Path,
) -> Result<Arc<LaminarDB>, String> {
    LaminarDB::builder()
        .delivery_guarantee(laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.into()),
            interval_ms: None,
            ..Default::default()
        })
        .register_connector(source::register(sources.to_vec(), Arc::clone(polled)))
        .build()
        .await
        .map_err(|e| e.to_string())
}

/// Whether a checkpoint flush on `db` ran past its first wave and idle confirmation, i.e. moved
/// rows across a remote exchange while the sources were held.
fn flush_crossed_exchange(db: &LaminarDB) -> bool {
    let Some(metrics) = db.engine_metrics() else {
        return false;
    };
    let families = prometheus::core::Collector::collect(&metrics.checkpoint_shuffle_flush_waves);
    let histogram = families[0].get_metric()[0].get_histogram();
    let within_two_waves = histogram
        .get_bucket()
        .iter()
        .find(|bucket| bucket.upper_bound() >= 2.0)
        .map_or(0, prometheus::proto::Bucket::cumulative_count);
    within_two_waves < histogram.get_sample_count()
}

async fn wait_until(deadline: Duration, mut done: impl FnMut() -> bool) -> Result<(), ()> {
    tokio::time::timeout(deadline, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| ())
}

/// Every row published before a committed checkpoint is already in the log: the cut drains the
/// graph and publishes cycle output before capture. Cluster barriers go to the distributed
/// subscription gateway, not this owner-local log, so no barrier frame is awaited.
fn read_published(
    portal: &mut SubscriptionPortal,
    batches: &mut Vec<RecordBatch>,
) -> Result<(), String> {
    while let Some(frame) = portal.try_next_frame() {
        match frame {
            PortalFrame::Batch { batch, .. } => batches.push(batch),
            PortalFrame::Barrier { .. } => {}
            terminal => return Err(format!("terminal portal frame: {terminal:?}")),
        }
    }
    Ok(())
}
