use super::{backoff_for_attempt, claim_restart_slot, spawn_supervised_restart};
use crate::config::RestartPolicy;
use crate::db::{DbState, LaminarDB};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceBatch, SourceConnector, SourceConsistency, SourceContract,
    SourceInputMode, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::catalog::CatalogObjectKind;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct TransientStartSource {
    failures_remaining: Arc<AtomicUsize>,
    poll_failure: Arc<AtomicBool>,
}

#[async_trait]
impl SourceConnector for TransientStartSource {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        if self
            .failures_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ConnectorError::ConnectionFailed(
                "injected transient metadata read".into(),
            ));
        }
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.poll_failure.swap(false, Ordering::AcqRel) {
            return Err(ConnectorError::ConfigurationError(
                "injected terminal source poll failure".into(),
            ));
        }
        Ok(None)
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[test]
fn restart_budget_caps_within_window_and_prunes_stale() {
    let p = RestartPolicy::default();
    let mut hist = Vec::new();
    let now = Instant::now();
    for i in 0..p.max_restarts {
        assert_eq!(
            claim_restart_slot(&mut hist, now, p.max_restarts, p.window),
            Some(i)
        );
    }
    assert_eq!(
        claim_restart_slot(&mut hist, now, p.max_restarts, p.window),
        None
    );
    // A window later the stale entries are pruned, freeing the budget again.
    let later = now + p.window * 2;
    assert_eq!(
        claim_restart_slot(&mut hist, later, p.max_restarts, p.window),
        Some(0)
    );
    assert_eq!(hist.len(), 1);
}

#[test]
fn backoff_grows_exponentially_capped() {
    let init = Duration::from_millis(100);
    let max = Duration::from_secs(1);
    assert_eq!(
        backoff_for_attempt(init, max, 0),
        Duration::from_millis(100)
    );
    assert_eq!(
        backoff_for_attempt(init, max, 1),
        Duration::from_millis(200)
    );
    assert_eq!(
        backoff_for_attempt(init, max, 3),
        Duration::from_millis(800)
    );
    assert_eq!(
        backoff_for_attempt(init, max, 4),
        max,
        "1600ms capped at 1s"
    );
    assert_eq!(
        backoff_for_attempt(init, max, 1000),
        max,
        "huge attempt must not overflow"
    );
}

// Drives the real watcher path and transfers startup to its owned driver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_restart_recovers_faulted_pipeline() {
    let db = LaminarDB::open().unwrap();
    db.enable_supervision();
    db.execute(
        "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id FROM trades")
        .await
        .unwrap();

    DbState::Faulted.store(&db.state);
    *db.last_fault.lock() = Some("operator boom".to_string());
    db.shutdown_signal.notify_one();

    let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(
        &prometheus::Registry::new(),
    ));
    let join = spawn_supervised_restart(
        Arc::clone(&db),
        Arc::clone(&db.restart_history),
        Some(Arc::clone(&metrics)),
    )
    .expect("spawn restart thread");
    join.await.expect("restart task");

    assert_eq!(db.pipeline_state(), "Running");
    assert!(db.last_fault().is_none());
    assert_eq!(metrics.pipeline_restarts_total.get(), 1);
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_restart_retries_transient_start_failures_within_budget() {
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let failures_remaining = Arc::new(AtomicUsize::new(0));
    let poll_failure = Arc::new(AtomicBool::new(false));
    let factory_failures = Arc::clone(&failures_remaining);
    let factory_poll_failure = Arc::clone(&poll_failure);
    let db = LaminarDB::builder()
        .storage_dir(checkpoint_dir.path())
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .restart_policy(RestartPolicy {
            max_restarts: 3,
            window: Duration::from_secs(30),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        })
        .register_connector(move |registry| {
            let factory_failures = Arc::clone(&factory_failures);
            let factory_poll_failure = Arc::clone(&factory_poll_failure);
            registry.register_source(
                "transient-start",
                ConnectorInfo {
                    name: "transient-start".into(),
                    display_name: "Transient start test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_| {
                    Ok(Box::new(TransientStartSource {
                        failures_remaining: Arc::clone(&factory_failures),
                        poll_failure: Arc::clone(&factory_poll_failure),
                    }))
                }),
            )
        })
        .build()
        .await
        .unwrap();
    db.execute("CREATE SOURCE trades (id BIGINT NOT NULL) FROM \"transient-start\"")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();
    let checkpoint = db.checkpoint().await.unwrap();
    assert!(checkpoint.success, "{:?}", checkpoint.error);

    failures_remaining.store(2, Ordering::Release);
    poll_failure.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(5), async {
        while db.pipeline_state() != "Faulted" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source poll failure must fault the pipeline");
    let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(
        &prometheus::Registry::new(),
    ));
    let join = spawn_supervised_restart(
        Arc::clone(&db),
        Arc::clone(&db.restart_history),
        Some(Arc::clone(&metrics)),
    )
    .expect("spawn restart thread");
    join.await.expect("restart task");

    assert_eq!(
        db.pipeline_state(),
        "Running",
        "remaining={}, restarts={}, history={}, fault={:?}",
        failures_remaining.load(Ordering::Acquire),
        metrics.pipeline_restarts_total.get(),
        db.restart_history.lock().len(),
        db.last_fault()
    );
    assert_eq!(failures_remaining.load(Ordering::Acquire), 0);
    assert_eq!(metrics.pipeline_restarts_total.get(), 3);
    assert_eq!(db.restart_history.lock().len(), 3);
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_catalog_cleanup_is_terminal_across_stop_and_supervision() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE fenced (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    *db.catalog_cleanup_deregister_fault.lock() = Some("fenced".into());

    let drop_error = db.execute("DROP TABLE fenced").await.unwrap_err();
    assert!(drop_error.to_string().contains("[LDB-6044]"));
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert!(db.ctx.table_exist("fenced").unwrap());
    assert_eq!(
        db.catalog_namespace.lock().get("fenced"),
        Some(&CatalogObjectKind::Table)
    );
    let terminal_reason = db.last_fault().expect("terminal reason");

    db.stop_pipeline().await.unwrap();
    let start_error = db.start().await.unwrap_err();
    assert!(start_error.to_string().contains("[LDB-6044]"));
    assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));

    let create_error = db
        .execute("CREATE TABLE fenced (id BIGINT PRIMARY KEY)")
        .await
        .unwrap_err();
    assert!(create_error.to_string().contains("[LDB-6044]"));
    assert_eq!(
        db.catalog_namespace.lock().get("fenced"),
        Some(&CatalogObjectKind::Table)
    );

    let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(
        &prometheus::Registry::new(),
    ));
    let join = spawn_supervised_restart(
        Arc::clone(&db),
        Arc::clone(&db.restart_history),
        Some(Arc::clone(&metrics)),
    )
    .expect("spawn restart thread");
    join.await.expect("restart task");

    assert!(db.restart_history.lock().is_empty());
    assert_eq!(metrics.pipeline_restarts_total.get(), 0);
    assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));
}
