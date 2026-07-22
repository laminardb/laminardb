use super::{backoff_for_attempt, claim_restart_slot, spawn_supervised_restart};
use crate::config::RestartPolicy;
use crate::db::{DbState, LaminarDB};
use laminar_core::catalog::CatalogObjectKind;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
