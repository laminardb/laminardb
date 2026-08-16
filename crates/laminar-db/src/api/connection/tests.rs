use super::*;

#[test]
fn test_connection_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Connection>();
}

#[test]
fn test_connection_open_close() {
    let conn = Connection::open().unwrap();
    assert!(!conn.is_closed());
    conn.close().unwrap();
}

#[test]
fn close_is_terminal_after_start_from_temporary_runtime() {
    let conn = Connection::open().unwrap();
    conn.execute("CREATE SOURCE events (id BIGINT)").unwrap();

    conn.start().unwrap();
    let db = Arc::clone(&conn.inner);
    assert_eq!(
        crate::db::DbState::load(&db.state),
        crate::db::DbState::Running
    );

    let inspection_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    conn.close().unwrap();
    assert_eq!(
        crate::db::DbState::load(&db.state),
        crate::db::DbState::Stopped
    );
    inspection_runtime.block_on(async {
        assert!(db.runtime_handle.lock().await.is_none());
    });
    assert!(db.owned_source_tasks.lock().is_empty());
    assert!(db.owned_sink_handles.lock().is_empty());
    assert!(db.checkpoint_namespace_lock.lock().is_none());
}

#[test]
fn test_connection_thread_safe() {
    let conn = Arc::new(Connection::open().unwrap());

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let conn = Arc::clone(&conn);
            std::thread::spawn(move || {
                let _ = conn.list_sources();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_execute_create_source() {
    let conn = Connection::open().unwrap();
    let result = conn.execute("CREATE SOURCE test_api (id BIGINT, name VARCHAR)");
    assert!(result.is_ok());

    let sources = conn.list_sources();
    assert!(sources.contains(&"test_api".to_string()));
}

#[test]
fn test_get_schema() {
    let conn = Connection::open().unwrap();
    conn.execute("CREATE SOURCE schema_test (id BIGINT, value DOUBLE)")
        .unwrap();

    let schema = conn.get_schema("schema_test").unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
}

#[test]
fn test_get_schema_not_found() {
    let conn = Connection::open().unwrap();
    let result = conn.get_schema("nonexistent");
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().code(),
        super::super::error::codes::TABLE_NOT_FOUND
    );
}

#[test]
fn named_subscription_uses_resolved_output_schema() {
    let conn = Connection::open().unwrap();
    conn.execute("CREATE SOURCE input (id BIGINT, value DOUBLE)")
        .unwrap();
    conn.execute("CREATE STREAM output AS SELECT id, value FROM input")
        .unwrap();

    let Err(unresolved) = conn.subscribe("output") else {
        panic!("unresolved stream schema must not open a subscription");
    };
    assert_eq!(
        unresolved.code(),
        super::super::error::codes::TABLE_NOT_FOUND
    );

    conn.start().unwrap();
    let subscription = conn.subscribe("output").unwrap();
    let schema = subscription.schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "value");
    conn.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_subscribe_fails_closed_inside_runtime() {
    let conn = Connection::open().unwrap();
    let Err(error) = conn.subscribe("anything") else {
        panic!("blocking subscribe must reject an async runtime");
    };
    assert_eq!(
        error.code(),
        super::super::error::codes::SUBSCRIPTION_FAILED
    );
    assert!(error.message().contains("subscribe_async"));
}

#[tokio::test(flavor = "current_thread")]
async fn async_subscribe_opens_on_current_thread_runtime() {
    let conn = Connection::open().unwrap();
    conn.inner
        .execute("CREATE SOURCE async_input (id BIGINT)")
        .await
        .unwrap();
    conn.inner
        .execute("CREATE STREAM async_output AS SELECT id FROM async_input")
        .await
        .unwrap();
    conn.inner.start().await.unwrap();

    let mut subscription = conn.subscribe_async("async_output").await.unwrap();
    assert_eq!(subscription.schema().field(0).name(), "id");
    let error = subscription.next_frame().unwrap_err();
    assert_eq!(
        error.code(),
        super::super::error::codes::SUBSCRIPTION_FAILED
    );
    assert!(error.message().contains("next_frame_async"));
    conn.inner.shutdown().await.unwrap();
}

#[test]
fn test_source_info() {
    let conn = Connection::open().unwrap();
    conn.execute("CREATE SOURCE test_info (id BIGINT, name VARCHAR)")
        .unwrap();
    let info = conn.source_info();
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].name, "test_info");
    assert_eq!(info[0].schema.fields().len(), 2);
}

#[test]
fn test_pipeline_state() {
    let conn = Connection::open().unwrap();
    let state = conn.pipeline_state();
    assert!(!state.is_empty());
}

#[test]
fn test_metrics() {
    let conn = Connection::open().unwrap();
    let m = conn.metrics();
    assert_eq!(m.total_events_ingested, 0);
}

#[test]
fn test_source_count() {
    let conn = Connection::open().unwrap();
    assert_eq!(conn.source_count(), 0);
    conn.execute("CREATE SOURCE cnt_test (x BIGINT)").unwrap();
    assert_eq!(conn.source_count(), 1);
}

#[test]
fn test_cancel_query_invalid() {
    let conn = Connection::open().unwrap();
    let result = conn.cancel_query(999);
    assert!(result.is_err());
}

#[test]
fn test_shutdown() {
    let conn = Connection::open().unwrap();
    assert!(conn.shutdown().is_ok());
}
