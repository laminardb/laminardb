use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorInfo;
use laminar_connectors::error::ConnectorError;
use laminar_connectors::reference::ReferenceTableSource;
use laminar_connectors::registry::ConnectorRegistry;

use super::{
    create_reference_table_sources, hydrate_reference_table_sources, ReferenceTableRuntimeSource,
};
use crate::connector_manager::TableRegistration;
use crate::table_store::TableStore;

struct CountingSnapshotSource {
    polls: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    batches: VecDeque<RecordBatch>,
    fail_poll: bool,
    fail_close: bool,
}

#[async_trait::async_trait]
impl ReferenceTableSource for CountingSnapshotSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.fail_poll {
            self.fail_poll = false;
            return Err(ConnectorError::ReadError(
                "injected snapshot failure".into(),
            ));
        }
        Ok(self.batches.pop_front())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        if self.fail_close {
            Err(ConnectorError::ReadError("injected close failure".into()))
        } else {
            Ok(())
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn batch(id: i32, value: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap()
}

fn runtime_source(
    name: &str,
    polls: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    batches: Vec<RecordBatch>,
    fail_poll: bool,
    fail_close: bool,
) -> ReferenceTableRuntimeSource {
    (
        name.into(),
        Box::new(CountingSnapshotSource {
            polls,
            closes,
            batches: batches.into(),
            fail_poll,
            fail_close,
        }),
    )
}

fn registration(name: &str) -> TableRegistration {
    TableRegistration {
        name: name.into(),
        primary_key: "id".into(),
        connector_type: Some("mock".into()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    }
}

#[tokio::test]
async fn complete_table_restore_skips_source_construction() {
    let mut table_store = TableStore::new();
    table_store.create_table("t", schema(), "id").unwrap();
    table_store.upsert("t", &batch(1, "checkpoint")).unwrap();
    table_store.set_ready("t", true);
    let table_store = parking_lot::RwLock::new(table_store);

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&factory_calls);
    let registry = ConnectorRegistry::new();
    registry
        .register_table_source(
            "mock",
            ConnectorInfo {
                name: "mock".into(),
                display_name: "Mock".into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(CountingSnapshotSource {
                    polls: Arc::new(AtomicUsize::new(0)),
                    closes: Arc::new(AtomicUsize::new(0)),
                    batches: VecDeque::new(),
                    fail_poll: false,
                    fail_close: false,
                }))
            }),
        )
        .unwrap();
    let registrations = HashMap::from([("t".into(), registration("t"))]);
    let sources = create_reference_table_sources(&registry, &registrations, &table_store, true)
        .await
        .unwrap();

    assert!(sources.is_empty());
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    let restored = table_store.read().to_record_batch("t").unwrap().unwrap();
    assert_eq!(restored.num_rows(), 1);
    assert_eq!(
        restored
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[tokio::test]
async fn later_source_construction_failure_closes_prior_sources() {
    let mut table_store = TableStore::new();
    table_store.create_table("a", schema(), "id").unwrap();
    table_store.create_table("b", schema(), "id").unwrap();
    let table_store = parking_lot::RwLock::new(table_store);

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&factory_calls);
    let closes = Arc::new(AtomicUsize::new(0));
    let source_closes = Arc::clone(&closes);
    let registry = ConnectorRegistry::new();
    registry
        .register_table_source(
            "mock",
            ConnectorInfo {
                name: "mock".into(),
                display_name: "Mock".into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_, _| {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "injected factory failure".into(),
                    ));
                }
                Ok(Box::new(CountingSnapshotSource {
                    polls: Arc::new(AtomicUsize::new(0)),
                    closes: Arc::clone(&source_closes),
                    batches: VecDeque::new(),
                    fail_poll: false,
                    fail_close: false,
                }))
            }),
        )
        .unwrap();
    let registrations = HashMap::from([
        ("b".into(), registration("b")),
        ("a".into(), registration("a")),
    ]);

    let result =
        create_reference_table_sources(&registry, &registrations, &table_store, false).await;
    let error = match result {
        Ok(_) => panic!("the second table-source factory must fail"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("injected factory failure"));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fresh_start_exhausts_upstream_snapshot_and_marks_table_ready() {
    let mut table_store = TableStore::new();
    table_store.create_table("t", schema(), "id").unwrap();
    let table_store = parking_lot::RwLock::new(table_store);

    let polls = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let sources = vec![runtime_source(
        "t",
        Arc::clone(&polls),
        Arc::clone(&closes),
        vec![batch(2, "upstream")],
        false,
        false,
    )];
    let names = hydrate_reference_table_sources(sources, &table_store)
        .await
        .unwrap();

    assert_eq!(names, ["t"]);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert!(table_store.read().is_ready("t"));
    assert_eq!(table_store.read().table_row_count("t"), 1);
}

#[tokio::test]
async fn snapshot_poll_failure_closes_every_source_without_mutation() {
    let mut table_store = TableStore::new();
    table_store.create_table("a", schema(), "id").unwrap();
    table_store.create_table("b", schema(), "id").unwrap();
    let table_store = parking_lot::RwLock::new(table_store);

    let a_polls = Arc::new(AtomicUsize::new(0));
    let b_polls = Arc::new(AtomicUsize::new(0));
    let a_closes = Arc::new(AtomicUsize::new(0));
    let b_closes = Arc::new(AtomicUsize::new(0));
    let sources = vec![
        runtime_source(
            "a",
            Arc::clone(&a_polls),
            Arc::clone(&a_closes),
            Vec::new(),
            true,
            false,
        ),
        runtime_source(
            "b",
            Arc::clone(&b_polls),
            Arc::clone(&b_closes),
            vec![batch(2, "upstream")],
            false,
            false,
        ),
    ];

    let error = hydrate_reference_table_sources(sources, &table_store)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected snapshot failure"));
    assert_eq!(a_polls.load(Ordering::SeqCst), 1);
    assert_eq!(b_polls.load(Ordering::SeqCst), 0);
    assert_eq!(a_closes.load(Ordering::SeqCst), 1);
    assert_eq!(b_closes.load(Ordering::SeqCst), 1);
    assert_eq!(table_store.read().table_row_count("a"), 0);
    assert_eq!(table_store.read().table_row_count("b"), 0);
    assert!(!table_store.read().is_ready("a"));
    assert!(!table_store.read().is_ready("b"));
}

#[tokio::test]
async fn snapshot_close_failure_prevents_install() {
    let mut table_store = TableStore::new();
    table_store.create_table("t", schema(), "id").unwrap();
    let table_store = parking_lot::RwLock::new(table_store);

    let polls = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let sources = vec![runtime_source(
        "t",
        Arc::clone(&polls),
        Arc::clone(&closes),
        vec![batch(2, "upstream")],
        false,
        true,
    )];

    let error = hydrate_reference_table_sources(sources, &table_store)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected close failure"));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
    assert_eq!(table_store.read().table_row_count("t"), 0);
    assert!(!table_store.read().is_ready("t"));
}
