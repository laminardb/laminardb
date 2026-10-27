#![allow(clippy::disallowed_types)]
//! `SUBSCRIBE` to a materialized view delivers the view's rows when the subscription attaches,
//! then each cycle's change to them. Folding everything a subscriber received gives the view's
//! rows at every later point: a changelog view's frames carry `__weight`, an append view's are
//! the rows it kept.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::util::display::array_value_to_string;
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};

const WEIGHT: &str = "__weight";

/// Each view shape a subscription can attach to: keyed and global aggregates, a filter that
/// keeps the key, a projection that drops it, and an append view.
const VIEWS: [(&str, &str); 5] = [
    (
        "agg",
        "CREATE MATERIALIZED VIEW agg AS \
         SELECT k, SUM(v) AS total, COUNT(*) AS n FROM events GROUP BY k",
    ),
    (
        "overall",
        "CREATE MATERIALIZED VIEW overall AS SELECT SUM(v) AS total, COUNT(*) AS n FROM events",
    ),
    (
        "big",
        "CREATE MATERIALIZED VIEW big AS SELECT k, total FROM agg WHERE total > 10",
    ),
    (
        "totals",
        "CREATE MATERIALIZED VIEW totals AS SELECT total FROM agg",
    ),
    (
        "recent",
        "CREATE MATERIALIZED VIEW recent AS SELECT k, v FROM events \
         WITH (retention = '1000 rows')",
    ),
];

fn config(dir: &std::path::Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn events(ks: &[i64], vs: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(vec![
        ("k", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
        ("v", Arc::new(Int64Array::from(vs.to_vec())) as _, true),
    ])
    .unwrap()
}

type Rows = BTreeMap<String, i64>;

/// Add `batch`'s rows to `rows`, each with its `__weight` or once.
fn fold(rows: &mut Rows, batch: &RecordBatch) {
    let weights = batch
        .column_by_name(WEIGHT)
        .map(|column| column.as_any().downcast_ref::<Int64Array>().unwrap());
    let columns: Vec<usize> = (0..batch.num_columns())
        .filter(|&index| batch.schema().field(index).name() != WEIGHT)
        .collect();
    for row in 0..batch.num_rows() {
        let rendered = columns
            .iter()
            .map(|&column| array_value_to_string(batch.column(column), row).unwrap())
            .collect::<Vec<_>>()
            .join("|");
        *rows.entry(rendered).or_default() += weights.map_or(1, |weights| weights.value(row));
    }
    rows.retain(|_, weight| *weight != 0);
}

async fn select(db: &LaminarDB, view: &str) -> Rows {
    let ExecuteResult::Query(mut query) =
        db.execute(&format!("SELECT * FROM {view}")).await.unwrap()
    else {
        panic!("expected a query result");
    };
    let batches = tokio::time::timeout(
        Duration::from_secs(30),
        query.subscribe_raw().unwrap().read_all(),
    )
    .await
    .expect("query produced no result within 30 seconds")
    .unwrap();
    let mut rows = Rows::new();
    for batch in &batches {
        fold(&mut rows, batch);
    }
    rows
}

/// One subscriber's frames, folded.
struct Subscriber {
    view: &'static str,
    portal: SubscriptionPortal,
    rows: Rows,
}

impl Subscriber {
    async fn open(db: &LaminarDB, view: &'static str) -> Self {
        let portal = db
            .open_subscription(view, None, SubscribeStart::Tail)
            .await
            .unwrap();
        Self {
            view,
            portal,
            rows: Rows::new(),
        }
    }

    fn read_available(&mut self) {
        while let Some(frame) = self.portal.try_next_frame() {
            match frame {
                PortalFrame::Batch { batch, .. } => {
                    let weighted = batch.column_by_name(WEIGHT).is_some();
                    assert_eq!(weighted, self.view != "recent", "{}", self.view);
                    assert_eq!(batch.schema(), self.portal.schema(), "{}", self.view);
                    if let Some(weights) = batch.column_by_name(WEIGHT) {
                        let weights = weights.as_any().downcast_ref::<Int64Array>().unwrap();
                        assert!(
                            weights.iter().all(|weight| weight != Some(0)),
                            "{}",
                            self.view
                        );
                    }
                    fold(&mut self.rows, &batch);
                }
                PortalFrame::Barrier { .. } => {}
                other => panic!("{}: unexpected frame {other:?}", self.view),
            }
        }
    }

    /// Read until the folded frames equal the view's rows, which stop changing once the input
    /// is processed.
    async fn converge(&mut self, db: &LaminarDB) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            self.read_available();
            let rows = select(db, self.view).await;
            if self.rows == rows {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{}: subscription folded to {:?}, the view holds {rows:?}",
                self.view,
                self.rows
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn settle(db: &LaminarDB, expected_events: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let overall = select(db, "overall").await;
        if overall
            .keys()
            .any(|row| row.ends_with(&format!("|{expected_events}")))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "events not processed: {overall:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Subscribers attach to running views holding committed and uncommitted rows. Their snapshot
/// equals each view, and after updates that move keys in and out of a filter, retract
/// duplicated projected rows and append more rows, everything they received still folds to
/// each view.
#[tokio::test]
async fn snapshot_then_changes_fold_to_the_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute("CREATE SOURCE events (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    for (_, ddl) in VIEWS {
        db.execute(ddl).await.unwrap();
    }
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();

    source
        .push_arrow(events(&[1, 2, 3, 4], &[5, 20, 5, 7]))
        .unwrap();
    settle(&db, 4).await;
    assert!(db.checkpoint().await.unwrap().success);
    source.push_arrow(events(&[1, 5], &[3, 20])).unwrap();
    settle(&db, 6).await;

    let mut subscribers = Vec::new();
    for (view, _) in VIEWS {
        subscribers.push(Subscriber::open(&db, view).await);
    }
    for subscriber in &mut subscribers {
        subscriber.converge(&db).await;
        assert!(!subscriber.rows.is_empty(), "{}", subscriber.view);
    }

    // Key 1 enters `big`, key 2 leaves it, and keys 3 and 4 project to the same total twice.
    source
        .push_arrow(events(&[1, 2, 3, 4], &[10, -15, 2, 0]))
        .unwrap();
    assert!(db.checkpoint().await.unwrap().success);
    source.push_arrow(events(&[6, 5], &[1, -20])).unwrap();
    settle(&db, 12).await;
    for subscriber in &mut subscribers {
        subscriber.converge(&db).await;
    }
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_local_view_subscription_starts_at_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config(dir.path())).unwrap();
    db.execute("CREATE SOURCE events (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    db.execute(VIEWS[0].1).await.unwrap();
    db.start().await.unwrap();
    for start in [
        SubscribeStart::AsOfEpoch(1),
        SubscribeStart::AfterSequence(0),
    ] {
        let error = db
            .open_subscription("agg", None, start)
            .await
            .expect_err("a view subscription replays nothing");
        assert!(
            error.to_string().contains("subscribe from the tail"),
            "{error}"
        );
    }
    db.shutdown().await.unwrap();
}
