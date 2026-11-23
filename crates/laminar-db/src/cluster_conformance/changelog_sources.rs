//! A full-changelog singleton source: one node reads the changelog in log order, and each
//! consumer downstream of it equals embedded and batch SQL over the table's final rows.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use super::oracle::OutputForm;
use super::source::{FixtureKind, FixtureSource, QueueCommit, QueueRows};
use super::tests::{case, verdict};
use super::{views, Case, Verdict};

const KEYS: i64 = 23;
const ROWS_PER_BATCH: usize = 16;

/// The table's final rows: each `(k, v)` whose weights sum above zero.
macro_rules! final_rows {
    () => {
        "(SELECT k, v FROM changes GROUP BY k, v HAVING SUM(__weight) > 0)"
    };
}

/// Every key inserted, then some updated, deleted and inserted again, in change-feed order. An
/// update retracts its pre-image before its post-image, and one such pair spans a batch boundary.
/// An update moves its row to another `v % 7` group, whose owner may be another node.
fn change_rows() -> Vec<(i64, i64, i64)> {
    let updated = |key: i64| key % 3 == 0;
    let mut rows: Vec<_> = (0..KEYS).map(|key| (key, 100 + key, 1)).collect();
    for key in (0..KEYS).filter(|&key| updated(key)) {
        rows.extend([(key, 100 + key, -1), (key, 200 + key, 1)]);
    }
    for key in (0..KEYS).filter(|key| key % 4 == 1) {
        let current = if updated(key) { 200 + key } else { 100 + key };
        rows.push((key, current, -1));
    }
    rows.extend(
        (0..KEYS)
            .filter(|key| key % 8 == 1)
            .map(|key| (key, 300 + key, 1)),
    );
    rows
}

fn change_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    let column = |pick: fn(&(i64, i64, i64)) -> i64| {
        Arc::new(Int64Array::from(rows.iter().map(pick).collect::<Vec<_>>()))
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new(
                laminar_core::changelog::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ),
        ])),
        vec![
            column(|row| row.0),
            column(|row| row.1),
            column(|row| row.2),
        ],
    )
    .unwrap()
}

/// A placed full-changelog queue of [`change_rows`].
pub(super) fn changes() -> FixtureSource {
    let rows = change_rows();
    FixtureSource {
        name: "changes",
        columns: "(k BIGINT NOT NULL, v BIGINT NOT NULL, __weight BIGINT NOT NULL)",
        partitions: vec![rows.chunks(ROWS_PER_BATCH).map(change_batch).collect()].into(),
        kind: FixtureKind::Queue {
            commit: QueueCommit::Cumulative,
            rows: QueueRows::FullChangelog,
        },
    }
}

fn changelog_case(name: &'static str, streams: &[&str], ground_truth: &'static str) -> Case {
    Case {
        sources: vec![changes()],
        ..case(name, streams, OutputForm::Changelog, Some(ground_truth))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_aggregate_over_a_changelog_singleton_is_equal() {
    let case = changelog_case(
        "keyed_aggregate_over_a_changelog_singleton",
        &[
            "CREATE STREAM out AS SELECT v % 7 AS g, COUNT(*) AS n, SUM(k) AS s FROM changes \
           GROUP BY v % 7 EMIT CHANGES",
        ],
        concat!(
            "SELECT v % 7 AS g, COUNT(*) AS n, SUM(k) AS s FROM ",
            final_rows!(),
            " GROUP BY v % 7"
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// A group's owner keeps every value, so a retracted extreme falls back to the next one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn min_max_over_a_changelog_singleton_is_equal() {
    let case = changelog_case(
        "min_max_over_a_changelog_singleton",
        &[
            "CREATE STREAM out AS SELECT v % 7 AS g, MIN(k) AS lo, MAX(k) AS hi FROM changes \
           GROUP BY v % 7 EMIT CHANGES",
        ],
        concat!(
            "SELECT v % 7 AS g, MIN(k) AS lo, MAX(k) AS hi FROM ",
            final_rows!(),
            " GROUP BY v % 7"
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// The projection runs where the changelog is read and forwards each row's weight to the
/// aggregate's owners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_over_a_projected_changelog_singleton_is_equal() {
    let case = changelog_case(
        "aggregate_over_a_projected_changelog_singleton",
        &[
            "CREATE STREAM grouped AS SELECT v % 7 AS g, k FROM changes WHERE k % 5 <> 0",
            "CREATE STREAM out AS SELECT g, COUNT(*) AS n, SUM(k) AS s FROM grouped GROUP BY g \
             EMIT CHANGES",
        ],
        concat!(
            "SELECT v % 7 AS g, COUNT(*) AS n, SUM(k) AS s FROM ",
            final_rows!(),
            " WHERE k % 5 <> 0 GROUP BY v % 7"
        ),
    );
    assert_eq!(verdict(case).await, Verdict::Equal);
}

/// Keyed and global views over a full-changelog singleton apply every retraction the one reading
/// node forwards, and every node reads them at the committed cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn views_over_a_changelog_singleton_match_batch_sql_in_cluster() {
    let case = views::ViewCase {
        name: "views_over_a_changelog_singleton",
        sources: vec![changes()],
        views: views::views(&[
            "CREATE MATERIALIZED VIEW by_group AS \
             SELECT v % 7 AS g, COUNT(*) AS n, SUM(k) AS s FROM changes GROUP BY v % 7",
            "CREATE MATERIALIZED VIEW totals AS \
             SELECT COUNT(*) AS n, SUM(v) AS total, MIN(k) AS lo, MAX(k) AS hi FROM changes",
        ]),
        reads: vec![
            (
                "SELECT * FROM by_group",
                concat!(
                    "SELECT v % 7 AS g, COUNT(*) AS n, SUM(k) AS s FROM ",
                    final_rows!(),
                    " GROUP BY v % 7"
                ),
            ),
            (
                "SELECT * FROM totals",
                concat!(
                    "SELECT COUNT(*) AS n, SUM(v) AS total, MIN(k) AS lo, MAX(k) AS hi FROM ",
                    final_rows!()
                ),
            ),
        ],
        same_cycle_invariants: vec![],
    };
    assert_eq!(views::verdict(case).await, Verdict::Equal);
}
