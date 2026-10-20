use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::AsArray;

use super::derivation::dependency_order;
use super::ExchangePlanError;
use crate::connector_manager::StreamRegistration;
use crate::handle::ExecuteResult;
use crate::LaminarDB;

const EVENTS: &str = "CREATE SOURCE events (k BIGINT NOT NULL, v BIGINT NOT NULL, \
     ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)";
const ORDERS: &str = "CREATE SOURCE orders (k BIGINT NOT NULL, v BIGINT NOT NULL, \
     ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)";
const PAYMENTS: &str = "CREATE SOURCE payments (k BIGINT NOT NULL, v BIGINT NOT NULL, \
     ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)";

async fn db(ddl: &[&str]) -> Arc<LaminarDB> {
    let db = LaminarDB::open().unwrap();
    for statement in ddl {
        db.execute(statement).await.unwrap();
    }
    db
}

/// The `exchange*` rows of `EXPLAIN <statement>`, in order.
async fn exchange(db: &LaminarDB, statement: &str) -> Vec<(String, String)> {
    let ExecuteResult::Metadata(batch) = db.execute(&format!("EXPLAIN {statement}")).await.unwrap()
    else {
        panic!("EXPLAIN returns metadata");
    };
    let keys = batch.column(0).as_string::<i32>();
    let values = batch.column(1).as_string::<i32>();
    (0..batch.num_rows())
        .filter(|&row| keys.value(row).starts_with("exchange"))
        .map(|row| (keys.value(row).to_string(), values.value(row).to_string()))
        .collect()
}

fn rows(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[tokio::test]
async fn keyed_aggregate_repartitions_and_projections_keep_or_lose_its_key() {
    let db = db(&[
        EVENTS,
        "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k",
    ])
    .await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM probe AS SELECT k, COUNT(*) AS n FROM events GROUP BY k"
        )
        .await,
        rows(&[
            (
                "exchange.input.events",
                "repartition on (k Int64) from arbitrary, append-only"
            ),
            ("exchange.output", "hash(k Int64), running state"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM renamed AS SELECT k AS key, n FROM per_key WHERE n > 1"
        )
        .await,
        rows(&[
            (
                "exchange.input.per_key",
                "forward from hash(k Int64), running state"
            ),
            ("exchange.output", "hash(key Int64), running state"),
        ])
    );
    assert_eq!(
        exchange(&db, "CREATE STREAM dropped AS SELECT n FROM per_key").await,
        rows(&[
            (
                "exchange.input.per_key",
                "forward from hash(k Int64), running state"
            ),
            ("exchange.output", "arbitrary, running state"),
        ])
    );
}

#[tokio::test]
async fn changelog_aggregate_feeds_a_keyed_aggregate_on_a_different_key() {
    let db = db(&[
        EVENTS,
        "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES",
    ])
    .await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM per_key GROUP BY n EMIT CHANGES"
        )
        .await,
        rows(&[
            (
                "exchange.input.per_key",
                "repartition on (n Int64) from hash(k Int64), weighted changelog"
            ),
            ("exchange.output", "hash(n Int64), weighted changelog"),
        ])
    );
}

#[tokio::test]
async fn global_aggregate_gathers_to_the_global_vnode() {
    let db = db(&[EVENTS]).await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM total AS SELECT COUNT(*) AS n FROM events EMIT CHANGES"
        )
        .await,
        rows(&[
            (
                "exchange.input.events",
                "gather partials to vnode 0 from arbitrary, append-only"
            ),
            ("exchange.output", "singleton(vnode 0), weighted changelog"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM windows AS SELECT tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n FROM events GROUP BY tumble(ts, INTERVAL '100' MILLISECOND) \
             EMIT ON WINDOW CLOSE"
        )
        .await,
        rows(&[
            (
                "exchange.input.events",
                "gather to vnode 0 from arbitrary, append-only"
            ),
            ("exchange.output", "singleton(vnode 0), append-only"),
        ])
    );
}

#[tokio::test]
async fn computed_group_key_and_window_markers() {
    let db = db(&[EVENTS]).await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM grouped AS SELECT k % 4 AS g, SUM(v) AS s FROM events GROUP BY k % 4"
        )
        .await,
        rows(&[
            (
                "exchange.input.events",
                "repartition on ((events.k % Int64(4)) Int64) from arbitrary, append-only"
            ),
            ("exchange.output", "hash(g Int64), running state"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM windows AS SELECT k, tumble(ts, INTERVAL '100' MILLISECOND) AS w, \
             COUNT(*) AS n FROM events GROUP BY k, tumble(ts, INTERVAL '100' MILLISECOND) \
             EMIT ON WINDOW CLOSE"
        )
        .await,
        rows(&[
            (
                "exchange.input.events",
                "repartition on (k Int64) from arbitrary, append-only"
            ),
            ("exchange.output", "hash(k Int64), append-only"),
        ])
    );
}

#[tokio::test]
async fn interval_join_repartitions_each_side_and_union_forwards() {
    let db = db(&[ORDERS, PAYMENTS]).await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM joined AS SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
             INNER JOIN payments p ON o.k = p.k \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
        )
        .await,
        rows(&[
            (
                "exchange.input.left:orders",
                "repartition on (k Int64) from arbitrary, append-only"
            ),
            (
                "exchange.input.right:payments",
                "repartition on (k Int64) from arbitrary, append-only"
            ),
            ("exchange.output", "hash(k Int64), append-only"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM merged AS SELECT k, v FROM orders UNION ALL SELECT k, v FROM payments"
        )
        .await,
        rows(&[
            (
                "exchange.input.orders",
                "forward from arbitrary, append-only"
            ),
            (
                "exchange.input.payments",
                "forward from arbitrary, append-only"
            ),
            ("exchange.output", "arbitrary, append-only"),
        ])
    );
}

const JOIN_BOUND: &str = "ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND";

#[tokio::test]
async fn interval_join_output_is_hashed_on_the_side_every_row_carries() {
    let db = db(&[ORDERS, PAYMENTS]).await;
    for (join, projection, output) in [
        (
            "INNER",
            "p.k AS pk, o.v AS ov",
            "hash(pk Int64), append-only",
        ),
        (
            "LEFT",
            "o.k AS ok, p.k AS pk",
            "hash(ok Int64), append-only",
        ),
        (
            "RIGHT",
            "o.k AS ok, p.k AS pk",
            "hash(pk Int64), append-only",
        ),
        ("FULL", "o.k AS ok, p.k AS pk", "arbitrary, append-only"),
        (
            "INNER",
            "o.k + 1 AS k1, o.v AS ov",
            "arbitrary, append-only",
        ),
    ] {
        let rows = exchange(
            &db,
            &format!(
                "CREATE STREAM joined AS SELECT {projection} FROM orders o \
                 {join} JOIN payments p {JOIN_BOUND}"
            ),
        )
        .await;
        assert_eq!(
            rows.last(),
            Some(&("exchange.output".to_string(), output.to_string())),
            "{join} JOIN {projection}"
        );
    }
}

#[tokio::test]
async fn aggregate_on_its_input_hash_key_is_colocated() {
    let db = db(&[
        ORDERS,
        PAYMENTS,
        &format!(
            "CREATE STREAM joined AS SELECT o.k AS k, o.v AS ov, p.v AS pv FROM orders o \
             INNER JOIN payments p {JOIN_BOUND}"
        ),
        "CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM orders GROUP BY k EMIT CHANGES",
    ])
    .await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM per_join_key AS SELECT k, SUM(pv) AS total FROM joined GROUP BY k \
             EMIT CHANGES"
        )
        .await,
        rows(&[
            (
                "exchange.input.joined",
                "colocated on (k Int64) from hash(k Int64), append-only"
            ),
            ("exchange.output", "hash(k Int64), weighted changelog"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM chained AS SELECT k, SUM(n) AS total FROM per_key GROUP BY k \
             EMIT CHANGES"
        )
        .await,
        rows(&[
            (
                "exchange.input.per_key",
                "colocated on (k Int64) from hash(k Int64), weighted changelog"
            ),
            ("exchange.output", "hash(k Int64), weighted changelog"),
        ])
    );
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM by_value AS SELECT ov, COUNT(*) AS n FROM joined GROUP BY ov \
             EMIT CHANGES"
        )
        .await[0],
        (
            "exchange.input.joined".to_string(),
            "repartition on (ov Int64) from hash(k Int64), append-only".to_string()
        )
    );
}

#[tokio::test]
async fn lookup_join_forwards_the_stream_and_gives_the_table_no_port() {
    let db = db(&[
        ORDERS,
        "CREATE LOOKUP TABLE rates (k BIGINT NOT NULL, r DOUBLE NOT NULL, PRIMARY KEY (k)) \
         WITH ('connector' = 'static')",
    ])
    .await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM enriched AS SELECT o.k, o.v, t.r FROM orders o JOIN rates t ON o.k = t.k"
        )
        .await,
        rows(&[
            (
                "exchange.input.orders",
                "forward from arbitrary, append-only"
            ),
            ("exchange.output", "arbitrary, append-only"),
        ])
    );
}

#[tokio::test]
async fn sql_reference_example() {
    let db = db(&[
        "CREATE SOURCE trades (symbol VARCHAR NOT NULL, price DOUBLE NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
    ])
    .await;
    assert_eq!(
        exchange(
            &db,
            "CREATE STREAM per_symbol AS \
             SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol EMIT CHANGES"
        )
        .await,
        rows(&[
            (
                "exchange.input.trades",
                "repartition on (symbol Utf8) from arbitrary, append-only"
            ),
            ("exchange.output", "hash(symbol Utf8), weighted changelog"),
        ])
    );
}

#[tokio::test]
async fn explain_without_create_stream_has_no_exchange_rows() {
    let db = db(&[EVENTS]).await;
    assert!(exchange(&db, "SELECT k FROM events").await.is_empty());
}

fn registration(name: &str, query_sql: &str) -> StreamRegistration {
    StreamRegistration {
        name: name.into(),
        query_sql: query_sql.into(),
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config: None,
        has_analytic: false,
        has_frame: false,
        incremental: false,
        subscription_output: None,
        subscription_retention_bytes: 0,
        catalog_generation: 1,
        subscription_certificate: None,
    }
}

#[test]
fn streams_that_read_each_other_are_a_cycle() {
    let streams: HashMap<String, StreamRegistration> = [
        registration("a", "SELECT k FROM b"),
        registration("b", "SELECT k FROM a"),
        registration("c", "SELECT k FROM events"),
        registration("d", "SELECT k FROM a"),
    ]
    .into_iter()
    .map(|registration| (registration.name.clone(), registration))
    .collect();
    let Err(ExchangePlanError::Cycle(streams)) = dependency_order(&streams) else {
        panic!("a cycle has no dependency order");
    };
    assert_eq!(streams, ["a", "b", "d"]);
}

#[test]
fn dependency_order_puts_upstreams_first() {
    let streams: HashMap<String, StreamRegistration> = [
        registration("z", "SELECT k FROM events"),
        registration("a", "SELECT k FROM z"),
        registration("m", "SELECT k FROM a UNION ALL SELECT k FROM z"),
    ]
    .into_iter()
    .map(|registration| (registration.name.clone(), registration))
    .collect();
    let order: Vec<&str> = dependency_order(&streams)
        .unwrap()
        .into_iter()
        .map(|registration| registration.name.as_str())
        .collect();
    assert_eq!(order, ["z", "a", "m"]);
}
