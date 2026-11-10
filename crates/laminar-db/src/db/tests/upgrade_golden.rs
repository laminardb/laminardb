//! The durable bindings a dependency upgrade can move with no compile error and no behaviour
//! change: a moved value makes checkpoints written before the upgrade unrestorable.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;
use crate::pipeline_identity::{canonical_value, PipelineIdentityContext, PipelineRegistrations};

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/upgrade/pipeline_identity.json"
);
const BLESS: &str = "LAMINAR_BLESS_UPGRADE_GOLDEN";

const CORPUS: &[&str] = &[
    "CREATE SOURCE events (k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    "CREATE SOURCE orders (k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    "CREATE SOURCE payments (k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
     WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    "CREATE STREAM filtered AS SELECT k, v, ts FROM events WHERE v > 0",
    "CREATE STREAM keyed AS SELECT k, SUM(v) AS total, COUNT(*) AS n, MIN(v) AS lo, \
     MAX(v) AS hi, AVG(v) AS mean FROM events GROUP BY k",
    "CREATE STREAM changes AS SELECT k, SUM(v) AS total FROM events GROUP BY k EMIT CHANGES",
    "CREATE STREAM global_count AS SELECT COUNT(*) AS n FROM events",
    "CREATE STREAM tumbling AS SELECT k, TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
     COUNT(*) AS n FROM events GROUP BY k, TUMBLE(ts, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
    "CREATE STREAM hopping AS SELECT k, \
     HOP(ts, INTERVAL '50' MILLISECOND, INTERVAL '100' MILLISECOND) AS bucket, COUNT(*) AS n \
     FROM events GROUP BY k, HOP(ts, INTERVAL '50' MILLISECOND, INTERVAL '100' MILLISECOND) \
     EMIT ON WINDOW CLOSE",
    "CREATE STREAM sessions AS SELECT k, SESSION(ts, INTERVAL '5' MILLISECOND) AS s, \
     COUNT(*) AS n FROM events GROUP BY k, SESSION(ts, INTERVAL '5' MILLISECOND) \
     EMIT ON WINDOW CLOSE",
    "CREATE STREAM periodic AS SELECT k, TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
     COUNT(*) AS n FROM events GROUP BY k, TUMBLE(ts, INTERVAL '1' MINUTE) \
     EMIT PERIODICALLY INTERVAL '5' SECOND",
    "CREATE STREAM joined AS SELECT o.k AS k, o.v AS ordered, p.v AS paid, o.ts AS ts FROM orders o \
     JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND",
    "CREATE STREAM merged AS SELECT k, v, ts FROM orders UNION ALL SELECT k, v, ts FROM payments",
    "CREATE STREAM split_join AS SELECT o.k AS k, SUM(p.v) AS total FROM orders o \
     JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
     GROUP BY o.k",
    "CREATE STREAM over_changes AS SELECT total, COUNT(*) AS keys FROM changes GROUP BY total",
];

async fn actual() -> Value {
    let db = LaminarDB::open().unwrap();
    for statement in CORPUS {
        db.execute(statement).await.unwrap();
    }
    let outputs = db.cluster_stream_outputs().await.unwrap();
    let manager = db.connector_manager.lock();
    let pipeline = canonical_value(&PipelineIdentityContext::new(
        &db.config,
        &db.catalog,
        &db.connector_registry,
        PipelineRegistrations::new(
            manager.sources().values(),
            manager.sinks().values(),
            manager.streams().values(),
            manager.tables().values(),
        ),
        &outputs.exchange_plan,
        db.checkpoint_key_groups().get(),
    ))
    .unwrap();
    let schemas: BTreeMap<_, Vec<_>> = outputs
        .schemas
        .iter()
        .map(|(name, schema)| {
            let fields = schema.fields().iter().map(|field| {
                let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
                format!("{}: {}{nullability}", field.name(), field.data_type())
            });
            (name.clone(), fields.collect())
        })
        .collect();
    json!({ "pipeline": pipeline, "stream_output_schemas": schemas })
}

#[tokio::test]
async fn pipeline_identity_and_output_schemas_match_the_recorded_baseline() {
    let actual = actual().await;
    let rendered = format!("{}\n", serde_json::to_string_pretty(&actual).unwrap());
    if std::env::var_os(BLESS).is_some() {
        std::fs::write(GOLDEN, rendered).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(GOLDEN).unwrap();
    assert_eq!(
        serde_json::to_string_pretty(&serde_json::from_str::<Value>(&expected).unwrap()).unwrap(),
        rendered.trim_end(),
        "a checkpoint written before this change would be refused; re-bless with {BLESS}=1 only \
         for an intended identity or output-schema change"
    );
}
