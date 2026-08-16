use super::*;
use crate::sql_analysis::TemporalBound;

fn lower_strict_ttl(x_ms: i64) -> TemporalFilterConfig {
    // `evt > now() - INTERVAL X` ⇒ lower strict, off = -X.
    TemporalFilterConfig {
        source_table: "events".into(),
        time_col: "evt".into(),
        proj_cols: Vec::new(),
        lower: Some(TemporalBound {
            off_ms: -x_ms,
            strict: true,
        }),
        upper: None,
    }
}

fn batch_evt(schema: &SchemaRef, evts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(evts.to_vec()))],
    )
    .unwrap()
}

fn src_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("evt", DataType::Int64, false)]))
}

fn weights(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let w = b
            .column(b.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        out.extend(w.iter().map(Option::unwrap));
    }
    out
}

fn evts_out(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend(c.iter().map(Option::unwrap));
    }
    out
}

#[test]
fn empty_checkpoint_payload_is_a_recovery_fault() {
    let mut op =
        TemporalFilterOperator::new("tf", "SELECT * FROM events", lower_strict_ttl(10_000), None);
    let env = TemporalFilterCheckpoint {
        fingerprint: 0,
        last_frontier: 0,
        batch_ipc: Vec::new(),
    };
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&env)
        .unwrap()
        .to_vec();

    let error = op.restore(OperatorCheckpoint { data }).unwrap_err();

    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.requires_pipeline_recovery());
    assert!(error.to_string().contains("no buffered-state payload"));
}

#[tokio::test]
async fn projection_buffers_and_emits_only_selected_columns() {
    // SELECT id FROM events WHERE evt > now() - 10s  (drop `evt`).
    let cfg = TemporalFilterConfig {
        source_table: "events".into(),
        time_col: "evt".into(),
        proj_cols: vec!["id".into()],
        lower: Some(TemporalBound {
            off_ms: -10_000,
            strict: true,
        }),
        upper: None,
    };
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("evt", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7_i64])),
            Arc::new(Int64Array::from(vec![50_000_i64])),
        ],
    )
    .unwrap();

    let mut op = TemporalFilterOperator::new("tf", "SELECT id FROM events", cfg.clone(), None);
    // Frontier 55_000 < exit 60_000 ⇒ insert.
    let out = op.process(&[vec![batch]], &[55_000]).await.unwrap();
    assert_eq!(out.len(), 1);
    let sch = out[0].schema();
    let names: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "__weight"], "`evt` projected away");
    assert_eq!(weights(&out), vec![1]);
    assert_eq!(evts_out(&out), vec![7], "projected `id` value");

    // Checkpoint/restore preserves the projected schema, then the row
    // ages out correctly at the lower bound.
    let ck = op.checkpoint().unwrap().expect("state");
    let mut op2 = TemporalFilterOperator::new("tf", "SELECT id FROM events", cfg, None);
    op2.restore(ck).unwrap();
    let out = op2.process(&[vec![]], &[60_000]).await.unwrap();
    assert_eq!(weights(&out), vec![-1]);
    assert_eq!(evts_out(&out), vec![7]);
    assert_eq!(op2.state.buffered_rows(), 0);
}

#[tokio::test]
async fn no_frontier_buffers_emits_nothing() {
    let cfg = lower_strict_ttl(10_000);
    let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
    let s = src_schema();
    let out = op
        .process(&[vec![batch_evt(&s, &[50_000])]], &[i64::MIN])
        .await
        .unwrap();
    assert!(out.is_empty(), "no watermark ⇒ no emission");
    assert_eq!(op.state.buffered_rows(), 1);
}

#[tokio::test]
async fn insert_then_retract_as_frontier_advances() {
    // evt > now() - 10_000ms. Row at evt=50_000 ⇒ exit_key=60_000.
    let cfg = lower_strict_ttl(10_000);
    let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
    let s = src_schema();

    // Frontier at 55_000 (floored 55_000): 55_000 < 60_000 ⇒ member, +1.
    let out = op
        .process(&[vec![batch_evt(&s, &[50_000])]], &[55_000])
        .await
        .unwrap();
    assert_eq!(weights(&out), vec![1]);
    assert_eq!(evts_out(&out), vec![50_000]);

    // Frontier still < 60_000, no new rows ⇒ nothing.
    let out = op.process(&[vec![]], &[59_999]).await.unwrap();
    assert!(out.is_empty());

    // Frontier reaches 60_000 (>= exit_key) ⇒ retract -1.
    let out = op.process(&[vec![]], &[60_000]).await.unwrap();
    assert_eq!(weights(&out), vec![-1]);
    assert_eq!(evts_out(&out), vec![50_000]);

    // Fully GC'd.
    assert_eq!(op.state.buffered_rows(), 0);

    // Further advance ⇒ nothing (no double-retract).
    let out = op.process(&[vec![]], &[120_000]).await.unwrap();
    assert!(out.is_empty());
}

#[tokio::test]
async fn late_row_dropped_no_phantom_retract() {
    let cfg = lower_strict_ttl(10_000);
    let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
    let s = src_schema();
    // Establish a frontier far ahead first.
    let _ = op.process(&[vec![]], &[500_000]).await.unwrap();
    // Row at evt=50_000 ⇒ exit_key=60_000 < frontier 500_000 ⇒ already
    // expired: dropped, NO insert and NO retract.
    let out = op
        .process(&[vec![batch_evt(&s, &[50_000])]], &[500_000])
        .await
        .unwrap();
    assert!(out.is_empty(), "late row must not emit a phantom retract");
    assert_eq!(op.state.buffered_rows(), 0);
}

#[tokio::test]
async fn upper_bound_future_row_enters_late() {
    // ts < now() + 0  (strict upper, off=0) ⇒ enter_key = t+1, never exits.
    let cfg = TemporalFilterConfig {
        source_table: "s".into(),
        time_col: "evt".into(),
        proj_cols: Vec::new(),
        lower: None,
        upper: Some(TemporalBound {
            off_ms: 0,
            strict: true,
        }),
    };
    let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM s", cfg, None);
    let s = src_schema();
    // evt=100_000, enter_key=100_001. Frontier 100_000 < enter ⇒ buffered, no emit.
    let out = op
        .process(&[vec![batch_evt(&s, &[100_000])]], &[100_000])
        .await
        .unwrap();
    assert!(out.is_empty());
    // Frontier advances past enter_key ⇒ +1, and never retracts.
    let out = op.process(&[vec![]], &[101_000]).await.unwrap();
    assert_eq!(weights(&out), vec![1]);
    let out = op.process(&[vec![]], &[10_000_000]).await.unwrap();
    assert!(out.is_empty(), "upper-only bound never retracts");
}

#[tokio::test]
async fn checkpoint_restore_round_trip() {
    let cfg = lower_strict_ttl(10_000);
    let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg.clone(), None);
    let s = src_schema();
    // One member (evt=50_000, exit 60_000) + one pending-future via upper? keep simple:
    let out = op
        .process(&[vec![batch_evt(&s, &[50_000, 58_000])]], &[55_000])
        .await
        .unwrap();
    assert_eq!(weights(&out), vec![1, 1]);

    let ck = op.checkpoint().unwrap().expect("state present");
    let mut op2 = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
    op2.restore(ck).unwrap();
    assert_eq!(op2.state.buffered_rows(), 2);
    assert_eq!(op2.state.last_frontier, 55_000);

    // After restore, advancing past evt=50_000's exit (60_000) retracts
    // exactly that row; 58_000's row (exit 68_000) still lives.
    let out = op2.process(&[vec![]], &[60_000]).await.unwrap();
    assert_eq!(weights(&out), vec![-1]);
    assert_eq!(evts_out(&out), vec![50_000]);
    assert_eq!(op2.state.buffered_rows(), 1);
}
