use super::plan_frame_query;

#[test]
fn detects_corr_frame_and_rewrites_to_alias() {
    let plan = plan_frame_query(
        "SELECT bucket_start, close, mean_sentiment, \
         CORR(close, mean_sentiment) OVER (ORDER BY bucket_start ROWS 30 PRECEDING) AS corr_30 \
         FROM sentiment_price_join",
    )
    .expect("frame plan");
    assert_eq!(plan.x_column, "close");
    assert_eq!(plan.y_column, "mean_sentiment");
    assert_eq!(plan.output_alias, "corr_30");
    assert_eq!(plan.retain, 30);
    assert_eq!(plan.source_table, "sentiment_price_join");
    // The CORR term is gone; the residual reads the alias from the temp table.
    assert!(!plan.projection_sql.to_uppercase().contains("CORR("));
    assert!(plan.projection_sql.contains("corr_30"));
    assert!(plan.projection_sql.contains("__frame_tmp"));
}

#[test]
fn rejects_partition_by_and_non_frame_queries() {
    // PARTITION BY is not supported.
    assert!(plan_frame_query(
        "SELECT CORR(a, b) OVER (PARTITION BY g ORDER BY t ROWS 5 PRECEDING) AS c FROM s"
    )
    .is_none());
    // No window frame → not a frame query.
    assert!(plan_frame_query("SELECT a, b FROM s").is_none());
}
