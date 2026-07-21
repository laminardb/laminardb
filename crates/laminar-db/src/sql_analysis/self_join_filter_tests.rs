use super::*;

#[test]
fn test_basic_self_join_simple_predicates() {
    let d = detect_stream_join_query(
        "SELECT l.key, r.key FROM events l \
         JOIN events r ON l.key = r.key \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
         WHERE l.type = 'A' AND r.type = 'B'",
    )
    .expect("should detect self-join");

    assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
    assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
    // Only the directional filter remains (user's WHERE pushed to
    // pre-filters); no user-derived predicate stays post-join.
    assert!(
        !d.projection_sql.contains("type"),
        "user predicates should be pushed to pre-filters, got: {}",
        d.projection_sql
    );
}

#[test]
fn test_cross_alias_predicate_stays_post_join() {
    let d = detect_stream_join_query(
        "SELECT p.key FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE p.type = 'A' AND a.type = 'B' AND p.cost > a.cost",
    )
    .expect("should detect self-join");

    assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
    assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
    assert!(
        d.projection_sql.contains("WHERE"),
        "cross-alias p.cost > a.cost should stay post-join: {}",
        d.projection_sql
    );
}

#[test]
fn test_unqualified_column_stays_post_join() {
    let d = detect_stream_join_query(
        "SELECT p.key FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE p.type = 'A' AND status = 'active'",
    )
    .expect("should detect self-join");

    assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
    assert!(d.right_pre_filter.is_none());
    assert!(
        d.projection_sql.contains("WHERE"),
        "unqualified 'status' should stay post-join: {}",
        d.projection_sql
    );
}

#[test]
fn test_non_self_join_no_pre_filters() {
    let d = detect_stream_join_query(
        "SELECT o.order_id FROM orders o \
         JOIN payments p ON o.order_id = p.order_id \
         AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
         WHERE o.amount > 100",
    )
    .expect("should detect interval join");

    assert!(d.left_pre_filter.is_none());
    assert!(d.right_pre_filter.is_none());
    assert!(d.projection_sql.contains("WHERE"));
}

#[test]
fn test_left_join_keeps_right_predicate_in_post_where() {
    let d = detect_stream_join_query(
        "SELECT p.key FROM events p \
         LEFT JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE p.type = 'A' AND a.type = 'B'",
    )
    .expect("should detect self-join");

    assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
    assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
    assert!(
        d.projection_sql.contains("WHERE"),
        "LEFT JOIN must keep right predicate in WHERE: {}",
        d.projection_sql
    );
}

#[test]
fn test_residual_self_join_aliases_collisions() {
    // `p.type` and `a.type` both rewrite to step-0 columns and would
    // otherwise alias to `AS type` twice. Collision-aware aliasing
    // must emit `AS p_type` / `AS a_type` so output names stay unique.
    let d = detect_stream_join_query(
        "SELECT p.type, a.type, p.key FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         JOIN dim d ON d.key = p.key",
    )
    .expect("should detect self-join");

    assert!(
        d.projection_sql.contains("AS p_type"),
        "expected `AS p_type`, got: {}",
        d.projection_sql
    );
    assert!(
        d.projection_sql.contains("AS a_type"),
        "expected `AS a_type`, got: {}",
        d.projection_sql
    );
    // Non-colliding `p.key` keeps the natural-name alias.
    assert!(
        d.projection_sql.contains("AS key"),
        "non-colliding `p.key` should still alias to `key`: {}",
        d.projection_sql
    );
}

#[test]
fn test_bounded_later_join_is_not_selected_as_stream_step() {
    let detected = detect_stream_join_query(
        "SELECT p.key FROM events p \
         JOIN dim d ON p.key = d.key \
         JOIN other_events o ON d.key = o.key \
         AND o.ts BETWEEN d.ts AND d.ts + INTERVAL '10' SECOND",
    );

    assert!(
        detected.is_none(),
        "the interval operator can only execute the first join step"
    );
}

#[test]
fn test_self_join_no_where_clause() {
    let d = detect_stream_join_query(
        "SELECT l.key, r.key FROM events l \
         JOIN events r ON l.key = r.key \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND",
    )
    .expect("should detect self-join");

    assert!(d.left_pre_filter.is_none());
    assert!(d.right_pre_filter.is_none());
}

#[test]
fn test_nested_function_predicate() {
    let d = detect_stream_join_query(
        "SELECT p.key FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE jsonb_get_text(from_json(p.attrs), 'name') = 'prompt' \
         AND jsonb_get_text(from_json(a.attrs), 'name') = 'api'",
    )
    .expect("should detect self-join");

    assert!(d.left_pre_filter.is_some());
    assert!(d.right_pre_filter.is_some());
    let left = d.left_pre_filter.unwrap();
    assert!(!left.contains("p."), "alias should be stripped: {left}");
    assert!(left.contains("attrs"), "column name should survive: {left}");
}

#[test]
fn test_cast_predicate_classified_correctly() {
    let d = detect_stream_join_query(
        "SELECT l.key FROM events l \
         JOIN events r ON l.key = r.key \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
         WHERE CAST(l.duration AS DOUBLE) > 1000",
    )
    .expect("should detect self-join");

    assert!(d.left_pre_filter.is_some());
    assert!(d.right_pre_filter.is_none());
    let left = d.left_pre_filter.unwrap();
    assert!(!left.contains("l."), "alias should be stripped: {left}");
}

#[test]
fn test_is_not_null_predicate() {
    let d = detect_stream_join_query(
        "SELECT l.key FROM events l \
         JOIN events r ON l.key = r.key \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
         WHERE l.name IS NOT NULL AND r.name IS NOT NULL",
    )
    .expect("should detect self-join");

    assert!(d.left_pre_filter.is_some());
    assert!(d.right_pre_filter.is_some());
    let left = d.left_pre_filter.unwrap();
    assert!(
        left.contains("IS NOT NULL"),
        "should preserve IS NOT NULL: {left}"
    );
    assert!(!left.contains("l."), "alias should be stripped: {left}");
}

#[test]
fn test_string_literal_containing_alias_not_corrupted() {
    let d = detect_stream_join_query(
        "SELECT p.key FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE a.type = 'p.internal'",
    )
    .expect("should detect self-join");

    assert!(d.left_pre_filter.is_none());
    assert!(d.right_pre_filter.is_some());
    let right = d.right_pre_filter.unwrap();
    assert!(
        right.contains("'p.internal'"),
        "string literal must not be corrupted: {right}"
    );
}
