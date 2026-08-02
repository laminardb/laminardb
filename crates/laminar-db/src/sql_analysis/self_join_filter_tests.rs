use super::*;

#[test]
fn bounded_join_unaliased_projections_are_detected_for_fail_closed_admission() {
    assert!(has_unaliased_projection(
        "SELECT l.* FROM left_stream l JOIN right_stream r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
    ));
    assert!(has_unaliased_projection(
        "SELECT * FROM left_stream l JOIN right_stream r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
    ));
    assert!(!has_unaliased_projection(
        "SELECT l.id AS left_id, r.id AS right_id FROM left_stream l JOIN right_stream r \
         ON l.id = r.id AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
    ));
}

#[test]
fn bounded_join_detection_preserves_kind_and_composite_key_order() {
    for (keyword, expected) in [
        ("JOIN", JoinType::Inner),
        ("LEFT JOIN", JoinType::Left),
        ("RIGHT JOIN", JoinType::Right),
        ("FULL JOIN", JoinType::Full),
        ("LEFT SEMI JOIN", JoinType::LeftSemi),
        ("LEFT ANTI JOIN", JoinType::LeftAnti),
        ("RIGHT SEMI JOIN", JoinType::RightSemi),
        ("RIGHT ANTI JOIN", JoinType::RightAnti),
    ] {
        let sql = format!(
            "SELECT * FROM left_events l {keyword} right_events r \
             ON l.tenant = r.tenant AND l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND"
        );
        let detected = detect_stream_join_query(&sql).expect("bounded join should be detected");
        assert_eq!(detected.config.join_type, expected, "{keyword}");
        assert_eq!(detected.config.left_keys, ["tenant", "id"], "{keyword}");
        assert_eq!(detected.config.right_keys, ["tenant", "id"], "{keyword}");
    }
}

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
    // User predicates were pushed to pre-filters; none stays post-join.
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

    assert!(d.left_pre_filter.is_none());
    assert!(d.right_pre_filter.is_none());
    assert!(
        d.projection_sql.contains("WHERE"),
        "LEFT JOIN must keep right predicate in WHERE: {}",
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
