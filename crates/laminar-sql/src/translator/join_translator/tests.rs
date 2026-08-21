use super::*;

#[test]
fn test_stream_join_config() {
    let config = StreamJoinConfig::new(
        JoinType::RightAnti,
        vec!["tenant_id".to_string(), "order_id".to_string()],
        vec!["account_id".to_string(), "payment_order_id".to_string()],
        Duration::from_secs(3600),
    );

    assert_eq!(config.join_type, JoinType::RightAnti);
    assert_eq!(config.left_keys, ["tenant_id", "order_id"]);
    assert_eq!(config.right_keys, ["account_id", "payment_order_id"]);
    assert_eq!(config.time_bound, Duration::from_secs(3600));
}

#[test]
fn test_lookup_join_config() {
    let config = LookupJoinConfig::inner("customer_id".to_string(), "id".to_string())
        .with_cache_ttl(Duration::from_secs(600));

    assert_eq!(config.stream_key, "customer_id");
    assert_eq!(config.lookup_key, "id");
    assert_eq!(config.cache_ttl, Duration::from_secs(600));
    assert_eq!(config.join_type, LookupJoinType::Inner);
}

#[test]
fn test_from_analysis_lookup() {
    let analysis = JoinAnalysis::lookup(
        "orders".to_string(),
        "customers".to_string(),
        "customer_id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    );

    let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();

    assert!(config.is_lookup());
    assert!(!config.is_stream_stream());
    assert_eq!(config.left_keys(), ["customer_id"]);
    assert_eq!(config.right_keys(), ["id"]);
}

#[test]
fn test_from_analysis_stream_stream() {
    let mut analysis = JoinAnalysis::stream_stream(
        "orders".to_string(),
        "payments".to_string(),
        "tenant_id".to_string(),
        "account_id".to_string(),
        Duration::from_secs(3600),
        JoinType::Full,
    );
    analysis
        .additional_key_columns
        .push(("order_id".to_string(), "payment_order_id".to_string()));

    let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();

    assert!(config.is_stream_stream());
    assert!(!config.is_lookup());

    if let JoinOperatorConfig::StreamStream(stream_config) = config {
        assert_eq!(stream_config.join_type, JoinType::Full);
        assert_eq!(stream_config.left_keys, ["tenant_id", "order_id"]);
        assert_eq!(stream_config.right_keys, ["account_id", "payment_order_id"]);
        assert_eq!(stream_config.time_bound, Duration::from_secs(3600));
    }
}
#[test]
fn test_from_multi_analysis_single() {
    let analysis = JoinAnalysis::lookup(
        "a".to_string(),
        "b".to_string(),
        "id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    );
    let multi = MultiJoinAnalysis {
        joins: vec![analysis],
        tables: vec!["a".to_string(), "b".to_string()],
    };

    let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
    assert_eq!(configs.len(), 1);
    assert!(configs[0].is_lookup());
}

#[test]
fn test_from_multi_analysis_two_lookups() {
    let j1 = JoinAnalysis::lookup(
        "a".to_string(),
        "b".to_string(),
        "id".to_string(),
        "a_id".to_string(),
        JoinType::Inner,
    );
    let j2 = JoinAnalysis::lookup(
        "b".to_string(),
        "c".to_string(),
        "id".to_string(),
        "b_id".to_string(),
        JoinType::Inner,
    );
    let multi = MultiJoinAnalysis {
        joins: vec![j1, j2],
        tables: vec!["a".to_string(), "b".to_string(), "c".to_string()],
    };

    let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
    assert_eq!(configs.len(), 2);
    assert!(configs[0].is_lookup());
    assert!(configs[1].is_lookup());
    assert_eq!(configs[0].left_keys(), ["id"]);
    assert_eq!(configs[1].left_keys(), ["id"]);
}
#[test]
fn test_from_multi_analysis_stream_stream_and_lookup() {
    let j1 = JoinAnalysis::stream_stream(
        "orders".to_string(),
        "payments".to_string(),
        "id".to_string(),
        "order_id".to_string(),
        Duration::from_secs(3600),
        JoinType::Inner,
    );
    let j2 = JoinAnalysis::lookup(
        "payments".to_string(),
        "customers".to_string(),
        "cust_id".to_string(),
        "id".to_string(),
        JoinType::Left,
    );
    let multi = MultiJoinAnalysis {
        joins: vec![j1, j2],
        tables: vec![
            "orders".to_string(),
            "payments".to_string(),
            "customers".to_string(),
        ],
    };

    let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
    assert_eq!(configs.len(), 2);
    assert!(configs[0].is_stream_stream());
    assert!(configs[1].is_lookup());
}

#[test]
fn test_from_multi_analysis_order_preserved() {
    let j1 = JoinAnalysis::lookup(
        "a".to_string(),
        "b".to_string(),
        "k1".to_string(),
        "k1".to_string(),
        JoinType::Inner,
    );
    let j2 = JoinAnalysis::stream_stream(
        "b".to_string(),
        "c".to_string(),
        "k2".to_string(),
        "k2".to_string(),
        Duration::from_secs(60),
        JoinType::Inner,
    );
    let j3 = JoinAnalysis::lookup(
        "c".to_string(),
        "d".to_string(),
        "k3".to_string(),
        "k3".to_string(),
        JoinType::Inner,
    );
    let multi = MultiJoinAnalysis {
        joins: vec![j1, j2, j3],
        tables: vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ],
    };

    let configs = JoinOperatorConfig::from_multi_analysis(&multi).unwrap();
    assert_eq!(configs.len(), 3);
    assert!(configs[0].is_lookup());
    assert!(configs[1].is_stream_stream());
    assert!(configs[2].is_lookup());
    assert_eq!(configs[0].left_keys(), ["k1"]);
    assert_eq!(configs[1].left_keys(), ["k2"]);
    assert_eq!(configs[2].left_keys(), ["k3"]);
}

#[test]
fn test_display_stream_join() {
    let mut config = StreamJoinConfig::new(
        JoinType::LeftSemi,
        vec!["tenant_id".to_string(), "order_id".to_string()],
        vec!["account_id".to_string(), "payment_order_id".to_string()],
        Duration::from_secs(3600),
    );
    config.left_table = "orders".to_string();
    config.right_table = "payments".to_string();
    config.left_time_column = "ts".to_string();
    config.right_time_column = "ts".to_string();
    assert_eq!(
            format!("{config}"),
            "LEFT SEMI JOIN ON orders.tenant_id = payments.account_id AND orders.order_id = payments.payment_order_id (bound: 3600s, time: ts ~ ts)"
        );
}

#[test]
fn test_display_lookup_join() {
    let config = LookupJoinConfig::left("cust_id".to_string(), "id".to_string());
    assert_eq!(
        format!("{config}"),
        "LEFT LOOKUP JOIN ON stream.cust_id = lookup.id (cache_ttl: 300s)"
    );
}
#[test]
fn test_display_join_types() {
    assert_eq!(format!("{}", LookupJoinType::Inner), "INNER");
    assert_eq!(format!("{}", LookupJoinType::Left), "LEFT");
    for (join_type, sql) in [
        (JoinType::Inner, "INNER"),
        (JoinType::Left, "LEFT"),
        (JoinType::Right, "RIGHT"),
        (JoinType::Full, "FULL"),
        (JoinType::LeftSemi, "LEFT SEMI"),
        (JoinType::LeftAnti, "LEFT ANTI"),
        (JoinType::RightSemi, "RIGHT SEMI"),
        (JoinType::RightAnti, "RIGHT ANTI"),
    ] {
        assert_eq!(join_type.to_string(), sql);
    }
}

#[test]
fn test_from_analysis_temporal() {
    let mut analysis = JoinAnalysis::temporal(
        "orders".to_string(),
        "products".to_string(),
        "product_id".to_string(),
        "id".to_string(),
        "order_time".to_string(),
        JoinType::Inner,
    );
    analysis.right_time_column = Some("version_time".into());
    analysis
        .additional_key_columns
        .push(("venue".into(), "market".into()));

    let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
    assert!(config.is_temporal());
    assert!(!config.is_lookup());
    assert!(!config.is_stream_stream());
    assert_eq!(config.left_keys(), ["product_id", "venue"]);
    assert_eq!(config.right_keys(), ["id", "market"]);

    if let JoinOperatorConfig::Temporal(tc) = config {
        assert_eq!(tc.left_key_columns, ["product_id", "venue"]);
        assert_eq!(tc.right_key_columns, ["id", "market"]);
        assert_eq!(tc.left_time_column, "order_time");
        assert_eq!(tc.right_time_column, "version_time");
        assert_eq!(tc.join_kind, TemporalJoinKind::Inner);
        assert_eq!(tc.probe_schedule.offsets_ms(), [0]);
    } else {
        panic!("Expected Temporal config");
    }
}

#[test]
fn test_temporal_left_join() {
    let mut analysis = JoinAnalysis::temporal(
        "orders".to_string(),
        "products".to_string(),
        "product_id".to_string(),
        "id".to_string(),
        "order_time".to_string(),
        JoinType::Left,
    );
    analysis.right_time_column = Some("version_time".into());

    let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
    if let JoinOperatorConfig::Temporal(tc) = config {
        assert_eq!(tc.join_kind, TemporalJoinKind::Left);
    } else {
        panic!("Expected Temporal config");
    }
}

#[test]
fn temporal_join_requires_non_empty_keys() {
    let mut analysis = JoinAnalysis::temporal(
        "orders".into(),
        "products".into(),
        String::new(),
        "id".into(),
        "order_time".into(),
        JoinType::Inner,
    );
    analysis.right_time_column = Some("version_time".into());

    let error = JoinOperatorConfig::from_analysis(&analysis).unwrap_err();
    assert!(error.contains("non-empty"), "{error}");
}

#[test]
fn test_display_temporal_join() {
    let mut analysis = JoinAnalysis::temporal(
        "orders".to_string(),
        "products".to_string(),
        "product_id".to_string(),
        "id".to_string(),
        "order_time".to_string(),
        JoinType::Inner,
    );
    analysis.right_time_column = Some("version_time".into());
    let config = JoinOperatorConfig::from_analysis(&analysis).unwrap();
    let s = format!("{config}");
    assert!(s.contains("TEMPORAL JOIN"), "got: {s}");
    assert!(s.contains("order_time"), "got: {s}");
}

#[test]
fn stream_join_requires_explicit_time_bound() {
    let mut analysis = JoinAnalysis::stream_stream(
        "orders".to_string(),
        "payments".to_string(),
        "order_id".to_string(),
        "order_id".to_string(),
        Duration::from_secs(1),
        JoinType::Inner,
    );
    analysis.time_bound = None;

    let error = JoinOperatorConfig::from_analysis(&analysis).unwrap_err();
    assert!(error.contains("explicit finite time bound"));

    analysis.time_bound = Some(Duration::ZERO);
    let error = JoinOperatorConfig::from_analysis(&analysis).unwrap_err();
    assert!(error.contains("positive finite time bound"));
}

#[test]
fn unsupported_join_analysis_fails_closed() {
    let unsupported_lookup_types = [
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ];
    for join_type in unsupported_lookup_types {
        let lookup = JoinAnalysis::lookup(
            "orders".to_string(),
            "customers".to_string(),
            "customer_id".to_string(),
            "id".to_string(),
            join_type,
        );
        assert!(JoinOperatorConfig::from_analysis(&lookup)
            .unwrap_err()
            .contains("only INNER or LEFT"));

        let mut temporal = JoinAnalysis::temporal(
            "orders".to_string(),
            "customers".to_string(),
            "customer_id".to_string(),
            "id".to_string(),
            "order_time".to_string(),
            join_type,
        );
        temporal.right_time_column = Some("version_time".into());
        assert!(JoinOperatorConfig::from_analysis(&temporal)
            .unwrap_err()
            .contains("only INNER or LEFT"));
    }

    let mut composite = JoinAnalysis::lookup(
        "orders".to_string(),
        "customers".to_string(),
        "customer_id".to_string(),
        "id".to_string(),
        JoinType::Inner,
    );
    composite
        .additional_key_columns
        .push(("tenant_id".to_string(), "tenant_id".to_string()));
    assert!(JoinOperatorConfig::from_analysis(&composite)
        .unwrap_err()
        .contains("exactly one equality key"));
}
