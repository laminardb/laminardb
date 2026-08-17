use super::*;

#[test]
fn test_scalar_value_display() {
    assert_eq!(ScalarValue::Null.to_string(), "NULL");
    assert_eq!(ScalarValue::Bool(true).to_string(), "true");
    assert_eq!(ScalarValue::Int64(42).to_string(), "42");
    assert_eq!(ScalarValue::Float64(1.23).to_string(), "1.23");
    assert_eq!(ScalarValue::Utf8("hello".into()).to_string(), "'hello'");
    assert_eq!(ScalarValue::Binary(vec![0xDE, 0xAD]).to_string(), "X'dead'");
}

#[test]
fn test_predicate_column() {
    let pred = Predicate::Eq {
        column: "id".into(),
        value: ScalarValue::Int64(1),
    };
    assert_eq!(pred.column(), "id");

    let pred = Predicate::IsNull {
        column: "name".into(),
    };
    assert_eq!(pred.column(), "name");
}

#[test]
fn test_predicate_to_sql() {
    assert_eq!(
        predicate_to_sql(&Predicate::Eq {
            column: "id".into(),
            value: ScalarValue::Int64(42),
        }),
        "\"id\" = 42"
    );

    assert_eq!(
        predicate_to_sql(&Predicate::In {
            column: "status".into(),
            values: vec![
                ScalarValue::Utf8("active".into()),
                ScalarValue::Utf8("pending".into()),
            ],
        }),
        "\"status\" IN ('active', 'pending')"
    );

    // Reserved word column name
    assert_eq!(
        predicate_to_sql(&Predicate::Gt {
            column: "order".into(),
            value: ScalarValue::Int64(10),
        }),
        "\"order\" > 10"
    );

    assert_eq!(
        predicate_to_sql(&Predicate::IsNull {
            column: "deleted_at".into(),
        }),
        "\"deleted_at\" IS NULL"
    );
}

#[test]
fn test_split_predicates() {
    let capabilities = SourceCapabilities {
        eq_columns: vec!["id".into(), "name".into()],
        range_columns: vec!["created_at".into()],
        in_columns: vec!["status".into()],
        supports_null_check: false,
    };

    let predicates = vec![
        Predicate::Eq {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        },
        Predicate::Gt {
            column: "created_at".into(),
            value: ScalarValue::Timestamp(1_000_000),
        },
        Predicate::IsNull {
            column: "deleted_at".into(),
        },
        Predicate::In {
            column: "status".into(),
            values: vec![ScalarValue::Utf8("active".into())],
        },
        // This Eq is on a non-pushable column
        Predicate::Eq {
            column: "region".into(),
            value: ScalarValue::Utf8("us-east".into()),
        },
    ];

    let split = split_predicates(predicates, &capabilities);
    assert_eq!(split.pushable.len(), 3); // id=, created_at>, status IN
    assert_eq!(split.local.len(), 2); // IS NULL (no null support), region=
}

#[test]
fn test_scalar_value_display_escapes_single_quotes() {
    // SQL injection vector: O'Brien must become O''Brien
    assert_eq!(
        ScalarValue::Utf8("O'Brien".into()).to_string(),
        "'O''Brien'"
    );
    // Double quotes are not special in SQL string literals
    assert_eq!(
        ScalarValue::Utf8(r#"say "hello""#.into()).to_string(),
        r#"'say "hello"'"#
    );
    // Multiple consecutive single quotes
    assert_eq!(ScalarValue::Utf8("it''s".into()).to_string(), "'it''''s'");
    // Empty string
    assert_eq!(ScalarValue::Utf8(String::new()).to_string(), "''");
}

#[test]
fn test_not_eq_never_pushed_down() {
    let capabilities = SourceCapabilities {
        eq_columns: vec!["id".into()],
        range_columns: vec![],
        in_columns: vec![],
        supports_null_check: false,
    };

    let predicates = vec![
        Predicate::Eq {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        },
        Predicate::NotEq {
            column: "id".into(),
            value: ScalarValue::Int64(2),
        },
    ];

    let split = split_predicates(predicates, &capabilities);
    // Eq should be pushed, NotEq should stay local
    assert_eq!(split.pushable.len(), 1);
    assert!(matches!(&split.pushable[0], Predicate::Eq { .. }));
    assert_eq!(split.local.len(), 1);
    assert!(matches!(&split.local[0], Predicate::NotEq { .. }));
}

#[test]
fn test_split_predicates_empty_capabilities() {
    let capabilities = SourceCapabilities::default();
    let predicates = vec![Predicate::Eq {
        column: "id".into(),
        value: ScalarValue::Int64(1),
    }];

    let split = split_predicates(predicates, &capabilities);
    assert!(split.pushable.is_empty());
    assert_eq!(split.local.len(), 1);
}
