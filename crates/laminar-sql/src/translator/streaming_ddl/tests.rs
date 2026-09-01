use super::*;
use crate::parser::{parse_streaming_sql, StreamingStatement};

fn parse_and_translate(sql: &str) -> Result<SourceDefinition, ParseError> {
    let statements = parse_streaming_sql(sql)?;
    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| ParseError::StreamingError("No statement found".to_string()))?;
    match stmt {
        StreamingStatement::CreateSource(source) => translate_create_source(*source),
        _ => Err(ParseError::StreamingError(
            "Expected CREATE SOURCE".to_string(),
        )),
    }
}

#[test]
fn test_basic_source() {
    let def =
        parse_and_translate("CREATE SOURCE events (id BIGINT NOT NULL, name VARCHAR)").unwrap();

    assert_eq!(def.name, "events");
    assert_eq!(def.columns.len(), 2);
    assert_eq!(def.columns[0].name, "id");
    assert_eq!(def.columns[0].data_type, DataType::Int64);
    assert!(!def.columns[0].nullable);
    assert_eq!(def.columns[1].name, "name");
    assert!(def.columns[1].nullable);
}

#[test]
fn full_changelog_weight_requires_one_canonical_trailing_field() {
    let definition =
        parse_and_translate("CREATE SOURCE changes (id BIGINT NOT NULL, __weight BIGINT NOT NULL)")
            .unwrap();
    let weight = definition.schema.field(1);
    assert_eq!(weight.name(), "__weight");
    assert_eq!(weight.data_type(), &DataType::Int64);
    assert!(!weight.is_nullable());

    for sql in [
        "CREATE SOURCE changes (__WEIGHT BIGINT NOT NULL)",
        "CREATE SOURCE changes (__weight BIGINT NOT NULL, id BIGINT)",
        "CREATE SOURCE changes (id BIGINT, __weight BIGINT)",
        "CREATE SOURCE changes (id BIGINT, __weight INT NOT NULL)",
    ] {
        let error = parse_and_translate(sql).unwrap_err();
        assert!(
            error.to_string().contains("full-changelog metadata"),
            "{error}"
        );
    }

    for name in ["_op", "__op"] {
        let error = parse_and_translate(&format!("CREATE SOURCE changes ({name} BIGINT NOT NULL)"))
            .unwrap_err();
        assert!(
            error.to_string().contains("reserved mutation metadata"),
            "{error}"
        );
    }
}

#[test]
fn test_source_with_options() {
    let def = parse_and_translate(
        "CREATE SOURCE events (id BIGINT) WITH (
                'buffer_size' = '4096'
            )",
    )
    .unwrap();

    assert_eq!(def.config.buffer_size, 4096);
}

#[test]
fn test_source_with_watermark() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            )",
    )
    .unwrap();

    assert!(def.watermark.is_some());
    let wm = def.watermark.unwrap();
    assert_eq!(wm.column, "ts");
    assert_eq!(wm.max_out_of_orderness, Duration::from_secs(5));
}

#[test]
fn test_buffer_size_bounds() {
    // Too small
    let result = parse_and_translate("CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '1')");
    assert!(result.is_err());

    // Too large
    let result =
        parse_and_translate("CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '999999999')");
    assert!(result.is_err());

    // Valid
    let result =
        parse_and_translate("CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '1024')");
    assert!(result.is_ok());
}

#[test]
fn test_sql_type_conversions() {
    let def = parse_and_translate(
        "CREATE SOURCE types (
                a TINYINT,
                b SMALLINT,
                c INT,
                d BIGINT,
                e FLOAT,
                f DOUBLE,
                g DECIMAL(10,2),
                h VARCHAR(255),
                i TEXT,
                j BOOLEAN,
                k TIMESTAMP,
                l DATE
            )",
    )
    .unwrap();

    assert_eq!(def.columns.len(), 12);
    assert_eq!(def.columns[0].data_type, DataType::Int8);
    assert_eq!(def.columns[1].data_type, DataType::Int16);
    assert_eq!(def.columns[2].data_type, DataType::Int32);
    assert_eq!(def.columns[3].data_type, DataType::Int64);
    assert_eq!(def.columns[4].data_type, DataType::Float32);
    assert_eq!(def.columns[5].data_type, DataType::Float64);
    assert_eq!(def.columns[6].data_type, DataType::Decimal128(10, 2));
    assert_eq!(def.columns[7].data_type, DataType::Utf8);
    assert_eq!(def.columns[8].data_type, DataType::Utf8);
    assert_eq!(def.columns[9].data_type, DataType::Boolean);
    assert!(matches!(
        def.columns[10].data_type,
        DataType::Timestamp(_, _)
    ));
    assert_eq!(def.columns[11].data_type, DataType::Date32);
}

#[test]
fn test_schema_generation() {
    let def = parse_and_translate(
        "CREATE SOURCE events (id BIGINT NOT NULL, name VARCHAR NOT NULL, value DOUBLE)",
    )
    .unwrap();

    let schema = def.schema;
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "name");
    assert!(!schema.field(1).is_nullable());
    assert_eq!(schema.field(2).name(), "value");
    assert!(schema.field(2).is_nullable());
}

#[test]
fn test_watermark_column_not_found() {
    let result = parse_and_translate(
        "CREATE SOURCE events (
                id BIGINT,
                WATERMARK FOR nonexistent AS nonexistent - INTERVAL '1' SECOND
            )",
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_watermark_wrong_type() {
    let result = parse_and_translate(
        "CREATE SOURCE events (
                name VARCHAR,
                WATERMARK FOR name AS name - INTERVAL '1' SECOND
            )",
    );

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must be a TIMESTAMP"));
}

#[test]
fn test_watermark_milliseconds() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECOND
            )",
    )
    .unwrap();

    let wm = def.watermark.unwrap();
    assert_eq!(wm.max_out_of_orderness, Duration::from_millis(100));
}

#[test]
fn test_watermark_minutes() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' MINUTE
            )",
    )
    .unwrap();

    let wm = def.watermark.unwrap();
    assert_eq!(wm.max_out_of_orderness, Duration::from_secs(300));
}

#[test]
fn test_default_config() {
    let def = parse_and_translate("CREATE SOURCE events (id BIGINT)").unwrap();

    assert_eq!(def.config.buffer_size, DEFAULT_BUFFER_SIZE);
}

#[test]
fn source_connector_options_require_from() {
    let error =
        parse_and_translate("CREATE SOURCE events (id BIGINT) WITH ('connector' = 'kafka')")
            .unwrap_err();
    assert!(
        error.to_string().contains("connector options in FROM"),
        "{error}"
    );
}

#[test]
fn discovered_nullable_primary_key_is_rejected() {
    let statements =
        crate::parse_streaming_sql("CREATE SOURCE events FROM KAFKA SCHEMA (PRIMARY KEY (id))")
            .unwrap();
    let StreamingStatement::CreateSource(stmt) = statements.into_iter().next().unwrap() else {
        panic!("expected CREATE SOURCE");
    };

    let error = translate_create_source_with_columns(
        *stmt,
        vec![ColumnDefinition {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: true,
        }],
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("discovered PRIMARY KEY column 'id' must be non-nullable"),
        "{error}"
    );
}

#[test]
fn declared_primary_key_preserves_identifier_quote_semantics() {
    let def =
        parse_and_translate("CREATE SOURCE events (id BIGINT, PRIMARY KEY (\"id\"))").unwrap();
    assert_eq!(def.primary_key, ["id"]);

    let def =
        parse_and_translate("CREATE SOURCE events (id BIGINT, \"ID\" BIGINT, PRIMARY KEY (ID))")
            .unwrap();
    assert_eq!(def.primary_key, ["id"]);
    assert!(!def.columns[0].nullable);
    assert!(def.columns[1].nullable);

    let def = parse_and_translate(
        "CREATE SOURCE events (id BIGINT, \"ID\" BIGINT, PRIMARY KEY (\"ID\"))",
    )
    .unwrap();
    assert_eq!(def.primary_key, ["ID"]);
    assert!(def.columns[0].nullable);
    assert!(!def.columns[1].nullable);

    let error =
        parse_and_translate("CREATE SOURCE events (\"ID\" BIGINT, PRIMARY KEY (id))").unwrap_err();
    assert!(error.to_string().contains("does not exist"), "{error}");
}

#[test]
fn discovered_primary_key_uses_external_field_matching() {
    let statements =
        crate::parse_streaming_sql("CREATE SOURCE events FROM KAFKA SCHEMA (PRIMARY KEY (ID))")
            .unwrap();
    let StreamingStatement::CreateSource(stmt) = statements.into_iter().next().unwrap() else {
        panic!("expected CREATE SOURCE");
    };
    let def = translate_create_source_with_columns(
        *stmt,
        vec![ColumnDefinition {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }],
    )
    .unwrap();
    assert_eq!(def.primary_key, ["id"]);
}

#[test]
fn test_source_watermark_no_expression() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts
            )",
    )
    .unwrap();

    assert!(def.watermark.is_some());
    let wm = def.watermark.unwrap();
    assert_eq!(wm.column, "ts");
    assert_eq!(wm.max_out_of_orderness, Duration::ZERO);
}

#[test]
fn test_source_watermark_bigint_column_rejected() {
    let result = parse_and_translate(
        "CREATE SOURCE events (
                ts BIGINT,
                WATERMARK FOR ts
            )",
    );

    let error = result.unwrap_err().to_string();
    assert!(error.contains("must be a TIMESTAMP"), "{error}");
}

#[test]
fn test_watermark_proctime() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS PROCTIME()
            )",
    )
    .unwrap();

    assert!(def.watermark.is_some());
    let wm = def.watermark.unwrap();
    assert_eq!(wm.column, "ts");
    assert!(wm.is_processing_time);
    assert_eq!(wm.max_out_of_orderness, Duration::ZERO);
}

#[test]
fn test_watermark_event_time_not_proctime() {
    let def = parse_and_translate(
        "CREATE SOURCE events (
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            )",
    )
    .unwrap();

    let wm = def.watermark.unwrap();
    assert!(!wm.is_processing_time);
}

#[test]
fn array_of_int_parses() {
    let def = parse_and_translate("CREATE SOURCE events (tags ARRAY<INT>)").unwrap();
    match &def.columns[0].data_type {
        DataType::List(field) => assert_eq!(field.data_type(), &DataType::Int32),
        other => panic!("expected DataType::List, got {other:?}"),
    }
}

#[test]
fn decimal_with_precision_parses() {
    let def = parse_and_translate("CREATE SOURCE events (amount DECIMAL(10, 2))").unwrap();
    assert_eq!(def.columns[0].data_type, DataType::Decimal128(10, 2));
}

/// Hand-declared MAP columns point users at auto-discovery.
#[test]
fn hand_declared_map_column_errors_actionably() {
    let err = parse_and_translate("CREATE SOURCE events (data MAP(VARCHAR, VARCHAR))").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("use auto-discovery") || msg.contains("unsupported"),
        "expected actionable error for hand-declared MAP, got: {msg}"
    );
}
