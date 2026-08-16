use super::*;

/// Helper to parse SQL and return the first statement.
fn parse_one(sql: &str) -> StreamingStatement {
    let stmts = StreamingParser::parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1, "Expected exactly 1 statement");
    stmts.into_iter().next().unwrap()
}

fn assert_standard_as_of_query(sql: &str) {
    let reparsed = StreamingParser::parse_sql(sql).unwrap();
    let [StreamingStatement::Standard(statement)] = reparsed.as_slice() else {
        panic!("expected exactly one standard query, got {reparsed:?}");
    };
    assert_eq!(
        crate::temporal::temporal_table_version_count(statement.as_ref()),
        1,
        "expected one AS-OF table version in {sql}"
    );
}

#[test]
fn custom_catalog_parsers_reject_duplicate_options_and_trailing_tokens() {
    let duplicates = [
            "CREATE SOURCE events (id BIGINT) FROM KAFKA ('topic' = 'a', 'TOPIC' = 'b')",
            "CREATE SINK output FROM events INTO KAFKA ('topic' = 'a', 'TOPIC' = 'b')",
            "CREATE LOOKUP TABLE users (id BIGINT, PRIMARY KEY (id)) WITH ('connector' = 'postgres', 'CONNECTOR' = 'redis')",
        ];
    for sql in duplicates {
        let error = StreamingParser::parse_sql(sql).unwrap_err().to_string();
        assert!(error.contains("duplicate connector option"), "{error}");
    }

    let trailing = [
            "CREATE SOURCE events (id BIGINT) WITH ('buffer_size' = '4096') TRAILING",
            "CREATE SINK output FROM events INTO KAFKA ('topic' = 'a') TRAILING",
            "CREATE LOOKUP TABLE users (id BIGINT, PRIMARY KEY (id)) WITH ('connector' = 'postgres') TRAILING",
        ];
    for sql in trailing {
        let error = StreamingParser::parse_sql(sql).unwrap_err().to_string();
        assert!(error.contains("unexpected trailing token"), "{error}");
    }
}

#[test]
fn test_parse_drop_source() {
    let stmt = parse_one("DROP SOURCE events");
    match stmt {
        StreamingStatement::DropSource {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "events");
            assert!(!if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_source_if_exists() {
    let stmt = parse_one("DROP SOURCE IF EXISTS events");
    match stmt {
        StreamingStatement::DropSource {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "events");
            assert!(if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_source_cascade() {
    let stmt = parse_one("DROP SOURCE IF EXISTS events CASCADE");
    match stmt {
        StreamingStatement::DropSource {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "events");
            assert!(if_exists);
            assert!(cascade);
        }
        _ => panic!("Expected DropSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_sink() {
    let stmt = parse_one("DROP SINK output");
    match stmt {
        StreamingStatement::DropSink {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "output");
            assert!(!if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropSink, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_sink_if_exists() {
    let stmt = parse_one("DROP SINK IF EXISTS output");
    match stmt {
        StreamingStatement::DropSink {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "output");
            assert!(if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropSink, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_sink_cascade() {
    let stmt = parse_one("DROP SINK output CASCADE");
    match stmt {
        StreamingStatement::DropSink {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "output");
            assert!(!if_exists);
            assert!(cascade);
        }
        _ => panic!("Expected DropSink, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_materialized_view() {
    let stmt = parse_one("DROP MATERIALIZED VIEW live_stats");
    match stmt {
        StreamingStatement::DropMaterializedView {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert!(!if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_materialized_view_if_exists_cascade() {
    let stmt = parse_one("DROP MATERIALIZED VIEW IF EXISTS live_stats CASCADE");
    match stmt {
        StreamingStatement::DropMaterializedView {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert!(if_exists);
            assert!(cascade);
        }
        _ => panic!("Expected DropMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_show_sources() {
    let stmt = parse_one("SHOW SOURCES");
    assert!(matches!(
        stmt,
        StreamingStatement::Show(ShowCommand::Sources)
    ));
}

#[test]
fn test_parse_show_sinks() {
    let stmt = parse_one("SHOW SINKS");
    assert!(matches!(stmt, StreamingStatement::Show(ShowCommand::Sinks)));
}

#[test]
fn test_parse_show_queries() {
    let stmt = parse_one("SHOW QUERIES");
    assert!(matches!(
        stmt,
        StreamingStatement::Show(ShowCommand::Queries)
    ));
}

#[test]
fn test_parse_show_materialized_views() {
    let stmt = parse_one("SHOW MATERIALIZED VIEWS");
    assert!(matches!(
        stmt,
        StreamingStatement::Show(ShowCommand::MaterializedViews)
    ));
}

#[test]
fn test_parse_describe() {
    let stmt = parse_one("DESCRIBE events");
    match stmt {
        StreamingStatement::Describe { name, extended } => {
            assert_eq!(name.to_string(), "events");
            assert!(!extended);
        }
        _ => panic!("Expected Describe, got {stmt:?}"),
    }
}

#[test]
fn test_parse_describe_extended() {
    let stmt = parse_one("DESCRIBE EXTENDED my_schema.events");
    match stmt {
        StreamingStatement::Describe { name, extended } => {
            assert_eq!(name.to_string(), "my_schema.events");
            assert!(extended);
        }
        _ => panic!("Expected Describe, got {stmt:?}"),
    }
}

#[test]
fn test_parse_explain_select() {
    let stmt = parse_one("EXPLAIN SELECT * FROM events");
    match stmt {
        StreamingStatement::Explain {
            statement, analyze, ..
        } => {
            assert!(matches!(*statement, StreamingStatement::Standard(_)));
            assert!(!analyze);
        }
        _ => panic!("Expected Explain, got {stmt:?}"),
    }
}

#[test]
fn test_parse_explain_create_source() {
    let stmt = parse_one("EXPLAIN CREATE SOURCE events (id BIGINT)");
    match stmt {
        StreamingStatement::Explain { statement, .. } => {
            assert!(matches!(*statement, StreamingStatement::CreateSource(_)));
        }
        _ => panic!("Expected Explain wrapping CreateSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_explain_analyze_select() {
    let stmt = parse_one("EXPLAIN ANALYZE SELECT * FROM events");
    match stmt {
        StreamingStatement::Explain {
            statement, analyze, ..
        } => {
            assert!(matches!(*statement, StreamingStatement::Standard(_)));
            assert!(analyze, "Expected analyze=true for EXPLAIN ANALYZE");
        }
        _ => panic!("Expected Explain, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_materialized_view() {
    let stmt = parse_one("CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events");
    match stmt {
        StreamingStatement::CreateMaterializedView {
            name,
            emit_clause,
            or_replace,
            if_not_exists,
            ..
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert!(emit_clause.is_none());
            assert!(!or_replace);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_materialized_view_with_emit() {
    let stmt = parse_one(
        "CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE",
    );
    match stmt {
        StreamingStatement::CreateMaterializedView {
            name, emit_clause, ..
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
        }
        _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_or_replace_materialized_view() {
    let stmt =
        parse_one("CREATE OR REPLACE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events");
    match stmt {
        StreamingStatement::CreateMaterializedView {
            name,
            or_replace,
            if_not_exists,
            ..
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert!(or_replace);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_materialized_view_if_not_exists() {
    let stmt = parse_one(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS live_stats AS SELECT COUNT(*) FROM events",
    );
    match stmt {
        StreamingStatement::CreateMaterializedView {
            name,
            or_replace,
            if_not_exists,
            ..
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert!(!or_replace);
            assert!(if_not_exists);
        }
        _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
    }
}

#[test]
fn test_parse_insert_into() {
    let stmt = parse_one("INSERT INTO events (id, name) VALUES (1, 'test')");
    match stmt {
        StreamingStatement::InsertInto {
            table_name,
            columns,
            values,
        } => {
            assert_eq!(table_name.to_string(), "events");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].to_string(), "id");
            assert_eq!(columns[1].to_string(), "name");
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 2);
        }
        _ => panic!("Expected InsertInto, got {stmt:?}"),
    }
}

#[test]
fn test_parse_insert_into_multiple_rows() {
    let stmt = parse_one("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    match stmt {
        StreamingStatement::InsertInto {
            table_name,
            columns,
            values,
        } => {
            assert_eq!(table_name.to_string(), "events");
            assert!(columns.is_empty());
            assert_eq!(values.len(), 3);
        }
        _ => panic!("Expected InsertInto, got {stmt:?}"),
    }
}

// ── CREATE STREAM tests ─────────────────────────────

#[test]
fn test_parse_create_stream() {
    let stmt = parse_one(
            "CREATE STREAM session_activity AS SELECT session_id, COUNT(*) as cnt FROM clicks GROUP BY session_id",
        );
    match stmt {
        StreamingStatement::CreateStream {
            name,
            or_replace,
            if_not_exists,
            emit_clause,
            ..
        } => {
            assert_eq!(name.to_string(), "session_activity");
            assert!(!or_replace);
            assert!(!if_not_exists);
            assert!(emit_clause.is_none());
        }
        _ => panic!("Expected CreateStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_or_replace_stream() {
    let stmt = parse_one("CREATE OR REPLACE STREAM metrics AS SELECT AVG(value) FROM events");
    match stmt {
        StreamingStatement::CreateStream { or_replace, .. } => {
            assert!(or_replace);
        }
        _ => panic!("Expected CreateStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_stream_if_not_exists() {
    let stmt = parse_one("CREATE STREAM IF NOT EXISTS counts AS SELECT COUNT(*) FROM events");
    match stmt {
        StreamingStatement::CreateStream { if_not_exists, .. } => {
            assert!(if_not_exists);
        }
        _ => panic!("Expected CreateStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_create_stream_with_emit() {
    let stmt =
        parse_one("CREATE STREAM windowed AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE");
    match stmt {
        StreamingStatement::CreateStream { emit_clause, .. } => {
            assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
        }
        _ => panic!("Expected CreateStream, got {stmt:?}"),
    }
}

#[test]
fn create_stream_with_retain_history() {
    let stmt =
        parse_one("CREATE STREAM trades AS SELECT * FROM events WITH ('retain_history' = '64mb')");
    let StreamingStatement::CreateStream {
        retention_bytes, ..
    } = stmt
    else {
        panic!("expected CreateStream");
    };
    assert_eq!(retention_bytes, Some(64 * 1024 * 1024));
}

#[test]
fn create_stream_with_retain_history_after_emit() {
    let stmt = parse_one(
        "CREATE STREAM trades AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE \
             WITH ('retain_history' = '8mb')",
    );
    let StreamingStatement::CreateStream {
        retention_bytes,
        emit_clause,
        ..
    } = stmt
    else {
        panic!("expected CreateStream");
    };
    assert_eq!(retention_bytes, Some(8 * 1024 * 1024));
    assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
}

#[test]
fn create_stream_rejects_unknown_with_option() {
    let res = parse_streaming_sql("CREATE STREAM s AS SELECT * FROM e WITH ('bogus' = '1')");
    let err = res.expect_err("unknown WITH key must error");
    assert!(err.to_string().contains("bogus"), "got: {err}");
}

#[test]
fn create_stream_with_retain_history_excludes_with_from_query_sql() {
    // Regression: query_body_sql used to extend to original_sql.len() when
    // EMIT was absent, which sucked the trailing WITH clause into query_sql
    // and broke planner re-parsing.
    let stmt =
        parse_one("CREATE STREAM trades AS SELECT * FROM events WITH ('retain_history' = '64mb')");
    let StreamingStatement::CreateStream { query_sql, .. } = stmt else {
        panic!("expected CreateStream");
    };
    assert!(
        !query_sql.to_uppercase().contains("RETAIN_HISTORY"),
        "query_sql leaked WITH clause: {query_sql}"
    );
    assert!(
        query_sql.contains("FROM events"),
        "query_sql should still hold the SELECT body, got: {query_sql}"
    );
}

#[test]
fn test_parse_drop_stream() {
    let stmt = parse_one("DROP STREAM my_stream");
    match stmt {
        StreamingStatement::DropStream {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "my_stream");
            assert!(!if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_stream_if_exists() {
    let stmt = parse_one("DROP STREAM IF EXISTS my_stream");
    match stmt {
        StreamingStatement::DropStream {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "my_stream");
            assert!(if_exists);
            assert!(!cascade);
        }
        _ => panic!("Expected DropStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_stream_cascade() {
    let stmt = parse_one("DROP STREAM my_stream CASCADE");
    match stmt {
        StreamingStatement::DropStream {
            name,
            if_exists,
            cascade,
        } => {
            assert_eq!(name.to_string(), "my_stream");
            assert!(!if_exists);
            assert!(cascade);
        }
        _ => panic!("Expected DropStream, got {stmt:?}"),
    }
}

#[test]
fn test_parse_show_streams() {
    let stmt = parse_one("SHOW STREAMS");
    assert!(matches!(
        stmt,
        StreamingStatement::Show(ShowCommand::Streams)
    ));
}

#[test]
fn test_parse_alter_source_add_column() {
    let stmt = parse_one("ALTER SOURCE events ADD COLUMN new_col INT");
    match stmt {
        StreamingStatement::AlterSource { name, operation } => {
            assert_eq!(name.to_string(), "events");
            match operation {
                statements::AlterSourceOperation::AddColumn { column_def } => {
                    assert_eq!(column_def.name.value, "new_col");
                    assert_eq!(column_def.data_type, sqlparser::ast::DataType::Int(None));
                }
                statements::AlterSourceOperation::SetProperties { .. } => {
                    panic!("Expected AddColumn")
                }
            }
        }
        _ => panic!("Expected AlterSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_alter_source_set_properties() {
    let stmt = parse_one("ALTER SOURCE events SET ('batch.size' = '1000', 'timeout' = '5s')");
    match stmt {
        StreamingStatement::AlterSource { name, operation } => {
            assert_eq!(name.to_string(), "events");
            match operation {
                statements::AlterSourceOperation::SetProperties { properties } => {
                    assert_eq!(properties.get("batch.size"), Some(&"1000".to_string()));
                    assert_eq!(properties.get("timeout"), Some(&"5s".to_string()));
                }
                statements::AlterSourceOperation::AddColumn { .. } => {
                    panic!("Expected SetProperties")
                }
            }
        }
        _ => panic!("Expected AlterSource, got {stmt:?}"),
    }
}

#[test]
fn test_parse_checkpoint() {
    let stmt = parse_one("CHECKPOINT");
    assert!(
        matches!(stmt, StreamingStatement::Checkpoint),
        "Expected Checkpoint, got {stmt:?}"
    );
}

#[test]
fn test_parse_show_checkpoint_status() {
    let stmt = parse_one("SHOW CHECKPOINT STATUS");
    assert!(
        matches!(
            stmt,
            StreamingStatement::Show(ShowCommand::CheckpointStatus)
        ),
        "Expected Show(CheckpointStatus), got {stmt:?}"
    );
}

#[test]
fn test_parse_restore_checkpoint() {
    let stmt = parse_one("RESTORE FROM CHECKPOINT 42");
    match stmt {
        StreamingStatement::RestoreCheckpoint { checkpoint_id } => {
            assert_eq!(checkpoint_id, 42);
        }
        _ => panic!("Expected RestoreCheckpoint, got {stmt:?}"),
    }
}

#[test]
fn test_parse_restore_checkpoint_large_id() {
    let stmt = parse_one("RESTORE FROM CHECKPOINT 123456");
    match stmt {
        StreamingStatement::RestoreCheckpoint { checkpoint_id } => {
            assert_eq!(checkpoint_id, 123_456);
        }
        _ => panic!("Expected RestoreCheckpoint, got {stmt:?}"),
    }
}

#[test]
fn test_parse_show_create_source() {
    let stmt = parse_one("SHOW CREATE SOURCE events");
    match stmt {
        StreamingStatement::Show(ShowCommand::CreateSource { name }) => {
            assert_eq!(name.to_string(), "events");
        }
        _ => panic!("Expected Show(CreateSource), got {stmt:?}"),
    }
}

#[test]
fn test_parse_show_create_sink() {
    let stmt = parse_one("SHOW CREATE SINK output");
    match stmt {
        StreamingStatement::Show(ShowCommand::CreateSink { name }) => {
            assert_eq!(name.to_string(), "output");
        }
        _ => panic!("Expected Show(CreateSink), got {stmt:?}"),
    }
}

#[test]
fn create_stream_normalizes_temporal_probe_join() {
    let sql = "CREATE STREAM markouts_long AS \
                   SELECT t.s, p.offset_ms FROM trade_probe t \
                   TEMPORAL PROBE JOIN price_ref r \
                       ON (s, venue) TIMESTAMPS (ts, ts) \
                       LIST (0s, 1s, 5s, 30s) AS p";
    let StreamingStatement::CreateStream {
        query, query_sql, ..
    } = parse_one(sql)
    else {
        panic!("expected CreateStream");
    };
    let StreamingStatement::TemporalProbeQuery { analysis, .. } = *query else {
        panic!("expected normalized temporal probe query");
    };
    assert_eq!(
        analysis
            .temporal_probe_schedule
            .as_ref()
            .unwrap()
            .offsets_ms(),
        [0, 1_000, 5_000, 30_000]
    );
    assert_eq!(analysis.temporal_probe_alias.as_deref(), Some("p"));
    assert_eq!(analysis.left_key_column, "s");
    assert_eq!(analysis.right_key_column, "s");
    assert_eq!(
        analysis.additional_key_columns,
        [("venue".to_string(), "venue".to_string())]
    );
    assert!(query_sql.contains("FOR SYSTEM_TIME AS OF"), "{query_sql}");
    assert!(
        query_sql.contains("t.s = r.s AND t.venue = r.venue"),
        "{query_sql}"
    );
    assert!(
        query_sql.contains("price_ref FOR SYSTEM_TIME AS OF t.ts AS r"),
        "{query_sql}"
    );
    assert!(!query_sql.contains("TEMPORAL PROBE"), "{query_sql}");
    assert_standard_as_of_query(&query_sql);
}

#[test]
fn materialized_view_temporal_probe_query_sql_reparses() {
    let sql = "CREATE MATERIALIZED VIEW markouts AS \
                   SELECT t.s, p.offset_ms FROM trade_probe t \
                   TEMPORAL PROBE JOIN price_ref r \
                       ON (s) TIMESTAMPS (ts, ts) \
                       LIST (0s, 5s) AS p";
    let StreamingStatement::CreateMaterializedView { query_sql, .. } = parse_one(sql) else {
        panic!("expected CreateMaterializedView");
    };

    assert!(
        query_sql.contains("price_ref FOR SYSTEM_TIME AS OF t.ts AS r"),
        "{query_sql}"
    );
    assert_standard_as_of_query(&query_sql);
}

#[test]
fn temporal_probe_range_is_checked_and_expanded_once() {
    let sql = "SELECT probe.offset_ms, probe.probe_time FROM trades t \
                   TEMPORAL PROBE JOIN quotes q ON (symbol) \
                   TIMESTAMPS (trade_time, quote_time) \
                   RANGE FROM -5s TO 5s STEP 5s AS probe";
    let statements = parse_streaming_sql(sql).unwrap();
    let [StreamingStatement::TemporalProbeQuery { analysis, .. }] = statements.as_slice() else {
        panic!("expected temporal probe query");
    };
    assert_eq!(
        analysis
            .temporal_probe_schedule
            .as_ref()
            .unwrap()
            .offsets_ms(),
        [-5_000, 0, 5_000]
    );

    for invalid in [
        "LIST (0s, 0s)",
        "RANGE FROM 0s TO 256s STEP 1s",
        "RANGE FROM 0s TO 10s STEP 3s",
        "LIST (366d)",
    ] {
        let sql = format!(
            "SELECT * FROM trades t TEMPORAL PROBE JOIN quotes q ON (symbol) \
                 TIMESTAMPS (trade_time, quote_time) {invalid} AS probe"
        );
        assert!(parse_streaming_sql(&sql).is_err(), "accepted: {sql}");
    }

    for keys in ["", "symbol,", ", symbol", "symbol venue"] {
        let sql = format!(
            "SELECT * FROM trades t TEMPORAL PROBE JOIN quotes q ON ({keys}) \
                 TIMESTAMPS (trade_time, quote_time) LIST (0s) AS probe"
        );
        assert!(parse_streaming_sql(&sql).is_err(), "accepted: {sql}");
    }
}

#[test]
fn create_stream_emit_is_not_captured_in_query_sql() {
    let sql = "CREATE STREAM s AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE";
    let StreamingStatement::CreateStream {
        query_sql,
        emit_clause,
        ..
    } = parse_one(sql)
    else {
        panic!("expected CreateStream");
    };
    assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
    assert!(
        !query_sql.to_uppercase().contains("EMIT"),
        "got: {query_sql}"
    );
    assert!(query_sql.contains("FROM events"), "got: {query_sql}");
}

#[test]
fn create_stream_plain_select_query_sql_executes() {
    let sql = "CREATE STREAM counts AS SELECT COUNT(*) AS c FROM events";
    let StreamingStatement::CreateStream { query_sql, .. } = parse_one(sql) else {
        panic!("expected CreateStream");
    };
    assert!(query_sql.to_uppercase().contains("SELECT"));
    assert!(query_sql.contains("FROM events"));
}
