use super::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::Tokenizer;

fn tokenize(sql: &str) -> Vec<TokenWithSpan> {
    let dialect = GenericDialect {};
    Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .unwrap()
}

#[test]
fn test_detect_create_source() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("CREATE SOURCE events (id INT)")),
        StreamingDdlKind::CreateSource { or_replace: false }
    );
}

#[test]
fn test_detect_create_or_replace_source() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("CREATE OR REPLACE SOURCE events (id INT)")),
        StreamingDdlKind::CreateSource { or_replace: true }
    );
}

#[test]
fn test_detect_create_sink() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("CREATE SINK output FROM events")),
        StreamingDdlKind::CreateSink { or_replace: false }
    );
}

#[test]
fn test_detect_create_continuous_query() {
    assert_eq!(
        detect_streaming_ddl(&tokenize(
            "CREATE CONTINUOUS QUERY q AS SELECT * FROM events"
        )),
        StreamingDdlKind::CreateContinuousQuery { or_replace: false }
    );
}

#[test]
fn test_detect_standard_sql() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SELECT * FROM events")),
        StreamingDdlKind::None
    );
}

#[test]
fn test_detect_create_table_is_not_streaming() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("CREATE TABLE events (id INT)")),
        StreamingDdlKind::None
    );
}

#[test]
fn test_detect_case_insensitive() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("create source events (id int)")),
        StreamingDdlKind::CreateSource { or_replace: false }
    );
}

#[test]
fn test_custom_keyword_helpers() {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql("WATERMARK FOR ts")
        .unwrap();

    assert!(try_parse_custom_keyword(&mut parser, "WATERMARK"));
    // WATERMARK consumed, next should be FOR
    assert!(parser.parse_keyword(Keyword::FOR));
}

#[test]
fn test_expect_custom_keyword_error() {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql("SELECT * FROM t")
        .unwrap();

    let result = expect_custom_keyword(&mut parser, "WATERMARK");
    assert!(result.is_err());
}

#[test]
fn test_parse_with_options_basic() {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql("WITH ('connector' = 'kafka', 'topic' = 'events')")
        .unwrap();

    let options = parse_with_options(&mut parser).unwrap();
    assert_eq!(options.get("connector"), Some(&"kafka".to_string()));
    assert_eq!(options.get("topic"), Some(&"events".to_string()));
}

#[test]
fn test_parse_with_options_empty() {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql("SELECT 1").unwrap();

    let options = parse_with_options(&mut parser).unwrap();
    assert!(options.is_empty());
}

#[test]
fn test_detect_drop_source() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP SOURCE events")),
        StreamingDdlKind::DropSource { if_exists: false }
    );
}

#[test]
fn test_detect_drop_source_if_exists() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP SOURCE IF EXISTS events")),
        StreamingDdlKind::DropSource { if_exists: true }
    );
}

#[test]
fn test_detect_drop_sink() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP SINK output")),
        StreamingDdlKind::DropSink { if_exists: false }
    );
}

#[test]
fn test_detect_drop_sink_if_exists() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP SINK IF EXISTS output")),
        StreamingDdlKind::DropSink { if_exists: true }
    );
}

#[test]
fn test_detect_drop_materialized_view() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP MATERIALIZED VIEW live_stats")),
        StreamingDdlKind::DropMaterializedView { if_exists: false }
    );
}

#[test]
fn test_detect_drop_materialized_view_if_exists() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DROP MATERIALIZED VIEW IF EXISTS live_stats")),
        StreamingDdlKind::DropMaterializedView { if_exists: true }
    );
}

#[test]
fn test_detect_show_sources() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW SOURCES")),
        StreamingDdlKind::ShowSources
    );
}

#[test]
fn test_detect_show_sinks() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW SINKS")),
        StreamingDdlKind::ShowSinks
    );
}

#[test]
fn test_detect_show_queries() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW QUERIES")),
        StreamingDdlKind::ShowQueries
    );
}

#[test]
fn test_detect_show_materialized_views() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW MATERIALIZED VIEWS")),
        StreamingDdlKind::ShowMaterializedViews
    );
}

#[test]
fn test_detect_show_tables() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW TABLES")),
        StreamingDdlKind::ShowTables
    );
}

#[test]
fn test_detect_describe() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DESCRIBE events")),
        StreamingDdlKind::DescribeSource
    );
}

#[test]
fn test_detect_explain_select() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("EXPLAIN SELECT * FROM events")),
        StreamingDdlKind::ExplainStreaming
    );
}

#[test]
fn test_detect_create_materialized_view() {
    assert_eq!(
        detect_streaming_ddl(&tokenize(
            "CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events"
        )),
        StreamingDdlKind::CreateMaterializedView { or_replace: false }
    );
}

#[test]
fn test_detect_create_or_replace_materialized_view() {
    assert_eq!(
        detect_streaming_ddl(&tokenize(
            "CREATE OR REPLACE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events"
        )),
        StreamingDdlKind::CreateMaterializedView { or_replace: true }
    );
}

#[test]
fn test_detect_show_case_insensitive() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("show sources")),
        StreamingDdlKind::ShowSources
    );
}

#[test]
fn test_detect_drop_source_case_insensitive() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("drop source events")),
        StreamingDdlKind::DropSource { if_exists: false }
    );
}

#[test]
fn test_detect_checkpoint() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("CHECKPOINT")),
        StreamingDdlKind::Checkpoint
    );
}

#[test]
fn test_detect_checkpoint_case_insensitive() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("checkpoint")),
        StreamingDdlKind::Checkpoint
    );
}

#[test]
fn test_detect_show_checkpoint_status() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SHOW CHECKPOINT STATUS")),
        StreamingDdlKind::ShowCheckpointStatus
    );
}

#[test]
fn test_detect_restore_checkpoint() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("RESTORE FROM CHECKPOINT 42")),
        StreamingDdlKind::RestoreCheckpoint
    );
}

#[test]
fn test_detect_subscribe() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("SUBSCRIBE foo")),
        StreamingDdlKind::Subscribe
    );
    assert_eq!(
        detect_streaming_ddl(&tokenize("subscribe foo with ('snapshot' = 'true')")),
        StreamingDdlKind::Subscribe
    );
}

#[test]
fn test_detect_declare_cursor_for_subscribe() {
    assert_eq!(
        detect_streaming_ddl(&tokenize("DECLARE c CURSOR FOR SUBSCRIBE foo")),
        StreamingDdlKind::DeclareCursor
    );
    assert_eq!(
        detect_streaming_ddl(&tokenize("DECLARE c NO SCROLL CURSOR FOR SUBSCRIBE foo")),
        StreamingDdlKind::DeclareCursor
    );
    assert_eq!(
        detect_streaming_ddl(&tokenize("declare c cursor without hold for subscribe foo")),
        StreamingDdlKind::DeclareCursor
    );
}

#[test]
fn test_detect_declare_cursor_for_select_falls_through() {
    // DECLARE…CURSOR FOR <regular query> is left to sqlparser.
    assert_eq!(
        detect_streaming_ddl(&tokenize("DECLARE c CURSOR FOR SELECT 1")),
        StreamingDdlKind::None
    );
}

#[test]
fn test_declare_does_not_cross_statement_boundary() {
    // A trailing `FOR SUBSCRIBE` in a *later* statement must not route
    // the leading DECLARE into our SUBSCRIBE-specific parser.
    assert_eq!(
        detect_streaming_ddl(&tokenize(
            "DECLARE x INT; SELECT 1 FROM t FOR SUBSCRIBE foo"
        )),
        StreamingDdlKind::None
    );
}
