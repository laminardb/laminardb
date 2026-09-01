use super::*;
use sqlparser::ast::{DataType, Expr, ObjectNamePart};

#[test]
fn test_create_source_statement() {
    let stmt = CreateSourceStatement {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
        columns: vec![
            ColumnDef {
                name: Ident::new("id"),
                data_type: DataType::BigInt(None),
                options: vec![],
            },
            ColumnDef {
                name: Ident::new("timestamp"),
                data_type: DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::None),
                options: vec![],
            },
        ],
        primary_key: vec![],
        watermark: Some(WatermarkDef {
            column: Ident::new("timestamp"),
            expression: Some(Expr::Identifier(Ident::new("timestamp"))),
        }),
        with_options: HashMap::from([("buffer_size".to_string(), "4096".to_string())]),
        or_replace: false,
        if_not_exists: true,
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
    };

    // Check the statement fields
    assert_eq!(stmt.columns.len(), 2);
    assert!(stmt.watermark.is_some());
    assert_eq!(
        stmt.with_options.get("buffer_size"),
        Some(&"4096".to_string())
    );
}

#[test]
fn test_emit_clause_variants() {
    let emit1 = EmitClause::AfterWatermark;
    let emit2 = EmitClause::OnWindowClose;
    let emit3 = EmitClause::Periodically {
        interval: Box::new(Expr::Identifier(Ident::new("5_SECONDS"))),
    };
    let emit4 = EmitClause::OnUpdate;

    match emit1 {
        EmitClause::AfterWatermark => (),
        _ => panic!("Expected AfterWatermark"),
    }

    match emit2 {
        EmitClause::OnWindowClose => (),
        _ => panic!("Expected OnWindowClose"),
    }

    match emit3 {
        EmitClause::Periodically { .. } => (),
        _ => panic!("Expected Periodically"),
    }

    match emit4 {
        EmitClause::OnUpdate => (),
        _ => panic!("Expected OnUpdate"),
    }
}

#[test]
fn test_window_functions() {
    let tumble = WindowFunction::Tumble {
        time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
        interval: Box::new(Expr::Identifier(Ident::new("5_MINUTES"))),
        offset: None,
    };

    let hop = WindowFunction::Hop {
        time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
        slide_interval: Box::new(Expr::Identifier(Ident::new("1_MINUTE"))),
        window_interval: Box::new(Expr::Identifier(Ident::new("5_MINUTES"))),
        offset: None,
    };

    match tumble {
        WindowFunction::Tumble { .. } => (),
        _ => panic!("Expected Tumble"),
    }

    match hop {
        WindowFunction::Hop { .. } => (),
        _ => panic!("Expected Hop"),
    }
}

#[test]
fn test_late_data_clause_default() {
    let clause = LateDataClause::default();
    assert!(clause.allowed_lateness.is_none());
    assert!(clause.side_output.is_none());
}

#[test]
fn test_late_data_clause_with_allowed_lateness() {
    let lateness_expr = Expr::Identifier(Ident::new("INTERVAL '1' HOUR"));
    let clause = LateDataClause::with_allowed_lateness(lateness_expr);
    assert!(clause.allowed_lateness.is_some());
    assert!(clause.side_output.is_none());
}

#[test]
fn test_late_data_clause_with_side_output() {
    let lateness_expr = Expr::Identifier(Ident::new("INTERVAL '1' HOUR"));
    let clause = LateDataClause::with_side_output(lateness_expr, "late_events".to_string());
    assert!(clause.allowed_lateness.is_some());
    assert_eq!(clause.side_output, Some("late_events".to_string()));
}

#[test]
fn test_late_data_clause_side_output_only() {
    let clause = LateDataClause::side_output_only("late_events".to_string());
    assert!(clause.allowed_lateness.is_none());
    assert_eq!(clause.side_output, Some("late_events".to_string()));
}

#[test]
fn test_show_command_variants() {
    let sources = ShowCommand::Sources;
    let sinks = ShowCommand::Sinks;
    let queries = ShowCommand::Queries;
    let mvs = ShowCommand::MaterializedViews;

    assert_eq!(sources, ShowCommand::Sources);
    assert_eq!(sinks, ShowCommand::Sinks);
    assert_eq!(queries, ShowCommand::Queries);
    assert_eq!(mvs, ShowCommand::MaterializedViews);
}

#[test]
fn test_show_command_clone() {
    let cmd = ShowCommand::Sources;
    let cloned = cmd.clone();
    assert_eq!(cmd, cloned);
}

#[test]
fn test_drop_source_statement() {
    let stmt = StreamingStatement::DropSource {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
        if_exists: true,
        cascade: false,
    };
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
        _ => panic!("Expected DropSource"),
    }
}

#[test]
fn test_drop_sink_statement() {
    let stmt = StreamingStatement::DropSink {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("output"))]),
        if_exists: false,
        cascade: false,
    };
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
        _ => panic!("Expected DropSink"),
    }
}

#[test]
fn test_drop_materialized_view_statement() {
    let stmt = StreamingStatement::DropMaterializedView {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("live_stats"))]),
        if_exists: true,
        cascade: true,
    };
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
        _ => panic!("Expected DropMaterializedView"),
    }
}

#[test]
fn test_show_statement() {
    let stmt = StreamingStatement::Show(ShowCommand::Sources);
    match stmt {
        StreamingStatement::Show(ShowCommand::Sources) => (),
        _ => panic!("Expected Show(Sources)"),
    }
}

#[test]
fn test_describe_statement() {
    let stmt = StreamingStatement::Describe {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
        extended: true,
    };
    match stmt {
        StreamingStatement::Describe { name, extended } => {
            assert_eq!(name.to_string(), "events");
            assert!(extended);
        }
        _ => panic!("Expected Describe"),
    }
}

#[test]
fn test_explain_statement() {
    // Build an inner Standard statement using sqlparser
    let dialect = sqlparser::dialect::GenericDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, "SELECT 1").unwrap();
    let inner = StreamingStatement::Standard(Box::new(stmts.into_iter().next().unwrap()));

    let stmt = StreamingStatement::Explain {
        statement: Box::new(inner),
        analyze: false,
    };
    match stmt {
        StreamingStatement::Explain { statement, .. } => {
            assert!(matches!(*statement, StreamingStatement::Standard(_)));
        }
        _ => panic!("Expected Explain"),
    }
}

#[test]
fn test_create_materialized_view_statement() {
    // Build a query statement using sqlparser
    let dialect = sqlparser::dialect::GenericDialect {};
    let stmts =
        sqlparser::parser::Parser::parse_sql(&dialect, "SELECT COUNT(*) FROM events").unwrap();
    let query = StreamingStatement::Standard(Box::new(stmts.into_iter().next().unwrap()));

    let stmt = StreamingStatement::CreateMaterializedView {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("live_stats"))]),
        query: Box::new(query),
        emit_clause: Some(EmitClause::OnWindowClose),
        or_replace: false,
        if_not_exists: true,
        query_sql: "SELECT COUNT(*) FROM events".to_string(),
    };
    match stmt {
        StreamingStatement::CreateMaterializedView {
            name,
            emit_clause,
            or_replace,
            if_not_exists,
            ..
        } => {
            assert_eq!(name.to_string(), "live_stats");
            assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
            assert!(!or_replace);
            assert!(if_not_exists);
        }
        _ => panic!("Expected CreateMaterializedView"),
    }
}

#[test]
fn test_insert_into_statement() {
    let stmt = StreamingStatement::InsertInto {
        table_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
        columns: vec![Ident::new("id"), Ident::new("name")],
        values: vec![vec![
            Expr::Value(sqlparser::ast::Value::Number("1".to_string(), false).into()),
            Expr::Value(sqlparser::ast::Value::SingleQuotedString("test".to_string()).into()),
        ]],
    };
    match stmt {
        StreamingStatement::InsertInto {
            table_name,
            columns,
            values,
        } => {
            assert_eq!(table_name.to_string(), "events");
            assert_eq!(columns.len(), 2);
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 2);
        }
        _ => panic!("Expected InsertInto"),
    }
}

#[test]
fn test_eowc_requires_watermark_helper() {
    // Watermark-dependent strategies
    assert!(EmitClause::OnWindowClose.requires_watermark());
    assert!(EmitClause::Final.requires_watermark());
    assert!(EmitClause::AfterWatermark.requires_watermark());

    // Non-watermark strategies
    assert!(!EmitClause::OnUpdate.requires_watermark());
    assert!(!EmitClause::Changes.requires_watermark());
    let periodic = EmitClause::Periodically {
        interval: Box::new(Expr::Identifier(Ident::new("5_SECONDS"))),
    };
    assert!(!periodic.requires_watermark());
}
