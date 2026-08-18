//! PgWire SQL dispatch: simple and extended query handling, statement-family
//! responses, and SQLSTATE error mapping.
//!
//! COMPAT: every SQLSTATE and error message here is the observable wire
//! contract; the rejection precedence (SUBSCRIBE before dispatch, aborted
//! transaction before execution) must not change.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use sqlparser::ast::{Expr, FunctionArguments, SelectItem, Set, SetExpr, Statement};

use laminar_db::subscription::SubscribeStart;
use laminar_db::LaminarDB;

use super::cursor::{
    fetch_direction_count, handle_close, handle_declare_cursor, handle_fetch, ConnState,
};
use super::encoding::{record_batch_response, text_response};
use super::session::LaminarPgwireHandler;
use super::subscription::{
    ensure_cached_subscription_schema, subscription_field_infos, subscription_open_error,
    subscription_query_response, validate_subscription_result_format, validate_subscription_schema,
    SUBSCRIPTION_METADATA_COLUMNS,
};

/// SQLSTATE-tagged user-facing error.
pub(super) fn user_error(code: &str, msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        msg.into(),
    )))
}

#[async_trait]
impl SimpleQueryHandler for LaminarPgwireHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let state = self.conn_state(client);
        state.prune_dead();
        let mut in_transaction = !matches!(
            client.transaction_status(),
            pgwire::messages::response::TransactionStatus::Idle
        );
        let mut failed_transaction = matches!(
            client.transaction_status(),
            pgwire::messages::response::TransactionStatus::Error
        );
        let stmts = parse_streaming_sql(query)
            .map_err(|e| user_error("42601", format!("parse error: {e}")))?;

        if stmts
            .iter()
            .any(|s| matches!(s, StreamingStatement::Subscribe(_)))
        {
            return Err(user_error(
                "0A000",
                "continuous pgwire SUBSCRIBE is not supported; use WebSocket or a bounded portal/cursor fetch",
            ));
        }

        let mut out = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            out.push(match stmt {
                StreamingStatement::Subscribe(_) => unreachable!("rejected before dispatch"),
                StreamingStatement::Show(cmd) => {
                    engine_metadata_response(&self.db, &show_sql(&cmd)).await?
                }
                StreamingStatement::DeclareCursorForSubscribe {
                    name, subscribe, ..
                } => {
                    if !in_transaction {
                        return Err(user_error(
                            "25001",
                            "subscription cursors require an explicit transaction",
                        ));
                    }
                    handle_declare_cursor(&self.db, &state, &name.value, *subscribe).await?
                }
                StreamingStatement::Standard(s) => {
                    let starts_transaction = matches!(&*s, Statement::StartTransaction { .. });
                    let ends_transaction =
                        matches!(&*s, Statement::Commit { .. } | Statement::Rollback { .. });
                    if failed_transaction && !ends_transaction {
                        return Err(user_error(
                            "25P02",
                            "current transaction is aborted; commands are ignored until ROLLBACK",
                        ));
                    }
                    let response =
                        standard_or_cursor_response(&self.db, &state, *s, in_transaction)?;
                    if starts_transaction {
                        in_transaction = true;
                    } else if ends_transaction {
                        in_transaction = false;
                        failed_transaction = false;
                    }
                    response
                }
                other => {
                    return Err(user_error(
                        "0A000",
                        format!("not supported on pgwire (use HTTP /api/v1/sql): {other:?}"),
                    ));
                }
            });
        }
        Ok(out)
    }
}

/// Wraps the original `standard_response` and intercepts cursor / transaction
/// statements that need ConnState. Anything else falls through to the
/// existing handler unchanged.
fn standard_or_cursor_response(
    db: &LaminarDB,
    state: &ConnState,
    stmt: Statement,
    in_transaction: bool,
) -> PgWireResult<Response> {
    match stmt {
        Statement::StartTransaction { .. } => Ok(Response::TransactionStart(Tag::new("BEGIN"))),
        Statement::Commit { .. } => {
            state.drop_all();
            Ok(Response::TransactionEnd(Tag::new("COMMIT")))
        }
        Statement::Rollback { .. } => {
            state.drop_all();
            Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
        }
        Statement::Fetch {
            ref name,
            ref direction,
            ..
        } => {
            if !in_transaction {
                return Err(user_error(
                    "25001",
                    "FETCH requires an explicit transaction",
                ));
            }
            let target = fetch_direction_count(direction)?;
            handle_fetch(state, &name.value, target)
        }
        Statement::Close { ref cursor } => {
            if !in_transaction {
                return Err(user_error(
                    "25001",
                    "CLOSE requires an explicit transaction",
                ));
            }
            handle_close(state, cursor)
        }
        Statement::Declare { .. } => Err(user_error(
            "0A000",
            "DECLARE on pgwire only supports CURSOR FOR SUBSCRIBE …",
        )),
        other => standard_response(db, other),
    }
}

/// Connection-setup statements: transaction control, `SET`, and a tiny set
/// of catalog probes drivers send during handshake. Anything DDL/DML hits
/// the "use HTTP" error.
pub(super) fn standard_response(db: &LaminarDB, stmt: Statement) -> PgWireResult<Response> {
    match stmt {
        Statement::StartTransaction { .. } => Ok(Response::TransactionStart(Tag::new("BEGIN"))),
        Statement::Commit { .. } => Ok(Response::TransactionEnd(Tag::new("COMMIT"))),
        Statement::Rollback { .. } => Ok(Response::TransactionEnd(Tag::new("ROLLBACK"))),
        Statement::Set(s) => apply_set(db, s),
        Statement::Query(q) => driver_select_response(*q),
        Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::CreateTable { .. }
        | Statement::CreateView { .. }
        | Statement::Drop { .. } => Err(user_error(
            "0A000",
            "DDL/DML is not supported on pgwire; use HTTP /api/v1/sql",
        )),
        other => Err(user_error(
            "0A000",
            format!("not supported on pgwire: {other}"),
        )),
    }
}

/// Handle the `SELECT`s drivers issue at connect time. Single literal,
/// `SELECT version()`, and `SELECT current_schema()` are answered inline.
/// Anything else is rejected — real queries belong on `/api/v1/sql`.
fn driver_select_response(query: sqlparser::ast::Query) -> PgWireResult<Response> {
    let SetExpr::Select(select) = *query.body else {
        return Err(unsupported_select());
    };
    if select.projection.len() != 1 || !select.from.is_empty() || select.selection.is_some() {
        return Err(unsupported_select());
    }
    let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
        return Err(unsupported_select());
    };

    match expr {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => {
                let parsed: i32 = n.parse().map_err(|_| unsupported_select())?;
                Ok(text_response("?column?", Type::INT4, parsed.to_string()))
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Ok(text_response("?column?", Type::VARCHAR, s.clone()))
            }
            _ => Err(unsupported_select()),
        },
        Expr::Function(func) => {
            // Only no-arg builtins. `func.args` is `FunctionArguments::None`
            // for `func()`, the only shape we accept.
            if !matches!(func.args, FunctionArguments::List(ref a) if a.args.is_empty())
                && !matches!(func.args, FunctionArguments::None)
            {
                return Err(unsupported_select());
            }
            let name = func.name.to_string().to_ascii_lowercase();
            let (col, ty, value) = match name.as_str() {
                "version" | "pg_catalog.version" => (
                    "version",
                    Type::VARCHAR,
                    format!("LaminarDB {} on pgwire", env!("CARGO_PKG_VERSION")),
                ),
                "current_schema" | "pg_catalog.current_schema" => {
                    ("current_schema", Type::VARCHAR, "public".to_string())
                }
                "current_database" | "pg_catalog.current_database" => {
                    ("current_database", Type::VARCHAR, "laminar".to_string())
                }
                "current_user" | "session_user" | "user" => {
                    ("current_user", Type::VARCHAR, "laminar".to_string())
                }
                _ => return Err(unsupported_select()),
            };
            Ok(text_response(col, ty, value))
        }
        _ => Err(unsupported_select()),
    }
}

fn unsupported_select() -> PgWireError {
    user_error(
        "0A000",
        "pgwire SELECT is limited to literals and connect-time builtins; use HTTP /api/v1/sql",
    )
}

/// `SET` handling. We thread plain `SET name = value` to the engine's
/// session-property store, and refuse `SET TRANSACTION`-class statements
/// since we don't honor isolation levels.
fn apply_set(db: &LaminarDB, set: Set) -> PgWireResult<Response> {
    match set {
        Set::SingleAssignment {
            variable, values, ..
        } => {
            let key = variable.to_string();
            let value = values
                .first()
                .map(ToString::to_string)
                .unwrap_or_default()
                .trim_matches('\'')
                .to_string();
            db.set_session_property(&key, &value);
            Ok(Response::Execution(Tag::new("SET")))
        }
        // Refuse anything that implies semantics we do not provide.
        Set::SetTransaction { .. } => Err(user_error(
            "0A000",
            "SET TRANSACTION is not supported (no transactional semantics)",
        )),
        // Lenient pass-through for the harmless catalog-style SETs drivers
        // issue (NAMES, TIME ZONE, ROLE...). We don't honor them, but failing
        // the connection is worse than silently accepting.
        _ => Ok(Response::Execution(Tag::new("SET"))),
    }
}

/// Reconstruct a single SHOW statement from the parsed variant. Used by the
/// pgwire dispatcher so a multi-statement query (`SHOW SOURCES; SHOW SINKS`)
/// re-executes only the matching statement, not the whole input string.
fn show_sql(cmd: &ShowCommand) -> String {
    match cmd {
        ShowCommand::Sources => "SHOW SOURCES".into(),
        ShowCommand::Sinks => "SHOW SINKS".into(),
        ShowCommand::Queries => "SHOW QUERIES".into(),
        ShowCommand::MaterializedViews => "SHOW MATERIALIZED VIEWS".into(),
        ShowCommand::Streams => "SHOW STREAMS".into(),
        ShowCommand::Tables => "SHOW TABLES".into(),
        ShowCommand::CheckpointStatus => "SHOW CHECKPOINT STATUS".into(),
        ShowCommand::CreateSource { name } => format!("SHOW CREATE SOURCE {name}"),
        ShowCommand::CreateSink { name } => format!("SHOW CREATE SINK {name}"),
    }
}

/// Run a SHOW through the engine and stream its `RecordBatch` to the wire.
async fn engine_metadata_response(db: &LaminarDB, sql: &str) -> PgWireResult<Response> {
    use laminar_db::ExecuteResult;
    let result = db
        .execute(sql)
        .await
        .map_err(|e| user_error("XX000", e.to_string()))?;
    let ExecuteResult::Metadata(batch) = result else {
        return Err(user_error("XX000", "SHOW did not return metadata"));
    };
    Ok(record_batch_response(batch))
}

/// Parsed statement carried through `Parse` → `Bind` → `Execute`.
#[derive(Clone, Debug)]
pub enum LaminarStmt {
    /// `SUBSCRIBE` with its schema resolved at parse time so `Describe` can
    /// answer before the portal is bound.
    Subscribe {
        name: String,
        filter_sql: Option<String>,
        as_of_epoch: Option<u64>,
        schema: arrow_schema::SchemaRef,
    },
    Show(ShowCommand),
    Standard(Box<Statement>),
}

/// Resolves SQL to `LaminarStmt`, looking up stream schemas against the
/// live `LaminarDB` so the extended-query `Describe` returns columns
/// without running the query.
#[derive(Clone)]
pub struct LaminarQueryParser {
    db: Arc<LaminarDB>,
}

#[async_trait]
impl QueryParser for LaminarQueryParser {
    type Statement = LaminarStmt;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut stmts = parse_streaming_sql(sql)
            .map_err(|e| user_error("42601", format!("parse error: {e}")))?;
        let stmt = stmts
            .pop()
            .ok_or_else(|| user_error("42601", "empty statement"))?;
        if !stmts.is_empty() {
            return Err(user_error(
                "42601",
                "extended query: multiple statements per Parse are not supported",
            ));
        }

        match stmt {
            StreamingStatement::Subscribe(s) => {
                let name = s.name.to_string();
                let schema = self.db.lookup_subscription_schema(&name).ok_or_else(|| {
                    user_error("42P01", format!("SUBSCRIBE '{name}': stream not found"))
                })?;
                validate_subscription_schema(&schema)?;
                Ok(LaminarStmt::Subscribe {
                    name,
                    filter_sql: s.filter_sql,
                    as_of_epoch: s.as_of_epoch,
                    schema,
                })
            }
            StreamingStatement::Show(cmd) => Ok(LaminarStmt::Show(cmd)),
            StreamingStatement::Standard(s) => Ok(LaminarStmt::Standard(s)),
            other => Err(user_error(
                "0A000",
                format!("not supported on pgwire (use HTTP /api/v1/sql): {other:?}"),
            )),
        }
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // SUBSCRIBE has no `$N` placeholders.
        Ok(Vec::new())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // SHOW and Standard are tiny single-row outputs whose schema only
        // materialises after execution; clients see it on Execute's
        // RowDescription instead.
        match stmt {
            LaminarStmt::Subscribe { schema, .. } => {
                if let Some(format) = column_format {
                    validate_subscription_result_format(
                        format,
                        schema.fields().len() + SUBSCRIPTION_METADATA_COLUMNS,
                    )?;
                }
                Ok(subscription_field_infos(schema, column_format))
            }
            LaminarStmt::Show(_) | LaminarStmt::Standard(_) => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for LaminarPgwireHandler {
    type Statement = LaminarStmt;
    type QueryParser = LaminarQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(LaminarQueryParser {
            db: Arc::clone(&self.db),
        })
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match &portal.statement.statement {
            LaminarStmt::Subscribe {
                name,
                filter_sql,
                as_of_epoch,
                schema,
            } => {
                if max_rows == 0 {
                    return Err(user_error(
                        "0A000",
                        "unbounded pgwire SUBSCRIBE is not supported; Execute must request a positive row count",
                    ));
                }
                validate_subscription_result_format(
                    &portal.result_column_format,
                    schema.fields().len() + SUBSCRIPTION_METADATA_COLUMNS,
                )?;
                let start = match as_of_epoch {
                    Some(n) => SubscribeStart::AsOfEpoch(*n),
                    None => SubscribeStart::Tail,
                };
                let sub = self
                    .db
                    .open_subscription(name, filter_sql.as_deref(), start)
                    .await
                    .map_err(|error| subscription_open_error(name, error))?;
                ensure_cached_subscription_schema(schema, &sub.schema())?;
                Ok(subscription_query_response(
                    sub,
                    Some(&portal.result_column_format),
                ))
            }
            LaminarStmt::Show(cmd) => engine_metadata_response(&self.db, &show_sql(cmd)).await,
            LaminarStmt::Standard(s) => standard_response(&self.db, *s.clone()),
        }
    }
}
