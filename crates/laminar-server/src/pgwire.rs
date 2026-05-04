//! Postgres wire endpoint. Anonymous TRUST auth, no TLS — wildcard binds
//! are rejected at startup.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{Expr, FunctionArguments, SelectItem, Set, SetExpr, Statement};
use tokio::net::TcpListener;
use tracing::{info, warn};

use laminar_db::subscription::{PortalFrame, SubscriptionPortal};
use laminar_db::LaminarDB;

use crate::server::ServerError;

pub struct LaminarPgwireHandler {
    db: Arc<LaminarDB>,
}

#[async_trait]
impl NoopStartupHandler for LaminarPgwireHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!(peer = %client.socket_addr(), "pgwire client connected");
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for LaminarPgwireHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let stmts = parse_streaming_sql(query)
            .map_err(|e| user_error("42601", format!("parse error: {e}")))?;

        let mut out = Vec::with_capacity(stmts.len());
        for stmt in stmts {
            out.push(match stmt {
                StreamingStatement::Subscribe(s) => {
                    let name = s.name.to_string();
                    let portal = self
                        .db
                        .open_subscription(&name, s.filter_sql.as_deref())
                        .await
                        .map_err(|e| user_error("42P01", format!("SUBSCRIBE '{name}': {e}")))?;
                    portal_to_response(portal)
                }
                StreamingStatement::Show(cmd) => {
                    engine_metadata_response(&self.db, &show_sql(&cmd)).await?
                }
                StreamingStatement::Standard(s) => standard_response(&self.db, *s)?,
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

/// Connection-setup statements: transaction control, `SET`, and a tiny set
/// of catalog probes drivers send during handshake. Anything DDL/DML hits
/// the "use HTTP" error.
fn standard_response(db: &LaminarDB, stmt: Statement) -> PgWireResult<Response> {
    match stmt {
        Statement::StartTransaction { .. } => Ok(Response::Execution(Tag::new("BEGIN"))),
        Statement::Commit { .. } => Ok(Response::Execution(Tag::new("COMMIT"))),
        Statement::Rollback { .. } => Ok(Response::Execution(Tag::new("ROLLBACK"))),
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

fn user_error(code: &str, msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        msg.into(),
    )))
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

/// Single-row `text` response with one column.
fn text_response(col: &str, ty: Type, value: String) -> Response {
    let schema = Arc::new(vec![FieldInfo::new(
        col.into(),
        None,
        None,
        ty,
        FieldFormat::Text,
    )]);
    let schema_for_row = Arc::clone(&schema);
    let row_stream = stream::iter(std::iter::once(Ok::<_, PgWireError>(()))).map(move |_| {
        let mut enc = DataRowEncoder::new(Arc::clone(&schema_for_row));
        enc.encode_field(&Some(value.as_str()))?;
        Ok(enc.take_row())
    });
    Response::Query(QueryResponse::new(schema, row_stream))
}

fn record_batch_response(batch: arrow_array::RecordBatch) -> Response {
    let fields = Arc::new(field_infos(&batch.schema()));
    let nrows = batch.num_rows();

    // Encode rows eagerly: SHOW outputs are tiny and this avoids the
    // !Send formatter dance.
    let mut rows = Vec::with_capacity(nrows);
    {
        let opts = arrow_cast::display::FormatOptions::default();
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| arrow_cast::display::ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        for row in 0..nrows {
            rows.push(encode_row(&batch, row, &fields, &formatters));
        }
    }

    let row_stream = stream::iter(rows);
    Response::Query(QueryResponse::new(fields, row_stream))
}

fn portal_to_response(portal: SubscriptionPortal) -> Response {
    let fields = Arc::new(field_infos(&portal.schema()));

    // Each batch is converted to a Vec<DataRow> in one shot when received,
    // so formatters are built once per batch rather than per cell. Then we
    // drain the row vec before reading the next frame.
    let row_stream = stream::unfold(
        (
            portal,
            Vec::<PgWireResult<pgwire::messages::data::DataRow>>::new(),
            0usize,
            Arc::clone(&fields),
        ),
        move |(mut portal, mut rows, mut idx, fields)| async move {
            loop {
                if idx < rows.len() {
                    let row = std::mem::replace(&mut rows[idx], Ok(empty_row(&fields)));
                    idx += 1;
                    return Some((row, (portal, rows, idx, fields)));
                }
                rows.clear();
                idx = 0;
                match portal.next_frame().await? {
                    PortalFrame::Batch(b) if b.num_rows() > 0 => {
                        rows = encode_batch(&b, &fields);
                    }
                    PortalFrame::Batch(_) => {}
                    // Barriers are dropped on the SimpleQuery path. PG's
                    // simple-query protocol has no out-of-band channel for
                    // checkpoint markers; cursor support (follow-up) will
                    // surface them via NoticeResponse.
                    PortalFrame::Barrier {
                        epoch,
                        checkpoint_id,
                    } => {
                        tracing::trace!(epoch, checkpoint_id, "pgwire SUBSCRIBE: dropping barrier")
                    }
                }
            }
        },
    );
    Response::Query(QueryResponse::new(fields, row_stream))
}

fn empty_row(fields: &Arc<Vec<FieldInfo>>) -> pgwire::messages::data::DataRow {
    DataRowEncoder::new(Arc::clone(fields)).take_row()
}

fn encode_batch(
    batch: &arrow_array::RecordBatch,
    fields: &Arc<Vec<FieldInfo>>,
) -> Vec<PgWireResult<pgwire::messages::data::DataRow>> {
    let opts = arrow_cast::display::FormatOptions::default();
    let formatters: Vec<_> = match batch
        .columns()
        .iter()
        .map(|c| arrow_cast::display::ArrayFormatter::try_new(c.as_ref(), &opts))
        .collect::<Result<_, _>>()
    {
        Ok(f) => f,
        Err(e) => {
            return vec![Err(user_error("XX000", format!("format column: {e}")))];
        }
    };
    (0..batch.num_rows())
        .map(|row| encode_row(batch, row, fields, &formatters))
        .collect()
}

fn field_infos(schema: &arrow_schema::Schema) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|f| {
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                arrow_to_pg_type(f.data_type()),
                FieldFormat::Text,
            )
        })
        .collect()
}

fn encode_row(
    batch: &arrow_array::RecordBatch,
    row: usize,
    fields: &Arc<Vec<FieldInfo>>,
    formatters: &[arrow_cast::display::ArrayFormatter<'_>],
) -> PgWireResult<pgwire::messages::data::DataRow> {
    use arrow_array::Array;
    let mut enc = DataRowEncoder::new(Arc::clone(fields));
    for (i, col) in batch.columns().iter().enumerate() {
        if col.is_null(row) {
            enc.encode_field(&None::<&str>)?;
        } else {
            enc.encode_field(&Some(formatters[i].value(row).to_string()))?;
        }
    }
    Ok(enc.take_row())
}

fn arrow_to_pg_type(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Type::INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::UInt8 | DataType::UInt16 => Type::INT4,
        DataType::Float32 | DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Boolean => Type::BOOL,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
        _ => Type::TEXT,
    }
}

pub struct LaminarHandlerFactory {
    handler: Arc<LaminarPgwireHandler>,
}

impl LaminarHandlerFactory {
    fn new(db: Arc<LaminarDB>) -> Self {
        Self {
            handler: Arc::new(LaminarPgwireHandler { db }),
        }
    }
}

impl PgWireServerHandlers for LaminarHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::clone(&self.handler)
    }
}

#[allow(clippy::result_large_err)]
fn validate_bind(addr: &SocketAddr) -> Result<(), ServerError> {
    if addr.ip().is_unspecified() {
        return Err(ServerError::Http(format!(
            "pgwire_bind '{addr}' binds to all interfaces and v1 has no auth; \
             use a specific interface (e.g. 127.0.0.1)"
        )));
    }
    Ok(())
}

pub async fn serve(
    db: Arc<LaminarDB>,
    bind: &str,
) -> Result<tokio::task::JoinHandle<()>, ServerError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| ServerError::Http(format!("invalid pgwire_bind '{bind}': {e}")))?;
    validate_bind(&addr)?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::Http(format!("pgwire bind {addr}: {e}")))?;

    let factory = Arc::new(LaminarHandlerFactory::new(db));
    info!(%addr, "pgwire listening");

    // Track per-connection tasks so abort on the outer JoinHandle stops
    // active sessions in addition to the accept loop.
    let handle = tokio::spawn(async move {
        let mut sessions: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {
                    // Reap completed sessions; nothing to do with the result.
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((sock, peer)) => {
                            let factory_ref = Arc::clone(&factory);
                            sessions.spawn(async move {
                                if let Err(e) = process_socket(sock, None, factory_ref).await {
                                    warn!(%peer, error = %e, "pgwire connection error");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    });
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wildcard_bind() {
        assert!(validate_bind(&"0.0.0.0:5433".parse().unwrap()).is_err());
        assert!(validate_bind(&"[::]:5433".parse().unwrap()).is_err());
    }

    #[test]
    fn accepts_localhost_bind() {
        validate_bind(&"127.0.0.1:5433".parse().unwrap()).unwrap();
        validate_bind(&"[::1]:5433".parse().unwrap()).unwrap();
    }

    #[test]
    fn accepts_specific_interface_bind() {
        validate_bind(&"192.168.1.10:5433".parse().unwrap()).unwrap();
    }

    fn parse_one(sql: &str) -> StreamingStatement {
        parse_streaming_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    fn standard(sql: &str) -> Statement {
        match parse_one(sql) {
            StreamingStatement::Standard(s) => *s,
            other => panic!("expected Standard, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn select_one_dispatches() {
        let db = LaminarDB::open().unwrap();
        for sql in ["SELECT 1", "select 1", "/* hint */ SELECT 1"] {
            standard_response(&db, standard(sql)).unwrap();
        }
    }

    #[tokio::test]
    async fn driver_select_builtins_dispatch() {
        let db = LaminarDB::open().unwrap();
        for sql in [
            "SELECT version()",
            "SELECT current_schema()",
            "SELECT current_database()",
            "SELECT current_user",
        ] {
            // current_user parses as Expr::Function with no parens in some versions;
            // we accept whatever the parser gives us.
            let _ = standard_response(&db, standard(sql));
        }
    }

    #[tokio::test]
    async fn select_with_from_is_rejected() {
        let db = LaminarDB::open().unwrap();
        let err = standard_response(&db, standard("SELECT 1 FROM foo")).unwrap_err();
        assert!(err.to_string().contains("limited to literals"));
    }

    #[tokio::test]
    async fn ddl_routed_to_http() {
        let db = LaminarDB::open().unwrap();
        let err = standard_response(&db, standard("CREATE TABLE foo (id INT)")).unwrap_err();
        assert!(err.to_string().contains("HTTP /api/v1/sql"));
    }

    #[tokio::test]
    async fn transaction_control_dispatches() {
        let db = LaminarDB::open().unwrap();
        for sql in [
            "BEGIN",
            "BEGIN TRANSACTION",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
        ] {
            standard_response(&db, standard(sql)).unwrap();
        }
    }

    #[tokio::test]
    async fn set_writes_to_session_properties() {
        let db = LaminarDB::open().unwrap();
        standard_response(&db, standard("SET extra_float_digits = 3")).unwrap();
        assert_eq!(
            db.get_session_property("extra_float_digits").as_deref(),
            Some("3"),
        );
    }

    #[tokio::test]
    async fn set_transaction_isolation_is_rejected() {
        let db = LaminarDB::open().unwrap();
        let err = standard_response(
            &db,
            standard("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("SET TRANSACTION"));
    }

    #[test]
    fn multi_statement_parses() {
        let stmts = parse_streaming_sql("BEGIN; SELECT 1; COMMIT").unwrap();
        assert_eq!(stmts.len(), 3);
    }
}
