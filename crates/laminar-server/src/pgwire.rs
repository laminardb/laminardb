//! Postgres wire endpoint. Trust by default; MD5 with `pgwire_users`;
//! TLS with `pgwire_tls_cert` + `pgwire_tls_key`. Non-loopback binds
//! require authenticated users, TLS, and `pgwire_allow_remote = true`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use laminar_sql::parser::{
    parse_streaming_sql, ShowCommand, StreamingStatement, SubscribeStatement,
};
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::cancel::DefaultCancelHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, ConnectionManager, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{
    CloseCursor, Expr, FetchDirection, FunctionArguments, SelectItem, Set, SetExpr, Statement,
    Value as AstValue,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};

use laminar_db::subscription::{
    PortalFrame, SubscribeStart, SubscriptionFrameLease, SubscriptionPortal,
};
use laminar_db::LaminarDB;

use crate::config::Secret;
use crate::server::ServerError;

const SUBSCRIPTION_FETCH_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
const SUBSCRIPTION_MAX_FETCH_ROWS: u64 = 1024;
const SUBSCRIPTION_KIND_COLUMN: &str = "__laminar_kind";
const SUBSCRIPTION_EPOCH_COLUMN: &str = "__laminar_epoch";
const SUBSCRIPTION_CHECKPOINT_COLUMN: &str = "__laminar_checkpoint_id";
const SUBSCRIPTION_LOG_SEQUENCE_COLUMN: &str = "__laminar_log_sequence";
const SUBSCRIPTION_ROW_INDEX_COLUMN: &str = "__laminar_row_index";
const SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN: &str = "__laminar_through_sequence";
const SUBSCRIPTION_METADATA_COLUMNS: usize = 6;
const MAX_PENDING_PGWIRE_HANDSHAKES: usize = 64;

pub struct LaminarPgwireHandler {
    db: Arc<LaminarDB>,
    connection_manager: Arc<ConnectionManager>,
}

impl LaminarPgwireHandler {
    fn new(db: Arc<LaminarDB>, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            db,
            connection_manager,
        }
    }

    fn conn_state<C: ClientInfo>(&self, client: &C) -> Arc<ConnState> {
        client
            .session_extensions()
            .get_or_insert_with(ConnState::default)
    }
}

#[async_trait]
impl NoopStartupHandler for LaminarPgwireHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }

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

async fn open_portal_for_subscribe(
    db: &LaminarDB,
    s: &SubscribeStatement,
) -> PgWireResult<SubscriptionPortal> {
    let name = s.name.to_string();
    let start = match s.as_of_epoch {
        Some(n) => SubscribeStart::AsOfEpoch(n),
        None => SubscribeStart::Tail,
    };
    let portal = db
        .open_subscription(&name, s.filter_sql.as_deref(), start)
        .await
        .map_err(|error| subscription_open_error(&name, error))?;
    validate_subscription_schema(&portal.schema())?;
    Ok(portal)
}

fn subscription_open_error(name: &str, error: laminar_db::DbError) -> PgWireError {
    let code = match &error {
        laminar_db::DbError::StreamNotFound(_) => "42P01",
        laminar_db::DbError::Unsupported(_) => "0A000",
        laminar_db::DbError::InvalidOperation(_)
        | laminar_db::DbError::SubscriptionReplayPruned { .. }
        | laminar_db::DbError::SubscriptionEpochNotCommitted { .. } => "22023",
        laminar_db::DbError::Pipeline(_) => "53300",
        laminar_db::DbError::Sql(_)
        | laminar_db::DbError::SqlParse(_)
        | laminar_db::DbError::DataFusion(_)
        | laminar_db::DbError::QueryPipeline { .. } => "42601",
        _ => "XX000",
    };
    user_error(code, format!("SUBSCRIBE '{name}': {error}"))
}

/// Wrap a `SubscriptionPortal` in a pgwire `Response::Query` so the
/// framework can chunk via `Execute(max_rows)` and emit PortalSuspended
/// automatically. Used by the chunked extended-query path.
fn subscription_query_response(
    portal: SubscriptionPortal,
    result_format: Option<&Format>,
) -> Response {
    use futures::stream;
    let schema = portal.schema();
    let fields = Arc::new(subscription_field_infos(&schema, result_format));
    struct State {
        portal: SubscriptionPortal,
        fields: Arc<Vec<FieldInfo>>,
        batch: Option<BatchCursor>,
        data_columns: usize,
        failed: bool,
    }
    let init = State {
        portal,
        fields: Arc::clone(&fields),
        batch: None,
        data_columns: schema.fields().len(),
        failed: false,
    };
    let row_stream = stream::unfold(init, move |mut s| async move {
        loop {
            if s.failed {
                return None;
            }
            if let Some(batch) = s.batch.as_mut() {
                if let Some(row) = batch.next_row(&s.fields) {
                    let failed = row.is_err();
                    let exhausted = batch.is_exhausted();
                    if failed {
                        s.failed = true;
                    }
                    if failed || exhausted {
                        s.batch = None;
                    }
                    return Some((row, s));
                }
                s.batch = None;
            }
            match s.portal.next_frame().await {
                None => return None,
                Some(PortalFrame::Batch {
                    batch,
                    sequence,
                    lease,
                }) if batch.num_rows() > 0 => {
                    s.batch = Some(BatchCursor::new(batch, sequence, lease));
                }
                Some(PortalFrame::Batch { .. }) => {}
                Some(PortalFrame::Barrier {
                    sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                }) => {
                    let row = encode_subscription_progress_row(
                        &s.fields,
                        s.data_columns,
                        sequence,
                        epoch,
                        checkpoint_id,
                        through_sequence,
                    );
                    if row.is_err() {
                        s.failed = true;
                    }
                    return Some((row, s));
                }
                Some(PortalFrame::Lagged(n)) => {
                    let err = user_error(
                        "54000",
                        format!("subscription lagged: skipped {n} messages, terminating"),
                    );
                    s.failed = true;
                    return Some((Err(err), s));
                }
                Some(PortalFrame::Error { message }) => {
                    let err = user_error("XX000", format!("subscription failed: {message}"));
                    s.failed = true;
                    return Some((Err(err), s));
                }
            }
        }
    });
    let mut resp = QueryResponse::new(fields, row_stream);
    resp.set_command_tag("SUBSCRIBE");
    Response::Query(resp)
}

/// State that shares the cursor's lifetime: the portal, the leftover-row
/// buffer, and the exhausted flag. Held by `Arc` so a row stream can keep
/// reading after `ConnState::get` returns.
struct CursorInner {
    state: TokioMutex<CursorState>,
    /// Flipped when the portal emits `None`, `Lagged`, or `Error` so the next
    /// command can reap the cursor.
    exhausted: AtomicBool,
}

struct CursorState {
    portal: SubscriptionPortal,
    batch: Option<BatchCursor>,
}

#[derive(Clone)]
struct ActiveCursor {
    inner: Arc<CursorInner>,
    schema: arrow_schema::SchemaRef,
}

#[derive(Default)]
struct ConnState {
    cursors: parking_lot::Mutex<HashMap<String, ActiveCursor>>,
}

impl ConnState {
    /// Cursor names follow PG identifier folding: unquoted → lowercase. We
    /// don't track `quote_style`, so quoted-mixed-case cursors collapse too —
    /// good enough for the `\set FETCH_COUNT` case this targets.
    fn key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn insert(&self, name: &str, cursor: ActiveCursor) {
        self.cursors.lock().insert(Self::key(name), cursor);
    }

    fn contains(&self, name: &str) -> bool {
        self.cursors.lock().contains_key(&Self::key(name))
    }

    fn get(&self, name: &str) -> Option<ActiveCursor> {
        self.cursors.lock().get(&Self::key(name)).cloned()
    }

    fn remove(&self, name: &str) -> bool {
        self.cursors.lock().remove(&Self::key(name)).is_some()
    }

    fn drop_all(&self) {
        self.cursors.lock().clear();
    }

    fn prune_dead(&self) {
        let mut cursors = self.cursors.lock();
        cursors.retain(|_, c| !c.inner.exhausted.load(Ordering::Acquire));
    }
}

/// Open a SUBSCRIBE behind a cursor name. Rejects with 42P03 if the name is
/// already in use on this connection (matches PG; user must `CLOSE` first).
async fn handle_declare_cursor(
    db: &LaminarDB,
    state: &ConnState,
    cursor_name: &str,
    subscribe: SubscribeStatement,
) -> PgWireResult<Response> {
    if state.contains(cursor_name) {
        return Err(user_error(
            "42P03",
            format!("cursor \"{cursor_name}\" already exists"),
        ));
    }
    let portal = open_portal_for_subscribe(db, &subscribe).await?;
    let schema = portal.schema();
    state.insert(
        cursor_name,
        ActiveCursor {
            inner: Arc::new(CursorInner {
                state: TokioMutex::new(CursorState {
                    portal,
                    batch: None,
                }),
                exhausted: AtomicBool::new(false),
            }),
            schema,
        },
    );
    Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
}

/// `FETCH NEXT` and bare `FETCH FORWARD` map to a single row, matching PG.
fn fetch_direction_count(dir: &FetchDirection) -> PgWireResult<FetchTarget> {
    match dir {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Ok(FetchTarget::Count(1)),
        FetchDirection::Count { limit } | FetchDirection::Forward { limit: Some(limit) } => {
            let count = value_to_u64(limit)?;
            if count > SUBSCRIPTION_MAX_FETCH_ROWS {
                return Err(user_error(
                    "22023",
                    format!(
                        "FETCH count exceeds the bounded subscription limit of {SUBSCRIPTION_MAX_FETCH_ROWS} rows"
                    ),
                ));
            }
            Ok(FetchTarget::Count(count))
        }
        FetchDirection::All | FetchDirection::ForwardAll => Err(user_error(
            "0A000",
            "FETCH ALL is not supported for subscriptions; request a positive bounded row count",
        )),
        FetchDirection::Prior
        | FetchDirection::First
        | FetchDirection::Last
        | FetchDirection::Absolute { .. }
        | FetchDirection::Relative { .. }
        | FetchDirection::Backward { .. }
        | FetchDirection::BackwardAll => Err(user_error(
            "0A000",
            "FETCH direction not supported (SUBSCRIBE cursors are forward-only): use FORWARD or NEXT",
        )),
    }
}

#[derive(Copy, Clone)]
enum FetchTarget {
    Count(u64),
}

fn value_to_u64(v: &AstValue) -> PgWireResult<u64> {
    match v {
        AstValue::Number(n, _) => n
            .parse::<u64>()
            .map_err(|_| user_error("22023", format!("invalid FETCH count: {n}"))),
        other => Err(user_error(
            "22023",
            format!("FETCH count must be an integer, got {other}"),
        )),
    }
}

fn handle_fetch(
    state: &ConnState,
    cursor_name: &str,
    target: FetchTarget,
) -> PgWireResult<Response> {
    let cursor = state
        .get(cursor_name)
        .ok_or_else(|| user_error("34000", format!("cursor \"{cursor_name}\" does not exist")))?;
    Ok(fetch_response(cursor, target))
}

fn handle_close(state: &ConnState, cursor: &CloseCursor) -> PgWireResult<Response> {
    match cursor {
        CloseCursor::All => {
            state.drop_all();
            Ok(Response::Execution(Tag::new("CLOSE CURSOR ALL")))
        }
        CloseCursor::Specific { name } => {
            if state.remove(&name.value) {
                Ok(Response::Execution(Tag::new("CLOSE CURSOR")))
            } else {
                Err(user_error(
                    "34000",
                    format!("cursor \"{}\" does not exist", name.value),
                ))
            }
        }
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
fn standard_response(db: &LaminarDB, stmt: Statement) -> PgWireResult<Response> {
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
    let fields = Arc::new(field_infos(&batch.schema(), None));
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

/// Strict-PG FETCH: blocks until `target` rows are produced, the portal exits,
/// or the subscription faults. Text format only; SimpleQuery has no binary.
/// A partially consumed Arrow batch remains on the cursor for the next FETCH.
fn fetch_response(cursor: ActiveCursor, target: FetchTarget) -> Response {
    let fields = Arc::new(subscription_field_infos(&cursor.schema, None));
    let data_columns = cursor.schema.fields().len();
    let FetchTarget::Count(remaining) = target;

    struct State {
        cursor: ActiveCursor,
        fields: Arc<Vec<FieldInfo>>,
        remaining: u64,
    }

    let init = State {
        cursor,
        fields: Arc::clone(&fields),
        remaining,
    };

    let row_stream = stream::unfold(init, move |mut s| async move {
        loop {
            if s.remaining == 0 {
                return None;
            }
            if s.cursor.inner.exhausted.load(Ordering::Acquire) {
                return None;
            }

            let mut cursor_state = s.cursor.inner.state.lock().await;
            if let Some(batch) = cursor_state.batch.as_mut() {
                if let Some(row) = batch.next_row(&s.fields) {
                    let failed = row.is_err();
                    if failed || batch.is_exhausted() {
                        cursor_state.batch = None;
                    }
                    if failed {
                        s.cursor.inner.exhausted.store(true, Ordering::Release);
                    }
                    drop(cursor_state);
                    if !failed {
                        s.remaining = s.remaining.saturating_sub(1);
                    }
                    return Some((row, s));
                }
                cursor_state.batch = None;
            }

            let next = match tokio::time::timeout(
                SUBSCRIPTION_FETCH_WAIT,
                cursor_state.portal.next_frame(),
            )
            .await
            {
                Ok(next) => next,
                Err(_) => {
                    drop(cursor_state);
                    return None;
                }
            };
            match next {
                None => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    return None;
                }
                Some(PortalFrame::Batch {
                    batch,
                    sequence,
                    lease,
                }) if batch.num_rows() > 0 => {
                    cursor_state.batch = Some(BatchCursor::new(batch, sequence, lease));
                }
                Some(PortalFrame::Batch { .. }) => {}
                Some(PortalFrame::Barrier {
                    sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                }) => {
                    let row = encode_subscription_progress_row(
                        &s.fields,
                        data_columns,
                        sequence,
                        epoch,
                        checkpoint_id,
                        through_sequence,
                    );
                    let failed = row.is_err();
                    drop(cursor_state);
                    if failed {
                        s.cursor.inner.exhausted.store(true, Ordering::Release);
                    } else {
                        s.remaining = s.remaining.saturating_sub(1);
                    }
                    return Some((row, s));
                }
                Some(PortalFrame::Lagged(n)) => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    let err = user_error(
                        "54000",
                        format!("subscription lagged: skipped {n} messages, terminating cursor"),
                    );
                    return Some((Err(err), s));
                }
                Some(PortalFrame::Error { message }) => {
                    drop(cursor_state);
                    s.cursor.inner.exhausted.store(true, Ordering::Release);
                    let err = user_error(
                        "XX000",
                        format!("subscription failed: {message}; terminating cursor"),
                    );
                    return Some((Err(err), s));
                }
            }
        }
    });
    Response::Query(QueryResponse::new(fields, row_stream))
}

struct BatchCursor {
    batch: arrow_array::RecordBatch,
    sequence: u64,
    row: usize,
    _lease: SubscriptionFrameLease,
}

impl BatchCursor {
    fn new(batch: arrow_array::RecordBatch, sequence: u64, lease: SubscriptionFrameLease) -> Self {
        Self {
            batch,
            sequence,
            row: 0,
            _lease: lease,
        }
    }

    fn next_row(
        &mut self,
        fields: &Arc<Vec<FieldInfo>>,
    ) -> Option<PgWireResult<pgwire::messages::data::DataRow>> {
        if self.row >= self.batch.num_rows() {
            return None;
        }
        let row = self.row;
        let encoded = encode_subscription_batch_row(&self.batch, row, self.sequence, fields);
        if encoded.is_ok() {
            self.row += 1;
        }
        Some(encoded)
    }

    fn is_exhausted(&self) -> bool {
        self.row >= self.batch.num_rows()
    }
}

fn encode_subscription_batch_row(
    batch: &arrow_array::RecordBatch,
    row: usize,
    sequence: u64,
    fields: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    let opts = arrow_cast::display::FormatOptions::default();
    let formatters: Vec<_> = match batch
        .columns()
        .iter()
        .map(|c| arrow_cast::display::ArrayFormatter::try_new(c.as_ref(), &opts))
        .collect::<Result<_, _>>()
    {
        Ok(f) => f,
        Err(e) => {
            return Err(user_error("XX000", format!("format column: {e}")));
        }
    };
    if fields.len() != batch.num_columns() + SUBSCRIPTION_METADATA_COLUMNS {
        return Err(user_error(
            "XX000",
            "subscription result schema does not match the emitted batch",
        ));
    }
    let mut enc = DataRowEncoder::new(Arc::clone(fields));
    for (i, col) in batch.columns().iter().enumerate() {
        let info = &fields[i];
        match info.format() {
            FieldFormat::Text => encode_field_text(&mut enc, col.as_ref(), row, &formatters[i])?,
            FieldFormat::Binary => {
                encode_field_binary(&mut enc, col.as_ref(), row, info.name())?;
            }
        }
    }
    enc.encode_field(&Some("data"))?;
    enc.encode_field(&None::<&str>)?;
    enc.encode_field(&None::<&str>)?;
    let sequence = sequence.to_string();
    let row = row.to_string();
    enc.encode_field(&Some(sequence.as_str()))?;
    enc.encode_field(&Some(row.as_str()))?;
    enc.encode_field(&None::<&str>)?;
    Ok(enc.take_row())
}

/// Build pgwire `FieldInfo`s from an Arrow schema. `result_format` (from a
/// `Bind`) sets per-column text/binary; `None` defaults all-text.
fn field_infos(schema: &arrow_schema::Schema, result_format: Option<&Format>) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let format = result_format.map_or(FieldFormat::Text, |rf| safe_format_for(rf, i));
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                arrow_to_pg_type(f.data_type()),
                format,
            )
        })
        .collect()
}

fn subscription_field_infos(
    schema: &arrow_schema::Schema,
    result_format: Option<&Format>,
) -> Vec<FieldInfo> {
    let mut fields = field_infos(schema, result_format);
    for name in [
        SUBSCRIPTION_KIND_COLUMN,
        SUBSCRIPTION_EPOCH_COLUMN,
        SUBSCRIPTION_CHECKPOINT_COLUMN,
        SUBSCRIPTION_LOG_SEQUENCE_COLUMN,
        SUBSCRIPTION_ROW_INDEX_COLUMN,
        SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN,
    ] {
        let format =
            result_format.map_or(FieldFormat::Text, |rf| safe_format_for(rf, fields.len()));
        fields.push(FieldInfo::new(
            name.to_string(),
            None,
            None,
            Type::VARCHAR,
            format,
        ));
    }
    fields
}

fn safe_format_for(format: &Format, index: usize) -> FieldFormat {
    match format {
        Format::UnifiedText => FieldFormat::Text,
        Format::UnifiedBinary => FieldFormat::Binary,
        Format::Individual(codes) => codes
            .get(index)
            .copied()
            .map(FieldFormat::from)
            .unwrap_or(FieldFormat::Text),
    }
}

fn validate_subscription_result_format(format: &Format, columns: usize) -> PgWireResult<()> {
    if let Format::Individual(codes) = format {
        if codes.len() != columns {
            return Err(user_error(
                "08P01",
                format!(
                    "Bind supplied {} result format codes for a {columns}-column subscription",
                    codes.len()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_subscription_schema(schema: &arrow_schema::Schema) -> PgWireResult<()> {
    if let Some(field) = schema
        .fields()
        .iter()
        .find(|field| field.name().to_ascii_lowercase().starts_with("__laminar_"))
    {
        return Err(user_error(
            "42701",
            format!(
                "subscription column '{}' uses the reserved __laminar_ prefix",
                field.name()
            ),
        ));
    }
    Ok(())
}

fn encode_subscription_progress_row(
    fields: &Arc<Vec<FieldInfo>>,
    data_columns: usize,
    sequence: u64,
    epoch: u64,
    checkpoint_id: u64,
    through_sequence: u64,
) -> PgWireResult<pgwire::messages::data::DataRow> {
    if fields.len() != data_columns + SUBSCRIPTION_METADATA_COLUMNS {
        return Err(user_error(
            "XX000",
            "subscription progress schema does not match the result type",
        ));
    }
    let mut enc = DataRowEncoder::new(Arc::clone(fields));
    for _ in 0..data_columns {
        enc.encode_field(&None::<&str>)?;
    }
    let epoch = epoch.to_string();
    let checkpoint_id = checkpoint_id.to_string();
    let sequence = sequence.to_string();
    let through_sequence = through_sequence.to_string();
    enc.encode_field(&Some("progress"))?;
    enc.encode_field(&Some(epoch.as_str()))?;
    enc.encode_field(&Some(checkpoint_id.as_str()))?;
    enc.encode_field(&Some(sequence.as_str()))?;
    enc.encode_field(&None::<&str>)?;
    enc.encode_field(&Some(through_sequence.as_str()))?;
    Ok(enc.take_row())
}

fn ensure_cached_subscription_schema(
    cached: &arrow_schema::Schema,
    current: &arrow_schema::Schema,
) -> PgWireResult<()> {
    if cached == current {
        Ok(())
    } else {
        Err(user_error("0A000", "cached result type changed"))
    }
}

fn encode_row(
    batch: &arrow_array::RecordBatch,
    row: usize,
    fields: &Arc<Vec<FieldInfo>>,
    formatters: &[arrow_cast::display::ArrayFormatter<'_>],
) -> PgWireResult<pgwire::messages::data::DataRow> {
    if fields.len() != batch.num_columns() || formatters.len() != batch.num_columns() {
        return Err(user_error(
            "XX000",
            "result schema does not match the emitted batch",
        ));
    }
    let mut enc = DataRowEncoder::new(Arc::clone(fields));
    for (i, col) in batch.columns().iter().enumerate() {
        let info = &fields[i];
        match info.format() {
            FieldFormat::Text => encode_field_text(&mut enc, col.as_ref(), row, &formatters[i])?,
            FieldFormat::Binary => encode_field_binary(&mut enc, col.as_ref(), row, info.name())?,
        }
    }
    Ok(enc.take_row())
}

fn encode_field_text(
    enc: &mut DataRowEncoder,
    col: &dyn arrow_array::Array,
    row: usize,
    formatter: &arrow_cast::display::ArrayFormatter<'_>,
) -> PgWireResult<()> {
    use arrow_schema::DataType;
    if col.is_null(row) {
        return enc.encode_field(&None::<&str>);
    }
    if matches!(col.data_type(), DataType::UInt64) {
        let values = col
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| user_error("XX000", "UInt64 column has an invalid Arrow array"))?;
        let value = values.value(row);
        let value = i64::try_from(value)
            .map_err(|_| user_error("22003", "UInt64 value exceeds PostgreSQL BIGINT"))?;
        return enc.encode_field(&Some(value.to_string()));
    }
    // A TEXT[] column must serialize as a Postgres array literal `{..}`, not
    // Arrow's `[..]` display, so text-mode clients parse it as an array.
    if matches!(col.data_type(), DataType::List(f) if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
    {
        return enc.encode_field(&Some(pg_text_array_literal(&list_text_elements(col, row))));
    }
    enc.encode_field(&Some(formatter.value(row).to_string()))
}

/// Owned elements of a `List<Utf8|LargeUtf8>` row, NULLs preserved.
fn list_text_elements(col: &dyn arrow_array::Array, row: usize) -> Vec<Option<String>> {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    let values = col.as_list::<i32>().value(row);
    if matches!(values.data_type(), DataType::LargeUtf8) {
        let s = values.as_string::<i64>();
        (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_owned()))
            .collect()
    } else {
        let s = values.as_string::<i32>();
        (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_owned()))
            .collect()
    }
}

/// Postgres `text[]` literal, e.g. `{"en","ja",NULL}`. Every element is quoted
/// (NULL excepted) so commas/braces/quotes in values are unambiguous.
fn pg_text_array_literal(elements: &[Option<String>]) -> String {
    let mut out = String::from("{");
    for (i, elem) in elements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match elem {
            None => out.push_str("NULL"),
            Some(v) => {
                out.push('"');
                for ch in v.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    out
}

/// Binary-encode a single Arrow value via `postgres-types` `ToSql`.
///
/// Coverage: Int{8,16,32,64}, UInt{8,16,32,64}, Float{32,64}, Bool,
/// Utf8/LargeUtf8, Timestamp (any unit, naive), Date32, Date64, and
/// `List<Utf8>` (as `text[]`). UInt64 values outside PostgreSQL BIGINT fail
/// with `22003`. Any other column type yields `0A000`.
fn encode_field_binary(
    enc: &mut DataRowEncoder,
    col: &dyn arrow_array::Array,
    row: usize,
    name: &str,
) -> PgWireResult<()> {
    use arrow_array::{cast::AsArray, types::*};
    use arrow_schema::DataType;

    if col.is_null(row) {
        return enc.encode_field(&None::<&str>);
    }

    // Pull the typed Arrow value and pass it to `DataRowEncoder`, which
    // calls `postgres-types::ToSql` for the wire format. The `as $cast`
    // arm widens a narrower Arrow int to the matching Postgres OID (see
    // `arrow_to_pg_type`); only lossless `From` casts go through here.
    macro_rules! prim {
        ($ty:ty as $cast:ty) => {
            enc.encode_field(&Some(<$cast>::from(col.as_primitive::<$ty>().value(row))))
        };
        ($ty:ty) => {
            enc.encode_field(&Some(col.as_primitive::<$ty>().value(row)))
        };
    }

    match col.data_type() {
        DataType::Int8 => prim!(Int8Type as i32),
        DataType::Int16 => prim!(Int16Type as i32),
        DataType::Int32 => prim!(Int32Type),
        DataType::Int64 => prim!(Int64Type),
        DataType::UInt8 => prim!(UInt8Type as i32),
        DataType::UInt16 => prim!(UInt16Type as i32),
        DataType::UInt32 => prim!(UInt32Type as i64),
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(row);
            let v = i64::try_from(v)
                .map_err(|_| user_error("22003", "UInt64 value exceeds PostgreSQL BIGINT"))?;
            enc.encode_field(&Some(v))
        }
        DataType::Float32 => prim!(Float32Type as f64),
        DataType::Float64 => prim!(Float64Type),
        DataType::Boolean => enc.encode_field(&Some(col.as_boolean().value(row))),
        DataType::Utf8 => enc.encode_field(&Some(col.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => enc.encode_field(&Some(col.as_string::<i64>().value(row))),
        DataType::Timestamp(unit, _tz) => {
            // Each unit has its own Arrow type — `PrimitiveArray<TimestampMicrosecondType>`
            // is *not* `PrimitiveArray<Int64Type>`, so the downcast must match the unit.
            use arrow_array::temporal_conversions::{
                timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
                timestamp_us_to_datetime,
            };
            use arrow_schema::TimeUnit;
            let (raw, dt) = match unit {
                TimeUnit::Second => {
                    let v = col.as_primitive::<TimestampSecondType>().value(row);
                    (v, timestamp_s_to_datetime(v))
                }
                TimeUnit::Millisecond => {
                    let v = col.as_primitive::<TimestampMillisecondType>().value(row);
                    (v, timestamp_ms_to_datetime(v))
                }
                TimeUnit::Microsecond => {
                    let v = col.as_primitive::<TimestampMicrosecondType>().value(row);
                    (v, timestamp_us_to_datetime(v))
                }
                TimeUnit::Nanosecond => {
                    let v = col.as_primitive::<TimestampNanosecondType>().value(row);
                    (v, timestamp_ns_to_datetime(v))
                }
            };
            let dt =
                dt.ok_or_else(|| user_error("22008", format!("timestamp out of range: {raw}")))?;
            enc.encode_field(&Some(dt))
        }
        DataType::Date32 => {
            let v = col.as_primitive::<Date32Type>().value(row);
            let dt = arrow_array::temporal_conversions::date32_to_datetime(v)
                .ok_or_else(|| user_error("22008", format!("DATE out of range: {v}")))?;
            enc.encode_field(&Some(dt.date()))
        }
        DataType::Date64 => {
            let v = col.as_primitive::<Date64Type>().value(row);
            let dt = arrow_array::temporal_conversions::date64_to_datetime(v)
                .ok_or_else(|| user_error("22008", format!("DATE out of range: {v}")))?;
            enc.encode_field(&Some(dt.date()))
        }
        DataType::List(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            // `postgres-types` encodes Vec<Option<String>> as the binary
            // text[] wire format (the column's OID is TEXT_ARRAY).
            enc.encode_field(&Some(list_text_elements(col, row)))
        }
        other => Err(user_error(
            "0A000",
            format!("binary format not supported for column '{name}' (type {other:?})"),
        )),
    }
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
        DataType::List(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Type::TEXT_ARRAY
        }
        _ => Type::TEXT,
    }
}

/// Per-call salt + stored credential for the MD5 challenge flow. The
/// stored value is either plaintext (legacy) or `md5<32-hex>`, the same
/// format Postgres' `pg_authid` uses, where the hex is `md5(password ‖
/// user)`. The pre-hashed form lets operators avoid plaintext at rest.
#[derive(Debug)]
struct LaminarAuthSource {
    users: Arc<HashMap<String, Secret>>,
}

/// If `stored` is a `pg_authid`-style pre-hash, return the inner hex
/// (the bit after the `md5` tag). Lowercase hex only; uppercase or
/// other lengths fall back to plaintext handling.
pub(crate) fn parse_pre_hashed_md5(stored: &str) -> Option<&str> {
    let inner = stored.strip_prefix("md5")?;
    if inner.len() == 32 && inner.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
        Some(inner)
    } else {
        None
    }
}

/// MD5 challenge response when only the inner hash is known: the client
/// sends `md5{hex(md5(inner_hex || salt))}` and the server precomputes
/// the same string for comparison.
fn outer_md5_challenge(inner_hex: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(inner_hex.as_bytes());
    hasher.update(salt);
    format!("md5{:x}", hasher.finalize())
}

#[async_trait]
impl AuthSource for LaminarAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().unwrap_or("");
        // Indistinguishable from a wrong-password failure: both branches must
        // surface the same wire error so a client can't probe which usernames
        // are configured. pgwire emits exactly this variant on bad password.
        let stored = self
            .users
            .get(user)
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_string()))?;
        let salt: [u8; 4] = rand::random();
        let expected = match parse_pre_hashed_md5(stored.expose()) {
            Some(inner_hex) => outer_md5_challenge(inner_hex, &salt),
            None => hash_md5_password(user, stored.expose(), &salt),
        };
        Ok(Password::new(Some(salt.to_vec()), expected.into_bytes()))
    }
}

type Md5Handler = Md5PasswordAuthStartupHandler<LaminarAuthSource, DefaultServerParameterProvider>;

/// Startup-phase dispatch. `Md5` requires password auth; `Trust` accepts any
/// connection. Selected once at listener startup based on whether
/// `pgwire_users` is non-empty.
enum StartupAuth {
    Trust(Arc<LaminarPgwireHandler>),
    Md5(Arc<Md5Handler>),
}

/// Permit held for the full authenticated-session lifetime through the
/// per-connection extension store.
struct SessionPermit {
    _permit: OwnedSemaphorePermit,
}

/// Admission wrapper created per accepted socket. The pending-handshake
/// permit protects TLS negotiation and startup decoding, then is released as
/// soon as the first valid Startup packet has been classified.
struct StartupAdmission {
    auth: Arc<StartupAuth>,
    sessions: Arc<Semaphore>,
    pending: parking_lot::Mutex<Option<OwnedSemaphorePermit>>,
    require_tls: bool,
}

#[async_trait]
impl StartupHandler for StartupAdmission {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if matches!(&message, PgWireFrontendMessage::Startup(_)) {
            // Classification is complete. CancelRequest never reaches this
            // handler and therefore never consumes a normal session slot.
            self.pending.lock().take();

            if self.require_tls && !client.is_secure() {
                return Err(fatal_startup_error(
                    "08004",
                    "TLS is required for this pgwire listener",
                ));
            }

            let permit = Arc::clone(&self.sessions)
                .try_acquire_owned()
                .map_err(|_| fatal_startup_error("53300", "too many pgwire connections"))?;
            client
                .session_extensions()
                .insert(SessionPermit { _permit: permit });
        }

        self.auth.on_startup(client, message).await
    }
}

fn fatal_startup_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".into(),
        code.into(),
        message.into(),
    )))
}

#[async_trait]
impl StartupHandler for StartupAuth {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            Self::Trust(h) => h.on_startup(client, message).await,
            Self::Md5(h) => h.on_startup(client, message).await,
        }
    }
}

struct LaminarHandlerFactory {
    handler: Arc<LaminarPgwireHandler>,
    startup: Arc<StartupAuth>,
    cancel: Arc<DefaultCancelHandler>,
    sessions: Arc<Semaphore>,
    require_tls: bool,
}

impl LaminarHandlerFactory {
    fn new(
        db: Arc<LaminarDB>,
        users: HashMap<String, Secret>,
        max_connections: usize,
        require_tls: bool,
    ) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new());
        let handler = Arc::new(LaminarPgwireHandler::new(
            db,
            Arc::clone(&connection_manager),
        ));
        let startup = if users.is_empty() {
            Arc::new(StartupAuth::Trust(Arc::clone(&handler)))
        } else {
            let auth = LaminarAuthSource {
                users: Arc::new(users),
            };
            let md5 = Md5PasswordAuthStartupHandler::new(
                Arc::new(auth),
                Arc::new(DefaultServerParameterProvider::default()),
            )
            .with_connection_manager(Arc::clone(&connection_manager));
            Arc::new(StartupAuth::Md5(Arc::new(md5)))
        };
        let cancel = Arc::new(DefaultCancelHandler::new(connection_manager));
        Self {
            handler,
            startup,
            cancel,
            sessions: Arc::new(Semaphore::new(max_connections)),
            require_tls,
        }
    }

    fn for_connection(&self, pending: OwnedSemaphorePermit) -> LaminarConnectionHandlers {
        LaminarConnectionHandlers {
            handler: Arc::clone(&self.handler),
            startup: Arc::new(StartupAdmission {
                auth: Arc::clone(&self.startup),
                sessions: Arc::clone(&self.sessions),
                pending: parking_lot::Mutex::new(Some(pending)),
                require_tls: self.require_tls,
            }),
            cancel: Arc::clone(&self.cancel),
        }
    }
}

struct LaminarConnectionHandlers {
    handler: Arc<LaminarPgwireHandler>,
    startup: Arc<StartupAdmission>,
    cancel: Arc<DefaultCancelHandler>,
}

impl PgWireServerHandlers for LaminarConnectionHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        Arc::clone(&self.cancel)
    }
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

pub struct TlsPaths<'a> {
    pub cert: &'a std::path::Path,
    pub key: &'a std::path::Path,
    pub min_version: TlsMinVersion,
    /// PEM bundle of CA roots; presence enables mTLS — every client must
    /// present a cert that chains to one of these roots.
    pub client_ca: Option<&'a std::path::Path>,
}

/// Owned counterpart to `TlsPaths` that the listener keeps for the
/// lifetime of `serve()` so the file watcher can rebuild the acceptor
/// without the original config still being in scope.
#[derive(Debug, Clone)]
struct TlsConfigPaths {
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    min_version: TlsMinVersion,
    client_ca: Option<std::path::PathBuf>,
}

impl TlsConfigPaths {
    fn from_paths(paths: &TlsPaths<'_>) -> Self {
        Self {
            cert: paths.cert.to_path_buf(),
            key: paths.key.to_path_buf(),
            min_version: paths.min_version,
            client_ca: paths.client_ca.map(|p| p.to_path_buf()),
        }
    }

    fn borrow(&self) -> TlsPaths<'_> {
        TlsPaths {
            cert: &self.cert,
            key: &self.key,
            min_version: self.min_version,
            client_ca: self.client_ca.as_deref(),
        }
    }
}

/// Live TLS acceptor + paths needed to rebuild it on cert rotation.
/// Reads on the accept path are a single mutex acquire and a cheap
/// `TlsAcceptor` clone; reloads are triggered by the file watcher.
pub struct TlsReloadState {
    paths: TlsConfigPaths,
    acceptor: parking_lot::Mutex<Arc<tokio_rustls::TlsAcceptor>>,
}

impl TlsReloadState {
    fn snapshot(&self) -> Arc<tokio_rustls::TlsAcceptor> {
        Arc::clone(&self.acceptor.lock())
    }
}

/// Rebuild the TLS acceptor from `state.paths` and atomically swap it in.
/// On any error the previous acceptor is left in place, so a bad rotation
/// (truncated file, expired cert) doesn't take TLS down.
#[allow(clippy::result_large_err)]
pub(crate) fn try_reload_tls(state: &TlsReloadState) -> Result<(), ServerError> {
    let new_acceptor = load_tls_acceptor(state.paths.borrow())?;
    *state.acceptor.lock() = Arc::new(new_acceptor);
    Ok(())
}

/// Watch the cert / key / client-CA files and call `try_reload_tls` after
/// debounced changes. Mirrors the pattern in `watcher.rs` (parent-dir
/// watch, debounce, then act). Runs until the channel closes; the caller
/// drives shutdown by aborting the task that owns this future.
async fn watch_tls_files(state: Arc<TlsReloadState>, debounce: std::time::Duration) {
    use crossfire::{mpsc, MTx};
    use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

    // Track raw + canonical paths so symlink-swap rotations and edits to
    // the symlink target both produce visible events.
    let mut raw_targets: Vec<std::path::PathBuf> = Vec::new();
    let mut canon_targets: Vec<std::path::PathBuf> = Vec::new();
    for path in [
        Some(state.paths.cert.clone()),
        Some(state.paths.key.clone()),
        state.paths.client_ca.clone(),
    ]
    .into_iter()
    .flatten()
    {
        match path.canonicalize() {
            Ok(canonical) => {
                canon_targets.push(canonical);
                raw_targets.push(path);
            }
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "pgwire TLS watcher: cannot canonicalize path; reload disabled",
                );
                return;
            }
        }
    }
    let mut dirs: Vec<std::path::PathBuf> = raw_targets
        .iter()
        .chain(canon_targets.iter())
        .filter_map(|p| p.parent().map(|d| d.to_path_buf()))
        .collect();
    dirs.sort();
    dirs.dedup();

    let (tx, rx) = mpsc::bounded_async::<()>(16);
    let blocking_tx: MTx<_> = tx.clone().into_blocking();
    let watch_raw = raw_targets.clone();
    let watch_canon = canon_targets.clone();

    let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
        move |result: Result<Event, notify::Error>| match result {
            Ok(event) => {
                let touched = event.paths.iter().any(|p| {
                    watch_raw.iter().any(|t| t == p)
                        || p.canonicalize()
                            .ok()
                            .as_ref()
                            .is_some_and(|c| watch_canon.contains(c))
                });
                if touched {
                    let _ = blocking_tx.send(());
                }
            }
            Err(e) => warn!(error = %e, "pgwire TLS watcher: notify error"),
        },
    ) {
        Ok(w) => w,
        Err(e) => {
            warn!(error = %e, "pgwire TLS watcher: failed to create watcher; reload disabled");
            return;
        }
    };

    for dir in &dirs {
        if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
            warn!(
                dir = %dir.display(),
                error = %e,
                "pgwire TLS watcher: failed to watch directory; reload disabled",
            );
            return;
        }
    }
    info!(
        files = ?raw_targets.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
        "pgwire TLS watcher started",
    );

    loop {
        if rx.recv().await.is_err() {
            return;
        }
        // Debounce: sleep then drain so a burst of inotify events
        // (cert + key written separately) coalesces into one reload.
        tokio::time::sleep(debounce).await;
        while rx.try_recv().is_ok() {}

        match try_reload_tls(&state) {
            Ok(()) => tracing::info!(
                target: "audit",
                event = "pgwire.tls_reload",
                outcome = "ok",
            ),
            Err(e) => tracing::warn!(
                target: "audit",
                event = "pgwire.tls_reload",
                outcome = "failed",
                error = %e,
                "pgwire TLS reload failed; previous certificate kept",
            ),
        }
    }
}

/// Minimum TLS protocol version accepted on the pgwire listener. rustls
/// already disables TLS 1.0/1.1; this narrows further when an operator
/// needs TLS 1.3 only.
#[derive(Clone, Copy, Debug)]
pub enum TlsMinVersion {
    V1_2,
    V1_3,
}

impl TlsMinVersion {
    pub(crate) fn from_config_str(s: &str) -> Option<Self> {
        match s {
            "1.2" => Some(Self::V1_2),
            "1.3" => Some(Self::V1_3),
            _ => None,
        }
    }

    fn versions(self) -> &'static [&'static tokio_rustls::rustls::SupportedProtocolVersion] {
        use tokio_rustls::rustls::version::{TLS12, TLS13};
        static BOTH: &[&tokio_rustls::rustls::SupportedProtocolVersion] = &[&TLS12, &TLS13];
        static ONLY_13: &[&tokio_rustls::rustls::SupportedProtocolVersion] = &[&TLS13];
        match self {
            Self::V1_2 => BOTH,
            Self::V1_3 => ONLY_13,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::V1_2 => "1.2",
            Self::V1_3 => "1.3",
        }
    }
}

/// Warn if the key file is group/other-readable.
#[cfg(unix)]
fn warn_if_key_world_readable(file: &std::fs::File, path: &std::path::Path) {
    use std::os::unix::fs::MetadataExt;
    if let Ok(meta) = file.metadata() {
        let mode = meta.mode();
        if mode & 0o077 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{:o}", mode & 0o777),
                "pgwire_tls_key permissions are too broad; tighten to 0600",
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_key_world_readable(_file: &std::fs::File, _path: &std::path::Path) {}

/// Rolling-window auth-failure count per peer IP.
#[derive(Debug, Default)]
struct FailureTracker {
    inner: parking_lot::Mutex<
        HashMap<std::net::IpAddr, std::collections::VecDeque<std::time::Instant>>,
    >,
}

impl FailureTracker {
    fn is_blocked(&self, ip: std::net::IpAddr, limit: u32, window: std::time::Duration) -> bool {
        if limit == 0 {
            return false;
        }
        let cutoff = std::time::Instant::now() - window;
        let mut inner = self.inner.lock();
        let Some(failures) = inner.get_mut(&ip) else {
            return false;
        };
        while failures.front().is_some_and(|t| *t < cutoff) {
            failures.pop_front();
        }
        let blocked = failures.len() >= limit as usize;
        if failures.is_empty() {
            inner.remove(&ip);
        }
        blocked
    }

    fn record_failure(&self, ip: std::net::IpAddr) {
        let mut inner = self.inner.lock();
        // When full, evict the entry whose newest failure is oldest.
        if !inner.contains_key(&ip) && inner.len() >= MAX_TRACKED_IPS {
            if let Some(oldest) = inner
                .iter()
                .min_by_key(|(_, q)| q.back().copied())
                .map(|(k, _)| *k)
            {
                inner.remove(&oldest);
            }
        }
        inner
            .entry(ip)
            .or_default()
            .push_back(std::time::Instant::now());
    }
}

const MAX_TRACKED_IPS: usize = 4096;

/// Stable audit code for a session's exit status.
fn classify_outcome(result: &Result<(), std::io::Error>) -> &'static str {
    match result {
        Ok(()) => "ok",
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("28P01") {
                "auth_failed"
            } else if msg.contains("HandshakeFailure")
                || msg.contains("rustls")
                || msg.contains("tls")
            {
                "tls_failed"
            } else {
                "error"
            }
        }
    }
}

/// Reject certs past `notAfter`; warn within 30 days.
#[allow(clippy::result_large_err)]
fn check_cert_expiry(
    der: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
    path: &std::path::Path,
) -> Result<(), ServerError> {
    use x509_parser::prelude::FromDer;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der.as_ref())
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_cert {}: {e}", path.display())))?;
    let now = x509_parser::time::ASN1Time::now();
    let not_after = cert.validity().not_after;
    if not_after < now {
        return Err(ServerError::Http(format!(
            "pgwire_tls_cert {} expired at {not_after}",
            path.display()
        )));
    }
    let remaining = not_after.to_datetime() - now.to_datetime();
    if remaining <= time::Duration::days(30) {
        warn!(
            path = %path.display(),
            expires_at = %not_after,
            "pgwire_tls_cert expires within 30 days; rotate before it lapses",
        );
    }
    Ok(())
}

/// Idempotent install of aws-lc-rs as rustls' default provider.
fn ensure_tls_provider() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
}

#[allow(clippy::result_large_err)]
fn load_tls_acceptor(paths: TlsPaths<'_>) -> Result<tokio_rustls::TlsAcceptor, ServerError> {
    use std::fs::File;
    use std::io::BufReader;

    ensure_tls_provider();

    let cert_file = File::open(paths.cert)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_cert: {e}")))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_cert: {e}")))?;
    if certs.is_empty() {
        return Err(ServerError::Http(format!(
            "pgwire_tls_cert {} contains no certificates",
            paths.cert.display()
        )));
    }
    for cert in &certs {
        check_cert_expiry(cert, paths.cert)?;
    }

    let key_file = File::open(paths.key)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_key: {e}")))?;
    warn_if_key_world_readable(&key_file, paths.key);
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_key: {e}")))?
        .ok_or_else(|| {
            ServerError::Http(format!(
                "pgwire_tls_key {} contains no private key",
                paths.key.display()
            ))
        })?;

    let builder = tokio_rustls::rustls::ServerConfig::builder_with_protocol_versions(
        paths.min_version.versions(),
    );
    let builder = match paths.client_ca {
        Some(ca_path) => {
            let verifier = build_client_cert_verifier(ca_path)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let server_config = builder
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::Http(format!("rustls server config: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
}

#[allow(clippy::result_large_err)]
fn build_client_cert_verifier(
    ca_path: &std::path::Path,
) -> Result<Arc<dyn tokio_rustls::rustls::server::danger::ClientCertVerifier>, ServerError> {
    use std::fs::File;
    use std::io::BufReader;
    use tokio_rustls::rustls::server::WebPkiClientVerifier;
    use tokio_rustls::rustls::RootCertStore;

    let file = File::open(ca_path)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_client_ca: {e}")))?;
    let mut roots = RootCertStore::empty();
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
        let cert =
            cert.map_err(|e| ServerError::Http(format!("parse pgwire_tls_client_ca: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| ServerError::Http(format!("invalid CA in pgwire_tls_client_ca: {e}")))?;
        added += 1;
    }
    if added == 0 {
        return Err(ServerError::Http(format!(
            "pgwire_tls_client_ca {} contains no certificates",
            ca_path.display()
        )));
    }
    WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| ServerError::Http(format!("build client-cert verifier: {e}")))
}

pub async fn serve(
    db: Arc<LaminarDB>,
    bind: &str,
    users: HashMap<String, Secret>,
    allow_remote: bool,
    tls: Option<TlsPaths<'_>>,
    max_connections: usize,
    max_auth_failures_per_min: u32,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), ServerError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| ServerError::Http(format!("invalid pgwire_bind '{bind}': {e}")))?;

    let auth_mode = if users.is_empty() { "trust" } else { "md5" };
    let is_remote_bind = !addr.ip().is_loopback();
    match (auth_mode, is_remote_bind, allow_remote) {
        ("trust", true, _) => {
            return Err(ServerError::Http(format!(
                "pgwire_bind '{addr}' is not loopback and pgwire_users is empty (trust auth); \
             configure pgwire_users + pgwire_allow_remote=true, or bind to 127.0.0.1"
            )))
        }
        ("md5", true, false) => {
            return Err(ServerError::Http(format!(
                "pgwire_bind '{addr}' is not loopback; set pgwire_allow_remote=true to opt in"
            )))
        }
        _ => {}
    }

    let tls_min_label = tls.as_ref().map(|p| p.min_version.label());
    let mtls_on = tls.as_ref().is_some_and(|p| p.client_ca.is_some());
    let tls_state: Option<Arc<TlsReloadState>> = match tls {
        Some(paths) => {
            let acceptor = load_tls_acceptor(TlsPaths {
                cert: paths.cert,
                key: paths.key,
                min_version: paths.min_version,
                client_ca: paths.client_ca,
            })?;
            Some(Arc::new(TlsReloadState {
                paths: TlsConfigPaths::from_paths(&paths),
                acceptor: parking_lot::Mutex::new(Arc::new(acceptor)),
            }))
        }
        None => None,
    };

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::Http(format!("pgwire bind {addr}: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| ServerError::Http(format!("pgwire local_addr: {e}")))?;
    let require_tls = !local_addr.ip().is_loopback() || mtls_on;
    if require_tls && tls_state.is_none() {
        return Err(ServerError::Http(format!(
            "pgwire listener '{local_addr}' requires pgwire_tls_cert + pgwire_tls_key"
        )));
    }

    let tls_mode = if tls_state.is_some() { "on" } else { "off" };
    let tls_min = tls_min_label.unwrap_or("-");
    let mtls = if mtls_on { "on" } else { "off" };
    if auth_mode == "trust" {
        warn!(
            addr = %local_addr,
            tls = tls_mode,
            tls_min,
            mtls,
            "pgwire listening with TRUST auth — any client reaching this address is admin",
        );
    } else {
        info!(
            addr = %local_addr,
            auth = auth_mode,
            tls = tls_mode,
            tls_min,
            mtls,
            "pgwire listening",
        );
    }

    // Track per-connection tasks so abort on the outer JoinHandle stops
    // active sessions in addition to the accept loop.
    let failures = Arc::new(FailureTracker::default());
    let factory = Arc::new(LaminarHandlerFactory::new(
        db,
        users,
        max_connections,
        require_tls,
    ));
    let pending_handshakes = Arc::new(Semaphore::new(MAX_PENDING_PGWIRE_HANDSHAKES));
    let watcher_state = tls_state.as_ref().map(Arc::clone);
    let watcher_disabled =
        std::env::var("LAMINAR_DISABLE_FILE_WATCH").is_ok_and(|v| v == "1" || v == "true");
    let handle = tokio::spawn(async move {
        let mut sessions: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        // Watcher in its own JoinSet so it doesn't count toward max_connections.
        let mut watcher_set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        if let (Some(state), false) = (watcher_state, watcher_disabled) {
            watcher_set.spawn(async move {
                watch_tls_files(state, std::time::Duration::from_millis(500)).await;
            });
        }
        loop {
            tokio::select! {
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {
                    // Reap completed sessions; nothing to do with the result.
                }
                Some(_) = watcher_set.join_next(), if !watcher_set.is_empty() => {}
                accepted = listener.accept() => {
                    match accepted {
                        Ok((sock, peer)) => {
                            let Ok(pending) = Arc::clone(&pending_handshakes).try_acquire_owned()
                            else {
                                tracing::info!(
                                    target: "audit",
                                    event = "pgwire.connection_rejected",
                                    peer = %peer,
                                    reason = "pending_handshake_limit",
                                    in_flight = MAX_PENDING_PGWIRE_HANDSHAKES,
                                );
                                drop(sock);
                                continue;
                            };
                            if failures.is_blocked(
                                peer.ip(),
                                max_auth_failures_per_min,
                                std::time::Duration::from_secs(60),
                            ) {
                                tracing::warn!(
                                    target: "audit",
                                    event = "pgwire.connection_rejected",
                                    peer = %peer,
                                    reason = "auth_failure_throttle",
                                );
                                drop(sock);
                                continue;
                            }
                            let handlers = factory.for_connection(pending);
                            // Snapshot the live acceptor so that an in-flight
                            // handshake completes against whatever cert was
                            // current when the socket was accepted, even if a
                            // hot-reload swaps it under us.
                            let tls_ref: Option<tokio_rustls::TlsAcceptor> =
                                tls_state.as_ref().map(|s| (*s.snapshot()).clone());
                            let failures_ref = Arc::clone(&failures);
                            let peer_str = peer.to_string();
                            tracing::info!(
                                target: "audit",
                                event = "pgwire.connection_accepted",
                                peer = %peer,
                                auth = auth_mode,
                                tls = tls_mode,
                            );
                            let peer_ip = peer.ip();
                            sessions.spawn(async move {
                                let result = process_socket(sock, tls_ref, handlers).await;
                                let outcome = classify_outcome(&result);
                                if outcome == "auth_failed" {
                                    failures_ref.record_failure(peer_ip);
                                }
                                tracing::info!(
                                    target: "audit",
                                    event = "pgwire.connection_closed",
                                    peer = %peer_str,
                                    outcome,
                                );
                                if let Err(e) = result {
                                    warn!(peer = %peer_str, error = %e, "pgwire connection error");
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
    Ok((local_addr, handle))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
