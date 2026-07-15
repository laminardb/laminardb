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
mod tests {
    use super::*;

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

    #[test]
    fn pg_text_array_literal_quotes_nulls_and_escapes() {
        assert_eq!(pg_text_array_literal(&[]), "{}");
        assert_eq!(
            pg_text_array_literal(&[Some("en".into()), Some("ja".into())]),
            r#"{"en","ja"}"#
        );
        assert_eq!(
            pg_text_array_literal(&[None, Some("x".into())]),
            r#"{NULL,"x"}"#
        );
        // Embedded quote and backslash are escaped, not left ambiguous.
        assert_eq!(
            pg_text_array_literal(&[Some("a\"b\\c".into())]),
            r#"{"a\"b\\c"}"#
        );
    }

    #[test]
    fn subscription_progress_row_uses_ordered_envelope_columns() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let fields = Arc::new(subscription_field_infos(&schema, None));
        assert_eq!(fields.len(), 7);
        assert_eq!(fields[1].name(), SUBSCRIPTION_KIND_COLUMN);
        assert_eq!(fields[2].name(), SUBSCRIPTION_EPOCH_COLUMN);
        assert_eq!(fields[3].name(), SUBSCRIPTION_CHECKPOINT_COLUMN);
        assert_eq!(fields[4].name(), SUBSCRIPTION_LOG_SEQUENCE_COLUMN);
        assert_eq!(fields[5].name(), SUBSCRIPTION_ROW_INDEX_COLUMN);
        assert_eq!(fields[6].name(), SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN);
        encode_subscription_progress_row(&fields, 1, 8, 7, 99, 6).unwrap();
    }

    #[test]
    fn uint64_subscription_fails_instead_of_corrupting_bigint() {
        use arrow_array::{RecordBatch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};

        fn batch(value: u64) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                vec![Arc::new(UInt64Array::from(vec![value]))],
            )
            .unwrap()
        }

        fn assert_out_of_range(error: PgWireError) {
            let PgWireError::UserError(info) = error else {
                panic!("expected user error");
            };
            assert_eq!(info.code, "22003");
        }

        for binary in [false, true] {
            let ok = batch(i64::MAX as u64);
            let mut fields = subscription_field_infos(&ok.schema(), None);
            if binary {
                fields[0] = FieldInfo::new(
                    "id".to_string(),
                    None,
                    None,
                    Type::INT8,
                    FieldFormat::Binary,
                );
            }
            let fields = Arc::new(fields);
            encode_subscription_batch_row(&ok, 0, 7, &fields).unwrap();

            for value in [i64::MAX as u64 + 1, u64::MAX] {
                assert_out_of_range(
                    encode_subscription_batch_row(&batch(value), 0, 7, &fields).unwrap_err(),
                );
            }
        }
    }

    #[test]
    fn cached_subscription_schema_change_is_rejected() {
        use arrow_schema::{DataType, Field, Schema};

        let cached = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let same = cached.clone();
        let changed = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);
        ensure_cached_subscription_schema(&cached, &same).unwrap();
        let error = ensure_cached_subscription_schema(&cached, &changed).unwrap_err();
        let PgWireError::UserError(info) = error else {
            panic!("expected user error");
        };
        assert_eq!(info.code, "0A000");
        assert_eq!(info.message, "cached result type changed");
    }

    #[test]
    fn subscription_open_errors_keep_distinct_sqlstates() {
        for (error, expected) in [
            (laminar_db::DbError::StreamNotFound("s".into()), "42P01"),
            (laminar_db::DbError::Unsupported("cluster".into()), "0A000"),
            (
                laminar_db::DbError::InvalidOperation("epoch is not committed".into()),
                "22023",
            ),
            (
                laminar_db::DbError::Pipeline("subscriber cap".into()),
                "53300",
            ),
        ] {
            let PgWireError::UserError(info) = subscription_open_error("s", error) else {
                panic!("expected user error");
            };
            assert_eq!(info.code, expected);
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

    #[test]
    fn classify_outcome_buckets_errors() {
        use std::io::{Error, ErrorKind};
        assert_eq!(super::classify_outcome(&Ok(())), "ok");
        assert_eq!(
            super::classify_outcome(&Err(Error::other("FATAL: 28P01 bad pass"))),
            "auth_failed"
        );
        assert_eq!(
            super::classify_outcome(&Err(Error::other("rustls HandshakeFailure"))),
            "tls_failed"
        );
        assert_eq!(
            super::classify_outcome(&Err(Error::new(ErrorKind::BrokenPipe, "broken"))),
            "error"
        );
    }

    #[test]
    fn failure_tracker_blocks_after_threshold() {
        use std::net::{IpAddr, Ipv4Addr};
        use std::time::Duration;
        let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
        let tracker = super::FailureTracker::default();
        let limit = 3;
        let window = Duration::from_secs(60);

        for _ in 0..limit {
            assert!(!tracker.is_blocked(ip, limit, window));
            tracker.record_failure(ip);
        }
        assert!(tracker.is_blocked(ip, limit, window));
    }

    #[test]
    fn failure_tracker_disabled_when_limit_zero() {
        use std::net::{IpAddr, Ipv4Addr};
        use std::time::Duration;
        let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
        let tracker = super::FailureTracker::default();
        for _ in 0..100 {
            tracker.record_failure(ip);
        }
        assert!(!tracker.is_blocked(ip, 0, Duration::from_secs(60)));
    }

    #[test]
    fn failure_tracker_expires_old_entries() {
        use std::net::{IpAddr, Ipv4Addr};
        use std::time::Duration;
        let ip: IpAddr = Ipv4Addr::LOCALHOST.into();
        let tracker = super::FailureTracker::default();
        for _ in 0..5 {
            tracker.record_failure(ip);
        }
        // Window of 0 means every recorded failure is already expired.
        assert!(!tracker.is_blocked(ip, 5, Duration::from_secs(0)));
    }

    #[test]
    fn failure_tracker_caps_distinct_ips() {
        use std::net::{IpAddr, Ipv4Addr};
        let tracker = super::FailureTracker::default();
        // Push past the cap; map size must stay bounded.
        for i in 0..(super::MAX_TRACKED_IPS + 100) {
            #[allow(clippy::cast_possible_truncation)]
            let ip: IpAddr = Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8).into();
            tracker.record_failure(ip);
        }
        let len = tracker.inner.lock().len();
        assert!(
            len <= super::MAX_TRACKED_IPS,
            "tracker exceeded cap: {len} > {}",
            super::MAX_TRACKED_IPS
        );
    }

    #[tokio::test]
    async fn serve_rejects_remote_bind_in_trust_mode() {
        let db = LaminarDB::open().expect("db opens");
        let err = serve(db, "0.0.0.0:0", HashMap::new(), false, None, 256, 10)
            .await
            .expect_err("trust + 0.0.0.0 must fail");
        assert!(err.to_string().contains("trust auth"), "got: {err}");
    }

    #[tokio::test]
    async fn serve_rejects_remote_bind_without_explicit_optin() {
        let db = LaminarDB::open().expect("db opens");
        let mut users = HashMap::new();
        users.insert("alice".into(), Secret::new("wonderland-key"));
        let err = serve(db, "0.0.0.0:0", users, false, None, 256, 10)
            .await
            .expect_err("md5 + 0.0.0.0 without allow_remote must fail");
        assert!(
            err.to_string().contains("pgwire_allow_remote"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn serve_rejects_remote_bind_without_tls() {
        let db = LaminarDB::open().expect("db opens");
        let mut users = HashMap::new();
        users.insert("alice".into(), Secret::new("wonderland-key"));
        let err = serve(db, "0.0.0.0:0", users, true, None, 256, 10)
            .await
            .expect_err("remote pgwire must not start without TLS");
        assert!(
            err.to_string().contains("requires pgwire_tls_cert"),
            "got: {err}"
        );
    }
}

#[cfg(test)]
mod integration_tests {
    //! End-to-end pgwire driven by `tokio_postgres` against an in-process
    //! `LaminarDB`. Verifies the wire-protocol surface — handshake, SimpleQuery
    //! dispatch, error reporting — that unit tests can't reach. Engine-level
    //! row flow is covered in `laminar-db`'s `db::tests`.

    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use laminar_db::subscription::SubscribeStart;
    use laminar_db::LaminarDB;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_postgres::{NoTls, SimpleQueryMessage};

    use super::{
        Secret, SUBSCRIPTION_CHECKPOINT_COLUMN, SUBSCRIPTION_EPOCH_COLUMN, SUBSCRIPTION_FETCH_WAIT,
        SUBSCRIPTION_KIND_COLUMN, SUBSCRIPTION_LOG_SEQUENCE_COLUMN, SUBSCRIPTION_ROW_INDEX_COLUMN,
        SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN,
    };

    async fn spawn_server_with(
        users: HashMap<String, Secret>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute(
            "CREATE MATERIALIZED VIEW prices AS \
             SELECT symbol, price FROM trades",
        )
        .await
        .expect("create mv");
        db.start().await.expect("db starts");

        let (addr, handle) =
            super::serve(Arc::clone(&db), "127.0.0.1:0", users, false, None, 256, 10)
                .await
                .expect("pgwire serve");
        (addr, handle)
    }

    async fn spawn_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        spawn_server_with(HashMap::new()).await
    }

    async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
        let conn_str = format!(
            "host={} port={} user=any dbname=laminardb",
            addr.ip(),
            addr.port()
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("pgwire connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
    }

    async fn raw_read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let message_type = stream.read_u8().await.expect("backend message type");
        let length = stream.read_i32().await.expect("backend message length");
        assert!(length >= 4, "invalid backend message length {length}");
        let mut body = vec![0; (length - 4) as usize];
        stream
            .read_exact(&mut body)
            .await
            .expect("backend message body");
        (message_type, body)
    }

    async fn raw_read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        let mut messages = Vec::new();
        loop {
            let message = raw_read_message(stream).await;
            let ready = message.0 == b'Z';
            messages.push(message);
            if ready {
                return messages;
            }
        }
    }

    fn raw_frame(message_type: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(message_type);
        frame.put_i32(i32::try_from(body.len() + 4).expect("test frame length"));
        frame.extend_from_slice(body);
        frame.to_vec()
    }

    async fn raw_connect(addr: std::net::SocketAddr) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.expect("raw connect");
        let mut body = BytesMut::new();
        body.put_i32(196_608);
        body.extend_from_slice(b"user\0any\0database\0laminardb\0\0");
        let mut startup = BytesMut::new();
        startup.put_i32(i32::try_from(body.len() + 4).unwrap());
        startup.extend_from_slice(&body);
        stream.write_all(&startup).await.expect("write startup");
        let messages = raw_read_until_ready(&mut stream).await;
        assert!(messages.iter().all(|message| message.0 != b'E'));
        stream
    }

    async fn raw_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut body = BytesMut::new();
        body.extend_from_slice(sql.as_bytes());
        body.put_u8(0);
        stream
            .write_all(&raw_frame(b'Q', &body))
            .await
            .expect("write Query");
        raw_read_until_ready(stream).await
    }

    async fn raw_parse_bind_sync(
        stream: &mut TcpStream,
        statement: &str,
        portal: &str,
        sql: &str,
    ) -> Vec<(u8, Vec<u8>)> {
        let mut parse = BytesMut::new();
        parse.extend_from_slice(statement.as_bytes());
        parse.put_u8(0);
        parse.extend_from_slice(sql.as_bytes());
        parse.put_u8(0);
        parse.put_u16(0);

        let mut bind = BytesMut::new();
        bind.extend_from_slice(portal.as_bytes());
        bind.put_u8(0);
        bind.extend_from_slice(statement.as_bytes());
        bind.put_u8(0);
        bind.put_u16(0);
        bind.put_u16(0);
        bind.put_u16(0);

        let mut frames = raw_frame(b'P', &parse);
        frames.extend_from_slice(&raw_frame(b'B', &bind));
        frames.extend_from_slice(&raw_frame(b'S', &[]));
        stream
            .write_all(&frames)
            .await
            .expect("write Parse/Bind/Sync");
        raw_read_until_ready(stream).await
    }

    async fn raw_execute_sync(
        stream: &mut TcpStream,
        portal: &str,
        max_rows: i32,
    ) -> Vec<(u8, Vec<u8>)> {
        let mut execute = BytesMut::new();
        execute.extend_from_slice(portal.as_bytes());
        execute.put_u8(0);
        execute.put_i32(max_rows);

        let mut frames = raw_frame(b'E', &execute);
        frames.extend_from_slice(&raw_frame(b'S', &[]));
        stream.write_all(&frames).await.expect("write Execute/Sync");
        raw_read_until_ready(stream).await
    }

    fn first_row_value(messages: &[SimpleQueryMessage], col: usize) -> Option<&str> {
        messages.iter().find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(col),
            _ => None,
        })
    }

    #[tokio::test]
    async fn handshake_and_builtins() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let messages = client
            .simple_query("SELECT version()")
            .await
            .expect("version");
        let v = first_row_value(&messages, 0).expect("row");
        assert!(v.contains("LaminarDB"), "version: {v}");

        let messages = client
            .simple_query("SELECT current_database()")
            .await
            .expect("current_database");
        assert_eq!(first_row_value(&messages, 0), Some("laminar"));

        handle.abort();
    }

    #[tokio::test]
    async fn show_streams_runs() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        // No assertion on contents — just that the dispatch path returns rows
        // without error. Engine-level SHOW behavior is covered in laminar-db.
        client
            .simple_query("SHOW STREAMS")
            .await
            .expect("SHOW STREAMS");

        handle.abort();
    }

    #[tokio::test]
    async fn simple_subscribe_is_rejected_as_unbounded() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("SUBSCRIBE prices")
            .await
            .expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "0A000");
        assert!(
            db_err.message().contains("WebSocket"),
            "message: {}",
            db_err.message()
        );

        handle.abort();
    }

    #[tokio::test]
    async fn bounded_subscribe_with_unknown_filter_column_returns_pg_error() {
        let (addr, handle) = spawn_server().await;
        let mut client = connect(addr).await;
        let tx = client.transaction().await.expect("BEGIN");
        let stmt = tx
            .prepare("SUBSCRIBE prices WHERE no_such_col > 1")
            .await
            .expect("parse resolves the stream schema");
        let portal = tx.bind(&stmt, &[]).await.expect("bind");

        let err = tx.query_portal(&portal, 1).await.expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("no_such_col"),
            "filter error must name the bad column, got: {}",
            db_err.message()
        );

        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_as_of_uncommitted_returns_pg_error() {
        // No checkpoint has committed on `prices`, so a future AS OF cut must
        // be distinguished from pruned history.
        let (addr, handle) = spawn_server().await;
        let mut client = connect(addr).await;
        let tx = client.transaction().await.expect("BEGIN");
        let stmt = tx
            .prepare("SUBSCRIBE prices AS OF EPOCH 1")
            .await
            .expect("prepare");
        let portal = tx.bind(&stmt, &[]).await.expect("bind");

        let err = tx.query_portal(&portal, 1).await.expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "22023");
        assert!(
            db_err.message().contains("not committed"),
            "message: {}",
            db_err.message()
        );

        handle.abort();
    }

    /// SUBSCRIBE must actually stream emitted MV rows over the socket: bind a
    /// portal, push rows into the source, and read them back via the
    /// extended-query portal (the chunked path JDBC/asyncpg use).
    #[tokio::test]
    async fn subscribe_streams_emitted_rows_over_the_wire() {
        use std::time::Duration;

        use arrow_array::{Float64Array, RecordBatch, StringArray};

        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
            .await
            .expect("create mv");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("serve");
        let mut client = connect(addr).await;
        let txn = client.transaction().await.expect("begin");

        // The subscription opens when the first Execute runs, so push once the
        // read is in flight (Tail would otherwise miss earlier rows).
        let stmt = txn.prepare("SUBSCRIBE prices").await.expect("prepare");
        let portal = txn.bind(&stmt, &[]).await.expect("bind");

        let pusher = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let src = db.source_untyped("trades").expect("source handle");
                let batch = RecordBatch::try_new(
                    src.schema().clone(),
                    vec![
                        Arc::new(StringArray::from(vec!["AAPL", "MSFT"])),
                        Arc::new(Float64Array::from(vec![100.0, 200.0])),
                    ],
                )
                .expect("batch");
                src.push_arrow(batch).expect("push");
            }
        });

        let rows = tokio::time::timeout(Duration::from_secs(10), txn.query_portal(&portal, 2))
            .await
            .expect("read did not time out")
            .expect("query_portal");
        pusher.await.expect("pusher");

        let mut symbols: Vec<String> = rows
            .iter()
            .map(|r| r.get::<_, &str>(0).to_string())
            .collect();
        symbols.sort();
        assert_eq!(
            symbols,
            ["AAPL", "MSFT"],
            "both emitted rows arrive over pgwire"
        );

        handle.abort();
    }

    /// A TEXT[] column must round-trip over the binary wire (asyncpg/JDBC
    /// request binary): the column advertises the _text OID and encodes as a
    /// Postgres array, so tokio_postgres decodes it into a Vec<String>.
    #[tokio::test]
    async fn subscribe_decodes_text_array_in_binary_format() {
        use std::time::Duration;

        use arrow_array::{Int64Array, RecordBatch};

        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE feed (id BIGINT)")
            .await
            .expect("create source");
        db.execute(
            "CREATE MATERIALIZED VIEW tagged AS SELECT id, make_array('en','ja') AS tags FROM feed",
        )
        .await
        .expect("create mv");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("serve");
        let mut client = connect(addr).await;
        let txn = client.transaction().await.expect("begin");
        let stmt = txn.prepare("SUBSCRIBE tagged").await.expect("prepare");
        let portal = txn.bind(&stmt, &[]).await.expect("bind");

        let pusher = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let src = db.source_untyped("feed").expect("source handle");
                let batch = RecordBatch::try_new(
                    src.schema().clone(),
                    vec![Arc::new(Int64Array::from(vec![1_i64]))],
                )
                .expect("batch");
                src.push_arrow(batch).expect("push");
            }
        });

        let rows = tokio::time::timeout(Duration::from_secs(10), txn.query_portal(&portal, 1))
            .await
            .expect("read did not time out")
            .expect("query_portal");
        pusher.await.expect("pusher");

        assert_eq!(rows.len(), 1);
        let tags: Vec<String> = rows[0].get(1);
        assert_eq!(
            tags,
            vec!["en".to_string(), "ja".to_string()],
            "TEXT[] decoded over the binary wire"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn ddl_returns_pg_error_pointing_at_http() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("CREATE SOURCE more_trades (sym VARCHAR)")
            .await
            .expect_err("DDL must be rejected");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("/api/v1/sql"),
            "message: {}",
            db_err.message()
        );

        handle.abort();
    }

    fn md5_users() -> HashMap<String, Secret> {
        let mut u = HashMap::new();
        u.insert("alice".to_string(), Secret::new(TEST_PASSWORD));
        u
    }

    const TEST_PASSWORD: &str = "wonderland-key";

    async fn connect_with_password(
        addr: std::net::SocketAddr,
        user: &str,
        password: &str,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let conn_str = format!(
            "host={} port={} user={user} password={password} dbname=laminardb",
            addr.ip(),
            addr.port()
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(client)
    }

    #[tokio::test]
    async fn md5_auth_accepts_correct_password() {
        let (addr, handle) = spawn_server_with(md5_users()).await;

        let client = connect_with_password(addr, "alice", TEST_PASSWORD)
            .await
            .expect("auth must succeed");

        let messages = client
            .simple_query("SELECT version()")
            .await
            .expect("query after auth");
        let v = first_row_value(&messages, 0).expect("row");
        assert!(v.contains("LaminarDB"), "version: {v}");

        handle.abort();
    }

    #[tokio::test]
    async fn concurrent_md5_challenges_are_session_isolated() {
        let mut users = HashMap::new();
        users.insert("alice".to_owned(), Secret::new("alice-password"));
        users.insert("bob".to_owned(), Secret::new("bob-password"));
        let (addr, handle) = spawn_server_with(users).await;

        let attempts = (0..64).map(|index| async move {
            let (user, password) = if index % 2 == 0 {
                ("alice", "alice-password")
            } else {
                ("bob", "bob-password")
            };
            let client = connect_with_password(addr, user, password)
                .await
                .expect("concurrent authentication must succeed");
            client
                .simple_query("SELECT 1")
                .await
                .expect("authenticated session remains usable");
        });
        futures::future::join_all(attempts).await;

        handle.abort();
    }

    #[tokio::test]
    async fn md5_auth_rejects_wrong_password() {
        let (addr, handle) = spawn_server_with(md5_users()).await;

        let err = connect_with_password(addr, "alice", "not-the-password")
            .await
            .expect_err("auth must fail");

        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

        handle.abort();
    }

    /// Pre-hashed pgwire_users entry: stored value is `md5{hex(md5(pw||user))}`,
    /// matching pg_authid. Plaintext never touches disk yet auth still succeeds.
    fn md5_users_prehashed(user: &str, password: &str) -> HashMap<String, Secret> {
        use md5::{Digest, Md5};
        let mut h = Md5::new();
        h.update(password.as_bytes());
        h.update(user.as_bytes());
        let inner = format!("{:x}", h.finalize());
        let mut u = HashMap::new();
        u.insert(user.to_string(), Secret::new(format!("md5{inner}")));
        u
    }

    #[tokio::test]
    async fn md5_auth_accepts_correct_password_against_prehash() {
        let (addr, handle) = spawn_server_with(md5_users_prehashed("alice", TEST_PASSWORD)).await;
        let client = connect_with_password(addr, "alice", TEST_PASSWORD)
            .await
            .expect("auth must succeed against pre-hashed entry");
        let messages = client
            .simple_query("SELECT version()")
            .await
            .expect("query after auth");
        let v = first_row_value(&messages, 0).expect("row");
        assert!(v.contains("LaminarDB"), "version: {v}");
        handle.abort();
    }

    #[tokio::test]
    async fn md5_auth_rejects_wrong_password_against_prehash() {
        let (addr, handle) = spawn_server_with(md5_users_prehashed("alice", TEST_PASSWORD)).await;
        let err = connect_with_password(addr, "alice", "not-the-password")
            .await
            .expect_err("auth must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");
        handle.abort();
    }

    #[test]
    fn parse_pre_hashed_md5_strict_format() {
        // 32 lowercase hex after the tag → accepted.
        let inner = "5d41402abc4b2a76b9719d911017c592";
        assert_eq!(
            super::parse_pre_hashed_md5(&format!("md5{inner}")),
            Some(inner),
        );
        // Wrong length, uppercase hex, missing prefix, or non-hex → rejected.
        assert_eq!(super::parse_pre_hashed_md5("md5short"), None);
        assert_eq!(
            super::parse_pre_hashed_md5("md55D41402ABC4B2A76B9719D911017C592"),
            None,
        );
        assert_eq!(super::parse_pre_hashed_md5(inner), None);
        assert_eq!(
            super::parse_pre_hashed_md5("md5zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
            None,
        );
    }

    #[tokio::test]
    async fn md5_auth_rejects_unknown_user() {
        let (addr, handle) = spawn_server_with(md5_users()).await;

        let err = connect_with_password(addr, "mallory", "anything")
            .await
            .expect_err("auth must fail");

        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

        handle.abort();
    }

    #[tokio::test]
    async fn connection_cap_drops_excess_clients() {
        // Cap of 1; first client occupies the slot, second receives a startup
        // FATAL without displacing the active session.
        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
            .await
            .expect("create mv");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            1,
            10,
        )
        .await
        .expect("pgwire serve");

        // An authenticated connection occupies the only session slot.
        let _first = connect(addr).await;
        let conn_str = format!(
            "host={} port={} user=any dbname=laminardb",
            addr.ip(),
            addr.port()
        );
        let error = match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok(_) => panic!("second connect must be refused"),
            Err(error) => error,
        };
        let db_error = error.as_db_error().expect("typed startup FATAL");
        assert_eq!(db_error.code().code(), "53300");

        handle.abort();
    }

    #[tokio::test]
    async fn cancel_request_bypasses_full_session_cap() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
            .await
            .expect("create mv");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "0.0.0.0:0",
            md5_users(),
            true,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: None,
            }),
            1,
            10,
        )
        .await
        .expect("pgwire serve");

        // Prefer negotiates TLS for the normal session but lets NoTls below
        // send the protocol-defined plaintext CancelRequest on a fresh socket.
        let conn_str = format!(
            "host=localhost hostaddr=127.0.0.1 port={} user=alice password={} \
             dbname=laminardb sslmode=prefer",
            addr.port(),
            TEST_PASSWORD,
        );
        let tls = make_client_tls(&cert_path, None);
        let (client, connection) = tokio_postgres::connect(&conn_str, tls)
            .await
            .expect("TLS pgwire connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let cancel = client.cancel_token();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let query = tokio::spawn(async move {
            let mut client = client;
            let transaction = client.transaction().await.expect("BEGIN");
            let statement = transaction
                .prepare("SUBSCRIBE prices")
                .await
                .expect("prepare");
            let portal = transaction.bind(&statement, &[]).await.expect("bind");
            ready_tx.send(()).expect("query ready");
            transaction
                .query_portal(&portal, 1)
                .await
                .expect_err("quiet fetch must be cancelled")
                .as_db_error()
                .map(|error| error.code().code().to_owned())
        });

        ready_rx.await.expect("query ready");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel
            .cancel_query(NoTls)
            .await
            .expect("plaintext CancelRequest bypasses TLS and the full session semaphore");
        let code = tokio::time::timeout(std::time::Duration::from_secs(3), query)
            .await
            .expect("cancel response")
            .expect("query task");
        assert_eq!(code.as_deref(), Some("57014"));

        handle.abort();
    }

    /// Self-signed cert+key written to a tempdir for the duration of the
    /// test. `rcgen` is the well-maintained option for ad-hoc certs.
    fn self_signed_pem() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let cert =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen issue cert");
        let dir = tempfile::tempdir().expect("tempdir");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
        (dir, cert_path, key_path)
    }

    /// CA + client-leaf bundle for mTLS tests. The CA PEM is written to a
    /// tempfile so the server can be pointed at it via `pgwire_tls_client_ca`;
    /// the leaf cert+key are returned in DER form for direct use by a rustls
    /// `ClientConfig`.
    struct MintedClientPki {
        _dir: tempfile::TempDir,
        ca_pem_path: std::path::PathBuf,
        leaf_chain: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
        leaf_key: tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
    }

    fn mint_ca_and_client_leaf(common_name: &str) -> MintedClientPki {
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let mut ca_params = rcgen::CertificateParams::new(vec!["mtls-test-ca".into()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let mut leaf_params = rcgen::CertificateParams::new(vec![common_name.into()]).unwrap();
        leaf_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let ca_pem_path = dir.path().join("ca.pem");
        std::fs::write(&ca_pem_path, ca_cert.pem()).unwrap();

        let leaf_chain = vec![CertificateDer::from(leaf_cert.der().to_vec())];
        let leaf_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));

        MintedClientPki {
            _dir: dir,
            ca_pem_path,
            leaf_chain,
            leaf_key,
        }
    }

    /// Builds a tokio_postgres TLS connector that trusts `server_cert_path`
    /// for the server hello and (optionally) presents a client cert for mTLS.
    fn make_client_tls(
        server_cert_path: &std::path::Path,
        client_auth: Option<(
            Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
            tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
        )>,
    ) -> tokio_postgres_rustls::MakeRustlsConnect {
        super::ensure_tls_provider();
        let cert_bytes = std::fs::read(server_cert_path).unwrap();
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        {
            roots.add(c).unwrap();
        }
        let builder = tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots);
        let client_cfg = match client_auth {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).unwrap(),
            None => builder.with_no_client_auth(),
        };
        tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg)
    }

    async fn assert_plaintext_startup_is_fatal(addr: std::net::SocketAddr) {
        let mut stream = TcpStream::connect(addr).await.expect("raw NoTls connect");
        let mut body = BytesMut::new();
        body.put_i32(196_608);
        body.extend_from_slice(b"user\0alice\0database\0laminardb\0\0");
        let mut startup = BytesMut::new();
        startup.put_i32(i32::try_from(body.len() + 4).expect("startup length"));
        startup.extend_from_slice(&body);
        stream
            .write_all(&startup)
            .await
            .expect("write plaintext StartupMessage");

        let (message_type, body) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            raw_read_message(&mut stream),
        )
        .await
        .expect("startup FATAL response");
        assert_eq!(
            message_type, b'E',
            "authentication must not begin on plaintext"
        );
        assert!(
            body.windows(b"TLS is required".len())
                .any(|window| window == b"TLS is required"),
            "unexpected ErrorResponse: {body:?}"
        );
    }

    #[tokio::test]
    async fn remote_listener_rejects_raw_notls_startup_before_auth() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (bound, handle) = super::serve(
            Arc::clone(&db),
            "0.0.0.0:0",
            md5_users(),
            true,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: None,
            }),
            256,
            10,
        )
        .await
        .expect("remote TLS listener");

        assert_plaintext_startup_is_fatal(std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            bound.port(),
        )))
        .await;
        handle.abort();
    }

    #[tokio::test]
    async fn client_ca_requires_tls_on_loopback() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let pki = mint_ca_and_client_leaf("alice");
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: Some(&pki.ca_pem_path),
            }),
            256,
            10,
        )
        .await
        .expect("mTLS listener");

        assert_plaintext_startup_is_fatal(addr).await;
        handle.abort();
    }

    /// Self-signed cert with notAfter in the past, for the expiry test.
    fn expired_self_signed_pem() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        let one_year_ago = time::OffsetDateTime::now_utc() - time::Duration::days(365);
        params.not_before = one_year_ago - time::Duration::days(2);
        params.not_after = one_year_ago;
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (dir, cert_path, key_path)
    }

    #[tokio::test]
    async fn tls_load_rejects_expired_cert() {
        let (_dir, cert_path, key_path) = expired_self_signed_pem();
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let err = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: None,
            }),
            256,
            10,
        )
        .await
        .expect_err("expired cert must be rejected");
        assert!(err.to_string().contains("expired"), "got: {err}");
    }

    #[tokio::test]
    async fn tls_min_1_3_rejects_tls_1_2_client() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_3,
                client_ca: None,
            }),
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        let cert_bytes = std::fs::read(&cert_path).unwrap();
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        {
            roots.add(c).unwrap();
        }
        super::ensure_tls_provider();
        // Client pinned to TLS 1.2 only — must be refused by a 1.3-min server.
        let client_cfg = tokio_rustls::rustls::ClientConfig::builder_with_protocol_versions(&[
            &tokio_rustls::rustls::version::TLS12,
        ])
        .with_root_certificates(roots)
        .with_no_client_auth();

        let conn_str = format!(
            "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
            addr.ip(),
            addr.port(),
        );
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg);
        let err = match tokio_postgres::connect(&conn_str, tls).await {
            Ok(_) => panic!("TLS 1.2 client must be refused by a 1.3-min server"),
            Err(e) => e,
        };
        // tokio_postgres wraps the rustls error; flatten the chain so we can
        // assert against the version-mismatch token rustls emits.
        let chain = std::iter::successors(Some(&err as &dyn std::error::Error), |e| e.source())
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" | ");
        assert!(
            chain.contains("ProtocolVersion") || chain.contains("incompatible"),
            "expected a TLS version-mismatch error, got: {chain}"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn tls_handshake_succeeds() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: None,
            }),
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        // Build a client TLS config that trusts the same self-signed cert.
        let cert_bytes = std::fs::read(&cert_path).unwrap();
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut std::io::Cursor::new(cert_bytes))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        {
            roots.add(c).unwrap();
        }
        super::ensure_tls_provider();
        let client_cfg = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        let conn_str = format!(
            "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
            addr.ip(),
            addr.port(),
        );
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_cfg);
        let (client, conn) = tokio_postgres::connect(&conn_str, tls)
            .await
            .expect("TLS handshake + connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let messages = client
            .simple_query("SELECT version()")
            .await
            .expect("query over TLS");
        let v = first_row_value(&messages, 0).expect("row");
        assert!(v.contains("LaminarDB"), "version: {v}");

        handle.abort();
    }

    /// mTLS: with a client_ca configured, a client that presents no cert
    /// must be refused at handshake time.
    #[tokio::test]
    async fn mtls_rejects_client_without_cert() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let pki = mint_ca_and_client_leaf("alice");
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: Some(&pki.ca_pem_path),
            }),
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        let tls = make_client_tls(&cert_path, None);
        let conn_str = format!(
            "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
            addr.ip(),
            addr.port(),
        );
        let err = match tokio_postgres::connect(&conn_str, tls).await {
            Ok(_) => panic!("client without a cert must be refused under mTLS"),
            Err(e) => e,
        };
        assert!(
            err_chain(&err).contains("CertificateRequired")
                || err_chain(&err).contains("HandshakeFailure")
                || err_chain(&err).contains("certificate required"),
            "expected a missing-client-cert error, got: {}",
            err_chain(&err),
        );
        handle.abort();
    }

    /// mTLS: a client cert signed by an unknown CA must be refused.
    #[tokio::test]
    async fn mtls_rejects_untrusted_client_cert() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let trusted = mint_ca_and_client_leaf("trusted");
        let stranger = mint_ca_and_client_leaf("stranger");
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: Some(&trusted.ca_pem_path),
            }),
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        // Client presents a leaf signed by a CA the server doesn't know.
        let tls = make_client_tls(
            &cert_path,
            Some((stranger.leaf_chain.clone(), stranger.leaf_key.clone_key())),
        );
        let conn_str = format!(
            "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
            addr.ip(),
            addr.port(),
        );
        let err = match tokio_postgres::connect(&conn_str, tls).await {
            Ok(_) => panic!("untrusted client cert must be refused"),
            Err(e) => e,
        };
        // rustls maps a verifier-rejected client cert to a fatal alert; the
        // exact variant depends on the protocol version and verifier path
        // (UnknownCA / BadCertificate on 1.2, DecryptError or
        // CertificateUnknown on 1.3). We assert it failed at the TLS layer.
        let chain = err_chain(&err);
        assert!(
            chain.contains("UnknownCA")
                || chain.contains("BadCertificate")
                || chain.contains("CertificateUnknown")
                || chain.contains("DecryptError")
                || chain.contains("HandshakeFailure"),
            "expected a cert-rejection alert, got: {chain}",
        );
        handle.abort();
    }

    /// mTLS: a client cert signed by the configured CA is accepted, and a
    /// SimpleQuery completes over the encrypted+authenticated session.
    #[tokio::test]
    async fn mtls_accepts_trusted_client_cert() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let pki = mint_ca_and_client_leaf("alice");
        let db = LaminarDB::open().expect("db opens");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
                min_version: super::TlsMinVersion::V1_2,
                client_ca: Some(&pki.ca_pem_path),
            }),
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        let tls = make_client_tls(
            &cert_path,
            Some((pki.leaf_chain.clone(), pki.leaf_key.clone_key())),
        );
        let conn_str = format!(
            "host=localhost hostaddr={} port={} user=any dbname=laminardb sslmode=require",
            addr.ip(),
            addr.port(),
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, tls)
            .await
            .expect("mTLS handshake + connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let messages = client
            .simple_query("SELECT version()")
            .await
            .expect("query over mTLS");
        let v = first_row_value(&messages, 0).expect("row");
        assert!(v.contains("LaminarDB"), "version: {v}");
        handle.abort();
    }

    /// Build a `TlsReloadState` directly for unit-testing the reload path
    /// without standing up a listener.
    fn build_reload_state(cert: &std::path::Path, key: &std::path::Path) -> super::TlsReloadState {
        let paths = super::TlsPaths {
            cert,
            key,
            min_version: super::TlsMinVersion::V1_2,
            client_ca: None,
        };
        let acceptor = super::load_tls_acceptor(super::TlsPaths {
            cert: paths.cert,
            key: paths.key,
            min_version: paths.min_version,
            client_ca: paths.client_ca,
        })
        .expect("initial acceptor loads");
        super::TlsReloadState {
            paths: super::TlsConfigPaths::from_paths(&paths),
            acceptor: parking_lot::Mutex::new(Arc::new(acceptor)),
        }
    }

    /// Hot-reload: writing a fresh cert+key over the configured paths and
    /// calling `try_reload_tls` swaps the acceptor under the mutex.
    #[test]
    fn tls_reload_swaps_acceptor_on_valid_pair() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        // Initial cert.
        let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(&cert_path, first.cert.pem()).unwrap();
        std::fs::write(&key_path, first.key_pair.serialize_pem()).unwrap();

        let state = build_reload_state(&cert_path, &key_path);
        let before = state.snapshot();

        // Rotate to a brand-new pair.
        let second = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(&cert_path, second.cert.pem()).unwrap();
        std::fs::write(&key_path, second.key_pair.serialize_pem()).unwrap();

        super::try_reload_tls(&state).expect("reload succeeds");
        let after = state.snapshot();
        assert!(
            !Arc::ptr_eq(&before, &after),
            "acceptor pointer must change after a successful reload",
        );
    }

    /// Hot-reload: a corrupt cert file leaves the previous acceptor in
    /// place — TLS doesn't go down on a bad rotation.
    #[test]
    fn tls_reload_keeps_old_acceptor_on_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(&cert_path, first.cert.pem()).unwrap();
        std::fs::write(&key_path, first.key_pair.serialize_pem()).unwrap();

        let state = build_reload_state(&cert_path, &key_path);
        let before = state.snapshot();

        // Truncate cert.pem to non-PEM garbage.
        std::fs::write(&cert_path, b"this is not a certificate").unwrap();
        let err = super::try_reload_tls(&state).expect_err("reload must fail");
        let after = state.snapshot();
        assert!(
            Arc::ptr_eq(&before, &after),
            "acceptor must be unchanged on reload failure",
        );
        assert!(
            err.to_string().contains("pgwire_tls_cert"),
            "error should mention pgwire_tls_cert, got: {err}",
        );
    }

    /// Flatten an error and its `source()` chain to a single string for
    /// substring assertions.
    fn err_chain(err: &(dyn std::error::Error + 'static)) -> String {
        std::iter::successors(Some(err), |e| e.source())
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" | ")
    }

    /// Push one row into the `trades` source so subsequent SUBSCRIBE
    /// reads have something to drain. Returns the schema for tests
    /// that want to build their own batches.
    async fn push_one_trade(
        db: &Arc<LaminarDB>,
        symbol: &str,
        price: f64,
    ) -> arrow_schema::SchemaRef {
        let handle = db.source_untyped("trades").expect("source handle");
        let schema = handle.schema().clone();
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow_array::StringArray::from(vec![symbol])),
                Arc::new(arrow_array::Float64Array::from(vec![price])),
            ],
        )
        .expect("batch");
        handle.push_arrow(batch).expect("push");
        schema
    }

    /// Ingest a row and return both the running server and the underlying db
    /// so tests can keep pushing rows after the listener is up.
    async fn spawn_with_data() -> (
        Arc<LaminarDB>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
    ) {
        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute(
            "CREATE MATERIALIZED VIEW prices AS \
             SELECT symbol, price FROM trades",
        )
        .await
        .expect("create mv");
        db.start().await.expect("db starts");

        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("pgwire serve");
        (db, addr, handle)
    }

    /// Same as `spawn_with_data`, but `prices` is a STREAM with retained
    /// history. Lets cursor tests push rows *before* SUBSCRIBE attaches
    /// without losing them — the receiver replays on attach.
    async fn spawn_with_retained_data() -> (
        Arc<LaminarDB>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
    ) {
        let db = LaminarDB::open().expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute(
            "CREATE STREAM prices AS SELECT symbol, price FROM trades \
             WITH ('retain_history' = '4mb')",
        )
        .await
        .expect("create stream");
        db.start().await.expect("db starts");

        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("pgwire serve");
        (db, addr, handle)
    }

    /// `prepare()` triggers `Parse` + `Describe(Statement)`. Verifies the
    /// extended-query parser resolves stream schemas at parse time and
    /// returns column metadata to the client.
    #[tokio::test]
    async fn extended_query_describe_subscribe_returns_columns() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        let stmt = client
            .prepare("SUBSCRIBE prices")
            .await
            .expect("prepare SUBSCRIBE prices");

        let cols = stmt.columns();
        assert_eq!(cols.len(), 8, "expected 8 columns, got {}", cols.len());
        assert_eq!(cols[0].name(), "symbol");
        assert_eq!(cols[1].name(), "price");
        assert_eq!(cols[0].type_(), &tokio_postgres::types::Type::VARCHAR);
        assert_eq!(cols[1].type_(), &tokio_postgres::types::Type::FLOAT8);
        assert_eq!(cols[2].name(), SUBSCRIPTION_KIND_COLUMN);
        assert_eq!(cols[3].name(), SUBSCRIPTION_EPOCH_COLUMN);
        assert_eq!(cols[4].name(), SUBSCRIPTION_CHECKPOINT_COLUMN);
        assert_eq!(cols[5].name(), SUBSCRIPTION_LOG_SEQUENCE_COLUMN);
        assert_eq!(cols[6].name(), SUBSCRIPTION_ROW_INDEX_COLUMN);
        assert_eq!(cols[7].name(), SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN);

        handle.abort();
    }

    #[tokio::test]
    async fn execute_zero_rejects_before_acquiring_subscription_slot() {
        let (db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;
        let stmt = client.prepare("SUBSCRIBE prices").await.expect("prepare");

        let error = client
            .query(&stmt, &[])
            .await
            .expect_err("Execute(0) must be rejected");
        let db_error = error.as_db_error().expect("typed PG error");
        assert_eq!(db_error.code().code(), "0A000");

        let mut portals = Vec::new();
        for _ in 0..64 {
            portals.push(
                db.open_subscription("prices", None, SubscribeStart::Tail)
                    .await
                    .expect("rejected Execute must not consume a slot"),
            );
        }
        assert!(
            db.open_subscription("prices", None, SubscribeStart::Tail)
                .await
                .is_err(),
            "the configured 64-slot limit must still be enforced"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn sync_obeys_transaction_scoped_portal_lifetime() {
        let (_db, addr, handle) = spawn_with_data().await;

        let mut outside = raw_connect(addr).await;
        let bind = raw_parse_bind_sync(
            &mut outside,
            "outside_statement",
            "outside_portal",
            "SUBSCRIBE prices",
        )
        .await;
        assert!(bind.iter().all(|message| message.0 != b'E'));
        let execute = raw_execute_sync(&mut outside, "outside_portal", 1).await;
        assert!(
            execute.iter().any(|message| message.0 == b'E'),
            "Sync outside BEGIN must end the implicit transaction and destroy portals"
        );

        let mut inside = raw_connect(addr).await;
        let begin = raw_query(&mut inside, "BEGIN").await;
        assert_eq!(
            begin.last().and_then(|message| message.1.first()).copied(),
            Some(b'T')
        );
        for (statement, portal) in [("named_statement", "named_portal"), ("", "")] {
            let bind =
                raw_parse_bind_sync(&mut inside, statement, portal, "SUBSCRIBE prices").await;
            assert!(bind.iter().all(|message| message.0 != b'E'));
            assert_eq!(
                bind.last().and_then(|message| message.1.first()).copied(),
                Some(b'T')
            );

            let execute = raw_execute_sync(&mut inside, portal, i32::MAX).await;
            assert!(execute.iter().all(|message| message.0 != b'E'));
            assert!(
                execute.iter().any(|message| message.0 == b's'),
                "bounded fetch must suspend without allocating from i32::MAX"
            );
        }
        let rollback = raw_query(&mut inside, "ROLLBACK").await;
        assert_eq!(
            rollback
                .last()
                .and_then(|message| message.1.first())
                .copied(),
            Some(b'I')
        );

        handle.abort();
    }

    #[tokio::test]
    async fn cancel_interrupts_subscription_fetch_and_releases_slot() {
        let (db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;
        let cancel = client.cancel_token();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let query = tokio::spawn(async move {
            let mut client = client;
            let tx = client.transaction().await.expect("BEGIN");
            let statement = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
            let portal = tx.bind(&statement, &[]).await.expect("bind");
            ready_tx.send(()).expect("signal query readiness");
            let error = tx
                .query_portal(&portal, 1)
                .await
                .expect_err("cancel must interrupt the fetch");
            error
                .as_db_error()
                .map(|error| error.code().code().to_owned())
        });

        ready_rx.await.expect("query ready");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel
            .cancel_query(NoTls)
            .await
            .expect("send CancelRequest");
        let code = tokio::time::timeout(std::time::Duration::from_secs(3), query)
            .await
            .expect("cancel response")
            .expect("query task");
        assert_eq!(code.as_deref(), Some("57014"));

        let mut portals = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while portals.len() < 64 {
            match db
                .open_subscription("prices", None, SubscribeStart::Tail)
                .await
            {
                Ok(portal) => portals.push(portal),
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => panic!("cancel did not release subscription slot: {error}"),
            }
        }

        handle.abort();
    }

    #[tokio::test]
    async fn extended_query_emits_committed_checkpoint_progress() {
        let checkpoint_dir = tempfile::tempdir().expect("checkpoint tempdir");
        let db = LaminarDB::open_with_config(laminar_db::LaminarConfig {
            checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                data_dir: Some(checkpoint_dir.path().to_path_buf()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("db opens");
        db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
            .await
            .expect("create source");
        db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol, price FROM trades")
            .await
            .expect("create mv");
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("pgwire serve");
        let mut client = connect(addr).await;
        let tx = client.transaction().await.expect("BEGIN");
        let stmt = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
        let portal = tx.bind(&stmt, &[]).await.expect("bind portal");

        let pusher = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                push_one_trade(&db, "AAPL", 150.0).await;
            }
        });
        let mut rows = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("data row arrives")
        .expect("query portal");
        pusher.await.expect("pusher");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let committed = db.checkpoint().await.expect("checkpoint");
        rows.extend(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tx.query_portal(&portal, 1),
            )
            .await
            .expect("progress row arrives")
            .expect("query portal"),
        );

        assert!(committed.success, "checkpoint must commit");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, &str>(0), "AAPL");
        assert_eq!(rows[0].get::<_, &str>(2), "data");
        assert!(rows[0].get::<_, Option<&str>>(3).is_none());
        assert!(rows[0].get::<_, Option<&str>>(4).is_none());
        assert_eq!(rows[0].get::<_, &str>(5), "0");
        assert_eq!(rows[0].get::<_, &str>(6), "0");
        assert!(rows[0].get::<_, Option<&str>>(7).is_none());
        assert!(rows[1].get::<_, Option<&str>>(0).is_none());
        assert!(rows[1].get::<_, Option<f64>>(1).is_none());
        assert_eq!(rows[1].get::<_, &str>(2), "progress");
        assert_eq!(rows[1].get::<_, &str>(3), committed.epoch.to_string());
        assert_eq!(
            rows[1].get::<_, &str>(4),
            committed.checkpoint_id.to_string()
        );
        assert_eq!(rows[1].get::<_, &str>(5), "1");
        assert!(rows[1].get::<_, Option<&str>>(6).is_none());
        assert_eq!(rows[1].get::<_, &str>(7), "1");

        handle.abort();
    }

    #[tokio::test]
    async fn prepared_subscribe_rejects_drop_recreate_schema_change() {
        let (db, addr, handle) = spawn_with_data().await;
        let mut client = connect(addr).await;

        let stmt = client
            .prepare("SUBSCRIBE prices")
            .await
            .expect("prepare old result type");
        db.execute("DROP MATERIALIZED VIEW prices")
            .await
            .expect("drop old view");
        db.execute("CREATE MATERIALIZED VIEW prices AS SELECT symbol FROM trades")
            .await
            .expect("create changed view");

        let tx = client.transaction().await.expect("BEGIN");
        let portal = tx.bind(&stmt, &[]).await.expect("bind cached statement");
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("schema fence responds")
        .expect_err("cached result type must not execute");
        let db_error = error.as_db_error().expect("typed PG error");
        assert_eq!(db_error.code().code(), "0A000");
        assert_eq!(db_error.message(), "cached result type changed");

        handle.abort();
    }

    #[tokio::test]
    async fn abrupt_disconnect_releases_named_cursor_subscription_slot() {
        let (db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;
        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE abandoned CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        drop(client);

        let mut portals = Vec::new();
        for _ in 0..63 {
            portals.push(
                db.open_subscription("prices", None, SubscribeStart::Tail)
                    .await
                    .expect("63 direct slots remain"),
            );
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            match db
                .open_subscription("prices", None, SubscribeStart::Tail)
                .await
            {
                Ok(portal) => {
                    portals.push(portal);
                    break;
                }
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => panic!("disconnect did not release cursor slot: {error}"),
            }
        }
        assert_eq!(portals.len(), 64);
        assert!(
            db.open_subscription("prices", None, SubscribeStart::Tail)
                .await
                .is_err(),
            "the 64-slot limit remains enforced"
        );

        handle.abort();
    }

    /// Unknown stream → typed PG error at `Parse` time, before any rows
    /// are pulled.
    #[tokio::test]
    async fn extended_query_prepare_unknown_stream_errors() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        let err = client
            .prepare("SUBSCRIBE no_such_view")
            .await
            .expect_err("must fail at Parse");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(db_err.message().contains("no_such_view"));

        handle.abort();
    }

    /// Bind + Execute with `max_rows=1` against a portal returns one row at a
    /// time and `PortalSuspended`. Drives the binary-format encoders for
    /// VARCHAR + FLOAT8.
    #[tokio::test]
    async fn extended_query_binary_chunked_subscribe() {
        let (db, addr, handle) = spawn_with_data().await;
        let mut client = connect(addr).await;

        // tokio_postgres' `bind` + `query_portal` uses the extended-query
        // protocol with binary format for known column types — the path
        // JDBC and asyncpg take with prepared statements.
        let tx = client.transaction().await.expect("BEGIN");
        let stmt = tx.prepare("SUBSCRIBE prices").await.expect("prepare");
        let portal = tx.bind(&stmt, &[]).await.expect("bind portal");

        // The MV broadcast has no receiver until `Execute` reaches the
        // server and runs `do_query` → `open_subscription`. We can't push
        // from this task before query_portal because query_portal blocks
        // waiting for a row, so spawn the pushes from a sibling task with
        // a short head start for the receiver to attach. With cap=0
        // retention, a push that lands before the receiver is dropped.
        let pusher = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                push_one_trade(&db, "AAPL", 150.5).await;
                push_one_trade(&db, "GOOG", 2700.25).await;
            })
        };

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("first chunk arrives within 3s")
        .expect("query_portal #1");
        assert_eq!(first.len(), 1);
        let symbol: &str = first[0].get(0);
        let price: f64 = first[0].get(1);
        assert_eq!(symbol, "AAPL");
        assert!((price - 150.5).abs() < 1e-9);

        let second = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("second chunk arrives within 3s")
        .expect("query_portal #2");
        assert_eq!(second.len(), 1);
        let symbol: &str = second[0].get(0);
        let price: f64 = second[0].get(1);
        assert_eq!(symbol, "GOOG");
        assert!((price - 2700.25).abs() < 1e-9);

        pusher.await.expect("push task");
        handle.abort();
    }

    /// Regression: binary encoding of `TIMESTAMP` columns must downcast
    /// the Arrow array as its unit-specific primitive type
    /// (`PrimitiveArray<TimestampMicrosecondType>`, not
    /// `PrimitiveArray<Int64Type>`). A bug in this branch would panic on
    /// the first row.
    #[tokio::test]
    async fn extended_query_binary_timestamp() {
        let db = LaminarDB::open().expect("db opens");
        // `WATERMARK FOR ts AS ts - INTERVAL '0' SECOND` declares event time
        // so the streaming pipeline drives progress on the timestamp
        // column — without it, the MV stays empty.
        db.execute(
            "CREATE SOURCE events (ts TIMESTAMP, sym VARCHAR, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .expect("create source");
        db.execute("CREATE MATERIALIZED VIEW ev AS SELECT ts, sym FROM events")
            .await
            .expect("create mv");
        db.start().await.expect("db starts");

        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            None,
            256,
            10,
        )
        .await
        .expect("pgwire serve");

        let mut client = connect(addr).await;
        let tx = client.transaction().await.expect("BEGIN");
        let stmt = tx.prepare("SUBSCRIBE ev").await.expect("prepare");
        let portal = tx.bind(&stmt, &[]).await.expect("bind");

        let expected = chrono::NaiveDate::from_ymd_opt(2026, 5, 9)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let ts_us = expected.and_utc().timestamp_micros();

        // Push from a sibling task after a short delay so the MV
        // broadcast receiver (created inside `Execute`) is attached
        // before send_batch fires. See the matching note in
        // `extended_query_binary_chunked_subscribe`.
        let pusher = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let src = db.source_untyped("events").expect("source");
                let batch = arrow_array::RecordBatch::try_new(
                    src.schema().clone(),
                    vec![
                        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![ts_us])),
                        Arc::new(arrow_array::StringArray::from(vec!["AAPL"])),
                    ],
                )
                .expect("batch");
                src.push_arrow(batch).expect("push");
            })
        };

        let rows = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tx.query_portal(&portal, 1),
        )
        .await
        .expect("row arrives within 3s")
        .expect("query_portal");
        assert_eq!(rows.len(), 1);

        let ts: chrono::NaiveDateTime = rows[0].get(0);
        let sym: &str = rows[0].get(1);
        assert_eq!(ts, expected);
        assert_eq!(sym, "AAPL");

        pusher.await.expect("push task");
        handle.abort();
    }

    /// DDL on the extended-query path is refused at `Parse` with a typed
    /// 0A000 error pointing at the HTTP endpoint — same surface as the
    /// SimpleQuery path.
    #[tokio::test]
    async fn extended_query_ddl_rejected() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        let err = client
            .prepare("CREATE SOURCE more_trades (sym VARCHAR)")
            .await
            .expect_err("DDL must be rejected at Parse");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("/api/v1/sql"),
            "message: {}",
            db_err.message()
        );

        handle.abort();
    }

    /// `\set FETCH_COUNT N` flow: BEGIN; DECLARE …; FETCH N FROM …; CLOSE; COMMIT.
    /// All over SimpleQuery — the path psql uses when `FETCH_COUNT` is set.
    /// Uses the retained-history variant so we can push before SUBSCRIBE.
    #[tokio::test]
    async fn cursor_declare_fetch_close_happy_path() {
        let (db, addr, handle) = spawn_with_retained_data().await;
        let client = connect(addr).await;

        for i in 0..4 {
            push_one_trade(&db, &format!("S{i}"), i as f64).await;
        }

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");

        let messages = client
            .simple_query("FETCH 2 FROM c")
            .await
            .expect("FETCH 2");
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(row_count, 2, "expected exactly 2 rows from FETCH 2");

        client.simple_query("CLOSE c").await.expect("CLOSE");
        client.simple_query("COMMIT").await.expect("COMMIT");

        handle.abort();
    }

    #[tokio::test]
    async fn cursor_requires_explicit_transaction() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        let error = client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect_err("DECLARE outside BEGIN must fail");
        let db_error = error.as_db_error().expect("typed PG error");
        assert_eq!(db_error.code().code(), "25001");

        handle.abort();
    }

    #[tokio::test]
    async fn cursor_rejects_unbounded_and_oversized_fetches() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        for (sql, code) in [
            ("FETCH ALL FROM c", "0A000"),
            ("FETCH 1025 FROM c", "22023"),
        ] {
            client.simple_query("BEGIN").await.expect("BEGIN");
            client
                .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
                .await
                .expect("DECLARE");
            let error = client.simple_query(sql).await.expect_err("FETCH must fail");
            let db_error = error.as_db_error().expect("typed PG error");
            assert_eq!(db_error.code().code(), code, "{sql}: {db_error:?}");
            client.simple_query("ROLLBACK").await.expect("ROLLBACK");
        }

        handle.abort();
    }

    #[tokio::test]
    async fn quiet_cursor_fetch_returns_a_bounded_empty_poll() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;
        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");

        let started = tokio::time::Instant::now();
        let messages = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            client.simple_query("FETCH 1 FROM c"),
        )
        .await
        .expect("bounded poll must return")
        .expect("FETCH");
        assert!(messages
            .iter()
            .all(|message| !matches!(message, SimpleQueryMessage::Row(_))));
        assert!(started.elapsed() >= SUBSCRIPTION_FETCH_WAIT);
        client.simple_query("ROLLBACK").await.expect("ROLLBACK");

        handle.abort();
    }

    /// COMMIT must close any open cursors. After COMMIT, FETCH against the
    /// same name returns "cursor does not exist".
    #[tokio::test]
    async fn cursor_commit_closes_cursors() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        client.simple_query("COMMIT").await.expect("COMMIT");
        client.simple_query("BEGIN").await.expect("BEGIN again");

        let err = client
            .simple_query("FETCH 1 FROM c")
            .await
            .expect_err("FETCH after COMMIT must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

        handle.abort();
    }

    /// ROLLBACK closes cursors too — same reaper as COMMIT.
    #[tokio::test]
    async fn cursor_rollback_closes_cursors() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        client.simple_query("ROLLBACK").await.expect("ROLLBACK");
        client.simple_query("BEGIN").await.expect("BEGIN again");

        let err = client
            .simple_query("FETCH 1 FROM c")
            .await
            .expect_err("FETCH after ROLLBACK must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

        handle.abort();
    }

    /// Explicit CLOSE destroys the cursor while its transaction remains open.
    #[tokio::test]
    async fn cursor_close_explicit() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");
        client.simple_query("CLOSE c").await.expect("CLOSE");

        let err = client
            .simple_query("FETCH 1 FROM c")
            .await
            .expect_err("FETCH after CLOSE must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

        handle.abort();
    }

    /// `SCROLL`, `BINARY`, `WITH HOLD` all rejected at parse time.
    #[tokio::test]
    async fn cursor_unsupported_modifiers_rejected() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        for sql in [
            "DECLARE c SCROLL CURSOR FOR SUBSCRIBE prices",
            "DECLARE c BINARY CURSOR FOR SUBSCRIBE prices",
            "DECLARE c CURSOR WITH HOLD FOR SUBSCRIBE prices",
            "DECLARE c INSENSITIVE CURSOR FOR SUBSCRIBE prices",
        ] {
            let err = client
                .simple_query(sql)
                .await
                .expect_err(&format!("{sql} must fail"));
            let db_err = err.as_db_error().expect("typed PG error");
            assert_eq!(
                db_err.code().code(),
                "42601",
                "{sql}: expected parse error, got {db_err:?}"
            );
        }

        handle.abort();
    }

    /// `FETCH BACKWARD` and other reverse / absolute directions are rejected
    /// because SUBSCRIBE is forward-only.
    #[tokio::test]
    async fn cursor_backward_directions_rejected() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        for sql in [
            "FETCH PRIOR FROM c",
            "FETCH BACKWARD 1 FROM c",
            "FETCH FIRST FROM c",
            "FETCH LAST FROM c",
            "FETCH ABSOLUTE 1 FROM c",
            "FETCH RELATIVE 1 FROM c",
        ] {
            client.simple_query("BEGIN").await.expect("BEGIN");
            client
                .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
                .await
                .expect("DECLARE");
            let err = client
                .simple_query(sql)
                .await
                .expect_err(&format!("{sql} must fail"));
            let db_err = err.as_db_error().expect("typed PG error");
            assert_eq!(db_err.code().code(), "0A000", "{sql}: got {db_err:?}");
            client.simple_query("ROLLBACK").await.expect("ROLLBACK");
        }
        handle.abort();
    }

    /// `DECLARE … CURSOR FOR <SELECT …>` (regular query, not SUBSCRIBE) is
    /// not supported on pgwire.
    #[tokio::test]
    async fn cursor_for_non_subscribe_rejected() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("DECLARE c CURSOR FOR SELECT 1")
            .await
            .expect_err("DECLARE FOR SELECT must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "0A000", "got {db_err:?}");

        handle.abort();
    }

    /// FETCH against a name we never declared returns 34000 (invalid_cursor_name).
    #[tokio::test]
    async fn cursor_fetch_unknown_name_errors() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        let err = client
            .simple_query("FETCH 1 FROM nope")
            .await
            .expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "34000", "got {db_err:?}");

        handle.abort();
    }

    /// A multi-row batch with `FETCH 1` repeated must return each row in
    /// order — leftover rows persist on the cursor instead of being dropped
    /// when the response stream ends. With the bug, `FETCH 1` would consume
    /// the batch internally, return row[0], and discard row[1].
    #[tokio::test]
    async fn cursor_fetch_preserves_leftover_rows_in_one_batch() {
        let (db, addr, handle) = spawn_with_retained_data().await;
        let client = connect(addr).await;

        let src = db.source_untyped("trades").expect("source");
        let batch = arrow_array::RecordBatch::try_new(
            src.schema().clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .expect("batch");
        src.push_arrow(batch).expect("push");

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");

        let first = client
            .simple_query("FETCH 1 FROM c")
            .await
            .expect("FETCH 1");
        let r1: Vec<&str> = first
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => r.get(0),
                _ => None,
            })
            .collect();
        assert_eq!(r1, vec!["AAPL"]);

        let second = client
            .simple_query("FETCH 1 FROM c")
            .await
            .expect("FETCH 1");
        let r2: Vec<&str> = second
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => r.get(0),
                _ => None,
            })
            .collect();
        assert_eq!(r2, vec!["GOOG"]);

        client.simple_query("CLOSE c").await.expect("CLOSE");
        client.simple_query("COMMIT").await.expect("COMMIT");
        handle.abort();
    }

    /// Re-DECLAREing an open cursor name returns 42P03; user must CLOSE first.
    #[tokio::test]
    async fn cursor_duplicate_declare_rejected() {
        let (_db, addr, handle) = spawn_with_data().await;
        let client = connect(addr).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("first DECLARE");

        let err = client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect_err("duplicate DECLARE must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "42P03", "got {db_err:?}");

        client.simple_query("ROLLBACK").await.expect("ROLLBACK");
        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE c CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("re-DECLARE after transaction rollback");
        client.simple_query("CLOSE c").await.expect("CLOSE");
        client.simple_query("COMMIT").await.expect("COMMIT");

        handle.abort();
    }

    /// Cursor name lookup is case-insensitive (PG identifier folding rules).
    #[tokio::test]
    async fn cursor_name_case_insensitive() {
        let (db, addr, handle) = spawn_with_retained_data().await;
        let client = connect(addr).await;
        push_one_trade(&db, "AAPL", 1.0).await;

        client.simple_query("BEGIN").await.expect("BEGIN");
        client
            .simple_query("DECLARE MyCursor CURSOR FOR SUBSCRIBE prices")
            .await
            .expect("DECLARE");

        let messages = client
            .simple_query("FETCH 1 FROM mycursor")
            .await
            .expect("FETCH from lowercased name");
        let row_count = messages
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(row_count, 1);

        client.simple_query("CLOSE MYCURSOR").await.expect("CLOSE");
        client.simple_query("COMMIT").await.expect("COMMIT");
        handle.abort();
    }
}
