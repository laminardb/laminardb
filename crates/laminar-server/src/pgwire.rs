//! Postgres wire endpoint. Trust by default; MD5 with `pgwire_users`;
//! TLS with `pgwire_tls_cert` + `pgwire_tls_key`. Non-loopback binds
//! require `pgwire_allow_remote = true`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use sqlparser::ast::{Expr, FunctionArguments, SelectItem, Set, SetExpr, Statement};
use tokio::net::TcpListener;
use tracing::{info, warn};

use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use laminar_db::LaminarDB;

use crate::config::Secret;
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
                    let start = match s.as_of_epoch {
                        Some(n) => SubscribeStart::AsOfEpoch(n),
                        None => SubscribeStart::Tail,
                    };
                    let portal = self
                        .db
                        .open_subscription(&name, s.filter_sql.as_deref(), start)
                        .await
                        .map_err(|e| user_error("42P01", format!("SUBSCRIBE '{name}': {e}")))?;
                    portal_to_response(portal, None)
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

/// Stream a `SubscriptionPortal`'s frames as a pgwire `Response::Query`.
///
/// `result_format` is `None` on the SimpleQuery path (text always) and
/// `Some(format)` on the extended-query path, where the `Bind` message
/// supplies per-column format codes. Binary-encoded fields use
/// `postgres-types` `ToSql` impls dispatched per Arrow type.
fn portal_to_response(portal: SubscriptionPortal, result_format: Option<Format>) -> Response {
    let fields = Arc::new(field_infos(&portal.schema(), result_format.as_ref()));

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
                    // Barriers are dropped: PG has no out-of-band marker
                    // for checkpoint epochs on either protocol path. Reads
                    // are at-most-once-after-epoch via `AS OF EPOCH n`.
                    PortalFrame::Barrier {
                        epoch,
                        checkpoint_id,
                    } => {
                        tracing::trace!(epoch, checkpoint_id, "pgwire SUBSCRIBE: dropping barrier")
                    }
                    // Lag → typed error so the disconnect isn't silent.
                    PortalFrame::Lagged(n) => {
                        let err = user_error(
                            "54000",
                            format!("subscription lagged: skipped {n} messages, terminating"),
                        );
                        return Some((Err(err), (portal, rows, idx, fields)));
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

/// Build pgwire `FieldInfo`s from an Arrow schema. `result_format` (from a
/// `Bind`) sets per-column text/binary; `None` defaults all-text.
fn field_infos(schema: &arrow_schema::Schema, result_format: Option<&Format>) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let format = result_format.map_or(FieldFormat::Text, |rf| rf.format_for(i));
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

fn encode_row(
    batch: &arrow_array::RecordBatch,
    row: usize,
    fields: &Arc<Vec<FieldInfo>>,
    formatters: &[arrow_cast::display::ArrayFormatter<'_>],
) -> PgWireResult<pgwire::messages::data::DataRow> {
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
    if col.is_null(row) {
        enc.encode_field(&None::<&str>)
    } else {
        enc.encode_field(&Some(formatter.value(row).to_string()))
    }
}

/// Binary-encode a single Arrow value via `postgres-types` `ToSql`.
///
/// Coverage: Int{8,16,32,64}, UInt{8,16,32,64}, Float{32,64}, Bool,
/// Utf8/LargeUtf8, Timestamp (any unit, naive), Date32, Date64. UInt64
/// is widened to INT8 with saturation since Postgres has no unsigned 64.
/// Any other column type yields `0A000` — client should request text.
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
            // PG has no unsigned 64; saturate so we never wrap.
            let v = col.as_primitive::<UInt64Type>().value(row);
            enc.encode_field(&Some(i64::try_from(v).unwrap_or(i64::MAX)))
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
        _ => Type::TEXT,
    }
}

/// Per-call salt + stored plaintext for the MD5 challenge flow.
#[derive(Debug)]
struct LaminarAuthSource {
    users: Arc<HashMap<String, Secret>>,
}

#[async_trait]
impl AuthSource for LaminarAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().unwrap_or("");
        // Indistinguishable from a wrong-password failure: both branches must
        // surface the same wire error so a client can't probe which usernames
        // are configured. pgwire emits exactly this variant on bad password.
        let password = self
            .users
            .get(user)
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_string()))?;
        let salt: [u8; 4] = rand::random();
        let expected = hash_md5_password(user, password.expose(), &salt);
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

pub struct LaminarHandlerFactory {
    handler: Arc<LaminarPgwireHandler>,
    startup: Arc<StartupAuth>,
}

impl LaminarHandlerFactory {
    fn new(db: Arc<LaminarDB>, users: HashMap<String, Secret>) -> Self {
        let handler = Arc::new(LaminarPgwireHandler { db });
        let startup = if users.is_empty() {
            Arc::new(StartupAuth::Trust(Arc::clone(&handler)))
        } else {
            let auth = LaminarAuthSource {
                users: Arc::new(users),
            };
            let md5 = Md5PasswordAuthStartupHandler::new(
                Arc::new(auth),
                Arc::new(DefaultServerParameterProvider::default()),
            );
            Arc::new(StartupAuth::Md5(Arc::new(md5)))
        };
        Self { handler, startup }
    }
}

impl PgWireServerHandlers for LaminarHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
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
                let (schema, _) = self.db.lookup_subscription_schema(&name).ok_or_else(|| {
                    user_error("42P01", format!("SUBSCRIBE '{name}': stream not found"))
                })?;
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
            LaminarStmt::Subscribe { schema, .. } => Ok(field_infos(schema, column_format)),
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
        _max_rows: usize,
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
                ..
            } => {
                let start = match as_of_epoch {
                    Some(n) => SubscribeStart::AsOfEpoch(*n),
                    None => SubscribeStart::Tail,
                };
                let sub = self
                    .db
                    .open_subscription(name, filter_sql.as_deref(), start)
                    .await
                    .map_err(|e| user_error("42P01", format!("SUBSCRIBE '{name}': {e}")))?;
                Ok(portal_to_response(
                    sub,
                    Some(portal.result_column_format.clone()),
                ))
            }
            LaminarStmt::Show(cmd) => engine_metadata_response(&self.db, &show_sql(cmd)).await,
            LaminarStmt::Standard(s) => standard_response(&self.db, *s.clone()),
        }
    }

    /// Per-Sync portal cleanup: only the unnamed portal is destroyed.
    ///
    /// The pgwire 0.39 default `on_sync` calls `clear_portals()`, which wipes
    /// every named portal on the connection. PostgreSQL keeps named portals
    /// alive until `Close` or end-of-transaction, so the default would break
    /// any client that does `Bind named_portal; Sync; Execute named_portal;`
    /// — the standard JDBC / asyncpg / tokio-postgres pattern for chunked
    /// fetches via `setFetchSize` / `query_portal`.
    async fn on_sync<C>(
        &self,
        client: &mut C,
        _message: pgwire::messages::extendedquery::Sync,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        use futures::SinkExt;
        use pgwire::messages::response::ReadyForQuery;

        // Drop only the unnamed portal; named portals survive Sync.
        client.portal_store().rm_portal("");

        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                client.transaction_status(),
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }
}

pub struct TlsPaths<'a> {
    pub cert: &'a std::path::Path,
    pub key: &'a std::path::Path,
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

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::Http(format!("rustls server config: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
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

    let tls_acceptor = match tls {
        Some(paths) => Some(load_tls_acceptor(paths)?),
        None => None,
    };

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::Http(format!("pgwire bind {addr}: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| ServerError::Http(format!("pgwire local_addr: {e}")))?;

    let factory = Arc::new(LaminarHandlerFactory::new(db, users));
    let tls_mode = if tls_acceptor.is_some() { "on" } else { "off" };
    if auth_mode == "trust" {
        warn!(
            addr = %local_addr,
            tls = tls_mode,
            "pgwire listening with TRUST auth — any client reaching this address is admin",
        );
    } else {
        info!(addr = %local_addr, auth = auth_mode, tls = tls_mode, "pgwire listening");
    }

    // Track per-connection tasks so abort on the outer JoinHandle stops
    // active sessions in addition to the accept loop.
    let failures = Arc::new(FailureTracker::default());
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
                            if sessions.len() >= max_connections {
                                tracing::info!(
                                    target: "audit",
                                    event = "pgwire.connection_rejected",
                                    peer = %peer,
                                    reason = "max_connections",
                                    in_flight = sessions.len(),
                                );
                                drop(sock);
                                continue;
                            }
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
                            let factory_ref = Arc::clone(&factory);
                            let tls_ref = tls_acceptor.clone();
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
                                let result = process_socket(sock, tls_ref, factory_ref).await;
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
        let db = Arc::new(LaminarDB::open().expect("db opens"));
        let err = serve(db, "0.0.0.0:0", HashMap::new(), false, None, 256, 10)
            .await
            .expect_err("trust + 0.0.0.0 must fail");
        assert!(err.to_string().contains("trust auth"), "got: {err}");
    }

    #[tokio::test]
    async fn serve_rejects_remote_bind_without_explicit_optin() {
        let db = Arc::new(LaminarDB::open().expect("db opens"));
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
}

#[cfg(test)]
mod integration_tests {
    //! End-to-end pgwire driven by `tokio_postgres` against an in-process
    //! `LaminarDB`. Verifies the wire-protocol surface — handshake, SimpleQuery
    //! dispatch, error reporting — that unit tests can't reach. Engine-level
    //! row flow is covered in `laminar-db`'s `db::tests`.

    use std::collections::HashMap;
    use std::sync::Arc;

    use laminar_db::LaminarDB;
    use tokio_postgres::{NoTls, SimpleQueryMessage};

    use super::Secret;

    async fn spawn_server_with(
        users: HashMap<String, Secret>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let db = Arc::new(LaminarDB::open().expect("db opens"));
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
    async fn subscribe_unknown_name_returns_pg_error() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("SUBSCRIBE no_such_view")
            .await
            .expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("no_such_view"),
            "message: {}",
            db_err.message()
        );

        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_with_valid_where_is_accepted() {
        // SUBSCRIBE never returns CommandComplete, so a successful compile
        // shows up as a timeout. Anything else — Ok(Ok) or Ok(Err) — is a
        // regression.
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let r = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            client.simple_query("SUBSCRIBE prices WHERE symbol = 'AAPL'"),
        )
        .await;

        assert!(
            r.is_err(),
            "subscribe must stay open until timeout, got: {r:?}"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_with_unknown_column_in_where_returns_pg_error() {
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("SUBSCRIBE prices WHERE no_such_col > 1")
            .await
            .expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("no_such_col"),
            "filter error must name the bad column, got: {}",
            db_err.message()
        );

        handle.abort();
    }

    #[tokio::test]
    async fn subscribe_as_of_unretained_returns_pg_error() {
        // No retention configured on the `prices` MV from the default setup,
        // so AS OF EPOCH 1 must come back as a typed PG error.
        let (addr, handle) = spawn_server().await;
        let client = connect(addr).await;

        let err = client
            .simple_query("SUBSCRIBE prices AS OF EPOCH 1")
            .await
            .expect_err("must fail");
        let db_err = err.as_db_error().expect("typed PG error");
        assert!(
            db_err.message().contains("no longer retained"),
            "message: {}",
            db_err.message()
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

    async fn md5_users() -> HashMap<String, Secret> {
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
        let (addr, handle) = spawn_server_with(md5_users().await).await;

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
    async fn md5_auth_rejects_wrong_password() {
        let (addr, handle) = spawn_server_with(md5_users().await).await;

        let err = connect_with_password(addr, "alice", "not-the-password")
            .await
            .expect_err("auth must fail");

        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

        handle.abort();
    }

    #[tokio::test]
    async fn md5_auth_rejects_unknown_user() {
        let (addr, handle) = spawn_server_with(md5_users().await).await;

        let err = connect_with_password(addr, "mallory", "anything")
            .await
            .expect_err("auth must fail");

        let db_err = err.as_db_error().expect("typed PG error");
        assert_eq!(db_err.code().code(), "28P01", "got: {db_err:?}");

        handle.abort();
    }

    #[tokio::test]
    async fn connection_cap_drops_excess_clients() {
        // Cap of 1; first client occupies the slot, second is dropped.
        let db = Arc::new(LaminarDB::open().expect("db opens"));
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

        // First client occupies the slot via SUBSCRIBE (stays open until drop).
        let first = connect(addr).await;
        let _bg = tokio::spawn(async move {
            let _ = first.simple_query("SUBSCRIBE prices").await;
        });
        // Give the server a moment to register the session in the JoinSet.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Second connect: server accepts the TCP, then closes it because the
        // cap is hit. tokio_postgres surfaces this as an IO error during
        // startup. Exact string varies; just assert it failed.
        let conn_str = format!(
            "host={} port={} user=any dbname=laminardb",
            addr.ip(),
            addr.port()
        );
        let result = tokio_postgres::connect(&conn_str, NoTls).await;
        assert!(result.is_err(), "second connect must be refused");

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
        let db = Arc::new(LaminarDB::open().expect("db opens"));
        db.start().await.expect("db starts");
        let err = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
            }),
            256,
            10,
        )
        .await
        .expect_err("expired cert must be rejected");
        assert!(err.to_string().contains("expired"), "got: {err}");
    }

    #[tokio::test]
    async fn tls_handshake_succeeds() {
        let (_dir, cert_path, key_path) = self_signed_pem();
        let db = Arc::new(LaminarDB::open().expect("db opens"));
        db.start().await.expect("db starts");
        let (addr, handle) = super::serve(
            Arc::clone(&db),
            "127.0.0.1:0",
            HashMap::new(),
            false,
            Some(super::TlsPaths {
                cert: &cert_path,
                key: &key_path,
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
        let db = Arc::new(LaminarDB::open().expect("db opens"));
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
        assert_eq!(cols.len(), 2, "expected 2 columns, got {}", cols.len());
        assert_eq!(cols[0].name(), "symbol");
        assert_eq!(cols[1].name(), "price");
        assert_eq!(cols[0].type_(), &tokio_postgres::types::Type::VARCHAR);
        assert_eq!(cols[1].type_(), &tokio_postgres::types::Type::FLOAT8);

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
        let db = Arc::new(LaminarDB::open().expect("db opens"));
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
}
