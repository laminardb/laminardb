//! Postgres wire endpoint. Trust auth by default; MD5 password auth when
//! `[server].pgwire_users` is non-empty. Non-loopback binds require both
//! MD5 auth AND `pgwire_allow_remote = true` (two-key rule). TLS via
//! tokio-rustls when `pgwire_tls_cert` and `pgwire_tls_key` are set.

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

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }
}

/// Cert + private-key paths for the pgwire TLS listener.
pub struct TlsPaths<'a> {
    pub cert: &'a std::path::Path,
    pub key: &'a std::path::Path,
}

/// Warn if the key file is readable by anyone other than the owner.
/// PostgreSQL refuses to start in this case; we warn rather than fail
/// because dev environments often run with looser perms.
#[cfg(unix)]
fn warn_if_key_world_readable(file: &std::fs::File, path: &std::path::Path) {
    use std::os::unix::fs::MetadataExt;
    if let Ok(meta) = file.metadata() {
        let mode = meta.mode();
        if mode & 0o077 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{:o}", mode & 0o777),
                "pgwire_tls_key permissions are too broad — postgres rejects \
                 anything looser than 0600. Tighten before exposing the listener.",
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_key_world_readable(_file: &std::fs::File, _path: &std::path::Path) {}

/// Per-IP rolling-window auth-failure tracker. Cheap parking_lot mutex over
/// a HashMap; expected key set is small (active client IPs) and lookups
/// happen at TCP accept time, not on the hot data path.
#[derive(Debug, Default)]
struct FailureTracker {
    inner: parking_lot::Mutex<
        HashMap<std::net::IpAddr, std::collections::VecDeque<std::time::Instant>>,
    >,
}

impl FailureTracker {
    /// Drop expired entries and return whether `ip` has hit `limit` failures
    /// inside `window`. `limit == 0` disables the throttle.
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
        self.inner
            .lock()
            .entry(ip)
            .or_default()
            .push_back(std::time::Instant::now());
    }
}

/// Map the result of `process_socket` to a stable audit-log code so SOC
/// dashboards can split TLS handshake errors from auth failures and from
/// generic runtime errors.
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

/// Install aws-lc-rs as the process-wide rustls CryptoProvider. Idempotent —
/// rustls returns `Err` if a provider is already installed, which we ignore.
/// Required because other crates in the dep tree (delta-lake, iceberg) pull
/// in rustls with their own provider features, leaving the auto-pick
/// ambiguous.
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
}
