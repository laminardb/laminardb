//! Ad-hoc `SELECT` on pgwire: per-connection read settings, the epoch of the connection's last
//! committed read, and rows streamed from the query as the client reads them.
//!
//! COMPAT: the `laminar.*` setting names and values, and the SQLSTATE of each read error, are the
//! observable wire contract.

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::{stream, StreamExt};
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use sqlparser::ast::{Expr, Set, Statement, Value};

use laminar_db::{CommittedRead, CommittedReadError, DbError, LaminarDB, QueryRows, ReadLevel};

use super::dispatch::user_error;
use super::encoding::{encode_row, field_infos, text_response};

/// `latest` or `committed`: how a `SELECT` reads materialized views.
pub(super) const READ_LEVEL: &str = "laminar.read_level";
/// The earliest epoch a committed read may observe.
pub(super) const MIN_EPOCH: &str = "laminar.min_epoch";
/// Read-only: the epoch of this connection's last committed read.
pub(super) const READ_EPOCH: &str = "laminar.read_epoch";

/// Read settings of one connection. A committed read observes no earlier epoch than the
/// connection's minimum epoch or its own previous committed read.
#[derive(Debug, Default)]
pub(super) struct ReadSession {
    settings: parking_lot::Mutex<ReadSettings>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ReadSettings {
    /// `None` reads at the database's default level.
    committed: Option<bool>,
    min_epoch: u64,
    read_epoch: Option<u64>,
}

impl ReadSession {
    fn level(&self, db: &LaminarDB) -> ReadLevel {
        let settings = *self.settings.lock();
        let committed = settings
            .committed
            .unwrap_or_else(|| matches!(db.default_read_level(), ReadLevel::Committed(_)));
        if !committed {
            return ReadLevel::Latest;
        }
        match settings.min_epoch.max(settings.read_epoch.unwrap_or(0)) {
            0 => ReadLevel::Committed(CommittedRead::Latest),
            floor => ReadLevel::Committed(CommittedRead::AtLeast(floor)),
        }
    }

    fn observed(&self, epoch: u64) {
        let mut settings = self.settings.lock();
        settings.read_epoch = Some(settings.read_epoch.map_or(epoch, |read| read.max(epoch)));
    }

    fn set(&self, db: &LaminarDB, name: &str, value: &str) -> PgWireResult<()> {
        let value = value.to_ascii_lowercase();
        let mut settings = self.settings.lock();
        match (name, value.as_str()) {
            (READ_LEVEL, "default") => settings.committed = None,
            (READ_LEVEL, "committed") => settings.committed = Some(true),
            (READ_LEVEL, "latest") if matches!(db.default_read_level(), ReadLevel::Latest) => {
                settings.committed = Some(false);
            }
            (READ_LEVEL, "latest") => {
                return Err(user_error(
                    "0A000",
                    "a cluster node holds only its part of each materialized view; it reads them \
                     at a committed checkpoint",
                ));
            }
            (READ_LEVEL, _) => {
                return Err(user_error(
                    "22023",
                    format!("{READ_LEVEL} must be 'latest', 'committed' or 'default'"),
                ));
            }
            (MIN_EPOCH, "default") => settings.min_epoch = 0,
            (MIN_EPOCH, epoch) => {
                settings.min_epoch = epoch.parse().map_err(|_| {
                    user_error(
                        "22023",
                        format!("{MIN_EPOCH} must be a non-negative epoch or 'default'"),
                    )
                })?;
            }
            (READ_EPOCH, _) => {
                return Err(user_error("55P02", format!("{READ_EPOCH} is read-only")));
            }
            _ => return Err(unknown_setting(name)),
        }
        Ok(())
    }

    fn show(&self, db: &LaminarDB, name: &str) -> PgWireResult<String> {
        let settings = *self.settings.lock();
        Ok(match name {
            READ_LEVEL => match self.level(db) {
                ReadLevel::Latest => "latest".into(),
                ReadLevel::Committed(_) => "committed".into(),
            },
            MIN_EPOCH => settings.min_epoch.to_string(),
            READ_EPOCH => settings
                .read_epoch
                .map(|epoch| epoch.to_string())
                .unwrap_or_default(),
            _ => return Err(unknown_setting(name)),
        })
    }
}

fn unknown_setting(name: &str) -> PgWireError {
    user_error(
        "42704",
        format!("unrecognized configuration parameter \"{name}\""),
    )
}

/// The connection's read settings.
pub(super) fn read_session<C: pgwire::api::ClientInfo>(client: &C) -> Arc<ReadSession> {
    client
        .session_extensions()
        .get_or_insert_with(ReadSession::default)
}

/// Answer `SET` and `SHOW` of a `laminar.*` setting; `None` for any other statement.
pub(super) fn session_statement(
    db: &LaminarDB,
    session: &ReadSession,
    statement: &Statement,
) -> Option<PgWireResult<Response>> {
    match statement {
        Statement::Set(Set::SingleAssignment {
            variable, values, ..
        }) => {
            let name = variable.to_string().to_ascii_lowercase();
            if !name.starts_with("laminar.") {
                return None;
            }
            let [value] = values.as_slice() else {
                return Some(Err(user_error("22023", format!("{name} takes one value"))));
            };
            Some(
                session
                    .set(db, &name, &setting_value(value))
                    .map(|()| Response::Execution(Tag::new("SET"))),
            )
        }
        Statement::ShowVariable { variable } => {
            let name = setting_name(variable)?;
            Some(
                session
                    .show(db, &name)
                    .map(|value| text_response(&name, Type::VARCHAR, value)),
            )
        }
        _ => None,
    }
}

/// The column `SHOW` of a `laminar.*` setting returns, which `Describe` reports; `None` for any
/// other statement.
pub(super) fn setting_fields(statement: &Statement) -> Option<Vec<FieldInfo>> {
    let Statement::ShowVariable { variable } = statement else {
        return None;
    };
    let name = setting_name(variable)?;
    Some(vec![FieldInfo::new(
        name,
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )])
}

fn setting_name(variable: &[sqlparser::ast::Ident]) -> Option<String> {
    let name = variable
        .iter()
        .map(|part| part.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".");
    name.starts_with("laminar.").then_some(name)
}

fn setting_value(value: &Expr) -> String {
    match value {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::Number(text, _) => text.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Run the query `sql` at the connection's read level and stream its rows. `described` is the
/// schema a `Describe` reported, which the rows must still have.
pub(super) async fn select_response(
    db: &LaminarDB,
    session: &ReadSession,
    sql: &str,
    result_format: Option<&Format>,
    described: Option<&arrow_schema::SchemaRef>,
) -> PgWireResult<Response> {
    let read = db.read(sql, session.level(db)).await.map_err(read_error)?;
    if let Some(cut) = read.cut {
        session.observed(cut.epoch);
    }
    let schema = read.rows.schema();
    if described.is_some_and(|described| !same_columns(described, &schema)) {
        return Err(user_error(
            "0A000",
            "cached plan must not change result type",
        ));
    }
    let fields = Arc::new(field_infos(&schema, result_format));
    Ok(Response::Query(QueryResponse::new(
        Arc::clone(&fields),
        data_rows(read.rows, fields),
    )))
}

fn same_columns(left: &arrow_schema::Schema, right: &arrow_schema::Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| {
                left.name() == right.name() && left.data_type() == right.data_type()
            })
}

/// Encoded rows, one batch at a time as the client reads them.
fn data_rows(
    rows: QueryRows,
    fields: Arc<Vec<FieldInfo>>,
) -> impl futures::Stream<Item = PgWireResult<DataRow>> + Send + 'static {
    stream::unfold(Some(rows), |rows| async move {
        let mut rows = rows?;
        match rows.next_batch().await {
            Ok(Some(batch)) => Some((Ok(batch), Some(rows))),
            Ok(None) => None,
            Err(error) => Some((Err(read_error(error)), None)),
        }
    })
    .flat_map(move |batch| {
        stream::iter(match batch {
            Ok(batch) => encode_batch(&batch, &fields),
            Err(error) => vec![Err(error)],
        })
    })
}

fn encode_batch(batch: &RecordBatch, fields: &Arc<Vec<FieldInfo>>) -> Vec<PgWireResult<DataRow>> {
    let options = arrow_cast::display::FormatOptions::default();
    let formatters = match batch
        .columns()
        .iter()
        .map(|column| arrow_cast::display::ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(formatters) => formatters,
        Err(error) => return vec![Err(user_error("XX000", error.to_string()))],
    };
    (0..batch.num_rows())
        .map(|row| encode_row(batch, row, fields, &formatters))
        .collect()
}

/// The SQLSTATE of a failed read.
pub(super) fn read_error(error: DbError) -> PgWireError {
    let code = match &error {
        DbError::CommittedRead(error) => match error {
            CommittedReadError::CheckpointingDisabled
            | CommittedReadError::NotCommitted {
                requested: None, ..
            }
            | CommittedReadError::ViewNotCommitted { .. } => "55000",
            CommittedReadError::NotCommitted {
                requested: Some(_), ..
            }
            | CommittedReadError::DeadlineExceeded { .. } => "57014",
            CommittedReadError::Retired { .. } => "22023",
            CommittedReadError::LiveTable { .. } => "0A000",
            CommittedReadError::PinCapacity => "53000",
        },
        DbError::DataFusion(error) if error.to_string().contains("not found") => "42P01",
        DbError::Sql(_)
        | DbError::SqlParse(_)
        | DbError::DataFusion(_)
        | DbError::QueryPipeline { .. } => "42601",
        DbError::InvalidOperation(_) | DbError::Unsupported(_) => "0A000",
        DbError::Shutdown => "57P01",
        _ => "XX000",
    };
    user_error(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use laminar_sql::parser::{parse_streaming_sql, StreamingStatement};

    use super::*;

    fn statement(sql: &str) -> Statement {
        match parse_streaming_sql(sql).unwrap().pop().unwrap() {
            StreamingStatement::Standard(statement) => *statement,
            other => panic!("{sql} parsed as {other:?}"),
        }
    }

    #[tokio::test]
    async fn settings_choose_the_level_and_raise_the_epoch_floor() {
        let db = LaminarDB::open().unwrap();
        let session = ReadSession::default();
        assert_eq!(session.level(&db), ReadLevel::Latest);
        for sql in [
            "SET laminar.read_level = 'committed'",
            "SET laminar.min_epoch TO 7",
        ] {
            assert!(session_statement(&db, &session, &statement(sql))
                .unwrap()
                .is_ok());
        }
        assert_eq!(
            session.level(&db),
            ReadLevel::Committed(CommittedRead::AtLeast(7))
        );
        session.observed(9);
        session.observed(8);
        assert_eq!(
            session.level(&db),
            ReadLevel::Committed(CommittedRead::AtLeast(9)),
            "a later read observes no earlier epoch than an earlier one"
        );
        assert_eq!(session.show(&db, READ_EPOCH).unwrap(), "9");
        assert!(session_statement(&db, &session, &statement("SHOW laminar.read_level")).is_some());
        assert!(session_statement(&db, &session, &statement("SET search_path = public")).is_none());

        for (sql, code) in [
            ("SET laminar.read_level = 'eventual'", "22023"),
            ("SET laminar.min_epoch = -1", "22023"),
            ("SET laminar.read_epoch = 3", "55P02"),
            ("SET laminar.colour = 'blue'", "42704"),
        ] {
            let Some(Err(PgWireError::UserError(info))) =
                session_statement(&db, &session, &statement(sql))
            else {
                panic!("{sql} was accepted");
            };
            assert_eq!(info.code, code, "{sql}");
        }
        db.shutdown().await.unwrap();
    }
}
