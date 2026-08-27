//! Subscription portals on pgwire: opening, schema validation, the ordered
//! envelope columns, and batch/progress row encoding.
//!
//! COMPAT: every subscription result carries six trailing `__laminar_*` metadata
//! columns in a fixed order; clients and tests pin that order.

use std::sync::Arc;

use futures::stream;
use laminar_sql::parser::SubscribeStatement;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

use laminar_db::subscription::{
    PortalFrame, SubscribeStart, SubscriptionEnvelope, SubscriptionFrameLease, SubscriptionPortal,
};
use laminar_db::LaminarDB;

use super::dispatch::user_error;
use super::encoding::{encode_field_binary, encode_field_text, field_infos, safe_format_for};

pub(crate) const SUBSCRIPTION_KIND_COLUMN: &str = "__laminar_kind";
pub(crate) const SUBSCRIPTION_EPOCH_COLUMN: &str = "__laminar_epoch";
pub(crate) const SUBSCRIPTION_CHECKPOINT_COLUMN: &str = "__laminar_checkpoint_id";
pub(crate) const SUBSCRIPTION_LOG_SEQUENCE_COLUMN: &str = "__laminar_log_sequence";
pub(crate) const SUBSCRIPTION_ROW_INDEX_COLUMN: &str = "__laminar_row_index";
pub(crate) const SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN: &str = "__laminar_through_sequence";
pub(crate) const SUBSCRIPTION_METADATA_COLUMNS: usize = 6;

pub(super) async fn open_portal_for_subscribe(
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

pub(super) fn subscription_open_error(name: &str, error: laminar_db::DbError) -> PgWireError {
    let code = match &error {
        laminar_db::DbError::StreamNotFound(_) => "42P01",
        laminar_db::DbError::Unsupported(_) => "0A000",
        laminar_db::DbError::InvalidOperation(_)
        | laminar_db::DbError::SubscriptionReplayPruned { .. }
        | laminar_db::DbError::SubscriptionEpochNotCommitted { .. } => "22023",
        laminar_db::DbError::Pipeline(_) => "53300",
        laminar_db::DbError::Subscription(error) => match error {
            laminar_db::subscription::ClusterSubscriptionError::UnsupportedPlan { .. } => "0A000",
            laminar_db::subscription::ClusterSubscriptionError::GenerationMismatch
            | laminar_db::subscription::ClusterSubscriptionError::EpochNotCommitted { .. }
            | laminar_db::subscription::ClusterSubscriptionError::ReplayPruned { .. }
            | laminar_db::subscription::ClusterSubscriptionError::ResumeTokenInvalid
            | laminar_db::subscription::ClusterSubscriptionError::ResumeTokenExpired
            | laminar_db::subscription::ClusterSubscriptionError::RetentionLost => "22023",
            laminar_db::subscription::ClusterSubscriptionError::SubscriberLagged => "54000",
            laminar_db::subscription::ClusterSubscriptionError::BackendUnavailable => "58000",
            _ => "XX000",
        },
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
pub(super) fn subscription_query_response(
    portal: SubscriptionPortal,
    result_format: Option<&Format>,
) -> Response {
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
            match s.portal.next_envelope().await {
                None => return None,
                Some(SubscriptionEnvelope {
                    frame:
                        PortalFrame::Batch {
                            batch,
                            sequence,
                            lease,
                        },
                    ..
                }) if batch.num_rows() > 0 => {
                    s.batch = Some(BatchCursor::new(batch, sequence, lease));
                }
                Some(SubscriptionEnvelope {
                    frame: PortalFrame::Batch { .. },
                    ..
                }) => {}
                Some(SubscriptionEnvelope {
                    frame:
                        PortalFrame::Barrier {
                            sequence,
                            epoch,
                            checkpoint_id,
                            through_sequence,
                        },
                    ..
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
                Some(SubscriptionEnvelope {
                    frame: PortalFrame::Lagged(n),
                    ..
                }) => {
                    let err = user_error(
                        "54000",
                        format!("subscription lagged: skipped {n} messages, terminating"),
                    );
                    s.failed = true;
                    return Some((Err(err), s));
                }
                Some(SubscriptionEnvelope {
                    frame: PortalFrame::Error { message },
                    ..
                }) => {
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

pub(super) struct BatchCursor {
    batch: arrow_array::RecordBatch,
    sequence: u64,
    row: usize,
    _lease: SubscriptionFrameLease,
}

impl BatchCursor {
    pub(super) fn new(
        batch: arrow_array::RecordBatch,
        sequence: u64,
        lease: SubscriptionFrameLease,
    ) -> Self {
        Self {
            batch,
            sequence,
            row: 0,
            _lease: lease,
        }
    }

    pub(super) fn next_row(
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

    pub(super) fn is_exhausted(&self) -> bool {
        self.row >= self.batch.num_rows()
    }
}

pub(super) fn encode_subscription_batch_row(
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

pub(super) fn subscription_field_infos(
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

pub(super) fn validate_subscription_result_format(
    format: &Format,
    columns: usize,
) -> PgWireResult<()> {
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

pub(super) fn validate_subscription_schema(schema: &arrow_schema::Schema) -> PgWireResult<()> {
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

pub(super) fn encode_subscription_progress_row(
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

pub(super) fn ensure_cached_subscription_schema(
    cached: &arrow_schema::Schema,
    current: &arrow_schema::Schema,
) -> PgWireResult<()> {
    if cached == current {
        Ok(())
    } else {
        Err(user_error("0A000", "cached result type changed"))
    }
}
