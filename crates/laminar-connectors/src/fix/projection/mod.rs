//! Projection of journaled FIX messages into Arrow batches.
//!
//! Live and replayed records take the same path: the engine re-parses the journaled message
//! with the pinned dictionary, and the compiled plan reads fields by tag. Every row carries the
//! deterministic position `(session, journal index, row ordinal)`.

mod builder;
mod plan;
mod values;

pub(crate) use plan::{compile, ProjectionPlan};

use std::sync::Arc;

use arrow_array::{BinaryArray, RecordBatch, UInt32Array};

use self::builder::ColumnBuilder;
use self::plan::{ColumnPlan, Conversion, Meta, Scope, Source};
use crate::connector::SourceRowPositions;
use crate::error::ConnectorError;
use crate::fix::engine::{DataDictionary, FieldMapRef, MessageRef};
use crate::fix::journal::ApplicationRecord;

/// A message that cannot be projected under the configured contract.
#[derive(Debug, thiserror::Error)]
#[error("FIX message {session} series {series} seq {seq} (journal record {index}): column '{column}': {reason}")]
pub(crate) struct ProjectionError {
    session: Arc<str>,
    series: u32,
    seq: u64,
    index: u64,
    column: String,
    reason: String,
}

impl From<ProjectionError> for ConnectorError {
    fn from(error: ProjectionError) -> Self {
        ConnectorError::SchemaMismatch(error.to_string())
    }
}

/// Values shared by every row of one message.
struct MessageMeta<'a> {
    index: u64,
    record: &'a ApplicationRecord,
    session: &'a str,
    gap_before: bool,
}

/// Field maps visible to one row.
#[derive(Clone, Copy)]
struct RowContext<'a> {
    header: FieldMapRef<'a>,
    body: FieldMapRef<'a>,
    trailer: FieldMapRef<'a>,
    expanded: Option<FieldMapRef<'a>>,
    entry: Option<FieldMapRef<'a>>,
    row: u32,
}

impl<'a> RowContext<'a> {
    fn map(&self, scope: Scope) -> Option<FieldMapRef<'a>> {
        match scope {
            Scope::Header => Some(self.header),
            Scope::Body => Some(self.body),
            Scope::Trailer => Some(self.trailer),
            Scope::Expanded => self.expanded,
            Scope::Entry => self.entry,
        }
    }
}

/// Reusable projection state for one source.
pub(crate) struct Projection {
    plan: ProjectionPlan,
    dictionary: Arc<DataDictionary>,
    session: Arc<str>,
    max_rows_per_message: usize,
    builders: Vec<ColumnBuilder>,
    order_keys: Vec<[u8; 8]>,
    sub_offsets: Vec<u32>,
    gap_pending: bool,
}

impl Projection {
    pub(crate) fn new(
        plan: ProjectionPlan,
        dictionary: Arc<DataDictionary>,
        session: Arc<str>,
        max_rows_per_message: usize,
    ) -> Self {
        let builders = plan.columns.iter().map(ColumnBuilder::new).collect();
        Self {
            plan,
            dictionary,
            session,
            max_rows_per_message,
            builders,
            order_keys: Vec::new(),
            sub_offsets: Vec::new(),
            gap_pending: false,
        }
    }

    /// Flag the next emitted row as following an unrecoverable sequence gap.
    pub(crate) fn mark_gap(&mut self) {
        self.gap_pending = true;
    }

    pub(crate) fn rows(&self) -> usize {
        self.sub_offsets.len()
    }

    /// Discard partially appended rows after an error.
    pub(crate) fn reset(&mut self) {
        self.builders = self.plan.columns.iter().map(ColumnBuilder::new).collect();
        self.order_keys.clear();
        self.sub_offsets.clear();
    }

    /// Append the rows of one journaled message; returns the number of rows.
    ///
    /// # Errors
    /// A projection error leaves partially appended rows; the caller must `reset`.
    pub(crate) fn push(
        &mut self,
        index: u64,
        record: &ApplicationRecord,
    ) -> Result<usize, ProjectionError> {
        let fail = |column: &str, reason: String| ProjectionError {
            session: self.session.clone(),
            series: record.series,
            seq: record.seq,
            index,
            column: column.to_string(),
            reason,
        };
        let message = self
            .dictionary
            .parse(&record.message)
            .map_err(|error| fail("<message>", error.to_string()))?;
        let view = message.view();
        let (header, body, trailer) =
            sections(view).map_err(|error| fail("<message>", error.to_string()))?;
        let entries = match self.plan.expand {
            None => None,
            Some(expand) => Some((
                expand,
                body.group_count(expand.count_tag)
                    .map_err(|error| fail("<expand>", error.to_string()))?,
            )),
        };
        let rows = match entries {
            None => 1,
            Some((expand, 0)) => usize::from(expand.empty_row),
            Some((_, count)) => count,
        };
        if rows > self.max_rows_per_message {
            return Err(fail(
                "<expand>",
                format!(
                    "{rows} rows exceed 'max.rows.per.message' ({})",
                    self.max_rows_per_message
                ),
            ));
        }
        let meta = MessageMeta {
            index,
            record,
            session: &self.session,
            gap_before: self.gap_pending,
        };
        for row in 0..rows {
            let expanded = match entries {
                Some((expand, count)) if count > 0 => Some(
                    body.group_entry(expand.count_tag, row + 1)
                        .map_err(|error| fail("<expand>", error.to_string()))?,
                ),
                _ => None,
            };
            let row_ordinal =
                u32::try_from(row).map_err(|_| fail("<expand>", "row ordinal overflow".into()))?;
            let context = RowContext {
                header,
                body,
                trailer,
                expanded,
                entry: None,
                row: row_ordinal,
            };
            for (builder, column) in self.builders.iter_mut().zip(&self.plan.columns) {
                append(builder, column, &context, &meta, self.max_rows_per_message)
                    .map_err(|reason| fail(&column.name, reason))?;
            }
            self.order_keys.push(index.to_be_bytes());
            self.sub_offsets.push(row_ordinal);
        }
        if rows > 0 {
            self.gap_pending = false;
        }
        Ok(rows)
    }

    /// Finish the accumulated rows into a batch with row positions.
    ///
    /// # Errors
    /// Returns an error when the arrays do not match the declared schema.
    pub(crate) fn finish(&mut self) -> Result<(RecordBatch, SourceRowPositions), ConnectorError> {
        let columns = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(self.plan.schema.clone(), columns)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
        let partition = BinaryArray::from_iter_values(std::iter::repeat_n(
            self.session.as_bytes(),
            self.order_keys.len(),
        ));
        let order = BinaryArray::from_iter_values(self.order_keys.iter().map(<[u8; 8]>::as_slice));
        let sub_offsets = UInt32Array::from(std::mem::take(&mut self.sub_offsets));
        self.order_keys.clear();
        let positions = SourceRowPositions::try_new(partition, order, sub_offsets)?;
        Ok((batch, positions))
    }
}

fn sections(
    view: MessageRef<'_>,
) -> Result<(FieldMapRef<'_>, FieldMapRef<'_>, FieldMapRef<'_>), crate::fix::engine::EngineError> {
    Ok((view.header()?, view.body()?, view.trailer()?))
}

fn append(
    builder: &mut ColumnBuilder,
    column: &ColumnPlan,
    context: &RowContext<'_>,
    meta: &MessageMeta<'_>,
    max_entries: usize,
) -> Result<(), String> {
    match &column.source {
        Source::Meta(kind) => append_meta(builder, &column.conversion, *kind, context, meta),
        Source::Field { scope, tag } => {
            let value = match context.map(*scope) {
                Some(map) => map.field(*tag).map_err(|error| error.to_string())?,
                None => None,
            };
            match value {
                Some(bytes) => builder.append_bytes(bytes).map_err(str::to_string),
                None if column.nullable => {
                    builder.append_null();
                    Ok(())
                }
                None => Err(format!("required tag {tag} is missing")),
            }
        }
        Source::Group {
            scope,
            count_tag,
            children,
        } => {
            let Some(map) = context.map(*scope) else {
                return null_or_missing(builder, column, *count_tag);
            };
            if map
                .field(*count_tag)
                .map_err(|error| error.to_string())?
                .is_none()
            {
                return null_or_missing(builder, column, *count_tag);
            }
            let count = map
                .group_count(*count_tag)
                .map_err(|error| error.to_string())?;
            if count > max_entries {
                return Err(format!(
                    "group {count_tag} has {count} entries, over 'max.rows.per.message'"
                ));
            }
            let list = builder
                .list_mut()
                .ok_or("group column has a non-list builder")?;
            for index in 1..=count {
                let entry = map
                    .group_entry(*count_tag, index)
                    .map_err(|error| error.to_string())?;
                let entry_context = RowContext {
                    entry: Some(entry),
                    ..*context
                };
                for (child, plan) in list.children.iter_mut().zip(children) {
                    append(child, plan, &entry_context, meta, max_entries)
                        .map_err(|reason| format!("{}: {reason}", plan.name))?;
                }
            }
            list.close_entry(count).map_err(str::to_string)
        }
    }
}

fn null_or_missing(
    builder: &mut ColumnBuilder,
    column: &ColumnPlan,
    count_tag: i32,
) -> Result<(), String> {
    if column.nullable {
        builder.append_null();
        Ok(())
    } else {
        Err(format!("required group {count_tag} is missing"))
    }
}

fn append_meta(
    builder: &mut ColumnBuilder,
    conversion: &Conversion,
    kind: Meta,
    context: &RowContext<'_>,
    meta: &MessageMeta<'_>,
) -> Result<(), String> {
    let record = meta.record;
    match kind {
        Meta::MsgType => builder.append_str(&record.msg_type),
        Meta::Session => builder.append_str(meta.session),
        Meta::SeqNum => builder.append_unsigned(u128::from(record.seq)),
        Meta::JournalIndex => builder.append_unsigned(u128::from(meta.index)),
        Meta::Series => builder.append_unsigned(u128::from(record.series)),
        Meta::Row => builder.append_unsigned(u128::from(context.row)),
        Meta::PossDup => builder.append_bool(record.poss_dup),
        Meta::PossResend => builder.append_bool(record.poss_resend),
        Meta::GapBefore => builder.append_bool(meta.gap_before && context.row == 0),
        Meta::Message => builder.append_raw(&record.message),
        Meta::ReceivedAt => {
            let Conversion::Timestamp { unit, .. } = conversion else {
                return Err("receive time needs a timestamp column".into());
            };
            builder.append_timestamp(values::nanos_to_unit(record.received_at_nanos, *unit));
            Ok(())
        }
    }
    .map_err(str::to_string)
}

#[cfg(test)]
mod tests;
