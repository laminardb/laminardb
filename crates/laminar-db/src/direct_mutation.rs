//! Direct delivery of source batches to sinks that read a source by name.
//!
//! Append sources pass their visible columns through unchanged. A keyed-upsert source may feed
//! sinks directly when nothing else reads it: each `Put` row becomes `_op = 'U'` and each
//! key-only `Tombstone` becomes `_op = 'D'`, which the sink applies by the source primary key
//! with its existing changelog writer. No previous-row state exists on this route, so filters,
//! projections, and aggregates over a keyed source stay rejected: they would need retractions
//! of rows the engine never retained.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::connector::{source_mutations, strip_source_row_positions, SourceMutation};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::connector_manager::{SinkRegistration, StreamRegistration};

/// Operation column appended to a direct mutation sink's input.
pub(crate) const OP_COLUMN: &str = "_op";

/// Changelog column names a direct mutation source schema must not declare, because the sink
/// side would interpret them as engine operations.
const RESERVED_SOURCE_COLUMNS: &[&str] = &["_op", "_ts_ms", "__weight"];

/// The sink input schema for a direct mutation source: its visible fields plus `_op`.
pub(crate) fn sink_input_schema(source_schema: &SchemaRef) -> SchemaRef {
    let mut fields = source_schema.fields().to_vec();
    fields.push(Arc::new(Field::new(OP_COLUMN, DataType::Utf8, false)));
    Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ))
}

/// Convert one ingress-validated source batch (visible fields, optional mutation column, row
/// positions) into the sink input batch. Visible buffers are shared, not copied.
///
/// # Errors
/// Returns an error when the batch metadata is malformed or its fields differ from the sink
/// input schema.
pub(crate) fn sink_batch(
    routed: &RecordBatch,
    sink_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let visible = sink_schema.fields().len() - 1;
    let mutations = source_mutations(routed).map_err(|error| error.to_string())?;
    let rows = routed.num_rows();
    let operations: StringArray = match mutations {
        Some(mutations) => (0..rows)
            .map(|row| match mutations.get(row) {
                Some(SourceMutation::Tombstone) => Some("D"),
                Some(SourceMutation::Put) | None => Some("U"),
            })
            .collect(),
        None => std::iter::repeat_n(Some("U"), rows).collect(),
    };
    if routed.num_columns() < visible {
        return Err("direct mutation batch lost its visible columns".into());
    }
    let mut columns: Vec<ArrayRef> = routed.columns()[..visible].to_vec();
    columns.push(Arc::new(operations));
    RecordBatch::try_new(Arc::clone(sink_schema), columns)
        .map_err(|error| format!("direct mutation batch does not match its sink schema: {error}"))
}

/// How a source read directly by sinks reaches them.
pub(crate) enum DirectSinkInput {
    /// Visible columns, unchanged.
    Append,
    /// Keyed mutations as `_op` rows with this sink input schema.
    KeyedMutations(SchemaRef),
}

/// Per-cycle direct sink input, owned by the pipeline callback.
#[derive(Default)]
pub(crate) struct DirectSinkInputs {
    inputs: FxHashMap<Arc<str>, DirectSinkInput>,
    /// The current cycle's converted batches, consumed by the next sink publication.
    staged: FxHashMap<Arc<str>, Vec<RecordBatch>>,
}

impl DirectSinkInputs {
    pub(crate) fn new(inputs: FxHashMap<Arc<str>, DirectSinkInput>) -> Self {
        Self {
            inputs,
            staged: FxHashMap::default(),
        }
    }

    /// Convert this cycle's staged source batches for their direct sinks.
    ///
    /// # Errors
    /// Returns a recovery error when a batch's source metadata is malformed.
    pub(crate) fn stage(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), crate::pipeline::CycleError> {
        self.staged.clear();
        for (source, input) in &self.inputs {
            let Some(batches) = source_batches.get(source) else {
                continue;
            };
            let converted = batches
                .iter()
                .filter(|batch| batch.num_rows() > 0)
                .map(|batch| match input {
                    DirectSinkInput::Append => {
                        strip_source_row_positions(batch).map_err(|error| error.to_string())
                    }
                    DirectSinkInput::KeyedMutations(schema) => sink_batch(batch, schema),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    crate::pipeline::CycleError::Recovery(format!(
                        "direct sink input '{source}': {error}"
                    ))
                })?;
            if !converted.is_empty() {
                self.staged.insert(Arc::clone(source), converted);
            }
        }
        Ok(())
    }

    pub(crate) fn discard_staged(&mut self) {
        self.staged.clear();
    }

    /// The cycle results extended with the staged direct sink input, or `None` when nothing is
    /// staged. Consumes the stage.
    pub(crate) fn take_merged(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Option<FxHashMap<Arc<str>, Vec<RecordBatch>>> {
        if self.staged.is_empty() {
            return None;
        }
        let mut merged = results.clone();
        merged.extend(self.staged.drain());
        Some(merged)
    }
}

/// Connector sinks reading `source` directly, or `None` when anything else reads it (a stream,
/// a sink query, a connector-less catalog sink) or nothing does.
pub(crate) fn direct_sink_consumers<'a>(
    source: &str,
    stream_regs: impl IntoIterator<Item = &'a StreamRegistration>,
    sink_regs: impl IntoIterator<Item = &'a SinkRegistration>,
) -> Option<Vec<&'a SinkRegistration>> {
    for stream in stream_regs {
        let joined = stream.join_config.as_deref().is_some_and(|joins| {
            joins.iter().any(|join| match join {
                laminar_sql::translator::JoinOperatorConfig::Temporal(config) => {
                    config.left_table == source || config.right_table == source
                }
                laminar_sql::translator::JoinOperatorConfig::StreamStream(config) => {
                    config.left_table == source || config.right_table == source
                }
                laminar_sql::translator::JoinOperatorConfig::Lookup(_) => false,
            })
        });
        if joined
            || crate::sql_analysis::extract_table_references(&stream.query_sql).contains(source)
        {
            return None;
        }
    }
    let mut sinks = Vec::new();
    for sink in sink_regs {
        if sink.query_inputs.iter().any(|input| input == source)
            || (sink.input == source && sink.connector_type.is_none())
        {
            return None;
        }
        if sink.input == source {
            sinks.push(sink);
        }
    }
    (!sinks.is_empty()).then_some(sinks)
}

/// Validate the shape of a source admitted to the direct mutation route.
///
/// # Errors
/// Returns the reason the source schema, key, or consumer sinks cannot carry key-only deletes.
pub(crate) fn validate_route_shape(
    source: &str,
    schema: &Schema,
    primary_key: &[String],
    sinks: &[&SinkRegistration],
) -> Result<(), String> {
    if primary_key.is_empty() {
        return Err(format!(
            "direct mutation source '{source}' requires an explicit PRIMARY KEY"
        ));
    }
    for field in schema.fields() {
        if RESERVED_SOURCE_COLUMNS
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
        {
            return Err(format!(
                "direct mutation source '{source}' cannot declare engine changelog column '{}'",
                field.name()
            ));
        }
        if !primary_key.contains(field.name()) && !field.is_nullable() {
            return Err(format!(
                "direct mutation source '{source}' non-key column '{}' must be nullable: \
                 deletes carry only the key",
                field.name()
            ));
        }
    }
    if let Some(sink) = sinks.iter().find(|sink| sink.filter_expr.is_some()) {
        return Err(format!(
            "sink '{}' cannot filter mutation source '{source}': a filter on mutable values \
             would drop the deletes and replacements that leave its predicate",
            sink.name
        ));
    }
    Ok(())
}

/// Require a sink's keyed-mutation key to be exactly the source primary key.
///
/// # Errors
/// Returns the reason the sink cannot apply this source's keyed mutations.
pub(crate) fn validate_sink_key(
    sink: &str,
    source: &str,
    source_key: &[String],
    sink_key: Option<Vec<String>>,
) -> Result<(), String> {
    let Some(sink_key) = sink_key else {
        return Err(format!(
            "sink '{sink}' cannot apply keyed puts and key-only deletes from '{source}'; use \
             PostgreSQL write.mode=upsert with changelog.mode=true, Delta write.mode=upsert, or \
             MongoDB cdc_replay over output.mode=history"
        ));
    };
    let expected: FxHashSet<&str> = source_key.iter().map(String::as_str).collect();
    let actual: FxHashSet<&str> = sink_key.iter().map(String::as_str).collect();
    if expected != actual || sink_key.len() != source_key.len() {
        return Err(format!(
            "sink '{sink}' key {sink_key:?} must equal the PRIMARY KEY {source_key:?} of \
             mutation source '{source}'"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryArray, Int64Array, UInt32Array};
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceRowPositionCapability, SourceRowPositions,
    };

    fn sink(
        name: &str,
        input: &str,
        query_inputs: &[&str],
        filter: Option<&str>,
    ) -> SinkRegistration {
        SinkRegistration {
            schema_binding: None,
            catalog_generation: 1,
            name: name.into(),
            input: input.into(),
            query_inputs: query_inputs
                .iter()
                .map(|input| (*input).to_string())
                .collect(),
            connector_type: Some("postgres-sink".into()),
            connector_options: std::collections::HashMap::new(),
            format: None,
            format_options: std::collections::HashMap::new(),
            filter_expr: filter.map(str::to_string),
        }
    }

    fn stream(query_sql: &str) -> StreamRegistration {
        StreamRegistration {
            name: "copy".into(),
            query_sql: query_sql.into(),
            emit_clause: None,
            window_config: None,
            order_config: None,
            join_config: None,
            has_analytic: false,
            has_frame: false,
            incremental: false,
            subscription_output: None,
            subscription_retention_bytes: 0,
            catalog_generation: 1,
            subscription_certificate: None,
        }
    }

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn encoded(mutations: Option<Vec<SourceMutation>>) -> RecordBatch {
        let schema = source_schema();
        let records = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
            ],
        )
        .unwrap();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from_iter_values([b"p", b"p", b"p"]),
            BinaryArray::from_iter_values([[0_u8], [1], [2]]),
            UInt32Array::from(vec![0, 0, 0]),
        )
        .unwrap();
        let mut batch = SourceBatch::positioned(records, positions).unwrap();
        if let Some(mutations) = mutations {
            batch = batch.with_mutations(mutations).unwrap();
        }
        batch
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &schema_with_source_row_positions(&schema).unwrap(),
                &schema_with_source_mutations_and_row_positions(&schema).unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn tombstones_become_key_only_deletes_and_puts_become_upserts() {
        let sink_schema = sink_input_schema(&source_schema());
        let batch = sink_batch(
            &encoded(Some(vec![
                SourceMutation::Put,
                SourceMutation::Tombstone,
                SourceMutation::Put,
            ])),
            &sink_schema,
        )
        .unwrap();
        assert_eq!(batch.schema(), sink_schema);
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..3).map(|row| ops.value(row)).collect::<Vec<_>>(),
            ["U", "D", "U"]
        );
        assert!(
            batch.column(1).is_null(1),
            "no value is fabricated for a delete"
        );

        let all_put = sink_batch(&encoded(None), &sink_schema).unwrap();
        let ops = all_put
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..3).all(|row| ops.value(row) == "U"));
    }

    #[test]
    fn route_shape_requires_key_nullable_values_and_no_filters() {
        let schema = source_schema();
        let sink = sink("out", "src", &[], None);
        validate_route_shape("src", &schema, &["_id".into()], &[&sink]).unwrap();
        assert!(validate_route_shape("src", &schema, &[], &[&sink])
            .unwrap_err()
            .contains("PRIMARY KEY"));
        let strict = Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]);
        assert!(
            validate_route_shape("src", &strict, &["_id".into()], &[&sink])
                .unwrap_err()
                .contains("nullable")
        );
        let reserved = Schema::new(vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("_op", DataType::Utf8, true),
        ]);
        assert!(
            validate_route_shape("src", &reserved, &["_id".into()], &[&sink])
                .unwrap_err()
                .contains("_op")
        );
        let filtered = super::tests::sink("out", "src", &[], Some("v > 1"));
        assert!(
            validate_route_shape("src", &schema, &["_id".into()], &[&filtered])
                .unwrap_err()
                .contains("cannot filter")
        );
    }

    #[test]
    fn sink_key_must_equal_the_source_primary_key() {
        validate_sink_key("out", "src", &["_id".into()], Some(vec!["_id".into()])).unwrap();
        assert!(validate_sink_key("out", "src", &["_id".into()], None)
            .unwrap_err()
            .contains("cannot apply"));
        assert!(validate_sink_key(
            "out",
            "src",
            &["_id".into()],
            Some(vec!["_id".into(), "region".into()])
        )
        .unwrap_err()
        .contains("must equal"));
    }

    #[test]
    fn any_stream_or_sink_query_disqualifies_the_direct_route() {
        let sink = sink("out", "src", &[], None);
        assert_eq!(
            direct_sink_consumers("src", std::iter::empty(), [&sink])
                .unwrap()
                .len(),
            1
        );
        assert!(direct_sink_consumers("src", std::iter::empty(), std::iter::empty()).is_none());
        let query_sink = super::tests::sink("q", "s2", &["src"], None);
        assert!(direct_sink_consumers("src", std::iter::empty(), [&sink, &query_sink]).is_none());
        let stream = stream("SELECT * FROM src");
        assert!(direct_sink_consumers("src", [&stream], [&sink]).is_none());
    }
}
