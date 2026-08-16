//! Per-batch JSON parsing, expansion, and Arrow column assembly.

use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;

use super::value::{append_null, append_value, create_builders, ColumnBuilder};
use super::{ColumnExtraction, JsonDecoder, UnknownFieldStrategy};
use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::json::jsonb::JsonbEncoder;

impl JsonDecoder {
    /// Decode borrowed JSON payload slices into a `RecordBatch` without copying
    /// the input payloads.
    ///
    /// # Errors
    /// Returns a decode error if a payload is invalid or cannot be represented
    /// by the configured Arrow schema.
    pub fn decode_slices(&self, values: &[&[u8]]) -> SchemaResult<RecordBatch> {
        self.decode_slices_bounded(values, usize::MAX)
    }

    /// Decode borrowed JSON payloads while rejecting output expansion beyond
    /// `max_rows` before values are appended to Arrow builders.
    ///
    /// # Errors
    /// Returns a decode error when parsing, coercion, or `json.explode` exceeds
    /// the row bound.
    pub fn decode_slices_bounded(
        &self,
        values: &[&[u8]],
        max_rows: usize,
    ) -> SchemaResult<RecordBatch> {
        if values.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let mut batch = BatchDecodeState::new(self, values.len(), max_rows);
        for &bytes in values {
            batch.decode_record(bytes)?;
        }
        batch.finish()
    }

    /// O(n) field lookup. Linear scan is faster for typical schemas with fewer
    /// than 50 fields.
    fn field_index(&self, name: &str) -> Option<usize> {
        self.field_indices
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, index)| *index)
    }
}

struct BatchDecodeState<'a> {
    decoder: &'a JsonDecoder,
    builders: Vec<Box<dyn ColumnBuilder>>,
    extra_builder: Option<LargeBinaryBuilder>,
    jsonb_encoder: Option<JsonbEncoder>,
    populated: Vec<bool>,
    output_rows: usize,
    max_rows: usize,
}

impl<'a> BatchDecodeState<'a> {
    fn new(decoder: &'a JsonDecoder, capacity: usize, max_rows: usize) -> Self {
        let extra_builder = matches!(
            decoder.config.unknown_fields,
            UnknownFieldStrategy::CollectExtra
        )
        .then(|| LargeBinaryBuilder::with_capacity(capacity, capacity * 64));
        let jsonb_encoder = decoder.config.nested_as_jsonb.then(JsonbEncoder::new);
        Self {
            decoder,
            builders: create_builders(&decoder.schema, capacity),
            extra_builder,
            jsonb_encoder,
            populated: vec![false; decoder.schema.fields().len()],
            output_rows: 0,
            max_rows,
        }
    }

    #[inline]
    fn decode_record(&mut self, bytes: &[u8]) -> SchemaResult<()> {
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|error| SchemaError::DecodeError(format!("JSON parse error: {error}")))?;
        let Some(default_target) =
            navigate_path_opt(&value, self.decoder.config.json_path.as_deref())
        else {
            // Some sources interleave non-data frames that lack the configured path.
            return Ok(());
        };

        if self.decoder.explode_col_indices.is_some() {
            self.decode_exploded(default_target)
        } else {
            self.decode_object(&value, default_target)
        }
    }

    fn decode_exploded(&mut self, target: &serde_json::Value) -> SchemaResult<()> {
        let elements = target.as_array().ok_or_else(|| {
            SchemaError::DecodeError("json.explode target must be an array".into())
        })?;
        self.reserve_rows(elements.len())?;
        for element in elements {
            self.populated.fill(false);
            self.append_exploded_element(element)?;
            self.append_missing_fields();
            if let Some(extra) = &mut self.extra_builder {
                extra.append_null();
            }
        }
        Ok(())
    }

    fn append_exploded_element(&mut self, element: &serde_json::Value) -> SchemaResult<()> {
        match element {
            serde_json::Value::Array(items) => {
                let positions = self
                    .decoder
                    .explode_col_indices
                    .as_ref()
                    .expect("explode positions exist in explode mode")
                    .len();
                for position in 0..positions {
                    let column = self
                        .decoder
                        .explode_col_indices
                        .as_ref()
                        .and_then(|indices| indices.get(position).copied().flatten());
                    if let Some(column) = column {
                        let value = items.get(position).unwrap_or(&serde_json::Value::Null);
                        self.append_column(column, value)?;
                        self.populated[column] = true;
                    }
                }
            }
            serde_json::Value::Object(object) => {
                for column in 0..self.decoder.field_indices.len() {
                    let name = self.decoder.field_indices[column].0.as_str();
                    if let Some(value) = object.get(name) {
                        self.append_column(column, value)?;
                        self.populated[column] = true;
                    }
                }
            }
            _ => {
                return Err(SchemaError::DecodeError(
                    "json.explode array elements must be arrays or objects".into(),
                ));
            }
        }
        Ok(())
    }

    fn decode_object(
        &mut self,
        root: &serde_json::Value,
        target: &serde_json::Value,
    ) -> SchemaResult<()> {
        self.reserve_rows(1)?;
        let object = target
            .as_object()
            .ok_or_else(|| SchemaError::DecodeError("JSON value must be an object".into()))?;
        self.populated.fill(false);
        self.append_object_fields(root, object)?;
        let extra_fields = self.collect_or_reject_unknown_fields(object)?;
        self.append_missing_fields();
        self.append_extra_fields(extra_fields.as_ref());
        Ok(())
    }

    fn append_object_fields(
        &mut self,
        root: &serde_json::Value,
        object: &serde_json::Map<String, serde_json::Value>,
    ) -> SchemaResult<()> {
        for column in 0..self.decoder.field_indices.len() {
            let name = self.decoder.field_indices[column].0.as_str();
            let value = match &self.decoder.column_extractions[column] {
                ColumnExtraction::DefaultPath => object.get(name),
                ColumnExtraction::CustomPath { segments } => navigate_path(root, segments),
            };
            if let Some(value) = value {
                self.append_column(column, value)?;
                self.populated[column] = true;
            }
        }
        Ok(())
    }

    fn collect_or_reject_unknown_fields(
        &self,
        object: &serde_json::Map<String, serde_json::Value>,
    ) -> SchemaResult<Option<serde_json::Map<String, serde_json::Value>>> {
        match self.decoder.config.unknown_fields {
            UnknownFieldStrategy::CollectExtra => {
                let mut extra = serde_json::Map::new();
                for (key, value) in object {
                    if self.decoder.field_index(key).is_none() {
                        extra.insert(key.clone(), value.clone());
                    }
                }
                Ok(Some(extra))
            }
            UnknownFieldStrategy::Reject => {
                for key in object.keys() {
                    if self.decoder.field_index(key).is_none() {
                        return Err(SchemaError::DecodeError(format!(
                            "unknown field '{key}' not in schema"
                        )));
                    }
                }
                Ok(None)
            }
            UnknownFieldStrategy::Ignore => Ok(None),
        }
    }

    #[inline]
    fn append_column(&mut self, column: usize, value: &serde_json::Value) -> SchemaResult<()> {
        let field = &self.decoder.schema.fields()[column];
        append_value(
            &mut self.builders[column],
            field.data_type(),
            value,
            &self.decoder.config,
            &self.decoder.mismatch_count,
            self.jsonb_encoder.as_mut(),
            self.decoder.column_epoch_units[column],
        )
    }

    #[inline]
    fn append_missing_fields(&mut self) {
        for (column, populated) in self.populated.iter().enumerate() {
            if !populated {
                append_null(&mut self.builders[column]);
            }
        }
    }

    fn append_extra_fields(
        &mut self,
        extra_fields: Option<&serde_json::Map<String, serde_json::Value>>,
    ) {
        let Some(extra_builder) = &mut self.extra_builder else {
            return;
        };
        let Some(extra_fields) = extra_fields.filter(|extra| !extra.is_empty()) else {
            extra_builder.append_null();
            return;
        };

        let mut encoder = self
            .jsonb_encoder
            .as_mut()
            .map_or_else(JsonbEncoder::new, |_| JsonbEncoder::new());
        let bytes = encoder.encode(&serde_json::Value::Object(extra_fields.clone()));
        extra_builder.append_value(&bytes);
    }

    fn reserve_rows(&mut self, additional: usize) -> SchemaResult<()> {
        self.output_rows = self
            .output_rows
            .checked_add(additional)
            .filter(|rows| *rows <= self.max_rows)
            .ok_or_else(|| {
                SchemaError::DecodeError(format!(
                    "JSON output exceeds the {}-row batch limit",
                    self.max_rows
                ))
            })?;
        Ok(())
    }

    fn finish(self) -> SchemaResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(|mut builder| builder.finish())
            .collect();
        let final_schema = if let Some(mut extra_builder) = self.extra_builder {
            columns.push(Arc::new(extra_builder.finish()));
            let mut fields = self.decoder.schema.fields().to_vec();
            fields.push(Arc::new(arrow_schema::Field::new(
                "_extra",
                DataType::LargeBinary,
                true,
            )));
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            self.decoder.schema.clone()
        };
        RecordBatch::try_new(final_schema, columns)
            .map_err(|error| SchemaError::DecodeError(format!("RecordBatch construction: {error}")))
    }
}

fn navigate_path<'a>(
    root: &'a serde_json::Value,
    segments: &[String],
) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for segment in segments {
        current = current.get(segment.as_str())?;
    }
    Some(current)
}

fn navigate_path_opt<'a>(
    root: &'a serde_json::Value,
    segments: Option<&[String]>,
) -> Option<&'a serde_json::Value> {
    match segments {
        Some(segments) => navigate_path(root, segments),
        None => Some(root),
    }
}
