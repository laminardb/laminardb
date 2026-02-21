//! JSON table-valued functions (F-SCHEMA-012).
//!
//! Implements PostgreSQL-compatible JSON TVFs as DataFusion table functions:
//!
//! - `jsonb_array_elements(jsonb)` → set of JSONB values
//! - `jsonb_array_elements_text(jsonb)` → set of text values
//! - `jsonb_each(jsonb)` → set of (key, value) pairs
//! - `jsonb_each_text(jsonb)` → set of (key, text_value) pairs
//! - `jsonb_object_keys(jsonb)` → set of text keys
//!
//! Each TVF implements `TableFunctionImpl`, producing a `MemTable`-backed
//! `TableProvider` that holds the expanded rows.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int64Array, LargeBinaryArray, StringArray};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::Expr;

use super::json_types;

// ── Helpers ─────────────────────────────────────────────────────────

/// Builds a 1-based ordinality vector for `n` elements.
fn ordinality_vec(n: usize) -> Vec<i64> {
    (1..=n)
        .map(|i| i64::try_from(i).unwrap_or(i64::MAX))
        .collect()
}

/// JSONB tag constants (mirrored from json_types).
mod tags {
    pub const ARRAY: u8 = 0x06;
    pub const OBJECT: u8 = 0x07;
}

/// Extracts a literal JSONB binary from an expression.
fn extract_jsonb_literal(expr: &Expr) -> Result<Option<Vec<u8>>> {
    match expr {
        Expr::Literal(ScalarValue::LargeBinary(bytes), _) => Ok(bytes.clone()),
        Expr::Literal(ScalarValue::Null | ScalarValue::Utf8(None), _) => Ok(None),
        // Also accept Utf8 string literals (parse as JSON then encode to JSONB)
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => {
            let json_val: serde_json::Value = serde_json::from_str(s).map_err(|e| {
                datafusion_common::DataFusionError::Plan(format!("invalid JSON literal: {e}"))
            })?;
            Ok(Some(json_types::encode_jsonb(&json_val)))
        }
        other => plan_err!(
            "JSON TVF argument must be a JSONB (LargeBinary) or JSON string literal, got {other:?}"
        ),
    }
}

/// Iterates over JSONB array elements, returning each element as a bounded byte vec.
///
/// Returns `None` if the input is not a JSONB array.
fn jsonb_array_elements_iter(data: &[u8]) -> Option<Vec<Vec<u8>>> {
    if data.is_empty() || data[0] != tags::ARRAY {
        return None;
    }
    if data.len() < 5 {
        return None;
    }
    let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    // Offset table: count * 4 bytes (each offset is u32 LE, relative to data_start)
    let offsets_start = 5;
    let data_start = offsets_start + count * 4;
    if data.len() < data_start {
        return None;
    }

    let mut elements = Vec::with_capacity(count);
    for i in 0..count {
        let off_pos = offsets_start + i * 4;
        let offset = u32::from_le_bytes([
            data[off_pos],
            data[off_pos + 1],
            data[off_pos + 2],
            data[off_pos + 3],
        ]) as usize;

        let abs_start = data_start + offset;
        // Element end: next element's absolute start, or end of data
        let abs_end = if i + 1 < count {
            let next_pos = offsets_start + (i + 1) * 4;
            data_start
                + u32::from_le_bytes([
                    data[next_pos],
                    data[next_pos + 1],
                    data[next_pos + 2],
                    data[next_pos + 3],
                ]) as usize
        } else {
            data.len()
        };

        if abs_start <= abs_end && abs_end <= data.len() {
            elements.push(data[abs_start..abs_end].to_vec());
        }
    }
    Some(elements)
}

/// Iterates over JSONB object key-value pairs.
///
/// Returns `None` if the input is not a JSONB object.
fn jsonb_object_entries(data: &[u8]) -> Option<Vec<(String, Vec<u8>)>> {
    if data.is_empty() || data[0] != tags::OBJECT {
        return None;
    }
    if data.len() < 5 {
        return None;
    }
    let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    // Offset table: count * 8 bytes (key_off u32 + val_off u32, relative to data_start)
    let offsets_start = 5;
    let data_start = offsets_start + count * 8;
    if data.len() < data_start {
        return None;
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = offsets_start + i * 8;
        let key_off =
            u32::from_le_bytes([data[base], data[base + 1], data[base + 2], data[base + 3]])
                as usize;
        let val_off = u32::from_le_bytes([
            data[base + 4],
            data[base + 5],
            data[base + 6],
            data[base + 7],
        ]) as usize;

        // Key at data_start + key_off: u16 LE length + UTF-8 bytes
        let key_abs = data_start + key_off;
        if key_abs + 2 > data.len() {
            continue;
        }
        let key_len = u16::from_le_bytes([data[key_abs], data[key_abs + 1]]) as usize;
        let key_start = key_abs + 2;
        let key_end = key_start + key_len;
        if key_end > data.len() {
            continue;
        }
        let key = String::from_utf8_lossy(&data[key_start..key_end]).to_string();

        // Value at data_start + val_off, ending at next key's abs position
        let val_abs = data_start + val_off;
        let val_end = if i + 1 < count {
            let next_base = offsets_start + (i + 1) * 8;
            data_start
                + u32::from_le_bytes([
                    data[next_base],
                    data[next_base + 1],
                    data[next_base + 2],
                    data[next_base + 3],
                ]) as usize
        } else {
            data.len()
        };

        if val_abs <= val_end && val_end <= data.len() {
            entries.push((key, data[val_abs..val_end].to_vec()));
        }
    }
    Some(entries)
}

// ── jsonb_array_elements ─────────────────────────────────────────────

/// `jsonb_array_elements(jsonb) → setof jsonb`
///
/// Expands a JSONB array into a set of JSONB values (one row per element).
#[derive(Debug)]
pub struct JsonbArrayElementsTvf;

impl TableFunctionImpl for JsonbArrayElementsTvf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_array_elements requires exactly 1 argument");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::LargeBinary, true),
            Field::new("ordinality", DataType::Int64, false),
        ]));

        let bytes = extract_jsonb_literal(&args[0])?;
        let elements = bytes.as_deref().and_then(jsonb_array_elements_iter);

        match elements {
            Some(elems) if !elems.is_empty() => {
                let values: Vec<Option<&[u8]>> = elems.iter().map(|e| Some(e.as_slice())).collect();
                let ordinality = ordinality_vec(elems.len());
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(LargeBinaryArray::from(values)),
                        Arc::new(Int64Array::from(ordinality)),
                    ],
                )?;
                Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
            }
            _ => Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?)),
        }
    }
}

// ── jsonb_array_elements_text ────────────────────────────────────────

/// `jsonb_array_elements_text(jsonb) → setof text`
///
/// Same as `jsonb_array_elements` but returns each element as text.
#[derive(Debug)]
pub struct JsonbArrayElementsTextTvf;

impl TableFunctionImpl for JsonbArrayElementsTextTvf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_array_elements_text requires exactly 1 argument");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, true),
            Field::new("ordinality", DataType::Int64, false),
        ]));

        let bytes = extract_jsonb_literal(&args[0])?;
        let elements = bytes.as_deref().and_then(jsonb_array_elements_iter);

        match elements {
            Some(elems) if !elems.is_empty() => {
                let texts: Vec<Option<String>> =
                    elems.iter().map(|e| json_types::jsonb_to_text(e)).collect();
                let ordinality = ordinality_vec(elems.len());
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(texts)),
                        Arc::new(Int64Array::from(ordinality)),
                    ],
                )?;
                Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
            }
            _ => Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?)),
        }
    }
}

// ── jsonb_each ──────────────────────────────────────────────────────

/// `jsonb_each(jsonb) → setof (key text, value jsonb)`
///
/// Expands a JSONB object into a set of key-value pairs.
#[derive(Debug)]
pub struct JsonbEachTvf;

impl TableFunctionImpl for JsonbEachTvf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_each requires exactly 1 argument");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::LargeBinary, true),
            Field::new("ordinality", DataType::Int64, false),
        ]));

        let bytes = extract_jsonb_literal(&args[0])?;
        let entries = bytes.as_deref().and_then(jsonb_object_entries);

        match entries {
            Some(kvs) if !kvs.is_empty() => {
                let keys: Vec<&str> = kvs.iter().map(|(k, _)| k.as_str()).collect();
                let values: Vec<Option<&[u8]>> =
                    kvs.iter().map(|(_, v)| Some(v.as_slice())).collect();
                let ordinality = ordinality_vec(kvs.len());
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(keys)),
                        Arc::new(LargeBinaryArray::from(values)),
                        Arc::new(Int64Array::from(ordinality)),
                    ],
                )?;
                Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
            }
            _ => Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?)),
        }
    }
}

// ── jsonb_each_text ─────────────────────────────────────────────────

/// `jsonb_each_text(jsonb) → setof (key text, value text)`
///
/// Same as `jsonb_each` but casts each value to text.
#[derive(Debug)]
pub struct JsonbEachTextTvf;

impl TableFunctionImpl for JsonbEachTextTvf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_each_text requires exactly 1 argument");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("ordinality", DataType::Int64, false),
        ]));

        let bytes = extract_jsonb_literal(&args[0])?;
        let entries = bytes.as_deref().and_then(jsonb_object_entries);

        match entries {
            Some(kvs) if !kvs.is_empty() => {
                let keys: Vec<&str> = kvs.iter().map(|(k, _)| k.as_str()).collect();
                let texts: Vec<Option<String>> = kvs
                    .iter()
                    .map(|(_, v)| json_types::jsonb_to_text(v))
                    .collect();
                let ordinality = ordinality_vec(kvs.len());
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(keys)),
                        Arc::new(StringArray::from(texts)),
                        Arc::new(Int64Array::from(ordinality)),
                    ],
                )?;
                Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
            }
            _ => Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?)),
        }
    }
}

// ── jsonb_object_keys ───────────────────────────────────────────────

/// `jsonb_object_keys(jsonb) → setof text`
///
/// Returns all keys of a JSONB object as text rows.
#[derive(Debug)]
pub struct JsonbObjectKeysTvf;

impl TableFunctionImpl for JsonbObjectKeysTvf {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return plan_err!("jsonb_object_keys requires exactly 1 argument");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ordinality", DataType::Int64, false),
        ]));

        let bytes = extract_jsonb_literal(&args[0])?;
        let entries = bytes.as_deref().and_then(jsonb_object_entries);

        match entries {
            Some(kvs) if !kvs.is_empty() => {
                let keys: Vec<&str> = kvs.iter().map(|(k, _)| k.as_str()).collect();
                let ordinality = ordinality_vec(kvs.len());
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(keys)),
                        Arc::new(Int64Array::from(ordinality)),
                    ],
                )?;
                Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
            }
            _ => Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?)),
        }
    }
}

// ── Registration ────────────────────────────────────────────────────

/// Registers all JSON table-valued functions with the `SessionContext`.
pub fn register_json_table_functions(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udtf("jsonb_array_elements", Arc::new(JsonbArrayElementsTvf));
    ctx.register_udtf(
        "jsonb_array_elements_text",
        Arc::new(JsonbArrayElementsTextTvf),
    );
    ctx.register_udtf("jsonb_each", Arc::new(JsonbEachTvf));
    ctx.register_udtf("jsonb_each_text", Arc::new(JsonbEachTextTvf));
    ctx.register_udtf("jsonb_object_keys", Arc::new(JsonbObjectKeysTvf));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    fn make_jsonb_expr(json_str: &str) -> Expr {
        let val: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let bytes = json_types::encode_jsonb(&val);
        Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), None)
    }

    fn make_null_expr() -> Expr {
        Expr::Literal(ScalarValue::Null, None)
    }

    // ── jsonb_array_elements tests ──

    #[test]
    fn test_array_elements_basic() {
        let tvf = JsonbArrayElementsTvf;
        let provider = tvf.call(&[make_jsonb_expr("[1, 2, 3]")]).unwrap();
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(schema.field(1).name(), "ordinality");
    }

    #[tokio::test]
    async fn test_array_elements_via_sql() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT value, ordinality FROM jsonb_array_elements('[10, 20, 30]')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3);

        // Check ordinality
        let ord = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ord.value(0), 1);
        assert_eq!(ord.value(1), 2);
        assert_eq!(ord.value(2), 3);
    }

    #[tokio::test]
    async fn test_array_elements_empty() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT value FROM jsonb_array_elements('[]')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_array_elements_not_array() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT value FROM jsonb_array_elements('{\"a\":1}')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0); // graceful: non-array returns 0 rows
    }

    // ── jsonb_array_elements_text tests ──

    #[tokio::test]
    async fn test_array_elements_text_strings() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT value FROM jsonb_array_elements_text('[\"a\", \"b\", \"c\"]')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3);

        let vals = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(vals.value(0), "a");
        assert_eq!(vals.value(1), "b");
        assert_eq!(vals.value(2), "c");
    }

    #[tokio::test]
    async fn test_array_elements_text_mixed() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT value FROM jsonb_array_elements_text('[1, \"hello\", true]')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3);
    }

    // ── jsonb_each tests ──

    #[tokio::test]
    async fn test_each_basic() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT key, ordinality FROM jsonb_each('{\"a\":1,\"b\":2}')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_each_empty() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx.sql("SELECT key FROM jsonb_each('{}')").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    // ── jsonb_each_text tests ──

    #[tokio::test]
    async fn test_each_text_basic() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT key, value FROM jsonb_each_text('{\"x\":\"hello\",\"y\":42}')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2);
    }

    // ── jsonb_object_keys tests ──

    #[tokio::test]
    async fn test_object_keys_basic() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT key FROM jsonb_object_keys('{\"a\":1,\"b\":2,\"c\":3}')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn test_object_keys_empty() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);

        let df = ctx
            .sql("SELECT key FROM jsonb_object_keys('{}')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    // ── Registration test ──

    #[test]
    fn test_registration() {
        let ctx = SessionContext::new();
        register_json_table_functions(&ctx);
        assert!(ctx.table_function("jsonb_array_elements").is_ok());
        assert!(ctx.table_function("jsonb_array_elements_text").is_ok());
        assert!(ctx.table_function("jsonb_each").is_ok());
        assert!(ctx.table_function("jsonb_each_text").is_ok());
        assert!(ctx.table_function("jsonb_object_keys").is_ok());
    }
}
