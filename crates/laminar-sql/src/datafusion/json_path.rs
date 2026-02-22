//! SQL/JSON path query functions (F-SCHEMA-012).
//!
//! Implements a basic JSON path compiler and two scalar UDFs:
//!
//! - `jsonb_path_exists(jsonb, path_text) → boolean`
//! - `jsonb_path_match(jsonb, path_text) → boolean`
//!
//! The path language supports a PostgreSQL SQL/JSON subset:
//! - `$` — root element
//! - `.key` — object member access
//! - `[n]` — array element by index
//! - `[*]` — wildcard array elements

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{Array, BooleanArray, LargeBinaryArray};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use parking_lot::Mutex;

use super::json_types;

/// One-slot cache for compiled JSON paths.
///
/// Since a given UDF instance almost always evaluates the same constant path
/// string, a single-entry cache avoids recompilation on every batch.
type PathCache = Mutex<Option<(String, CompiledJsonPath)>>;

// ── JSON Path Compiler ──────────────────────────────────────────────

/// A single step in a compiled JSON path.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonPathStep {
    /// Root element (`$`).
    Root,
    /// Object member access (`.key`).
    Member(String),
    /// Array element by index (`[n]`).
    ArrayIndex(i64),
    /// Wildcard array element (`[*]`).
    ArrayWildcard,
}

/// A compiled SQL/JSON path expression.
///
/// Path expressions are compiled from string form (e.g., `$.items[*].price`)
/// into a sequence of steps for evaluation against JSONB binary data.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledJsonPath {
    /// Sequence of path steps.
    pub steps: Vec<JsonPathStep>,
}

impl CompiledJsonPath {
    /// Compiles a SQL/JSON path string.
    ///
    /// # Supported syntax
    ///
    /// - `$` — root element (must appear first)
    /// - `.key` — object member access
    /// - `["key"]` — quoted member access
    /// - `[n]` — array index (non-negative)
    /// - `[*]` — wildcard array element
    ///
    /// # Errors
    ///
    /// Returns an error if the path syntax is invalid.
    pub fn compile(path_str: &str) -> std::result::Result<Self, String> {
        let path_str = path_str.trim();
        if path_str.is_empty() {
            return Err("empty path expression".into());
        }

        let mut steps = Vec::new();
        let chars: Vec<char> = path_str.chars().collect();
        let mut pos = 0;

        // Must start with '$'
        if chars.first() != Some(&'$') {
            return Err(format!(
                "path must start with '$', got '{}'",
                chars.first().unwrap_or(&' ')
            ));
        }
        steps.push(JsonPathStep::Root);
        pos += 1;

        while pos < chars.len() {
            match chars[pos] {
                '.' => {
                    pos += 1;
                    // Read member name
                    let start = pos;
                    while pos < chars.len()
                        && chars[pos] != '.'
                        && chars[pos] != '['
                        && !chars[pos].is_whitespace()
                    {
                        pos += 1;
                    }
                    if pos == start {
                        return Err(format!("empty member name at position {start}"));
                    }
                    let name: String = chars[start..pos].iter().collect();
                    steps.push(JsonPathStep::Member(name));
                }
                '[' => {
                    pos += 1;
                    // Skip whitespace
                    while pos < chars.len() && chars[pos].is_whitespace() {
                        pos += 1;
                    }
                    if pos >= chars.len() {
                        return Err("unclosed bracket".into());
                    }
                    if chars[pos] == '*' {
                        steps.push(JsonPathStep::ArrayWildcard);
                        pos += 1;
                    } else if chars[pos] == '"' || chars[pos] == '\'' {
                        // Quoted member access: ["key"] or ['key']
                        let quote = chars[pos];
                        pos += 1;
                        let start = pos;
                        while pos < chars.len() && chars[pos] != quote {
                            pos += 1;
                        }
                        if pos >= chars.len() {
                            return Err("unclosed quoted member".into());
                        }
                        let name: String = chars[start..pos].iter().collect();
                        steps.push(JsonPathStep::Member(name));
                        pos += 1; // skip closing quote
                    } else {
                        // Array index
                        let start = pos;
                        let mut negative = false;
                        if pos < chars.len() && chars[pos] == '-' {
                            negative = true;
                            pos += 1;
                        }
                        while pos < chars.len() && chars[pos].is_ascii_digit() {
                            pos += 1;
                        }
                        if pos == start || (negative && pos == start + 1) {
                            return Err(format!("expected array index or '*' at position {start}"));
                        }
                        let idx_str: String = chars[start..pos].iter().collect();
                        let idx: i64 = idx_str
                            .parse()
                            .map_err(|_| format!("invalid array index: '{idx_str}'"))?;
                        steps.push(JsonPathStep::ArrayIndex(idx));
                    }
                    // Skip whitespace and expect ']'
                    while pos < chars.len() && chars[pos].is_whitespace() {
                        pos += 1;
                    }
                    if pos >= chars.len() || chars[pos] != ']' {
                        return Err(format!("expected ']' at position {pos}"));
                    }
                    pos += 1;
                }
                c if c.is_whitespace() => {
                    pos += 1;
                }
                c => {
                    return Err(format!("unexpected character '{c}' at position {pos}"));
                }
            }
        }

        Ok(Self { steps })
    }

    /// Evaluates the compiled path against JSONB binary data.
    ///
    /// Returns all matched sub-values as byte slices into the original data.
    #[must_use]
    pub fn evaluate<'a>(&self, jsonb: &'a [u8]) -> Vec<&'a [u8]> {
        if jsonb.is_empty() {
            return Vec::new();
        }
        let mut current = vec![jsonb];

        for step in &self.steps {
            match step {
                JsonPathStep::Root => {
                    // Already at root
                }
                JsonPathStep::Member(name) => {
                    let mut next = Vec::new();
                    for data in &current {
                        if let Some(val) = json_types::jsonb_get_field(data, name) {
                            next.push(val);
                        }
                    }
                    current = next;
                }
                JsonPathStep::ArrayIndex(idx) => {
                    let mut next = Vec::new();
                    for data in &current {
                        if *idx >= 0 {
                            if let Ok(i) = usize::try_from(*idx) {
                                if let Some(val) = json_types::jsonb_array_get(data, i) {
                                    next.push(val);
                                }
                            }
                        }
                    }
                    current = next;
                }
                JsonPathStep::ArrayWildcard => {
                    let mut next = Vec::new();
                    for data in &current {
                        if !data.is_empty() && data[0] == 0x06 {
                            // ARRAY tag
                            if data.len() >= 5 {
                                let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]])
                                    as usize;
                                for i in 0..count {
                                    if let Some(val) = json_types::jsonb_array_get(data, i) {
                                        next.push(val);
                                    }
                                }
                            }
                        }
                    }
                    current = next;
                }
            }
        }

        current
    }

    /// Returns `true` if evaluating this path against the JSONB data
    /// produces at least one match.
    #[must_use]
    pub fn exists(&self, jsonb: &[u8]) -> bool {
        !self.evaluate(jsonb).is_empty()
    }
}

// ── jsonb_path_exists UDF ───────────────────────────────────────────

/// `jsonb_path_exists(jsonb, path_text) → boolean`
///
/// Looks up or compiles a JSON path, caching the result.
fn compile_cached(
    cache: &PathCache,
    path_str: &str,
) -> std::result::Result<CompiledJsonPath, String> {
    let mut guard = cache.lock();
    if let Some((cached_str, cached_path)) = guard.as_ref() {
        if cached_str == path_str {
            return Ok(cached_path.clone());
        }
    }
    let compiled = CompiledJsonPath::compile(path_str)?;
    *guard = Some((path_str.to_owned(), compiled.clone()));
    Ok(compiled)
}

/// Returns `true` if the JSON path matches any element in the JSONB value.
#[derive(Debug)]
pub struct JsonbPathExistsUdf {
    signature: Signature,
    path_cache: PathCache,
}

impl Default for JsonbPathExistsUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonbPathExistsUdf {
    /// Creates a new `jsonb_path_exists` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                Volatility::Immutable,
            ),
            path_cache: Mutex::new(None),
        }
    }
}

impl PartialEq for JsonbPathExistsUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbPathExistsUdf {}

impl Hash for JsonbPathExistsUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_path_exists".hash(state);
    }
}

impl ScalarUDFImpl for JsonbPathExistsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_path_exists"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let first = &args.args[0];
        let second = &args.args[1];

        match (first, second) {
            (
                ColumnarValue::Array(bin_arr),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(path_str))),
            ) => {
                let compiled = compile_cached(&self.path_cache, path_str)
                    .map_err(datafusion_common::DataFusionError::Execution)?;
                let binary = bin_arr
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "expected LargeBinary array".into(),
                        )
                    })?;
                let results: BooleanArray = (0..binary.len())
                    .map(|i| {
                        if binary.is_null(i) {
                            None
                        } else {
                            Some(compiled.exists(binary.value(i)))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(datafusion_common::ScalarValue::LargeBinary(Some(bytes))),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(path_str))),
            ) => {
                let compiled = compile_cached(&self.path_cache, path_str)
                    .map_err(datafusion_common::DataFusionError::Execution)?;
                let result = compiled.exists(bytes);
                Ok(ColumnarValue::Scalar(
                    datafusion_common::ScalarValue::Boolean(Some(result)),
                ))
            }
            _ => Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::Boolean(None),
            )),
        }
    }
}

// ── jsonb_path_match UDF ────────────────────────────────────────────

/// `jsonb_path_match(jsonb, path_text) → boolean`
///
/// Returns the boolean result of evaluating a JSON path. The path must
/// yield exactly one boolean value. Returns NULL if the path matches
/// no values or the matched value is not boolean.
#[derive(Debug)]
pub struct JsonbPathMatchUdf {
    signature: Signature,
    path_cache: PathCache,
}

impl Default for JsonbPathMatchUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonbPathMatchUdf {
    /// Creates a new `jsonb_path_match` UDF.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::LargeBinary, DataType::Utf8]),
                Volatility::Immutable,
            ),
            path_cache: Mutex::new(None),
        }
    }
}

impl PartialEq for JsonbPathMatchUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonbPathMatchUdf {}

impl Hash for JsonbPathMatchUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "jsonb_path_match".hash(state);
    }
}

/// Checks if a JSONB binary is a boolean true value.
fn jsonb_is_true(data: &[u8]) -> Option<bool> {
    if data.is_empty() {
        return None;
    }
    match data[0] {
        0x01 => Some(false), // BOOL_FALSE
        0x02 => Some(true),  // BOOL_TRUE
        _ => None,           // not a boolean
    }
}

impl ScalarUDFImpl for JsonbPathMatchUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "jsonb_path_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let first = &args.args[0];
        let second = &args.args[1];

        match (first, second) {
            (
                ColumnarValue::Array(bin_arr),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(path_str))),
            ) => {
                let compiled = compile_cached(&self.path_cache, path_str)
                    .map_err(datafusion_common::DataFusionError::Execution)?;
                let binary = bin_arr
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "expected LargeBinary array".into(),
                        )
                    })?;
                let results: BooleanArray = (0..binary.len())
                    .map(|i| {
                        if binary.is_null(i) {
                            None
                        } else {
                            let matched = compiled.evaluate(binary.value(i));
                            if matched.len() == 1 {
                                jsonb_is_true(matched[0])
                            } else {
                                None
                            }
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(datafusion_common::ScalarValue::LargeBinary(Some(bytes))),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(path_str))),
            ) => {
                let compiled = compile_cached(&self.path_cache, path_str)
                    .map_err(datafusion_common::DataFusionError::Execution)?;
                let matched = compiled.evaluate(bytes);
                let result = if matched.len() == 1 {
                    jsonb_is_true(matched[0])
                } else {
                    None
                };
                Ok(ColumnarValue::Scalar(
                    datafusion_common::ScalarValue::Boolean(result),
                ))
            }
            _ => Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::Boolean(None),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Path compiler tests ──

    #[test]
    fn test_compile_root_only() {
        let path = CompiledJsonPath::compile("$").unwrap();
        assert_eq!(path.steps, vec![JsonPathStep::Root]);
    }

    #[test]
    fn test_compile_member_access() {
        let path = CompiledJsonPath::compile("$.name").unwrap();
        assert_eq!(
            path.steps,
            vec![JsonPathStep::Root, JsonPathStep::Member("name".into()),]
        );
    }

    #[test]
    fn test_compile_nested_members() {
        let path = CompiledJsonPath::compile("$.user.address.city").unwrap();
        assert_eq!(
            path.steps,
            vec![
                JsonPathStep::Root,
                JsonPathStep::Member("user".into()),
                JsonPathStep::Member("address".into()),
                JsonPathStep::Member("city".into()),
            ]
        );
    }

    #[test]
    fn test_compile_array_index() {
        let path = CompiledJsonPath::compile("$.items[0]").unwrap();
        assert_eq!(
            path.steps,
            vec![
                JsonPathStep::Root,
                JsonPathStep::Member("items".into()),
                JsonPathStep::ArrayIndex(0),
            ]
        );
    }

    #[test]
    fn test_compile_wildcard() {
        let path = CompiledJsonPath::compile("$.items[*].price").unwrap();
        assert_eq!(
            path.steps,
            vec![
                JsonPathStep::Root,
                JsonPathStep::Member("items".into()),
                JsonPathStep::ArrayWildcard,
                JsonPathStep::Member("price".into()),
            ]
        );
    }

    #[test]
    fn test_compile_quoted_member() {
        let path = CompiledJsonPath::compile("$[\"spaced key\"]").unwrap();
        assert_eq!(
            path.steps,
            vec![
                JsonPathStep::Root,
                JsonPathStep::Member("spaced key".into()),
            ]
        );
    }

    #[test]
    fn test_compile_empty_path_error() {
        assert!(CompiledJsonPath::compile("").is_err());
    }

    #[test]
    fn test_compile_no_root_error() {
        assert!(CompiledJsonPath::compile("name").is_err());
    }

    // ── Path evaluation tests ──

    #[test]
    fn test_evaluate_root() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a": 1}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$").unwrap();
        let results = path.evaluate(&jsonb);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_evaluate_member() {
        let json: serde_json::Value = serde_json::from_str(r#"{"name": "Alice"}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.name").unwrap();
        let results = path.evaluate(&jsonb);
        assert_eq!(results.len(), 1);
        // Should be the string "Alice" in JSONB format
        assert_eq!(
            json_types::jsonb_to_text(results[0]),
            Some("Alice".to_string())
        );
    }

    #[test]
    fn test_evaluate_missing_member() {
        let json: serde_json::Value = serde_json::from_str(r#"{"name": "Alice"}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.age").unwrap();
        let results = path.evaluate(&jsonb);
        assert!(results.is_empty());
    }

    #[test]
    fn test_evaluate_array_index() {
        let json: serde_json::Value = serde_json::from_str(r#"{"items": [10, 20, 30]}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.items[1]").unwrap();
        let results = path.evaluate(&jsonb);
        assert_eq!(results.len(), 1);
        assert_eq!(
            json_types::jsonb_to_text(results[0]),
            Some("20".to_string())
        );
    }

    #[test]
    fn test_evaluate_wildcard() {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"items": [{"price": 10}, {"price": 20}]}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.items[*].price").unwrap();
        let results = path.evaluate(&jsonb);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_exists_true() {
        let json: serde_json::Value = serde_json::from_str(r#"{"users": [{"age": 30}]}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.users[0].age").unwrap();
        assert!(path.exists(&jsonb));
    }

    #[test]
    fn test_exists_false() {
        let json: serde_json::Value = serde_json::from_str(r#"{"users": [{"age": 30}]}"#).unwrap();
        let jsonb = json_types::encode_jsonb(&json);
        let path = CompiledJsonPath::compile("$.users[0].email").unwrap();
        assert!(!path.exists(&jsonb));
    }

    // ── UDF tests ──

    #[test]
    fn test_path_exists_udf_scalar() {
        let udf = JsonbPathExistsUdf::new();
        assert_eq!(udf.name(), "jsonb_path_exists");
        assert_eq!(udf.return_type(&[]).unwrap(), DataType::Boolean);
    }

    #[test]
    fn test_path_match_udf_scalar() {
        let udf = JsonbPathMatchUdf::new();
        assert_eq!(udf.name(), "jsonb_path_match");
        assert_eq!(udf.return_type(&[]).unwrap(), DataType::Boolean);
    }
}
