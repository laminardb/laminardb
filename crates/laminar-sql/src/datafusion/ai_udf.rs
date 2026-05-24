//! Marker UDFs for the `ai_*` SQL functions.
//!
//! These exist so that `ai_classify`, `ai_embed`, … resolve as known functions
//! with a fixed return type, the same way `tumble()` and `watermark()` are
//! markers the engine rewrites rather than evaluates. In the normal path the AI
//! detector lifts an `ai_*` call out of the projection and replaces it with a
//! computed column before DataFusion plans the residual query, so these markers
//! are never invoked. If one survives to execution — used in a position the
//! detector does not rewrite — `invoke` returns a clear error rather than
//! producing a wrong value.
//!
//! The function name → return type map here must stay in step with the
//! name → task map in `laminar-db`'s `sql_analysis` (the eight locked AI
//! functions). The crate does not depend on `laminar-ai`, so the two lists are
//! intentionally independent.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// A placeholder scalar UDF for an `ai_*` function. Holds the function's name
/// and fixed return type; errors if evaluated directly.
#[derive(Debug)]
pub struct AiFunctionMarker {
    name: &'static str,
    signature: Signature,
    return_type: DataType,
}

impl AiFunctionMarker {
    /// Create a marker for `name` returning `return_type`.
    #[must_use]
    pub fn new(name: &'static str, return_type: DataType) -> Self {
        Self {
            name,
            // Accept any arguments (input plus `model =>` / `labels =>` named
            // args). Volatile so it is never const-folded away before the
            // detector can see it.
            signature: Signature::variadic_any(Volatility::Volatile),
            return_type,
        }
    }
}

impl PartialEq for AiFunctionMarker {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for AiFunctionMarker {}

impl Hash for AiFunctionMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for AiFunctionMarker {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "{} is an AI function and must be the top-level expression of a SELECT \
             projection over a stream; it cannot be evaluated in this position",
            self.name
        )
    }
}

/// Build the eight `ai_*` marker UDFs, ready to register on a session context.
///
/// Discriminative and text-generating tasks return `Utf8` (a label or text);
/// `ai_embed` returns a `List<Float32>` embedding.
#[must_use]
pub fn ai_function_markers() -> Vec<ScalarUDF> {
    let embedding = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
    let specs: [(&'static str, DataType); 8] = [
        ("ai_classify", DataType::Utf8),
        ("ai_sentiment", DataType::Utf8),
        ("ai_embed", embedding),
        ("ai_extract", DataType::Utf8),
        ("ai_complete", DataType::Utf8),
        ("ai_summarize", DataType::Utf8),
        ("ai_translate", DataType::Utf8),
        ("ai_gen", DataType::Utf8),
    ];
    specs
        .into_iter()
        .map(|(name, return_type)| {
            ScalarUDF::new_from_impl(AiFunctionMarker::new(name, return_type))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn registers_eight_markers_with_expected_return_types() {
        let markers = ai_function_markers();
        assert_eq!(markers.len(), 8);

        let classify = markers.iter().find(|m| m.name() == "ai_classify").unwrap();
        assert_eq!(classify.return_type(&[]).unwrap(), DataType::Utf8);

        let embed = markers.iter().find(|m| m.name() == "ai_embed").unwrap();
        assert!(matches!(embed.return_type(&[]).unwrap(), DataType::List(_)));
    }

    #[test]
    fn invoking_a_marker_is_an_error() {
        let marker = AiFunctionMarker::new("ai_classify", DataType::Utf8);
        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        assert!(marker.invoke_with_args(args).is_err());
    }
}
