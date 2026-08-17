//! PostgreSQL-compatible JSON scalar UDFs (F-SCHEMA-011).
//!
//! Implements:
//!
//! - **Extraction**: `jsonb_get`, `jsonb_get_idx`, `jsonb_get_text`,
//!   `jsonb_get_text_idx`, `jsonb_get_path`, `jsonb_get_path_text`
//! - **Existence**: `jsonb_exists`, `jsonb_exists_any`, `jsonb_exists_all`
//! - **Containment**: `jsonb_contains`, `jsonb_contained_by`
//! - **Interrogation**: `json_typeof`
//! - **Construction**: `json_build_object`, `json_build_array`, `to_jsonb`

mod construction;
mod extraction;
mod predicates;

use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;

pub use construction::{JsonBuildArray, JsonBuildObject, JsonTypeof, ToJsonb};
pub use extraction::{
    JsonbGet, JsonbGetIdx, JsonbGetPath, JsonbGetPathText, JsonbGetText, JsonbGetTextIdx,
};
pub use predicates::{
    JsonbContainedBy, JsonbContains, JsonbExists, JsonbExistsAll, JsonbExistsAny,
};

use super::json_types;

// ── Helpers ──────────────────────────────────────────────────────

/// Determine the output length from args (handling scalar/array combos).
fn output_len(args: &[ColumnarValue]) -> usize {
    for a in args {
        if let ColumnarValue::Array(arr) = a {
            return arr.len();
        }
    }
    1
}

/// Expand all args to arrays of the same length.
///
/// # Errors
///
/// Returns a `DataFusionError` if a scalar value cannot be expanded to the target length.
pub fn expand_args(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
    let len = output_len(args);
    args.iter()
        .map(|a| match a {
            ColumnarValue::Array(arr) => Ok(Arc::clone(arr)),
            ColumnarValue::Scalar(s) => s.to_array_of_size(len),
        })
        .collect()
}

#[cfg(test)]
mod tests;
