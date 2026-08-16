//! LaminarDB JSON transformation UDFs.
//!
//! Object operations own merge, cleanup, and key selection. Shape operations own flattening,
//! reconstruction, column extraction, and schema inference.

mod object_ops;
mod shape_ops;

pub use object_ops::{
    JsonbDeepMerge, JsonbExcept, JsonbMerge, JsonbPick, JsonbRenameKeys, JsonbStripNulls,
};
pub use shape_ops::{JsonInferSchema, JsonToColumns, JsonbFlatten, JsonbUnflatten};

/// Registers all JSON extension UDFs with the given session context.
pub fn register_json_extensions(ctx: &datafusion::prelude::SessionContext) {
    use datafusion_expr::ScalarUDF;

    ctx.register_udf(ScalarUDF::new_from_impl(JsonbMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbDeepMerge::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbStripNulls::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbRenameKeys::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbPick::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbExcept::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbFlatten::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonbUnflatten::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonToColumns::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(JsonInferSchema::new()));
}

#[cfg(test)]
mod tests;
