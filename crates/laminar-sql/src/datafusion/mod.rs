//! `DataFusion` integration for SQL processing

use datafusion::prelude::*;

/// Creates a `DataFusion` session context configured for streaming
#[must_use]
pub fn create_streaming_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(1); // Single partition for now

    SessionContext::new_with_config(config)
}

/// Registers `LaminarDB` custom functions
pub fn register_streaming_functions(_ctx: &SessionContext) {
    // TODO: Register TUMBLE, HOP, SESSION window functions
    // TODO: Register WATERMARK function
    // TODO: Register streaming-specific UDFs
}