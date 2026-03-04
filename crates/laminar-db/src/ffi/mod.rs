//! C FFI layer for LaminarDB.
//!
//! This module provides `extern "C"` functions for calling LaminarDB from C
//! and any language with C FFI support (Python, Java, Node.js, .NET, etc.).
//!
//! # Design
//!
//! - **Opaque handles**: C sees pointers, not struct layouts
//! - **Explicit memory management**: Caller frees with `*_free()` functions
//! - **Error codes**: All functions return `i32` (0 = success, negative = error)
//! - **Out-parameters**: Results returned via pointer arguments
//! - **Thread-local errors**: `laminar_last_error()` returns the last error message
//!
//! # Example (C)
//!
//! ```c
//! #include "laminar.h"
//!
//! int main() {
//!     LaminarConnection* conn = NULL;
//!     int32_t rc = laminar_open(&conn);
//!     if (rc != LAMINAR_OK) {
//!         printf("Error: %s\n", laminar_last_error());
//!         return 1;
//!     }
//!
//!     rc = laminar_execute(conn, "CREATE SOURCE test (id BIGINT)", NULL);
//!     laminar_close(conn);
//!     return 0;
//! }
//! ```

mod arrow_ffi;
mod callback;
mod connection;
mod error;
mod memory;
mod query;
mod schema;
mod writer;

// Re-export all FFI functions
pub use arrow_ffi::{
    laminar_batch_create, laminar_batch_export, laminar_batch_export_column, laminar_batch_import,
    laminar_schema_export,
};
pub use callback::{
    laminar_subscribe_callback, laminar_subscription_cancel, laminar_subscription_free,
    laminar_subscription_is_active, laminar_subscription_user_data, LaminarCompletionCallback,
    LaminarErrorCallback, LaminarSubscriptionCallback, LaminarSubscriptionHandle,
    LAMINAR_EVENT_DELETE, LAMINAR_EVENT_INSERT, LAMINAR_EVENT_SNAPSHOT, LAMINAR_EVENT_UPDATE,
    LAMINAR_EVENT_WATERMARK,
};
pub use connection::{
    laminar_close, laminar_execute, laminar_is_closed, laminar_open, laminar_query,
    laminar_query_stream, laminar_start, LaminarConnection,
};
pub use error::{
    laminar_clear_error, laminar_last_error, laminar_last_error_code, LAMINAR_ERR_CONNECTION,
    LAMINAR_ERR_INGESTION, LAMINAR_ERR_INTERNAL, LAMINAR_ERR_INVALID_UTF8,
    LAMINAR_ERR_NULL_POINTER, LAMINAR_ERR_QUERY, LAMINAR_ERR_SCHEMA_MISMATCH, LAMINAR_ERR_SHUTDOWN,
    LAMINAR_ERR_SUBSCRIPTION, LAMINAR_ERR_TABLE_EXISTS, LAMINAR_ERR_TABLE_NOT_FOUND, LAMINAR_OK,
};
pub use memory::{laminar_string_free, laminar_version};
pub use query::{
    laminar_batch_free, laminar_batch_num_columns, laminar_batch_num_rows, laminar_result_free,
    laminar_result_get_batch, laminar_result_num_batches, laminar_result_num_rows,
    laminar_result_schema, laminar_stream_cancel, laminar_stream_free, laminar_stream_is_active,
    laminar_stream_next, laminar_stream_schema, laminar_stream_try_next, LaminarQueryResult,
    LaminarQueryStream, LaminarRecordBatch,
};
pub use schema::{
    laminar_get_schema, laminar_list_sources, laminar_schema_field_name, laminar_schema_field_type,
    laminar_schema_free, laminar_schema_num_fields, LaminarSchema,
};
pub use writer::{
    laminar_writer_close, laminar_writer_create, laminar_writer_flush, laminar_writer_free,
    laminar_writer_write, LaminarWriter,
};
