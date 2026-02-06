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
pub use arrow_ffi::*;
pub use callback::*;
pub use connection::*;
pub use error::*;
pub use memory::*;
pub use query::*;
pub use schema::*;
pub use writer::*;
