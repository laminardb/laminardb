//! FFI-friendly API for language bindings (Python, Java, Node.js, .NET).
//!
//! This module provides a stable, thread-safe API surface with:
//!
//! - **Numeric error codes** for cross-language error handling
//! - **Arrow RecordBatch** at all data boundaries (zero-copy via C Data Interface)
//! - **Explicit resource management** (`close()` methods return `Result`)
//! - **Thread safety guarantees** (all types are `Send + Sync`)
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use laminar_db::api::{Connection, ApiError};
//!
//! // Open database
//! let conn = Connection::open()?;
//!
//! // Create a source
//! conn.execute("CREATE SOURCE trades (
//!     symbol VARCHAR,
//!     price DOUBLE,
//!     ts BIGINT
//! )")?;
//!
//! // Insert data
//! conn.execute("INSERT INTO trades VALUES ('AAPL', 150.0, 1234567890)")?;
//!
//! // Query
//! let result = conn.query("SELECT * FROM trades")?;
//! println!("Got {} rows", result.num_rows());
//!
//! // Explicit cleanup
//! conn.close()?;
//! ```
//!
//! # Error Handling
//!
//! All errors include numeric codes suitable for FFI:
//!
//! ```rust,ignore
//! match conn.get_schema("missing") {
//!     Ok(schema) => { /* use schema */ }
//!     Err(e) => {
//!         println!("Error code: {}", e.code());
//!         println!("Message: {}", e.message());
//!     }
//! }
//! ```
//!
//! See [`codes`](crate::api::codes) for the full list of error codes.
//!
//! # Thread Safety
//!
//! All types in this module are `Send + Sync`:
//!
//! ```rust,ignore
//! let conn = Arc::new(Connection::open()?);
//!
//! // Safe to share across threads
//! let handle = std::thread::spawn({
//!     let conn = Arc::clone(&conn);
//!     move || conn.list_sources()
//! });
//! ```
//!
//! # Arrow C Data Interface
//!
//! The `RecordBatch` type supports zero-copy export via Arrow's C Data Interface.
//! Language bindings can use this for efficient data exchange:
//!
//! - Python: `pyarrow.RecordBatch._import_from_c()`
//! - Java: Arrow Java C Data Interface
//! - Node.js: `apache-arrow` npm package
//! - .NET: `Apache.Arrow` NuGet package

mod connection;
mod error;
mod ingestion;
mod query;
mod subscription;

pub use connection::{Connection, DdlInfo, ExecuteResult};
pub use error::{codes, ApiError};
pub use ingestion::Writer;
pub use query::{QueryResult, QueryStream};
pub use subscription::ArrowSubscription;

// Re-export LaminarConfig for open_with_config
pub use crate::LaminarConfig;
