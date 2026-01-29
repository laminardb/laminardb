//! Unified database facade for `LaminarDB`.
//!
//! Provides a single entry point (`LaminarDB`) that ties together
//! the SQL parser, query planner, `DataFusion` context, and streaming API.
//!
//! # Example
//!
//! ```rust,ignore
//! use laminar_db::LaminarDB;
//!
//! let db = LaminarDB::open()?;
//!
//! db.execute("CREATE SOURCE trades (
//!     symbol VARCHAR, price DOUBLE, ts BIGINT,
//!     WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
//! )").await?;
//!
//! let query = db.execute("SELECT symbol, AVG(price)
//!     FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)
//! ").await?;
//! ```

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod catalog;
mod config;
mod db;
mod error;
mod handle;

pub use catalog::{SourceEntry, SourceCatalog};
pub use config::LaminarConfig;
pub use db::LaminarDB;
pub use error::DbError;
pub use handle::{
    ExecuteResult, DdlInfo, QueryHandle, SourceHandle, UntypedSourceHandle,
    TypedSubscription, QueryInfo, SourceInfo, SinkInfo,
};
