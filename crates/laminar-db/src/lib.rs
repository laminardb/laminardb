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

mod builder;
mod catalog;
mod config;
mod connector_manager;
mod db;
mod error;
mod handle;
mod sql_utils;
mod stream_executor;

pub use builder::LaminarDbBuilder;
pub use catalog::{SourceEntry, SourceCatalog};
pub use config::LaminarConfig;
pub use db::LaminarDB;
pub use error::DbError;
pub use handle::{
    ExecuteResult, DdlInfo, FromBatch, QueryHandle, SourceHandle, UntypedSourceHandle,
    TypedSubscription, QueryInfo, SourceInfo, SinkInfo, StreamInfo,
    PipelineTopology, PipelineNode, PipelineEdge, PipelineNodeType,
};

/// Re-export the connector registry for custom connector registration.
pub use laminar_connectors::registry::ConnectorRegistry;
