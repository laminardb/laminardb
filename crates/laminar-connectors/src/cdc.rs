//! CDC (Change Data Capture) connectors for databases.
//!
//! Provides CDC source connectors that stream row-level changes
//! from databases into LaminarDB using logical replication.
//!
//! # Supported Databases
//!
//! - **PostgreSQL** (F027): Logical replication via `pgoutput` plugin
//! - **MySQL** (F028): Binary log replication with GTID support

/// PostgreSQL logical replication CDC source connector (F027).
#[cfg(feature = "postgres-cdc")]
pub mod postgres;

/// MySQL binlog replication CDC source connector (F028).
#[cfg(feature = "mysql-cdc")]
pub mod mysql;
