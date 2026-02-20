//! CDC (Change Data Capture) connectors for databases.
//!
//! Provides CDC source connectors that stream row-level changes
//! from databases into LaminarDB using logical replication.
//!
//! # Supported Databases
//!
//! - **PostgreSQL**: Logical replication via `pgoutput` plugin
//! - **MySQL**: Binary log replication with GTID support

/// PostgreSQL logical replication CDC source connector.
#[cfg(feature = "postgres-cdc")]
pub mod postgres;

/// MySQL binlog replication CDC source connector.
#[cfg(feature = "mysql-cdc")]
pub mod mysql;
