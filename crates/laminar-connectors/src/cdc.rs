//! Change Data Capture (CDC) source connectors.
//! PostgreSQL uses logical replication (`pgoutput`); MySQL uses the
//! binary log with GTID support.

#[cfg(feature = "mysql-cdc")]
pub mod mysql;
#[cfg(feature = "postgres-cdc")]
pub mod postgres;
