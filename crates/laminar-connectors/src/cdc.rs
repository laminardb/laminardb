//! Change Data Capture (CDC) source connectors.
//! PostgreSQL uses logical replication (`pgoutput`).

#[cfg(feature = "postgres-cdc")]
pub mod postgres;
