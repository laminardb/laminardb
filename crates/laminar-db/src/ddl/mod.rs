//! DDL handlers — reopens `impl LaminarDB` across child modules, one statement
//! family per module, to keep `db.rs` focused on dispatch.
//!
//! - `catalog` — namespace ownership, cleanup/rollback fencing, topology gates;
//! - `source_sink` — source/sink statements and connector resolution;
//! - `table` — reference/lookup `CREATE TABLE` shape and orchestration;
//! - `stream` — continuous-query planning and `CREATE STREAM`;
//! - `cluster_checks` — temporal/interval topology and cluster query-shape admission;
//! - `materialized_view` — MV create/drop and incremental-emit admission;
//! - `drop` — drop planning, control acknowledgement, teardown;
//! - `control` — `SET` statements and control-ack resolution.
#![allow(clippy::disallowed_types)] // cold path

mod catalog;
mod cluster_checks;
mod control;
mod drop;
mod materialized_view;
mod source_sink;
mod stream;
mod table;
mod topology;

pub(crate) use stream::{
    logical_aggregate_stage_count, validate_managed_aggregate_admission, PlannedStreamingQuery,
};

#[cfg(test)]
pub(crate) use control::CONTROL_ACK_DEADLINE;

#[cfg(test)]
mod create_table_shape_tests;

#[cfg(test)]
use table::{build_table_fields_and_primary_key, validate_create_table_envelope};
