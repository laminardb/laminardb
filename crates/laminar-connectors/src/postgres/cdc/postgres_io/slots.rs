//! Replication-slot adoption checks, stale-holder termination, slots sharing a prefix, and the
//! transactions a slot creation waits for.

use std::time::Duration;

use super::super::lsn::Lsn;
use super::catalog::query;
use crate::error::ConnectorError;

const HOLDER_EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const HOLDER_EXIT_POLL: Duration = Duration::from_millis(100);

/// The session streaming from, or creating, a slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotHolder {
    pub(crate) pid: i32,
    pub(crate) application_name: String,
    pub(crate) client_addr: Option<String>,
}

impl std::fmt::Display for SlotHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pid {} (application_name '{}', client_addr {})",
            self.pid,
            self.application_name,
            self.client_addr.as_deref().unwrap_or("local")
        )
    }
}

/// Catalog attributes of an existing slot that decide whether this source may adopt it, one per
/// `pg_replication_slots` column.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default)]
pub(crate) struct SlotAttributes {
    pub(crate) slot_type: Option<String>,
    pub(crate) plugin: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) database_oid: Option<u32>,
    pub(crate) temporary: bool,
    pub(crate) two_phase: bool,
    pub(crate) failover: bool,
    pub(crate) synced: bool,
    pub(crate) invalidation_reason: Option<String>,
    pub(crate) conflicting: bool,
    pub(crate) wal_status: Option<String>,
}

impl SlotAttributes {
    /// Why the slot differs from the slots this source creates, or `None` when it matches.
    pub(crate) fn problem(&self, database: &str, database_oid: u32) -> Option<String> {
        if self.slot_type.as_deref() != Some("logical")
            || self.plugin.as_deref() != Some("pgoutput")
        {
            return Some("is not a logical pgoutput slot".into());
        }
        if self.database.as_deref() != Some(database) || self.database_oid != Some(database_oid) {
            return Some(format!(
                "belongs to database '{}', not '{database}'",
                self.database.as_deref().unwrap_or("<none>")
            ));
        }
        if self.temporary {
            return Some("is temporary".into());
        }
        if self.two_phase || self.failover || self.synced {
            return Some(
                "has two_phase, failover, or synced set, which LaminarDB never creates".into(),
            );
        }
        if let Some(reason) = &self.invalidation_reason {
            return Some(format!("is invalidated ({reason})"));
        }
        if self.conflicting {
            return Some("conflicted with recovery".into());
        }
        if self.wal_status.as_deref() == Some("lost") {
            return Some("lost WAL it had to retain (wal_status = lost)".into());
        }
        None
    }
}

/// An existing slot: its durable position, its holder, and whether it may be adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotFacts {
    pub(crate) confirmed_flush_lsn: Option<Lsn>,
    pub(crate) holder: Option<SlotHolder>,
    /// Why the slot cannot be adopted; `None` when every adoption check passes.
    pub(crate) unusable: Option<String>,
}

/// A slot whose name starts with a source's prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixSlot {
    pub(crate) name: String,
    /// `application_name` of the session holding the slot, if it is active.
    pub(crate) holder: Option<String>,
    pub(crate) retained_bytes: Option<i64>,
    pub(crate) inactive_since: Option<String>,
}

/// The statement an operator runs to drop `slot` and release its WAL.
pub(crate) fn drop_statement(slot: &str) -> String {
    format!("SELECT pg_drop_replication_slot('{slot}')")
}

/// Every slot whose name starts with `<prefix>_`, in name order.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub(crate) async fn prefix_slots(
    client: &tokio_postgres::Client,
    prefix: &str,
) -> Result<Vec<PrefixSlot>, ConnectorError> {
    let rows = query(
        client.query(
            "SELECT s.slot_name::text, \
                    CASE WHEN s.active_pid IS NULL THEN NULL \
                         ELSE COALESCE(a.application_name, '') END, \
                    pg_catalog.pg_wal_lsn_diff( \
                        CASE WHEN pg_catalog.pg_is_in_recovery() \
                             THEN pg_catalog.pg_last_wal_replay_lsn() \
                             ELSE pg_catalog.pg_current_wal_lsn() END, \
                        s.restart_lsn)::bigint, \
                    s.inactive_since::text \
             FROM pg_catalog.pg_replication_slots AS s \
             LEFT JOIN pg_catalog.pg_stat_activity AS a ON a.pid = s.active_pid \
             WHERE pg_catalog.starts_with(s.slot_name::text, $1) \
             ORDER BY s.slot_name",
            &[&format!("{prefix}_")],
        ),
        "list replication slots sharing the slot.name prefix",
    )
    .await?;
    Ok(rows
        .iter()
        .map(|row| PrefixSlot {
            name: row.get(0),
            holder: row.get(1),
            retained_bytes: row.get(2),
            inactive_since: row.get(3),
        })
        .collect())
}

/// Sessions holding a transaction id, oldest first, and prepared transactions: what a new
/// logical slot waits for before it reaches a consistent point.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub(crate) async fn blocking_transactions(
    client: &tokio_postgres::Client,
) -> Result<Vec<String>, ConnectorError> {
    let sessions = query(
        client.query(
            "SELECT pg_catalog.format('pid %s (application_name %L, client_addr %s, state %s) \
                    holds xid %s since %s', pid, application_name, \
                    COALESCE(client_addr::text, 'local'), state, backend_xid, xact_start) \
             FROM pg_catalog.pg_stat_activity \
             WHERE backend_xid IS NOT NULL AND pid <> pg_catalog.pg_backend_pid() \
             ORDER BY xact_start NULLS LAST LIMIT 20",
            &[],
        ),
        "list transactions that block slot creation",
    )
    .await?;
    let prepared = query(
        client.query(
            "SELECT pg_catalog.format('prepared transaction %L (xid %s, owner %s, database %s) \
                    since %s', gid, transaction, owner, database, prepared) \
             FROM pg_catalog.pg_prepared_xacts ORDER BY prepared LIMIT 20",
            &[],
        ),
        "list prepared transactions that block slot creation",
    )
    .await?;
    Ok(sessions
        .iter()
        .chain(&prepared)
        .map(|row| row.get(0))
        .collect())
}

/// End `holder`'s session if it still holds `slot`, then wait until the slot is released.
///
/// # Errors
///
/// Returns a retryable error when the slot is still held after the wait, or a query fails.
pub(crate) async fn terminate_holder(
    client: &tokio_postgres::Client,
    slot: &str,
    holder: &SlotHolder,
) -> Result<(), ConnectorError> {
    query(
        client.query(
            "SELECT pg_catalog.pg_terminate_backend(a.pid, 5000) \
             FROM pg_catalog.pg_replication_slots AS s \
             JOIN pg_catalog.pg_stat_activity AS a ON a.pid = s.active_pid \
             WHERE s.slot_name = $1 AND a.pid = $2 AND a.application_name = $3",
            &[&slot, &holder.pid, &holder.application_name],
        ),
        "terminate the stale replication slot holder",
    )
    .await?;
    let deadline = tokio::time::Instant::now() + HOLDER_EXIT_TIMEOUT;
    loop {
        let active: Option<i32> = query(
            client.query_opt(
                "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            ),
            "query the replication slot holder",
        )
        .await?
        .and_then(|row| row.get(0));
        let Some(pid) = active else {
            return Ok(());
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(ConnectorError::ConnectionFailed(format!(
                "PostgreSQL replication slot '{slot}' is still held by pid {pid} 10 seconds after \
                 terminating {holder}"
            )));
        }
        tokio::time::sleep(HOLDER_EXIT_POLL).await;
    }
}
