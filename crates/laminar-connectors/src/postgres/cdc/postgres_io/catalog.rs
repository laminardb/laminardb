//! Catalog admission of the one captured table and its publication.

use super::super::config::{PostgresCdcConfig, TableName};
use super::super::schema::RelationInfo;
use super::super::types::PgColumn;
use super::CONNECT_TIMEOUT;
use crate::error::ConnectorError;

/// Catalog facts of the captured table, in the layout `pgoutput` announces for it.
#[derive(Debug, Clone)]
pub(crate) struct CaptureTable {
    /// Published columns in attribute order. Under `REPLICA IDENTITY FULL` `pgoutput` flags
    /// every column as an identity column, so `is_key` is `true` throughout.
    pub(crate) relation: RelationInfo,
    /// `attnotnull` per relation column.
    pub(crate) not_null: Vec<bool>,
    /// Primary-key column names in attribute order.
    pub(crate) primary_key: Vec<String>,
}

async fn query<T>(
    future: impl std::future::Future<Output = Result<T, tokio_postgres::Error>>,
    context: &str,
) -> Result<T, ConnectorError> {
    tokio::time::timeout(CONNECT_TIMEOUT, future)
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!("{context} timed out after 10 seconds"))
        })?
        .map_err(|error| ConnectorError::ConnectionFailed(format!("{context}: {error}")))
}

/// Validate the fixed publication/table contract and read the table layout.
///
/// The publication must publish insert, update, delete, and truncate for exactly the configured
/// table, without a row filter. The table must be an ordinary table with a primary key and
/// `REPLICA IDENTITY FULL`, which makes every update and delete carry its complete old row.
///
/// # Errors
///
/// Returns an actionable configuration error for any contract violation.
pub(crate) async fn inspect_capture_table(
    client: &tokio_postgres::Client,
    config: &PostgresCdcConfig,
) -> Result<CaptureTable, ConnectorError> {
    let published = published_columns(client, &config.publication, &config.table).await?;
    let relation_id = full_identity_table(client, &config.table).await?;
    read_columns(client, &config.table, relation_id, &published).await
}

/// The columns `publication` publishes for `table`, its only member.
async fn published_columns(
    client: &tokio_postgres::Client,
    publication: &str,
    table: &TableName,
) -> Result<Vec<String>, ConnectorError> {
    let flags = query(
        client.query_opt(
            "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate \
             FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication],
        ),
        "query PostgreSQL publication",
    )
    .await?
    .ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' does not exist"
        ))
    })?;
    let all_tables: bool = flags.get(0);
    let publishes_all = (1..5).all(|index| flags.get::<_, bool>(index));
    if all_tables || !publishes_all {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' must be FOR TABLE {table} with the default \
             publish='insert, update, delete, truncate'"
        )));
    }
    let members = query(
        client.query(
            "SELECT schemaname::text, tablename::text, rowfilter IS NOT NULL, \
                    attnames::text[] \
             FROM pg_catalog.pg_publication_tables WHERE pubname = $1 LIMIT 2",
            &[&publication],
        ),
        "query PostgreSQL publication tables",
    )
    .await?;
    let [member] = members.as_slice() else {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' must contain exactly the captured table \
             {table}; it publishes {} tables",
            members.len()
        )));
    };
    let (schema, name, row_filter): (String, String, bool) =
        (member.get(0), member.get(1), member.get(2));
    if schema != table.schema || name != table.name {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' publishes {schema}.{name}, not {table}"
        )));
    }
    if row_filter {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' has a row filter on {table}; row filters \
             turn updates into inserts and deletes and are not supported"
        )));
    }
    Ok(member.get(3))
}

/// The OID of `table`, which must be an ordinary table with `REPLICA IDENTITY FULL`.
async fn full_identity_table(
    client: &tokio_postgres::Client,
    table: &TableName,
) -> Result<u32, ConnectorError> {
    let row = query(
        client.query_opt(
            "SELECT c.oid, c.relkind::text, c.relreplident::text \
             FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&table.schema, &table.name],
        ),
        "query PostgreSQL captured table",
    )
    .await?
    .ok_or_else(|| {
        ConnectorError::ConfigurationError(format!("PostgreSQL table {table} does not exist"))
    })?;
    let relkind: String = row.get(1);
    let replica_identity: String = row.get(2);
    if relkind != "r" {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL relation {table} must be an ordinary table; partitioned tables, views, \
             and foreign tables are not supported"
        )));
    }
    if replica_identity != "f" {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL table {table} must use REPLICA IDENTITY FULL so updates and deletes carry \
             complete old rows and unchanged TOAST values can be restored; run \
             ALTER TABLE {table} REPLICA IDENTITY FULL"
        )));
    }
    Ok(row.get(0))
}

/// The published columns of the table in attribute order, with nullability and primary key.
async fn read_columns(
    client: &tokio_postgres::Client,
    table: &TableName,
    relation_id: u32,
    published: &[String],
) -> Result<CaptureTable, ConnectorError> {
    let columns = query(
        client.query(
            "SELECT a.attname::text, a.atttypid, a.atttypmod, a.attnotnull, \
                    COALESCE(a.attnum = ANY(i.indkey), false) \
             FROM pg_catalog.pg_attribute AS a \
             LEFT JOIN pg_catalog.pg_index AS i \
                    ON i.indrelid = a.attrelid AND i.indisprimary \
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
               AND a.attname::text = ANY($2) \
             ORDER BY a.attnum",
            &[&relation_id, &published],
        ),
        "query PostgreSQL captured columns",
    )
    .await?;
    if columns.is_empty() || columns.len() != published.len() {
        return Err(ConnectorError::SchemaMismatch(format!(
            "PostgreSQL table {table} published columns changed while being inspected"
        )));
    }
    let mut capture = CaptureTable {
        relation: RelationInfo {
            relation_id,
            namespace: table.schema.clone(),
            name: table.name.clone(),
            replica_identity: 'f',
            columns: Vec::with_capacity(columns.len()),
        },
        not_null: Vec::with_capacity(columns.len()),
        primary_key: Vec::new(),
    };
    for column in &columns {
        let name: String = column.get(0);
        if column.get::<_, bool>(4) {
            capture.primary_key.push(name.clone());
        }
        capture.not_null.push(column.get(3));
        capture
            .relation
            .columns
            .push(PgColumn::new(name, column.get(1), column.get(2), true));
    }
    if capture.primary_key.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL table {table} has no published primary key; the source binds rows by \
             primary key"
        )));
    }
    Ok(capture)
}
