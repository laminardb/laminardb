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
    /// Row-level security filters what this role reads from the table, which logical
    /// replication does not.
    pub(crate) row_security: bool,
}

pub(super) async fn query<T>(
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
    let (relation_id, row_security) = full_identity_table(client, &config.table).await?;
    let mut capture = read_columns(client, &config.table, relation_id, &published).await?;
    capture.row_security = row_security;
    Ok(capture)
}

/// The `columns` of relation `relation_id` this role cannot `SELECT`.
///
/// # Errors
///
/// Returns an error when the privilege query fails.
pub(crate) async fn unreadable_columns(
    client: &tokio_postgres::Client,
    relation_id: u32,
    columns: &[String],
) -> Result<Vec<String>, ConnectorError> {
    let rows = query(
        client.query(
            "SELECT name FROM unnest($2::text[]) AS name \
             WHERE NOT pg_catalog.has_column_privilege($1::oid, name, 'SELECT')",
            &[&relation_id, &columns],
        ),
        "query PostgreSQL column privileges",
    )
    .await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// One primary-key column of the captured table, in index order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexKeyColumn {
    pub(crate) name: String,
    /// `format_type` of the column.
    pub(crate) type_sql: String,
    /// Quoted, schema-qualified collation of the index column, for collatable types.
    pub(crate) collation: Option<String>,
    pub(crate) deterministic: bool,
    /// The index column uses the column's own collation.
    pub(crate) column_collation: bool,
    /// The index column uses its type's default operator class.
    pub(crate) default_opclass: bool,
}

/// The primary key of relation `relation_id` in index order, with what the copy needs to order
/// and compare keys exactly as the index does.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub(crate) async fn primary_key_index(
    client: &tokio_postgres::Client,
    relation_id: u32,
) -> Result<Vec<IndexKeyColumn>, ConnectorError> {
    let rows = query(
        client.query(
            "SELECT a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    CASE WHEN k.coll = 0 THEN NULL \
                         ELSE pg_catalog.quote_ident(cn.nspname) || '.' || \
                              pg_catalog.quote_ident(c.collname) END, \
                    COALESCE(c.collisdeterministic, true), k.coll = a.attcollation, \
                    o.opcdefault \
             FROM pg_catalog.pg_index AS i \
             CROSS JOIN LATERAL unnest(i.indkey::int2[], i.indcollation::oid[], \
                                       i.indclass::oid[]) \
                  WITH ORDINALITY AS k(attnum, coll, opclass, ord) \
             JOIN pg_catalog.pg_attribute AS a \
                  ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             JOIN pg_catalog.pg_opclass AS o ON o.oid = k.opclass \
             LEFT JOIN pg_catalog.pg_collation AS c ON c.oid = k.coll \
             LEFT JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.collnamespace \
             WHERE i.indrelid = $1 AND i.indisprimary AND k.ord <= i.indnkeyatts \
             ORDER BY k.ord",
            &[&relation_id],
        ),
        "query PostgreSQL primary-key index",
    )
    .await?;
    Ok(rows
        .iter()
        .map(|row| IndexKeyColumn {
            name: row.get(0),
            type_sql: row.get(1),
            collation: row.get(2),
            deterministic: row.get(3),
            column_collation: row.get(4),
            default_opclass: row.get(5),
        })
        .collect())
}

/// One text value that changes when the table's columns, types, collations, nullability, kind,
/// replica identity, or primary-key index change.
pub(crate) fn layout_fingerprint_sql(relation_id: u32) -> String {
    format!(
        "pg_catalog.concat_ws('|', \
           (SELECT pg_catalog.string_agg(pg_catalog.concat_ws(':', a.attname, a.atttypid, \
                       a.atttypmod, a.attcollation, a.attnotnull), ',' ORDER BY a.attnum) \
            FROM pg_catalog.pg_attribute AS a \
            WHERE a.attrelid = {relation_id} AND a.attnum > 0 AND NOT a.attisdropped), \
           (SELECT c.relkind::text || c.relreplident::text FROM pg_catalog.pg_class AS c \
            WHERE c.oid = {relation_id}), \
           (SELECT pg_catalog.concat_ws('/', i.indkey, i.indclass, i.indcollation, \
                                        i.indimmediate) \
            FROM pg_catalog.pg_index AS i WHERE i.indrelid = {relation_id} AND i.indisprimary))"
    )
}

/// The layout fingerprint of relation `relation_id` now.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub(crate) async fn layout_fingerprint(
    client: &tokio_postgres::Client,
    relation_id: u32,
) -> Result<String, ConnectorError> {
    let row = query(
        client.query_one(
            &format!("SELECT {}", layout_fingerprint_sql(relation_id)),
            &[],
        ),
        "query PostgreSQL table layout",
    )
    .await?;
    Ok(row.get(0))
}

/// Whether the server is a standby, where the copy's WAL position functions are unavailable.
///
/// # Errors
///
/// Returns an error when the query fails.
pub(crate) async fn in_recovery(client: &tokio_postgres::Client) -> Result<bool, ConnectorError> {
    let row = query(
        client.query_one("SELECT pg_catalog.pg_is_in_recovery()", &[]),
        "query PostgreSQL recovery state",
    )
    .await?;
    Ok(row.get(0))
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

/// The OID of `table`, which must be an ordinary table with `REPLICA IDENTITY FULL`, and whether
/// row-level security applies to this role's reads of it.
async fn full_identity_table(
    client: &tokio_postgres::Client,
    table: &TableName,
) -> Result<(u32, bool), ConnectorError> {
    let row = query(
        client.query_opt(
            "SELECT c.oid, c.relkind::text, c.relreplident::text, \
                    pg_catalog.row_security_active(c.oid) \
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
    Ok((row.get(0), row.get(3)))
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
                    COALESCE(a.attnum = ANY(i.indkey), false), COALESCE(i.indimmediate, true) \
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
        row_security: false,
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
    // A deferred key lets one transaction pass a key value between rows, so a row can arrive
    // before the removal of the row that held its key and, applied by key, be deleted by it.
    if !columns.iter().all(|column| column.get::<_, bool>(5)) {
        return Err(ConnectorError::ConfigurationError(format!(
            "the primary key of PostgreSQL table {table} is DEFERRABLE; changes keyed by it are \
             exact only when uniqueness is checked immediately"
        )));
    }
    Ok(capture)
}
