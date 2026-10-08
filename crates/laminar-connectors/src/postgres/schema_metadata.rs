//! Shared bounded PostgreSQL relation metadata reads. No DDL or data consumption.

use crate::error::ConnectorError;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) position: i32,
    pub(super) type_name: String,
    pub(super) type_oid: u32,
    pub(super) type_modifier: i32,
    pub(super) required: bool,
    pub(super) generated: String,
    pub(super) identity: String,
    pub(super) default: Option<String>,
}

pub(super) async fn read_table<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    schema_name: &str,
    table_name: &str,
) -> Result<Option<(i64, i64, Vec<Column>)>, ConnectorError> {
    // COMPAT: PostgreSQL 11 has no attgenerated catalog field.
    let rows = client.query(
        "SELECT c.oid::bigint, d.oid::bigint, a.attname, a.attnum::int4, t.typname, a.atttypid, a.atttypmod, a.attnotnull, COALESCE(to_jsonb(a)->>'attgenerated', ''), a.attidentity::text, pg_get_expr(ad.adbin, ad.adrelid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_database d ON d.datname=current_database() LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 4097",
        &[&schema_name, &table_name]).await
        .map_err(|_| ConnectorError::ConnectionFailed("PostgreSQL table metadata query failed; verify table visibility and authorization".into()))?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() > 4096 {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL target exceeds 4096 columns".into(),
        ));
    }
    let relation_oid: i64 = rows[0].try_get(0).map_err(metadata_error)?;
    let database_oid: i64 = rows[0].try_get(1).map_err(metadata_error)?;
    let columns = rows
        .iter()
        .map(|row| {
            Ok(Column {
                name: row.try_get(2).map_err(metadata_error)?,
                position: row.try_get(3).map_err(metadata_error)?,
                type_name: row.try_get(4).map_err(metadata_error)?,
                type_oid: row.try_get(5).map_err(metadata_error)?,
                type_modifier: row.try_get(6).map_err(metadata_error)?,
                required: row.try_get(7).map_err(metadata_error)?,
                generated: row.try_get(8).map_err(metadata_error)?,
                identity: row.try_get(9).map_err(metadata_error)?,
                default: row.try_get(10).map_err(metadata_error)?,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(Some((relation_oid, database_oid, columns)))
}

fn metadata_error(_: tokio_postgres::Error) -> ConnectorError {
    ConnectorError::SchemaMismatch("PostgreSQL catalog returned malformed metadata".into())
}
