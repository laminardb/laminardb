//! Publication-specific pgoutput metadata. Discovery never creates or advances a slot.

use std::collections::BTreeMap;

use arrow_schema::SchemaRef;

use super::{config::PostgresCdcConfig, postgres_io, schema::RelationInfo, types::PgColumn};
use crate::config::ConnectorConfig;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use crate::schema::resolution::{fixed_binding, NativeSchema, SchemaBinding};

pub(super) async fn resolve(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    guard: ConnectorTaskGuard,
) -> Result<SchemaBinding, ConnectorError> {
    let mut parsed = PostgresCdcConfig::from_config(config)?;
    parsed.normalize_table_filters();
    let mut binding = fixed_binding(config, explicit, &super::schema::cdc_envelope_schema())?;
    let connection = postgres_io::connect(&parsed, guard).await?;
    let result = read_publication(connection.client(), &parsed, &mut binding).await;
    connection.close().await;
    result?;
    Ok(binding)
}

async fn read_publication(
    client: &tokio_postgres::Client,
    config: &PostgresCdcConfig,
    binding: &mut SchemaBinding,
) -> Result<(), ConnectorError> {
    let inspected = postgres_io::inspect_replication_slot(client, &config.slot_name, "pgoutput",
        &config.database, &config.publication, postgres_io::source_config_digest(config)).await?
        .ok_or_else(|| ConnectorError::ConfigurationError("PostgreSQL CDC schema resolution requires the existing recovery slot; discovery does not create slots and initial snapshot-to-WAL startup remains unsupported".into()))?;
    // The observed LSN is not schema authority and is never advanced or persisted here.
    let rows = client.query("SELECT c.oid, pt.schemaname, pt.tablename, c.relreplident::text, a.attname, a.atttypid, a.atttypmod, (c.relreplident='f' OR EXISTS(SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid=c.oid AND (i.indisreplident OR (c.relreplident='d' AND i.indisprimary)) AND a.attnum=ANY(i.indkey))) FROM pg_catalog.pg_publication_tables pt JOIN pg_catalog.pg_namespace n ON n.nspname=pt.schemaname JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=pt.tablename JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid WHERE pt.pubname=$1 AND a.attnum>0 AND NOT a.attisdropped AND a.attname=ANY(pt.attnames) ORDER BY c.oid,a.attnum LIMIT 4097", &[&config.publication]).await
        .map_err(|_| ConnectorError::ReadError("PostgreSQL publication column metadata is unavailable; verify authorization".into()))?;
    if rows.is_empty() || rows.len() > 4096 {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL publication must contain 1..=4096 published columns".into(),
        ));
    }
    let mut relations = BTreeMap::new();
    for row in rows {
        let oid: u32 = row.try_get(0).map_err(metadata_error)?;
        let namespace: String = row.try_get(1).map_err(metadata_error)?;
        let name: String = row.try_get(2).map_err(metadata_error)?;
        let identity: String = row.try_get(3).map_err(metadata_error)?;
        let replica_identity = identity
            .chars()
            .next()
            .ok_or_else(|| ConnectorError::SchemaMismatch("missing replica identity".into()))?;
        let relation = relations.entry(oid).or_insert_with(|| RelationInfo {
            relation_id: oid,
            namespace,
            name,
            replica_identity,
            columns: Vec::new(),
        });
        relation.columns.push(PgColumn {
            name: row.try_get(4).map_err(metadata_error)?,
            type_oid: row.try_get(5).map_err(metadata_error)?,
            type_modifier: row.try_get(6).map_err(metadata_error)?,
            is_key: row.try_get(7).map_err(metadata_error)?,
        });
    }
    let relations: Vec<_> = relations.into_values().collect();
    binding.value = Some(NativeSchema {
        format: "pgoutput".into(),
        identity: BTreeMap::from([
            (
                "system_identifier".into(),
                inspected.binding.system_identifier.to_string(),
            ),
            (
                "database_oid".into(),
                inspected.binding.database_oid.to_string(),
            ),
            (
                "publication_oid".into(),
                inspected.binding.publication_oid.to_string(),
            ),
            ("slot".into(), config.slot_name.clone()),
        ]),
        definition: serde_json::json!({"slot_binding": inspected.binding, "relations": relations, "envelope": "pgoutput-json-envelope-v1"}),
        references: Vec::new(),
    });
    Ok(())
}

pub(super) fn restore_relations(
    binding: Option<&SchemaBinding>,
    checkpoint: &postgres_io::PostgresCheckpointBinding,
) -> Result<Option<BTreeMap<u32, RelationInfo>>, ConnectorError> {
    let Some(binding) = binding else {
        return Ok(None);
    };
    let native = binding.value.as_ref().ok_or_else(|| {
        ConnectorError::SchemaMismatch(
            "committed PostgreSQL CDC binding lacks publication identity; migrate the legacy catalog before activation".into(),
        )
    })?;
    if native.format != "pgoutput" {
        return Err(ConnectorError::SchemaMismatch(
            "CDC contract is not pgoutput".into(),
        ));
    }
    let expected: postgres_io::PostgresCheckpointBinding =
        serde_json::from_value(native.definition["slot_binding"].clone()).map_err(|_| {
            ConnectorError::SchemaMismatch("invalid committed PostgreSQL slot binding".into())
        })?;
    if expected != *checkpoint {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL schema binding and resume authority disagree".into(),
        ));
    }
    let relations: Vec<RelationInfo> =
        serde_json::from_value(native.definition["relations"].clone()).map_err(|_| {
            ConnectorError::SchemaMismatch("invalid committed publication layouts".into())
        })?;
    Ok(Some(
        relations
            .into_iter()
            .map(|relation| (relation.relation_id, relation))
            .collect(),
    ))
}

pub(super) fn validate_relation(
    expected: Option<&BTreeMap<u32, RelationInfo>>,
    incoming: &RelationInfo,
    config: &PostgresCdcConfig,
) -> Result<(), ConnectorError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if !config.should_include_table(&incoming.full_name()?) {
        return Ok(());
    }
    let layout = expected.get(&incoming.relation_id).ok_or_else(|| {
        ConnectorError::SchemaMismatch(
            "pgoutput announced an unbound/replaced relation; migrate the catalog".into(),
        )
    })?;
    if layout.namespace != incoming.namespace
        || layout.name != incoming.name
        || layout.replica_identity != incoming.replica_identity
        || layout.columns != incoming.columns
    {
        return Err(ConnectorError::SchemaMismatch(
            "pgoutput relation layout changed; intake stops before decoding under the new layout"
                .into(),
        ));
    }
    Ok(())
}

fn metadata_error(_: tokio_postgres::Error) -> ConnectorError {
    ConnectorError::SchemaMismatch("PostgreSQL publication returned malformed metadata".into())
}

#[cfg(test)]
mod tests {
    use super::super::types::{INT8_OID, TEXT_OID};
    use super::*;

    #[test]
    fn relation_drift_is_rejected_without_replacing_the_committed_layout() {
        let layout = RelationInfo {
            relation_id: 42,
            namespace: "public".into(),
            name: "events".into(),
            replica_identity: 'd',
            columns: vec![
                PgColumn::new("id".into(), INT8_OID, -1, true),
                PgColumn::new("label".into(), TEXT_OID, -1, false),
            ],
        };
        let expected = BTreeMap::from([(42, layout.clone())]);
        let config = PostgresCdcConfig::default();
        validate_relation(Some(&expected), &layout, &config).unwrap();
        let mut reordered = layout.clone();
        reordered.columns.swap(0, 1);
        let mut replaced = layout.clone();
        replaced.relation_id = 43;
        let mut changed_type = layout.clone();
        changed_type.columns[0] = PgColumn::new("id".into(), TEXT_OID, -1, true);
        let mut changed_identity = layout.clone();
        changed_identity.replica_identity = 'f';
        for incoming in [reordered, replaced, changed_type, changed_identity] {
            assert!(validate_relation(Some(&expected), &incoming, &config).is_err());
            assert_eq!(expected[&42].columns, layout.columns);
        }
    }
}
