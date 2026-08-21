//! `PostgreSQL` relation schema cache.
//!
//! Caches the schema (column metadata) for each relation received in
//! `pgoutput` Relation messages. Required because DML messages only
//! reference relations by OID --- the schema must be looked up from this cache.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::ConnectorError;

use super::types::PgColumn;

/// Cached information about a `PostgreSQL` relation (table).
#[derive(Debug, Clone)]
pub struct RelationInfo {
    /// The relation OID from `pgoutput`.
    pub relation_id: u32,

    /// Schema (namespace) name.
    pub namespace: String,

    /// Table name.
    pub name: String,

    /// Replica identity setting: 'd' (default), 'n' (nothing),
    /// 'f' (full), 'i' (index).
    pub replica_identity: char,

    /// Column descriptors in ordinal order.
    pub columns: Vec<PgColumn>,
}

impl RelationInfo {
    /// Returns the fully qualified table name: `namespace.name`.
    pub(crate) fn full_name(&self) -> Result<String, ConnectorError> {
        if self.namespace.is_empty() || self.name.is_empty() {
            return Err(ConnectorError::ReadError(
                "PostgreSQL CDC relation has an empty schema or table name".into(),
            ));
        }
        let length = self
            .namespace
            .len()
            .checked_add(1)
            .and_then(|length| length.checked_add(self.name.len()))
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC schema-qualified table name size overflow".into(),
                )
            })?;
        let mut table = String::new();
        table.try_reserve_exact(length).map_err(|error| {
            ConnectorError::ReadError(format!(
                "PostgreSQL CDC could not reserve {length} table-name bytes: {error}"
            ))
        })?;
        table.push_str(&self.namespace);
        table.push('.');
        table.push_str(&self.name);
        debug_assert_eq!(table.len(), length);
        Ok(table)
    }

    pub(crate) fn variable_retained_bytes(&self) -> Result<usize, ConnectorError> {
        let mut retained = self
            .namespace
            .capacity()
            .checked_add(self.name.capacity())
            .and_then(|bytes| {
                self.columns
                    .capacity()
                    .checked_mul(std::mem::size_of::<PgColumn>())
                    .and_then(|column_bytes| bytes.checked_add(column_bytes))
            })
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC relation-cache retained-byte size overflow".into(),
                )
            })?;
        for column in &self.columns {
            retained = retained
                .checked_add(column.name.capacity())
                .ok_or_else(|| {
                    ConnectorError::ReadError(
                        "PostgreSQL CDC relation-cache retained-byte size overflow".into(),
                    )
                })?;
        }
        Ok(retained)
    }
}

/// Cache of relation schemas received from `pgoutput` Relation messages.
///
/// The decoder populates this cache as it encounters Relation messages.
/// DML decoders look up column metadata by relation ID.
#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    relations: Vec<RelationInfo>,
    variable_retained_bytes: usize,
}

impl RelationCache {
    /// Creates an empty relation cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a relation in the cache.
    pub(crate) fn try_reserve_for(&mut self, relation_id: u32) -> Result<(), ConnectorError> {
        if self
            .relations
            .binary_search_by_key(&relation_id, |relation| relation.relation_id)
            .is_err()
        {
            self.relations.try_reserve_exact(1).map_err(|error| {
                ConnectorError::ReadError(format!(
                    "PostgreSQL CDC could not reserve relation-cache storage: {error}"
                ))
            })?;
        }
        Ok(())
    }

    pub(crate) fn reservation_growth_bytes(
        &self,
        relation_id: u32,
    ) -> Result<usize, ConnectorError> {
        if self
            .relations
            .binary_search_by_key(&relation_id, |relation| relation.relation_id)
            .is_ok()
            || self.relations.len() < self.relations.capacity()
        {
            return Ok(0);
        }
        self.relations
            .capacity()
            .max(1)
            .checked_mul(std::mem::size_of::<RelationInfo>())
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC relation-cache growth size overflow".into(),
                )
            })
    }

    pub(crate) fn insert(&mut self, info: RelationInfo) -> Result<(), ConnectorError> {
        self.try_reserve_for(info.relation_id)?;
        let new_bytes = info.variable_retained_bytes()?;
        match self
            .relations
            .binary_search_by_key(&info.relation_id, |relation| relation.relation_id)
        {
            Ok(index) => {
                let old_bytes = self.relations[index].variable_retained_bytes()?;
                self.variable_retained_bytes = self
                    .variable_retained_bytes
                    .checked_sub(old_bytes)
                    .and_then(|bytes| bytes.checked_add(new_bytes))
                    .ok_or_else(|| {
                        ConnectorError::Internal(
                            "PostgreSQL CDC relation-cache retained-byte invariant failed".into(),
                        )
                    })?;
                self.relations[index] = info;
            }
            Err(index) => {
                self.variable_retained_bytes = self
                    .variable_retained_bytes
                    .checked_add(new_bytes)
                    .ok_or_else(|| {
                        ConnectorError::ReadError(
                            "PostgreSQL CDC relation-cache retained-byte accounting overflow"
                                .into(),
                        )
                    })?;
                self.relations.insert(index, info);
            }
        }
        Ok(())
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize, ConnectorError> {
        self.container_retained_bytes()?
            .checked_add(self.variable_retained_bytes)
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC relation-cache retained-byte accounting overflow".into(),
                )
            })
    }

    fn container_retained_bytes(&self) -> Result<usize, ConnectorError> {
        self.relations
            .capacity()
            .checked_mul(std::mem::size_of::<RelationInfo>())
            .ok_or_else(|| {
                ConnectorError::ReadError(
                    "PostgreSQL CDC relation-cache container size overflow".into(),
                )
            })
    }

    /// Looks up a relation by its OID.
    #[must_use]
    pub fn get(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relations
            .binary_search_by_key(&relation_id, |relation| relation.relation_id)
            .ok()
            .map(|index| &self.relations[index])
    }

    /// Returns the number of cached relations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.relations.len()
    }

    /// Returns `true` if no relations are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Clears the cache.
    pub fn clear(&mut self) {
        self.relations = Vec::new();
        self.variable_retained_bytes = 0;
    }
}

/// Builds the CDC envelope schema used by [`PostgresCdcSource`](super::source::PostgresCdcSource).
///
/// This schema wraps change events in a uniform envelope with metadata
/// columns, making it compatible with any table structure.
#[must_use]
pub fn cdc_envelope_schema() -> SchemaRef {
    use arrow_schema::TimeUnit;
    Arc::new(Schema::new(vec![
        Field::new("_table", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_lsn", DataType::UInt64, false),
        Field::new(
            "_ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("_before", DataType::Utf8, true),
        Field::new("_after", DataType::Utf8, true),
    ]))
}

#[cfg(test)]
mod tests;
