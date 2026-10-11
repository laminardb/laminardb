//! `PostgreSQL` relation layout announced by `pgoutput` Relation messages.

use crate::error::ConnectorError;

use super::types::PgColumn;

/// Layout of a `PostgreSQL` relation (table).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Heap bytes retained by this layout.
    pub(crate) fn retained_bytes(&self) -> Result<usize, ConnectorError> {
        let overflow = || ConnectorError::ReadError("PostgreSQL CDC relation size overflow".into());
        let mut bytes = self
            .columns
            .capacity()
            .checked_mul(std::mem::size_of::<PgColumn>())
            .and_then(|bytes| bytes.checked_add(self.namespace.capacity()))
            .and_then(|bytes| bytes.checked_add(self.name.capacity()))
            .ok_or_else(overflow)?;
        for column in &self.columns {
            bytes = bytes
                .checked_add(column.name.capacity())
                .ok_or_else(overflow)?;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::cdc::types::{INT8_OID, TEXT_OID};

    #[test]
    fn layout_reports_retained_bytes() {
        let relation = RelationInfo {
            relation_id: 16_384,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: 'f',
            columns: vec![
                PgColumn::new("id".into(), INT8_OID, -1, true),
                PgColumn::new("name".into(), TEXT_OID, -1, true),
            ],
        };
        assert!(
            relation.retained_bytes().unwrap()
                >= 2 * std::mem::size_of::<PgColumn>() + "publicusersidname".len()
        );
    }
}
