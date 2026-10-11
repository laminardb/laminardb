//! Primary-key range tests on the server, which compares keys with the index's own types and
//! collations; Rust byte or locale order would disagree with the chunk `ORDER BY` for collated
//! text.

use std::fmt::Write as _;

use crate::error::ConnectorError;

use super::super::super::postgres_io::CONNECT_TIMEOUT;
use super::chunk::statement_error;
use super::{CopyPlan, Upper};

/// `text` as an SQL string literal that is read the same whatever `standard_conforming_strings`.
pub(crate) fn literal(text: &str) -> String {
    format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

/// `ROW(<key literal>::<type>, …)` of a key in index order.
pub(crate) fn key_row(plan: &CopyPlan, key: &[String]) -> String {
    let mut row = String::from("ROW(");
    for (index, (column, text)) in plan.keys.iter().zip(key).enumerate() {
        if index > 0 {
            row.push_str(", ");
        }
        let _ = write!(row, "{}::{}", literal(text), column.type_sql);
    }
    row.push(')');
    row
}

/// Whether each key is at or below `bound` (copied) and at or below `upper` (held), in order.
/// Keys are texts in index order.
///
/// # Errors
/// Returns `ConnectionFailed` when the statement could not run, and a read error for an
/// unexpected answer.
pub(crate) async fn classify(
    client: &tokio_postgres::Client,
    plan: &CopyPlan,
    bound: Option<&[String]>,
    upper: Option<&Upper>,
    keys: &[Vec<&str>],
) -> Result<Vec<(bool, bool)>, ConnectorError> {
    let statement = statement(plan, bound, upper);
    let columns: Vec<Vec<&str>> = (0..plan.keys.len())
        .map(|column| keys.iter().map(|key| key[column]).collect())
        .collect();
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = columns
        .iter()
        .map(|column| column as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = tokio::time::timeout(CONNECT_TIMEOUT, client.query(&statement, &params))
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(
                "classifying streamed keys against the copy timed out after 10 seconds".into(),
            )
        })?
        .map_err(|error| statement_error("key classification", &error))?;
    if rows.len() != keys.len() {
        return Err(ConnectorError::ReadError(format!(
            "PostgreSQL classified {} of {} streamed keys",
            rows.len(),
            keys.len()
        )));
    }
    Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
}

fn statement(plan: &CopyPlan, bound: Option<&[String]>, upper: Option<&Upper>) -> String {
    let mut stream_key = String::from("ROW(");
    let mut aliases = String::new();
    let mut arrays = String::new();
    for (index, column) in plan.keys.iter().enumerate() {
        if index > 0 {
            stream_key.push_str(", ");
            arrays.push_str(", ");
        }
        let _ = write!(stream_key, "(k.c{index})::{}", column.type_sql);
        if let Some(collation) = &column.collation {
            let _ = write!(stream_key, " COLLATE {collation}");
        }
        let _ = write!(arrays, "${}::text[]", index + 1);
        let _ = write!(aliases, "c{index}, ");
    }
    stream_key.push(')');
    let copied = bound.map_or_else(
        || "false".to_string(),
        |bound| format!("{stream_key} <= {}", key_row(plan, bound)),
    );
    let held = match upper {
        None => "false".to_string(),
        Some(Upper::End) => "true".to_string(),
        Some(Upper::Key(upper)) => format!("{stream_key} <= {}", key_row(plan, upper)),
    };
    format!(
        "SELECT {copied}, {held} FROM unnest({arrays}) WITH ORDINALITY AS k({aliases}ord) \
         ORDER BY k.ord"
    )
}

#[cfg(test)]
mod tests {
    use super::super::KeyColumn;
    use super::*;

    fn plan() -> CopyPlan {
        CopyPlan {
            table: "\"public\".\"t\"".into(),
            columns: "\"a\", \"b\"".into(),
            keys: vec![
                KeyColumn {
                    declared: 0,
                    name: "\"a\"".into(),
                    type_sql: "integer".into(),
                    collation: None,
                },
                KeyColumn {
                    declared: 1,
                    name: "\"b\"".into(),
                    type_sql: "text".into(),
                    collation: Some("\"pg_catalog\".\"C\"".into()),
                },
            ],
            relation_id: 7,
            fingerprint: String::new(),
        }
    }

    #[test]
    fn literals_survive_quotes_and_backslashes() {
        assert_eq!(literal(r"it's a \ test"), r"E'it''s a \\ test'");
    }

    #[test]
    fn keys_compare_under_the_index_collation_against_both_ends() {
        let plan = plan();
        let bound = vec!["1".to_string(), "a'b".to_string()];
        let upper = Upper::Key(vec!["9".into(), "z".into()]);
        assert_eq!(
            statement(&plan, Some(&bound), Some(&upper)),
            "SELECT ROW((k.c0)::integer, (k.c1)::text COLLATE \"pg_catalog\".\"C\") <= \
             ROW(E'1'::integer, E'a''b'::text), ROW((k.c0)::integer, (k.c1)::text COLLATE \
             \"pg_catalog\".\"C\") <= ROW(E'9'::integer, E'z'::text) FROM unnest($1::text[], \
             $2::text[]) WITH ORDINALITY AS k(c0, c1, ord) ORDER BY k.ord"
        );
        assert_eq!(
            statement(&plan, None, Some(&Upper::End)),
            "SELECT false, true FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS k(c0, \
             c1, ord) ORDER BY k.ord"
        );
    }
}
