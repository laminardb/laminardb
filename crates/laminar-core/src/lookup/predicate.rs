//! Predicate types for lookup table query pushdown.
//!
//! These types represent filter predicates that can be pushed down to
//! [`LookupSource`](crate::lookup) implementations. Sources that support
//! predicate pushdown can filter data at the source, reducing network
//! and deserialization costs.
//!
//! ## Pushdown Flow
//!
//! 1. SQL planner extracts WHERE predicates from a lookup join
//! 2. [`split_predicates`] classifies each predicate as pushable or local
//! 3. Pushable predicates are sent to the source via `LookupSource::query()`
//! 4. Local predicates are applied after fetching results

use std::fmt;

/// A scalar value used in predicate evaluation.
///
/// This enum covers the value types supported by lookup table predicates.
/// It is intentionally kept small — only types that can be pushed down to
/// external sources are included.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// SQL NULL
    Null,
    /// Boolean
    Bool(bool),
    /// 64-bit signed integer (covers i8/i16/i32/i64)
    Int64(i64),
    /// 64-bit float (covers f32/f64)
    Float64(f64),
    /// UTF-8 string
    Utf8(String),
    /// Raw binary data
    Binary(Vec<u8>),
    /// Timestamp as microseconds since Unix epoch
    Timestamp(i64),
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Utf8(v) => write!(f, "'{v}'"),
            Self::Binary(v) => write!(f, "X'{}'", hex_encode(v)),
            Self::Timestamp(us) => write!(f, "TIMESTAMP '{us}'"),
        }
    }
}

/// Encode bytes as lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// A filter predicate for lookup table queries.
///
/// Each variant maps directly to a SQL comparison operator. The `column`
/// field names the lookup table column, and the value(s) come from the
/// stream side of the join.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `column = value`
    Eq {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column != value`
    NotEq {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column < value`
    Lt {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column <= value`
    LtEq {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column > value`
    Gt {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column >= value`
    GtEq {
        /// Column name
        column: String,
        /// Value to compare against
        value: ScalarValue,
    },
    /// `column IN (values...)`
    In {
        /// Column name
        column: String,
        /// Set of values to match against
        values: Vec<ScalarValue>,
    },
    /// `column IS NULL`
    IsNull {
        /// Column name
        column: String,
    },
    /// `column IS NOT NULL`
    IsNotNull {
        /// Column name
        column: String,
    },
}

impl Predicate {
    /// Returns the column name this predicate references.
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Self::Eq { column, .. }
            | Self::NotEq { column, .. }
            | Self::Lt { column, .. }
            | Self::LtEq { column, .. }
            | Self::Gt { column, .. }
            | Self::GtEq { column, .. }
            | Self::In { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column } => column,
        }
    }
}

/// Capabilities that a lookup source declares for predicate pushdown.
///
/// Used by [`split_predicates`] to decide which predicates can be
/// pushed to the source vs. evaluated locally.
#[derive(Debug, Clone, Default)]
pub struct SourceCapabilities {
    /// Columns that support equality pushdown.
    pub eq_columns: Vec<String>,
    /// Columns that support range pushdown (`Lt`, `LtEq`, `Gt`, `GtEq`).
    pub range_columns: Vec<String>,
    /// Columns that support IN-list pushdown.
    pub in_columns: Vec<String>,
    /// Whether the source supports IS NULL / IS NOT NULL pushdown.
    pub supports_null_check: bool,
}

/// Result of splitting predicates into pushable and local sets.
#[derive(Debug, Clone)]
pub struct SplitPredicates {
    /// Predicates that can be pushed down to the source.
    pub pushable: Vec<Predicate>,
    /// Predicates that must be evaluated locally after fetching.
    pub local: Vec<Predicate>,
}

/// Classify predicates as pushable or local based on source capabilities.
///
/// # Arguments
///
/// * `predicates` - The full set of predicates from the query plan
/// * `capabilities` - What the lookup source supports
///
/// # Returns
///
/// A [`SplitPredicates`] with predicates partitioned into pushable and local.
#[must_use]
pub fn split_predicates(
    predicates: Vec<Predicate>,
    capabilities: &SourceCapabilities,
) -> SplitPredicates {
    let mut pushable = Vec::new();
    let mut local = Vec::new();

    for pred in predicates {
        let can_push = match &pred {
            Predicate::Eq { column, .. } | Predicate::NotEq { column, .. } => {
                capabilities.eq_columns.iter().any(|c| c == column)
            }
            Predicate::Lt { column, .. }
            | Predicate::LtEq { column, .. }
            | Predicate::Gt { column, .. }
            | Predicate::GtEq { column, .. } => {
                capabilities.range_columns.iter().any(|c| c == column)
            }
            Predicate::In { column, .. } => {
                capabilities.in_columns.iter().any(|c| c == column)
            }
            Predicate::IsNull { .. } | Predicate::IsNotNull { .. } => {
                capabilities.supports_null_check
            }
        };

        if can_push {
            pushable.push(pred);
        } else {
            local.push(pred);
        }
    }

    SplitPredicates { pushable, local }
}

/// Convert a predicate to a SQL WHERE clause fragment.
///
/// This is used by SQL-based lookup sources (Postgres/MySQL) to
/// construct parameterized queries.
///
/// # Returns
///
/// A SQL string fragment like `"column = 42"` or `"column IN (1, 2, 3)"`.
#[must_use]
pub fn predicate_to_sql(predicate: &Predicate) -> String {
    match predicate {
        Predicate::Eq { column, value } => format!("{column} = {value}"),
        Predicate::NotEq { column, value } => format!("{column} != {value}"),
        Predicate::Lt { column, value } => format!("{column} < {value}"),
        Predicate::LtEq { column, value } => format!("{column} <= {value}"),
        Predicate::Gt { column, value } => format!("{column} > {value}"),
        Predicate::GtEq { column, value } => format!("{column} >= {value}"),
        Predicate::In { column, values } => {
            let vals: Vec<String> = values.iter().map(ToString::to_string).collect();
            format!("{column} IN ({})", vals.join(", "))
        }
        Predicate::IsNull { column } => format!("{column} IS NULL"),
        Predicate::IsNotNull { column } => format!("{column} IS NOT NULL"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_value_display() {
        assert_eq!(ScalarValue::Null.to_string(), "NULL");
        assert_eq!(ScalarValue::Bool(true).to_string(), "true");
        assert_eq!(ScalarValue::Int64(42).to_string(), "42");
        assert_eq!(ScalarValue::Float64(3.14).to_string(), "3.14");
        assert_eq!(ScalarValue::Utf8("hello".into()).to_string(), "'hello'");
        assert_eq!(
            ScalarValue::Binary(vec![0xDE, 0xAD]).to_string(),
            "X'dead'"
        );
    }

    #[test]
    fn test_predicate_column() {
        let pred = Predicate::Eq {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        };
        assert_eq!(pred.column(), "id");

        let pred = Predicate::IsNull {
            column: "name".into(),
        };
        assert_eq!(pred.column(), "name");
    }

    #[test]
    fn test_predicate_to_sql() {
        assert_eq!(
            predicate_to_sql(&Predicate::Eq {
                column: "id".into(),
                value: ScalarValue::Int64(42),
            }),
            "id = 42"
        );

        assert_eq!(
            predicate_to_sql(&Predicate::In {
                column: "status".into(),
                values: vec![
                    ScalarValue::Utf8("active".into()),
                    ScalarValue::Utf8("pending".into()),
                ],
            }),
            "status IN ('active', 'pending')"
        );

        assert_eq!(
            predicate_to_sql(&Predicate::IsNull {
                column: "deleted_at".into(),
            }),
            "deleted_at IS NULL"
        );
    }

    #[test]
    fn test_split_predicates() {
        let capabilities = SourceCapabilities {
            eq_columns: vec!["id".into(), "name".into()],
            range_columns: vec!["created_at".into()],
            in_columns: vec!["status".into()],
            supports_null_check: false,
        };

        let predicates = vec![
            Predicate::Eq {
                column: "id".into(),
                value: ScalarValue::Int64(1),
            },
            Predicate::Gt {
                column: "created_at".into(),
                value: ScalarValue::Timestamp(1_000_000),
            },
            Predicate::IsNull {
                column: "deleted_at".into(),
            },
            Predicate::In {
                column: "status".into(),
                values: vec![ScalarValue::Utf8("active".into())],
            },
            // This Eq is on a non-pushable column
            Predicate::Eq {
                column: "region".into(),
                value: ScalarValue::Utf8("us-east".into()),
            },
        ];

        let split = split_predicates(predicates, &capabilities);
        assert_eq!(split.pushable.len(), 3); // id=, created_at>, status IN
        assert_eq!(split.local.len(), 2); // IS NULL (no null support), region=
    }

    #[test]
    fn test_split_predicates_empty_capabilities() {
        let capabilities = SourceCapabilities::default();
        let predicates = vec![Predicate::Eq {
            column: "id".into(),
            value: ScalarValue::Int64(1),
        }];

        let split = split_predicates(predicates, &capabilities);
        assert!(split.pushable.is_empty());
        assert_eq!(split.local.len(), 1);
    }
}
