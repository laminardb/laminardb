//! TOML representation of profile files.

use std::collections::BTreeMap;

use serde::Deserialize;

/// One profile file. Only the root file has a `[profile]` header and `[[columns]]`; included
/// files contribute namespaces, sets, and struct templates.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProfileFile {
    pub(crate) profile: Option<Header>,
    #[serde(default)]
    pub(crate) include: Vec<String>,
    #[serde(default)]
    pub(crate) namespaces: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) sets: BTreeMap<String, SetSpec>,
    #[serde(default)]
    pub(crate) structs: BTreeMap<String, StructSpec>,
    #[serde(default)]
    pub(crate) columns: Vec<FieldSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Header {
    pub(crate) name: String,
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) description: String,
    /// Schema closure identifier this profile is written against.
    pub(crate) artifact: String,
    /// `"documents"` for every global element derived from FpML `Document`, or local names.
    pub(crate) roots: Roots,
    /// Selector, from the document root, of the elements that become rows.
    pub(crate) rows: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum Roots {
    Keyword(String),
    Names(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum SetSpec {
    Names(Vec<String>),
    Substitution { substitution: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StructSpec {
    #[serde(default)]
    pub(crate) description: String,
    pub(crate) fields: Vec<FieldSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldSpec {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) ty: String,
    /// Selector relative to the row (or to the enclosing struct or list item).
    #[serde(default)]
    pub(crate) path: Option<String>,
    /// Connector metadata instead of a selector.
    #[serde(default)]
    pub(crate) meta: Option<String>,
    /// Absent values are rejected instead of becoming null.
    #[serde(default)]
    pub(crate) required: bool,
    /// `collapse` applies XML Schema whitespace collapsing to a `utf8` value.
    #[serde(default)]
    pub(crate) whitespace: Whitespace,
    /// For `timestamp(unit, UTC)`: `optional` makes a value written without an offset null
    /// instead of rejecting the document.
    #[serde(default)]
    pub(crate) offset: OffsetPolicy,
    #[serde(default)]
    pub(crate) description: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Whitespace {
    #[default]
    Preserve,
    Collapse,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OffsetPolicy {
    #[default]
    Required,
    Optional,
}
