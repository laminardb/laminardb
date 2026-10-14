//! Compiles column mappings into a projection plan, validated against the declared Arrow schema
//! and the pinned FIX dictionary before activation.
//!
//! Mapping grammar, one property per declared column (`column.<name>`):
//! - `TAG` body field, `header.TAG`, `trailer.TAG`;
//! - `GROUP` repeating group as `List<Struct>`, whose struct fields map through
//!   `column.<name>.<field>` to member tags or nested groups;
//! - `GROUP[].TAG` / `GROUP[].NESTED` the current entry of the `expand` group;
//! - `@metadata` connector metadata. A column named `_fix_<metadata>` maps to it implicitly.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, SchemaRef, TimeUnit};

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::fix::config::COLUMN_PREFIX;
use crate::fix::engine::{DataDictionary, FieldType, GroupDictionary, Section};

/// Connector metadata available to columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Meta {
    MsgType,
    SeqNum,
    Series,
    Row,
    PossDup,
    PossResend,
    ReceivedAt,
    JournalIndex,
    Session,
    Message,
    GapBefore,
}

const META: &[(&str, Meta)] = &[
    ("msg_type", Meta::MsgType),
    ("seq_num", Meta::SeqNum),
    ("series", Meta::Series),
    ("row", Meta::Row),
    ("poss_dup", Meta::PossDup),
    ("poss_resend", Meta::PossResend),
    ("received_at", Meta::ReceivedAt),
    ("journal_index", Meta::JournalIndex),
    ("session", Meta::Session),
    ("message", Meta::Message),
    ("gap_before", Meta::GapBefore),
];

const RESERVED_PREFIX: &str = "_fix_";

/// Field map a value is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Header,
    Body,
    Trailer,
    /// Current entry of the expanded group.
    Expanded,
    /// Current entry of the enclosing list column's group.
    Entry,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Source {
    Meta(Meta),
    Field {
        scope: Scope,
        tag: i32,
    },
    Group {
        scope: Scope,
        count_tag: i32,
        children: Vec<ColumnPlan>,
    },
}

/// Arrow representation of one column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Conversion {
    Utf8,
    Binary,
    Boolean,
    Int64,
    Int32,
    UInt64,
    UInt32,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Timestamp {
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
    },
    Date32,
    List {
        item: FieldRef,
        fields: Fields,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnPlan {
    pub(crate) name: String,
    pub(crate) source: Source,
    pub(crate) conversion: Conversion,
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Expand {
    pub(crate) count_tag: i32,
    /// Emit one row with null entry values for a message without entries.
    pub(crate) empty_row: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectionPlan {
    pub(crate) columns: Vec<ColumnPlan>,
    pub(crate) expand: Option<Expand>,
    pub(crate) schema: SchemaRef,
}

fn error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

fn tag(text: &str) -> Result<i32, ConnectorError> {
    text.parse::<i32>()
        .ok()
        .filter(|tag| *tag > 0)
        .ok_or_else(|| error(format!("'{text}' is not a FIX tag number")))
}

fn conversion(field: &Field) -> Result<Conversion, ConnectorError> {
    Ok(match field.data_type() {
        DataType::Utf8 => Conversion::Utf8,
        DataType::Binary => Conversion::Binary,
        DataType::Boolean => Conversion::Boolean,
        DataType::Int64 => Conversion::Int64,
        DataType::Int32 => Conversion::Int32,
        DataType::UInt64 => Conversion::UInt64,
        DataType::UInt32 => Conversion::UInt32,
        DataType::Decimal128(precision, scale) if *scale >= 0 => Conversion::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Timestamp(unit, timezone) => Conversion::Timestamp {
            unit: *unit,
            timezone: timezone.clone(),
        },
        DataType::Date32 => Conversion::Date32,
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => Conversion::List {
                item: item.clone(),
                fields: fields.clone(),
            },
            _ => {
                return Err(error(format!(
                    "FIX list column '{}' must be LIST<STRUCT<...>>",
                    field.name()
                )))
            }
        },
        other => {
            return Err(error(format!(
                "FIX column '{}' has unsupported type {other}",
                field.name()
            )))
        }
    })
}

fn meta_compatible(meta: Meta, conversion: &Conversion) -> bool {
    match meta {
        Meta::MsgType | Meta::Session => matches!(conversion, Conversion::Utf8),
        Meta::SeqNum | Meta::JournalIndex => {
            matches!(conversion, Conversion::UInt64 | Conversion::Int64)
        }
        Meta::Series | Meta::Row => matches!(
            conversion,
            Conversion::UInt32 | Conversion::UInt64 | Conversion::Int32 | Conversion::Int64
        ),
        Meta::PossDup | Meta::PossResend | Meta::GapBefore => {
            matches!(conversion, Conversion::Boolean)
        }
        Meta::ReceivedAt => matches!(conversion, Conversion::Timestamp { .. }),
        Meta::Message => matches!(conversion, Conversion::Binary),
    }
}

fn field_compatible(field_type: FieldType, conversion: &Conversion) -> bool {
    match conversion {
        Conversion::Utf8 => field_type != FieldType::Data,
        Conversion::Binary => true,
        Conversion::Boolean => field_type == FieldType::Boolean,
        Conversion::Int64 | Conversion::Int32 | Conversion::UInt64 | Conversion::UInt32 => {
            field_type == FieldType::Integer
        }
        Conversion::Decimal { .. } => {
            matches!(field_type, FieldType::Decimal | FieldType::Integer)
        }
        Conversion::Timestamp { .. } => field_type == FieldType::UtcTimestamp,
        Conversion::Date32 => field_type == FieldType::Date,
        Conversion::List { .. } => false,
    }
}

/// Group dictionaries that define a group in at least one selected message type.
#[derive(Clone)]
struct Groups<'a> {
    by_message: Vec<(&'a str, GroupDictionary<'a>)>,
}

struct Compiler<'a> {
    config: &'a ConnectorConfig,
    dictionary: &'a DataDictionary,
    messages: &'a BTreeSet<String>,
    expand: Option<(i32, Groups<'a>)>,
    used_keys: BTreeSet<String>,
}

impl<'a> Compiler<'a> {
    fn mapping(&mut self, key: &str) -> Option<&'a str> {
        let value = self.config.get(key)?;
        self.used_keys.insert(key.to_string());
        Some(value.trim())
    }

    fn top_level_groups(&self, count_tag: i32) -> Result<Groups<'a>, ConnectorError> {
        let mut by_message = Vec::new();
        for message in self.messages {
            if let Some(group) = self.dictionary.group(message, count_tag)? {
                by_message.push((message.as_str(), group));
            }
        }
        if by_message.is_empty() {
            return Err(error(format!(
                "tag {count_tag} is not a repeating group of any selected message type"
            )));
        }
        Ok(Groups { by_message })
    }

    fn nested_groups(
        &self,
        parent: &Groups<'a>,
        count_tag: i32,
    ) -> Result<Groups<'a>, ConnectorError> {
        let mut by_message = Vec::new();
        for (message, group) in &parent.by_message {
            if let Some(nested) = group.group(message, count_tag)? {
                by_message.push((*message, nested));
            }
        }
        if by_message.is_empty() {
            return Err(error(format!(
                "tag {count_tag} is not a nested group of its parent group"
            )));
        }
        Ok(Groups { by_message })
    }

    fn check_field(
        &self,
        scope_groups: Option<&Groups<'a>>,
        section: Section,
        tag: i32,
        field: &Field,
        conversion: &Conversion,
    ) -> Result<(), ConnectorError> {
        let field_type = self.dictionary.field_type(tag)?.ok_or_else(|| {
            error(format!(
                "column '{}': tag {tag} is not in the dictionary",
                field.name()
            ))
        })?;
        if !field_compatible(field_type, conversion) {
            return Err(error(format!(
                "column '{}': FIX field {tag} ({field_type:?}) cannot be read as {}",
                field.name(),
                field.data_type()
            )));
        }
        let present = match scope_groups {
            Some(groups) => groups
                .by_message
                .iter()
                .map(|(_, group)| group.has_field(tag))
                .collect::<Result<Vec<_>, _>>()?
                .contains(&true),
            None => self
                .messages
                .iter()
                .map(|message| self.dictionary.placement(message, tag))
                .collect::<Result<Vec<_>, _>>()?
                .contains(&Some(section)),
        };
        if !present {
            return Err(error(format!(
                "column '{}': tag {tag} is not a {section:?} field of any selected message or group",
                field.name()
            )));
        }
        Ok(())
    }

    fn column(
        &mut self,
        key_prefix: &str,
        field: &Field,
        entry: Option<&Groups<'a>>,
    ) -> Result<ColumnPlan, ConnectorError> {
        let conversion = conversion(field)?;
        let key = format!("{key_prefix}{}", field.name());
        let path = match self.mapping(&key) {
            Some(path) => path.to_string(),
            None => match field.name().strip_prefix(RESERVED_PREFIX) {
                Some(meta) if entry.is_none() => format!("@{meta}"),
                _ => {
                    return Err(error(format!(
                        "column '{}' has no '{key}' mapping",
                        field.name()
                    )))
                }
            },
        };
        let source = if let Some(meta) = path.strip_prefix('@') {
            if entry.is_some() {
                return Err(error(format!(
                    "struct field '{}' cannot map metadata",
                    field.name()
                )));
            }
            let meta = META
                .iter()
                .find(|(name, _)| *name == meta)
                .map(|(_, meta)| *meta)
                .ok_or_else(|| error(format!("unknown FIX metadata '@{meta}'")))?;
            if !meta_compatible(meta, &conversion) {
                return Err(error(format!(
                    "column '{}': metadata '@{path}' cannot be {}",
                    field.name(),
                    field.data_type()
                )));
            }
            Source::Meta(meta)
        } else if let Some(groups) = entry {
            self.member(&key, field, &conversion, Scope::Entry, groups, &path)?
        } else if let Some((group, member)) = path.split_once("[].") {
            let Some((expand, groups)) = &self.expand else {
                return Err(error(format!(
                    "column '{}' reads '{group}[]' but 'expand' is not set",
                    field.name()
                )));
            };
            if tag(group)? != *expand {
                return Err(error(format!(
                    "column '{}' reads group {group}, but 'expand' is {expand}",
                    field.name()
                )));
            }
            let groups = groups.clone();
            self.member(&key, field, &conversion, Scope::Expanded, &groups, member)?
        } else {
            let (section, scope, text) = if let Some(text) = path.strip_prefix("header.") {
                (Section::Header, Scope::Header, text)
            } else if let Some(text) = path.strip_prefix("trailer.") {
                (Section::Trailer, Scope::Trailer, text)
            } else {
                (Section::Body, Scope::Body, path.as_str())
            };
            let tag = tag(text)?;
            if let Conversion::List { fields, .. } = &conversion {
                if section != Section::Body {
                    return Err(error(format!(
                        "column '{}': only body groups map to lists",
                        field.name()
                    )));
                }
                let groups = self.top_level_groups(tag)?;
                Source::Group {
                    scope,
                    count_tag: tag,
                    children: self.children(&key, fields, &groups)?,
                }
            } else {
                self.check_field(None, section, tag, field, &conversion)?;
                Source::Field { scope, tag }
            }
        };
        Ok(ColumnPlan {
            name: field.name().clone(),
            source,
            conversion,
            nullable: field.is_nullable(),
        })
    }

    fn member(
        &mut self,
        key: &str,
        field: &Field,
        conversion: &Conversion,
        scope: Scope,
        groups: &Groups<'a>,
        text: &str,
    ) -> Result<Source, ConnectorError> {
        let tag = tag(text)?;
        if let Conversion::List { fields, .. } = conversion {
            let nested = self.nested_groups(groups, tag)?;
            return Ok(Source::Group {
                scope,
                count_tag: tag,
                children: self.children(key, fields, &nested)?,
            });
        }
        self.check_field(Some(groups), Section::Body, tag, field, conversion)?;
        Ok(Source::Field { scope, tag })
    }

    fn children(
        &mut self,
        key: &str,
        fields: &Fields,
        groups: &Groups<'a>,
    ) -> Result<Vec<ColumnPlan>, ConnectorError> {
        let prefix = format!("{key}.");
        fields
            .iter()
            .map(|child| self.column(&prefix, child, Some(groups)))
            .collect()
    }
}

/// Compile the mapping for `schema`.
///
/// # Errors
/// Returns a configuration error for an unmapped column, unknown mapping key, unknown tag,
/// incompatible type, or a field absent from every selected message type.
pub(crate) fn compile(
    config: &ConnectorConfig,
    schema: &SchemaRef,
    dictionary: &DataDictionary,
    messages: &BTreeSet<String>,
) -> Result<ProjectionPlan, ConnectorError> {
    for message in messages {
        if !dictionary.has_message(message)? {
            return Err(error(format!(
                "selected message type '{message}' is not in the dictionary"
            )));
        }
    }
    let mut compiler = Compiler {
        config,
        dictionary,
        messages,
        expand: None,
        used_keys: BTreeSet::new(),
    };
    let expand = match config
        .get("expand")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(text) => {
            let count_tag = tag(text)?;
            let groups = compiler.top_level_groups(count_tag)?;
            compiler.expand = Some((count_tag, groups));
            let empty_row = match config.get("expand.empty").map(str::trim) {
                Some("row") | None => true,
                Some("skip") => false,
                Some(other) => {
                    return Err(error(format!(
                        "'expand.empty' must be 'row' or 'skip', got '{other}'"
                    )))
                }
            };
            Some(Expand {
                count_tag,
                empty_row,
            })
        }
        None => None,
    };
    let columns = schema
        .fields()
        .iter()
        .map(|field| compiler.column(COLUMN_PREFIX, field, None))
        .collect::<Result<Vec<_>, _>>()?;
    let unused: Vec<&String> = config
        .properties()
        .keys()
        .filter(|key| key.starts_with(COLUMN_PREFIX) && !compiler.used_keys.contains(*key))
        .collect();
    if let Some(key) = unused.first() {
        return Err(error(format!(
            "mapping '{key}' does not name a declared column or struct field"
        )));
    }
    Ok(ProjectionPlan {
        columns,
        expand,
        schema: schema.clone(),
    })
}

impl ProjectionPlan {
    /// Canonical mapping bound into the schema contract.
    pub(crate) fn canonical(&self) -> serde_json::Value {
        fn column(plan: &ColumnPlan) -> serde_json::Value {
            let source = match &plan.source {
                Source::Meta(meta) => serde_json::json!({ "meta": format!("{meta:?}") }),
                Source::Field { scope, tag } => {
                    serde_json::json!({ "scope": format!("{scope:?}"), "tag": tag })
                }
                Source::Group {
                    scope,
                    count_tag,
                    children,
                } => serde_json::json!({
                    "scope": format!("{scope:?}"),
                    "group": count_tag,
                    "children": children.iter().map(column).collect::<Vec<_>>(),
                }),
            };
            serde_json::json!({ "name": plan.name, "source": source, "nullable": plan.nullable })
        }
        let mut canonical = BTreeMap::new();
        canonical.insert(
            "columns",
            serde_json::json!(self.columns.iter().map(column).collect::<Vec<_>>()),
        );
        canonical.insert(
            "expand",
            serde_json::json!(self.expand.map(|expand| serde_json::json!({
                "group": expand.count_tag,
                "empty_row": expand.empty_row,
            }))),
        );
        serde_json::json!(canonical)
    }
}
