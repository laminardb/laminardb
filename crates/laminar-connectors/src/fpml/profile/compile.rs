//! Compile merged profile files into an interned [`Plan`] and its Arrow schema.
//!
//! Every selector is checked against the closure's content models: each step must name an
//! element (or attribute) that the schema admits under the types the previous step can have,
//! including types derived from them (`xsi:type`) and substitution-group members. Steps whose
//! context type is unknown (`**`, `..`, unannotated `->`) fall back to checking that the name
//! is declared at all. A struct template may be a union over several element types (for example
//! every underlying asset): a template field that a particular use cannot match compiles to an
//! always-null value there, and is an error only if no use of the template can match it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use super::plan::{
    Alternative, Column, Conversion, Meta, NameTest, Plan, Selector, Source, Step, Terminal, META,
};
use super::selector::{self, ParsedSelector, ParsedStep, ParsedTerminal, ParsedTest};
use super::spec::{FieldSpec, Header, OffsetPolicy, Roots, SetSpec, StructSpec, Whitespace};
use super::types::{self, ParsedType, UTC_ZONE};
use crate::fpml::artifacts::Artifact;
use crate::fpml::document::Interner;
use crate::fpml::schema_model::{QName, SchemaModel};

const FPML_DOCUMENT_TYPE: &str = "Document";
const MAX_TEMPLATE_DEPTH: usize = 32;
/// Mapping errors reported at once; compilation continues past each to report the rest.
const MAX_REPORTED_ERRORS: usize = 50;

/// Schema types an element may have at a point in a selector; `None` when unknown.
type Types = Option<BTreeSet<QName>>;

/// Profile content after includes are merged.
#[derive(Debug, Default)]
pub(crate) struct Merged {
    pub(crate) header: Option<Header>,
    pub(crate) namespaces: BTreeMap<String, String>,
    pub(crate) sets: BTreeMap<String, SetSpec>,
    pub(crate) structs: BTreeMap<String, StructSpec>,
    pub(crate) columns: Vec<FieldSpec>,
}

struct Set {
    ids: Arc<HashSet<u32>>,
    members: BTreeSet<QName>,
}

struct Compiler<'a> {
    artifact: &'a Artifact,
    model: &'a SchemaModel,
    namespaces: BTreeMap<String, String>,
    structs: &'a BTreeMap<String, StructSpec>,
    sets: HashMap<String, Set>,
    root_types: BTreeSet<QName>,
    interner: Interner,
    templates: Vec<String>,
    /// `(template, field)` to whether any use of the template can match the field.
    template_fields: BTreeMap<(String, String), bool>,
}

pub(crate) fn compile(
    merged: &Merged,
    artifact: &Artifact,
    model: &SchemaModel,
) -> Result<Plan, String> {
    let header = merged
        .header
        .as_ref()
        .ok_or("the root profile file has no [profile] header")?;
    if header.artifact != artifact.id {
        return Err(format!(
            "profile '{}' is written for schema closure '{}', not '{}'",
            header.name, header.artifact, artifact.id
        ));
    }
    let mut namespaces = merged.namespaces.clone();
    namespaces
        .entry(String::new())
        .or_insert_with(|| artifact.namespace.clone());
    let mut compiler = Compiler {
        artifact,
        model,
        namespaces,
        structs: &merged.structs,
        sets: HashMap::new(),
        root_types: BTreeSet::new(),
        interner: Interner::default(),
        templates: Vec::new(),
        template_fields: BTreeMap::new(),
    };
    for (name, spec) in &merged.sets {
        let set = compiler.set(name, spec)?;
        compiler.sets.insert(name.clone(), set);
    }
    let (roots, root_types) = compiler.roots(&header.roots)?;
    compiler.root_types.clone_from(&root_types);
    let (rows, row_types) = compiler.selector(&header.rows, &Some(root_types))?;
    if !rows.yields_elements() {
        return Err("'rows' must select elements".into());
    }
    if merged.columns.is_empty() {
        return Err("the profile declares no columns".into());
    }
    let mut columns = Vec::with_capacity(merged.columns.len());
    let mut fields = Vec::with_capacity(merged.columns.len());
    let mut seen = HashSet::new();
    let mut errors = Vec::new();
    for spec in &merged.columns {
        if !seen.insert(spec.name.as_str()) {
            errors.push(format!("duplicate column '{}'", spec.name));
            continue;
        }
        match compiler.column(spec, true, &row_types) {
            Ok((column, field)) => {
                columns.push(column);
                fields.push(field);
            }
            Err(error) => errors.push(error),
        }
    }
    for ((template, field), matched) in &compiler.template_fields {
        if !matched {
            errors.push(format!(
                "struct template '{template}': field '{field}' matches nothing in any use of the template"
            ));
        }
    }
    if !errors.is_empty() {
        let shown = errors.len().min(MAX_REPORTED_ERRORS);
        let mut report = errors[..shown].join("\n");
        if errors.len() > shown {
            let _ = write!(report, "\n… and {} more", errors.len() - shown);
        }
        return Err(report);
    }
    let row_bytes = fields
        .iter()
        .map(|field| fixed_bytes(field.data_type()))
        .sum();
    let fpml_version = compiler.interner.intern("", "fpmlVersion");
    Ok(Plan {
        schema: Arc::new(Schema::new(fields)),
        roots,
        rows,
        columns,
        interner: compiler.interner,
        fpml_version,
        release: artifact.release.clone(),
        row_bytes,
    })
}

impl Compiler<'_> {
    fn qname(&self, text: &str) -> Result<QName, String> {
        let (prefix, local) = match text.split_once(':') {
            Some((prefix, local)) => (prefix, local),
            None => ("", text),
        };
        let namespace = self
            .namespaces
            .get(prefix)
            .ok_or_else(|| format!("prefix '{prefix}' is not declared in [namespaces]"))?;
        Ok((namespace.clone(), local.to_owned()))
    }

    fn set(&mut self, name: &str, spec: &SetSpec) -> Result<Set, String> {
        let members: BTreeSet<QName> = match spec {
            SetSpec::Names(names) => names
                .iter()
                .map(|member| self.qname(member))
                .collect::<Result<_, _>>()?,
            SetSpec::Substitution { substitution } => {
                let head = self.qname(substitution)?;
                let members = self.model.substitution_members(&head);
                if members.is_empty() {
                    return Err(format!(
                        "set '{name}': '{substitution}' heads no substitution group in the closure"
                    ));
                }
                members
            }
        };
        let mut ids = HashSet::new();
        for member in &members {
            self.declared(member)?;
            ids.insert(self.interner.intern(&member.0, &member.1));
        }
        Ok(Set {
            ids: Arc::new(ids),
            members,
        })
    }

    fn roots(&mut self, roots: &Roots) -> Result<(HashSet<u32>, BTreeSet<QName>), String> {
        let names: Vec<QName> = match roots {
            Roots::Keyword(keyword) if keyword == "documents" => self
                .model
                .globals_deriving_from(&(
                    self.artifact.namespace.clone(),
                    FPML_DOCUMENT_TYPE.into(),
                ))
                .into_iter()
                .collect(),
            Roots::Keyword(other) => {
                return Err(format!(
                    "roots must be \"documents\" or a list of root element names, not '{other}'"
                ))
            }
            Roots::Names(names) => names
                .iter()
                .map(|name| self.qname(name))
                .collect::<Result<_, _>>()?,
        };
        if names.is_empty() {
            return Err("the profile accepts no document roots".into());
        }
        let mut ids = HashSet::new();
        let mut types = BTreeSet::new();
        for name in &names {
            let Some(Some(ty)) = self.model.globals.get(name) else {
                return Err(format!(
                    "root '{}' is not a global element of the closure",
                    name.1
                ));
            };
            types.insert(ty.clone());
            ids.insert(self.interner.intern(&name.0, &name.1));
        }
        Ok((ids, types))
    }

    fn declared(&self, name: &QName) -> Result<(), String> {
        if self.model.element_types.contains_key(name) {
            return Ok(());
        }
        Err(format!(
            "element '{}' (namespace '{}') is not declared in the schema closure",
            name.1, name.0
        ))
    }

    /// Types of child `name` under `context`, or an error naming where it is not admitted.
    fn child_types(&self, context: &Types, name: &QName) -> Result<Types, String> {
        let Some(context) = context else {
            self.declared(name)?;
            return Ok(self.model.element_types.get(name).cloned());
        };
        let mut found = BTreeSet::new();
        let mut open = false;
        for ty in context {
            for candidate in self.model.descendants(ty) {
                let admitted = self.model.content(&candidate);
                if let Some(types) = admitted.elements.get(name) {
                    found.extend(types.iter().cloned());
                }
                open |= admitted.wildcard;
            }
        }
        if !found.is_empty() {
            return Ok(Some(found));
        }
        if open {
            self.declared(name)?;
            return Ok(None);
        }
        Err(format!(
            "element '{}' is not a child of {}",
            name.1,
            type_list(context)
        ))
    }

    fn selector(&mut self, text: &str, context: &Types) -> Result<(Selector, Types), String> {
        let parsed = selector::parse_path(text, &self.namespaces)
            .map_err(|error| format!("selector '{text}': {error}"))?;
        let mut alternatives = Vec::with_capacity(parsed.len());
        let mut result: Option<BTreeSet<QName>> = Some(BTreeSet::new());
        for alternative in parsed {
            let (compiled, types) = self
                .alternative(alternative, context)
                .map_err(|error| format!("selector '{text}': {error}"))?;
            alternatives.push(compiled);
            result = match (result, types) {
                (Some(mut all), Some(types)) => {
                    all.extend(types);
                    Some(all)
                }
                _ => None,
            };
        }
        Ok((
            Selector {
                alternatives,
                text: Arc::from(text),
            },
            result,
        ))
    }

    fn alternative(
        &mut self,
        parsed: ParsedSelector,
        context: &Types,
    ) -> Result<(Alternative, Types), String> {
        let mut types = context.clone();
        let mut steps = Vec::with_capacity(parsed.steps.len());
        for step in parsed.steps {
            let (compiled, next) = match step {
                ParsedStep::Root => (Step::Root, Some(self.root_types.clone())),
                ParsedStep::Descendants => (Step::Descendants, None),
                ParsedStep::Parent => (Step::Parent, None),
                ParsedStep::Deref => (Step::Deref, self.dereferenced(&types)),
                ParsedStep::Filter(predicates) => {
                    let mut compiled_predicates = Vec::with_capacity(predicates.len());
                    for predicate in &predicates {
                        self.child_types(&types, predicate)?;
                        compiled_predicates.push(self.interner.intern(&predicate.0, &predicate.1));
                    }
                    (Step::Filter(compiled_predicates), types.clone())
                }
                ParsedStep::Child { test, predicates } => {
                    let (test, next) = match test {
                        ParsedTest::Any => (NameTest::Any, None),
                        ParsedTest::Set(set) => self.set_step(&set, &types)?,
                        ParsedTest::Name(name) => {
                            let next = self.child_types(&types, &name)?;
                            (NameTest::Name(self.interner.intern(&name.0, &name.1)), next)
                        }
                    };
                    let mut compiled_predicates = Vec::with_capacity(predicates.len());
                    for predicate in &predicates {
                        self.child_types(&next, predicate)?;
                        compiled_predicates.push(self.interner.intern(&predicate.0, &predicate.1));
                    }
                    (
                        Step::Child {
                            test,
                            predicates: compiled_predicates,
                        },
                        next,
                    )
                }
            };
            steps.push(compiled);
            types = next;
        }
        let terminal = match parsed.terminal {
            ParsedTerminal::Text => Terminal::Text,
            ParsedTerminal::Name => Terminal::Name,
            ParsedTerminal::Type => Terminal::Type,
            ParsedTerminal::Locator => Terminal::Locator,
            ParsedTerminal::Attribute(name) => {
                self.check_attribute(&types, &name)?;
                Terminal::Attribute(self.interner.intern(&name.0, &name.1))
            }
        };
        Ok((Alternative { steps, terminal }, types))
    }

    fn set_step(&self, set: &str, context: &Types) -> Result<(NameTest, Types), String> {
        let compiled = self
            .sets
            .get(set)
            .ok_or_else(|| format!("unknown set '{set}'"))?;
        let mut found = BTreeSet::new();
        let mut unknown = context.is_none();
        for member in &compiled.members {
            match self.child_types(context, member) {
                Ok(Some(types)) => found.extend(types),
                Ok(None) => unknown = true,
                Err(_) => {}
            }
        }
        if found.is_empty() && !unknown {
            return Err(format!(
                "no member of set '{set}' is a child of {}",
                type_list(context.as_ref().unwrap_or(&BTreeSet::new()))
            ));
        }
        Ok((
            NameTest::Set(Arc::clone(&compiled.ids)),
            (!unknown).then_some(found),
        ))
    }

    /// Target types of a reference step from `context`, when every context type is annotated.
    fn dereferenced(&self, context: &Types) -> Types {
        let context = context.as_ref()?;
        let mut targets = BTreeSet::new();
        for ty in context {
            let target = self.model.reference_target_of_type(ty)?;
            targets.extend(self.model.descendants(target));
        }
        // Element types of the target's declarations, so their content can be checked.
        Some(targets)
    }

    fn check_attribute(&self, context: &Types, name: &QName) -> Result<(), String> {
        if !name.0.is_empty() {
            return Ok(());
        }
        let Some(context) = context else {
            if self.model.attributes.contains(name) {
                return Ok(());
            }
            return Err(format!(
                "attribute '{}' is not declared in the schema closure",
                name.1
            ));
        };
        let admitted = context.iter().any(|ty| {
            self.model
                .descendants(ty)
                .iter()
                .any(|candidate| self.model.content(candidate).attributes.contains(name))
        });
        if admitted {
            Ok(())
        } else {
            Err(format!(
                "attribute '{}' is not declared on {}",
                name.1,
                type_list(context)
            ))
        }
    }

    fn column(
        &mut self,
        spec: &FieldSpec,
        top: bool,
        context: &Types,
    ) -> Result<(Column, Field), String> {
        let label = |error: String| format!("column '{}': {error}", spec.name);
        if spec.name.is_empty() {
            return Err("a column has an empty name".into());
        }
        let parsed = types::parse(&spec.ty).map_err(label)?;
        let (source, selected) = match (&spec.path, &spec.meta) {
            (Some(path), None) => {
                let (selector, selected) = self
                    .field_selector(spec, path, top, context)
                    .map_err(label)?;
                // Keeps per-row work proportional to the row's neighbourhood, not the document.
                if selector.has_descendants() {
                    return Err(label(
                        "'**' is only allowed in the profile's rows selector".into(),
                    ));
                }
                (Source::Path(selector), selected)
            }
            (None, Some(meta)) => {
                if !top {
                    return Err(label("metadata columns must be top-level".into()));
                }
                let meta = META
                    .iter()
                    .find(|(name, _)| name == meta)
                    .map(|(_, meta)| *meta)
                    .ok_or_else(|| label(format!("unknown metadata '{meta}'")))?;
                (Source::Meta(meta), None)
            }
            _ => return Err(label("needs exactly one of 'path' or 'meta'".into())),
        };
        if spec.whitespace == Whitespace::Collapse && parsed != ParsedType::Utf8 {
            return Err(label(
                "whitespace = \"collapse\" applies to utf8 columns".into(),
            ));
        }
        if spec.offset == OffsetPolicy::Optional
            && !matches!(parsed, ParsedType::Timestamp { utc: true, .. })
        {
            return Err(label(
                "offset = \"optional\" applies to timestamp(unit, UTC) columns".into(),
            ));
        }
        let (conversion, data_type) = self.conversion(&parsed, spec, &selected).map_err(label)?;
        if let Source::Meta(meta) = source {
            check_meta(meta, &conversion).map_err(label)?;
        }
        let structural = matches!(&conversion, Conversion::Struct(_))
            || matches!(&conversion, Conversion::List(item) if matches!(item.conversion, Conversion::Struct(_)));
        if let Source::Path(selector) = &source {
            if structural && !selector.yields_elements() {
                return Err(label("a struct column's path must select elements".into()));
            }
        }
        let mut field = Field::new(&spec.name, data_type, !spec.required);
        if let Some(description) = spec.description.as_deref().filter(|text| !text.is_empty()) {
            field = field.with_metadata(std::collections::HashMap::from([(
                "description".to_owned(),
                description.to_owned(),
            )]));
        }
        Ok((
            Column {
                name: spec.name.clone(),
                source,
                conversion,
                required: spec.required,
            },
            field,
        ))
    }

    fn conversion(
        &mut self,
        parsed: &ParsedType,
        spec: &FieldSpec,
        selected: &Types,
    ) -> Result<(Conversion, DataType), String> {
        Ok(match parsed {
            ParsedType::Utf8 => (
                Conversion::Utf8 {
                    collapse: spec.whitespace == Whitespace::Collapse,
                },
                DataType::Utf8,
            ),
            ParsedType::Bool => (Conversion::Bool, DataType::Boolean),
            ParsedType::Int32 => (Conversion::Int32, DataType::Int32),
            ParsedType::Int64 => (Conversion::Int64, DataType::Int64),
            ParsedType::UInt32 => (Conversion::UInt32, DataType::UInt32),
            ParsedType::UInt64 => (Conversion::UInt64, DataType::UInt64),
            ParsedType::Decimal { precision, scale } => (
                Conversion::Decimal {
                    precision: *precision,
                    scale: *scale,
                },
                DataType::Decimal128(*precision, *scale),
            ),
            ParsedType::Date32 => (Conversion::Date32, DataType::Date32),
            ParsedType::DateTime => (Conversion::DateTime, datetime_type()),
            ParsedType::Timestamp { unit, utc } => (
                Conversion::Timestamp {
                    unit: *unit,
                    utc: *utc,
                    offset_optional: spec.offset == OffsetPolicy::Optional,
                },
                DataType::Timestamp(*unit, utc.then(|| Arc::from(UTC_ZONE))),
            ),
            ParsedType::Struct(template) => {
                let (columns, fields) = self.template(template, selected)?;
                (Conversion::Struct(columns), DataType::Struct(fields))
            }
            ParsedType::List(item) => {
                let item_spec = FieldSpec {
                    name: "item".into(),
                    ty: String::new(),
                    path: Some(".".into()),
                    meta: None,
                    required: false,
                    whitespace: spec.whitespace,
                    offset: spec.offset,
                    description: None,
                };
                let (conversion, data_type) = self.conversion(item, &item_spec, selected)?;
                let item_column = Column {
                    name: "item".into(),
                    source: Source::Path(self.selector(".", selected)?.0),
                    conversion,
                    required: false,
                };
                (
                    Conversion::List(Box::new(item_column)),
                    DataType::List(Arc::new(Field::new("item", data_type, true))),
                )
            }
        })
    }

    /// Compile a column's path. Inside a template, a path this use cannot match falls back to a
    /// name-only check and is recorded, so union templates work while typos still fail.
    fn field_selector(
        &mut self,
        spec: &FieldSpec,
        path: &str,
        top: bool,
        context: &Types,
    ) -> Result<(Selector, Types), String> {
        let template = match (top, self.templates.last()) {
            (false, Some(template)) => template.clone(),
            _ => return self.selector(path, context),
        };
        let key = (template, spec.name.clone());
        match self.selector(path, context) {
            Ok(compiled) => {
                self.template_fields.insert(key, true);
                Ok(compiled)
            }
            Err(_) if context.is_some() => {
                self.template_fields.entry(key).or_insert(false);
                self.selector(path, &None)
            }
            Err(error) => Err(error),
        }
    }

    fn template(&mut self, name: &str, context: &Types) -> Result<(Vec<Column>, Fields), String> {
        if self.templates.iter().any(|open| open == name) {
            return Err(format!("struct template '{name}' contains itself"));
        }
        if self.templates.len() >= MAX_TEMPLATE_DEPTH {
            return Err(format!(
                "struct templates nest deeper than {MAX_TEMPLATE_DEPTH}"
            ));
        }
        let spec = self
            .structs
            .get(name)
            .ok_or_else(|| format!("unknown struct template '{name}'"))?;
        if spec.fields.is_empty() {
            return Err(format!("struct template '{name}' has no fields"));
        }
        self.templates.push(name.to_owned());
        let mut columns = Vec::with_capacity(spec.fields.len());
        let mut fields = Vec::with_capacity(spec.fields.len());
        let mut seen = HashSet::new();
        let mut errors = Vec::new();
        for field in &spec.fields {
            if !seen.insert(field.name.as_str()) {
                errors.push(format!(
                    "struct template '{name}' repeats field '{}'",
                    field.name
                ));
                continue;
            }
            match self.column(field, false, context) {
                Ok((column, arrow)) => {
                    columns.push(column);
                    fields.push(arrow);
                }
                Err(error) => errors.push(format!("struct template '{name}': {error}")),
            }
        }
        self.templates.pop();
        if errors.is_empty() {
            Ok((columns, Fields::from(fields)))
        } else {
            Err(errors.join("\n"))
        }
    }
}

fn type_list(types: &BTreeSet<QName>) -> String {
    let names: Vec<&str> = types
        .iter()
        .take(4)
        .map(|(_, local)| local.as_str())
        .collect();
    let more = types.len().saturating_sub(names.len());
    if more > 0 {
        format!("{} (+{more} more types)", names.join(", "))
    } else {
        names.join(", ")
    }
}

/// `struct<utc: timestamp(ns, UTC), local: timestamp(ns), offset_minutes: int32>`.
fn datetime_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new(
            "utc",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(UTC_ZONE))),
            true,
        ),
        Field::new(
            "local",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("offset_minutes", DataType::Int32, true),
    ]))
}

fn check_meta(meta: Meta, conversion: &Conversion) -> Result<(), String> {
    let compatible = match meta {
        Meta::DocumentSha256 | Meta::Root | Meta::FpmlVersion | Meta::RowLocator => {
            matches!(conversion, Conversion::Utf8 { .. })
        }
        Meta::DocumentBytes | Meta::Row | Meta::RowCount => matches!(
            conversion,
            Conversion::UInt32 | Conversion::UInt64 | Conversion::Int32 | Conversion::Int64
        ),
    };
    if compatible {
        Ok(())
    } else {
        Err(format!(
            "metadata {meta:?} cannot be stored in this column type"
        ))
    }
}

/// Fixed bytes one value of `data_type` stages, counting nested struct children.
fn fixed_bytes(data_type: &DataType) -> usize {
    match data_type {
        DataType::Struct(fields) => {
            1 + fields
                .iter()
                .map(|field| fixed_bytes(field.data_type()))
                .sum::<usize>()
        }
        DataType::Decimal128(_, _) => 16,
        DataType::Utf8
        | DataType::List(_)
        | DataType::Int32
        | DataType::UInt32
        | DataType::Date32 => 4,
        _ => 8,
    }
}
