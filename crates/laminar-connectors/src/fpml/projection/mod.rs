//! Evaluate a compiled plan over one validated document and build its Arrow batch.
//!
//! The whole document is projected before anything is returned: a conversion error anywhere
//! rejects the document and discards every row built for it.

mod builder;
mod values;

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};

use self::builder::Builder;
use super::document::{Document, ElementId, Interner, NameId, NO_ELEMENT};
use super::error::{Rejection, Stage};
use super::limits::Limits;
use super::profile::{
    Alternative, Column, Conversion, Meta, Plan, Selector, Source, Step, Terminal,
};
use super::references::ReferenceRules;

/// Facts about the document bytes, supplied by the decoder.
pub(crate) struct DocumentInfo<'a> {
    pub(crate) sha256: &'a str,
    pub(crate) bytes: usize,
}

enum Value<'d> {
    Element(ElementId),
    Text(Cow<'d, str>),
}

struct RowMeta {
    ordinal: usize,
    count: usize,
    element: ElementId,
}

struct Projector<'a> {
    plan: &'a Plan,
    document: &'a Document,
    interner: &'a Interner,
    rules: &'a ReferenceRules,
    href: NameId,
    limits: &'a Limits,
    info: &'a DocumentInfo<'a>,
    staged: usize,
}

/// Project `document` through `plan`.
pub(crate) fn project(
    plan: &Plan,
    document: &Document,
    interner: &Interner,
    rules: &ReferenceRules,
    href: NameId,
    limits: &Limits,
    info: &DocumentInfo<'_>,
) -> Result<RecordBatch, Rejection> {
    let root = document
        .root()
        .ok_or_else(|| Rejection::new(Stage::Parse, "document has no root element"))?;
    if !plan.roots.contains(&root.name) {
        return Err(Rejection::new(
            Stage::Root,
            format!(
                "document root '{}' in namespace '{}' is not accepted by this profile",
                interner.local(root.name),
                interner.namespace_of(root.name)
            ),
        ));
    }
    match document.attribute(root, plan.fpml_version) {
        Some(version) if version == plan.release => {}
        Some(version) if version.len() <= 16 => {
            return Err(Rejection::new(
                Stage::Root,
                format!(
                    "fpmlVersion '{version}' does not match the pinned release '{}'",
                    plan.release
                ),
            ))
        }
        _ => {
            return Err(Rejection::new(
                Stage::Root,
                format!("fpmlVersion must be '{}'", plan.release),
            ))
        }
    }
    let mut projector = Projector {
        plan,
        document,
        interner,
        rules,
        href,
        limits,
        info,
        staged: 0,
    };
    let rows = projector.elements(&plan.rows, 0)?;
    if rows.len() > limits.max_rows {
        return Err(Rejection::new(
            Stage::Projection,
            format!(
                "document has {} rows; the limit is {}",
                rows.len(),
                limits.max_rows
            ),
        ));
    }
    let mut builders: Vec<Builder> = plan
        .columns
        .iter()
        .zip(plan.schema.fields())
        .map(|(column, field)| Builder::new(&column.conversion, field.data_type()))
        .collect();
    for (ordinal, row) in rows.iter().enumerate() {
        projector.stage(plan.row_bytes)?;
        let meta = RowMeta {
            ordinal,
            count: rows.len(),
            element: *row,
        };
        for (column, builder) in plan.columns.iter().zip(builders.iter_mut()) {
            projector.append(column, builder, *row, &meta)?;
        }
    }
    let arrays = builders
        .iter_mut()
        .map(Builder::finish)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Rejection::new(Stage::Projection, error.to_string()))?;
    RecordBatch::try_new_with_options(
        Arc::clone(&plan.schema),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .map_err(|error| Rejection::new(Stage::Projection, error.to_string()))
}

impl<'a> Projector<'a> {
    fn stage(&mut self, bytes: usize) -> Result<(), Rejection> {
        self.staged = self.staged.saturating_add(bytes);
        if self.staged > self.limits.max_staged_bytes {
            return Err(Rejection::new(
                Stage::Projection,
                format!(
                    "document stages more than {} Arrow bytes",
                    self.limits.max_staged_bytes
                ),
            ));
        }
        Ok(())
    }

    fn append(
        &mut self,
        column: &Column,
        builder: &mut Builder,
        context: ElementId,
        meta: &RowMeta,
    ) -> Result<(), Rejection> {
        let values = match &column.source {
            Source::Meta(source) => vec![Value::Text(self.meta(*source, meta))],
            Source::Path(selector) => self.values(selector, context)?,
        };
        self.append_values(column, builder, &values, meta)
            .map_err(|error| within(&column.name, error))
    }

    fn append_values(
        &mut self,
        column: &Column,
        builder: &mut Builder,
        values: &[Value<'a>],
        meta: &RowMeta,
    ) -> Result<(), Rejection> {
        if values.is_empty() {
            if column.required {
                return Err(Self::error(column, None, "required value is absent"));
            }
            builder.append_null();
            return Ok(());
        }
        if let Conversion::List(item) = &column.conversion {
            let child = &mut builder.children_mut()[0];
            for value in values {
                self.stage(4)?;
                self.append_one(item, child, value, meta)?;
            }
            return builder
                .close_list(values.len())
                .map_err(|reason| Self::error(column, None, reason));
        }
        match values {
            [value] => self.append_one(column, builder, value, meta),
            many => Err(Self::error(
                column,
                None,
                &format!(
                    "selector '{}' matched {} nodes; declare a list to keep them",
                    selector_text(column),
                    many.len()
                ),
            )),
        }
    }

    fn append_one(
        &mut self,
        column: &Column,
        builder: &mut Builder,
        value: &Value<'a>,
        meta: &RowMeta,
    ) -> Result<(), Rejection> {
        if let Conversion::Struct(children) = &column.conversion {
            let Value::Element(element) = value else {
                return Err(Self::error(column, None, "a struct needs an element"));
            };
            for (child, child_builder) in children.iter().zip(builder.children_mut()) {
                self.append(child, child_builder, *element, meta)?;
            }
            builder.close_struct();
            return Ok(());
        }
        let (text, line) = match value {
            Value::Text(text) => (text.as_ref(), None),
            Value::Element(id) => {
                let element = self
                    .document
                    .element(*id)
                    .ok_or_else(|| Self::error(column, None, "element is out of range"))?;
                if element.has_element_children {
                    return Err(Self::error(
                        column,
                        Some(element.line),
                        "selector reaches an element with element content; select a value inside it",
                    ));
                }
                (self.document.text_of(element), Some(element.line))
            }
        };
        let staged = builder
            .append_text(text)
            .map_err(|reason| Self::error(column, line, reason))?;
        self.stage(staged)
    }

    fn meta(&self, meta: Meta, row: &RowMeta) -> Cow<'a, str> {
        match meta {
            Meta::DocumentSha256 => Cow::Owned(self.info.sha256.to_owned()),
            Meta::DocumentBytes => Cow::Owned(self.info.bytes.to_string()),
            Meta::Root => Cow::Borrowed(
                self.document
                    .root()
                    .map_or("", |root| self.interner.local(root.name)),
            ),
            Meta::FpmlVersion => Cow::Borrowed(
                self.document
                    .root()
                    .and_then(|root| self.document.attribute(root, self.plan.fpml_version))
                    .unwrap_or(""),
            ),
            Meta::Row => Cow::Owned(row.ordinal.to_string()),
            Meta::RowCount => Cow::Owned(row.count.to_string()),
            Meta::RowLocator => Cow::Owned(self.document.locator(self.interner, row.element)),
        }
    }

    /// Elements selected by `selector` from `context`, alternatives concatenated in order.
    fn elements(
        &self,
        selector: &Selector,
        context: ElementId,
    ) -> Result<Vec<ElementId>, Rejection> {
        let mut out = Vec::new();
        for alternative in &selector.alternatives {
            out.extend(self.alternative(alternative, selector, context)?);
        }
        Ok(out)
    }

    fn alternative(
        &self,
        alternative: &Alternative,
        selector: &Selector,
        context: ElementId,
    ) -> Result<Vec<ElementId>, Rejection> {
        let mut current = vec![context];
        let mut next = Vec::new();
        for step in &alternative.steps {
            next.clear();
            for &id in &current {
                self.step(step, id, &mut next)?;
            }
            if next.len() > self.limits.max_elements {
                return Err(Rejection::new(
                    Stage::Projection,
                    format!("selector '{}' selects too many nodes", selector.text),
                ));
            }
            std::mem::swap(&mut current, &mut next);
        }
        Ok(current)
    }

    fn step(&self, step: &Step, id: ElementId, out: &mut Vec<ElementId>) -> Result<(), Rejection> {
        let document = self.document;
        match step {
            Step::Root => out.push(0),
            Step::Child { test, predicates } => {
                for child in document.children(id) {
                    let Some(element) = document.element(child) else {
                        continue;
                    };
                    if test.matches(element.name)
                        && predicates.iter().all(|required| {
                            document.children(child).any(|grandchild| {
                                document
                                    .element(grandchild)
                                    .is_some_and(|element| element.name == *required)
                            })
                        })
                    {
                        out.push(child);
                    }
                }
            }
            Step::Descendants => {
                let mut stack = vec![id];
                while let Some(current) = stack.pop() {
                    out.push(current);
                    let start = stack.len();
                    stack.extend(document.children(current));
                    stack[start..].reverse();
                }
            }
            Step::Parent => {
                if let Some(element) = document.element(id) {
                    if element.parent != NO_ELEMENT {
                        out.push(element.parent);
                    }
                }
            }
            Step::Deref => {
                let Some(source) = document.element(id) else {
                    return Ok(());
                };
                let Some(href) = document.attribute(source, self.href) else {
                    return Ok(());
                };
                let target_id = document.dereference(href.trim()).ok_or_else(|| {
                    Rejection::new(
                        Stage::References,
                        format!(
                            "line {}: '{}' refers to an id that does not exist",
                            source.line,
                            self.interner.local(source.name)
                        ),
                    )
                })?;
                if let Some(target) = document.element(target_id) {
                    self.rules.check_target(source, target, self.interner)?;
                }
                out.push(target_id);
            }
        }
        Ok(())
    }

    fn values(&self, selector: &Selector, context: ElementId) -> Result<Vec<Value<'a>>, Rejection> {
        let mut out = Vec::new();
        for alternative in &selector.alternatives {
            let elements = self.alternative(alternative, selector, context)?;
            out.extend(self.terminal(alternative.terminal, elements));
        }
        Ok(out)
    }

    fn terminal(&self, terminal: Terminal, elements: Vec<ElementId>) -> Vec<Value<'a>> {
        let document = self.document;
        match terminal {
            Terminal::Text => elements.into_iter().map(Value::Element).collect(),
            Terminal::Attribute(name) => elements
                .into_iter()
                .filter_map(|id| document.element(id))
                .filter_map(|element| document.attribute(element, name))
                .map(|value| Value::Text(Cow::Borrowed(value)))
                .collect(),
            Terminal::Name => elements
                .into_iter()
                .filter_map(|id| document.element(id))
                .map(|element| Value::Text(Cow::Borrowed(self.interner.local(element.name))))
                .collect(),
            Terminal::Type => elements
                .into_iter()
                .filter_map(|id| document.element(id))
                .filter_map(|element| element.xsi_type)
                .map(|ty| Value::Text(Cow::Borrowed(self.interner.local(ty))))
                .collect(),
            Terminal::Locator => elements
                .into_iter()
                .map(|id| Value::Text(Cow::Owned(document.locator(self.interner, id))))
                .collect(),
        }
    }

    fn error(column: &Column, line: Option<u32>, reason: &str) -> Rejection {
        let at = line
            .map(|line| format!(" at line {line}"))
            .unwrap_or_default();
        Rejection::new(
            Stage::Projection,
            format!("column '{}'{at}: {reason}", column.name),
        )
    }
}

fn selector_text(column: &Column) -> &str {
    match &column.source {
        Source::Path(selector) => &selector.text,
        Source::Meta(_) => "",
    }
}

/// Prefix the enclosing column for nested failures (`swap.legs.payer: …`).
fn within(name: &str, error: Rejection) -> Rejection {
    if error.stage != Stage::Projection {
        return error;
    }
    match error.detail.strip_prefix("column '") {
        Some(rest)
            if !rest.starts_with(&format!("{name}.")) && !rest.starts_with(&format!("{name}'")) =>
        {
            Rejection::new(Stage::Projection, format!("column '{name}.{rest}"))
        }
        _ => error,
    }
}
