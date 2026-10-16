//! Document-local reference integrity: every FpML `href` must name an element `id` in the same
//! document, and that element must have the type the schema's `ecore:reference` annotation
//! requires (FpML rules `ref-*`). `href` is never fetched or treated as a URL.

use std::collections::{HashMap, HashSet};

use super::document::{Document, Element, Interner, NameId};
use super::error::{Rejection, Stage};
use super::schema_model::SchemaModel;

/// How far `href` checking goes beyond resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferencePolicy {
    /// Every `href` resolves to a unique `id` in the document; a reference the projection
    /// dereferences must also reach the annotated target type.
    Resolve,
    /// Additionally, every `href` reaches the annotated target type. Official FpML 5.13 examples
    /// violate annotations the FpML validation rules do not enforce, so this is opt-in.
    Schema,
}

/// Interned reference rules for one closure.
#[derive(Debug, Default)]
pub(crate) struct ReferenceRules {
    /// Referencing element (or `xsi:type`) to the types its target may have.
    targets: HashMap<NameId, Vec<NameId>>,
    /// Element name to every type its declarations are or derive from.
    element_types: HashMap<NameId, HashSet<NameId>>,
    /// Type to itself and its ancestors, for elements carrying `xsi:type`.
    type_ancestors: HashMap<NameId, HashSet<NameId>>,
}

impl ReferenceRules {
    pub(crate) fn new(model: &SchemaModel, interner: &mut Interner) -> Self {
        let mut rules = Self::default();
        for (element, types) in &model.element_types {
            let name = interner.intern(&element.0, &element.1);
            let ancestors = rules.element_types.entry(name).or_default();
            for ty in types {
                for ancestor in model.ancestors(ty) {
                    ancestors.insert(interner.intern(&ancestor.0, &ancestor.1));
                }
            }
            if let Some(targets) = model.reference_targets_of(element) {
                rules.targets.insert(
                    name,
                    targets
                        .iter()
                        .map(|target| interner.intern(&target.0, &target.1))
                        .collect(),
                );
            }
        }
        for ty in model.bases.keys().chain(model.reference_targets.keys()) {
            let id = interner.intern(&ty.0, &ty.1);
            let ancestors = model
                .ancestors(ty)
                .map(|ancestor| interner.intern(&ancestor.0, &ancestor.1))
                .collect::<HashSet<_>>();
            if let Some(target) = model.reference_target_of_type(ty) {
                rules
                    .targets
                    .entry(id)
                    .or_insert_with(|| vec![interner.intern(&target.0, &target.1)]);
            }
            rules.type_ancestors.insert(id, ancestors);
        }
        rules
    }

    /// Check every `href` in `document`.
    pub(crate) fn check(
        &self,
        document: &Document,
        interner: &Interner,
        href: NameId,
        policy: ReferencePolicy,
    ) -> Result<(), Rejection> {
        for &source_id in &document.references {
            let Some(source) = document.element(source_id) else {
                continue;
            };
            let value = document.attribute(source, href).unwrap_or_default().trim();
            let Some(target) = document
                .dereference(value)
                .and_then(|id| document.element(id))
            else {
                return Err(Rejection::new(
                    Stage::References,
                    format!(
                        "line {}: '{}' refers to an id that does not exist in the document",
                        source.line,
                        interner.local(source.name)
                    ),
                ));
            };
            if policy == ReferencePolicy::Schema {
                self.check_target(source, target, interner)?;
            }
        }
        Ok(())
    }

    /// Reject a reference whose target lacks the annotated type. Unannotated or ambiguous
    /// reference names are not checked.
    pub(crate) fn check_target(
        &self,
        source: &Element,
        target: &Element,
        interner: &Interner,
    ) -> Result<(), Rejection> {
        let allowed = source
            .xsi_type
            .and_then(|ty| self.targets.get(&ty))
            .or_else(|| self.targets.get(&source.name));
        let Some(allowed) = allowed else {
            return Ok(());
        };
        let target_types = target
            .xsi_type
            .and_then(|ty| self.type_ancestors.get(&ty))
            .or_else(|| self.element_types.get(&target.name));
        if target_types.is_some_and(|types| allowed.iter().any(|ty| types.contains(ty))) {
            return Ok(());
        }
        Err(Rejection::new(
            Stage::References,
            format!(
                "line {}: '{}' refers to a '{}' element, which is not a {}",
                source.line,
                interner.local(source.name),
                interner.local(target.name),
                allowed
                    .iter()
                    .map(|ty| interner.local(*ty))
                    .collect::<Vec<_>>()
                    .join(" or ")
            ),
        ))
    }
}
