//! Facts read from the pinned schema closure: vocabulary, content models, type derivation,
//! substitution groups, document roots, and `href` target types.
//!
//! The closure is trusted (digest-pinned) input; this is a declaration index used to check
//! profiles before activation, not a validator. Content models are flattened to "which child
//! elements and attributes may appear under a type"; occurrence and ordering are left to the
//! validator. Reference target types come from the FpML `ecore:reference` annotation on `href`
//! attribute declarations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;
use quick_xml::XmlVersion;

use super::artifacts::Artifact;
use crate::error::ConnectorError;

const XSD: &str = "http://www.w3.org/2001/XMLSchema";
const ECORE: &str = "http://www.eclipse.org/emf/2002/Ecore";

/// `(namespace, local name)`.
pub(crate) type QName = (String, String);

/// What a type, group, or attribute group declares directly.
#[derive(Debug, Default, Clone)]
struct Container {
    elements: BTreeMap<QName, BTreeSet<QName>>,
    element_refs: BTreeSet<QName>,
    groups: BTreeSet<QName>,
    attribute_groups: BTreeSet<QName>,
    attributes: BTreeSet<QName>,
    wildcard: bool,
}

/// The children a type admits, after groups, references, substitution groups, and base types.
#[derive(Debug, Default, Clone)]
pub(crate) struct Content {
    /// Child element name to the types it is declared with.
    pub(crate) elements: BTreeMap<QName, BTreeSet<QName>>,
    pub(crate) attributes: BTreeSet<QName>,
    /// The content model has a wildcard; child names cannot be checked.
    pub(crate) wildcard: bool,
}

/// Declarations of one closure.
#[derive(Debug, Default)]
pub(crate) struct SchemaModel {
    /// Element name to the types it is declared with, globally or locally.
    pub(crate) element_types: BTreeMap<QName, BTreeSet<QName>>,
    /// Attribute names declared anywhere.
    pub(crate) attributes: BTreeSet<QName>,
    /// Type to its base type.
    pub(crate) bases: BTreeMap<QName, QName>,
    /// Type declaring an `href` attribute to the type its targets must have.
    pub(crate) reference_targets: BTreeMap<QName, QName>,
    /// Substitution group head to its direct members.
    pub(crate) substitutions: BTreeMap<QName, BTreeSet<QName>>,
    /// Global element declarations.
    pub(crate) globals: BTreeMap<QName, Option<QName>>,
    containers: BTreeMap<QName, Container>,
    derived: BTreeMap<QName, BTreeSet<QName>>,
    contents: Mutex<HashMap<QName, Arc<Content>>>,
}

impl SchemaModel {
    /// The model for `artifact`, built once per closure fingerprint.
    pub(crate) fn for_artifact(artifact: &Artifact) -> Result<Arc<Self>, ConnectorError> {
        static MODELS: OnceLock<Mutex<HashMap<String, Arc<SchemaModel>>>> = OnceLock::new();
        let models = MODELS.get_or_init(Mutex::default);
        let poisoned = || ConnectorError::Internal("schema model cache poisoned".into());
        if let Some(model) = models
            .lock()
            .map_err(|_| poisoned())?
            .get(&artifact.fingerprint)
        {
            return Ok(Arc::clone(model));
        }
        let mut model = Self::default();
        let mut anonymous = 0_usize;
        for (name, bytes) in &artifact.files.files {
            model.read(name, bytes, &mut anonymous)?;
        }
        for (ty, base) in &model.bases {
            model
                .derived
                .entry(base.clone())
                .or_default()
                .insert(ty.clone());
        }
        let model = Arc::new(model);
        models
            .lock()
            .map_err(|_| poisoned())?
            .insert(artifact.fingerprint.clone(), Arc::clone(&model));
        Ok(model)
    }

    /// `ty` and every type it derives from.
    pub(crate) fn ancestors<'a>(&'a self, ty: &'a QName) -> impl Iterator<Item = &'a QName> + 'a {
        let mut next = Some(ty);
        let mut steps = 0_usize;
        std::iter::from_fn(move || {
            let current = next?;
            steps += 1;
            // Derivation chains are acyclic in a valid schema; bound them anyway.
            next = if steps < 64 {
                self.bases.get(current)
            } else {
                None
            };
            Some(current)
        })
    }

    /// Whether `ty` is or derives from `base`.
    pub(crate) fn derives_from(&self, ty: &QName, base: &QName) -> bool {
        self.ancestors(ty).any(|ancestor| ancestor == base)
    }

    /// `ty` and every type derived from it, which `xsi:type` may select.
    pub(crate) fn descendants(&self, ty: &QName) -> BTreeSet<QName> {
        let mut out = BTreeSet::from([ty.clone()]);
        let mut pending = vec![ty.clone()];
        while let Some(current) = pending.pop() {
            for derived in self.derived.get(&current).into_iter().flatten() {
                if out.insert(derived.clone()) {
                    pending.push(derived.clone());
                }
            }
        }
        out
    }

    /// Every member of the substitution group headed by `head`, transitively.
    pub(crate) fn substitution_members(&self, head: &QName) -> BTreeSet<QName> {
        let mut members = BTreeSet::new();
        let mut pending = vec![head.clone()];
        while let Some(current) = pending.pop() {
            for member in self.substitutions.get(&current).into_iter().flatten() {
                if members.insert(member.clone()) {
                    pending.push(member.clone());
                }
            }
        }
        members
    }

    /// Global elements whose type derives from `base` (for example FpML's `Document`).
    pub(crate) fn globals_deriving_from(&self, base: &QName) -> BTreeSet<QName> {
        self.globals
            .iter()
            .filter(|(_, ty)| ty.as_ref().is_some_and(|ty| self.derives_from(ty, base)))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Types an element named `name` may target through `href`. `None` unless every
    /// declaration of that name carries an `ecore:reference` target: without type-aware parsing
    /// the declaration that applies is unknown, so a name with any unconstrained declaration
    /// is unconstrained.
    pub(crate) fn reference_targets_of(&self, name: &QName) -> Option<BTreeSet<QName>> {
        let types = self.element_types.get(name)?;
        types
            .iter()
            .map(|ty| self.reference_target_of_type(ty).cloned())
            .collect()
    }

    /// The `ecore:reference` target of `ty` or its nearest annotated ancestor.
    pub(crate) fn reference_target_of_type(&self, ty: &QName) -> Option<&QName> {
        self.ancestors(ty)
            .find_map(|ancestor| self.reference_targets.get(ancestor))
    }

    /// Children and attributes allowed under `ty`, including inherited ones.
    pub(crate) fn content(&self, ty: &QName) -> Arc<Content> {
        if let Some(content) = self
            .contents
            .lock()
            .ok()
            .and_then(|contents| contents.get(ty).cloned())
        {
            return content;
        }
        let mut content = Content::default();
        let mut seen = BTreeSet::new();
        for ancestor in self.ancestors(ty) {
            self.collect(ancestor, &mut content, &mut seen);
        }
        let content = Arc::new(content);
        if let Ok(mut contents) = self.contents.lock() {
            contents.insert(ty.clone(), Arc::clone(&content));
        }
        content
    }

    fn collect(&self, container: &QName, out: &mut Content, seen: &mut BTreeSet<QName>) {
        if !seen.insert(container.clone()) {
            return;
        }
        let Some(declared) = self.containers.get(container) else {
            return;
        };
        out.wildcard |= declared.wildcard;
        for (name, types) in &declared.elements {
            out.elements
                .entry(name.clone())
                .or_default()
                .extend(types.iter().cloned());
        }
        for head in &declared.element_refs {
            for member in std::iter::once(head.clone()).chain(self.substitution_members(head)) {
                let types = out.elements.entry(member.clone()).or_default();
                if let Some(Some(ty)) = self.globals.get(&member) {
                    types.insert(ty.clone());
                }
            }
        }
        out.attributes.extend(declared.attributes.iter().cloned());
        for group in declared.groups.iter().chain(&declared.attribute_groups) {
            self.collect(group, out, seen);
        }
    }

    fn read(
        &mut self,
        file: &str,
        bytes: &[u8],
        anonymous: &mut usize,
    ) -> Result<(), ConnectorError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| schema_error(file, "schema file is not UTF-8"))?;
        let mut reader = NsReader::from_str(text);
        let mut scope = Scope::default();
        loop {
            let event = reader
                .read_event()
                .map_err(|error| schema_error(file, &error.to_string()))?;
            match event {
                Event::Start(start) => {
                    let frame = self.declare(&reader, &start, &mut scope, anonymous, file)?;
                    scope.stack.push(frame);
                }
                Event::Empty(start) => {
                    self.declare(&reader, &start, &mut scope, anonymous, file)?;
                }
                Event::End(_) => {
                    scope.stack.pop();
                }
                Event::Eof => break,
                _ => {}
            }
        }
        Ok(())
    }

    /// Record one XSD component and return the frame it opens.
    fn declare(
        &mut self,
        reader: &NsReader<&[u8]>,
        start: &BytesStart<'_>,
        scope: &mut Scope,
        anonymous: &mut usize,
        file: &str,
    ) -> Result<Frame, ConnectorError> {
        let (namespace, _) = reader.resolver().resolve_element(start.name());
        if !matches!(namespace, ResolveResult::Bound(ns) if ns.as_ref() == XSD.as_bytes()) {
            return Ok(Frame::Other);
        }
        let component = Component {
            reader,
            start,
            file,
        };
        let container = scope.container();
        Ok(match start.local_name().as_ref() {
            b"schema" => {
                scope.target = component.attribute("targetNamespace")?.unwrap_or_default();
                scope.qualified_elements =
                    component.attribute("elementFormDefault")?.as_deref() == Some("qualified");
                Frame::Schema
            }
            b"element" => self.declare_element(&component, scope, container.as_ref(), anonymous)?,
            b"complexType" | b"simpleType" => {
                let ty = match (component.attribute("name")?, scope.stack.last()) {
                    (Some(name), _) => (scope.target.clone(), name),
                    (None, Some(Frame::Element(ty))) => ty.clone(),
                    (None, _) => return Ok(Frame::Other),
                };
                self.container(&ty);
                Frame::Container(ty)
            }
            b"group" | b"attributeGroup" => {
                if let Some(name) = component.attribute("name")? {
                    return Ok(Frame::Container((
                        scope.target.clone(),
                        format!("#group-{name}"),
                    )));
                }
                if let (Some(reference), Some(container)) =
                    (component.attribute("ref")?, &container)
                {
                    let referenced = component.qname(&reference)?;
                    let group = (referenced.0, format!("#group-{}", referenced.1));
                    let declared = self.container(container);
                    if start.local_name().as_ref() == b"group" {
                        declared.groups.insert(group);
                    } else {
                        declared.attribute_groups.insert(group);
                    }
                }
                Frame::Other
            }
            b"extension" | b"restriction" => {
                if let (Some(ty), Some(base)) = (&container, component.attribute("base")?) {
                    if !ty.1.starts_with("#group-") {
                        self.bases.insert(ty.clone(), component.qname(&base)?);
                    }
                }
                Frame::Other
            }
            b"any" => {
                if let Some(container) = &container {
                    self.container(container).wildcard = true;
                }
                Frame::Other
            }
            b"attribute" => {
                self.declare_attribute(&component, container.as_ref())?;
                Frame::Other
            }
            _ => Frame::Other,
        })
    }

    fn declare_element(
        &mut self,
        component: &Component<'_, '_>,
        scope: &Scope,
        container: Option<&QName>,
        anonymous: &mut usize,
    ) -> Result<Frame, ConnectorError> {
        if let Some(reference) = component.attribute("ref")? {
            if let Some(container) = container {
                let referenced = component.qname(&reference)?;
                self.container(container).element_refs.insert(referenced);
            }
            return Ok(Frame::Other);
        }
        let Some(name) = component.attribute("name")? else {
            return Ok(Frame::Other);
        };
        let global = matches!(scope.stack.last(), Some(Frame::Schema));
        let qualified = global
            || component
                .attribute("form")?
                .map_or(scope.qualified_elements, |form| form == "qualified");
        let namespace = if qualified {
            scope.target.clone()
        } else {
            String::new()
        };
        let element = (namespace, name);
        let ty = if let Some(ty) = component.attribute("type")? {
            component.qname(&ty)?
        } else {
            *anonymous += 1;
            (scope.target.clone(), format!("#anonymous-{anonymous}"))
        };
        if global {
            self.globals.insert(element.clone(), Some(ty.clone()));
            if let Some(head) = component.attribute("substitutionGroup")? {
                self.substitutions
                    .entry(component.qname(&head)?)
                    .or_default()
                    .insert(element.clone());
            }
        }
        self.element_types
            .entry(element.clone())
            .or_default()
            .insert(ty.clone());
        if let Some(container) = container {
            self.container(container)
                .elements
                .entry(element)
                .or_default()
                .insert(ty.clone());
        }
        Ok(Frame::Element(ty))
    }

    fn declare_attribute(
        &mut self,
        component: &Component<'_, '_>,
        container: Option<&QName>,
    ) -> Result<(), ConnectorError> {
        let Some(name) = component.attribute("name")? else {
            return Ok(());
        };
        let attribute = (String::new(), name);
        self.attributes.insert(attribute.clone());
        let Some(container) = container else {
            return Ok(());
        };
        self.container(container)
            .attributes
            .insert(attribute.clone());
        if attribute.1 == "href" {
            if let Some(target) = component.ecore_reference()? {
                let target = component.qname(&target)?;
                self.reference_targets.insert(container.clone(), target);
            }
        }
        Ok(())
    }

    fn container(&mut self, name: &QName) -> &mut Container {
        self.containers.entry(name.clone()).or_default()
    }
}

/// One XSD element being declared, with the in-scope namespace bindings.
struct Component<'r, 's> {
    reader: &'r NsReader<&'r [u8]>,
    start: &'r BytesStart<'s>,
    file: &'r str,
}

impl Component<'_, '_> {
    fn attribute(&self, name: &str) -> Result<Option<String>, ConnectorError> {
        self.start
            .try_get_attribute(name)
            .map_err(|error| schema_error(self.file, &error.to_string()))?
            .map(|value| {
                value
                    .normalized_value(XmlVersion::Implicit1_0)
                    .map(std::borrow::Cow::into_owned)
                    .map_err(|error| schema_error(self.file, &error.to_string()))
            })
            .transpose()
    }

    /// Resolve a QName-valued attribute against the bindings in scope.
    fn qname(&self, value: &str) -> Result<QName, ConnectorError> {
        let (resolved, local) = self
            .reader
            .resolver()
            .resolve(quick_xml::name::QName(value.as_bytes()), true);
        let namespace = match resolved {
            ResolveResult::Bound(namespace) => utf8(namespace.as_ref(), self.file)?,
            ResolveResult::Unbound => String::new(),
            ResolveResult::Unknown(_) => {
                return Err(schema_error(
                    self.file,
                    &format!("unbound prefix in '{value}'"),
                ))
            }
        };
        Ok((namespace, utf8(local.as_ref(), self.file)?))
    }

    /// The `ecore:reference` annotation attribute, when present.
    fn ecore_reference(&self) -> Result<Option<String>, ConnectorError> {
        self.start
            .attributes()
            .flatten()
            .find(|attribute| {
                matches!(
                    self.reader.resolver().resolve_attribute(attribute.key).0,
                    ResolveResult::Bound(ns) if ns.as_ref() == ECORE.as_bytes()
                ) && attribute.key.local_name().as_ref() == b"reference"
            })
            .map(|attribute| {
                attribute
                    .normalized_value(XmlVersion::Implicit1_0)
                    .map(std::borrow::Cow::into_owned)
                    .map_err(|error| schema_error(self.file, &error.to_string()))
            })
            .transpose()
    }
}

#[derive(Debug, Clone)]
enum Frame {
    Schema,
    /// An element declaration and the (possibly anonymous) type it is declared with.
    Element(QName),
    /// A complex or simple type, model group, or attribute group.
    Container(QName),
    Other,
}

#[derive(Default)]
struct Scope {
    target: String,
    qualified_elements: bool,
    stack: Vec<Frame>,
}

impl Scope {
    /// The innermost type or group whose content a declaration belongs to.
    fn container(&self) -> Option<QName> {
        self.stack.iter().rev().find_map(|frame| match frame {
            Frame::Container(container) => Some(container.clone()),
            _ => None,
        })
    }
}

fn utf8(bytes: &[u8], file: &str) -> Result<String, ConnectorError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| schema_error(file, "name is not UTF-8"))
}

fn schema_error(file: &str, detail: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("schema closure file '{file}': {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FPML: &str = "http://www.fpml.org/FpML-5/confirmation";

    fn q(local: &str) -> QName {
        (FPML.to_owned(), local.to_owned())
    }

    fn model() -> Arc<SchemaModel> {
        SchemaModel::for_artifact(&Artifact::bundled().unwrap()).unwrap()
    }

    #[test]
    fn bundled_closure_declares_fifty_products_and_its_document_roots() {
        let model = model();
        let products = model.substitution_members(&q("product"));
        assert_eq!(products.len(), 50, "{products:?}");
        for product in [
            "swap",
            "fxSingleLeg",
            "creditDefaultSwap",
            "repo",
            "securityLending",
            "strategy",
            "genericProduct",
        ] {
            assert!(products.contains(&q(product)), "{product}");
        }
        let roots = model.globals_deriving_from(&q("Document"));
        for root in [
            "dataDocument",
            "requestConfirmation",
            "executionNotification",
            "facilityNotification",
            "loanContractNotification",
        ] {
            assert!(roots.contains(&q(root)), "{root}");
        }
        assert!(!roots.contains(&q("trade")));
    }

    #[test]
    fn href_targets_follow_ecore_reference_annotations() {
        let model = model();
        assert_eq!(
            model.reference_targets_of(&q("payerPartyReference")),
            Some(BTreeSet::from([q("Party")]))
        );
        assert!(model
            .reference_targets_of(&q("paymentDatesReference"))
            .is_some_and(|targets| targets.contains(&q("PaymentDates"))));
        // Declared both as DateReference (unannotated) and PartyReference.
        assert_eq!(model.reference_targets_of(&q("dateRelativeTo")), None);
        assert!(model.derives_from(&q("Swap"), &q("Product")));
    }

    #[test]
    fn content_models_flatten_groups_references_and_inheritance() {
        let model = model();
        let trade = model.content(&q("Trade"));
        assert!(trade.elements.contains_key(&q("tradeHeader")));
        // `product` is abstract; its substitution members are the admissible children.
        assert!(trade.elements.contains_key(&q("swap")));
        assert!(trade.elements.contains_key(&q("fxOption")));
        let swap = model.content(&q("Swap"));
        assert!(swap.elements.contains_key(&q("swapStream")));
        // Inherited from Product through the Product.model group.
        assert!(swap.elements.contains_key(&q("primaryAssetClass")));
        assert!(swap.attributes.contains(&(String::new(), "id".into())));
        let stream = model.content(&q("InterestRateStream"));
        assert!(stream.elements.contains_key(&q("payerPartyReference")));
        assert!(!stream.elements.contains_key(&q("tradeHeader")));
        assert!(model.descendants(&q("Product")).contains(&q("Swap")));
    }

    #[test]
    fn every_id_attribute_is_named_id() {
        // The tree copier indexes `id`; FpML declares no other xsd:ID attribute.
        let artifact = Artifact::bundled().unwrap();
        for (name, bytes) in &artifact.files.files {
            let text = std::str::from_utf8(bytes).unwrap();
            for (index, _) in text.match_indices("type=\"xsd:ID\"") {
                let line_start = text[..index].rfind('<').unwrap();
                assert!(
                    text[line_start..index].contains("name=\"id\""),
                    "{name}: {}",
                    &text[line_start..index]
                );
            }
        }
    }
}
