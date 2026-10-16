//! Owned, bounded element tree of one validated document.
//!
//! The tree is copied out of libxml2's post-validation DOM, so it is exactly what the validator
//! accepted, including schema default attributes. Element and attribute names are interned in
//! the worker's [`Interner`], seeded with every name the compiled profile uses, so a selector
//! compares integers, never prefixes or strings.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::ops::Range;

/// Index of an interned `(namespace, local name)` pair.
pub(crate) type NameId = u32;
/// Index of an element in document order; the root is `0`.
pub(crate) type ElementId = u32;

pub(crate) const NO_ELEMENT: ElementId = u32::MAX;

/// Namespace URIs and qualified names, seeded with every name a profile uses.
#[derive(Debug, Clone, Default)]
pub(crate) struct Interner {
    namespaces: Vec<Box<str>>,
    namespace_ids: HashMap<Box<str>, u32>,
    names: Vec<(u32, Box<str>)>,
    name_ids: HashMap<(u32, Box<str>), NameId>,
}

impl Interner {
    pub(crate) fn namespace(&mut self, uri: &str) -> u32 {
        if let Some(id) = self.namespace_ids.get(uri) {
            return *id;
        }
        let id = u32::try_from(self.namespaces.len()).unwrap_or(u32::MAX);
        self.namespaces.push(uri.into());
        self.namespace_ids.insert(uri.into(), id);
        id
    }

    pub(crate) fn intern(&mut self, namespace: &str, local: &str) -> NameId {
        let namespace = self.namespace(namespace);
        let key = (namespace, Box::<str>::from(local));
        if let Some(id) = self.name_ids.get(&key) {
            return *id;
        }
        let id = u32::try_from(self.names.len()).unwrap_or(u32::MAX);
        self.names.push(key.clone());
        self.name_ids.insert(key, id);
        id
    }

    pub(crate) fn len(&self) -> usize {
        self.names.len()
    }

    pub(crate) fn local(&self, name: NameId) -> &str {
        self.names
            .get(name as usize)
            .map_or("", |(_, local)| local.as_ref())
    }

    pub(crate) fn namespace_of(&self, name: NameId) -> &str {
        self.names
            .get(name as usize)
            .and_then(|(namespace, _)| self.namespaces.get(*namespace as usize))
            .map_or("", AsRef::as_ref)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Element {
    pub(crate) name: NameId,
    pub(crate) parent: ElementId,
    pub(crate) first_child: ElementId,
    pub(crate) next_sibling: ElementId,
    pub(crate) attributes: Range<u32>,
    /// Direct character data; empty for an element with element children.
    pub(crate) text: Range<u32>,
    pub(crate) has_element_children: bool,
    /// Resolved `xsi:type`, when present.
    pub(crate) xsi_type: Option<NameId>,
    pub(crate) line: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct Attribute {
    pub(crate) name: NameId,
    pub(crate) value: Range<u32>,
}

/// A validated document. Only constructed by the validator worker.
#[derive(Debug)]
pub(crate) struct Document {
    pub(crate) elements: Vec<Element>,
    pub(crate) attributes: Vec<Attribute>,
    pub(crate) text: String,
    /// `id` attribute value to element.
    pub(crate) ids: HashMap<Box<str>, ElementId>,
    /// Elements carrying an `href` attribute, in document order.
    pub(crate) references: Vec<ElementId>,
}

impl Document {
    pub(crate) fn root(&self) -> Option<&Element> {
        self.elements.first()
    }

    pub(crate) fn element(&self, id: ElementId) -> Option<&Element> {
        self.elements.get(id as usize)
    }

    pub(crate) fn slice(&self, range: &Range<u32>) -> &str {
        self.text
            .get(range.start as usize..range.end as usize)
            .unwrap_or("")
    }

    pub(crate) fn text_of(&self, element: &Element) -> &str {
        self.slice(&element.text)
    }

    pub(crate) fn attribute(&self, element: &Element, name: NameId) -> Option<&str> {
        self.attributes
            .get(element.attributes.start as usize..element.attributes.end as usize)?
            .iter()
            .find(|attribute| attribute.name == name)
            .map(|attribute| self.slice(&attribute.value))
    }

    /// Children of `parent` in document order.
    pub(crate) fn children(&self, parent: ElementId) -> Children<'_> {
        Children {
            document: self,
            next: self
                .element(parent)
                .map_or(NO_ELEMENT, |element| element.first_child),
        }
    }

    /// The element whose `id` is `href`.
    pub(crate) fn dereference(&self, href: &str) -> Option<ElementId> {
        self.ids.get(href).copied()
    }

    /// Position path such as `/dataDocument/trade[2]`, from schema-validated local names.
    pub(crate) fn locator(&self, interner: &Interner, mut id: ElementId) -> String {
        let mut steps = Vec::new();
        while let Some(element) = self.element(id) {
            let ordinal = self
                .children(element.parent)
                .take_while(|sibling| *sibling != id)
                .filter(|sibling| {
                    self.element(*sibling)
                        .is_some_and(|other| other.name == element.name)
                })
                .count()
                + 1;
            steps.push((interner.local(element.name), ordinal));
            id = element.parent;
        }
        let mut path = String::new();
        for (local, ordinal) in steps.iter().rev() {
            path.push('/');
            path.push_str(local);
            if *ordinal > 1 {
                let _ = write!(path, "[{ordinal}]");
            }
        }
        path
    }
}

pub(crate) struct Children<'d> {
    document: &'d Document,
    next: ElementId,
}

impl Iterator for Children<'_> {
    type Item = ElementId;

    fn next(&mut self) -> Option<ElementId> {
        let current = self.next;
        let element = self.document.element(current)?;
        self.next = element.next_sibling;
        Some(current)
    }
}
