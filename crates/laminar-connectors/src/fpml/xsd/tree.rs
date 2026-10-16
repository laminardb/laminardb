//! Copy a validated libxml2 tree into an owned [`Document`].

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::{c_char, CStr, CString};
use std::ops::Range;
use std::ptr;

use libxml2_ffi as ffi;

use crate::fpml::document::{
    Attribute, Document, Element, ElementId, Interner, NameId, NO_ELEMENT,
};
use crate::fpml::error::{Rejection, Stage};
use crate::fpml::limits::Limits;
use crate::fpml::schema_model::QName;

const XSI: &str = "http://www.w3.org/2001/XMLSchema-instance";

/// Names the copy treats specially. `references` lists the namespaces whose elements carry
/// FpML `id`/`href` semantics; elements in other namespaces, and everything inside an
/// `unvalidated` container (content the validator skipped), are copied but not indexed.
pub(crate) struct TreeNames {
    pub(crate) id: NameId,
    pub(crate) href: NameId,
    pub(crate) xsi_type: NameId,
    pub(crate) references: Vec<u32>,
    pub(crate) unvalidated: HashSet<NameId>,
}

impl TreeNames {
    pub(crate) fn new(
        interner: &mut Interner,
        reference_namespaces: &[String],
        unvalidated: &BTreeSet<QName>,
    ) -> Self {
        Self {
            id: interner.intern("", "id"),
            href: interner.intern("", "href"),
            xsi_type: interner.intern(XSI, "type"),
            references: reference_namespaces
                .iter()
                .map(|namespace| interner.namespace(namespace))
                .collect(),
            unvalidated: unvalidated
                .iter()
                .map(|(namespace, local)| interner.intern(namespace, local))
                .collect(),
        }
    }
}

/// Borrowed view of one libxml2 element, read through the generated struct layouts.
struct NodeView<'d> {
    node: *mut ffi::xmlNode,
    raw: &'d ffi::xmlNode,
}

/// Copy `document` element by element.
///
/// # Safety
/// `document` must be a live, fully parsed libxml2 document that is not modified or freed
/// while this runs; the returned value borrows nothing from it.
pub(super) unsafe fn copy(
    document: *mut ffi::xmlDoc,
    limits: &Limits,
    interner: &mut Interner,
    names: &TreeNames,
) -> Result<Document, Rejection> {
    // SAFETY: `document` is live per the caller's contract.
    let root = unsafe { ffi::xmlDocGetRootElement(document) };
    if root.is_null() {
        return Err(Rejection::new(Stage::Parse, "document has no root element"));
    }
    let mut copier = Copier {
        document,
        limits,
        ceiling: limits
            .max_document_bytes
            .saturating_mul(4)
            .min(u32::MAX as usize),
        interner,
        names,
        out: Document {
            elements: Vec::new(),
            attributes: Vec::new(),
            text: String::new(),
            ids: HashMap::new(),
            references: Vec::new(),
        },
        last_child: Vec::new(),
    };
    // Each entry carries whether the node lies inside skipped (unvalidated) content.
    let mut stack: Vec<(*mut ffi::xmlNode, ElementId, bool)> = vec![(root, NO_ELEMENT, false)];
    let mut children: Vec<*mut ffi::xmlNode> = Vec::new();
    while let Some((node, parent, skipped)) = stack.pop() {
        // SAFETY: every pointer on the stack is the root or an element child of a live node.
        let view = NodeView {
            node,
            raw: unsafe { &*node },
        };
        let (id, children_skipped) = copier.element(&view, parent, skipped, &mut children)?;
        stack.extend(
            children
                .iter()
                .rev()
                .map(|child| (*child, id, children_skipped)),
        );
    }
    Ok(copier.out)
}

struct Copier<'a> {
    document: *mut ffi::xmlDoc,
    limits: &'a Limits,
    ceiling: usize,
    interner: &'a mut Interner,
    names: &'a TreeNames,
    out: Document,
    last_child: Vec<ElementId>,
}

impl Copier<'_> {
    /// Copy one element and collect its element children (in document order) into `children`;
    /// also returns whether those children lie inside skipped content.
    fn element(
        &mut self,
        view: &NodeView<'_>,
        parent: ElementId,
        skipped: bool,
        children: &mut Vec<*mut ffi::xmlNode>,
    ) -> Result<(ElementId, bool), Rejection> {
        if self.out.elements.len() >= self.limits.max_elements {
            return Err(limit("elements", self.limits.max_elements));
        }
        let id = ElementId::try_from(self.out.elements.len())
            .map_err(|_| limit("elements", self.limits.max_elements))?;
        let namespace = namespace_of(view.raw.ns)?;
        let name = self.interner.intern(namespace, string(view.raw.name)?);
        let indexed = !skipped
            && self
                .names
                .references
                .contains(&self.interner.namespace(namespace));
        let attributes = self.attributes(view.raw.properties)?;
        let xsi_type = self.xsi_type(view.node, &attributes)?;
        if indexed {
            self.index(id, &attributes)?;
        }

        children.clear();
        let text_start = self.offset()?;
        let mut child = view.raw.children;
        while !child.is_null() {
            // SAFETY: `child` is a node in the live child list of `view`.
            let current = unsafe { &*child };
            match current.type_ {
                ffi::XML_ELEMENT_NODE => children.push(child),
                ffi::XML_TEXT_NODE | ffi::XML_CDATA_SECTION_NODE => {
                    self.out.text.push_str(string(current.content)?);
                    self.offset()?;
                }
                ffi::XML_ENTITY_REF_NODE => {
                    return Err(Rejection::new(
                        Stage::Parse,
                        "entity references are not accepted",
                    ));
                }
                _ => {}
            }
            child = current.next;
        }
        let mut text_end = self.offset()?;
        if !children.is_empty() {
            // Whitespace between child elements is not data.
            self.out.text.truncate(text_start as usize);
            text_end = text_start;
        }
        // SAFETY: `view.node` is a live element.
        let line = unsafe { ffi::xmlGetLineNo(view.node) };
        self.out.elements.push(Element {
            name,
            parent,
            first_child: NO_ELEMENT,
            next_sibling: NO_ELEMENT,
            attributes,
            text: text_start..text_end,
            has_element_children: !children.is_empty(),
            xsi_type,
            line: u32::try_from(line).unwrap_or(0),
        });
        self.last_child.push(NO_ELEMENT);
        if parent != NO_ELEMENT {
            let previous = self.last_child[parent as usize];
            if previous == NO_ELEMENT {
                self.out.elements[parent as usize].first_child = id;
            } else {
                self.out.elements[previous as usize].next_sibling = id;
            }
            self.last_child[parent as usize] = id;
        }
        Ok((id, skipped || self.names.unvalidated.contains(&name)))
    }

    fn attributes(&mut self, mut attribute: *mut ffi::_xmlAttr) -> Result<Range<u32>, Rejection> {
        let start = index_u32(self.out.attributes.len())?;
        let mut count = 0_usize;
        while !attribute.is_null() {
            count += 1;
            if count > self.limits.max_attributes_per_element {
                return Err(limit(
                    "attributes per element",
                    self.limits.max_attributes_per_element,
                ));
            }
            // SAFETY: `attribute` is a node in the live attribute list of the current element.
            let current = unsafe { &*attribute };
            let name = self
                .interner
                .intern(namespace_of(current.ns)?, string(current.name)?);
            let value_start = self.offset()?;
            let mut value = current.children;
            while !value.is_null() {
                // SAFETY: `value` is a node in the live value list of `current`.
                let node = unsafe { &*value };
                if node.type_ != ffi::XML_TEXT_NODE {
                    return Err(Rejection::new(
                        Stage::Parse,
                        "attribute values must be character data",
                    ));
                }
                self.out.text.push_str(string(node.content)?);
                value = node.next;
            }
            let value_end = self.offset()?;
            if (value_end - value_start) as usize > self.limits.max_text_bytes {
                return Err(limit("text bytes", self.limits.max_text_bytes));
            }
            self.out.attributes.push(Attribute {
                name,
                value: value_start..value_end,
            });
            attribute = current.next;
        }
        Ok(start..index_u32(self.out.attributes.len())?)
    }

    fn index(&mut self, element: ElementId, attributes: &Range<u32>) -> Result<(), Rejection> {
        let Document {
            attributes: all,
            text,
            ids,
            references,
            ..
        } = &mut self.out;
        for attribute in &all[attributes.start as usize..attributes.end as usize] {
            if attribute.name == self.names.id {
                if ids.len() >= self.limits.max_ids {
                    return Err(limit("ids", self.limits.max_ids));
                }
                let value = text
                    .get(attribute.value.start as usize..attribute.value.end as usize)
                    .unwrap_or("");
                if ids.insert(value.into(), element).is_some() {
                    return Err(Rejection::new(
                        Stage::References,
                        "duplicate id attribute value",
                    ));
                }
            } else if attribute.name == self.names.href {
                if references.len() >= self.limits.max_references {
                    return Err(limit("references", self.limits.max_references));
                }
                references.push(element);
            }
        }
        Ok(())
    }

    /// Resolve an `xsi:type` QName against the namespaces in scope at `node`.
    fn xsi_type(
        &mut self,
        node: *mut ffi::xmlNode,
        attributes: &Range<u32>,
    ) -> Result<Option<NameId>, Rejection> {
        let Some(value) = self.out.attributes[attributes.start as usize..attributes.end as usize]
            .iter()
            .find(|attribute| attribute.name == self.names.xsi_type)
            .map(|attribute| self.out.slice(&attribute.value).trim().to_owned())
        else {
            return Ok(None);
        };
        let (prefix, local) = match value.split_once(':') {
            Some((prefix, local)) => (Some(prefix), local),
            None => (None, value.as_str()),
        };
        let prefix = prefix
            .map(CString::new)
            .transpose()
            .map_err(|_| Rejection::new(Stage::Parse, "invalid xsi:type prefix"))?;
        // SAFETY: the document and node are live; the prefix is NUL-terminated, or null to look
        // up the default namespace, as QName resolution requires.
        let namespace = unsafe {
            ffi::xmlSearchNs(
                self.document,
                node,
                prefix
                    .as_ref()
                    .map_or(ptr::null(), |prefix| prefix.as_ptr().cast()),
            )
        };
        if namespace.is_null() && prefix.is_some() {
            return Err(Rejection::new(
                Stage::Parse,
                "xsi:type uses an undeclared prefix",
            ));
        }
        Ok(Some(self.interner.intern(namespace_of(namespace)?, local)))
    }

    fn offset(&self) -> Result<u32, Rejection> {
        if self.out.text.len() > self.ceiling {
            return Err(Rejection::new(
                Stage::Prescan,
                "document text exceeds its bound",
            ));
        }
        index_u32(self.out.text.len())
    }
}

fn namespace_of<'d>(namespace: *mut ffi::xmlNs) -> Result<&'d str, Rejection> {
    if namespace.is_null() {
        return Ok("");
    }
    // SAFETY: a non-null namespace pointer reached from a live node of the live document.
    string(unsafe { (*namespace).href })
}

/// Borrow a libxml2 name or content string. libxml2 stores the document as UTF-8 and has
/// validated it; anything else is rejected rather than replaced.
fn string<'d>(value: *const u8) -> Result<&'d str, Rejection> {
    if value.is_null() {
        return Ok("");
    }
    // SAFETY: a non-null name or content pointer of a live node, NUL-terminated by libxml2 and
    // alive as long as the document.
    let bytes = unsafe { CStr::from_ptr(value.cast::<c_char>()) }.to_bytes();
    std::str::from_utf8(bytes)
        .map_err(|_| Rejection::new(Stage::Parse, "libxml2 produced non-UTF-8 text"))
}

fn index_u32(value: usize) -> Result<u32, Rejection> {
    u32::try_from(value).map_err(|_| Rejection::new(Stage::Prescan, "document exceeds 2^32 items"))
}

fn limit(what: &str, bound: usize) -> Rejection {
    Rejection::new(
        Stage::Prescan,
        format!("document exceeds {what} limit {bound}"),
    )
}
