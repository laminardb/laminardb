//! Streaming pre-scan that runs before libxml2 sees a document.
//!
//! It rejects what the strict profile never accepts (non-UTF-8 input, an encoding declaration
//! other than UTF-8, XML 1.1, DTDs, processing instructions, undeclared entity references,
//! unbound prefixes) and enforces structural limits without building a tree. It decides
//! nothing about content: the published values come from the validated libxml2 tree, and the
//! element count recorded here must match that tree.

use quick_xml::events::{BytesDecl, BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use super::error::{Rejection, Stage};
use super::limits::Limits;

const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];
const PREDEFINED_ENTITIES: &[&[u8]] = &[b"lt", b"gt", b"amp", b"apos", b"quot"];

/// What the pre-scan observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prescan {
    pub(crate) elements: usize,
    pub(crate) root_namespace: String,
    pub(crate) root_local: String,
}

fn reject(detail: impl Into<String>) -> Rejection {
    Rejection::new(Stage::Prescan, detail)
}

struct Scan<'l> {
    limits: &'l Limits,
    depth: usize,
    elements: usize,
    namespace_declarations: usize,
    root: Option<(String, String)>,
    closed_root: bool,
}

/// Scan one complete document.
pub(crate) fn prescan(bytes: &[u8], limits: &Limits) -> Result<Prescan, Rejection> {
    if bytes.len() > limits.max_document_bytes {
        return Err(reject(format!(
            "document has {} bytes; the limit is {}",
            bytes.len(),
            limits.max_document_bytes
        )));
    }
    let body = check_encoding(bytes)?;
    let mut reader = NsReader::from_str(body);
    let config = reader.config_mut();
    config.check_end_names = true;
    config.check_comments = true;
    config.allow_unmatched_ends = false;
    config.allow_dangling_amp = false;
    config.expand_empty_elements = false;

    let mut scan = Scan {
        limits,
        depth: 0,
        elements: 0,
        namespace_declarations: 0,
        root: None,
        closed_root: false,
    };
    let mut first = true;
    loop {
        let event = match reader.read_event() {
            Ok(event) => event,
            Err(error) => {
                return Err(reject(format!(
                    "malformed XML near byte {}: {error}",
                    reader.error_position()
                )))
            }
        };
        let at_start = std::mem::replace(&mut first, false);
        match event {
            Event::Decl(declaration) if at_start => check_declaration(&declaration)?,
            Event::Decl(_) => {
                return Err(reject(
                    "XML declaration is not at the start of the document",
                ))
            }
            Event::DocType(_) => return Err(reject("DTDs are not accepted")),
            Event::PI(_) => return Err(reject("processing instructions are not accepted")),
            Event::GeneralRef(reference) => {
                if reference.is_char_ref() {
                    if !matches!(reference.resolve_char_ref(), Ok(Some(_))) {
                        return Err(reject("invalid character reference"));
                    }
                } else if !PREDEFINED_ENTITIES.contains(&reference.as_ref()) {
                    return Err(reject("undeclared entity reference"));
                }
            }
            Event::Start(start) => {
                scan.open(&start, &reader.resolver().resolve_element(start.name()).0)?;
                scan.depth += 1;
            }
            Event::Empty(start) => {
                scan.open(&start, &reader.resolver().resolve_element(start.name()).0)?;
                scan.closed_root |= scan.depth == 0;
            }
            Event::End(_) => {
                scan.depth = scan.depth.saturating_sub(1);
                scan.closed_root |= scan.depth == 0;
            }
            Event::Text(text) | Event::Comment(text) => scan.text(text.len())?,
            Event::CData(text) => scan.text(text.len())?,
            Event::Eof => break,
        }
    }
    let Some((root_namespace, root_local)) = scan.root else {
        return Err(reject("document has no root element"));
    };
    if scan.depth != 0 || !scan.closed_root {
        return Err(reject("document is truncated"));
    }
    Ok(Prescan {
        elements: scan.elements,
        root_namespace,
        root_local,
    })
}

impl Scan<'_> {
    fn open(
        &mut self,
        start: &BytesStart<'_>,
        namespace: &ResolveResult<'_>,
    ) -> Result<(), Rejection> {
        if self.closed_root {
            return Err(reject("document has more than one root element"));
        }
        self.elements += 1;
        if self.elements > self.limits.max_elements {
            return Err(reject(format!(
                "document exceeds elements limit {}",
                self.limits.max_elements
            )));
        }
        if self.depth + 1 > self.limits.max_depth {
            return Err(reject(format!(
                "document exceeds depth limit {}",
                self.limits.max_depth
            )));
        }
        let namespace = match namespace {
            ResolveResult::Bound(namespace) => utf8(namespace.as_ref())?,
            ResolveResult::Unbound => "",
            ResolveResult::Unknown(_) => return Err(reject("element uses an undeclared prefix")),
        };
        if self.root.is_none() {
            self.root = Some((
                namespace.to_owned(),
                utf8(start.local_name().as_ref())?.to_owned(),
            ));
        }
        let mut attributes = 0_usize;
        for attribute in start.attributes().with_checks(true) {
            let attribute =
                attribute.map_err(|error| reject(format!("malformed attribute: {error}")))?;
            attributes += 1;
            if attributes > self.limits.max_attributes_per_element {
                return Err(reject(format!(
                    "element exceeds attributes limit {}",
                    self.limits.max_attributes_per_element
                )));
            }
            if attribute.value.len() > self.limits.max_text_bytes {
                return Err(reject(format!(
                    "attribute value exceeds text limit {}",
                    self.limits.max_text_bytes
                )));
            }
            let key = attribute.key.as_ref();
            if key == b"xmlns" || key.starts_with(b"xmlns:") {
                self.namespace_declarations += 1;
                if self.namespace_declarations > self.limits.max_namespace_declarations {
                    return Err(reject(format!(
                        "document exceeds namespace declarations limit {}",
                        self.limits.max_namespace_declarations
                    )));
                }
            }
        }
        Ok(())
    }

    fn text(&self, length: usize) -> Result<(), Rejection> {
        if length > self.limits.max_text_bytes {
            return Err(reject(format!(
                "text exceeds limit {}",
                self.limits.max_text_bytes
            )));
        }
        Ok(())
    }
}

/// Accept only UTF-8 (with or without a BOM). Return the text after any BOM.
fn check_encoding(bytes: &[u8]) -> Result<&str, Rejection> {
    if bytes.starts_with(&[0x00, 0x00, 0xFE, 0xFF])
        || bytes.starts_with(&[0xFF, 0xFE, 0x00, 0x00])
        || bytes.starts_with(&[0xFE, 0xFF])
        || bytes.starts_with(&[0xFF, 0xFE])
        || bytes.starts_with(&[0x00, b'<'])
        || bytes.starts_with(&[b'<', 0x00])
    {
        return Err(reject(
            "only UTF-8 documents are accepted (UTF-16/32 detected)",
        ));
    }
    let body = bytes.strip_prefix(UTF8_BOM).unwrap_or(bytes);
    std::str::from_utf8(body).map_err(|error| {
        reject(format!(
            "document is not valid UTF-8 at byte {}",
            error.valid_up_to() + (bytes.len() - body.len())
        ))
    })
}

/// The declaration must say XML 1.0 and, when it names an encoding, UTF-8.
fn check_declaration(declaration: &BytesDecl<'_>) -> Result<(), Rejection> {
    let version = declaration
        .version()
        .map_err(|_| reject("XML declaration has no version"))?;
    if version.as_ref() != b"1.0" {
        return Err(reject("only XML 1.0 documents are accepted"));
    }
    if let Some(encoding) = declaration.encoding() {
        let encoding = encoding.map_err(|_| reject("malformed encoding declaration"))?;
        if !encoding.as_ref().eq_ignore_ascii_case(b"UTF-8") {
            return Err(reject(
                "the declared encoding must be UTF-8 (the bytes are UTF-8)",
            ));
        }
    }
    Ok(())
}

fn utf8(bytes: &[u8]) -> Result<&str, Rejection> {
    std::str::from_utf8(bytes).map_err(|_| reject("name is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(document: &str) -> Result<Prescan, Rejection> {
        prescan(document.as_bytes(), &Limits::default())
    }

    #[test]
    fn accepts_a_namespaced_utf8_document() {
        let found = scan(
            "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?><!-- c --><p:r xmlns:p=\"urn:x\"><p:a>&amp;&#x20AC;</p:a><b xmlns=\"urn:y\"/></p:r>",
        )
        .unwrap();
        assert_eq!(found.elements, 3);
        assert_eq!(found.root_namespace, "urn:x");
        assert_eq!(found.root_local, "r");
    }

    #[test]
    fn rejects_documents_outside_the_strict_profile() {
        for (document, expected) in [
            ("<!DOCTYPE r><r/>", "DTDs"),
            ("<r><?php evil?></r>", "processing instructions"),
            ("<r>&ext;</r>", "undeclared entity"),
            ("<r>&#0;</r>", "character reference"),
            ("<?xml version=\"1.1\"?><r/>", "XML 1.0"),
            (
                "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><r/>",
                "UTF-8",
            ),
            ("<p:r/>", "undeclared prefix"),
            ("<r><a></r>", "malformed"),
            ("<r><a>", "truncated"),
            ("<r/><r/>", "more than one root"),
            ("<r a=\"1\" a=\"2\"/>", "malformed attribute"),
            ("", "no root"),
        ] {
            let error = scan(document).unwrap_err();
            assert_eq!(error.stage, Stage::Prescan);
            assert!(error.detail.contains(expected), "{document}: {error}");
        }
    }

    #[test]
    fn rejects_non_utf8_encodings_without_lossy_decoding() {
        let utf16: Vec<u8> = "\u{feff}<r/>"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        assert!(prescan(&utf16, &Limits::default())
            .unwrap_err()
            .detail
            .contains("UTF-16"));
        let latin1 = b"<r>caf\xe9</r>";
        assert!(prescan(latin1, &Limits::default())
            .unwrap_err()
            .detail
            .contains("not valid UTF-8 at byte 6"));
    }

    #[test]
    fn enforces_structural_limits_before_parsing() {
        let limits = Limits {
            max_depth: 3,
            max_attributes_per_element: 2,
            max_text_bytes: 4,
            max_namespace_declarations: 1,
            max_elements: 4,
            max_document_bytes: 64,
            ..Limits::default()
        };
        for (document, expected) in [
            ("<a><b><c><d/></c></b></a>", "depth limit 3"),
            ("<a x=\"1\" y=\"2\" z=\"3\"/>", "attributes limit 2"),
            ("<a>12345</a>", "text exceeds limit 4"),
            ("<a x=\"12345\"/>", "attribute value exceeds"),
            (
                "<a xmlns=\"u\"><b xmlns=\"v\"/></a>",
                "namespace declarations limit 1",
            ),
            ("<a><b/><b/><b/><b/></a>", "elements limit 4"),
            (&format!("<a>{}</a>", " ".repeat(64)), "limit is 64"),
        ] {
            let error = prescan(document.as_bytes(), &limits).unwrap_err();
            assert!(error.detail.contains(expected), "{document}: {error}");
        }
    }
}
