use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;

const NS: &str = "urn:laminardb:test";

fn files(entries: &[(&str, &str)]) -> SchemaFiles {
    SchemaFiles {
        entry: entries[0].0.to_owned(),
        files: entries
            .iter()
            .map(|(name, body)| ((*name).to_owned(), Arc::<[u8]>::from(body.as_bytes())))
            .collect::<BTreeMap<_, _>>(),
    }
}

fn schema(body: &str) -> String {
    format!(
        r#"<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns="{NS}" targetNamespace="{NS}" elementFormDefault="qualified">{body}</xsd:schema>"#
    )
}

fn validate(validator: &mut Validator, document: &str) -> Result<(Document, Refusals), Rejection> {
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &[NS.to_owned()]);
    validator.validate(document.as_bytes(), &Limits::default(), &mut interner, &names)
}

const SIMPLE: &str = r#"<xsd:element name="r" type="xsd:string"/>"#;

#[test]
fn compiles_only_from_the_closure_and_reports_escapes() {
    let directory = tempfile::tempdir().unwrap();
    let outside = directory.path().join("outside.xsd");
    std::fs::write(&outside, schema(SIMPLE)).unwrap();
    let outside_url = url::Url::from_file_path(&outside).unwrap().to_string();
    for location in [
        "missing.xsd".to_owned(),
        "../outside.xsd".to_owned(),
        outside_url,
        "http://127.0.0.1:9/evil.xsd".to_owned(),
    ] {
        let entry = schema(&format!(
            r#"<xsd:include schemaLocation="{location}"/>{SIMPLE}"#
        ));
        let Err(error) = Validator::compile(&files(&[("main.xsd", &entry)])) else {
            panic!("schema include of {location} must not compile");
        };
        assert!(
            matches!(&error, CompileError::Escape(urls) if !urls.is_empty()),
            "{location}: {error}"
        );
    }
    // A native path is either refused by the loader or not a URI at all; neither compiles.
    let entry = schema(&format!(
        r#"<xsd:include schemaLocation="{}"/>{SIMPLE}"#,
        outside.display()
    ));
    assert!(Validator::compile(&files(&[("main.xsd", &entry)])).is_err());
}

/// Guards the vendored libxml2 patch: nested includes are parsed by a temporary context, which
/// upstream creates without the caller's resource loader.
#[test]
fn nested_includes_use_the_closure_loader() {
    let main = schema(r#"<xsd:include schemaLocation="a.xsd"/>"#);
    let middle = schema(r#"<xsd:include schemaLocation="b.xsd"/>"#);
    let leaf = schema(SIMPLE);
    assert!(Validator::compile(&files(&[
        ("main.xsd", &main),
        ("a.xsd", &middle),
        ("b.xsd", &leaf)
    ]))
    .is_ok());
    let Err(error) = Validator::compile(&files(&[("main.xsd", &main), ("a.xsd", &middle)])) else {
        panic!("a nested include outside the closure must not compile");
    };
    assert!(
        matches!(&error, CompileError::Escape(urls) if urls == &["laminardb-schema:/b.xsd"]),
        "{error}"
    );
}

#[test]
fn includes_resolve_inside_the_closure() {
    let main = schema(r#"<xsd:include schemaLocation="types/part.xsd"/>"#);
    let part = schema(SIMPLE);
    let mut validator =
        Validator::compile(&files(&[("main.xsd", &main), ("types/part.xsd", &part)])).unwrap();
    let (document, refusals) =
        validate(&mut validator, &format!(r#"<r xmlns="{NS}">ok</r>"#)).unwrap();
    assert_eq!(document.elements.len(), 1);
    assert_eq!(refusals.attempts, 0);
}

#[test]
fn instance_dtds_and_external_entities_are_rejected_without_loading() {
    let directory = tempfile::tempdir().unwrap();
    let secret = directory.path().join("secret.txt");
    std::fs::write(&secret, "TOP-SECRET").unwrap();
    let secret_url = url::Url::from_file_path(&secret).unwrap();
    let mut validator = Validator::compile(&files(&[("main.xsd", &schema(SIMPLE))])).unwrap();
    for document in [
        format!(r#"<!DOCTYPE r [<!ENTITY x SYSTEM "{secret_url}">]><r xmlns="{NS}">&x;</r>"#),
        format!(r#"<!DOCTYPE r SYSTEM "{secret_url}"><r xmlns="{NS}">a</r>"#),
        format!(
            r#"<!DOCTYPE r [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;"><!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">]><r xmlns="{NS}">&c;</r>"#
        ),
        format!(r#"<!DOCTYPE r [<!ENTITY % p SYSTEM "{secret_url}"> %p;]><r xmlns="{NS}">a</r>"#),
    ] {
        let error = validate(&mut validator, &document).unwrap_err();
        assert!(
            matches!(error.stage, Stage::Parse),
            "{document}: {error}"
        );
        assert!(!error.detail.contains("TOP-SECRET"), "{error}");
    }
}

#[test]
fn schema_location_hints_are_never_followed() {
    let directory = tempfile::tempdir().unwrap();
    let evil = directory.path().join("evil.xsd");
    // Would reject the document (an int root) if the hint were followed.
    std::fs::write(&evil, schema(r#"<xsd:element name="r" type="xsd:int"/>"#)).unwrap();
    let evil_url = url::Url::from_file_path(&evil).unwrap();
    let mut validator = Validator::compile(&files(&[("main.xsd", &schema(SIMPLE))])).unwrap();
    let (document, refusals) = validate(
        &mut validator,
        &format!(
            r#"<r xmlns="{NS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="{NS} {evil_url}">text</r>"#
        ),
    )
    .unwrap();
    assert_eq!(document.text_of(document.root().unwrap()), "text");
    assert_eq!(refusals.attempts, 0);

    let other = validate(
        &mut validator,
        &format!(
            r#"<r xmlns="{NS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:other {evil_url}"><o:x xmlns:o="urn:other"/></r>"#
        ),
    )
    .unwrap_err();
    assert_eq!(other.stage, Stage::Schema, "{other}");
}

#[test]
fn schema_errors_are_redacted_and_bounded() {
    let mut validator = Validator::compile(&files(&[(
        "main.xsd",
        &schema(r#"<xsd:element name="r" type="xsd:decimal"/>"#),
    )]))
    .unwrap();
    let error = validate(
        &mut validator,
        &format!(r#"<r xmlns="{NS}">ACCOUNT-1234-SECRET</r>"#),
    )
    .unwrap_err();
    assert_eq!(error.stage, Stage::Schema);
    assert!(!error.detail.contains("ACCOUNT-1234-SECRET"), "{error}");
    assert!(error.detail.contains("line 1"), "{error}");
}

#[test]
fn validated_tree_carries_schema_defaults_and_resolved_xsi_types() {
    let body = r#"
      <xsd:complexType name="Base"><xsd:simpleContent><xsd:extension base="xsd:string">
        <xsd:attribute name="scheme" type="xsd:string" default="urn:default-scheme"/>
      </xsd:extension></xsd:simpleContent></xsd:complexType>
      <xsd:complexType name="Derived"><xsd:simpleContent><xsd:extension base="Base"/>
      </xsd:simpleContent></xsd:complexType>
      <xsd:element name="r"><xsd:complexType><xsd:sequence>
        <xsd:element name="v" type="Base" maxOccurs="unbounded"/>
      </xsd:sequence><xsd:attribute name="id" type="xsd:ID"/></xsd:complexType></xsd:element>"#;
    let mut validator = Validator::compile(&files(&[("main.xsd", &schema(body))])).unwrap();
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &[NS.to_owned()]);
    let scheme = interner.intern("", "scheme");
    let derived = interner.intern(NS, "Derived");
    let document = format!(
        r#"<p:r xmlns:p="{NS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" id="root">
             <p:v>plain</p:v>
             <q:v xmlns:q="{NS}" scheme="urn:explicit" xsi:type="q:Derived">derived</q:v>
             <v xmlns="{NS}" xmlns:p="urn:rebound" xsi:type="Derived">default-ns</v>
           </p:r>"#
    );
    let (tree, _) = validator
        .validate(document.as_bytes(), &Limits::default(), &mut interner, &names)
        .unwrap();
    let values: Vec<_> = tree.children(0).collect();
    assert_eq!(values.len(), 3);
    let first = tree.element(values[0]).unwrap();
    assert_eq!(tree.attribute(first, scheme), Some("urn:default-scheme"));
    assert_eq!(first.xsi_type, None);
    let second = tree.element(values[1]).unwrap();
    assert_eq!(tree.attribute(second, scheme), Some("urn:explicit"));
    assert_eq!(second.xsi_type, Some(derived));
    assert_eq!(tree.element(values[2]).unwrap().xsi_type, Some(derived));
    assert_eq!(tree.text_of(tree.element(values[2]).unwrap()), "default-ns");
    assert_eq!(tree.dereference("root"), Some(0));
    assert_eq!(tree.locator(&interner, values[1]), "/r/v[2]");
}

#[test]
fn duplicate_ids_are_rejected() {
    let body = r#"<xsd:element name="r"><xsd:complexType><xsd:sequence>
        <xsd:element name="v" maxOccurs="unbounded"><xsd:complexType>
          <xsd:attribute name="id" type="xsd:ID"/></xsd:complexType></xsd:element>
      </xsd:sequence></xsd:complexType></xsd:element>"#;
    let mut validator = Validator::compile(&files(&[("main.xsd", &schema(body))])).unwrap();
    let error = validate(
        &mut validator,
        &format!(r#"<r xmlns="{NS}"><v id="a"/><v id="a"/></r>"#),
    )
    .unwrap_err();
    assert!(
        matches!(error.stage, Stage::Schema | Stage::References),
        "{error}"
    );
}

#[test]
fn element_and_attribute_limits_are_enforced() {
    let body = r#"<xsd:element name="r"><xsd:complexType><xsd:sequence>
        <xsd:element name="v" maxOccurs="unbounded" type="xsd:string"/>
      </xsd:sequence></xsd:complexType></xsd:element>"#;
    let mut validator = Validator::compile(&files(&[("main.xsd", &schema(body))])).unwrap();
    let limits = Limits {
        max_elements: 3,
        ..Limits::default()
    };
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &[NS.to_owned()]);
    let document = format!(r#"<r xmlns="{NS}"><v/><v/><v/></r>"#);
    let error = validator
        .validate(document.as_bytes(), &limits, &mut interner, &names)
        .unwrap_err();
    assert!(error.detail.contains("elements limit 3"), "{error}");
}

#[test]
fn redaction_keeps_component_names_and_drops_values() {
    let message = "Element '{urn:x}tradeId', attribute 'tradeIdScheme': 'http://http://secret' is not a valid value of the atomic type '{urn:x}NonEmptyURI'.\n";
    assert_eq!(
        redact(message),
        "Element '{urn:x}tradeId', attribute 'tradeIdScheme': '…' is not a valid value of the atomic type '{urn:x}NonEmptyURI'."
    );
    assert_eq!(redact("value 'PARTY-SECRET' and 'unterminated"), "value '…' and '…");
}

#[derive(serde::Deserialize)]
struct Corpus {
    example: Vec<CorpusEntry>,
}

#[derive(serde::Deserialize)]
struct CorpusEntry {
    path: String,
    status: String,
}

#[test]
fn official_examples_validate_against_the_bundled_closure() {
    let artifact = crate::fpml::Artifact::bundled().unwrap();
    let mut validator = Validator::compile(&artifact.files).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fpml/fpml-5-13-8-rec-2");
    let corpus: Corpus =
        toml::from_str(&std::fs::read_to_string(root.join("corpus.toml")).unwrap()).unwrap();
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &artifact.reference_namespaces);
    let (mut valid, mut invalid, mut unavailable) = (0, 0, 0);
    for entry in &corpus.example {
        if entry.status == "unavailable" {
            unavailable += 1;
            continue;
        }
        let bytes = std::fs::read(root.join("confirmation").join(&entry.path)).unwrap();
        let verdict = validator.validate(&bytes, &Limits::default(), &mut interner, &names);
        match entry.status.as_str() {
            "xsd-valid" => {
                let (_, refusals) = verdict.unwrap_or_else(|error| panic!("{}: {error}", entry.path));
                assert_eq!(refusals.attempts, 0, "{}", entry.path);
                valid += 1;
            }
            "xsd-invalid" => {
                let error = verdict.expect_err(&entry.path);
                assert_eq!(error.stage, Stage::Schema, "{}: {error}", entry.path);
                invalid += 1;
            }
            other => panic!("unknown corpus status {other}"),
        }
    }
    assert_eq!((valid, invalid, unavailable), (535, 1, 22));
}
