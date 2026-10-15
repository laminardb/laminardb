use super::{Profile, ProfileSource};
use crate::fpml::Artifact;

const HEADER: &str = r#"
[profile]
name = "test"
version = 1
artifact = "fpml-5-13-8-rec-2/confirmation"
roots = "documents"
rows = "trade[tradeHeader]"
"#;

fn compile(body: &str) -> Result<Profile, String> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("profile.toml");
    std::fs::write(&path, format!("{HEADER}{body}")).unwrap();
    Profile::load(&ProfileSource::File(path), &Artifact::bundled().unwrap())
        .map_err(|error| error.to_string())
}

#[test]
fn union_templates_match_per_use_and_reject_unmatched_fields() {
    let union = r#"
[structs.dated]
fields = [
  { name = "unadjusted_date", type = "date32", path = "unadjustedDate" },
  { name = "trade_date", type = "date32", path = "tradeDate" },
]

[[columns]]
name = "header"
type = "struct<dated>"
path = "tradeHeader"

[[columns]]
name = "termination"
type = "struct<dated>"
path = "swap/swapStream/calculationPeriodDates/terminationDate"
"#;
    compile(union).unwrap();

    let typo = union.replace("path = \"tradeDate\"", "path = \"tradeDat\"");
    let error = compile(&typo).unwrap_err();
    assert!(error.contains("'tradeDat'"), "{error}");
}

#[test]
fn a_template_use_matching_none_of_its_element_fields_is_rejected() {
    // AdjustableDate fields on an AdjustableOrRelativeDate: the element is present in documents,
    // but every field would be null.
    let misused = r#"
[structs.adjustable]
fields = [
  { name = "id", type = "utf8", path = "@id" },
  { name = "unadjusted_date", type = "date32", path = "unadjustedDate" },
]

[[columns]]
name = "effective"
type = "struct<adjustable>"
path = "swap/swapStream/calculationPeriodDates/effectiveDate"

[[columns]]
name = "settlement"
type = "struct<adjustable>"
path = "varianceSwap/additionalPayment/paymentDate"
"#;
    let error = compile(misused).unwrap_err();
    assert!(
        error.contains("struct template 'adjustable' matches none of its element fields"),
        "{error}"
    );
    assert!(error.contains("column 'settlement'"), "{error}");
    assert!(!error.contains("column 'effective'"), "{error}");
}

#[test]
fn set_predicates_keep_elements_with_a_child_in_the_set() {
    let admissible = r#"
[sets]
leg_or_fra = ["swapStream", "fra"]

[[columns]]
name = "swap_ids"
type = "list<utf8>"
path = "swap[{leg_or_fra}]/swapStream/@id"
"#;
    compile(admissible).unwrap();

    let inadmissible = admissible.replace(r#"["swapStream", "fra"]"#, r#"["partyId"]"#);
    let error = compile(&inadmissible).unwrap_err();
    assert!(
        error.contains("no member of set 'leg_or_fra' is a child of"),
        "{error}"
    );
}

#[test]
fn included_columns_precede_their_includer_and_load_once() {
    let directory = tempfile::tempdir().unwrap();
    let write = |name: &str, text: &str| std::fs::write(directory.path().join(name), text).unwrap();
    write(
        "dates.toml",
        r#"
[[columns]]
name = "trade_date"
type = "date32"
path = "tradeHeader/tradeDate"
"#,
    );
    write(
        "ids.toml",
        r#"
include = ["dates.toml"]

[[columns]]
name = "trade_id"
type = "utf8"
path = "@id"
"#,
    );
    let own = r#"
[[columns]]
name = "swap_streams"
type = "list<utf8>"
path = "swap/swapStream/@id"
"#;
    write(
        "profile.toml",
        &format!("include = [\"ids.toml\", \"dates.toml\"]\n{HEADER}{own}"),
    );
    let profile = Profile::load(
        &ProfileSource::File(directory.path().join("profile.toml")),
        &Artifact::bundled().unwrap(),
    )
    .unwrap();
    let names: Vec<_> = profile
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["trade_date", "trade_id", "swap_streams"]);
}

#[test]
fn self_tests_narrow_a_union_to_one_element() {
    let narrowed = r#"
[sets]
product = { substitution = "product" }

[structs.swap_only]
fields = [
  { name = "streams", type = "list<utf8>", path = "swapStream/@id" },
]

[[columns]]
name = "swap"
type = "struct<swap_only>"
path = "{product}/self::swap"
"#;
    compile(narrowed).unwrap();

    let error = compile(&narrowed.replace("self::swap", "self::fra")).unwrap_err();
    assert!(
        error.contains("struct template 'swap_only' matches none of its element fields in Fra"),
        "{error}"
    );
    let error = compile(&narrowed.replace("{product}/self::swap", "self::swap")).unwrap_err();
    assert!(error.contains("self::swap can never match here"), "{error}");
}
