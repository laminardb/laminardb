//! Whole-pipeline tests over the official corpus.

use super::document::Interner;
use super::limits::Limits;
use super::references::{ReferencePolicy, ReferenceRules};
use super::schema_model::SchemaModel;
use super::xsd::{TreeNames, Validator};
use super::Artifact;

#[derive(serde::Deserialize)]
struct Corpus {
    example: Vec<CorpusEntry>,
}

#[derive(serde::Deserialize)]
struct CorpusEntry {
    path: String,
    status: String,
    errata: Option<String>,
}

const NEWLINE: &str = "\n";

fn corpus_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fpml/fpml-5-13-8-rec-2")
}

#[test]
fn reference_policies_over_the_official_corpus() {
    let artifact = Artifact::bundled().unwrap();
    let model = SchemaModel::for_artifact(&artifact).unwrap();
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &artifact.reference_namespaces);
    let rules = ReferenceRules::new(&model, &mut interner);
    let mut validator = Validator::compile(&artifact.files).unwrap();
    let corpus: Corpus =
        toml::from_str(&std::fs::read_to_string(corpus_root().join("corpus.toml")).unwrap())
            .unwrap();
    let expected: Vec<String> =
        std::fs::read_to_string(corpus_root().join("reference-annotation-violations.txt"))
            .unwrap()
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .map(str::to_owned)
            .collect();
    let mut flagged = Vec::new();
    let mut errata = Vec::new();
    for entry in corpus
        .example
        .iter()
        .filter(|entry| entry.status == "xsd-valid")
    {
        let bytes = std::fs::read(corpus_root().join("confirmation").join(&entry.path)).unwrap();
        let (document, _) = validator
            .validate(&bytes, &Limits::default(), &mut interner, &names)
            .unwrap();
        rules
            .check(&document, &interner, names.href, ReferencePolicy::Resolve)
            .unwrap_or_else(|error| panic!("{}: {error}", entry.path));
        if rules
            .check(&document, &interner, names.href, ReferencePolicy::Schema)
            .is_err()
        {
            flagged.push(entry.path.clone());
            if matches!(entry.errata.as_deref(), Some("ref-8" | "ref-11")) {
                errata.push(entry.path.clone());
            }
        }
    }
    assert_eq!(flagged, expected);
    assert_eq!(
        errata.len(),
        3,
        "every published reference erratum is detected"
    );
}

fn decoder_for(profile: &str) -> super::FpmlDecoder {
    let config = super::FpmlConfig {
        profile: super::ProfileSource::Bundled(profile.into()),
        schema_path: None,
        references: ReferencePolicy::Resolve,
        limits: Limits::default(),
    };
    let (artifact, profile) = config.resolve().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(super::FpmlDecoder::start(
            artifact, &profile, &config, "test", None, None,
        ))
        .unwrap()
}

fn example(path: &str) -> Vec<u8> {
    std::fs::read(corpus_root().join("confirmation").join(path)).unwrap()
}

#[test]
fn vanilla_swap_projects_identifiers_parties_and_legs() {
    let decoder = decoder_for("fpml-5-13-confirmation-trade");
    let batch = decoder
        .decode(&example(
            "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
        ))
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let json = super::golden::rows(&batch);
    let row = &json[0];
    assert_eq!(row["document_root"], "dataDocument");
    assert_eq!(row["product_type"], "swap");
    assert_eq!(row["row_locator"], "/dataDocument/trade");
    assert_eq!(row["trade_context"], "dataDocument");
    let header = &row["trade_header"];
    assert_eq!(header["trade_date"], "1994-12-12");
    let identifiers = header["party_trade_identifiers"].as_array().unwrap();
    assert_eq!(identifiers[0]["trade_ids"][0]["value"], "TW9235");
    assert_eq!(
        identifiers[0]["trade_ids"][0]["scheme"],
        "http://www.partyA.com/swaps/trade-id"
    );
    assert_eq!(
        identifiers[0]["party_ids"][0]["value"],
        "549300VBWWV6BYQOWM67"
    );
    let streams = row["swap"]["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 2);
    assert_eq!(streams[0]["payer_party_href"], "party1");
    let floating = &streams[0]["calculation"];
    assert_eq!(
        floating["notional_schedule"]["initial_value"],
        "50000000.000000000000000000"
    );
    assert_eq!(floating["notional_schedule"]["currency"], "EUR");
    assert_eq!(
        floating["floating_rate_calculation"]["floating_rate_index"],
        "EUR-LIBOR-BBA"
    );
    assert_eq!(floating["rate_calculation_type"], "floatingRateCalculation");
    assert_eq!(floating["day_count_fraction"], "ACT/360");
    let fixed = &streams[1]["calculation"];
    assert_eq!(
        fixed["fixed_rate_schedule"]["initial_value"],
        "0.060000000000000000"
    );
    assert!(fixed.get("floating_rate_calculation").is_none());
    let termination = &streams[0]["calculation_period_dates"]["termination_date"];
    assert_eq!(termination["unadjusted_date"], "1999-12-14");
    assert_eq!(
        termination["date_adjustments"]["business_centers"][0],
        "DEFR"
    );
    // The fixed leg's termination date names its business centres by reference.
    let fixed_termination = &streams[1]["calculation_period_dates"]["termination_date"];
    assert_eq!(
        fixed_termination["date_adjustments"]["business_centers"][0],
        "DEFR"
    );
    assert_eq!(
        fixed_termination["date_adjustments"]["business_centers_href"],
        "primaryBusinessCenters"
    );
    assert_eq!(row["parties"].as_array().unwrap().len(), 2);
    assert_eq!(row["parties"][0]["party_name"], "Party A");
}

/// Every schema-valid official example decodes through the trade profile, producing one row per
/// confirmation-view trade and none for documents without trades.
#[test]
fn trade_profile_decodes_the_whole_official_corpus() {
    let decoder = decoder_for("fpml-5-13-confirmation-trade");
    let corpus: Corpus =
        toml::from_str(&std::fs::read_to_string(corpus_root().join("corpus.toml")).unwrap())
            .unwrap();
    let mut rows = 0;
    let mut failures = Vec::new();
    for entry in corpus
        .example
        .iter()
        .filter(|entry| entry.status == "xsd-valid")
    {
        match decoder.decode(&example(&entry.path)) {
            Ok(batch) => rows += batch.num_rows(),
            Err(error) => failures.push(format!("{}: {error}", entry.path)),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join(NEWLINE));
    // Counted independently with Python ElementTree: trade elements that have a tradeHeader.
    assert_eq!(rows, 366);
}
