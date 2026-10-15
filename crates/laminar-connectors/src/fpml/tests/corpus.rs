//! Reference integrity and hand-checked economics over the official corpus.

use super::{corpus, corpus_root, decoder_for, example, valid_examples};
use crate::fpml::document::Interner;
use crate::fpml::golden;
use crate::fpml::limits::Limits;
use crate::fpml::references::{ReferencePolicy, ReferenceRules};
use crate::fpml::schema_model::SchemaModel;
use crate::fpml::xsd::{TreeNames, Validator};
use crate::fpml::Artifact;

#[test]
fn reference_policies_over_the_official_corpus() {
    let artifact = Artifact::bundled().unwrap();
    let model = SchemaModel::for_artifact(&artifact).unwrap();
    let mut interner = Interner::default();
    let names = TreeNames::new(&mut interner, &artifact.reference_namespaces);
    let rules = ReferenceRules::new(&model, &mut interner);
    let mut validator = Validator::compile(&artifact.files).unwrap();
    let expected: Vec<String> =
        std::fs::read_to_string(corpus_root().join("reference-annotation-violations.txt"))
            .unwrap()
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .map(str::to_owned)
            .collect();
    let mut flagged = Vec::new();
    let mut errata = Vec::new();
    for entry in corpus()
        .example
        .iter()
        .filter(|entry| entry.status == "xsd-valid")
    {
        let (document, _) = validator
            .validate(
                &example(&entry.path),
                &Limits::default(),
                &mut interner,
                &names,
            )
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

/// Values read by hand from the example, not from a golden.
#[test]
fn vanilla_swap_projects_identifiers_parties_and_legs() {
    let decoder = decoder_for("fpml-5-13-confirmation-trade");
    let batch = decoder
        .decode(&example(
            "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
        ))
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let json = golden::rows(&batch);
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

/// Every schema-valid official example decodes through the trade profile: one row per
/// confirmation-view trade, none for documents without trades.
#[test]
fn trade_profile_rows_match_an_independent_count() {
    let decoder = decoder_for("fpml-5-13-confirmation-trade");
    let rows: usize = valid_examples()
        .iter()
        .map(|path| {
            decoder
                .decode(&example(path))
                .unwrap_or_else(|error| panic!("{path}: {error}"))
                .num_rows()
        })
        .sum();
    // Counted with Python ElementTree: trade elements that have a tradeHeader child.
    assert_eq!(rows, 366);
}
