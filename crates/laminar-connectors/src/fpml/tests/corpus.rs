//! Reference integrity and hand-checked economics over the official corpus.

use std::collections::{BTreeMap, BTreeSet};

use super::{corpus, corpus_root, decoder_for, example, expected_rejection, valid_examples};
use crate::fpml::document::Interner;
use crate::fpml::golden;
use crate::fpml::limits::Limits;
use crate::fpml::references::{ReferencePolicy, ReferenceRules};
use crate::fpml::schema_model::SchemaModel;
use crate::fpml::xsd::{TreeNames, Validator};
use crate::fpml::{Artifact, Profile, ProfileSource, BUNDLED_PROFILES};

const TRADE_PROFILE: &str = "fpml-5-13-confirmation-trade";

#[test]
fn reference_policies_over_the_official_corpus() {
    let artifact = Artifact::bundled().unwrap();
    let model = SchemaModel::for_artifact(&artifact).unwrap();
    let mut interner = Interner::default();
    let names = TreeNames::new(
        &mut interner,
        &artifact.reference_namespaces,
        &model.unvalidated_containers(),
    );
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
    let decoder = decoder_for("fpml-5-13-confirmation-rates");
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

/// `(document, row_locator)` to `product_type` for every row a profile yields over the corpus;
/// documents the profile is expected to reject contribute none.
fn trades(profile: &str) -> BTreeMap<(String, String), String> {
    let decoder = decoder_for(profile);
    let mut trades = BTreeMap::new();
    for path in valid_examples() {
        let batch = match (
            decoder.decode(&example(&path)),
            expected_rejection(profile, &path),
        ) {
            (Ok(batch), None) => batch,
            (Err(error), Some(reason)) => {
                assert!(
                    error.to_string().contains(reason),
                    "{profile} {path}: rejected for another reason: {error}"
                );
                continue;
            }
            (Ok(_), Some(reason)) => panic!("{profile} {path}: expected a rejection ({reason})"),
            (Err(error), None) => panic!("{profile} {path}: {error}"),
        };
        for row in golden::rows(&batch) {
            let locator = row["row_locator"].as_str().unwrap().to_owned();
            let product = row["product_type"].as_str().unwrap().to_owned();
            assert!(
                trades.insert((path.clone(), locator), product).is_none(),
                "{profile} {path}: duplicate row locator"
            );
        }
    }
    trades
}

/// Asset-class profiles are the ones built on the shared trade-level columns.
fn is_trade_class_profile(profile: &str) -> bool {
    let loaded = Profile::load(
        &ProfileSource::Bundled(profile.to_owned()),
        &Artifact::bundled().unwrap(),
    )
    .unwrap();
    loaded.schema().field_with_name("trade_header").is_ok()
}

/// Every schema-valid official example decodes through the trade profile: one row per
/// confirmation-view trade, none for documents without trades.
#[test]
fn trade_profile_rows_match_an_independent_count() {
    // Counted with Python ElementTree: trade elements that have a tradeHeader child.
    assert_eq!(trades(TRADE_PROFILE).len(), 366);
}

/// Each asset-class profile yields exactly the trades whose product types it covers, and no
/// trade is a row of two asset-class profiles.
#[test]
fn asset_class_profiles_partition_the_trades_they_cover() {
    let all = trades(TRADE_PROFILE);
    let mut claimed: BTreeSet<(String, String)> = BTreeSet::new();
    for (profile, _) in BUNDLED_PROFILES {
        if *profile == TRADE_PROFILE || !is_trade_class_profile(profile) {
            continue;
        }
        let rows = trades(profile);
        let products: BTreeSet<&String> = rows.values().collect();
        let expected: BTreeMap<_, _> = all
            .iter()
            .filter(|(key, product)| {
                products.contains(product) && expected_rejection(profile, &key.0).is_none()
            })
            .map(|(key, product)| (key.clone(), product.clone()))
            .collect();
        assert!(!rows.is_empty(), "{profile} yields no rows");
        assert_eq!(rows, expected, "{profile}");
        for key in rows.into_keys() {
            assert!(claimed.insert(key.clone()), "{profile} also claims {key:?}");
        }
    }
}
