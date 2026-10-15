//! An operator-supplied closure that extends the bundled FpML schemas with a new product, and an
//! operator profile over it: both are configuration, with no parser or code change.

use std::path::PathBuf;

use super::fixtures_root;
use crate::fpml::{
    golden, Artifact, FpmlConfig, FpmlDecoder, Limits, ProfileSource, ReferencePolicy,
};

fn extension() -> PathBuf {
    fixtures_root().join("extension")
}

fn config(profile: ProfileSource) -> FpmlConfig {
    FpmlConfig {
        profile,
        schema_path: Some(extension().to_string_lossy().into_owned()),
        references: ReferencePolicy::Resolve,
        limits: Limits::default(),
    }
}

#[test]
fn an_extension_closure_and_operator_profile_decode_a_new_product() {
    let config = config(ProfileSource::File(extension().join("weather-trades.toml")));
    let (artifact, profile) = config.resolve().unwrap();
    assert_eq!(artifact.id, "example-fpml-5-13-weather-1");
    assert_ne!(
        artifact.fingerprint,
        Artifact::bundled().unwrap().fingerprint
    );
    let decoder = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(FpmlDecoder::start(
            artifact, &profile, &config, "test", None, None,
        ))
        .unwrap();
    let document = std::fs::read(extension().join("weather-swap.xml")).unwrap();
    let rows = golden::rows(&decoder.decode(&document).unwrap());
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["product_type"], "weatherIndexSwap");
    assert_eq!(row["trade_header"]["trade_date"], "2026-09-01");
    let swap = &row["weather_index_swap"];
    assert_eq!(swap["station"], "KJFK");
    assert_eq!(swap["payer"], "Example Utility");
    assert_eq!(swap["strike"], "1250.5000");
    assert_eq!(swap["notional"]["currency"], "USD");
    assert_eq!(swap["observation_end"], "2027-03-31");
}

#[test]
fn a_bundled_profile_stays_bound_to_the_bundled_closure() {
    let config = config(ProfileSource::Bundled(
        "fpml-5-13-confirmation-trade".into(),
    ));
    let error = config.resolve().unwrap_err().to_string();
    assert!(
        error.contains("is written for schema closure 'fpml-5-13-8-rec-2/confirmation'"),
        "{error}"
    );
}
