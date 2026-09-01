use super::*;

#[test]
fn test_write_mode_default() {
    assert!(matches!(WriteMode::default(), WriteMode::Insert));
}

#[test]
fn test_validate_timeseries_insert_ok() {
    validate_timeseries_write_mode(&WriteMode::Insert).unwrap();
}

#[test]
fn test_validate_timeseries_upsert_fails() {
    let mode = WriteMode::Upsert {
        key_fields: vec!["id".to_string()],
    };
    let err = validate_timeseries_write_mode(&mode).unwrap_err();
    assert!(err.to_string().contains("time series"));
}

#[test]
fn test_validate_timeseries_cdc_replay_fails() {
    let err = validate_timeseries_write_mode(&WriteMode::CdcReplay).unwrap_err();
    assert!(err.to_string().contains("time series"));
}
