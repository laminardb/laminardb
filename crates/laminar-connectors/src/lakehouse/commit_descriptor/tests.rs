use super::*;

fn binding() -> DeltaTableBinding {
    DeltaTableBinding {
        table_id: "018f0000-0000-7000-8000-000000000001".into(),
        write_metadata_sha256: "11".repeat(32),
    }
}

#[test]
fn roundtrip_empty_descriptor_and_reject_non_current_versions() {
    let bytes = encode(&binding(), &[]).unwrap();
    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded.binding, binding());
    assert!(decoded.adds.is_empty());

    let obsolete =
        br#"{"version":1,"binding":{"table_id":"t","write_metadata_sha256":"00"},"adds":[]}"#;
    let error = decode(obsolete).unwrap_err().to_string();
    assert!(error.contains("version 1"), "got: {error}");

    let future =
        br#"{"version":999,"binding":{"table_id":"t","write_metadata_sha256":"00"},"adds":[]}"#;
    let error = decode(future).unwrap_err().to_string();
    assert!(error.contains("version 999"), "got: {error}");
}
