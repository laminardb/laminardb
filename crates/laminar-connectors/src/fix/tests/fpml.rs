//! FpML documents carried in SecurityXML (1185) over a FIXT.1.1 session.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use super::{collect, column, execution_report, poll_error, schema, Harness};
use crate::config::encode_arrow_schema_ipc;
use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};
use crate::fix::FixSource;

const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation"
);
const SWAP: &str = "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml";
const CLEARING: &str =
    "business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml";
const FPML: &[(&str, &str)] = &[("fpml.field", "1185"), ("max.message.bytes", "65536")];

fn document(path: &str) -> String {
    std::fs::read_to_string(Path::new(CORPUS).join(path)).expect("corpus document")
}

fn send(harness: &Harness, exec_id: &str, security_xml: Option<&str>) {
    let mut message = execution_report(exec_id, Some(exec_id));
    if let Some(xml) = security_xml {
        message.fields.push((1184, xml.len().to_string()));
        message.fields.push((1185, xml.to_string()));
    }
    harness.venue().send(&message).expect("send");
}

/// Start as the runtime does: the start-time schema is the committed output, document column
/// included.
async fn start(harness: &Harness, extra: &[(&str, &str)]) -> FixSource {
    let mut source = FixSource::new(None);
    let mut config = harness.config(extra);
    let binding = source
        .resolve_schema(&config, Some(schema()))
        .await
        .expect("resolve");
    config.set(
        "_arrow_schema",
        encode_arrow_schema_ipc(&Arc::new(binding.logical.clone())),
    );
    config.set_schema_binding(binding).expect("binding");
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .expect("request"),
        )
        .await
        .expect("start");
    assert!(
        harness.venue().wait_logged_on(Duration::from_secs(20)),
        "client logon"
    );
    source
}

fn roots(batches: &[RecordBatch]) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let documents = batch.column_by_name("fpml").expect("fpml").as_struct();
            let roots = documents
                .column_by_name("document_root")
                .expect("document_root")
                .as_string::<i32>();
            (0..documents.len())
                .map(|row| {
                    documents
                        .is_valid(row)
                        .then(|| roots.value(row).to_string())
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn security_xml_documents_become_a_struct_column_and_replay_identically() {
    let harness = Harness::fixt("fpml", "FIX.5.0SP2");
    let mut source = start(&harness, FPML).await;
    let schema = source.schema();
    assert_eq!(schema.fields().len(), super::schema().fields().len() + 1);
    assert!(matches!(
        schema.field_with_name("fpml").expect("fpml").data_type(),
        DataType::Struct(_)
    ));
    send(&harness, "E1", Some(&document(SWAP)));
    send(&harness, "E2", None);
    send(&harness, "E3", Some(&document(CLEARING)));
    let (live, _) = collect(&mut source, 3).await;
    assert_eq!(column(&live, "exec_id"), vec!["E1", "E2", "E3"]);
    assert_eq!(
        roots(&live),
        vec![
            Some("dataDocument".into()),
            None,
            Some("clearingConfirmed".into())
        ]
    );
    source.close().await.expect("close");

    let mut source = start(&harness, FPML).await;
    let (replayed, _) = collect(&mut source, 3).await;
    let fpml = |batches: &[RecordBatch]| {
        let columns: Vec<_> = batches
            .iter()
            .map(|batch| Arc::clone(batch.column_by_name("fpml").expect("fpml")))
            .collect();
        let columns: Vec<&dyn Array> = columns.iter().map(AsRef::as_ref).collect();
        arrow_select::concat::concat(&columns).expect("concat")
    };
    assert_eq!(
        fpml(&live).to_data(),
        fpml(&replayed).to_data(),
        "the journal replays to the same documents"
    );
    source.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_document_stops_the_source() {
    let harness = Harness::fixt("fpml_reject", "FIX.5.0SP2");
    let mut source = start(&harness, FPML).await;
    send(&harness, "E1", Some("<notFpml/>"));
    let error = poll_error(&mut source).await;
    assert!(
        error.contains("FpML document in tag 1185 was rejected"),
        "{error}"
    );
    source.close().await.expect("close");
}

async fn resolve_error(harness: &Harness, extra: &[(&str, &str)], declared: Arc<Schema>) -> String {
    FixSource::new(None)
        .resolve_schema(&harness.config(extra), Some(declared))
        .await
        .expect_err("rejected")
        .to_string()
}

#[tokio::test]
async fn the_field_and_profile_are_checked_before_the_session_starts() {
    let harness = Harness::fixt("fpml_config", "FIX.5.0SP2");
    let cases: [(&[(&str, &str)], &str); 5] = [
        (
            &[("fpml.profile", "fpml-5-13-confirmation-document")],
            "applies only with 'fpml.field'",
        ),
        (&[("fpml.field", "14")], "needs a DATA or XMLDATA field"),
        (&[("fpml.field", "9999")], "not in the dictionary"),
        (&[("fpml.field", "x")], "must be a FIX tag number"),
        (
            &[
                ("fpml.field", "1185"),
                ("fpml.profile", "fpml-5-13-confirmation-trade"),
            ],
            "must select the document root",
        ),
    ];
    for (extra, expected) in cases {
        let error = resolve_error(&harness, extra, schema()).await;
        assert!(error.contains(expected), "{extra:?}: {error}");
    }
    let mut fields = schema().fields().to_vec();
    fields.push(Arc::new(Field::new("fpml", DataType::Utf8, true)));
    let error = resolve_error(
        &harness,
        &[("fpml.field", "1185")],
        Arc::new(Schema::new(fields)),
    )
    .await;
    assert!(error.contains("rename the declared column"), "{error}");
}

#[tokio::test]
async fn a_changed_document_field_is_refused_at_start() {
    let harness = Harness::fixt("fpml_changed", "FIX.5.0SP2");
    let mut source = FixSource::new(None);
    let mut config = harness.config(FPML);
    let binding = source
        .resolve_schema(&config, Some(schema()))
        .await
        .expect("resolve");
    config.set_schema_binding(binding).expect("binding");
    config.set("fpml.field", "355");
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .expect("request"),
        )
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("recreate the source"), "{error}");
}
