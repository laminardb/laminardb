//! FpML documents carried in a FIX data field, such as SecurityXML (1185).
//!
//! Each message's document is validated and projected by the FpML decoder into one nullable
//! struct column after the declared columns, repeated on every row the message projects. A
//! message without the field has a null document; a rejected document stops the source at that
//! journal record. The field, closure, and profile identity are part of the schema contract, so
//! replaying the journal projects the same rows.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{new_null_array, Array, ArrayRef, RecordBatch, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use crate::config::ConnectorConfig;
use crate::connector::ConnectorTaskAdmission;
use crate::error::{ConnectorError, SerdeError};
use crate::fix::engine::{FieldType, Section, SessionDictionaries};
use crate::fix::journal::ApplicationRecord;
use crate::fpml::{message, FpmlDecoder, Profile};
use crate::schema::resolution::NativeSchema;

/// The appended document column.
pub(crate) const COLUMN: &str = "fpml";
const FIELD_KEY: &str = "fpml.field";
const PREFIX: &str = "fpml.";

/// Whether `key` is an FpML option of the FIX source.
pub(crate) fn is_option(key: &str) -> bool {
    key == FIELD_KEY || message::OPTION_KEYS.contains(&key)
}

/// The configured document field, if any.
///
/// # Errors
/// Returns a configuration error for a malformed tag, or FpML options without `fpml.field`.
pub(crate) fn field_tag(config: &ConnectorConfig) -> Result<Option<i32>, ConnectorError> {
    let Some(text) = config.get(FIELD_KEY).map(str::trim) else {
        if let Some(key) = config
            .properties()
            .keys()
            .find(|key| key.starts_with(PREFIX))
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "'{key}' applies only with '{FIELD_KEY}', the tag of the field holding the \
                 FpML document"
            )));
        }
        return Ok(None);
    };
    text.parse::<i32>()
        .ok()
        .filter(|tag| *tag > 0)
        .map(Some)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "'{FIELD_KEY}' must be a FIX tag number, got '{text}'"
            ))
        })
}

/// Output schema and identity entries of a source with an FpML field.
pub(crate) struct Resolved {
    pub(crate) output: SchemaRef,
    pub(crate) identity: BTreeMap<String, String>,
}

/// Validate the field against the dictionary and bind the profile's document column.
pub(crate) async fn resolve(
    config: &ConnectorConfig,
    tag: i32,
    declared: &SchemaRef,
    dictionaries: &SessionDictionaries,
    messages: &std::collections::BTreeSet<String>,
) -> Result<Resolved, ConnectorError> {
    check_field(tag, dictionaries, messages)?;
    if declared.field_with_name(COLUMN).is_ok() {
        return Err(ConnectorError::ConfigurationError(format!(
            "column '{COLUMN}' is the FpML document column of '{FIELD_KEY}'; rename the declared \
             column"
        )));
    }
    let (_, artifact, profile) = message::resolve_profile(config, "A FIX FpML field").await?;
    Ok(Resolved {
        output: with_column(declared, &profile),
        identity: identity(tag, &profile.native_schema(&artifact)),
    })
}

fn check_field(
    tag: i32,
    dictionaries: &SessionDictionaries,
    messages: &std::collections::BTreeSet<String>,
) -> Result<(), ConnectorError> {
    // An XMLDATA field reads as text: QuickFIX delimits it at SOH, which XML cannot contain.
    match dictionaries.field_type(tag)? {
        Some(FieldType::Data | FieldType::Text) => {}
        Some(other) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "'{FIELD_KEY}' {tag} is a {other:?} field; an FpML document needs a DATA or \
                 XMLDATA field"
            )))
        }
        None => {
            return Err(ConnectorError::ConfigurationError(format!(
                "'{FIELD_KEY}' {tag} is not in the dictionary"
            )))
        }
    }
    let placements = messages
        .iter()
        .map(|message| dictionaries.placement(message, tag))
        .collect::<Result<Vec<_>, _>>()?;
    if !placements.contains(&Some(Section::Body)) {
        return Err(ConnectorError::ConfigurationError(format!(
            "'{FIELD_KEY}' {tag} is not a body field of any selected message type"
        )));
    }
    Ok(())
}

fn column_field(profile: &Profile) -> FieldRef {
    Arc::new(Field::new(
        COLUMN,
        DataType::Struct(profile.schema().fields().clone()),
        true,
    ))
}

fn with_column(declared: &SchemaRef, profile: &Profile) -> SchemaRef {
    let mut fields = declared.fields().to_vec();
    fields.push(column_field(profile));
    Arc::new(Schema::new_with_metadata(
        fields,
        declared.metadata().clone(),
    ))
}

/// The declared columns of a start-time schema and, with an FpML field, its started decoder.
pub(crate) async fn open(
    config: &ConnectorConfig,
    schema: SchemaRef,
    dictionary: &Arc<SessionDictionaries>,
    registry: Option<&prometheus::Registry>,
    tasks: ConnectorTaskAdmission,
) -> Result<(SchemaRef, Option<Documents>), ConnectorError> {
    let Some(tag) = field_tag(config)? else {
        return Ok((schema, None));
    };
    let declared = declared(&schema);
    let committed = config
        .schema_binding()
        .and_then(|binding| binding.value.as_ref());
    let documents = Documents::start(
        config,
        tag,
        committed,
        &declared,
        Arc::clone(dictionary),
        registry,
        tasks,
    )
    .await?;
    Ok((declared, Some(documents)))
}

/// The declared columns of an output schema that ends with the document column.
fn declared(output: &SchemaRef) -> SchemaRef {
    let count = output.fields().len();
    match output.fields().last() {
        Some(last) if last.name() == COLUMN => Arc::new(Schema::new_with_metadata(
            output.fields()[..count - 1].to_vec(),
            output.metadata().clone(),
        )),
        _ => Arc::clone(output),
    }
}

fn identity(tag: i32, native: &NativeSchema) -> BTreeMap<String, String> {
    let mut identity: BTreeMap<String, String> = native
        .identity
        .iter()
        .map(|(key, value)| (format!("{PREFIX}{key}"), value.clone()))
        .collect();
    identity.insert(FIELD_KEY.into(), tag.to_string());
    identity
}

struct Captured {
    index: u64,
    seq: u64,
    document: Option<Vec<u8>>,
    rows: usize,
}

/// Running decoder of the document column.
pub(crate) struct Documents {
    tag: i32,
    decoder: Arc<FpmlDecoder>,
    dictionary: Arc<SessionDictionaries>,
    field: FieldRef,
    schema: SchemaRef,
    tasks: ConnectorTaskAdmission,
    captured: Vec<Captured>,
}

impl Documents {
    /// Start the decoder for the field, closure, and profile the committed binding names.
    async fn start(
        config: &ConnectorConfig,
        tag: i32,
        committed: Option<&NativeSchema>,
        declared: &SchemaRef,
        dictionary: Arc<SessionDictionaries>,
        registry: Option<&prometheus::Registry>,
        tasks: ConnectorTaskAdmission,
    ) -> Result<Self, ConnectorError> {
        let (fpml, artifact, profile) =
            message::resolve_profile(config, "A FIX FpML field").await?;
        let expected = identity(tag, &profile.native_schema(&artifact));
        let bound: Option<BTreeMap<&String, &String>> = committed.map(|native| {
            native
                .identity
                .iter()
                .filter(|(key, _)| key.starts_with(PREFIX))
                .collect()
        });
        if bound != Some(expected.iter().collect()) {
            return Err(ConnectorError::SchemaMismatch(
                "the FpML field, profile, or schema closure differs from the one this source was \
                 created with; recreate the source to change it"
                    .into(),
            ));
        }
        let guard = tasks.track().ok_or_else(retired)?;
        let source = config.get("laminar.source.name").unwrap_or("fix");
        let decoder =
            FpmlDecoder::start(artifact, &profile, &fpml, source, registry, Some(guard)).await?;
        Ok(Self {
            tag,
            decoder: Arc::new(decoder),
            dictionary,
            field: column_field(&profile),
            schema: with_column(declared, &profile),
            tasks,
            captured: Vec::new(),
        })
    }

    /// The source output: the declared columns, then the document column.
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Record the document of a message that projected `rows` rows.
    pub(crate) fn capture(
        &mut self,
        index: u64,
        record: &ApplicationRecord,
        rows: usize,
    ) -> Result<(), ConnectorError> {
        if rows == 0 {
            return Ok(());
        }
        let message = self.dictionary.parse(&record.message)?;
        let document = message.view().body()?.field(self.tag)?.map(<[u8]>::to_vec);
        self.captured.push(Captured {
            index,
            seq: record.seq,
            document,
            rows,
        });
        Ok(())
    }

    /// Discard documents captured for rows that were discarded.
    pub(crate) fn reset(&mut self) {
        self.captured.clear();
    }

    /// Validate and project the captured documents off the async runtime, then append them to
    /// the projected rows.
    pub(crate) async fn append(
        &mut self,
        records: RecordBatch,
    ) -> Result<RecordBatch, ConnectorError> {
        let captured = std::mem::take(&mut self.captured);
        let guard = self.tasks.track().ok_or_else(retired)?;
        let decoder = Arc::clone(&self.decoder);
        let field = Arc::clone(&self.field);
        let tag = self.tag;
        let column = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            column(&decoder, &field, tag, &captured)
        })
        .await
        .map_err(|error| ConnectorError::Internal(format!("FpML decode task: {error}")))??;
        let mut columns = records.columns().to_vec();
        columns.push(column);
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))
    }
}

/// One struct value per row: the message's projected document, or null without the field.
fn column(
    decoder: &FpmlDecoder,
    field: &Field,
    tag: i32,
    captured: &[Captured],
) -> Result<ArrayRef, ConnectorError> {
    let mut documents: Vec<StructArray> = Vec::new();
    let mut rows: Vec<Option<u32>> = Vec::new();
    for message in captured {
        let slot = match &message.document {
            None => None,
            Some(bytes) => {
                let batch = decoder
                    .decode(bytes)
                    .map_err(|rejection| rejected(message, tag, &rejection.to_string()))?;
                if batch.num_rows() != 1 {
                    return Err(rejected(
                        message,
                        tag,
                        &format!("the profile projected {} rows, not one", batch.num_rows()),
                    ));
                }
                documents.push(StructArray::from(batch));
                Some(u32::try_from(documents.len() - 1).map_err(|_| {
                    ConnectorError::Internal("too many FpML documents in one poll".into())
                })?)
            }
        };
        rows.extend(std::iter::repeat_n(slot, message.rows));
    }
    if documents.is_empty() {
        return Ok(new_null_array(field.data_type(), rows.len()));
    }
    let values: Vec<&dyn Array> = documents.iter().map(|array| array as &dyn Array).collect();
    let values = arrow_select::concat::concat(&values)
        .map_err(|error| ConnectorError::Internal(format!("FpML document column: {error}")))?;
    arrow_select::take::take(&values, &UInt32Array::from(rows), None)
        .map_err(|error| ConnectorError::Internal(format!("FpML document column: {error}")))
}

fn rejected(message: &Captured, tag: i32, reason: &str) -> ConnectorError {
    ConnectorError::Serde(SerdeError::MalformedInput(format!(
        "FIX journal record {} (seq {}): the FpML document in tag {tag} was rejected: {reason}",
        message.index, message.seq
    )))
}

fn retired() -> ConnectorError {
    ConnectorError::Internal("FIX source generation is retired".into())
}
