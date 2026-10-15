//! The FpML decoder: one owned worker thread per source holding the compiled schema.
//!
//! Each document is pre-scanned, parsed and validated by libxml2, reference-checked, and
//! projected on the worker; the caller blocks for the result. The job queue holds one document,
//! so a slow validator applies backpressure instead of buffering. Dropping the decoder closes
//! the queue; the worker finishes the document in hand (native validation cannot be preempted)
//! and exits, releasing its connector task guard.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use sha2::{Digest, Sha256};

use super::artifacts::{hex, Artifact};
use super::document::Interner;
use super::error::{Rejection, Stage};
use super::limits::Limits;
use super::metrics::FpmlMetrics;
use super::prescan::prescan;
use super::profile::{Profile, ProfileSource};
use super::projection::{project, DocumentInfo};
use super::references::{ReferencePolicy, ReferenceRules};
use super::schema_model::SchemaModel;
use super::xsd::{TreeNames, Validator};
use crate::config::ConnectorConfig;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Bundled profile used when neither `fpml.profile` nor `fpml.profile.path` is set.
pub const DEFAULT_PROFILE: &str = "fpml-5-13-confirmation-trade";
/// Names interned beyond the profile's own before the worker resets its interner.
const MAX_EXTRA_NAMES: usize = 16_384;

/// Parsed `fpml.*` options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FpmlConfig {
    /// Projection profile.
    pub profile: ProfileSource,
    /// Directory of an operator-supplied schema closure; the bundled closure otherwise.
    pub schema_path: Option<String>,
    /// Reference checking beyond resolution.
    pub references: ReferencePolicy,
    /// Per-document bounds.
    pub limits: Limits,
}

impl FpmlConfig {
    /// Parse the `fpml.*` connector options.
    ///
    /// # Errors
    /// Returns a configuration error for conflicting or malformed options.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let profile = match (config.get("fpml.profile"), config.get("fpml.profile.path")) {
            (Some(_), Some(_)) => {
                return Err(ConnectorError::ConfigurationError(
                    "set either 'fpml.profile' or 'fpml.profile.path', not both".into(),
                ))
            }
            (Some(name), None) => ProfileSource::Bundled(name.to_owned()),
            (None, Some(path)) => ProfileSource::File(path.into()),
            (None, None) => ProfileSource::Bundled(DEFAULT_PROFILE.to_owned()),
        };
        let references = match config.get("fpml.references").unwrap_or("resolve") {
            "resolve" => ReferencePolicy::Resolve,
            "schema" => ReferencePolicy::Schema,
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "'fpml.references' must be 'resolve' or 'schema', not '{other}'"
                )))
            }
        };
        Ok(Self {
            profile,
            schema_path: config.get("fpml.schema.path").map(str::to_owned),
            references,
            limits: Limits::from_config(config)?,
        })
    }

    /// Load the closure and compile the profile. Reads operator files; call off the async
    /// runtime's worker threads.
    ///
    /// # Errors
    /// Returns a configuration error for an invalid closure or profile.
    pub fn resolve(&self) -> Result<(Artifact, Profile), ConnectorError> {
        let artifact = Artifact::resolve(self.schema_path.as_deref())?;
        let profile = Profile::load(&self.profile, &artifact)?;
        Ok((artifact, profile))
    }
}

struct Job {
    bytes: Vec<u8>,
    reply: mpsc::SyncSender<Result<RecordBatch, Rejection>>,
}

/// Decodes one complete FpML document per record into rows of the profile's schema.
pub struct FpmlDecoder {
    schema: SchemaRef,
    jobs: mpsc::SyncSender<Job>,
}

impl std::fmt::Debug for FpmlDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FpmlDecoder")
            .field("fields", &self.schema.fields().len())
            .finish_non_exhaustive()
    }
}

impl FpmlDecoder {
    /// Start the worker: compile the schema on it and wait until it is ready.
    ///
    /// `guard` is held by the worker for its lifetime, so the owning connector's task tracker
    /// observes the worker's exit.
    ///
    /// # Errors
    /// Returns a configuration error if the schema closure does not compile, or an internal
    /// error if the worker cannot start.
    pub async fn start(
        artifact: Artifact,
        profile: &Profile,
        config: &FpmlConfig,
        source: &str,
        registry: Option<&prometheus::Registry>,
        guard: Option<ConnectorTaskGuard>,
    ) -> Result<Self, ConnectorError> {
        let metrics = match registry {
            Some(registry) => FpmlMetrics::registered(source, registry)?,
            None => FpmlMetrics::local(source)?,
        };
        let (jobs, queue) = mpsc::sync_channel::<Job>(1);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let worker = Worker {
            artifact,
            plan: Arc::clone(&profile.plan),
            references: config.references,
            limits: config.limits,
            metrics,
        };
        std::thread::Builder::new()
            .name("fpml-validator".into())
            .spawn(move || {
                let _guard = guard;
                worker.run(&queue, ready_tx);
            })
            .map_err(|error| {
                ConnectorError::Internal(format!("cannot start the FpML validator thread: {error}"))
            })?;
        ready_rx.await.map_err(|_| {
            ConnectorError::Internal("FpML validator thread exited during startup".into())
        })??;
        Ok(Self {
            schema: profile.schema(),
            jobs,
        })
    }

    /// Decode one document. Blocks the calling thread until the worker answers.
    ///
    /// # Errors
    /// Returns the rejection when any stage rejects the document, or a worker rejection when
    /// the worker has stopped.
    pub fn decode(&self, bytes: &[u8]) -> Result<RecordBatch, Rejection> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.jobs
            .send(Job {
                bytes: bytes.to_vec(),
                reply,
            })
            .map_err(|_| Rejection::new(Stage::Worker, "the FpML validator has stopped"))?;
        answer.recv().map_err(|_| {
            Rejection::new(Stage::Worker, "the FpML validator stopped while decoding")
        })?
    }
}

impl FormatDecoder for FpmlDecoder {
    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        let mut batches = Vec::with_capacity(records.len());
        for record in records {
            batches.push(
                self.decode(&record.value)
                    .map_err(|rejection| SchemaError::DecodeError(rejection.to_string()))?,
            );
        }
        match batches.len() {
            0 => Ok(RecordBatch::new_empty(Arc::clone(&self.schema))),
            1 => Ok(batches.remove(0)),
            _ => arrow_select::concat::concat_batches(&self.schema, &batches)
                .map_err(|error| SchemaError::DecodeError(error.to_string())),
        }
    }

    fn format_name(&self) -> &'static str {
        "fpml"
    }
}

struct Worker {
    artifact: Artifact,
    plan: Arc<super::profile::Plan>,
    references: ReferencePolicy,
    limits: Limits,
    metrics: FpmlMetrics,
}

struct Prepared {
    validator: Validator,
    seed: Interner,
    interner: Interner,
    names: TreeNames,
    rules: ReferenceRules,
}

impl Worker {
    fn run(
        self,
        queue: &mpsc::Receiver<Job>,
        ready: tokio::sync::oneshot::Sender<Result<(), ConnectorError>>,
    ) {
        let mut prepared = match self.prepare() {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        };
        if ready.send(Ok(())).is_err() {
            return;
        }
        // Ends when the decoder (the only sender) is dropped.
        while let Ok(job) = queue.recv() {
            let started = Instant::now();
            self.metrics.document_bytes.inc_by(job.bytes.len() as u64);
            let result = self.decode(&mut prepared, &job.bytes);
            self.metrics
                .decode_seconds
                .observe(started.elapsed().as_secs_f64());
            match &result {
                Ok(batch) => {
                    self.metrics.documents_accepted.inc();
                    self.metrics.rows.inc_by(batch.num_rows() as u64);
                }
                Err(rejection) => self.metrics.rejected(rejection.stage),
            }
            let _ = job.reply.send(result);
            if prepared.interner.len() > prepared.seed.len() + MAX_EXTRA_NAMES {
                prepared.interner = prepared.seed.clone();
            }
        }
    }

    fn prepare(&self) -> Result<Prepared, ConnectorError> {
        let validator = Validator::compile(&self.artifact.files).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "FpML schema closure '{}': {error}",
                self.artifact.id
            ))
        })?;
        let model = SchemaModel::for_artifact(&self.artifact)?;
        let mut interner = self.plan.interner.clone();
        let names = TreeNames::new(&mut interner, &self.artifact.reference_namespaces);
        let rules = ReferenceRules::new(&model, &mut interner);
        Ok(Prepared {
            validator,
            seed: interner.clone(),
            interner,
            names,
            rules,
        })
    }

    fn decode(&self, prepared: &mut Prepared, bytes: &[u8]) -> Result<RecordBatch, Rejection> {
        let scan = prescan(bytes, &self.limits)?;
        if scan.root_namespace != self.artifact.namespace {
            return Err(Rejection::new(
                Stage::Root,
                format!(
                    "document root namespace '{}' is not the pinned view namespace '{}'",
                    truncate(&scan.root_namespace, 128),
                    self.artifact.namespace
                ),
            ));
        }
        let (document, refusals) = prepared.validator.validate(
            bytes,
            &self.limits,
            &mut prepared.interner,
            &prepared.names,
        )?;
        if refusals.attempts != 0 {
            return Err(Rejection::new(
                Stage::Parse,
                "the document asked libxml2 to load an external resource",
            ));
        }
        if document.elements.len() != scan.elements {
            return Err(Rejection::new(
                Stage::Worker,
                "the pre-scan and the validating parser disagree on the element count",
            ));
        }
        prepared.rules.check(
            &document,
            &prepared.interner,
            prepared.names.href,
            self.references,
        )?;
        let sha256 = hex(&Sha256::digest(bytes));
        project(
            &self.plan,
            &document,
            &prepared.interner,
            &prepared.rules,
            prepared.names.href,
            &self.limits,
            &DocumentInfo {
                sha256: &sha256,
                bytes: bytes.len(),
            },
        )
    }
}

fn truncate(text: &str, limit: usize) -> &str {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
