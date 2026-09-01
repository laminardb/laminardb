//! External startup admission and deployment/collection identity observation.

use std::sync::Arc;

use futures_util::TryStreamExt;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::{
    run_change_stream_reader, BufferedMongoEvent, ConnectorError, ConnectorState,
    MongoAdmissionObservation, MongoCollectionObservation, MongoDbCdcSource, MongoDbSourceConfig,
    MongoDeploymentIdentity, MongoReaderAdmissionGuard, MongoReaderFailure, MongoReaderReady,
    MongoResumePosition, READER_SHUTDOWN_TIMEOUT, READER_STARTUP_TIMEOUT,
};

// ── Feature-gated I/O (real MongoDB driver) ──

#[cfg(feature = "mongodb-cdc")]
pub(super) fn clamp_source_startup_timeout(
    configured: Option<std::time::Duration>,
) -> std::time::Duration {
    configured
        .filter(|timeout| !timeout.is_zero())
        .map_or(READER_STARTUP_TIMEOUT, |timeout| {
            timeout.min(READER_STARTUP_TIMEOUT)
        })
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn source_client_options(
    connection_uri: &str,
) -> Result<mongodb::options::ClientOptions, ConnectorError> {
    let mut options = mongodb::options::ClientOptions::parse(connection_uri)
        .await
        .map_err(|error| ConnectorError::ConfigurationError(format!("parse URI: {error}")))?;
    super::super::sink::harden_mongodb_tls(&mut options)?;
    options.connect_timeout = Some(clamp_source_startup_timeout(options.connect_timeout));
    options.server_selection_timeout = Some(clamp_source_startup_timeout(
        options.server_selection_timeout,
    ));

    if let Some(pool) = options.max_pool_size {
        if pool <= 1 {
            tracing::warn!(
                max_pool_size = pool,
                "max_pool_size is very small; mongos may exhaust per-shard cursors"
            );
        }
    }
    Ok(options)
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn source_database(
    connection_uri: &str,
    database: &str,
) -> Result<mongodb::Database, ConnectorError> {
    let options = source_client_options(connection_uri).await?;
    let client = mongodb::Client::with_options(options)
        .map_err(|error| ConnectorError::ConfigurationError(format!("create client: {error}")))?;
    Ok(client.database(database))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn await_mongo_reader_ready(
    ready_rx: tokio::sync::oneshot::Receiver<Result<MongoReaderReady, MongoReaderFailure>>,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
    handle: &mut tokio::task::JoinHandle<()>,
) -> Result<MongoReaderReady, ConnectorError> {
    let (error, include_join_error) =
        match tokio::time::timeout(READER_STARTUP_TIMEOUT, ready_rx).await {
            Ok(Ok(Ok(ready))) => return Ok(ready),
            Ok(Ok(Err(error))) => (error.into_connector(), false),
            Ok(Err(_)) => (
                ConnectorError::ReadError(
                    "MongoDB CDC reader exited before opening the change stream".into(),
                ),
                true,
            ),
            Err(_) => (
                ConnectorError::ReadError(format!(
                    "MongoDB CDC did not open a change stream within the {READER_STARTUP_TIMEOUT:?} startup deadline"
                )),
                false,
            ),
        };

    shutdown_tx.send_replace(true);
    let Ok(join_result) = tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, &mut *handle).await else {
        tracing::warn!(
            "MongoDB CDC admission reader exceeded its shutdown deadline; the retired generation remains tracked until it exits"
        );
        return Err(error);
    };
    let error = if include_join_error {
        match join_result {
            Err(join_error) => ConnectorError::ReadError(format!("{error}: {join_error}")),
            _ => error,
        }
    } else {
        error
    };
    Err(error)
}

#[cfg(feature = "mongodb-cdc")]
impl MongoDbCdcSource {
    /// Starts the background change stream reader task.
    pub(super) async fn start_change_stream_reader(
        &mut self,
        config: MongoDbSourceConfig,
        checkpoint_resume_token: Option<String>,
        checkpoint_requires_start_after: bool,
        initial_resume_position: Option<MongoResumePosition>,
        expected_collection_uuid: Option<Uuid>,
        expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ) -> Result<(), ConnectorError> {
        if self.reader_handle.is_some() {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: "reader already started".into(),
            });
        }
        if !self.event_buffer.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB CDC cannot start with pre-buffered test events".into(),
            ));
        }
        let max_buffered_bytes = config.max_buffered_bytes;
        let byte_budget = Arc::new(Semaphore::new(max_buffered_bytes));

        let channel_capacity = config.reader_channel_capacity();
        let (tx, rx) = crossfire::mpsc::bounded_async::<BufferedMongoEvent>(channel_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (error_tx, error_rx) = tokio::sync::watch::channel(None);
        let (ready_tx, ready_rx) =
            tokio::sync::oneshot::channel::<Result<MongoReaderReady, MongoReaderFailure>>();
        let reader_config = config.clone();
        let data_ready = Arc::clone(&self.data_ready);
        let terminal_ready = Arc::clone(&self.data_ready);
        let metrics = Arc::clone(&self.metrics);
        let task_byte_budget = Arc::clone(&byte_budget);

        let reader_guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("MongoDB CDC connector generation is already retired".into())
        })?;

        let mut handle = tokio::spawn(async move {
            let _reader_guard = reader_guard;
            let result = async {
                let db =
                    match source_database(&reader_config.connection_uri, &reader_config.database)
                        .await
                    {
                        Ok(database) => database,
                        Err(error) => {
                            let admission_error = MongoReaderFailure::from_connector(&error);
                            let _ = ready_tx.send(Err(admission_error));
                            return Err(error);
                        }
                    };
                run_change_stream_reader(
                    db,
                    reader_config,
                    tx,
                    shutdown_rx,
                    data_ready,
                    metrics,
                    task_byte_budget,
                    max_buffered_bytes,
                    initial_resume_position,
                    expected_collection_uuid,
                    expected_deployment_identity,
                    ready_tx,
                )
                .await
            }
            .await;
            if let Err(e) = result {
                tracing::error!(error = %e, "change stream reader task failed");
                error_tx.send_replace(Some(MongoReaderFailure::from_connector(&e)));
                terminal_ready.notify_one();
            }
        });
        let mut admission_guard = MongoReaderAdmissionGuard::new(shutdown_tx.clone());

        let ready = await_mongo_reader_ready(ready_rx, &shutdown_tx, &mut handle).await?;

        admission_guard.disarm();
        self.config = config;
        self.checkpoint_resume_token = ready.initial_resume_token.or(checkpoint_resume_token);
        self.checkpoint_requires_start_after = checkpoint_requires_start_after;
        self.collection_uuid = Some(ready.collection_uuid);
        self.deployment_identity = Some(ready.deployment_identity);
        self.byte_budget = byte_budget;
        self.reader_handle = Some(handle);
        self.event_rx = Some(rx);
        self.reader_shutdown = Some(shutdown_tx);
        self.reader_error = Some(error_rx);
        Ok(())
    }

    /// Drains events from the background reader channel into the buffer.
    pub(super) fn drain_channel(&mut self, max_events: usize) {
        let max_events = max_events.min(self.config.reader_channel_capacity());
        for _ in 0..max_events {
            let item = {
                let Some(rx) = self.event_rx.as_mut() else {
                    break;
                };
                let Ok(item) = rx.try_recv() else {
                    break;
                };
                item
            };
            if let Some(event) = item.event() {
                self.metrics.record_event(event.operation_type.as_str());
            }
            self.event_buffer.push_back(item);
        }
    }

    pub(super) fn check_reader_error(&mut self) -> Result<(), ConnectorError> {
        let error = self
            .reader_error
            .as_mut()
            .and_then(|receiver| receiver.borrow_and_update().clone());
        if let Some(error) = error {
            self.metrics.record_error();
            return Err(error.into_connector());
        }
        Ok(())
    }
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn mongodb_identity_command_is_permanent(code: i32, code_name: &str) -> bool {
    matches!(
        code,
        13 | 18 | 20 | 26 | 59 | 72 | 76 | 115 | 123 | 323 | 8000
    ) || matches!(
        code_name,
        "Unauthorized"
            | "AuthenticationFailed"
            | "IllegalOperation"
            | "NamespaceNotFound"
            | "CommandNotFound"
            | "InvalidOptions"
            | "NoReplicationEnabled"
            | "CommandNotSupported"
            | "NotAReplicaSet"
            | "APIStrictError"
            | "AtlasError"
    )
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn mongodb_identity_probe_is_permanent(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Authentication { .. } => true,
        mongodb::error::ErrorKind::Command(command) => {
            mongodb_identity_command_is_permanent(command.code, &command.code_name)
        }
        _ => false,
    }
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn observe_mongodb_deployment(
    db: &mongodb::Database,
) -> Result<MongoDeploymentIdentity, ConnectorError> {
    let hello = db
        .run_command(mongodb::bson::doc! { "hello": 1 })
        .await
        .map_err(|error| {
            if mongodb_identity_probe_is_permanent(&error) {
                return ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC cannot inspect deployment topology; verify credentials and \
                     deployment command support: {error}"
                ));
            }
            ConnectorError::ConnectionFailed(format!(
                "inspect MongoDB deployment topology with hello: {error}"
            ))
        })?;

    if hello.get_str("msg").ok() == Some("isdbgrid") {
        let version = db
            .client()
            .database("config")
            .collection::<mongodb::bson::Document>("version")
            .find_one(mongodb::bson::doc! { "_id": 1 })
            .projection(mongodb::bson::doc! { "clusterId": 1 })
            .await
            .map_err(|error| {
                if mongodb_identity_probe_is_permanent(&error) {
                    ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC requires read access to config.version {{_id: 1}}.clusterId \
                         to bind checkpoints to the sharded cluster identity: {error}"
                    ))
                } else {
                    ConnectorError::ConnectionFailed(format!(
                        "read MongoDB sharded cluster identity from config.version: {error}"
                    ))
                }
            })?
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB config.version {_id: 1} is missing; cannot bind CDC checkpoints to \
                     this sharded cluster"
                        .into(),
                )
            })?;
        let cluster_id = version.get_object_id("clusterId").map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB config.version.clusterId is missing or not an ObjectId: {error}"
            ))
        })?;
        return Ok(MongoDeploymentIdentity::ShardedCluster(cluster_id.to_hex()));
    }

    if hello.get_str("setName").is_ok() {
        let response = db
            .client()
            .database("admin")
            .run_command(mongodb::bson::doc! { "replSetGetConfig": 1 })
            .await
            .map_err(|error| {
                if mongodb_identity_probe_is_permanent(&error) {
                    ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC requires replSetGetConfig access to bind checkpoints to the \
                         replica-set identity; Atlas M0 and Flex tiers do not support this \
                         command: {error}"
                    ))
                } else {
                    ConnectorError::ConnectionFailed(format!(
                        "read MongoDB replica-set identity with replSetGetConfig: {error}"
                    ))
                }
            })?;
        let replica_set_id = response
            .get_document("config")
            .and_then(|config| config.get_document("settings"))
            .and_then(|settings| settings.get_object_id("replicaSetId"))
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB replSetGetConfig omitted settings.replicaSetId: {error}"
                ))
            })?;
        return Ok(MongoDeploymentIdentity::ReplicaSet(replica_set_id.to_hex()));
    }

    Err(ConnectorError::ConfigurationError(
        "MongoDB CDC requires a replica set or sharded cluster; hello reported neither topology"
            .into(),
    ))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn observe_mongodb_admission(
    db: &mongodb::Database,
    database: &str,
    collection: &str,
) -> Result<MongoAdmissionObservation, ConnectorError> {
    let (deployment_identity, collection) = tokio::try_join!(
        observe_mongodb_deployment(db),
        observe_mongodb_collection(db, database, collection),
    )?;
    Ok(MongoAdmissionObservation {
        deployment_identity,
        collection,
    })
}

/// Read the immutable identity and post-image capability for one fixed collection.
#[cfg(feature = "mongodb-cdc")]
pub(super) async fn observe_mongodb_collection(
    db: &mongodb::Database,
    database: &str,
    collection: &str,
) -> Result<MongoCollectionObservation, ConnectorError> {
    let mut cursor = db
        .list_collections()
        .filter(mongodb::bson::doc! { "name": collection })
        .batch_size(1)
        .await
        .map_err(|error| {
            if mongodb_identity_probe_is_permanent(&error) {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC requires database-scoped listCollections access to bind \
                     {database}.{collection} to its collection UUID: {error}"
                ))
            } else {
                ConnectorError::ConnectionFailed(format!(
                    "inspect MongoDB collection {database}.{collection}: {error}"
                ))
            }
        })?;
    let spec = cursor
        .try_next()
        .await
        .map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "read MongoDB collection identity for {database}.{collection}: {error}"
            ))
        })?
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB CDC collection {database}.{collection} does not exist; create the fixed \
                 collection before starting the source"
            ))
        })?;

    match spec.collection_type {
        mongodb::results::CollectionType::Collection => {}
        mongodb::results::CollectionType::Timeseries => {
            return Err(ConnectorError::ConfigurationError(format!(
                "time series collection {database}.{collection} does not support change streams"
            )));
        }
        mongodb::results::CollectionType::View => {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC source {database}.{collection} must be a collection, not a view"
            )));
        }
        _ => {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC source {database}.{collection} has an unsupported collection type"
            )));
        }
    }

    let post_images_enabled = spec
        .options
        .change_stream_pre_and_post_images
        .as_ref()
        .is_some_and(|options| options.enabled);
    let binary = spec.info.uuid.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB collection {database}.{collection} did not expose an immutable UUID"
        ))
    })?;
    if binary.subtype != mongodb::bson::spec::BinarySubtype::Uuid || binary.bytes.len() != 16 {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB collection {database}.{collection} returned a non-standard collection UUID"
        )));
    }
    let collection_uuid = Uuid::from_slice(&binary.bytes).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid UUID for MongoDB collection {database}.{collection}: {error}"
        ))
    })?;
    Ok(MongoCollectionObservation {
        collection_uuid,
        post_images_enabled,
    })
}
