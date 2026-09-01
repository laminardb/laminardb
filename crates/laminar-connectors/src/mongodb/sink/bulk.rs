//! Bounded `MongoDB` operation tasks, ordered bulk models, and collection I/O.

use super::{
    checked_converted_total, clamp_client_timeout, classify_mongo_bulk_failure, debug,
    encoded_document_size, ensure_working_set, harden_mongodb_tls, info, is_namespace_exists,
    validate_existing_timeseries_spec, CollectionKind, ConnectorError, ConnectorTaskOwner, Future,
    MongoBulkFailure, MongoDbSink, WriteMode, MAX_SINK_WORKING_SET_BYTES,
    MAX_STANDARD_DOCUMENT_BYTES, MONGODB_8_WIRE_VERSION,
};

#[derive(Debug)]
pub(super) enum CdcWrite {
    Insert {
        filter: mongodb::bson::Document,
        replacement: mongodb::bson::Document,
    },
    Update {
        filter: mongodb::bson::Document,
        update: mongodb::bson::Document,
    },
    Replace {
        filter: mongodb::bson::Document,
        replacement: mongodb::bson::Document,
    },
    Delete {
        filter: mongodb::bson::Document,
    },
    Noop,
}

#[derive(Default)]
pub(super) struct BulkCounts {
    pub(super) inserts: u64,
    pub(super) upserts: u64,
    pub(super) deletes: u64,
}

pub(super) enum MongoOperationOutcome<T> {
    Completed(T),
    Deadline,
    TaskFailed(tokio::task::JoinError),
}

pub(super) fn spawn_mongo_sink_operation<F, T>(
    task_owner: &ConnectorTaskOwner,
    operation: F,
) -> Result<tokio::task::JoinHandle<T>, ConnectorError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let guard = task_owner.track().ok_or_else(|| {
        ConnectorError::Internal(
            "MongoDB sink task generation was sealed before bulk operation admission".into(),
        )
    })?;
    // The driver has no supported socket/operation timeout. Dropping this handle therefore
    // detaches the operation; its guard remains live until the driver future actually exits.
    Ok(tokio::spawn(async move {
        let _guard = guard;
        operation.await
    }))
}

pub(super) async fn await_mongo_sink_operation<F, T>(
    task_owner: &ConnectorTaskOwner,
    deadline: tokio::time::Instant,
    operation: F,
) -> Result<MongoOperationOutcome<T>, ConnectorError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut task = spawn_mongo_sink_operation(task_owner, operation)?;
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(result)) => Ok(MongoOperationOutcome::Completed(result)),
        Ok(Err(error)) => Ok(MongoOperationOutcome::TaskFailed(error)),
        Err(_) => Ok(MongoOperationOutcome::Deadline),
    }
}

pub(super) fn cdc_bulk_models(
    namespace: &mongodb::Namespace,
    writes: Vec<CdcWrite>,
) -> (Vec<mongodb::options::WriteModel>, BulkCounts) {
    use mongodb::options::{DeleteOneModel, ReplaceOneModel, UpdateOneModel, WriteModel};

    let mut models = Vec::with_capacity(writes.len());
    let mut counts = BulkCounts::default();
    for write in writes {
        match write {
            CdcWrite::Insert {
                filter,
                replacement,
            } => {
                models.push(WriteModel::ReplaceOne(
                    ReplaceOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .replacement(replacement)
                        .upsert(true)
                        .build(),
                ));
                counts.inserts = counts.inserts.saturating_add(1);
            }
            CdcWrite::Update { filter, update } => {
                models.push(WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .update(update)
                        .build(),
                ));
                counts.upserts = counts.upserts.saturating_add(1);
            }
            CdcWrite::Replace {
                filter,
                replacement,
            } => {
                models.push(WriteModel::ReplaceOne(
                    ReplaceOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .replacement(replacement)
                        .upsert(true)
                        .build(),
                ));
                counts.upserts = counts.upserts.saturating_add(1);
            }
            CdcWrite::Delete { filter } => {
                models.push(WriteModel::DeleteOne(
                    DeleteOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .build(),
                ));
                counts.deletes = counts.deletes.saturating_add(1);
            }
            CdcWrite::Noop => {}
        }
    }
    (models, counts)
}

impl MongoDbSink {
    /// Connects to `MongoDB` and sets up the target collection with write concern.
    pub(super) async fn connect(&mut self) -> Result<(), ConnectorError> {
        use mongodb::options::{ClientOptions, CollectionOptions};

        let mut client_options = ClientOptions::parse(&self.config.connection_uri)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("parse URI: {e}")))?;
        harden_mongodb_tls(&mut client_options)?;
        let driver_timeout = self.driver_timeout();
        client_options.connect_timeout = Some(clamp_client_timeout(
            client_options.connect_timeout,
            driver_timeout,
        ));
        client_options.server_selection_timeout = Some(clamp_client_timeout(
            client_options.server_selection_timeout,
            driver_timeout,
        ));

        let wc = {
            let mut wc = mongodb::options::WriteConcern::default();
            wc.w = Some(mongodb::options::Acknowledgment::Majority);
            wc.journal = Some(true);
            wc.w_timeout = Some(driver_timeout);
            wc
        };
        // Client::bulk_write is client-scoped, so its inherited concern must live on the client.
        client_options.write_concern = Some(wc.clone());
        let client = mongodb::Client::with_options(client_options)
            .map_err(|e| ConnectorError::ConnectionFailed(format!("create client: {e}")))?;

        let db = client.database(&self.config.database);
        let hello = db
            .run_command(mongodb::bson::doc! { "hello": 1 })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "verify MongoDB bulk-write capability: {error}"
                ))
            })?;
        let max_wire_version = hello.get_i32("maxWireVersion").map_err(|_| {
            ConnectorError::ConnectionFailed(
                "MongoDB hello response omitted integer maxWireVersion".into(),
            )
        })?;
        if max_wire_version < MONGODB_8_WIRE_VERSION {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB sink requires MongoDB 8.0+ ordered bulk_write (maxWireVersion >= \
                 {MONGODB_8_WIRE_VERSION}); server reported {max_wire_version}"
            )));
        }

        match &self.config.collection_kind {
            CollectionKind::Standard => self.validate_standard_collection(&db).await?,
            CollectionKind::TimeSeries(ts_config) => {
                self.ensure_timeseries_collection(&db, ts_config).await?;
            }
        }

        let coll_opts = CollectionOptions::builder().write_concern(wc).build();

        let collection = db
            .collection_with_options::<mongodb::bson::Document>(&self.config.collection, coll_opts);

        self.client = Some(client);
        self.collection = Some(collection);

        Ok(())
    }

    /// Reject an existing view or time-series collection when standard mode was requested.
    pub(super) async fn validate_standard_collection(
        &self,
        db: &mongodb::Database,
    ) -> Result<(), ConnectorError> {
        use futures_util::TryStreamExt;
        use mongodb::bson::doc;
        use mongodb::results::CollectionType;

        let mut collections = db
            .list_collections()
            .filter(doc! { "name": &self.config.collection })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "inspect existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?;
        if let Some(spec) = collections.try_next().await.map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "read existing MongoDB collection '{}': {error}",
                self.config.collection
            ))
        })? {
            if spec.collection_type != CollectionType::Collection {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB standard sink target '{}' already exists as {:?}",
                    self.config.collection, spec.collection_type
                )));
            }
        }
        Ok(())
    }

    /// Ensures a time series collection exists with the correct configuration.
    pub(super) async fn ensure_timeseries_collection(
        &self,
        db: &mongodb::Database,
        ts_config: &super::super::timeseries::TimeSeriesConfig,
    ) -> Result<(), ConnectorError> {
        use mongodb::bson::doc;

        let mut ts_opts = doc! {
            "timeField": &ts_config.time_field,
        };

        if let Some(ref meta) = ts_config.meta_field {
            ts_opts.insert("metaField", meta);
        }

        match ts_config.granularity {
            super::super::timeseries::TimeSeriesGranularity::Seconds => {
                ts_opts.insert("granularity", "seconds");
            }
            super::super::timeseries::TimeSeriesGranularity::Minutes => {
                ts_opts.insert("granularity", "minutes");
            }
            super::super::timeseries::TimeSeriesGranularity::Hours => {
                ts_opts.insert("granularity", "hours");
            }
            super::super::timeseries::TimeSeriesGranularity::Custom {
                bucket_max_span_seconds,
                bucket_rounding_seconds,
            } => {
                ts_opts.insert("bucketMaxSpanSeconds", i64::from(bucket_max_span_seconds));
                ts_opts.insert("bucketRoundingSeconds", i64::from(bucket_rounding_seconds));
            }
        }

        let mut create_opts = doc! {
            "create": &self.config.collection,
            "timeseries": ts_opts,
        };

        if let Some(ttl) = ts_config.expire_after_seconds {
            let ttl = i64::try_from(ttl).map_err(|_| {
                ConnectorError::ConfigurationError(
                    "time series expire_after_seconds exceeds MongoDB's signed 64-bit range".into(),
                )
            })?;
            create_opts.insert("expireAfterSeconds", ttl);
        }

        // A concurrent creator is safe only when it created the same collection shape.
        match db.run_command(create_opts).await {
            Ok(_) => {
                info!(
                    collection = %self.config.collection,
                    time_field = %ts_config.time_field,
                    granularity = %ts_config.granularity,
                    "created time series collection"
                );
            }
            Err(e) => {
                if !is_namespace_exists(&e) {
                    return Err(ConnectorError::ConnectionFailed(format!(
                        "create time series collection: {e}"
                    )));
                }
                self.validate_existing_timeseries_collection(db, ts_config)
                    .await?;
                debug!(
                    collection = %self.config.collection,
                    "matching time series collection already exists"
                );
            }
        }

        Ok(())
    }

    pub(super) async fn validate_existing_timeseries_collection(
        &self,
        db: &mongodb::Database,
        expected: &super::super::timeseries::TimeSeriesConfig,
    ) -> Result<(), ConnectorError> {
        use futures_util::TryStreamExt;
        use mongodb::bson::doc;

        let mut collections = db
            .list_collections()
            .filter(doc! { "name": &self.config.collection })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "inspect existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?;
        let spec = collections
            .try_next()
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "read existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB reported NamespaceExists for collection '{}', but its metadata was not returned",
                    self.config.collection
                ))
            })?;

        validate_existing_timeseries_spec(&spec, expected)
    }

    /// Writes JSON value documents to `MongoDB` using the configured write mode.
    ///
    /// Accepts `serde_json::Value` directly (no intermediate string round-trip).
    /// Insert/upsert from documents already in BSON (no JSON hop).
    pub(super) async fn write_bson_docs(
        &self,
        docs: Vec<mongodb::bson::Document>,
        from_changelog: bool,
        encoded_bytes: u64,
    ) -> Result<(), ConnectorError> {
        use mongodb::bson::Document;
        use mongodb::options::{DeleteOneModel, InsertOneModel, ReplaceOneModel, WriteModel};

        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;
        let namespace = collection.namespace();
        let mut models = Vec::with_capacity(docs.len());
        let mut counts = BulkCounts::default();
        let mut model_bytes = encoded_bytes;

        match &self.config.write_mode {
            WriteMode::Upsert { key_fields } => {
                for document in &docs {
                    for key in key_fields {
                        match document.get(key) {
                            Some(value) if *value != mongodb::bson::Bson::Null => {}
                            _ => {
                                return Err(ConnectorError::ConfigurationError(format!(
                                    "MongoDB upsert document requires a non-null key field '{key}'"
                                )));
                            }
                        }
                    }
                }
            }
            WriteMode::Insert | WriteMode::CdcReplay => {}
        }

        match &self.config.write_mode {
            WriteMode::Insert => {
                counts.inserts = docs.len() as u64;
                models.extend(docs.into_iter().map(|document| {
                    WriteModel::InsertOne(
                        InsertOneModel::builder()
                            .namespace(namespace.clone())
                            .document(document)
                            .build(),
                    )
                }));
            }

            WriteMode::Upsert { ref key_fields } => {
                for mut bson_doc in docs {
                    // Only a collapsed changelog carries a synthesized `_op` (U/D): route D to a
                    // delete. A plain upsert keeps its columns verbatim (a user `_op` is not a delete).
                    let is_delete = from_changelog && matches!(bson_doc.get_str("_op"), Ok("D"));
                    if from_changelog {
                        bson_doc.remove("_op");
                    }
                    let mut filter = Document::new();
                    for key in key_fields {
                        let value = bson_doc.get(key).ok_or_else(|| {
                            ConnectorError::ConfigurationError(format!(
                                "MongoDB upsert document is missing key field '{key}'"
                            ))
                        })?;
                        filter.insert(key, value.clone());
                    }
                    let filter_bytes = encoded_document_size(
                        &filter,
                        MAX_STANDARD_DOCUMENT_BYTES,
                        "MongoDB upsert filter",
                    )?;
                    model_bytes = checked_converted_total(
                        model_bytes,
                        filter_bytes,
                        usize::MAX,
                        "MongoDB sink bulk",
                    )?;
                    if is_delete {
                        models.push(WriteModel::DeleteOne(
                            DeleteOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(filter)
                                .build(),
                        ));
                        counts.deletes = counts.deletes.saturating_add(1);
                    } else {
                        models.push(WriteModel::ReplaceOne(
                            ReplaceOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(filter)
                                .replacement(bson_doc)
                                .upsert(true)
                                .build(),
                        ));
                        counts.upserts = counts.upserts.saturating_add(1);
                    }
                    ensure_working_set(
                        0,
                        model_bytes,
                        models.len(),
                        0,
                        MAX_SINK_WORKING_SET_BYTES,
                        "MongoDB sink bulk",
                    )?;
                }
            }

            WriteMode::CdcReplay => {
                return Err(ConnectorError::Internal(
                    "CDC replay must use prepared CDC writes".to_string(),
                ));
            }
        }

        self.execute_bulk_models(models, counts, "MongoDB sink bulk_write")
            .await?;
        Ok(())
    }

    pub(super) async fn execute_bulk_models(
        &self,
        models: Vec<mongodb::options::WriteModel>,
        counts: BulkCounts,
        context: &str,
    ) -> Result<(), ConnectorError> {
        if models.is_empty() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("client not initialized".to_string()))?;
        let driver_timeout = self.driver_timeout();
        let deadline = tokio::time::Instant::now() + driver_timeout;
        let operation_client = client.clone();
        match await_mongo_sink_operation(&self.task_owner, deadline, async move {
            operation_client.bulk_write(models).ordered(true).await
        })
        .await?
        {
            MongoOperationOutcome::Completed(Ok(_)) => {}
            MongoOperationOutcome::Completed(Err(error)) => {
                self.metrics.record_error();
                return Err(classify_mongo_bulk_failure(
                    context,
                    MongoBulkFailure::Driver(&error),
                ));
            }
            MongoOperationOutcome::Deadline => {
                self.metrics.record_error();
                return Err(classify_mongo_bulk_failure(
                    context,
                    MongoBulkFailure::Deadline(driver_timeout),
                ));
            }
            MongoOperationOutcome::TaskFailed(error) => {
                self.metrics.record_error();
                return Err(ConnectorError::outcome_unknown(
                    format!(
                        "{context} task terminated before its MongoDB outcome was observed: {error}"
                    ),
                    false,
                ));
            }
        }
        self.metrics.record_bulk_write();
        self.metrics.record_inserts(counts.inserts);
        self.metrics.record_upserts(counts.upserts);
        self.metrics.record_deletes(counts.deletes);
        Ok(())
    }
}
