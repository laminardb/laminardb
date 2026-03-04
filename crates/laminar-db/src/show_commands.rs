//! SHOW and DESCRIBE command builders for `LaminarDB`.
//!
//! Reopens `impl LaminarDB` to keep the main `db.rs` focused on dispatch.

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};

use crate::db::LaminarDB;
use crate::error::DbError;

impl LaminarDB {
    /// Build a SHOW CHECKPOINT STATUS metadata result.
    pub(crate) fn build_show_checkpoint_status(&self) -> Result<RecordBatch, DbError> {
        let store = self.checkpoint_store();
        let (latest, list) = match &store {
            Some(s) => (s.load_latest().ok().flatten(), s.list().unwrap_or_default()),
            None => (None, vec![]),
        };

        // Build single-row result with checkpoint metadata.
        let (cp_id, epoch, ts_ms, sources, sinks, is_inc, total_checkpoints) =
            if let Some(ref m) = latest {
                (
                    m.checkpoint_id,
                    m.epoch,
                    m.timestamp_ms,
                    m.source_names.join(", "),
                    m.sink_names.join(", "),
                    m.is_incremental,
                    list.len() as u64,
                )
            } else {
                (0, 0, 0, String::new(), String::new(), false, 0)
            };

        let schema = Arc::new(Schema::new(vec![
            Field::new("checkpoint_id", DataType::UInt64, false),
            Field::new("epoch", DataType::UInt64, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("sources", DataType::Utf8, false),
            Field::new("sinks", DataType::Utf8, false),
            Field::new("is_incremental", DataType::Boolean, false),
            Field::new("total_checkpoints", DataType::UInt64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![cp_id])),
                Arc::new(UInt64Array::from(vec![epoch])),
                Arc::new(UInt64Array::from(vec![ts_ms])),
                Arc::new(StringArray::from(vec![sources.as_str()])),
                Arc::new(StringArray::from(vec![sinks.as_str()])),
                Arc::new(BooleanArray::from(vec![is_inc])),
                Arc::new(UInt64Array::from(vec![total_checkpoints])),
            ],
        )
        .map_err(|e| DbError::Checkpoint(format!("failed to build checkpoint status batch: {e}")))
    }

    /// Build a SHOW MATERIALIZED VIEWS metadata result.
    pub(crate) fn build_show_materialized_views(&self) -> RecordBatch {
        let registry = self.mv_registry.lock();
        let mut names = Vec::new();
        let mut sqls = Vec::new();
        let mut states = Vec::new();
        for view in registry.views() {
            names.push(view.name.clone());
            sqls.push(view.sql.clone());
            states.push(format!("{:?}", view.state));
        }
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let sqls_ref: Vec<&str> = sqls.iter().map(String::as_str).collect();
        let states_ref: Vec<&str> = states.iter().map(String::as_str).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("view_name", DataType::Utf8, false),
            Field::new("sql", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names_ref)),
                Arc::new(StringArray::from(sqls_ref)),
                Arc::new(StringArray::from(states_ref)),
            ],
        )
        .expect("show materialized views: schema matches columns")
    }

    /// Build a SHOW SOURCES metadata result with connector metadata.
    pub(crate) fn build_show_sources(&self) -> RecordBatch {
        let sources = self.sources();
        let mgr = self.connector_manager.lock();
        let regs = mgr.sources();

        let mut names = Vec::with_capacity(sources.len());
        let mut connectors: Vec<Option<&str>> = Vec::with_capacity(sources.len());
        let mut formats: Vec<Option<&str>> = Vec::with_capacity(sources.len());
        let mut watermarks: Vec<Option<&str>> = Vec::with_capacity(sources.len());

        for s in &sources {
            names.push(s.name.as_str());
            if let Some(reg) = regs.get(&s.name) {
                connectors.push(reg.connector_type.as_deref());
                formats.push(reg.format.as_deref());
            } else {
                connectors.push(None);
                formats.push(None);
            }
            watermarks.push(s.watermark_column.as_deref());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("source_name", DataType::Utf8, false),
            Field::new("connector", DataType::Utf8, true),
            Field::new("format", DataType::Utf8, true),
            Field::new("watermark_column", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(connectors)),
                Arc::new(StringArray::from(formats)),
                Arc::new(StringArray::from(watermarks)),
            ],
        )
        .expect("show sources: schema matches columns")
    }

    /// Build a SHOW SINKS metadata result with connector metadata.
    pub(crate) fn build_show_sinks(&self) -> RecordBatch {
        let sinks = self.sinks();
        let mgr = self.connector_manager.lock();
        let regs = mgr.sinks();

        let mut names = Vec::with_capacity(sinks.len());
        let mut inputs: Vec<Option<String>> = Vec::with_capacity(sinks.len());
        let mut connectors: Vec<Option<&str>> = Vec::with_capacity(sinks.len());
        let mut formats: Vec<Option<&str>> = Vec::with_capacity(sinks.len());

        for s in &sinks {
            names.push(s.name.as_str());
            // Input comes from catalog (always registered), connector metadata from ConnectorManager
            let catalog_input = self.catalog.get_sink_input(&s.name);
            if let Some(reg) = regs.get(&s.name) {
                inputs.push(Some(reg.input.clone()));
                connectors.push(reg.connector_type.as_deref());
                formats.push(reg.format.as_deref());
            } else {
                inputs.push(catalog_input);
                connectors.push(None);
                formats.push(None);
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("sink_name", DataType::Utf8, false),
            Field::new("input", DataType::Utf8, true),
            Field::new("connector", DataType::Utf8, true),
            Field::new("format", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(inputs)),
                Arc::new(StringArray::from(connectors)),
                Arc::new(StringArray::from(formats)),
            ],
        )
        .expect("show sinks: schema matches columns")
    }

    /// Build a SHOW QUERIES metadata result.
    pub(crate) fn build_show_queries(&self) -> RecordBatch {
        let queries = self.queries();
        let ids: Vec<u64> = queries.iter().map(|q| q.id).collect();
        let sqls: Vec<&str> = queries.iter().map(|q| q.sql.as_str()).collect();
        let actives: Vec<bool> = queries.iter().map(|q| q.active).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::UInt64, false),
            Field::new("sql", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(sqls)),
                Arc::new(BooleanArray::from(actives)),
            ],
        )
        .expect("show queries: schema matches columns")
    }

    /// Build a SHOW STREAMS metadata result with SQL definitions.
    pub(crate) fn build_show_streams(&self) -> RecordBatch {
        let streams = self.catalog.list_streams();
        let mgr = self.connector_manager.lock();
        let regs = mgr.streams();

        let mut names = Vec::with_capacity(streams.len());
        let mut sqls: Vec<Option<&str>> = Vec::with_capacity(streams.len());

        for name in &streams {
            names.push(name.as_str());
            sqls.push(regs.get(name.as_str()).map(|r| r.query_sql.as_str()));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("stream_name", DataType::Utf8, false),
            Field::new("sql", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(sqls)),
            ],
        )
        .expect("show streams: schema matches columns")
    }

    /// Build a SHOW CREATE SOURCE result.
    pub(crate) fn build_show_create_source(&self, name: &str) -> Result<RecordBatch, DbError> {
        if self.catalog.get_source(name).is_none() {
            return Err(DbError::SourceNotFound(name.to_string()));
        }
        let mgr = self.connector_manager.lock();
        let ddl = mgr
            .get_ddl(name)
            .ok_or_else(|| DbError::InvalidOperation(format!("No stored DDL for source '{name}'")))?
            .to_string();
        drop(mgr);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "create_statement",
            DataType::Utf8,
            false,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![ddl.as_str()]))],
        )
        .expect("show create source: schema matches columns"))
    }

    /// Build a SHOW CREATE SINK result.
    pub(crate) fn build_show_create_sink(&self, name: &str) -> Result<RecordBatch, DbError> {
        if self.catalog.get_sink_input(name).is_none() {
            return Err(DbError::SinkNotFound(name.to_string()));
        }
        let mgr = self.connector_manager.lock();
        let ddl = mgr
            .get_ddl(name)
            .ok_or_else(|| DbError::InvalidOperation(format!("No stored DDL for sink '{name}'")))?
            .to_string();
        drop(mgr);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "create_statement",
            DataType::Utf8,
            false,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![ddl.as_str()]))],
        )
        .expect("show create sink: schema matches columns"))
    }

    /// Build a SHOW TABLES metadata result.
    pub(crate) fn build_show_tables(&self) -> RecordBatch {
        let ts = self.table_store.lock();
        let mut names = Vec::new();
        let mut pks = Vec::new();
        let mut row_counts = Vec::new();
        let mut connectors = Vec::new();

        for name in ts.table_names() {
            let pk = ts.primary_key(&name).unwrap_or("").to_string();
            let count = ts.table_row_count(&name) as u64;
            let conn = ts.connector(&name).unwrap_or("").to_string();

            names.push(name);
            pks.push(pk);
            row_counts.push(count);
            connectors.push(conn);
        }

        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        let pks_ref: Vec<&str> = pks.iter().map(String::as_str).collect();
        let connectors_ref: Vec<&str> = connectors.iter().map(String::as_str).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("primary_key", DataType::Utf8, false),
            Field::new("row_count", DataType::UInt64, false),
            Field::new("connector", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names_ref)),
                Arc::new(StringArray::from(pks_ref)),
                Arc::new(UInt64Array::from(row_counts)),
                Arc::new(StringArray::from(connectors_ref)),
            ],
        )
        .expect("show tables: schema matches columns")
    }

    /// Build a DESCRIBE result.
    pub(crate) fn build_describe(&self, name: &str) -> Result<RecordBatch, DbError> {
        // Try sources first
        let schema = if let Some(s) = self.catalog.describe_source(name) {
            s
        // Then reference/dimension tables
        } else if let Some(s) = self.table_store.lock().table_schema(name) {
            s
        // Then materialized views
        } else if let Some(s) = self
            .mv_registry
            .lock()
            .get(name)
            .map(|mv| mv.schema.clone())
        {
            s
        // Then check sinks (no schema, but confirm existence)
        } else if self.catalog.list_sinks().contains(&name.to_string()) {
            return Err(DbError::InvalidOperation(
                "DESCRIBE is not supported for sinks. Use SHOW SINKS for details.".to_string(),
            ));
        // Then streams (no stored schema yet)
        } else if self.catalog.list_streams().contains(&name.to_string()) {
            return Err(DbError::InvalidOperation(format!(
                "Stream '{name}' exists but schema is only available after pipeline start"
            )));
        } else {
            return Err(DbError::TableNotFound(name.to_string()));
        };

        schema_to_describe_batch(&schema)
    }
}

/// Convert an Arrow schema to a DESCRIBE result `RecordBatch`.
pub(crate) fn schema_to_describe_batch(schema: &Schema) -> Result<RecordBatch, DbError> {
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{}", f.data_type()))
        .collect();
    let col_nullable: Vec<bool> = schema.fields().iter().map(|f| f.is_nullable()).collect();

    let names_ref: Vec<&str> = col_names.iter().map(String::as_str).collect();
    let types_ref: Vec<&str> = col_types.iter().map(String::as_str).collect();

    let result_schema = Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("nullable", DataType::Boolean, false),
    ]));

    RecordBatch::try_new(
        result_schema,
        vec![
            Arc::new(StringArray::from(names_ref)),
            Arc::new(StringArray::from(types_ref)),
            Arc::new(BooleanArray::from(col_nullable)),
        ],
    )
    .map_err(|e| DbError::InvalidOperation(format!("describe metadata: {e}")))
}
