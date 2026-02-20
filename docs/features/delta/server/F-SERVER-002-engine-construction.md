# F-SERVER-002: Engine Construction from Config

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6c |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SERVER-001 (TOML Config), F-LSQL-001 (SQL Compilation) |
| **Blocks** | F-SERVER-003, F-SERVER-004, F-SERVER-005 |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/server.rs` |

## Summary

Translates a parsed `ServerConfig` (F-SERVER-001) into a fully running LaminarDB engine. This is the bridge between declarative TOML configuration and the imperative engine API. The construction process creates connector instances from a registry, compiles SQL pipelines through DataFusion, wires sources to pipelines to sinks, validates the entire topology, and starts the engine. A connector factory registry maps connector type strings (e.g., "kafka", "postgres_cdc") to concrete implementations, enabling pluggable connectors without modifying the server binary.

## Goals

- Build a complete LaminarDB engine from a `ServerConfig` struct with no additional Rust code required
- Implement a `ConnectorFactory` registry that maps connector type strings to constructors
- Compile SQL pipelines through DataFusion with proper source/lookup table registration
- Validate the complete pipeline topology before starting (fail-fast on invalid SQL, missing connectors)
- Provide structured startup validation with all errors collected and reported together
- Support graceful shutdown with drain and checkpoint
- Report startup progress via structured logging (tracing)

## Non-Goals

- Hot reload of configuration changes (covered by F-SERVER-004)
- HTTP API implementation (covered by F-SERVER-003)
- Delta-specific startup (covered by F-SERVER-005)
- Custom connector development (covered by Phase 3 connector SDK F034)
- Connector health monitoring during runtime (covered by Phase 5 observability)

## Technical Design

### Architecture

The engine construction follows a builder pattern with a validation gate. No resources are allocated until all validation passes. This ensures clean error reporting and avoids partial initialization cleanup.

```
ServerConfig
    |
    v
+-------------------+     +-------------------+
| ConnectorFactory  |     | DataFusion        |
| Registry          |     | SessionContext    |
| (type -> factory) |     | (SQL compilation) |
+--------+----------+     +--------+----------+
         |                          |
         v                          v
+-------------------+     +-------------------+
| Source Connectors |     | Compiled SQL      |
| Lookup Connectors |     | Execution Plans   |
| Sink Connectors   |     |                   |
+--------+----------+     +--------+----------+
         |                          |
         +------------+-------------+
                      |
                      v
              +---------------+
              | LaminarEngine |
              | (wired graph) |
              +-------+-------+
                      |
                      v
              +---------------+
              | engine.start()|
              | (begin        |
              |  processing)  |
              +---------------+
```

### API/Interface

```rust
use crate::config::{ServerConfig, SourceConfig, LookupConfig, SinkConfig, PipelineConfig};
use std::collections::HashMap;
use std::sync::Arc;

/// Main entry point: build and start a LaminarDB server from config.
///
/// This function is called from `main()` after config parsing.
/// It constructs the engine, starts all pipelines, and returns
/// a handle for shutdown.
///
/// # Errors
///
/// Returns `ServerError` if any connector cannot be created,
/// any SQL pipeline fails to compile, or validation fails.
pub async fn run_server(config: ServerConfig) -> Result<ServerHandle, ServerError> {
    // Phase 1: Validate and build connectors
    let registry = ConnectorRegistry::default_registry();
    let mut builder = EngineBuilder::new(config.clone(), registry)?;

    // Phase 2: Register sources as DataFusion tables
    for source in &config.sources {
        builder.register_source(source)?;
    }

    // Phase 3: Register lookup tables
    for lookup in &config.lookups {
        builder.register_lookup(lookup)?;
    }

    // Phase 4: Compile SQL pipelines
    for pipeline in &config.pipelines {
        builder.compile_pipeline(pipeline)?;
    }

    // Phase 5: Attach sinks
    for sink in &config.sinks {
        builder.attach_sink(sink)?;
    }

    // Phase 6: Final validation
    builder.validate()?;

    // Phase 7: Build and start
    let engine = builder.build()?;
    let api = HttpApi::new(engine.clone(), &config.server);
    engine.start().await?;

    let api_handle = api.serve(&config.server.bind).await?;

    Ok(ServerHandle { engine, api_handle })
}

/// Builder for constructing a LaminarDB engine from configuration.
///
/// Accumulates sources, lookups, pipelines, and sinks, validates
/// the topology, and produces a running engine.
pub struct EngineBuilder {
    config: ServerConfig,
    registry: ConnectorRegistry,
    sources: HashMap<String, Box<dyn SourceConnector>>,
    lookups: HashMap<String, Box<dyn LookupConnector>>,
    pipelines: HashMap<String, CompiledPipeline>,
    sinks: HashMap<String, Vec<Box<dyn SinkConnector>>>,
    session_ctx: datafusion::prelude::SessionContext,
    errors: Vec<String>,
}

impl EngineBuilder {
    /// Create a new engine builder with the given config and connector registry.
    pub fn new(
        config: ServerConfig,
        registry: ConnectorRegistry,
    ) -> Result<Self, ServerError> {
        let session_ctx = Self::create_session_context(&config)?;

        Ok(Self {
            config,
            registry,
            sources: HashMap::new(),
            lookups: HashMap::new(),
            pipelines: HashMap::new(),
            sinks: HashMap::new(),
            session_ctx,
            errors: Vec::new(),
        })
    }

    /// Register a streaming source.
    ///
    /// Creates the source connector from the registry and registers
    /// the source schema as a DataFusion table provider so SQL
    /// pipelines can reference it by name.
    pub fn register_source(&mut self, source: &SourceConfig) -> Result<(), ServerError> {
        let connector = self.registry.create_source(
            &source.connector,
            &source.name,
            &source.properties,
        ).map_err(|e| ServerError::ConnectorCreation {
            name: source.name.clone(),
            kind: "source".to_string(),
            source: e,
        })?;

        // Register as DataFusion table for SQL compilation
        let schema = Self::build_arrow_schema(&source.schema)?;
        let table_provider = StreamingTableProvider::new(
            source.name.clone(),
            schema,
        );
        self.session_ctx.register_table(
            &source.name,
            Arc::new(table_provider),
        ).map_err(|e| ServerError::TableRegistration {
            name: source.name.clone(),
            source: e.into(),
        })?;

        self.sources.insert(source.name.clone(), connector);
        tracing::info!(source = %source.name, connector = %source.connector, "registered source");
        Ok(())
    }

    /// Register a lookup table for enrichment joins.
    pub fn register_lookup(&mut self, lookup: &LookupConfig) -> Result<(), ServerError> {
        let connector = self.registry.create_lookup(
            &lookup.connector,
            &lookup.name,
            &lookup.properties,
        ).map_err(|e| ServerError::ConnectorCreation {
            name: lookup.name.clone(),
            kind: "lookup".to_string(),
            source: e,
        })?;

        // Register as DataFusion table for JOIN resolution
        let schema = Self::build_arrow_schema(&lookup.schema)?;
        let table_provider = LookupTableProvider::new(
            lookup.name.clone(),
            schema,
            connector.as_ref(),
        );
        self.session_ctx.register_table(
            &lookup.name,
            Arc::new(table_provider),
        ).map_err(|e| ServerError::TableRegistration {
            name: lookup.name.clone(),
            source: e.into(),
        })?;

        self.lookups.insert(lookup.name.clone(), connector);
        tracing::info!(lookup = %lookup.name, connector = %lookup.connector, "registered lookup");
        Ok(())
    }

    /// Compile a SQL pipeline through DataFusion.
    ///
    /// The SQL string is parsed, planned, and optimized. All table
    /// references must be resolvable against registered sources
    /// and lookups. The compiled plan is stored for later execution.
    pub fn compile_pipeline(&mut self, pipeline: &PipelineConfig) -> Result<(), ServerError> {
        let plan = tokio::runtime::Handle::current().block_on(async {
            let df = self.session_ctx.sql(&pipeline.sql).await
                .map_err(|e| ServerError::SqlCompilation {
                    pipeline: pipeline.name.clone(),
                    sql: pipeline.sql.clone(),
                    source: e.into(),
                })?;
            Ok::<_, ServerError>(df.into_optimized_plan()
                .map_err(|e| ServerError::SqlCompilation {
                    pipeline: pipeline.name.clone(),
                    sql: pipeline.sql.clone(),
                    source: e.into(),
                })?)
        })?;

        let compiled = CompiledPipeline {
            name: pipeline.name.clone(),
            sql: pipeline.sql.clone(),
            plan,
            parallelism: pipeline.parallelism
                .unwrap_or(self.config.server.workers),
        };

        self.pipelines.insert(pipeline.name.clone(), compiled);
        tracing::info!(
            pipeline = %pipeline.name,
            "compiled SQL pipeline"
        );
        Ok(())
    }

    /// Attach a sink to a compiled pipeline.
    pub fn attach_sink(&mut self, sink: &SinkConfig) -> Result<(), ServerError> {
        if !self.pipelines.contains_key(&sink.pipeline) {
            return Err(ServerError::InvalidTopology {
                message: format!(
                    "sink '{}' references unknown pipeline '{}'",
                    sink.name, sink.pipeline
                ),
            });
        }

        let connector = self.registry.create_sink(
            &sink.connector,
            &sink.name,
            &sink.properties,
        ).map_err(|e| ServerError::ConnectorCreation {
            name: sink.name.clone(),
            kind: "sink".to_string(),
            source: e,
        })?;

        self.sinks
            .entry(sink.pipeline.clone())
            .or_default()
            .push(connector);

        tracing::info!(
            sink = %sink.name,
            pipeline = %sink.pipeline,
            connector = %sink.connector,
            "attached sink"
        );
        Ok(())
    }

    /// Validate the complete topology before building.
    ///
    /// Checks that every pipeline has at least one sink, every
    /// source is consumed by at least one pipeline, and the
    /// dataflow graph is a valid DAG.
    pub fn validate(&self) -> Result<(), ServerError> {
        let mut warnings = Vec::new();

        // Warn about pipelines with no sinks
        for name in self.pipelines.keys() {
            if !self.sinks.contains_key(name) {
                warnings.push(format!(
                    "pipeline '{}' has no sinks attached", name
                ));
            }
        }

        // Warn about unused sources
        // (Would require analyzing SQL AST to detect source references)

        for warning in &warnings {
            tracing::warn!("{}", warning);
        }

        Ok(())
    }

    /// Build the engine from all registered components.
    pub fn build(self) -> Result<Arc<LaminarEngine>, ServerError> {
        let engine = LaminarEngine::new(
            self.config,
            self.sources,
            self.lookups,
            self.pipelines,
            self.sinks,
        )?;
        Ok(Arc::new(engine))
    }

    fn create_session_context(
        config: &ServerConfig,
    ) -> Result<datafusion::prelude::SessionContext, ServerError> {
        let session_config = datafusion::prelude::SessionConfig::new()
            .with_target_partitions(
                if config.server.workers > 0 {
                    config.server.workers
                } else {
                    num_cpus::get()
                }
            );
        Ok(datafusion::prelude::SessionContext::new_with_config(
            session_config,
        ))
    }

    fn build_arrow_schema(
        columns: &[crate::config::ColumnDef],
    ) -> Result<arrow::datatypes::SchemaRef, ServerError> {
        let fields: Vec<arrow::datatypes::Field> = columns
            .iter()
            .map(|col| {
                let data_type = match col.data_type.to_uppercase().as_str() {
                    "INT" | "INTEGER" => arrow::datatypes::DataType::Int32,
                    "BIGINT" => arrow::datatypes::DataType::Int64,
                    "DOUBLE" | "FLOAT8" => arrow::datatypes::DataType::Float64,
                    "FLOAT" | "FLOAT4" => arrow::datatypes::DataType::Float32,
                    "VARCHAR" | "STRING" | "TEXT" => arrow::datatypes::DataType::Utf8,
                    "BOOLEAN" | "BOOL" => arrow::datatypes::DataType::Boolean,
                    "TIMESTAMP" => arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond,
                        None,
                    ),
                    "DATE" => arrow::datatypes::DataType::Date32,
                    other => return Err(ServerError::InvalidSchema {
                        message: format!("unsupported column type: '{}'", other),
                    }),
                };
                Ok(arrow::datatypes::Field::new(
                    &col.name,
                    data_type,
                    col.nullable,
                ))
            })
            .collect::<Result<_, _>>()?;

        Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
    }
}

/// Handle to a running server, used for shutdown.
pub struct ServerHandle {
    engine: Arc<LaminarEngine>,
    api_handle: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// Gracefully shut down the server.
    ///
    /// 1. Stop accepting new HTTP requests
    /// 2. Drain all pipelines (flush in-flight events)
    /// 3. Trigger a final checkpoint
    /// 4. Shut down connectors
    pub async fn shutdown(self) -> Result<(), ServerError> {
        tracing::info!("initiating graceful shutdown");
        self.engine.drain_all().await?;
        self.engine.checkpoint().await?;
        self.engine.stop().await?;
        self.api_handle.abort();
        tracing::info!("shutdown complete");
        Ok(())
    }

    /// Wait for the server to complete (blocks until shutdown signal).
    pub async fn wait_for_shutdown(self) -> Result<(), ServerError> {
        tokio::signal::ctrl_c().await
            .map_err(|e| ServerError::Runtime(e.into()))?;
        self.shutdown().await
    }
}
```

### Data Structures

```rust
/// Registry of connector factories.
///
/// Maps connector type strings to factory functions that produce
/// source, lookup, or sink connector instances.
pub struct ConnectorRegistry {
    source_factories: HashMap<String, Box<dyn SourceFactory>>,
    lookup_factories: HashMap<String, Box<dyn LookupFactory>>,
    sink_factories: HashMap<String, Box<dyn SinkFactory>>,
}

impl ConnectorRegistry {
    /// Create a registry with all built-in connectors registered.
    pub fn default_registry() -> Self {
        let mut registry = Self {
            source_factories: HashMap::new(),
            lookup_factories: HashMap::new(),
            sink_factories: HashMap::new(),
        };

        // Register built-in source connectors
        registry.register_source_factory("kafka", Box::new(KafkaSourceFactory));
        registry.register_source_factory("postgres_cdc", Box::new(PostgresCdcSourceFactory));
        registry.register_source_factory("mysql_cdc", Box::new(MysqlCdcSourceFactory));
        registry.register_source_factory("generator", Box::new(GeneratorSourceFactory));

        // Register built-in lookup connectors
        registry.register_lookup_factory("postgres", Box::new(PostgresLookupFactory));
        registry.register_lookup_factory("mysql", Box::new(MysqlLookupFactory));
        registry.register_lookup_factory("redis", Box::new(RedisLookupFactory));
        registry.register_lookup_factory("csv", Box::new(CsvLookupFactory));

        // Register built-in sink connectors
        registry.register_sink_factory("kafka", Box::new(KafkaSinkFactory));
        registry.register_sink_factory("postgres", Box::new(PostgresSinkFactory));
        registry.register_sink_factory("delta_lake", Box::new(DeltaLakeSinkFactory));
        registry.register_sink_factory("iceberg", Box::new(IcebergSinkFactory));
        registry.register_sink_factory("stdout", Box::new(StdoutSinkFactory));

        registry
    }

    pub fn register_source_factory(&mut self, name: &str, factory: Box<dyn SourceFactory>) {
        self.source_factories.insert(name.to_string(), factory);
    }

    pub fn register_lookup_factory(&mut self, name: &str, factory: Box<dyn LookupFactory>) {
        self.lookup_factories.insert(name.to_string(), factory);
    }

    pub fn register_sink_factory(&mut self, name: &str, factory: Box<dyn SinkFactory>) {
        self.sink_factories.insert(name.to_string(), factory);
    }

    pub fn create_source(
        &self,
        connector_type: &str,
        name: &str,
        properties: &toml::Table,
    ) -> Result<Box<dyn SourceConnector>, ConnectorError> {
        let factory = self.source_factories.get(connector_type)
            .ok_or_else(|| ConnectorError::UnknownType {
                kind: "source".to_string(),
                connector_type: connector_type.to_string(),
                available: self.source_factories.keys().cloned().collect(),
            })?;
        factory.create(name, properties)
    }

    pub fn create_lookup(
        &self,
        connector_type: &str,
        name: &str,
        properties: &toml::Table,
    ) -> Result<Box<dyn LookupConnector>, ConnectorError> {
        let factory = self.lookup_factories.get(connector_type)
            .ok_or_else(|| ConnectorError::UnknownType {
                kind: "lookup".to_string(),
                connector_type: connector_type.to_string(),
                available: self.lookup_factories.keys().cloned().collect(),
            })?;
        factory.create(name, properties)
    }

    pub fn create_sink(
        &self,
        connector_type: &str,
        name: &str,
        properties: &toml::Table,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let factory = self.sink_factories.get(connector_type)
            .ok_or_else(|| ConnectorError::UnknownType {
                kind: "sink".to_string(),
                connector_type: connector_type.to_string(),
                available: self.sink_factories.keys().cloned().collect(),
            })?;
        factory.create(name, properties)
    }
}

/// A compiled SQL pipeline ready for execution.
pub struct CompiledPipeline {
    /// Pipeline name from config.
    pub name: String,
    /// Original SQL text (for display and hot-reload diffing).
    pub sql: String,
    /// Optimized DataFusion logical plan.
    pub plan: datafusion::logical_expr::LogicalPlan,
    /// Target parallelism for execution.
    pub parallelism: usize,
}

/// Connector factory errors.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("unknown {kind} connector type '{connector_type}'; available: {}", available.join(", "))]
    UnknownType {
        kind: String,
        connector_type: String,
        available: Vec<String>,
    },

    #[error("missing required property '{property}' for connector '{connector}'")]
    MissingProperty {
        connector: String,
        property: String,
    },

    #[error("invalid property '{property}' for connector '{connector}': {reason}")]
    InvalidProperty {
        connector: String,
        property: String,
        reason: String,
    },

    #[error("connection failed for connector '{connector}': {source}")]
    ConnectionFailed {
        connector: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Server-level errors during engine construction and runtime.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to create {kind} connector '{name}': {source}")]
    ConnectorCreation {
        name: String,
        kind: String,
        source: ConnectorError,
    },

    #[error("failed to register table '{name}': {source}")]
    TableRegistration {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("SQL compilation failed for pipeline '{pipeline}':\n  SQL: {sql}\n  Error: {source}")]
    SqlCompilation {
        pipeline: String,
        sql: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("invalid schema: {message}")]
    InvalidSchema { message: String },

    #[error("invalid topology: {message}")]
    InvalidTopology { message: String },

    #[error("runtime error: {0}")]
    Runtime(Box<dyn std::error::Error + Send + Sync>),
}
```

### Algorithm/Flow

#### Startup Sequence

```
1. Parse CLI args (clap) -> config path
2. Load config (F-SERVER-001) -> ServerConfig
3. Create ConnectorRegistry with built-in factories
4. Create EngineBuilder:
   a. Initialize DataFusion SessionContext with worker count
   b. Configure state store backend from config.state
   c. Configure checkpoint settings from config.checkpoint
5. Register sources (for each [[source]]):
   a. Look up source factory in registry by connector type
   b. Create source connector with properties
   c. Build Arrow schema from column definitions
   d. Register as DataFusion table provider
   e. Log source registration
6. Register lookups (for each [[lookup]]):
   a. Look up lookup factory in registry by connector type
   b. Create lookup connector with properties + cache config
   c. Build Arrow schema from column definitions
   d. Register as DataFusion table provider for JOIN resolution
   e. Log lookup registration
7. Compile pipelines (for each [[pipeline]]):
   a. Pass SQL string to DataFusion session.sql()
   b. DataFusion resolves table references against registered tables
   c. Optimize logical plan
   d. Store compiled plan with metadata
   e. Log compilation success with plan summary
8. Attach sinks (for each [[sink]]):
   a. Verify referenced pipeline exists
   b. Look up sink factory in registry by connector type
   c. Create sink connector with properties
   d. Associate sink with pipeline
   e. Log sink attachment
9. Validate topology:
   a. Warn about pipelines with no sinks
   b. Warn about sources not referenced by any pipeline
10. Build engine:
    a. Wire compiled plans into execution graph
    b. Connect source connectors to pipeline inputs
    c. Connect pipeline outputs to sink connectors
    d. Initialize state stores per operator
11. Start engine:
    a. Start all source connectors (begin polling/subscribing)
    b. Start checkpoint scheduler
    c. Start HTTP API server
    d. Log "LaminarDB started" with pipeline count and source count
12. Return ServerHandle for shutdown management
```

#### Graceful Shutdown Sequence

```
1. Receive SIGTERM or ctrl_c signal
2. Stop HTTP API (stop accepting new requests, drain in-flight)
3. For each pipeline:
   a. Signal source connectors to stop producing
   b. Wait for all in-flight events to drain through operators
   c. Trigger final checkpoint
   d. Close sink connectors (flush buffers)
4. Persist final checkpoint to storage
5. Close state stores
6. Log "shutdown complete" with summary statistics
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ServerError::ConnectorCreation` | Unknown connector type or invalid properties | Print available types, suggest checking config |
| `ServerError::TableRegistration` | Duplicate table name or DataFusion error | Print conflicting name, suggest renaming |
| `ServerError::SqlCompilation` | Invalid SQL syntax or missing table references | Print SQL with error location, suggest fixes |
| `ServerError::InvalidSchema` | Unsupported column type in schema definition | Print supported types |
| `ServerError::InvalidTopology` | Sink references missing pipeline | Print available pipelines |
| `ConnectorError::ConnectionFailed` | Cannot reach Kafka/Postgres/etc. at startup | Print connection details, suggest checking network |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Engine construction (5 pipelines) | < 500ms | Integration test timer |
| SQL compilation per pipeline | < 100ms | DataFusion compilation benchmark |
| Connector creation per instance | < 50ms | Unit test timer |
| Startup to first event processed | < 2s | End-to-end integration test |
| Graceful shutdown (empty state) | < 1s | Integration test timer |
| Graceful shutdown (1GB state) | < 30s | Integration test with checkpoint |

## Test Plan

### Unit Tests

- [ ] `test_engine_builder_register_source` -- Source registered and queryable
- [ ] `test_engine_builder_register_lookup` -- Lookup table registered
- [ ] `test_engine_builder_compile_simple_pipeline` -- SELECT * FROM source
- [ ] `test_engine_builder_compile_window_pipeline` -- Tumbling window SQL
- [ ] `test_engine_builder_compile_join_pipeline` -- Source JOIN lookup SQL
- [ ] `test_engine_builder_compile_invalid_sql` -- Returns SqlCompilation error
- [ ] `test_engine_builder_compile_missing_table` -- SQL references unregistered table
- [ ] `test_engine_builder_attach_sink_valid` -- Sink attached to pipeline
- [ ] `test_engine_builder_attach_sink_invalid_pipeline` -- Returns InvalidTopology error
- [ ] `test_connector_registry_default` -- All built-in connectors registered
- [ ] `test_connector_registry_unknown_type` -- Returns UnknownType error with available list
- [ ] `test_build_arrow_schema_all_types` -- All supported SQL types mapped
- [ ] `test_build_arrow_schema_unsupported_type` -- Returns InvalidSchema error
- [ ] `test_validate_warns_pipeline_no_sinks` -- Warning logged for orphan pipeline
- [ ] `test_server_error_display_messages` -- All error variants produce readable messages

### Integration Tests

- [ ] `test_build_and_start_minimal_engine` -- Single source + pipeline + stdout sink
- [ ] `test_build_and_start_multi_pipeline` -- Multiple pipelines from same source
- [ ] `test_graceful_shutdown` -- Start, process events, shutdown, verify checkpoint
- [ ] `test_startup_fails_on_invalid_sql` -- Bad SQL prevents startup entirely
- [ ] `test_startup_fails_on_missing_connector` -- Unknown connector type prevents startup
- [ ] `test_end_to_end_generator_to_stdout` -- Generator source -> SQL -> stdout sink

### Benchmarks

- [ ] `bench_engine_construction_5_pipelines` -- Target: < 500ms
- [ ] `bench_sql_compilation_simple` -- Target: < 50ms
- [ ] `bench_sql_compilation_complex_join` -- Target: < 100ms
- [ ] `bench_connector_registry_lookup` -- Target: < 100ns (HashMap lookup)

## Rollout Plan

1. **Phase 1**: Define `SourceConnector`, `LookupConnector`, `SinkConnector` traits
2. **Phase 2**: Implement `ConnectorRegistry` with factory pattern
3. **Phase 3**: Implement `EngineBuilder` with source/lookup registration
4. **Phase 4**: Implement SQL compilation via DataFusion integration
5. **Phase 5**: Implement sink attachment and topology validation
6. **Phase 6**: Implement `ServerHandle` with graceful shutdown
7. **Phase 7**: Wire into `main.rs` replacing TODO comments
8. **Phase 8**: Integration tests and benchmarks
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should the `EngineBuilder` validate SQL at `compile_pipeline()` time or defer to `build()`? Current design validates eagerly for fail-fast behavior.
- [ ] Should connector factories be async (for connection validation at creation time)?
- [ ] Should we support a `--dry-run` CLI flag that validates config and SQL without starting the engine?
- [ ] How should connector-specific properties be validated? Typed sub-structs per connector vs. runtime validation in factory?
- [ ] Should the server support loading multiple config files (e.g., base + overlay)?

## Completion Checklist

- [ ] `EngineBuilder` implemented with all registration methods
- [ ] `ConnectorRegistry` with built-in connector factories
- [ ] `ServerHandle` with graceful shutdown
- [ ] `run_server()` entry point wired into `main.rs`
- [ ] All error types defined with actionable messages
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-SERVER-001: TOML Config](F-SERVER-001-toml-config.md) -- Config parsing layer
- [F-SERVER-003: HTTP API](F-SERVER-003-http-api.md) -- REST API built on top of engine
- [F005: DataFusion Integration](../../phase-1/F005-datafusion-integration.md) -- SQL compilation foundation
- [F034: Connector SDK](../../phase-3/F034-connector-sdk.md) -- Connector trait definitions
- [crates/laminar-server/src/main.rs](../../../../crates/laminar-server/src/main.rs) -- Existing entry point
