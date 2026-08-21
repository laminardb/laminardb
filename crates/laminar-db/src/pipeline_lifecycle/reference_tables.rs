use super::{
    create_reference_table_sources, hydrate_reference_table_sources,
    physical_recovered_input_channel_progress, recovered_source_watermark,
    restore_source_watermark_state, validate_recovered_input_channels,
    validate_recovered_source_watermark, Arc, DbError, FxHashMap, HashMap, LaminarDB,
    PipelineWatermarks, RecoveredInputChannelProgress, SourceContract, SourceRowPositionCapability,
    SourceWatermarkState, TrackedSourceRegistration,
};

impl LaminarDB {
    pub(super) async fn initialize_reference_tables(
        &self,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        restored_reference_tables: bool,
    ) -> Result<(), DbError> {
        use crate::connector_manager::build_table_config;
        let table_sources = create_reference_table_sources(
            &self.connector_registry,
            table_regs,
            &self.table_store,
            restored_reference_tables,
        )
        .await?;
        let mut tables_to_publish = if restored_reference_tables {
            self.table_store.read().table_names()
        } else {
            hydrate_reference_table_sources(table_sources, &self.table_store).await?
        };

        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                // Recovery and Prepared-manifest reconciliation completed before epoch admission.
                coord.reconcile_sink_open_witness().await?;
                coord.begin_initial_epoch().await?;
            }
        }

        tables_to_publish.sort_unstable();
        tables_to_publish.dedup();
        for name in tables_to_publish {
            self.sync_table_to_datafusion(&name)?;
        }

        for (name, reg) in table_regs {
            if !reg.on_demand {
                continue;
            }
            let capacity_bytes = reg.cache_max_bytes.unwrap_or(64 * 1024 * 1024);
            let schema = self.table_store.read().table_schema(name).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "On-demand lookup table '{name}' has no registered schema"
                ))
            })?;
            let pk_csv = &reg.primary_key;
            let pk_cols: Vec<String> = pk_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let key_sort_fields: Vec<arrow::row::SortField> = pk_cols
                .iter()
                .map(|col| {
                    schema
                        .field_with_name(col)
                        .map(|f| arrow::row::SortField::new(f.data_type().clone()))
                        .map_err(|error| {
                            DbError::Pipeline(format!(
                                "On-demand lookup table '{name}' has invalid key column \
                                 '{col}': {error}"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;

            let cache = Arc::new(laminar_core::lookup::lookup_cache::LookupMemoryCache::new(
                0,
                laminar_core::lookup::lookup_cache::LookupMemoryCacheConfig {
                    capacity_bytes,
                    ttl: reg.cache_ttl,
                },
            ));
            let mut config = build_table_config(reg)?;
            config.set("_primary_key_columns", pk_csv.as_str());
            let lookup_source = match self
                .connector_registry
                .create_lookup_source(config, Some(Arc::clone(&schema)))
                .await
            {
                Some(Ok(source)) => source,
                Some(Err(error)) => {
                    return Err(DbError::Connector(format!(
                        "Cannot create on-demand lookup source '{name}': {error}"
                    )));
                }
                None => {
                    return Err(DbError::Connector(format!(
                        "On-demand lookup source factory for '{name}' disappeared after DDL admission"
                    )));
                }
            };

            let projection = crate::sql_analysis::compute_lookup_projection(
                &schema,
                &pk_cols,
                name.as_str(),
                stream_regs.values().map(|r| r.query_sql.as_str()),
            );

            self.lookup_registry.register_partial(
                name,
                laminar_sql::datafusion::PartialLookupState {
                    lookup_cache: cache,
                    schema,
                    key_columns: pk_cols,
                    key_sort_fields,
                    source: Some(lookup_source),
                    fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                    projection,
                },
            );
            tracing::info!(
                table = %name,
                capacity_bytes,
                ttl = ?reg.cache_ttl,
                pk = %pk_csv,
                "registered on-demand lookup table (partial cache)"
            );
        }

        Ok(())
    }
    pub(super) fn prepare_pipeline_watermarks(
        &self,
        sources: &[TrackedSourceRegistration],
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        recovered_channel_progress: &FxHashMap<
            String,
            FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>,
        >,
        recovered_input_channels: &FxHashMap<String, Arc<[Vec<u8>]>>,
        recovered_source_watermarks: &FxHashMap<String, i64>,
        recovered_checkpoint_index_version: Option<u32>,
        recovered_watermark_frontier: Option<i64>,
    ) -> Result<PipelineWatermarks, DbError> {
        let stream_entries: Vec<_> = self
            .catalog
            .list_streams()
            .into_iter()
            .map(|name| {
                if !stream_regs.contains_key(&name) {
                    return Err(DbError::Pipeline(format!(
                        "catalog stream '{name}' has no executable registration"
                    )));
                }
                self.catalog.get_stream_entry(&name).ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "catalog stream '{name}' disappeared during startup"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        let future_skew_ms =
            crate::config::event_time_max_future_skew_ms(self.config.event_time_max_future_skew)
                .map_err(|error| DbError::Config(error.to_string()))?;
        let idle_timeout = self.config.source_idle_timeout;
        let source_contracts: FxHashMap<&str, SourceContract> = sources
            .iter()
            .map(|source| (source.name.as_str(), source.contract()))
            .collect();
        let mut checkpoint_source_names = self.catalog.list_sources();
        checkpoint_source_names.sort_unstable();
        let mut watermark_states: FxHashMap<String, SourceWatermarkState> =
            FxHashMap::with_capacity_and_hasher(
                checkpoint_source_names.len(),
                rustc_hash::FxBuildHasher,
            );
        let mut source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>> =
            FxHashMap::with_capacity_and_hasher(
                checkpoint_source_names.len(),
                rustc_hash::FxBuildHasher,
            );
        let mut source_ids: FxHashMap<String, usize> = FxHashMap::with_capacity_and_hasher(
            checkpoint_source_names.len(),
            rustc_hash::FxBuildHasher,
        );
        for name in checkpoint_source_names.iter().cloned() {
            if let Some(entry) = self.catalog.get_source(&name) {
                let watermark = entry
                    .watermark_column
                    .clone()
                    .zip(entry.max_out_of_orderness)
                    .or_else(|| {
                        entry.source.event_time_column().map(|column| {
                            (
                                column,
                                entry
                                    .source
                                    .max_out_of_orderness()
                                    .unwrap_or(std::time::Duration::ZERO),
                            )
                        })
                    });
                if let Some((column, out_of_orderness)) = watermark {
                    let extractor = laminar_core::time::EventTimeExtractor::from_column(&column)
                        .with_mode(laminar_core::time::ExtractionMode::Max);
                    let processing_time = entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> =
                        if processing_time {
                            Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                        } else {
                            Box::new(
                                laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                                    out_of_orderness,
                                )
                                .with_max_future_skew(future_skew_ms),
                            )
                        };
                    let contract = source_contracts.get(name.as_str()).ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "watermarked source '{name}' has no runtime source contract"
                        ))
                    })?;
                    let mut state = SourceWatermarkState::new(extractor, generator, column);
                    if !processing_time
                        && contract.row_positions
                            == SourceRowPositionCapability::OrderedDeterministic
                    {
                        let recovered = physical_recovered_input_channel_progress(
                            recovered_channel_progress.get(&name),
                        );
                        let recovered_inventory = recovered_input_channels.get(&name).cloned();
                        if recovered_channel_progress.contains_key(&name)
                            || recovered_input_channels.contains_key(&name)
                        {
                            validate_recovered_input_channels(
                                &name,
                                &recovered,
                                recovered_inventory.as_ref(),
                            )?;
                        }
                        state = state.with_input_channels(
                            out_of_orderness,
                            future_skew_ms,
                            idle_timeout,
                            recovered,
                            recovered_inventory,
                        );
                    }
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(name.clone(), state);
                }
                source_entries_for_wm.insert(name, entry);
            }
        }

        let mut tracker = if source_ids.is_empty() {
            None
        } else {
            let mut t = laminar_core::time::WatermarkTracker::new(source_ids.len());
            if let Some(timeout) = idle_timeout {
                for (name, id) in &source_ids {
                    if !watermark_states
                        .get(name)
                        .is_some_and(SourceWatermarkState::is_partitioned)
                    {
                        t.set_idle_timeout(*id, Some(timeout));
                    }
                }
            }
            Some(t)
        };

        // Mixed watermarked/un-watermarked sources: un-watermarked ones inherit
        // the global clock, so joins/windows close on the watermarked source's
        // time. Surface the mismatch rather than silently dropping late rows.
        let registered = self.catalog.list_sources();
        let unwatermarked: Vec<&str> = registered
            .iter()
            .filter(|n| !source_ids.contains_key(*n))
            .map(String::as_str)
            .collect();
        if !source_ids.is_empty() && !unwatermarked.is_empty() {
            tracing::warn!(
                watermarked = source_ids.len(),
                unwatermarked = unwatermarked.len(),
                unwatermarked_names = ?unwatermarked,
                "Pipeline mixes watermarked and un-watermarked sources. An un-watermarked \
                 source in a join/window inherits the global watermark — time-based \
                 operators may behave unexpectedly. Add `WATERMARK FOR` to the missing \
                 sources or split into separate pipelines."
            );
        }

        let mut tracker_watermarks = vec![None; source_ids.len()];
        let mut idle_sources = vec![false; source_ids.len()];
        for (name, &source_id) in &source_ids {
            let owns_empty_inventory = watermark_states
                .get(name)
                .is_some_and(SourceWatermarkState::is_partitioned)
                && recovered_input_channels
                    .get(name)
                    .is_some_and(|inventory| inventory.is_empty());
            let (recovered, idle) = recovered_source_watermark(
                recovered_channel_progress.get(name),
                owns_empty_inventory,
                recovered_source_watermarks.get(name).copied(),
            );
            validate_recovered_source_watermark(
                name,
                recovered,
                idle,
                recovered_source_watermarks.get(name).copied(),
                recovered_checkpoint_index_version,
            )?;
            tracker_watermarks[source_id] = recovered;
            idle_sources[source_id] = idle;
            if let Some(state) = watermark_states.get_mut(name) {
                restore_source_watermark_state(
                    state,
                    recovered,
                    idle,
                    recovered_source_watermarks.get(name).copied(),
                );
            }
        }
        if let Some(tracker) = tracker.as_mut() {
            tracker
                .restore_for_recovery(
                    &tracker_watermarks,
                    &idle_sources,
                    recovered_watermark_frontier,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "failed to restore the committed watermark tracker: {error}"
                    ))
                })?;
        }
        self.pipeline_watermark.store(
            recovered_watermark_frontier.unwrap_or(i64::MIN),
            std::sync::atomic::Ordering::Release,
        );
        tracing::info!(
            sources = tracker_watermarks.len(),
            pipeline_watermark = ?recovered_watermark_frontier,
            idle_sources = idle_sources.iter().filter(|idle| **idle).count(),
            "restored checkpoint watermark state"
        );

        Ok(PipelineWatermarks {
            stream_entries,
            watermark_states,
            source_entries: source_entries_for_wm,
            source_ids,
            source_names: checkpoint_source_names,
            tracker,
        })
    }
}
