use super::{DbError, HashMap, ReferenceTableRuntimeSource};

pub(super) async fn close_reference_table_sources(
    table_sources: &mut [ReferenceTableRuntimeSource],
) -> Result<(), DbError> {
    let mut first_error = None;
    for (name, source) in table_sources {
        if let Err(error) = source.close().await {
            first_error.get_or_insert_with(|| {
                DbError::Connector(format!("Table '{name}' snapshot close error: {error}"))
            });
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub(super) async fn create_reference_table_sources(
    connector_registry: &laminar_connectors::registry::ConnectorRegistry,
    table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
    table_store: &parking_lot::RwLock<crate::table_store::TableStore>,
    restored_complete_inventory: bool,
) -> Result<Vec<ReferenceTableRuntimeSource>, DbError> {
    if restored_complete_inventory {
        return Ok(Vec::new());
    }

    let mut registrations: Vec<_> = table_regs
        .iter()
        .filter(|(_, registration)| {
            registration.connector_type.is_some() && !registration.on_demand
        })
        .collect();
    registrations.sort_unstable_by_key(|(name, _)| *name);

    let mut sources = Vec::with_capacity(registrations.len());
    for (name, registration) in registrations {
        let result = (|| {
            let config = crate::connector_manager::build_table_config(registration)?;
            let schema = table_store.read().table_schema(name).ok_or_else(|| {
                DbError::Pipeline(format!("Reference table '{name}' has no registered schema"))
            })?;
            connector_registry
                .create_table_source(&config, schema)
                .map_err(|error| {
                    DbError::Connector(format!("Cannot create table source '{name}': {error}"))
                })
        })();

        match result {
            Ok(source) => sources.push((name.clone(), source)),
            Err(error) => {
                if let Err(close_error) = close_reference_table_sources(&mut sources).await {
                    tracing::warn!(%close_error, "Failed to close table sources after startup error");
                }
                return Err(error);
            }
        }
    }
    Ok(sources)
}

pub(super) async fn hydrate_reference_table_sources(
    mut table_sources: Vec<ReferenceTableRuntimeSource>,
    table_store: &parking_lot::RwLock<crate::table_store::TableStore>,
) -> Result<Vec<String>, DbError> {
    let mut prepared = Vec::with_capacity(table_sources.len());
    let mut names = Vec::with_capacity(table_sources.len());
    let mut hydration_error = None;

    for (name, source) in &mut table_sources {
        let mut batches = Vec::new();
        loop {
            match source.poll_snapshot().await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(error) => {
                    hydration_error = Some(DbError::Connector(format!(
                        "Table '{name}' snapshot error: {error}"
                    )));
                    break;
                }
            }
        }
        if hydration_error.is_some() {
            break;
        }

        match table_store.read().prepare_snapshot(name, &batches) {
            Ok(snapshot) => {
                prepared.push(snapshot);
                names.push(name.clone());
            }
            Err(error) => {
                hydration_error = Some(DbError::Connector(format!(
                    "Table '{name}' snapshot validation error: {error}"
                )));
                break;
            }
        }
    }

    let close_result = close_reference_table_sources(&mut table_sources).await;
    if let Some(error) = hydration_error {
        if let Err(close_error) = close_result {
            tracing::warn!(%close_error, "Failed to close table sources after snapshot error");
        }
        return Err(error);
    }
    close_result?;

    table_store
        .write()
        .install_prepared_snapshots(prepared)
        .map_err(|error| DbError::Connector(format!("Table snapshot install error: {error}")))?;
    Ok(names)
}
