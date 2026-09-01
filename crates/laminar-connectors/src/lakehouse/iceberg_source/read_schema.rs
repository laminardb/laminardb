use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Fields, Schema, SchemaRef};
use iceberg::spec::Schema as IcebergSchema;
use quick_cache::sync::Cache;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg::SchemaAlignmentPlan;

const FIELD_ID: &str = parquet::arrow::PARQUET_FIELD_ID_META_KEY;
const EVOLUTION_ERROR: &str = "[LDB-ICEBERG-SCHEMA-EVOLUTION]";
const MAX_CACHED_SCHEMA_PLANS: usize = 64;

#[derive(Clone)]
pub(super) struct ReadSchemaBinding {
    schema_id: i32,
    output_schema: SchemaRef,
    bound_schema: SchemaRef,
    field_ids: Vec<i32>,
    projection_cache: Arc<Cache<i32, Arc<ReadProjection>>>,
}

pub(super) struct ReadProjection {
    pub columns: Vec<String>,
    alignment: SchemaAlignmentPlan,
    output_schema: SchemaRef,
}

impl ReadSchemaBinding {
    pub fn bind(
        root: &IcebergSchema,
        configured_projection: &[String],
        declared_schema: Option<SchemaRef>,
    ) -> Result<Self, ConnectorError> {
        let physical = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(root).map_err(|error| {
                ConnectorError::SchemaMismatch(format!(
                    "{EVOLUTION_ERROR} cannot convert retained Iceberg schema: {error}"
                ))
            })?,
        );
        let output_schema = output_schema(&physical, configured_projection, declared_schema)?;
        let mut seen_names = HashSet::with_capacity(output_schema.fields().len());
        let mut bound_fields = Vec::with_capacity(output_schema.fields().len());
        let mut field_ids = Vec::with_capacity(output_schema.fields().len());
        for declared in output_schema.fields() {
            if !seen_names.insert(declared.name()) {
                return Err(schema_error(format!(
                    "declared output column '{}' is duplicated",
                    declared.name()
                )));
            }
            let physical_field = resolve_physical_field(
                physical.fields(),
                declared,
                &format!("table schema.{}", declared.name()),
            )?;
            field_ids.push(required_field_id(
                physical_field,
                &format!("table schema.{}", physical_field.name()),
            )?);
            bound_fields.push(bind_field_ids(
                declared,
                physical_field,
                &format!("table schema.{}", declared.name()),
            )?);
        }
        if field_ids.is_empty() {
            return Err(schema_error("Iceberg read projection must not be empty"));
        }
        let bound_schema = Arc::new(Schema::new_with_metadata(
            bound_fields,
            output_schema.metadata().clone(),
        ));
        let binding = Self {
            schema_id: root.schema_id(),
            output_schema,
            bound_schema,
            field_ids,
            projection_cache: Arc::new(Cache::new(MAX_CACHED_SCHEMA_PLANS)),
        };
        binding.projection(root)?;
        Ok(binding)
    }

    pub fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    pub fn projection(
        &self,
        snapshot_schema: &IcebergSchema,
    ) -> Result<Arc<ReadProjection>, ConnectorError> {
        if let Some(cached) = self.projection_cache.get(&snapshot_schema.schema_id()) {
            return Ok(cached);
        }
        let physical = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(snapshot_schema).map_err(|error| {
                schema_error(format!(
                    "cannot convert snapshot schema {}: {error}",
                    snapshot_schema.schema_id()
                ))
            })?,
        );
        let mut columns = Vec::with_capacity(self.field_ids.len());
        let mut source_fields = Vec::with_capacity(self.field_ids.len());
        for field_id in &self.field_ids {
            let field = snapshot_schema
                .as_struct()
                .field_by_id(*field_id)
                .ok_or_else(|| {
                    schema_error(format!(
                        "retained field ID {field_id} is absent or no longer top-level in snapshot schema {}",
                        snapshot_schema.schema_id()
                    ))
                })?;
            let index = physical.index_of(&field.name).map_err(|_| {
                schema_error(format!(
                    "retained field ID {field_id} is absent from converted snapshot schema {}",
                    snapshot_schema.schema_id()
                ))
            })?;
            columns.push(field.name.clone());
            source_fields.push(physical.field(index).clone());
        }
        let source_schema = Arc::new(Schema::new(source_fields));
        let alignment = SchemaAlignmentPlan::new_read_projection(
            snapshot_schema.schema_id(),
            source_schema,
            Arc::clone(&self.bound_schema),
        )
        .map_err(|error| {
            schema_error(format!(
                "snapshot schema {} is incompatible with the retained read schema: {error}",
                snapshot_schema.schema_id()
            ))
        })?;
        let projection = Arc::new(ReadProjection {
            columns,
            alignment,
            output_schema: Arc::clone(&self.output_schema),
        });
        self.projection_cache
            .insert(snapshot_schema.schema_id(), Arc::clone(&projection));
        Ok(projection)
    }
}

impl ReadProjection {
    pub fn align(&self, batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
        let aligned = self.alignment.align(batch).map_err(|error| {
            schema_error(format!(
                "snapshot batch violates its retained read schema: {error}"
            ))
        })?;
        let (_, columns, _) = aligned.into_parts();
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            schema_error(format!(
                "cannot restore the declared Iceberg output schema: {error}"
            ))
        })
    }
}

fn output_schema(
    physical: &SchemaRef,
    configured_projection: &[String],
    declared_schema: Option<SchemaRef>,
) -> Result<SchemaRef, ConnectorError> {
    if let Some(declared) = declared_schema {
        let declared_names = declared
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        if !configured_projection.is_empty() && configured_projection != declared_names {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg projection must exactly match the declared source columns".into(),
            ));
        }
        return Ok(declared);
    }
    if configured_projection.is_empty() {
        return Ok(Arc::clone(physical));
    }
    let fields = configured_projection
        .iter()
        .map(|name| {
            physical
                .index_of(name)
                .map(|index| physical.field(index).clone())
                .map_err(|_| schema_error(format!("retained schema has no column '{name}'")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn bind_field_ids(
    declared: &FieldRef,
    physical: &FieldRef,
    path: &str,
) -> Result<FieldRef, ConnectorError> {
    let physical_id = required_field_id(physical, path)?;
    if let Some(declared_id) = optional_field_id(declared, path)? {
        if declared_id != physical_id {
            return Err(schema_error(format!(
                "declared field '{path}' carries field ID {declared_id}, expected {physical_id}"
            )));
        }
    }
    let data_type = bind_nested_ids(declared.data_type(), physical.data_type(), path)?;
    let mut metadata = declared.metadata().clone();
    metadata.insert(FIELD_ID.to_string(), physical_id.to_string());
    Ok(Arc::new(
        declared
            .as_ref()
            .clone()
            .with_data_type(data_type)
            .with_metadata(metadata),
    ))
}

fn bind_nested_ids(
    declared: &DataType,
    physical: &DataType,
    path: &str,
) -> Result<DataType, ConnectorError> {
    match (declared, physical) {
        (DataType::Struct(declared), DataType::Struct(physical)) => Ok(DataType::Struct(
            bind_fields(declared, physical, path)?.into(),
        )),
        (DataType::List(declared), DataType::List(physical)) => Ok(DataType::List(bind_field_ids(
            declared,
            physical,
            &format!("{path}.element"),
        )?)),
        (DataType::LargeList(declared), DataType::LargeList(physical)) => Ok(DataType::LargeList(
            bind_field_ids(declared, physical, &format!("{path}.element"))?,
        )),
        (
            DataType::FixedSizeList(declared, declared_size),
            DataType::FixedSizeList(physical, _),
        ) => Ok(DataType::FixedSizeList(
            bind_field_ids(declared, physical, &format!("{path}.element"))?,
            *declared_size,
        )),
        (DataType::Map(declared, declared_ordered), DataType::Map(physical, _)) => {
            Ok(DataType::Map(
                bind_field_ids(declared, physical, &format!("{path}.entries"))?,
                *declared_ordered,
            ))
        }
        _ => Ok(declared.clone()),
    }
}

fn bind_fields(
    declared: &Fields,
    physical: &Fields,
    path: &str,
) -> Result<Vec<FieldRef>, ConnectorError> {
    declared
        .iter()
        .map(|field| {
            let field_path = format!("{path}.{}", field.name());
            let source = resolve_physical_field(physical, field, &field_path)?;
            bind_field_ids(field, source, &field_path)
        })
        .collect()
}

fn resolve_physical_field<'a>(
    physical: &'a Fields,
    declared: &FieldRef,
    path: &str,
) -> Result<&'a FieldRef, ConnectorError> {
    if let Some(field_id) = optional_field_id(declared, path)? {
        for field in physical {
            if optional_field_id(field, path)? == Some(field_id) {
                return Ok(field);
            }
        }
        return Err(schema_error(format!(
            "retained schema has no field ID {field_id}"
        )));
    }
    physical
        .iter()
        .find(|field| field.name() == declared.name())
        .ok_or_else(|| schema_error(format!("retained schema has no field '{path}'")))
}

fn required_field_id(field: &FieldRef, path: &str) -> Result<i32, ConnectorError> {
    optional_field_id(field, path)?.ok_or_else(|| {
        schema_error(format!(
            "physical Iceberg field '{path}' has no field-ID metadata"
        ))
    })
}

fn optional_field_id(field: &FieldRef, path: &str) -> Result<Option<i32>, ConnectorError> {
    field
        .metadata()
        .get(FIELD_ID)
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| {
                    schema_error(format!("field '{path}' has invalid field-ID metadata"))
                })
        })
        .transpose()
}

fn schema_error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::SchemaMismatch(format!("{EVOLUTION_ERROR} {}", message.into()))
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};

    use super::*;

    fn schema(schema_id: i32, id: i32, name: &str, field_type: PrimitiveType) -> Schema {
        Schema::builder()
            .with_schema_id(schema_id)
            .with_fields(vec![NestedField::required(
                id,
                name,
                Type::Primitive(field_type),
            )
            .into()])
            .build()
            .unwrap()
    }

    #[test]
    fn field_id_binding_survives_a_rename() {
        let root = schema(1, 7, "old_name", PrimitiveType::Long);
        let declared = Arc::new(ArrowSchema::new(vec![Field::new(
            "old_name",
            DataType::Int64,
            false,
        )]));
        let binding = ReadSchemaBinding::bind(&root, &[], Some(Arc::clone(&declared))).unwrap();
        let renamed = schema(2, 7, "new_name", PrimitiveType::Long);
        let projection = binding.projection(&renamed).unwrap();
        assert_eq!(projection.columns, ["new_name"]);

        let source_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&renamed).unwrap());
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();
        let aligned = projection.align(&batch).unwrap();
        assert_eq!(aligned.schema(), declared);
        assert_eq!(
            aligned
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn declared_widening_is_stable_across_table_promotion() {
        let root = schema(1, 3, "id", PrimitiveType::Int);
        let declared = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let binding = ReadSchemaBinding::bind(&root, &[], Some(Arc::clone(&declared))).unwrap();
        let projection = binding.projection(&root).unwrap();
        let source_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&root).unwrap());
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![9])) as ArrayRef],
        )
        .unwrap();
        assert_eq!(projection.align(&batch).unwrap().schema(), declared);

        let promoted = schema(2, 3, "id", PrimitiveType::Long);
        assert_eq!(binding.projection(&promoted).unwrap().columns, ["id"]);
    }

    #[test]
    fn replacement_with_the_same_name_fails_closed() {
        let root = schema(1, 3, "id", PrimitiveType::Long);
        let binding = ReadSchemaBinding::bind(&root, &[], None).unwrap();
        let replacement = schema(2, 4, "id", PrimitiveType::Long);
        let error = binding
            .projection(&replacement)
            .err()
            .expect("field replacement must fail");
        assert!(error.to_string().contains("retained field ID 3"));
    }

    #[test]
    fn projection_cache_is_shared_and_bounded_by_schema_id() {
        let root = schema(1, 7, "id", PrimitiveType::Long);
        let binding = ReadSchemaBinding::bind(&root, &[], None).unwrap();
        let first = binding.projection(&root).unwrap();
        let cloned = binding.clone();
        assert!(Arc::ptr_eq(&first, &cloned.projection(&root).unwrap()));

        for schema_id in 2..=i32::try_from(MAX_CACHED_SCHEMA_PLANS + 16).unwrap() {
            let evolved = schema(schema_id, 7, "id", PrimitiveType::Long);
            binding.projection(&evolved).unwrap();
        }
        assert!(binding.projection_cache.len() <= MAX_CACHED_SCHEMA_PLANS);
    }

    #[test]
    fn nested_ids_survive_rename_and_ignore_later_additions() {
        let root = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                1,
                "payload",
                Type::Struct(StructType::new(vec![NestedField::optional(
                    2,
                    "old_name",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])),
            )
            .into()])
            .build()
            .unwrap();
        let binding = ReadSchemaBinding::bind(&root, &[], None).unwrap();
        let evolved = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![NestedField::required(
                1,
                "payload",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(2, "new_name", Type::Primitive(PrimitiveType::Long))
                        .into(),
                    NestedField::optional(3, "later", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
            )
            .into()])
            .build()
            .unwrap();
        let projection = binding.projection(&evolved).unwrap();
        let source_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&evolved).unwrap());
        let DataType::Struct(fields) = source_schema.field(0).data_type() else {
            panic!("test schema must contain a struct");
        };
        let payload = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![5])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("ignored")])) as ArrayRef,
            ],
            None,
        );
        let batch = RecordBatch::try_new(source_schema, vec![Arc::new(payload)]).unwrap();
        let aligned = projection.align(&batch).unwrap();
        let aligned_schema = aligned.schema();
        let DataType::Struct(fields) = aligned_schema.field(0).data_type() else {
            panic!("aligned schema must contain a struct");
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "old_name");
    }

    #[test]
    fn explicit_projection_must_match_the_declared_schema() {
        let root = schema(1, 3, "id", PrimitiveType::Long);
        let declared = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let error = ReadSchemaBinding::bind(&root, &["other".into()], Some(declared))
            .err()
            .expect("conflicting projection must fail");
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    }
}
