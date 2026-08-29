use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, SchemaRef};

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::is_safe_iceberg_widening;

const FIELD_ID: &str = parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[derive(Debug)]
pub(in crate::lakehouse) struct SchemaAlignmentPlan {
    schema_id: i32,
    source_schema: SchemaRef,
    target_schema: SchemaRef,
    fields: Vec<FieldPlan>,
}

#[derive(Debug)]
struct FieldPlan {
    source_index: Option<usize>,
    target: FieldRef,
    value: ValuePlan,
}

#[derive(Debug)]
enum ValuePlan {
    Null,
    Identity,
    Cast(DataType),
    Struct(Vec<FieldPlan>),
    List {
        field: FieldRef,
        value: Box<ValuePlan>,
    },
    LargeList {
        field: FieldRef,
        value: Box<ValuePlan>,
    },
    FixedSizeList {
        field: FieldRef,
        size: i32,
        value: Box<ValuePlan>,
    },
    Map {
        field: FieldRef,
        ordered: bool,
        entries: Box<ValuePlan>,
    },
}

#[derive(Clone, Copy)]
enum AlignmentUse {
    SinkWrite,
    ReadProjection,
}

impl AlignmentUse {
    const fn allows_missing_nullable_target(self) -> bool {
        matches!(self, Self::SinkWrite)
    }

    const fn allows_unused_source(self) -> bool {
        matches!(self, Self::ReadProjection)
    }
}

impl SchemaAlignmentPlan {
    pub(in crate::lakehouse) fn new(
        schema_id: i32,
        source_schema: SchemaRef,
        target_schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        Self::with_policy(
            schema_id,
            source_schema,
            target_schema,
            AlignmentUse::SinkWrite,
        )
    }

    pub(in crate::lakehouse) fn new_read_projection(
        schema_id: i32,
        source_schema: SchemaRef,
        target_schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        Self::with_policy(
            schema_id,
            source_schema,
            target_schema,
            AlignmentUse::ReadProjection,
        )
    }

    fn with_policy(
        schema_id: i32,
        source_schema: SchemaRef,
        target_schema: SchemaRef,
        alignment_use: AlignmentUse,
    ) -> Result<Self, ConnectorError> {
        if schema_id < 0 {
            return Err(ConnectorError::SchemaMismatch(
                "Iceberg alignment schema ID must be non-negative".into(),
            ));
        }
        let fields = build_field_plans(
            source_schema.fields(),
            target_schema.fields(),
            "table schema",
            alignment_use,
        )?;
        Ok(Self {
            schema_id,
            source_schema,
            target_schema,
            fields,
        })
    }

    pub(in crate::lakehouse) fn align(
        &self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, ConnectorError> {
        if batch.schema().as_ref() != self.source_schema.as_ref() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "input schema changed after Iceberg alignment plan {} was bound",
                self.schema_id
            )));
        }
        let columns = self
            .fields
            .iter()
            .map(|plan| align_field(plan, batch.columns(), batch.num_rows()))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(Arc::clone(&self.target_schema), columns).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("build Iceberg-aligned batch: {error}"))
        })
    }
}

fn build_field_plans(
    source: &Fields,
    target: &Fields,
    path: &str,
    alignment_use: AlignmentUse,
) -> Result<Vec<FieldPlan>, ConnectorError> {
    let source_ids = indexed_field_ids(source, path)?;
    let _ = indexed_field_ids(target, path)?;
    let mut used = HashSet::with_capacity(source.len());
    let mut plans = Vec::with_capacity(target.len());
    for target_field in target {
        let field_path = format!("{path}.{}", target_field.name());
        let source_index = source_index(source, &source_ids, target_field, &field_path)?;
        if let Some(index) = source_index {
            if !used.insert(index) {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "input field '{}' is bound more than once in {path}",
                    source[index].name()
                )));
            }
        }
        let value = match source_index {
            Some(index) => {
                let source_field = &source[index];
                if !target_field.is_nullable() && source_field.is_nullable() {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "nullable input field '{field_path}' cannot bind required Iceberg field"
                    )));
                }
                build_value_plan(source_field, target_field, &field_path, alignment_use)?
            }
            None if alignment_use.allows_missing_nullable_target()
                && target_field.is_nullable() =>
            {
                ValuePlan::Null
            }
            None => {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "Iceberg field '{field_path}' is absent from the input schema"
                )));
            }
        };
        plans.push(FieldPlan {
            source_index,
            target: Arc::clone(target_field),
            value,
        });
    }
    if !alignment_use.allows_unused_source() {
        if let Some((_, field)) = source
            .iter()
            .enumerate()
            .find(|(index, _)| !used.contains(index))
        {
            return Err(ConnectorError::SchemaMismatch(format!(
                "input field '{path}.{}' has no Iceberg field-ID binding",
                field.name()
            )));
        }
    }
    Ok(plans)
}

fn indexed_field_ids(fields: &Fields, path: &str) -> Result<HashMap<i32, usize>, ConnectorError> {
    let mut ids = HashMap::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let Some(id) = field_id(field, path)? else {
            continue;
        };
        if ids.insert(id, index).is_some() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "duplicate Iceberg field ID {id} in {path}"
            )));
        }
    }
    Ok(ids)
}

fn source_index(
    source: &Fields,
    source_ids: &HashMap<i32, usize>,
    target: &Field,
    path: &str,
) -> Result<Option<usize>, ConnectorError> {
    let target_id = field_id(target, path)?;
    if let Some(index) = target_id.and_then(|id| source_ids.get(&id).copied()) {
        return Ok(Some(index));
    }
    let by_name = source
        .iter()
        .position(|field| field.name() == target.name());
    let Some(index) = by_name else {
        return Ok(None);
    };
    let source_id = field_id(&source[index], path)?;
    if target_id.is_some() && source_id.is_some() && target_id != source_id {
        return Err(ConnectorError::SchemaMismatch(format!(
            "input field '{path}' carries Iceberg field ID {}, expected {}",
            source_id.unwrap_or_default(),
            target_id.unwrap_or_default()
        )));
    }
    Ok(Some(index))
}

fn field_id(field: &Field, path: &str) -> Result<Option<i32>, ConnectorError> {
    field
        .metadata()
        .get(FIELD_ID)
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| {
                    ConnectorError::SchemaMismatch(format!(
                        "field '{path}' carries invalid Iceberg field ID '{value}'"
                    ))
                })
        })
        .transpose()
}

fn build_value_plan(
    source: &Field,
    target: &Field,
    path: &str,
    alignment_use: AlignmentUse,
) -> Result<ValuePlan, ConnectorError> {
    let source_id = field_id(source, path)?;
    let target_id = field_id(target, path)?;
    if source_id.is_some() && target_id.is_some() && source_id != target_id {
        return Err(ConnectorError::SchemaMismatch(format!(
            "input field '{path}' carries Iceberg field ID {}, expected {}",
            source_id.unwrap_or_default(),
            target_id.unwrap_or_default()
        )));
    }
    if !target.is_nullable() && source.is_nullable() {
        return Err(ConnectorError::SchemaMismatch(format!(
            "nullable input field '{path}' cannot bind required Iceberg field"
        )));
    }
    if source.data_type() == target.data_type() {
        return Ok(ValuePlan::Identity);
    }
    match (source.data_type(), target.data_type()) {
        (DataType::Struct(source), DataType::Struct(target)) => Ok(ValuePlan::Struct(
            build_field_plans(source, target, path, alignment_use)?,
        )),
        (DataType::List(source), DataType::List(target)) => Ok(ValuePlan::List {
            field: Arc::clone(target),
            value: Box::new(build_value_plan(source, target, path, alignment_use)?),
        }),
        (DataType::LargeList(source), DataType::LargeList(target)) => Ok(ValuePlan::LargeList {
            field: Arc::clone(target),
            value: Box::new(build_value_plan(source, target, path, alignment_use)?),
        }),
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) if source_size == target_size => Ok(ValuePlan::FixedSizeList {
            field: Arc::clone(target),
            size: *target_size,
            value: Box::new(build_value_plan(source, target, path, alignment_use)?),
        }),
        (DataType::Map(source, source_ordered), DataType::Map(target, target_ordered))
            if source_ordered == target_ordered =>
        {
            Ok(ValuePlan::Map {
                field: Arc::clone(target),
                ordered: *target_ordered,
                entries: Box::new(build_value_plan(source, target, path, alignment_use)?),
            })
        }
        (from, to) if is_safe_iceberg_widening(from, to) => Ok(ValuePlan::Cast(to.clone())),
        (from, to) => Err(ConnectorError::SchemaMismatch(format!(
            "field '{path}' has incompatible input type {from} for Iceberg type {to}"
        ))),
    }
}

fn align_field(
    plan: &FieldPlan,
    source: &[ArrayRef],
    len: usize,
) -> Result<ArrayRef, ConnectorError> {
    let array = plan.source_index.map(|index| &source[index]);
    align_value(array, &plan.target, &plan.value, len)
}

fn align_value(
    source: Option<&ArrayRef>,
    target: &FieldRef,
    plan: &ValuePlan,
    len: usize,
) -> Result<ArrayRef, ConnectorError> {
    match plan {
        ValuePlan::Null => Ok(arrow_array::new_null_array(target.data_type(), len)),
        ValuePlan::Identity => source.cloned().ok_or_else(missing_planned_source),
        ValuePlan::Cast(data_type) => {
            arrow_cast::cast(source.ok_or_else(missing_planned_source)?, data_type).map_err(
                |error| {
                    ConnectorError::SchemaMismatch(format!(
                        "cast Iceberg field '{}': {error}",
                        target.name()
                    ))
                },
            )
        }
        ValuePlan::Struct(fields) => align_struct(source, target, fields),
        ValuePlan::List { field, value } => {
            let source = downcast::<ListArray>(source, target)?;
            let values = align_value(Some(source.values()), field, value, source.values().len())?;
            ListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| nested_array_error(target, &error))
        }
        ValuePlan::LargeList { field, value } => {
            let source = downcast::<LargeListArray>(source, target)?;
            let values = align_value(Some(source.values()), field, value, source.values().len())?;
            LargeListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| nested_array_error(target, &error))
        }
        ValuePlan::FixedSizeList { field, size, value } => {
            let source = downcast::<FixedSizeListArray>(source, target)?;
            let values = align_value(Some(source.values()), field, value, source.values().len())?;
            FixedSizeListArray::try_new(Arc::clone(field), *size, values, source.nulls().cloned())
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|error| nested_array_error(target, &error))
        }
        ValuePlan::Map {
            field,
            ordered,
            entries,
        } => {
            let source = downcast::<MapArray>(source, target)?;
            let source_entries = Arc::new(source.entries().clone()) as ArrayRef;
            let aligned = align_value(
                Some(&source_entries),
                field,
                entries,
                source.entries().len(),
            )?;
            let entries = aligned
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| nested_type_error(target))?
                .clone();
            MapArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                entries,
                source.nulls().cloned(),
                *ordered,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| nested_array_error(target, &error))
        }
    }
}

fn align_struct(
    source: Option<&ArrayRef>,
    target: &FieldRef,
    fields: &[FieldPlan],
) -> Result<ArrayRef, ConnectorError> {
    let source = downcast::<StructArray>(source, target)?;
    let columns = fields
        .iter()
        .map(|plan| align_field(plan, source.columns(), source.len()))
        .collect::<Result<Vec<_>, _>>()?;
    let DataType::Struct(target_fields) = target.data_type() else {
        return Err(nested_type_error(target));
    };
    StructArray::try_new_with_length(
        target_fields.clone(),
        columns,
        source.nulls().cloned(),
        source.len(),
    )
    .map(|array| Arc::new(array) as ArrayRef)
    .map_err(|error| nested_array_error(target, &error))
}

fn downcast<'a, T: 'static>(
    source: Option<&'a ArrayRef>,
    target: &FieldRef,
) -> Result<&'a T, ConnectorError> {
    source
        .ok_or_else(missing_planned_source)?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| nested_type_error(target))
}

fn missing_planned_source() -> ConnectorError {
    ConnectorError::Internal("Iceberg alignment plan lost its source column".into())
}

fn nested_type_error(target: &FieldRef) -> ConnectorError {
    ConnectorError::SchemaMismatch(format!(
        "input array for Iceberg field '{}' does not match its planned nested type",
        target.name()
    ))
}

fn nested_array_error(target: &FieldRef, error: &arrow_schema::ArrowError) -> ConnectorError {
    ConnectorError::SchemaMismatch(format!(
        "rebuild nested Iceberg field '{}': {error}",
        target.name()
    ))
}

#[cfg(test)]
mod tests {
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    fn field(name: &str, data_type: DataType, nullable: bool, id: i32) -> FieldRef {
        Arc::new(
            Field::new(name, data_type, nullable)
                .with_metadata(HashMap::from([(FIELD_ID.to_string(), id.to_string())])),
        )
    }

    #[test]
    fn nested_structs_are_reordered_renamed_and_widened_by_field_id() {
        let source_children: Fields = vec![
            field("old_text", DataType::Utf8, true, 4),
            field("old_number", DataType::Int32, false, 3),
        ]
        .into();
        let target_children: Fields = vec![
            field("number", DataType::Int64, false, 3),
            field("text", DataType::Utf8, true, 4),
        ]
        .into();
        let source_field = field(
            "old_payload",
            DataType::Struct(source_children.clone()),
            false,
            2,
        );
        let target_field = field("payload", DataType::Struct(target_children), false, 2);
        let source_schema = Arc::new(Schema::new(vec![Arc::clone(&source_field)]));
        let target_schema = Arc::new(Schema::new(vec![target_field]));
        let values = StructArray::try_new(
            source_children,
            vec![
                Arc::new(StringArray::from(vec![Some("seven")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ],
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(values) as ArrayRef],
        )
        .unwrap();

        let aligned = SchemaAlignmentPlan::new(9, source_schema, target_schema)
            .unwrap()
            .align(&batch)
            .unwrap();
        let payload = aligned
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(payload.fields()[0].name(), "number");
        assert_eq!(payload.fields()[1].name(), "text");
        assert_eq!(
            payload
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            payload
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "seven"
        );
    }

    #[test]
    fn list_elements_are_recursively_aligned() {
        let source_children: Fields = vec![
            field("old_text", DataType::Utf8, true, 4),
            field("old_number", DataType::Int32, false, 3),
        ]
        .into();
        let target_children: Fields = vec![
            field("number", DataType::Int64, false, 3),
            field("text", DataType::Utf8, true, 4),
        ]
        .into();
        let source_element = field(
            "element",
            DataType::Struct(source_children.clone()),
            false,
            2,
        );
        let target_element = field("element", DataType::Struct(target_children), false, 2);
        let source_field = field(
            "old_items",
            DataType::List(Arc::clone(&source_element)),
            false,
            1,
        );
        let target_field = field("items", DataType::List(target_element), false, 1);
        let source_schema = Arc::new(Schema::new(vec![source_field]));
        let target_schema = Arc::new(Schema::new(vec![target_field]));
        let elements = StructArray::try_new(
            source_children,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ],
            None,
        )
        .unwrap();
        let offsets =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)])]);
        let list = ListArray::try_new(
            source_element,
            offsets.offsets().clone(),
            Arc::new(elements),
            None,
        )
        .unwrap();
        let batch =
            RecordBatch::try_new(Arc::clone(&source_schema), vec![Arc::new(list) as ArrayRef])
                .unwrap();

        let aligned = SchemaAlignmentPlan::new(3, source_schema, target_schema)
            .unwrap()
            .align(&batch)
            .unwrap();
        let list = aligned
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let elements = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(elements.fields()[0].name(), "number");
        assert_eq!(elements.fields()[1].name(), "text");
        assert_eq!(elements.column(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn conflicting_ids_and_midstream_schema_drift_fail_closed() {
        let source_field = field("id", DataType::Int64, false, 1);
        let target_field = field("id", DataType::Int64, false, 2);
        let source_schema = Arc::new(Schema::new(vec![Arc::clone(&source_field)]));
        let target_schema = Arc::new(Schema::new(vec![target_field]));
        let error =
            SchemaAlignmentPlan::new(4, Arc::clone(&source_schema), Arc::clone(&target_schema))
                .unwrap_err();
        assert!(error.to_string().contains("field ID 1, expected 2"));

        let source_list = field(
            "items",
            DataType::List(field("element", DataType::Int64, false, 5)),
            false,
            3,
        );
        let target_list = field(
            "items",
            DataType::List(field("element", DataType::Int64, false, 6)),
            false,
            3,
        );
        let error = SchemaAlignmentPlan::new(
            4,
            Arc::new(Schema::new(vec![source_list])),
            Arc::new(Schema::new(vec![target_list])),
        )
        .unwrap_err();
        assert!(error.to_string().contains("field ID 5, expected 6"));

        let target_schema = Arc::new(Schema::new(vec![Arc::clone(&source_field)]));
        let plan = SchemaAlignmentPlan::new(4, Arc::clone(&source_schema), target_schema).unwrap();
        assert_eq!(plan.schema_id, 4);
        let drifted = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            drifted,
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        assert!(plan
            .align(&batch)
            .unwrap_err()
            .to_string()
            .contains("schema changed"));
    }
}
