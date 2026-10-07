//! Lookup table planning with declared or resolved reader fields.

use super::{
    object_name_to_string, Arc, Field, LookupTableInfo, ObjectName, PlanningError, Schema,
    SchemaRef, StreamingPlan, StreamingPlanner,
};
use crate::parser::lookup_table::validate_properties;
use crate::parser::CreateLookupTableStatement;

impl StreamingPlanner {
    /// Plans a CREATE LOOKUP TABLE statement.
    pub(super) fn plan_create_lookup_table(
        &mut self,
        lt: &CreateLookupTableStatement,
    ) -> Result<StreamingPlan, PlanningError> {
        let name = object_name_to_string(&lt.name);

        if !lt.or_replace && !lt.if_not_exists && self.lookup_tables.contains_key(&name) {
            return Err(PlanningError::InvalidQuery(format!(
                "Lookup table '{}' already exists",
                name
            )));
        }

        let columns: Vec<(String, String)> = lt
            .columns
            .iter()
            .map(|c| (c.name.value.clone(), c.data_type.to_string()))
            .collect();

        let properties = validate_properties(&lt.with_options).map_err(|e| {
            PlanningError::InvalidQuery(format!("Invalid lookup table properties: {e}"))
        })?;

        // Compute Arrow schema from column definitions
        let arrow_fields: Vec<Field> = lt
            .columns
            .iter()
            .map(|c| {
                let dt = crate::translator::streaming_ddl::sql_type_to_arrow(&c.data_type)
                    .map_err(|e| PlanningError::InvalidQuery(e.to_string()))?;
                let nullable = !c
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                Ok(Field::new(&c.name.value, dt, nullable))
            })
            .collect::<Result<_, PlanningError>>()?;
        let arrow_schema = Arc::new(Schema::new(arrow_fields));

        let info = LookupTableInfo {
            name: name.clone(),
            columns,
            primary_key: lt.primary_key.clone(),
            properties,
            arrow_schema,
            raw_options: lt.with_options.clone(),
        };

        self.lookup_tables.insert(name, info.clone());

        Ok(StreamingPlan::RegisterLookupTable(info))
    }

    /// Bind a lookup table to already resolved Arrow fields without a lossy SQL round trip.
    ///
    /// # Errors
    /// Returns the existing property/planner errors, or an empty resolved schema error.
    pub fn plan_lookup_table_with_schema(
        &mut self,
        create: &CreateLookupTableStatement,
        schema: SchemaRef,
    ) -> Result<StreamingPlan, PlanningError> {
        if schema.fields().is_empty() {
            return Err(PlanningError::InvalidQuery(
                "resolved lookup reader is empty".into(),
            ));
        }
        let StreamingPlan::RegisterLookupTable(mut info) = self.plan_create_lookup_table(create)?
        else {
            return Err(PlanningError::InvalidQuery(
                "lookup planner returned a different statement".into(),
            ));
        };
        info.columns = schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().to_string()))
            .collect();
        info.arrow_schema = schema;
        self.lookup_tables.insert(info.name.clone(), info.clone());
        Ok(StreamingPlan::RegisterLookupTable(info))
    }

    /// Plans a DROP LOOKUP TABLE statement.
    pub(super) fn plan_drop_lookup_table(
        &mut self,
        name: &ObjectName,
        if_exists: bool,
    ) -> Result<StreamingPlan, PlanningError> {
        let name_str = object_name_to_string(name);

        if !if_exists && !self.lookup_tables.contains_key(&name_str) {
            return Err(PlanningError::InvalidQuery(format!(
                "Lookup table '{}' does not exist",
                name_str
            )));
        }

        self.lookup_tables.remove(&name_str);

        Ok(StreamingPlan::DropLookupTable { name: name_str })
    }
}
