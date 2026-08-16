//! COPY, UPSERT, DELETE, and table-creation statement construction.

use std::sync::Arc;

use arrow_schema::{Field, SchemaRef};

use crate::error::ConnectorError;

use super::super::sink_config::{quote_sql_identifier, PostgresSinkConfig};
use super::super::types::{
    arrow_to_pg_ddl_type, arrow_type_to_pg_array_cast, arrow_type_to_pg_sql,
};
use super::input::{quoted_user_columns, user_fields, validate_sink_schema};
use super::PostgresSink;

impl PostgresSink {
    /// Builds the COPY BINARY SQL statement.
    ///
    /// ```sql
    /// COPY "public"."events" ("id", "value", "ts") FROM STDIN BINARY
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the schema or sink configuration cannot produce a valid COPY.
    pub fn build_copy_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let columns = quoted_user_columns(schema);
        Ok(format!(
            "COPY {} ({}) FROM STDIN BINARY",
            config.qualified_table_name(),
            columns.join(", "),
        ))
    }

    /// Builds the UNNEST-based upsert SQL statement.
    ///
    /// ```sql
    /// INSERT INTO "public"."target" ("id", "value", "updated_at")
    /// SELECT * FROM UNNEST($1::int8[], $2::text[], $3::timestamptz[])
    /// ON CONFLICT (id) DO UPDATE SET
    ///     value = EXCLUDED.value,
    ///     updated_at = EXCLUDED.updated_at
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the schema or sink configuration cannot produce a valid upsert.
    pub fn build_upsert_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let fields = user_fields(schema);

        let columns: Vec<String> = fields
            .iter()
            .map(|field| quote_sql_identifier(field.name()))
            .collect();

        let unnest_params: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| arrow_type_to_pg_array_cast(field.data_type(), i + 1))
            .collect::<Result<_, _>>()?;

        let non_key_columns: Vec<&Arc<Field>> = fields
            .iter()
            .copied()
            .filter(|field| {
                !config
                    .primary_key_columns
                    .iter()
                    .any(|primary_key| primary_key == field.name())
            })
            .collect();

        let update_clause: Vec<String> = non_key_columns
            .iter()
            .map(|field| {
                let column = quote_sql_identifier(field.name());
                format!("{column} = EXCLUDED.{column}")
            })
            .collect();

        let pk_list = config
            .primary_key_columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        if update_clause.is_empty() {
            // Key-only table: use DO NOTHING
            Ok(format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO NOTHING",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
            ))
        } else {
            Ok(format!(
                "INSERT INTO {} ({}) \
                 SELECT * FROM UNNEST({}) \
                 ON CONFLICT ({}) DO UPDATE SET {}",
                config.qualified_table_name(),
                columns.join(", "),
                unnest_params.join(", "),
                pk_list,
                update_clause.join(", "),
            ))
        }
    }

    /// Builds the DELETE SQL for changelog deletes. One array parameter is bound per primary-key
    /// column (`$1`, `$2`, …), each holding that column's values for the batch's deleted keys.
    ///
    /// ```sql
    /// -- single PK
    /// DELETE FROM "public"."events" WHERE "id" = ANY($1::int8[])
    /// -- composite PK: match tuple-wise via UNNEST, not the cross-product
    /// DELETE FROM "public"."events" AS "target"
    ///   USING UNNEST($1::int8[], $2::text[]) AS "keys"("id", "name")
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the configured primary key cannot produce a valid delete.
    pub fn build_delete_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let pg_type = |column: &str| -> Result<&'static str, ConnectorError> {
            let field = schema.field_with_name(column).map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "primary key column '{column}' is not present in PostgreSQL sink schema"
                ))
            })?;
            arrow_type_to_pg_sql(field.data_type())
        };
        let pk = &config.primary_key_columns;

        // A single PK column can use a plain ANY(); a composite PK must match keys tuple-wise, or
        // `col1 = ANY($1) AND col2 = ANY($2)` deletes the cross-product — e.g. deleting (1,'a') and
        // (2,'b') would also delete (1,'b') and (2,'a') (CN-2). UNNEST zips the arrays positionally.
        if pk.len() <= 1 {
            let column = pk.first().ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "PostgreSQL changelog delete requires a primary key".into(),
                )
            })?;
            let quoted_column = quote_sql_identifier(column);
            Ok(format!(
                "DELETE FROM {} WHERE {quoted_column} = ANY($1::{}[])",
                config.qualified_table_name(),
                pg_type(column)?,
            ))
        } else {
            let unnest_args: Vec<String> = pk
                .iter()
                .enumerate()
                .map(|(i, column)| Ok(format!("${}::{}[]", i + 1, pg_type(column)?)))
                .collect::<Result<_, ConnectorError>>()?;
            let quoted_keys: Vec<String> = pk
                .iter()
                .map(|column| quote_sql_identifier(column))
                .collect();
            let target_alias = quote_sql_identifier("target");
            let key_alias = quote_sql_identifier("keys");
            let match_conditions: Vec<String> = quoted_keys
                .iter()
                .map(|column| format!("{target_alias}.{column} = {key_alias}.{column}"))
                .collect();
            Ok(format!(
                "DELETE FROM {} AS {target_alias} USING UNNEST({}) AS {key_alias}({}) WHERE {}",
                config.qualified_table_name(),
                unnest_args.join(", "),
                quoted_keys.join(", "),
                match_conditions.join(" AND "),
            ))
        }
    }

    /// Builds CREATE TABLE DDL from the Arrow schema.
    ///
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS "public"."events" (
    ///     "id" BIGINT NOT NULL,
    ///     "value" TEXT,
    ///     "ts" TIMESTAMPTZ,
    ///     PRIMARY KEY ("id")
    /// )
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when an Arrow field or identifier cannot be represented in `PostgreSQL`.
    pub fn build_create_table_sql(
        schema: &SchemaRef,
        config: &PostgresSinkConfig,
    ) -> Result<String, ConnectorError> {
        validate_sink_schema(schema, config)?;
        let fields = user_fields(schema);

        let column_defs: Vec<String> = fields
            .iter()
            .map(|field| {
                let pg_type = arrow_to_pg_ddl_type(field.data_type())?;
                let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "    {} {}{}",
                    quote_sql_identifier(field.name()),
                    pg_type,
                    nullable
                ))
            })
            .collect::<Result<_, ConnectorError>>()?;

        let mut ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n",
            config.qualified_table_name(),
            column_defs.join(",\n"),
        );

        if !config.primary_key_columns.is_empty() {
            use std::fmt::Write;
            let primary_keys = config
                .primary_key_columns
                .iter()
                .map(|column| quote_sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(ddl, ",\n    PRIMARY KEY ({primary_keys})\n");
        }

        ddl.push(')');
        Ok(ddl)
    }
}
