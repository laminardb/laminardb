use super::*;
use std::sync::Arc;

use arrow_schema::{Field, Schema};

#[test]
fn test_inference_config_defaults() {
    let cfg = InferenceConfig::default();
    assert_eq!(cfg.format, "json");
    assert_eq!(cfg.max_samples, 1000);
    assert!((cfg.min_confidence - 0.8).abs() < f64::EPSILON);
    assert!(cfg.type_hints.is_empty());
}

#[test]
fn test_inference_config_builder() {
    let cfg = InferenceConfig::new("csv")
        .with_min_confidence(0.9)
        .with_max_samples(500)
        .with_type_hint("id", DataType::Int32)
        .with_empty_as_null();

    assert_eq!(cfg.format, "csv");
    assert!((cfg.min_confidence - 0.9).abs() < f64::EPSILON);
    assert_eq!(cfg.max_samples, 500);
    assert_eq!(cfg.type_hints.get("id"), Some(&DataType::Int32));
    assert!(cfg.empty_as_null);
}

#[test]
fn test_inferred_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let inferred = InferredSchema {
        schema: schema.clone(),
        confidence: 0.95,
        sample_count: 100,
        field_details: vec![
            FieldInferenceDetail {
                field_name: "id".into(),
                inferred_type: DataType::Int64,
                confidence: 1.0,
                non_null_count: 100,
                total_count: 100,
                hint_applied: false,
            },
            FieldInferenceDetail {
                field_name: "name".into(),
                inferred_type: DataType::Utf8,
                confidence: 0.9,
                non_null_count: 90,
                total_count: 100,
                hint_applied: false,
            },
        ],
        warnings: vec![],
    };

    assert_eq!(inferred.schema.fields().len(), 2);
    assert!((inferred.confidence - 0.95).abs() < f64::EPSILON);
    assert_eq!(inferred.field_details.len(), 2);
}

#[test]
fn test_schema_change_variants() {
    let changes = [
        SchemaChange::ColumnAdded {
            name: "email".into(),
            data_type: DataType::Utf8,
            nullable: true,
        },
        SchemaChange::ColumnRemoved {
            name: "legacy".into(),
        },
        SchemaChange::TypeChanged {
            name: "age".into(),
            old_type: DataType::Int32,
            new_type: DataType::Int64,
        },
        SchemaChange::NullabilityChanged {
            name: "name".into(),
            was_nullable: false,
            now_nullable: true,
        },
        SchemaChange::ColumnRenamed {
            old_name: "fname".into(),
            new_name: "first_name".into(),
        },
    ];
    assert_eq!(changes.len(), 5);
}

#[test]
fn test_evolution_verdict() {
    assert_eq!(EvolutionVerdict::Compatible, EvolutionVerdict::Compatible);
    assert_ne!(
        EvolutionVerdict::Compatible,
        EvolutionVerdict::RequiresMigration
    );
}

#[test]
fn test_column_projection() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, false),
    ]));

    let proj = ColumnProjection {
        mappings: vec![Some(0), None, Some(1)],
        target_schema: schema,
    };

    assert_eq!(proj.mappings.len(), 3);
    assert_eq!(proj.mappings[0], Some(0));
    assert_eq!(proj.mappings[1], None); // new column
    assert_eq!(proj.mappings[2], Some(1));
}

#[test]
fn test_warning_severity() {
    let w = InferenceWarning {
        field: Some("price".into()),
        message: "mixed int/float".into(),
        severity: WarningSeverity::Warning,
    };
    assert_eq!(w.severity, WarningSeverity::Warning);
    assert_eq!(w.field.as_deref(), Some("price"));
}
