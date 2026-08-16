use super::*;

fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
            .collect::<Vec<_>>(),
    ))
}

// ── is_safe_widening tests ────────────────────────────────

#[test]
fn test_int_widening() {
    assert!(is_safe_widening(&DataType::Int8, &DataType::Int16));
    assert!(is_safe_widening(&DataType::Int8, &DataType::Int32));
    assert!(is_safe_widening(&DataType::Int8, &DataType::Int64));
    assert!(is_safe_widening(&DataType::Int16, &DataType::Int32));
    assert!(is_safe_widening(&DataType::Int16, &DataType::Int64));
    assert!(is_safe_widening(&DataType::Int32, &DataType::Int64));
}

#[test]
fn test_uint_widening() {
    assert!(is_safe_widening(&DataType::UInt8, &DataType::UInt16));
    assert!(is_safe_widening(&DataType::UInt8, &DataType::UInt32));
    assert!(is_safe_widening(&DataType::UInt32, &DataType::UInt64));
}

#[test]
fn test_float_widening() {
    assert!(is_safe_widening(&DataType::Float16, &DataType::Float32));
    assert!(is_safe_widening(&DataType::Float16, &DataType::Float64));
    assert!(is_safe_widening(&DataType::Float32, &DataType::Float64));
}

#[test]
fn test_int_to_float_widening() {
    assert!(is_safe_widening(&DataType::Int8, &DataType::Float32));
    assert!(is_safe_widening(&DataType::Int16, &DataType::Float64));
    assert!(is_safe_widening(&DataType::Int32, &DataType::Float64));
}

#[test]
fn test_string_binary_widening() {
    assert!(is_safe_widening(&DataType::Utf8, &DataType::LargeUtf8));
    assert!(is_safe_widening(&DataType::Binary, &DataType::LargeBinary));
}

#[test]
fn test_narrowing_not_safe() {
    assert!(!is_safe_widening(&DataType::Int64, &DataType::Int32));
    assert!(!is_safe_widening(&DataType::Float64, &DataType::Float32));
    assert!(!is_safe_widening(&DataType::LargeUtf8, &DataType::Utf8));
}

#[test]
fn test_unrelated_types() {
    assert!(!is_safe_widening(&DataType::Int64, &DataType::Utf8));
    assert!(!is_safe_widening(&DataType::Boolean, &DataType::Int32));
}

// ── diff_schemas tests ────────────────────────────────────

#[test]
fn test_diff_identical() {
    let s = schema(&[("a", DataType::Int64, false)]);
    let changes = diff_schemas_by_name(&s, &s);
    assert!(changes.is_empty());
}

#[test]
fn test_diff_column_added() {
    let old = schema(&[("a", DataType::Int64, false)]);
    let new = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);
    let changes = diff_schemas_by_name(&old, &new);
    assert_eq!(changes.len(), 1);
    assert!(matches!(&changes[0], SchemaChange::ColumnAdded { name, .. } if name == "b"));
}

#[test]
fn test_diff_column_removed() {
    let old = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);
    let new = schema(&[("a", DataType::Int64, false)]);
    let changes = diff_schemas_by_name(&old, &new);
    assert_eq!(changes.len(), 1);
    assert!(matches!(&changes[0], SchemaChange::ColumnRemoved { name } if name == "b"));
}

#[test]
fn test_diff_type_changed() {
    let old = schema(&[("a", DataType::Int32, false)]);
    let new = schema(&[("a", DataType::Int64, false)]);
    let changes = diff_schemas_by_name(&old, &new);
    assert_eq!(changes.len(), 1);
    assert!(matches!(
        &changes[0],
        SchemaChange::TypeChanged {
            name,
            old_type: DataType::Int32,
            new_type: DataType::Int64,
        } if name == "a"
    ));
}

#[test]
fn test_diff_nullability_changed() {
    let old = schema(&[("a", DataType::Int64, false)]);
    let new = schema(&[("a", DataType::Int64, true)]);
    let changes = diff_schemas_by_name(&old, &new);
    assert_eq!(changes.len(), 1);
    assert!(matches!(
        &changes[0],
        SchemaChange::NullabilityChanged {
            name,
            was_nullable: false,
            now_nullable: true,
        } if name == "a"
    ));
}

#[test]
fn test_diff_multiple_changes() {
    let old = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);
    let new = schema(&[
        ("a", DataType::Int64, true),    // nullability changed
        ("c", DataType::Float64, false), // b removed, c added
    ]);
    let changes = diff_schemas_by_name(&old, &new);
    // Should have: ColumnRemoved(b), ColumnAdded(c), NullabilityChanged(a)
    assert_eq!(changes.len(), 3);
}

// ── evaluate_changes tests ────────────────────────────────

#[test]
fn test_evaluate_no_changes() {
    let verdict = evaluate_changes(&[], CompatibilityMode::Full);
    assert_eq!(verdict, EvolutionVerdict::Compatible);
}

#[test]
fn test_evaluate_none_mode_allows_all() {
    let changes = vec![
        SchemaChange::ColumnRemoved { name: "x".into() },
        SchemaChange::TypeChanged {
            name: "y".into(),
            old_type: DataType::Int64,
            new_type: DataType::Utf8, // unrelated type change
        },
    ];
    let verdict = evaluate_changes(&changes, CompatibilityMode::None);
    assert_eq!(verdict, EvolutionVerdict::Compatible);
}

#[test]
fn test_evaluate_backward_add_nullable_ok() {
    let changes = vec![SchemaChange::ColumnAdded {
        name: "email".into(),
        data_type: DataType::Utf8,
        nullable: true,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert_eq!(verdict, EvolutionVerdict::Compatible);
}

#[test]
fn test_evaluate_backward_add_non_nullable_rejected() {
    let changes = vec![SchemaChange::ColumnAdded {
        name: "email".into(),
        data_type: DataType::Utf8,
        nullable: false,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

#[test]
fn test_evaluate_forward_drop_rejected() {
    let changes = vec![SchemaChange::ColumnRemoved {
        name: "legacy".into(),
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Forward);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

#[test]
fn test_evaluate_backward_drop_ok() {
    let changes = vec![SchemaChange::ColumnRemoved {
        name: "legacy".into(),
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert_eq!(verdict, EvolutionVerdict::RequiresMigration);
}

#[test]
fn test_evaluate_backward_widening_ok() {
    let changes = vec![SchemaChange::TypeChanged {
        name: "count".into(),
        old_type: DataType::Int32,
        new_type: DataType::Int64,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert_eq!(verdict, EvolutionVerdict::Compatible);
}

#[test]
fn test_evaluate_full_widening_rejected() {
    let changes = vec![SchemaChange::TypeChanged {
        name: "count".into(),
        old_type: DataType::Int32,
        new_type: DataType::Int64,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Full);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

#[test]
fn test_evaluate_unrelated_type_change_rejected() {
    let changes = vec![SchemaChange::TypeChanged {
        name: "val".into(),
        old_type: DataType::Int64,
        new_type: DataType::Utf8,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

#[test]
fn test_evaluate_make_nullable_ok() {
    let changes = vec![SchemaChange::NullabilityChanged {
        name: "field".into(),
        was_nullable: false,
        now_nullable: true,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Full);
    assert_eq!(verdict, EvolutionVerdict::Compatible);
}

#[test]
fn test_evaluate_make_non_nullable_backward_rejected() {
    let changes = vec![SchemaChange::NullabilityChanged {
        name: "field".into(),
        was_nullable: true,
        now_nullable: false,
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

#[test]
fn test_evaluate_rename_rejected() {
    let changes = vec![SchemaChange::ColumnRenamed {
        old_name: "fname".into(),
        new_name: "first_name".into(),
    }];
    let verdict = evaluate_changes(&changes, CompatibilityMode::Backward);
    assert!(matches!(verdict, EvolutionVerdict::Incompatible(_)));
}

// ── apply_changes tests ───────────────────────────────────

#[test]
fn test_apply_add_column() {
    let old = schema(&[("a", DataType::Int64, false)]);
    let changes = vec![SchemaChange::ColumnAdded {
        name: "b".into(),
        data_type: DataType::Utf8,
        nullable: true,
    }];
    let proj = apply_changes(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.fields().len(), 2);
    assert_eq!(proj.mappings, vec![Some(0), None]);
}

#[test]
fn test_apply_remove_column() {
    let old = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);
    let changes = vec![SchemaChange::ColumnRemoved { name: "b".into() }];
    let proj = apply_changes(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.fields().len(), 1);
    assert_eq!(proj.target_schema.field(0).name(), "a");
    assert_eq!(proj.mappings, vec![Some(0)]);
}

#[test]
fn test_apply_widen_type() {
    let old = schema(&[("val", DataType::Int32, false)]);
    let changes = vec![SchemaChange::TypeChanged {
        name: "val".into(),
        old_type: DataType::Int32,
        new_type: DataType::Int64,
    }];
    let proj = apply_changes(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(proj.mappings, vec![Some(0)]);
}

#[test]
fn test_apply_change_nullability() {
    let old = schema(&[("val", DataType::Int64, false)]);
    let changes = vec![SchemaChange::NullabilityChanged {
        name: "val".into(),
        was_nullable: false,
        now_nullable: true,
    }];
    let proj = apply_changes(&old, &changes).unwrap();
    assert!(proj.target_schema.field(0).is_nullable());
}

#[test]
fn test_apply_rename() {
    let old = schema(&[("fname", DataType::Utf8, false)]);
    let changes = vec![SchemaChange::ColumnRenamed {
        old_name: "fname".into(),
        new_name: "first_name".into(),
    }];
    let proj = apply_changes(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.field(0).name(), "first_name");
    assert_eq!(proj.mappings, vec![Some(0)]);
}

#[test]
fn test_apply_multi_change() {
    let old = schema(&[
        ("a", DataType::Int64, false),
        ("b", DataType::Int32, true),
        ("c", DataType::Utf8, false),
    ]);
    let changes = vec![
        SchemaChange::ColumnRemoved { name: "c".into() },
        SchemaChange::ColumnAdded {
            name: "d".into(),
            data_type: DataType::Float64,
            nullable: true,
        },
        SchemaChange::TypeChanged {
            name: "b".into(),
            old_type: DataType::Int32,
            new_type: DataType::Int64,
        },
    ];
    let proj = apply_changes(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.fields().len(), 3); // a, b (widened), d
    assert_eq!(proj.target_schema.field(0).name(), "a");
    assert_eq!(proj.target_schema.field(1).name(), "b");
    assert_eq!(proj.target_schema.field(1).data_type(), &DataType::Int64);
    assert_eq!(proj.target_schema.field(2).name(), "d");
    // a→Some(0), b→Some(1), d→None
    assert_eq!(proj.mappings, vec![Some(0), Some(1), None]);
}

// ── SchemaHistory tests ───────────────────────────────────

#[test]
fn test_history_record_and_query() {
    let mut history = SchemaHistory::new();
    let s1 = schema(&[("a", DataType::Int64, false)]);
    let s2 = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);

    let v1 = history.record("test_source", s1, vec![], EvolutionTrigger::Ddl);
    assert_eq!(v1, 1);

    let v2 = history.record(
        "test_source",
        s2,
        vec![SchemaChange::ColumnAdded {
            name: "b".into(),
            data_type: DataType::Utf8,
            nullable: true,
        }],
        EvolutionTrigger::Ddl,
    );
    assert_eq!(v2, 2);

    assert_eq!(history.versions("test_source").len(), 2);
    assert_eq!(history.latest_version("test_source"), Some(2));
    assert_eq!(history.latest_version("unknown"), None);
}

// ── SchemaEvolutionEngine tests ───────────────────────────

#[test]
fn test_engine_no_change() {
    let mut engine = SchemaEvolutionEngine::new(CompatibilityMode::Backward);
    let evolver = SchemaEvolution::new(CompatibilityMode::Backward);
    let s = schema(&[("a", DataType::Int64, false)]);

    let result = engine
        .evolve("src", &evolver, &s, &s, EvolutionTrigger::Ddl)
        .unwrap();

    assert!(matches!(result, EvolutionResult::NoChange));
}

#[test]
fn test_engine_add_nullable_column() {
    let mut engine = SchemaEvolutionEngine::new(CompatibilityMode::Backward);
    let evolver = SchemaEvolution::new(CompatibilityMode::Backward);
    let old = schema(&[("a", DataType::Int64, false)]);
    let new = schema(&[("a", DataType::Int64, false), ("b", DataType::Utf8, true)]);

    let result = engine
        .evolve("src", &evolver, &old, &new, EvolutionTrigger::Ddl)
        .unwrap();

    match result {
        EvolutionResult::Applied {
            new_schema,
            version,
            changes,
            ..
        } => {
            assert_eq!(new_schema.fields().len(), 2);
            assert_eq!(version, 1);
            assert_eq!(changes.len(), 1);
        }
        EvolutionResult::NoChange => panic!("expected Applied"),
    }
}

#[test]
fn test_engine_incompatible_rejected() {
    let mut engine = SchemaEvolutionEngine::new(CompatibilityMode::Full);
    let evolver = SchemaEvolution::new(CompatibilityMode::Full);
    let old = schema(&[("a", DataType::Int32, false)]);
    let new = schema(&[("a", DataType::Int64, false)]); // widening under Full = incompatible

    let result = engine.evolve("src", &evolver, &old, &new, EvolutionTrigger::Ddl);
    assert!(result.is_err());
}

// ── SchemaEvolution integration ──────────────────────

#[test]
fn test_default_evolver_diff_and_apply() {
    let evolver = SchemaEvolution::default();
    let old = schema(&[("id", DataType::Int64, false)]);
    let new = schema(&[
        ("id", DataType::Int64, false),
        ("email", DataType::Utf8, true),
    ]);

    let changes = evolver.diff_schemas(&old, &new);
    assert_eq!(changes.len(), 1);

    let verdict = evolver.evaluate_evolution(&changes);
    assert_eq!(verdict, EvolutionVerdict::Compatible);

    let proj = evolver.apply_evolution(&old, &changes).unwrap();
    assert_eq!(proj.target_schema.fields().len(), 2);
    assert_eq!(proj.mappings, vec![Some(0), None]);
}
