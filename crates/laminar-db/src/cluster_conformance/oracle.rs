//! Output consolidation and batch-SQL ground truth for conformance comparison.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;

use super::source::FixtureSource;

/// How the observed stream's output is folded into a comparable multiset.
#[derive(Clone, Copy, Debug)]
pub(super) enum OutputForm {
    /// Append-only rows.
    Rows,
    /// `__weight` changelog; weights are summed per row.
    Changelog,
    /// Running-state snapshots; the last row seen for each key (column indices) is final.
    Upsert(&'static [usize]),
}

/// Rendered row to multiplicity. Zero multiplicities are removed.
pub(super) type Consolidated = BTreeMap<String, i64>;

const WEIGHT: &str = laminar_core::changelog::WEIGHT_COLUMN;

fn render(batch: &RecordBatch, row: usize, columns: &[usize]) -> String {
    columns
        .iter()
        .map(|&index| {
            let column = batch.column(index);
            if column.is_null(row) {
                return "NULL".to_string();
            }
            // Distributed float aggregation reassociates; compare at 12 significant digits.
            match column.data_type() {
                DataType::Float64 => {
                    format!("{:.11e}", column.as_primitive::<Float64Type>().value(row))
                }
                _ => array_value_to_string(column, row).unwrap_or_else(|e| format!("<{e}>")),
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn value_columns(batch: &RecordBatch) -> Vec<usize> {
    (0..batch.num_columns())
        .filter(|&index| batch.schema().field(index).name() != WEIGHT)
        .collect()
}

/// Fold observed output batches into a consolidated multiset. A `Rows` case fails on a
/// `__weight` column so it cannot silently ignore retractions.
pub(super) fn consolidate(
    batches: &[RecordBatch],
    form: OutputForm,
) -> Result<Consolidated, String> {
    let mut out = Consolidated::new();
    match form {
        OutputForm::Rows => {
            if batches
                .iter()
                .any(|batch| batch.column_by_name(WEIGHT).is_some())
            {
                return Err(format!(
                    "row output carries '{WEIGHT}'; compare it as a changelog"
                ));
            }
            for batch in batches {
                let columns = value_columns(batch);
                for row in 0..batch.num_rows() {
                    *out.entry(render(batch, row, &columns)).or_default() += 1;
                }
            }
        }
        OutputForm::Changelog => {
            for batch in batches {
                let columns = value_columns(batch);
                let weights = batch
                    .column_by_name(WEIGHT)
                    .map(|column| column.as_primitive::<Int64Type>().clone());
                for row in 0..batch.num_rows() {
                    let weight = weights.as_ref().map_or(1, |w| w.value(row));
                    *out.entry(render(batch, row, &columns)).or_default() += weight;
                }
            }
        }
        OutputForm::Upsert(keys) => {
            let mut last = BTreeMap::new();
            for batch in batches {
                let columns = value_columns(batch);
                for row in 0..batch.num_rows() {
                    last.insert(render(batch, row, keys), render(batch, row, &columns));
                }
            }
            for row in last.into_values() {
                *out.entry(row).or_default() += 1;
            }
        }
    }
    out.retain(|_, multiplicity| *multiplicity != 0);
    Ok(out)
}

/// Evaluate `sql` as one batch query over every fixture source's complete input.
pub(super) async fn ground_truth(
    sources: &[FixtureSource],
    sql: &str,
) -> Result<Consolidated, String> {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    for source in sources {
        let table = MemTable::try_new(source.schema(), vec![source.all_batches()])
            .map_err(|e| e.to_string())?;
        ctx.register_table(source.name, Arc::new(table))
            .map_err(|e| e.to_string())?;
    }
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    consolidate(&batches, OutputForm::Rows)
}

/// Human-readable symmetric difference, bounded to keep failures legible.
pub(super) fn diff(expected: &Consolidated, actual: &Consolidated) -> String {
    let mut lines = Vec::new();
    for (row, &count) in expected {
        let seen = actual.get(row).copied().unwrap_or(0);
        if seen != count {
            lines.push(format!("  expected {count:>3} got {seen:>3}  {row}"));
        }
    }
    for (row, &count) in actual {
        if !expected.contains_key(row) {
            lines.push(format!("  expected   0 got {count:>3}  {row}"));
        }
    }
    let total = lines.len();
    lines.truncate(40);
    format!("{total} differing rows:\n{}", lines.join("\n"))
}
