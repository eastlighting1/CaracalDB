use caracaldb_core::{
    execute_to_batches, open_bundle as core_open_bundle, open_node_store as core_open_node_store,
    parse_diagnostic, parse_tuft_subset, record_batches_to_ipc_stream, BatchSourceOperator,
    CaracalError, ExecCtx, FilterOperator, HashAggregateOperator, NodeScanOperator,
    PhysicalOperator, ProjectOperator, TopKOperator,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use serde_json::Value;

use crate::to_py_err;

type DynOperator = Box<dyn PhysicalOperator>;

#[pyfunction]
#[pyo3(signature = (bundle_path, plan_json, snapshot_lsn=None))]
fn execute_plan(
    bundle_path: String,
    plan_json: String,
    snapshot_lsn: Option<u64>,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let plan: Value = serde_json::from_str(&plan_json).map_err(|err| {
            to_py_err(CaracalError::new(
                "CDB-6020",
                format!("invalid Rust execution plan JSON: {err}"),
            ))
        })?;
        let mut operator = build_operator(&bundle_path, &plan).map_err(to_py_err)?;
        let batches = execute_to_batches(
            &mut operator,
            &ExecCtx {
                snapshot_lsn,
                ..ExecCtx::default()
            },
        )
        .map_err(to_py_err)?;
        let list = PyList::empty_bound(py);
        if !batches.is_empty() {
            let stream = record_batches_to_ipc_stream(&batches).map_err(to_py_err)?;
            list.append(PyBytes::new_bound(py, &stream))?;
        }
        Ok(list.into())
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(execute_plan, module)?)?;
    module.add_function(wrap_pyfunction!(lower_tuft_plan, module)?)?;
    module.add_function(wrap_pyfunction!(tuft_diagnostic, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (text, class_iri_prefix=None))]
fn lower_tuft_plan(text: String, class_iri_prefix: Option<String>) -> PyResult<String> {
    let parsed = parse_tuft_subset(&text).map_err(to_py_err)?;
    let class_iri_prefix = class_iri_prefix.unwrap_or_else(|| "http://example.org/".to_string());
    let scan = serde_json::json!({
        "op": "node_scan",
        "class_iri": format!("{}{}", class_iri_prefix, parsed.class_name),
        "local_name": parsed.class_name,
        "snapshot_lsn": null,
    });
    let columns = parsed
        .return_columns
        .iter()
        .map(|column| column.rsplit('.').next().unwrap_or(column).to_string())
        .collect::<Vec<_>>();
    let plan = serde_json::json!({
        "op": "project",
        "columns": columns,
        "input": scan,
    });
    serde_json::to_string(&plan).map_err(|err| {
        to_py_err(CaracalError::new(
            "CDB-6020",
            format!("failed to serialize lowered Rust plan: {err}"),
        ))
    })
}

#[pyfunction]
fn tuft_diagnostic(text: String) -> PyResult<Option<PyObject>> {
    Python::with_gil(|py| {
        let Some(diagnostic) = parse_diagnostic(&text) else {
            return Ok(None);
        };
        let dict = PyDict::new_bound(py);
        dict.set_item("code", diagnostic.code)?;
        dict.set_item("message", diagnostic.message)?;
        dict.set_item("span_start", diagnostic.span_start)?;
        dict.set_item("span_end", diagnostic.span_end)?;
        dict.set_item("hint", diagnostic.hint)?;
        Ok(Some(dict.into()))
    })
}

fn build_operator(bundle_path: &str, plan: &Value) -> caracaldb_core::Result<DynOperator> {
    let op = required_str(plan, "op")?;
    match op {
        "node_scan" => {
            let bundle = core_open_bundle(bundle_path)?;
            let store = core_open_node_store(
                &bundle,
                required_str(plan, "class_iri")?,
                required_str(plan, "local_name")?,
                false,
            )?;
            Ok(Box::new(NodeScanOperator::new(store)))
        }
        "filter_eq_u64" => Ok(Box::new(FilterOperator::eq_u64(
            build_operator(bundle_path, required_value(plan, "input")?)?,
            required_str(plan, "column")?,
            required_u64(plan, "value")?,
        ))),
        "project" => {
            let columns = required_array(plan, "columns")?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| unsupported("project columns must be an array of strings"))
                })
                .collect::<caracaldb_core::Result<Vec<_>>>()?;
            Ok(Box::new(ProjectOperator::new(
                build_operator(bundle_path, required_value(plan, "input")?)?,
                columns,
            )))
        }
        "top_k" => Ok(Box::new(TopKOperator::new(
            build_operator(bundle_path, required_value(plan, "input")?)?,
            topk_column(plan)?,
            optional_usize(plan, "skip")?.unwrap_or(0),
            optional_usize(plan, "limit")?,
        ))),
        "hash_aggregate_count" => Ok(Box::new(HashAggregateOperator::count(build_operator(
            bundle_path,
            required_value(plan, "input")?,
        )?))),
        "batch_source_empty" => Ok(Box::new(BatchSourceOperator::new(Vec::new()))),
        other => Err(unsupported(format!(
            "unsupported Rust execution op: {other}"
        ))),
    }
}

fn topk_column(plan: &Value) -> caracaldb_core::Result<String> {
    if let Some(value) = plan.get("order_by").and_then(Value::as_str) {
        return Ok(value.to_string());
    }
    let order_by = required_array(plan, "order_by")?;
    let first = order_by
        .first()
        .ok_or_else(|| unsupported("top_k requires at least one order_by expression"))?;
    let expr = required_value(first, "expr")?;
    if required_str(expr, "kind")? != "column" {
        return Err(unsupported(
            "top_k currently supports column order expressions",
        ));
    }
    Ok(required_str(expr, "name")?.to_string())
}

fn required_value<'a>(plan: &'a Value, key: &str) -> caracaldb_core::Result<&'a Value> {
    plan.get(key)
        .ok_or_else(|| unsupported(format!("missing required plan field: {key}")))
}

fn required_str<'a>(plan: &'a Value, key: &str) -> caracaldb_core::Result<&'a str> {
    required_value(plan, key)?
        .as_str()
        .ok_or_else(|| unsupported(format!("plan field must be a string: {key}")))
}

fn required_u64(plan: &Value, key: &str) -> caracaldb_core::Result<u64> {
    required_value(plan, key)?
        .as_u64()
        .ok_or_else(|| unsupported(format!("plan field must be a non-negative integer: {key}")))
}

fn optional_usize(plan: &Value, key: &str) -> caracaldb_core::Result<Option<usize>> {
    let Some(value) = plan.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_u64()
        .ok_or_else(|| unsupported(format!("plan field must be a non-negative integer: {key}")))?;
    usize::try_from(value)
        .map(Some)
        .map_err(|_| unsupported(format!("plan field is too large for this platform: {key}")))
}

fn required_array<'a>(plan: &'a Value, key: &str) -> caracaldb_core::Result<&'a Vec<Value>> {
    required_value(plan, key)?
        .as_array()
        .ok_or_else(|| unsupported(format!("plan field must be an array: {key}")))
}

fn unsupported(message: impl Into<String>) -> CaracalError {
    CaracalError::new("CDB-6020", message)
}
