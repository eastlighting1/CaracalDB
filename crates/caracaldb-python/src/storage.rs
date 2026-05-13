use caracaldb_core::{
    append_edge_batch_ipc_stream as core_append_edge_batch_ipc_stream,
    append_node_batch_ipc_stream as core_append_node_batch_ipc_stream,
    list_edge_stores as core_list_edge_stores, list_node_stores as core_list_node_stores,
    open_bundle as core_open_bundle, open_edge_store as core_open_edge_store,
    open_node_store as core_open_node_store,
    scan_edge_store_ipc_streams as core_scan_edge_store_ipc_streams,
    scan_edge_store_summary as core_scan_edge_store_summary,
    scan_node_store_ipc_streams as core_scan_node_store_ipc_streams,
    scan_node_store_summary as core_scan_node_store_summary,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::to_py_err;

#[pyfunction]
#[pyo3(signature = (bundle_path, class_iri, local_name, create=false))]
fn open_node_store(
    bundle_path: String,
    class_iri: String,
    local_name: String,
    create: bool,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store =
            core_open_node_store(&bundle, &class_iri, &local_name, create).map_err(to_py_err)?;
        let dict = PyDict::new_bound(py);
        dict.set_item("path", store.root.display().to_string())?;
        dict.set_item("class_iri", store.manifest.class_iri)?;
        dict.set_item("local_name", store.manifest.local_name)?;
        dict.set_item("next_nid", store.manifest.next_nid)?;
        dict.set_item("chunk_count", store.manifest.chunks.len())?;
        Ok(dict.into())
    })
}

#[pyfunction]
#[pyo3(signature = (bundle_path, property_iri, local_name, create=false))]
fn open_edge_store(
    bundle_path: String,
    property_iri: String,
    local_name: String,
    create: bool,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store = core_open_edge_store(&bundle, &property_iri, &local_name, None, None, create)
            .map_err(to_py_err)?;
        let dict = PyDict::new_bound(py);
        dict.set_item("path", store.root.display().to_string())?;
        dict.set_item("property_iri", store.manifest.property_iri)?;
        dict.set_item("local_name", store.manifest.local_name)?;
        dict.set_item("next_eid", store.manifest.next_eid)?;
        dict.set_item("chunk_count", store.manifest.chunks.len())?;
        Ok(dict.into())
    })
}

#[pyfunction]
fn list_node_stores(bundle_path: String) -> PyResult<Vec<String>> {
    let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
    core_list_node_stores(&bundle).map_err(to_py_err)
}

#[pyfunction]
fn list_edge_stores(bundle_path: String) -> PyResult<Vec<String>> {
    let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
    core_list_edge_stores(&bundle).map_err(to_py_err)
}

#[pyfunction]
fn scan_node_store_summary(
    bundle_path: String,
    class_iri: String,
    local_name: String,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store =
            core_open_node_store(&bundle, &class_iri, &local_name, false).map_err(to_py_err)?;
        let summary = core_scan_node_store_summary(&store).map_err(to_py_err)?;
        store_summary_to_dict(py, summary)
    })
}

#[pyfunction]
fn scan_edge_store_summary(
    bundle_path: String,
    property_iri: String,
    local_name: String,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store = core_open_edge_store(&bundle, &property_iri, &local_name, None, None, false)
            .map_err(to_py_err)?;
        let summary = core_scan_edge_store_summary(&store).map_err(to_py_err)?;
        store_summary_to_dict(py, summary)
    })
}

#[pyfunction]
#[pyo3(signature = (bundle_path, class_iri, local_name, snapshot_lsn=None))]
fn scan_node_store(
    bundle_path: String,
    class_iri: String,
    local_name: String,
    snapshot_lsn: Option<u64>,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store =
            core_open_node_store(&bundle, &class_iri, &local_name, false).map_err(to_py_err)?;
        let streams = core_scan_node_store_ipc_streams(&store, snapshot_lsn).map_err(to_py_err)?;
        ipc_streams_to_pylist(py, streams)
    })
}

#[pyfunction]
#[pyo3(signature = (bundle_path, property_iri, local_name, snapshot_lsn=None))]
fn scan_edge_store(
    bundle_path: String,
    property_iri: String,
    local_name: String,
    snapshot_lsn: Option<u64>,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let store = core_open_edge_store(&bundle, &property_iri, &local_name, None, None, false)
            .map_err(to_py_err)?;
        let streams = core_scan_edge_store_ipc_streams(&store, snapshot_lsn).map_err(to_py_err)?;
        ipc_streams_to_pylist(py, streams)
    })
}

#[pyfunction]
#[pyo3(signature = (bundle_path, class_iri, local_name, ipc_stream, created_lsn=0))]
fn append_node_batch(
    bundle_path: String,
    class_iri: String,
    local_name: String,
    ipc_stream: &Bound<'_, PyBytes>,
    created_lsn: u64,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let mut store =
            core_open_node_store(&bundle, &class_iri, &local_name, false).map_err(to_py_err)?;
        let result =
            core_append_node_batch_ipc_stream(&mut store, ipc_stream.as_bytes(), created_lsn)
                .map_err(to_py_err)?;
        append_result_to_dict(py, result, "nid")
    })
}

#[pyfunction]
#[pyo3(signature = (bundle_path, property_iri, local_name, ipc_stream, created_lsn=0))]
fn append_edge_batch(
    bundle_path: String,
    property_iri: String,
    local_name: String,
    ipc_stream: &Bound<'_, PyBytes>,
    created_lsn: u64,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(bundle_path).map_err(to_py_err)?;
        let mut store =
            core_open_edge_store(&bundle, &property_iri, &local_name, None, None, false)
                .map_err(to_py_err)?;
        let result =
            core_append_edge_batch_ipc_stream(&mut store, ipc_stream.as_bytes(), created_lsn)
                .map_err(to_py_err)?;
        append_result_to_dict(py, result, "eid")
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(open_node_store, module)?)?;
    module.add_function(wrap_pyfunction!(open_edge_store, module)?)?;
    module.add_function(wrap_pyfunction!(list_node_stores, module)?)?;
    module.add_function(wrap_pyfunction!(list_edge_stores, module)?)?;
    module.add_function(wrap_pyfunction!(scan_node_store_summary, module)?)?;
    module.add_function(wrap_pyfunction!(scan_edge_store_summary, module)?)?;
    module.add_function(wrap_pyfunction!(scan_node_store, module)?)?;
    module.add_function(wrap_pyfunction!(scan_edge_store, module)?)?;
    module.add_function(wrap_pyfunction!(append_node_batch, module)?)?;
    module.add_function(wrap_pyfunction!(append_edge_batch, module)?)?;
    Ok(())
}

fn store_summary_to_dict(
    py: Python<'_>,
    summary: caracaldb_core::StoreScanSummary,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("chunk_count", summary.chunk_count)?;
    dict.set_item("batch_count", summary.batch_count)?;
    dict.set_item("row_count", summary.row_count)?;
    dict.set_item("field_names", summary.field_names)?;
    Ok(dict.into())
}

fn ipc_streams_to_pylist(py: Python<'_>, streams: Vec<Vec<u8>>) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for stream in streams {
        list.append(PyBytes::new_bound(py, &stream))?;
    }
    Ok(list.into())
}

fn append_result_to_dict(
    py: Python<'_>,
    result: caracaldb_core::AppendResult,
    id_label: &str,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("path", result.path)?;
    dict.set_item("row_count", result.row_count)?;
    dict.set_item(format!("start_{id_label}"), result.start_id)?;
    dict.set_item(format!("end_{id_label}"), result.end_id)?;
    Ok(dict.into())
}
