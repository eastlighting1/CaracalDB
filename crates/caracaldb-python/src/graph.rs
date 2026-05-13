use caracaldb_core::{
    build_csc_arrays, build_csr_arrays, csr_k_hop, csr_neighbor_sample, csr_neighbors_of,
    csr_shortest_path, hnsw_manifest_boundary, read_csr as core_read_csr, typed_adjacency,
    typed_neighbors, write_csr, CaracalError, CsrFile, CsrNeighbors,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::to_py_err;

#[pyfunction]
#[pyo3(signature = (path, num_vertices, src, dst, eids=None))]
fn build_csr(
    path: String,
    num_vertices: u64,
    src: Vec<u64>,
    dst: Vec<u64>,
    eids: Option<Vec<u64>>,
) -> PyResult<PyObject> {
    let csr = build_csr_arrays(&src, &dst, num_vertices, eids.as_deref()).map_err(to_py_err)?;
    write_csr(&path, &csr).map_err(to_py_err)?;
    read_csr(path)
}

#[pyfunction]
#[pyo3(signature = (path, num_vertices, src, dst, eids=None))]
fn build_csc(
    path: String,
    num_vertices: u64,
    src: Vec<u64>,
    dst: Vec<u64>,
    eids: Option<Vec<u64>>,
) -> PyResult<PyObject> {
    let csc = build_csc_arrays(&src, &dst, num_vertices, eids.as_deref()).map_err(to_py_err)?;
    write_csr(&path, &csc).map_err(to_py_err)?;
    read_csr(path)
}

#[pyfunction]
fn read_csr(path: String) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    Python::with_gil(|py| csr_to_dict(py, &csr))
}

#[pyfunction]
fn csr_neighbors(path: String, vertex: u64) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    let neighbors = csr_neighbors_of(&csr, vertex).map_err(to_py_err)?;
    Python::with_gil(|py| csr_neighbors_to_dict(py, &neighbors))
}

#[pyfunction]
fn csr_k_hop_rows(
    path: String,
    seeds: Vec<u64>,
    min_depth: u32,
    max_depth: u32,
) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    let rows = csr_k_hop(&csr, &seeds, min_depth, max_depth).map_err(to_py_err)?;
    Python::with_gil(|py| {
        let list = pyo3::types::PyList::empty_bound(py);
        for row in rows {
            let dict = PyDict::new_bound(py);
            dict.set_item("seed", row.seed)?;
            dict.set_item("node", row.node)?;
            dict.set_item("depth", row.depth)?;
            dict.set_item("path_nodes", row.path.nodes)?;
            dict.set_item("path_eids", row.path.eids)?;
            list.append(dict)?;
        }
        Ok(list.into())
    })
}

#[pyfunction]
fn csr_shortest_path_row(
    path: String,
    source: u64,
    target: u64,
    max_depth: u32,
) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    let result = csr_shortest_path(&csr, source, target, max_depth).map_err(to_py_err)?;
    Python::with_gil(|py| match result {
        Some(path) => {
            let dict = PyDict::new_bound(py);
            dict.set_item("nodes", path.nodes)?;
            dict.set_item("eids", path.eids)?;
            Ok(dict.into())
        }
        None => Ok(py.None()),
    })
}

#[pyfunction]
fn csr_typed_neighbors(path: String, edge_type: String, vertex: u64) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    let adjacency = typed_adjacency(&edge_type, csr);
    let rows = typed_neighbors(&adjacency, vertex).map_err(to_py_err)?;
    Python::with_gil(|py| {
        let list = pyo3::types::PyList::empty_bound(py);
        for (edge_type, dst, eid) in rows {
            let dict = PyDict::new_bound(py);
            dict.set_item("edge_type", edge_type)?;
            dict.set_item("dst", dst)?;
            dict.set_item("eid", eid)?;
            list.append(dict)?;
        }
        Ok(list.into())
    })
}

#[pyfunction]
#[pyo3(signature = (path, seeds, fanout=None, replace=false))]
fn csr_neighbor_sample_rows(
    path: String,
    seeds: Vec<u64>,
    fanout: Option<usize>,
    replace: bool,
) -> PyResult<PyObject> {
    let csr = core_read_csr(path).map_err(to_py_err)?;
    let rows = csr_neighbor_sample(&csr, &seeds, fanout, replace).map_err(to_py_err)?;
    Python::with_gil(|py| {
        let list = pyo3::types::PyList::empty_bound(py);
        for row in rows {
            let dict = PyDict::new_bound(py);
            dict.set_item("src", row.src)?;
            dict.set_item("dst", row.dst)?;
            dict.set_item("eid", row.eid)?;
            list.append(dict)?;
        }
        Ok(list.into())
    })
}

#[pyfunction]
fn hnsw_boundary(
    index_name: String,
    vector_column: String,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    Ok(hnsw_manifest_boundary(&index_name, &vector_column))
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(build_csr, module)?)?;
    module.add_function(wrap_pyfunction!(build_csc, module)?)?;
    module.add_function(wrap_pyfunction!(read_csr, module)?)?;
    module.add_function(wrap_pyfunction!(csr_neighbors, module)?)?;
    module.add_function(wrap_pyfunction!(csr_k_hop_rows, module)?)?;
    module.add_function(wrap_pyfunction!(csr_shortest_path_row, module)?)?;
    module.add_function(wrap_pyfunction!(csr_typed_neighbors, module)?)?;
    module.add_function(wrap_pyfunction!(csr_neighbor_sample_rows, module)?)?;
    module.add_function(wrap_pyfunction!(hnsw_boundary, module)?)?;
    Ok(())
}

fn csr_to_dict(py: Python<'_>, csr: &CsrFile) -> PyResult<PyObject> {
    if csr.flags != 0 && csr.eids.is_none() {
        return Err(to_py_err(CaracalError::new(
            "CDB-7081",
            "CSR flags indicate eids but no eid array was decoded",
        )));
    }
    let dict = PyDict::new_bound(py);
    dict.set_item("num_vertices", csr.num_vertices)?;
    dict.set_item("num_edges", csr.num_edges)?;
    dict.set_item("flags", csr.flags)?;
    dict.set_item("offsets", &csr.offsets)?;
    dict.set_item("neighbors", &csr.neighbors)?;
    dict.set_item("eids", &csr.eids)?;
    Ok(dict.into())
}

fn csr_neighbors_to_dict(py: Python<'_>, neighbors: &CsrNeighbors) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("vertex", neighbors.vertex)?;
    dict.set_item("neighbors", &neighbors.neighbors)?;
    dict.set_item("eids", &neighbors.eids)?;
    Ok(dict.into())
}
