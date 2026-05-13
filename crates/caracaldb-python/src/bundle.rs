use std::path::PathBuf;

use caracaldb_core::{
    create_bundle as core_create_bundle, open_bundle as core_open_bundle, CaracalError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::to_py_err;

#[pyclass]
pub struct EngineHandle {
    path: PathBuf,
    mode: String,
}

#[pymethods]
impl EngineHandle {
    #[getter]
    fn path(&self) -> String {
        self.path.display().to_string()
    }

    #[getter]
    fn mode(&self) -> &str {
        &self.mode
    }
}

#[pyfunction]
#[pyo3(signature = (path, mode=None))]
fn open_database(path: String, mode: Option<String>) -> PyResult<EngineHandle> {
    let mode = mode.unwrap_or_else(|| "rw".to_string());
    if mode != "r" && mode != "rw" {
        return Err(to_py_err(CaracalError::new(
            "CDB-9007",
            format!("unsupported database mode: {mode}"),
        )));
    }
    let bundle = core_open_bundle(path).map_err(to_py_err)?;
    Ok(EngineHandle {
        path: bundle.path,
        mode,
    })
}

#[pyfunction]
#[pyo3(signature = (path, exist_ok=None))]
fn create_bundle(path: String, exist_ok: Option<bool>) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_create_bundle(path, exist_ok.unwrap_or(false)).map_err(to_py_err)?;
        bundle_to_dict(
            py,
            bundle.path.display().to_string(),
            bundle.manifest.format_version,
        )
    })
}

#[pyfunction]
fn open_bundle(path: String) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let bundle = core_open_bundle(path).map_err(to_py_err)?;
        bundle_to_dict(
            py,
            bundle.path.display().to_string(),
            bundle.manifest.format_version,
        )
    })
}

#[pyfunction]
fn rust_engine_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<EngineHandle>()?;
    module.add_function(wrap_pyfunction!(open_database, module)?)?;
    module.add_function(wrap_pyfunction!(create_bundle, module)?)?;
    module.add_function(wrap_pyfunction!(open_bundle, module)?)?;
    module.add_function(wrap_pyfunction!(rust_engine_version, module)?)?;
    Ok(())
}

fn bundle_to_dict(py: Python<'_>, path: String, format_version: u32) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("path", path)?;
    dict.set_item("format_version", format_version)?;
    Ok(dict.into())
}
