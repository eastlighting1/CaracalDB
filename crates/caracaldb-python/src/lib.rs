#![allow(unexpected_cfgs)]
#![allow(clippy::useless_conversion)]

mod bundle;
mod column;
mod error;
mod exec;
mod graph;
mod storage;

use pyo3::prelude::*;

pub use error::{to_py_err, CaracalDbError};

#[pymodule]
fn _caracaldb_rust(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("CaracalDbError", py.get_type_bound::<CaracalDbError>())?;
    bundle::register(module)?;
    storage::register(module)?;
    column::register(module)?;
    graph::register(module)?;
    exec::register(module)?;
    Ok(())
}
