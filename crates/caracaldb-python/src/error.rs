use caracaldb_core::CaracalError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(caracaldb_rust, CaracalDbError, PyException);

pub fn to_py_err(error: CaracalError) -> PyErr {
    let mut message = format!("{}: {}", error.code, error.message);
    if let Some(hint) = error.hint {
        message.push_str("\nHint: ");
        message.push_str(&hint);
    }
    PyErr::new::<CaracalDbError, _>(message)
}
