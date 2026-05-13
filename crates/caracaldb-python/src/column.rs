use caracaldb_core::{
    decode_column_segment as core_decode_column_segment,
    read_column_segment_info as core_read_column_segment_info,
    write_column_segment_ipc_stream as core_write_column_segment_ipc_stream,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::to_py_err;

#[pyfunction]
fn read_column_segment_info(path: String) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let info = core_read_column_segment_info(path).map_err(to_py_err)?;
        column_info_to_dict(py, &info)
    })
}

#[pyfunction]
fn decode_column_segment(path: String) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let decoded = core_decode_column_segment(path).map_err(to_py_err)?;
        let dict = PyDict::new_bound(py);
        dict.set_item("info", column_info_to_dict(py, &decoded.info)?)?;
        dict.set_item("field_names", decoded.field_names)?;
        let batches = PyList::empty_bound(py);
        for batch in decoded.batches {
            let item = PyDict::new_bound(py);
            item.set_item("row_count", batch.row_count)?;
            item.set_item("column_count", batch.column_count)?;
            batches.append(item)?;
        }
        dict.set_item("batches", batches)?;
        Ok(dict.into())
    })
}

#[pyfunction]
#[pyo3(signature = (path, ipc_stream, codec="none"))]
fn write_column_segment_from_ipc(
    path: String,
    ipc_stream: &Bound<'_, PyBytes>,
    codec: &str,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let info = core_write_column_segment_ipc_stream(path, ipc_stream.as_bytes(), codec)
            .map_err(to_py_err)?;
        column_info_to_dict(py, &info)
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(read_column_segment_info, module)?)?;
    module.add_function(wrap_pyfunction!(decode_column_segment, module)?)?;
    module.add_function(wrap_pyfunction!(write_column_segment_from_ipc, module)?)?;
    Ok(())
}

fn column_info_to_dict(
    py: Python<'_>,
    info: &caracaldb_core::ColumnSegmentInfo,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("format_version", info.footer.format_version)?;
    dict.set_item("codec", &info.footer.codec)?;
    dict.set_item("row_count", info.footer.row_count)?;
    dict.set_item("batch_count", info.footer.batch_count)?;
    dict.set_item("schema", &info.footer.schema)?;
    dict.set_item("uncompressed_size", info.footer.uncompressed_size)?;
    dict.set_item("payload_size", info.footer.payload_size)?;
    dict.set_item("payload_offset", info.payload_offset)?;
    dict.set_item("footer_offset", info.footer_offset)?;
    Ok(dict.into())
}
