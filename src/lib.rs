use pyo3::prelude::*;

mod fileprocessor;

use crate::fileprocessor::*;
/// split all documents in a file
#[pyfunction]
fn split_file_content(dbname: &str, cfgname: &str, filename: &str, reg_ex: &str) -> PyResult<bool> {
    Ok(split_file(dbname, cfgname, filename, reg_ex).unwrap())
}

/// processes all documents in a file
#[pyfunction]
fn process_file_content(dbname: &str, cfgname: &str) -> PyResult<String> {
    Ok(process_file(dbname, cfgname).unwrap())
}
#[pyfunction]
fn process_single_doc(doc_content: &str) -> PyResult<Vec<Tag>> {
    let result = process_single_document(doc_content).unwrap();
    Ok(result)
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn utilrs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_file_content, m)?)?;
    m.add_function(wrap_pyfunction!(process_file_content, m)?)?;
    m.add_function(wrap_pyfunction!(process_single_doc, m)?)?;
    // m.add_class();
    Ok(())
}
