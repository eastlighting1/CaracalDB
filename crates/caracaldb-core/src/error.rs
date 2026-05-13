use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[error("{code}: {message}")]
pub struct CaracalError {
    pub code: &'static str,
    pub message: String,
    pub hint: Option<String>,
}

impl CaracalError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
        }
    }

    pub fn with_hint(
        code: &'static str,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            hint: Some(hint.into()),
        }
    }
}

impl From<io::Error> for CaracalError {
    fn from(value: io::Error) -> Self {
        Self::new("CDB-9000", value.to_string())
    }
}

impl From<serde_json::Error> for CaracalError {
    fn from(value: serde_json::Error) -> Self {
        Self::new("CDB-9000", value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CaracalError>;
