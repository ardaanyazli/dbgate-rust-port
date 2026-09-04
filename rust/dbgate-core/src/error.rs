//! Shared error type for the DbGate Rust backend.
//!
//! All crate errors funnel into [`DbgmError`], which carries a stable
//! `DBGM-00000` style code (per project rules: never introduce numbered
//! codes, always `DBGM-00000` for new code) plus a message. Conversion from
//! underlying driver crates (rusqlite, etc.) is provided via `From`.

use thiserror::Error;

/// Sentinel code for all newly written code. Per AGENTS.md, new Rust code
/// must use `DBGM-00000` and must not invent numbered codes.
pub const DBGM_GENERIC: &str = "DBGM-00000";

#[derive(Debug, Error)]
#[error("{code}: {message}")]
pub struct DbgmError {
    pub code: String,
    pub message: String,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DbgmError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: DBGM_GENERIC.to_string(),
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source<E>(message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            code: DBGM_GENERIC.to_string(),
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn code(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            source: None,
        }
    }
}

impl From<rusqlite::Error> for DbgmError {
    fn from(err: rusqlite::Error) -> Self {
        Self::with_source(format!("SQLite error: {err}"), err)
    }
}

impl From<serde_json::Error> for DbgmError {
    fn from(err: serde_json::Error) -> Self {
        Self::with_source(format!("JSON error: {err}"), err)
    }
}

impl From<std::io::Error> for DbgmError {
    fn from(err: std::io::Error) -> Self {
        Self::with_source(format!("IO error: {err}"), err)
    }
}

impl From<String> for DbgmError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for DbgmError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

/// Convenience alias for `Result<T, DbgmError>`.
pub type DbgmResult<T> = Result<T, DbgmError>;
