//! Connection definition model.
//!
//! A serializable, engine-independent description of how to reach a
//! database server. The Rust `EngineDriver` implementations take this and
//! produce a connection handle.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A saved connection. Fields are a superset of what every driver needs;
/// each driver reads the subset it understands.
///
/// Serialized camelCase to match both the frontend connection object and the
/// stored `connections.jsonl` keys (`databaseFile`, `authType`, ...).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionDefinition {
    /// Dotted engine id, e.g. `mysql@dbgate-plugin-mysql`.
    pub engine: String,

    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// SQLite / DuckDB: path to the database file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_file: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_read_only: Option<bool>,

    /// Engine-specific auth type (e.g. `hostPort`, `socket`, `awsIam`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,

    /// Any engine-specific extra fields, keyed by field name.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub extra: Option<std::collections::BTreeMap<String, Value>>,
}

impl ConnectionDefinition {
    pub fn sqlite_default() -> Self {
        Self {
            engine: "sqlite@dbgate-plugin-sqlite".into(),
            name: "sqlite".into(),
            ..Default::default()
        }
    }
}
