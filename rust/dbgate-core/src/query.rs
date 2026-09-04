//! Query result model.
//!
//! Rust port of `packages/types/query.d.ts`. Rows are represented as JSON
//! values. Columns carry the metadata the frontend needs to render and
//! edit data.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metadata for a single column in a query result.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryResultColumn {
    pub column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_column_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub display: Option<String>,
    /// Alias used by Ui when rendering (snake_case widened versions).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_primary_key: Option<bool>,
}

/// A full query result: rows + column metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Value>,
    pub columns: Vec<QueryResultColumn>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self::default()
    }
}
