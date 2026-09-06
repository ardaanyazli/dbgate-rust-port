//! `query-history/*` bridge routes.
//!
//! Appends executed queries to `data_dir/query-history.jsonl`, mirroring the
//! Electron `queryHistory` API (`packages/api/src/controllers/queryHistory.js`)
//! so the frontend's recent-queries picker has data to read.

use serde_json::{json, Value};

use std::io::Write;

use super::route_error;
use crate::DbgmState;

pub fn write(state: &DbgmState, args: Value) -> Result<Value, String> {
    let entry = args.get("data").cloned().unwrap_or_else(|| json!({}));
    std::fs::create_dir_all(&state.data_dir)
        .map_err(|e| route_error(format!("query_history_write create_dir_all: {e}")))?;
    let path = state.data_dir.join("query-history.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| route_error(format!("query_history_write open: {e}")))?;
    let mut line = serde_json::to_string(&entry)
        .map_err(|e| route_error(format!("query_history_write serialize: {e}")))?;
    line.push('\n');
    file.write_all(line.as_bytes())
        .map_err(|e| route_error(format!("query_history_write append: {e}")))?;
    Ok(json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_appends_json_line() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-qhist-{nanos}"));
        let state = DbgmState::with_data_dir(dir.clone());

        let result = write(
            &state,
            json!({"data": {"sql": "SELECT 1", "conid": "c1", "database": "main", "date": "2026-01-01"}}),
        )
        .unwrap();
        assert_eq!(result, json!({ "status": "ok" }));

        let content = std::fs::read_to_string(dir.join("query-history.jsonl")).unwrap();
        assert!(content.contains("\"SELECT 1\""));
        assert!(content.ends_with('\n'));
    }
}
