//! In-memory result-set store.
//!
//! Replacement for the JS `JsLowLevelDb` streaming model: query results
//! produced by `sessions/execute-query` and `sessions/execute-reader` are
//! stored under a `jslid` (a "JS low-level id") and served to the frontend
//! through the `jsldata/get-stats` and `jsldata/get-rows` routes, with
//! `jsldata-stats-{jslid}` events fed per row push.

use std::collections::HashMap;
use std::sync::Mutex;

use dbgate_core::query::QueryResultColumn;
use serde_json::{json, Value};

use crate::events::emit_jsl_stats;
use crate::DbgmState;

pub struct JslData {
    pub jslid: String,
    pub columns: Vec<QueryResultColumn>,
    pub rows: Vec<Value>,
    pub change_index: u64,
    pub is_finished: bool,
}

impl JslData {
    fn stats(&self) -> Value {
        json!({
            "rowCount": self.rows.len(),
            "changeIndex": self.change_index,
            "isFinished": self.is_finished,
        })
    }
}

impl DbgmState {
    /// The jslid row store, exposed so route handlers can inspect it.
    pub fn jsl_map(&self) -> &Mutex<HashMap<String, JslData>> {
        &self.jsl
    }

    /// Create a new empty result set and return its jslid.
    pub fn jsl_create(&self, columns: Vec<QueryResultColumn>) -> String {
        let jslid = uuid::Uuid::new_v4().to_string();
        self.jsl.lock().unwrap_or_else(|e| e.into_inner()).insert(
            jslid.clone(),
            JslData {
                jslid: jslid.clone(),
                columns,
                rows: Vec::new(),
                change_index: 0,
                is_finished: false,
            },
        );
        emit_jsl_stats(self, &jslid, 0, 0, false);
        jslid
    }

    /// Append a row to a result set, bump its change index and notify the
    /// frontend.
    pub fn jsl_push_row(&self, jslid: &str, row: &Value) {
        let mut guard = self.jsl.lock().unwrap_or_else(|e| e.into_inner());
        let data = guard.get_mut(jslid);
        if let Some(data) = data {
            data.rows.push(row.clone());
            data.change_index += 1;
            let (row_count, change_index, is_finished) =
                (data.rows.len(), data.change_index, data.is_finished);
            drop(guard);
            emit_jsl_stats(self, jslid, row_count, change_index, is_finished);
        }
    }

    /// Mark a result set finished and notify the frontend.
    pub fn jsl_finish(&self, jslid: &str) {
        let (row_count, change_index) = {
            let mut guard = self.jsl.lock().unwrap_or_else(|e| e.into_inner());
            match guard.get_mut(jslid) {
                Some(data) => {
                    data.is_finished = true;
                    (data.rows.len(), data.change_index)
                }
                None => return,
            }
        };
        emit_jsl_stats(self, jslid, row_count, change_index, true);
    }

    /// Remove a result set (reader cancellation).
    pub fn jsl_remove(&self, jslid: &str) {
        self.jsl
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(jslid);
    }

    /// `{ rowCount, changeIndex, isFinished }` for a jslid, if it exists.
    pub fn jsl_stats(&self, jslid: &str) -> Option<Value> {
        self.jsl
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(jslid)
            .map(JslData::stats)
    }

    /// Paginated rows for a jslid, if it exists.
    pub fn jsl_rows(&self, jslid: &str, offset: usize, limit: usize) -> Option<Vec<Value>> {
        self.jsl
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(jslid)
            .map(|data| data.rows.iter().skip(offset).take(limit).cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn create_push_finish_roundtrip() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsl-test"));
        let emitted: Arc<Mutex<Vec<(String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = emitted.clone();
        state.install_event_emitter(Arc::new(move |event, payload| {
            sink.lock().unwrap().push((event.to_string(), payload));
        }));

        let jslid = state.jsl_create(vec![QueryResultColumn {
            column_name: "one".into(),
            ..Default::default()
        }]);
        state.jsl_push_row(&jslid, &json!(1));
        state.jsl_push_row(&jslid, &json!(2));
        state.jsl_finish(&jslid);

        let stats = state.jsl_stats(&jslid).unwrap();
        assert_eq!(stats["rowCount"], json!(2));
        assert_eq!(stats["changeIndex"], json!(2));
        assert_eq!(stats["isFinished"], json!(true));

        let rows = state.jsl_rows(&jslid, 1, 1).unwrap();
        assert_eq!(rows, vec![json!(2)]);

        let events = emitted.lock().unwrap();
        let stats_names: Vec<&str> = events
            .iter()
            .filter(|(name, _)| name.starts_with("jsldata-stats-"))
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(stats_names.contains(&format!("jsldata-stats-{jslid}").as_str()));
        let final_stats = events
            .iter()
            .rev()
            .find(|(name, _)| name == &format!("jsldata-stats-{jslid}"))
            .expect("final stats event emitted");
        assert_eq!(final_stats.1["isFinished"], json!(true));
    }

    #[test]
    fn unknown_jslid_returns_none() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsl-test-2"));
        assert!(state.jsl_stats("nope").is_none());
        assert!(state.jsl_rows("nope", 0, 10).is_none());
    }
}
