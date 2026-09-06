//! Event emission for the database session bridge.
//!
//! Mirrors the `session-*` events emitted by the Electron API
//! (`packages/api/src/controllers/sessions.js`) with the same event-name
//! scheme, so the unchanged Svelte frontend (`apiOn('session-…', …)`)
//! receives recordset/info/done/closed notifications while a query runs.

use dbgate_core::query::QueryResultColumn;
use serde_json::json;

use crate::DbgmState;

pub fn emit_session_info(state: &DbgmState, sesid: &str, message: &str, severity: &str) {
    state.emit_event(
        &format!("session-info-{sesid}"),
        json!({ "message": message, "severity": severity }),
    );
}

pub fn emit_session_done(state: &DbgmState, sesid: &str) {
    state.emit_event(&format!("session-done-{sesid}"), json!({}));
}

/// Emit `session-recordset-{sesid}` carrying the result jslid, index and
/// column metadata, mirroring the JS `handle_recordset` event payload shape
/// (`{ jslid, resultIndex }`, with column details on the `columns` field).
pub fn emit_session_recordset(
    state: &DbgmState,
    sesid: &str,
    result_index: usize,
    jslid: &str,
    columns: &[QueryResultColumn],
) {
    state.emit_event(
        &format!("session-recordset-{sesid}"),
        json!({ "jslid": jslid, "resultIndex": result_index, "columns": columns }),
    );
}

/// Emit `jsldata-stats-{jslid}` with the row count / change index of a
/// result set, mirroring the JS `handleJslDataStats` event payload
/// (`{ rowCount, changeIndex, isFinished }`) used by `fetchAll.ts` and the
/// data grids to track streaming progress.
pub fn emit_jsl_stats(
    state: &DbgmState,
    jslid: &str,
    row_count: usize,
    change_index: u64,
    is_finished: bool,
) {
    state.emit_event(
        &format!("jsldata-stats-{jslid}"),
        json!({ "rowCount": row_count, "changeIndex": change_index, "isFinished": is_finished }),
    );
}

/// Emit `session-jslid-done-{jslid}` when a table-data reader finishes,
/// mirroring the JS `LoadingDataGridCore.startFetchAll` completion signal.
pub fn emit_session_jslid_done(state: &DbgmState, jslid: &str) {
    state.emit_event(&format!("session-jslid-done-{jslid}"), json!({}));
}

pub fn emit_session_closed(state: &DbgmState, sesid: &str) {
    state.emit_event(&format!("session-closed-{sesid}"), json!({}));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn test_state() -> DbgmState {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-events-{nanos}"));
        DbgmState::with_data_dir(dir)
    }

    fn recording_emitter(state: &DbgmState) -> Arc<Mutex<Vec<(String, serde_json::Value)>>> {
        let emitted: Arc<Mutex<Vec<(String, serde_json::Value)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let sink = emitted.clone();
        state.install_event_emitter(Arc::new(move |event, payload| {
            sink.lock().unwrap().push((event.to_string(), payload));
        }));
        emitted
    }

    #[test]
    fn info_emits_session_info_with_message_and_severity() {
        let state = test_state();
        let emitted = recording_emitter(&state);

        emit_session_info(&state, "test-ses", "Query returned 2 rows", "info");

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "session-info-test-ses");
        assert_eq!(events[0].1["message"], json!("Query returned 2 rows"));
        assert_eq!(events[0].1["severity"], json!("info"));
    }

    #[test]
    fn done_emits_session_done() {
        let state = test_state();
        let emitted = recording_emitter(&state);

        emit_session_done(&state, "test-ses");

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "session-done-test-ses");
    }

    #[test]
    fn recordset_emits_jslid_result_index_and_columns() {
        let state = test_state();
        let emitted = recording_emitter(&state);
        let columns = vec![QueryResultColumn {
            column_name: "one".into(),
            ..Default::default()
        }];

        emit_session_recordset(&state, "test-ses", 0, "jsl-1", &columns);

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "session-recordset-test-ses");
        assert_eq!(events[0].1["jslid"], json!("jsl-1"));
        assert_eq!(events[0].1["resultIndex"], json!(0));
        assert_eq!(events[0].1["columns"][0]["columnName"], json!("one"));
    }

    #[test]
    fn jsl_stats_emits_row_count_change_index_and_finished() {
        let state = test_state();
        let emitted = recording_emitter(&state);

        emit_jsl_stats(&state, "jsl-1", 5, 5, false);

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "jsldata-stats-jsl-1");
        assert_eq!(events[0].1["rowCount"], json!(5));
        assert_eq!(events[0].1["changeIndex"], json!(5));
        assert_eq!(events[0].1["isFinished"], json!(false));
    }

    #[test]
    fn session_jslid_done_emits_done_event() {
        let state = test_state();
        let emitted = recording_emitter(&state);

        emit_session_jslid_done(&state, "jsl-1");

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "session-jslid-done-jsl-1");
    }

    #[test]
    fn closed_emits_session_closed() {
        let state = test_state();
        let emitted = recording_emitter(&state);

        emit_session_closed(&state, "test-ses");

        let events = emitted.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "session-closed-test-ses");
    }
}
