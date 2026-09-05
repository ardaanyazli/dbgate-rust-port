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

/// Emit `session-recordset-{sesid}` carrying the result index and column
/// metadata, mirroring the JS `handle_recordset` event payload shape
/// (`{ jslid, resultIndex }`, with column details on the `columns` field).
pub fn emit_session_recordset(
    state: &DbgmState,
    sesid: &str,
    result_index: usize,
    columns: &[QueryResultColumn],
) {
    state.emit_event(
        &format!("session-recordset-{sesid}"),
        json!({ "resultIndex": result_index, "columns": columns }),
    );
}