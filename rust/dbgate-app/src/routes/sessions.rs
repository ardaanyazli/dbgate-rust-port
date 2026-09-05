//! Session bridge routes.
//!
//! Mirrors `packages/api/src/controllers/sessions.js`: a session owns an
//! independent connection created from the saved connection definition
//! (the Rust equivalent of the JS fork-per-session subprocess model), and
//! `executeReader` streams results to the frontend through the `session-*`
//! Tauri events. Forked child processes, subprocess management and SQL
//! front matter parsing are explicitly out of scope.

use serde_json::{json, Value};

use dbgate_core::driver::QueryOptions;

use super::database_connections::load_definition;
use super::route_error;
use crate::events::{emit_session_done, emit_session_info, emit_session_recordset};
use crate::{DbgmState, Session};

/// `sessions_create` — open a session with a dedicated connection.
pub fn create(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("create missing conid"))?;
    let database = args.get("database").and_then(Value::as_str).map(String::from);

    let def = load_definition(state, conid)?;
    let driver = state
        .drivers
        .get(&def.engine)
        .ok_or_else(|| route_error(format!("No driver registered for engine '{}'", def.engine)))?;
    let handle = driver.connect(&def).map_err(|e| route_error(e.to_string()))?;

    let sesid = uuid::Uuid::new_v4().to_string();
    state
        .sessions
        .lock()
        .map_err(|_| route_error("sessions lock poisoned"))?
        .insert(
            sesid.clone(),
            Session {
                sesid: sesid.clone(),
                conid: conid.to_string(),
                database,
                driver,
                handle,
            },
        );

    Ok(json!({ "sesid": sesid }))
}

/// `sessions_execute_reader` — run SQL on a session, streaming the result
/// to the frontend via `session-recordset-*`, `session-info-*` and
/// `session-done-*` events, and return the full result set.
pub fn execute_reader(state: &DbgmState, args: Value) -> Result<Value, String> {
    let sesid = args
        .get("sesid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_reader missing sesid"))?;
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_reader missing sql"))?;

    let (session_id, target, query_result) = {
        let guard = state
            .sessions
            .lock()
            .map_err(|_| route_error("sessions lock poisoned"))?;
        let session = guard
            .get(sesid)
            .ok_or_else(|| route_error(format!("Invalid session {sesid}")))?;
        let options = QueryOptions::default();
        let target = session
            .database
            .as_deref()
            .unwrap_or(&session.conid)
            .to_string();
        let result = session
            .driver
            .query(&session.handle, sql, &options)
            .map_err(|e| route_error(e.to_string()))?;
        (session.sesid.clone(), target, result)
    };

    emit_session_recordset(state, &session_id, 0, &query_result.columns);
    emit_session_info(
        state,
        &session_id,
        &format!(
            "Query returned {} rows on {}",
            query_result.rows.len(),
            target
        ),
        "info",
    );
    emit_session_done(state, &session_id);

    serde_json::to_value(query_result).map_err(|e| route_error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::routes::connections::ConnectionsStore;

    fn test_state() -> DbgmState {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-sessions-{nanos}"));
        DbgmState::with_data_dir(dir)
    }

    fn save_sqlite_connection(state: &DbgmState, conid: &str) {
        let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
        store
            .insert(json!({
                "_id": conid,
                "engine": "sqlite@dbgate-plugin-sqlite",
                "name": "saved",
                "databaseFile": ":memory:"
            }))
            .unwrap();
    }

    fn recording_emitter(
        state: &DbgmState,
    ) -> Arc<Mutex<Vec<(String, Value)>>> {
        let emitted: Arc<Mutex<Vec<(String, Value)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = emitted.clone();
        state.install_event_emitter(Arc::new(move |event, payload| {
            sink.lock().unwrap().push((event.to_string(), payload));
        }));
        emitted
    }

    #[test]
    fn create_returns_sesid_and_opens_session() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let result = create(&state, json!({"conid": "saved-sqlite", "database": "main"})).unwrap();
        let sesid = result["sesid"].as_str().unwrap();
        assert!(!sesid.is_empty());

        let guard = state.sessions.lock().unwrap();
        let session = guard.get(sesid).unwrap();
        assert_eq!(session.database.as_deref(), Some("main"));
    }

    #[test]
    fn create_unknown_conid_errors() {
        let state = test_state();
        let err = create(&state, json!({"conid": "nope"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn execute_reader_returns_rows_and_emits_events() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);

        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        let result = execute_reader(
            &state,
            json!({"sesid": sesid, "sql": "SELECT 1 AS one"}),
        )
        .unwrap();

        assert_eq!(result["rows"][0]["one"], json!(1));
        assert_eq!(result["columns"][0]["columnName"], json!("one"));

        let events = emitted.lock().unwrap();
        let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
        assert!(names.contains(&format!("session-recordset-{sesid}").as_str()));
        assert!(names.contains(&format!("session-info-{sesid}").as_str()));
        assert!(names.contains(&format!("session-done-{sesid}").as_str()));

        let recordset = events
            .iter()
            .find(|(name, _)| name == &format!("session-recordset-{sesid}"))
            .unwrap();
        assert_eq!(recordset.1["resultIndex"], json!(0));
        assert_eq!(recordset.1["columns"][0]["columnName"], json!("one"));
        let info = events
            .iter()
            .find(|(name, _)| name == &format!("session-info-{sesid}"))
            .unwrap();
        assert_eq!(info.1["severity"], json!("info"));
    }

    #[test]
    fn execute_reader_unknown_sesid_errors() {
        let state = test_state();
        let err = execute_reader(&state, json!({"sesid": "nope", "sql": "SELECT 1"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid session nope"));
    }

    #[test]
    fn session_routes_dispatch() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let created = crate::routes::dispatch(
            &state,
            "sessions_create",
            json!({"conid": "saved-sqlite"}),
        )
        .unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let result = crate::routes::dispatch(
            &state,
            "sessions_execute_reader",
            json!({"sesid": sesid, "sql": "SELECT 2 AS two"}),
        )
        .unwrap();
        assert_eq!(result["rows"][0]["two"], json!(2));
    }
}