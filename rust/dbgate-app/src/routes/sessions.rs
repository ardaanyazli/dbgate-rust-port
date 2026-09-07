//! Session bridge routes.
//!
//! Mirrors `packages/api/src/controllers/sessions.js`: a session owns an
//! independent connection created from the saved connection definition
//! (the Rust equivalent of the JS fork-per-session subprocess model), and
//! `executeReader` streams results to the frontend through the `session-*`
//! Tauri events. Forked child processes, subprocess management and SQL
//! front matter parsing are explicitly out of scope.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use dbgate_core::driver::{DbHandle, EngineDriver, StreamSeverity, StreamSink};

use super::database_connections::{ensure_connected, load_definition};
use super::route_error;
use crate::events::{
    emit_session_closed, emit_session_done, emit_session_info, emit_session_jslid_done,
    emit_session_recordset,
};
use crate::{DbgmState, Session};

fn severity_str(severity: StreamSeverity) -> &'static str {
    match severity {
        StreamSeverity::Info => "info",
        StreamSeverity::Warning => "warning",
        StreamSeverity::Error => "error",
        StreamSeverity::Debug => "debug",
    }
}

/// Run `sql` through the driver, collecting each recordset into the jslid
/// store. When `sesid` is set the `session-recordset-{sesid}` /
/// `session-info-{sesid}` / `session-done-{sesid}` events are emitted (the
/// SQL-editor flow); when `emit_jslid_done` is set every produced jslid is
/// finished with a `session-jslid-done-{jslid}` event (the table-data
/// reader flow). Returns the produced jslids in recordset order.
fn stream_into_jsl(
    state: &DbgmState,
    driver: &Arc<dyn EngineDriver>,
    handle: &DbHandle,
    sql: &str,
    sesid: Option<&str>,
    emit_jslid_done: bool,
) -> Result<Vec<String>, String> {
    let jslids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let current: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let sink = StreamSink {
        on_recordset: &|columns| {
            let jslid = state.jsl_create(columns.to_vec());
            *current.lock().unwrap() = Some(jslid.clone());
            let mut list = jslids.lock().unwrap();
            let result_index = list.len();
            list.push(jslid.clone());
            if let Some(sesid) = sesid {
                emit_session_recordset(state, sesid, result_index, &jslid, columns);
            }
        },
        on_row: &|row| {
            if let Some(jslid) = current.lock().unwrap().as_ref() {
                state.jsl_push_row(jslid, row);
            }
        },
        on_info: &|info| {
            if let Some(sesid) = sesid {
                emit_session_info(state, sesid, &info.message, severity_str(info.severity));
            }
        },
        on_done: &|| {
            for jslid in jslids.lock().unwrap().iter() {
                state.jsl_finish(jslid);
            }
            if let Some(sesid) = sesid {
                emit_session_done(state, sesid);
            }
            if emit_jslid_done {
                for jslid in jslids.lock().unwrap().iter() {
                    emit_session_jslid_done(state, jslid);
                }
            }
        },
    };

    driver
        .stream(handle, sql, &sink)
        .map_err(|e| route_error(e.to_string()))?;
    let jslids_out = jslids.lock().unwrap().clone();
    Ok(jslids_out)
}

/// `sessions_create` — open a session with a dedicated connection.
pub fn create(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("create missing conid"))?;
    let database = args
        .get("database")
        .and_then(Value::as_str)
        .map(String::from);

    let def = load_definition(state, conid)?;
    let driver = state
        .drivers
        .get(&def.engine)
        .ok_or_else(|| route_error(format!("No driver registered for engine '{}'", def.engine)))?;
    let handle = driver
        .connect(&def)
        .map_err(|e| route_error(e.to_string()))?;

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
                isolation_level: None,
                driver,
                handle,
            },
        );

    Ok(json!({ "sesid": sesid }))
}

/// `sessions_execute_reader` — run SQL on a connection (used by the table
/// data grid `QueryDataTab`), collecting the result into a jslid and
/// returning `{ jslid }`. Row pushes are streamed to the frontend via
/// `jsldata-stats-{jslid}` events and completion signals
/// `session-jslid-done-{jslid}`.
pub fn execute_reader(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_reader missing conid"))?;
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_reader missing sql"))?;

    ensure_connected(state, conid)?;
    let jslids = {
        let guard = state
            .connections
            .lock()
            .map_err(|_| route_error("connections lock poisoned"))?;
        let conn = guard
            .get(conid)
            .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
        stream_into_jsl(state, &conn.driver, &conn.handle, sql, None, true)?
    };

    let jslid = jslids
        .first()
        .cloned()
        .ok_or_else(|| route_error("execute_reader produced no result set"))?;
    Ok(json!({ "jslid": jslid }))
}

/// `sessions_execute_query` — run a SQL script on a session (the SQL editor
/// flow), creating one jslid per recordset. Emits `session-recordset-{sesid}`
/// (`{ jslid, resultIndex }`), `session-info-{sesid}` and
/// `session-done-{sesid}` so `ResultTabs`/`QueryTab` can render the results.
pub fn execute_query(state: &DbgmState, args: Value) -> Result<Value, String> {
    let sesid = args
        .get("sesid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_query missing sesid"))?;
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("execute_query missing sql"))?;

    let jslids = {
        let guard = state
            .sessions
            .lock()
            .map_err(|_| route_error("sessions lock poisoned"))?;
        let session = guard
            .get(sesid)
            .ok_or_else(|| route_error(format!("Invalid session {sesid}")))?;
        stream_into_jsl(
            state,
            &session.driver,
            &session.handle,
            sql,
            Some(sesid),
            false,
        )?
    };

    Ok(json!({ "status": "ok", "jslids": jslids }))
}

/// `sessions_stop_loading_reader` — drop a reader's jslid (reader
/// cancellation from `LoadingDataGridCore`). Idempotent: unknown jslids are
/// not an error.
pub fn stop_loading_reader(state: &DbgmState, args: Value) -> Result<Value, String> {
    let jslid = args
        .get("jslid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("stop_loading_reader missing jslid"))?;
    state.jsl_remove(jslid);
    Ok(json!({ "status": "ok" }))
}

/// `sessions_set_isolation_level` — store the session's transaction
/// isolation level. Mirrors `sessions.js` `setIsolationLevel`; the level is
/// kept for future transaction handling (no SET is executed yet).
pub fn set_isolation_level(state: &DbgmState, args: Value) -> Result<Value, String> {
    let sesid = args
        .get("sesid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("set_isolation_level missing sesid"))?;
    let level = args
        .get("level")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("set_isolation_level missing level"))?;

    let mut guard = state
        .sessions
        .lock()
        .map_err(|_| route_error("sessions lock poisoned"))?;
    let session = guard
        .get_mut(sesid)
        .ok_or_else(|| route_error(format!("Invalid session {sesid}")))?;
    if session.isolation_level.as_deref() == Some(level) {
        return Ok(json!({ "status": "ok" }));
    }
    session.isolation_level = Some(level.to_string());

    Ok(json!({ "status": "ok" }))
}

/// `sessions_kill` — remove the session, closing its connection and
/// emitting the `session-closed-{sesid}` event.
pub fn kill(state: &DbgmState, args: Value) -> Result<Value, String> {
    let sesid = args
        .get("sesid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("kill missing sesid"))?;

    let session = state
        .sessions
        .lock()
        .map_err(|_| route_error("sessions lock poisoned"))?
        .remove(sesid)
        .ok_or_else(|| route_error(format!("Invalid session {sesid}")))?;
    session
        .driver
        .close(session.handle)
        .map_err(|e| route_error(e.to_string()))?;
    emit_session_closed(state, sesid);

    Ok(json!({ "status": "ok" }))
}

/// `sessions_ping` — report whether the session still exists. Matches the
/// `sessionPinger.ts` contract: `{ state: "ok" }` when alive,
/// `{ state: "missing" }` when not (an error here would be swallowed by
/// the frontend's `.catch(() => true)` and misreport dead sessions as
/// alive), identical to `sessions.js` `ping`.
pub fn ping(state: &DbgmState, args: Value) -> Result<Value, String> {
    let sesid = args
        .get("sesid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("ping missing sesid"))?;

    let alive = state
        .sessions
        .lock()
        .map_err(|_| route_error("sessions lock poisoned"))?
        .contains_key(sesid);

    Ok(json!({ "state": if alive { "ok" } else { "missing" } }))
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

    fn recording_emitter(state: &DbgmState) -> Arc<Mutex<Vec<(String, Value)>>> {
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
    fn execute_reader_returns_jslid_and_emits_jslid_done() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);

        let result = execute_reader(
            &state,
            json!({"conid": "saved-sqlite", "database": "main", "sql": "SELECT 1 AS one"}),
        )
        .unwrap();
        let jslid = result["jslid"].as_str().unwrap();

        let stats = state.jsl_stats(jslid).unwrap();
        assert_eq!(stats["rowCount"], json!(1));
        assert_eq!(stats["isFinished"], json!(true));
        let rows = state.jsl_rows(jslid, 0, 100).unwrap();
        assert_eq!(rows[0]["one"], json!(1));

        let events = emitted.lock().unwrap();
        assert!(events
            .iter()
            .any(|(name, _)| name == &format!("session-jslid-done-{jslid}")));
        assert!(events
            .iter()
            .all(|(name, _)| !name.starts_with("session-recordset-")));
    }

    #[test]
    fn execute_reader_unknown_conid_errors() {
        let state = test_state();
        let err = execute_reader(&state, json!({"conid": "nope", "sql": "SELECT 1"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn execute_reader_auto_connects_from_saved_definition() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let result = execute_reader(
            &state,
            json!({"conid": "saved-sqlite", "sql": "SELECT 1 AS one"}),
        )
        .unwrap();
        let jslid = result["jslid"].as_str().unwrap();

        assert!(state
            .connections
            .lock()
            .unwrap()
            .contains_key("saved-sqlite"));
        assert_eq!(state.jsl_stats(jslid).unwrap()["rowCount"], json!(1));
    }

    #[test]
    fn execute_query_emits_recordsets_and_done() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        let result =
            execute_query(&state, json!({"sesid": sesid, "sql": "SELECT 1 AS one"})).unwrap();
        assert_eq!(result["status"], json!("ok"));
        let jslids = result["jslids"].as_array().unwrap();
        assert_eq!(jslids.len(), 1);
        let jslid = jslids[0].as_str().unwrap();

        assert_eq!(state.jsl_stats(jslid).unwrap()["rowCount"], json!(1));
        assert_eq!(state.jsl_stats(jslid).unwrap()["isFinished"], json!(true));

        let events = emitted.lock().unwrap();
        let recordset = events
            .iter()
            .find(|(name, _)| name == &format!("session-recordset-{sesid}"))
            .expect("recordset event");
        assert_eq!(recordset.1["jslid"], json!(jslid));
        assert_eq!(recordset.1["resultIndex"], json!(0));
        assert_eq!(recordset.1["columns"][0]["columnName"], json!("one"));
        assert!(events
            .iter()
            .any(|(name, _)| name == &format!("session-done-{sesid}")));
    }

    #[test]
    fn execute_query_emits_info_for_affected_rows() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        execute_query(
            &state,
            json!({"sesid": sesid, "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"}),
        )
        .unwrap();
        let result = execute_query(
            &state,
            json!({"sesid": sesid, "sql": "INSERT INTO t (name) VALUES ('x')"}),
        )
        .unwrap();
        assert_eq!(result["jslids"].as_array().unwrap().len(), 0);

        let events = emitted.lock().unwrap();
        let info = events
            .iter()
            .find(|(name, _)| name == &format!("session-info-{sesid}"))
            .expect("info event");
        assert_eq!(info.1["message"], json!("1 rows affected"));
        assert_eq!(info.1["severity"], json!("info"));
    }

    #[test]
    fn execute_query_multiple_statements_produce_multiple_recordsets() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        let result = execute_query(
            &state,
            json!({"sesid": sesid, "sql": "SELECT 1 AS one; SELECT 2 AS two"}),
        )
        .unwrap();
        let jslids = result["jslids"].as_array().unwrap();
        assert_eq!(jslids.len(), 2);
        assert_eq!(
            state.jsl_stats(jslids[0].as_str().unwrap()).unwrap()["rowCount"],
            json!(1)
        );
        assert_eq!(
            state.jsl_stats(jslids[1].as_str().unwrap()).unwrap()["rowCount"],
            json!(1)
        );
        assert_eq!(
            state.jsl_rows(jslids[1].as_str().unwrap(), 0, 10).unwrap()[0]["two"],
            json!(2)
        );

        let events = emitted.lock().unwrap();
        let indices: Vec<u64> = events
            .iter()
            .filter(|(name, _)| *name == format!("session-recordset-{sesid}"))
            .map(|(_, payload)| payload["resultIndex"].as_u64().unwrap())
            .collect();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn execute_query_unknown_sesid_errors() {
        let state = test_state();
        let err = execute_query(&state, json!({"sesid": "nope", "sql": "SELECT 1"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid session nope"));
    }

    #[test]
    fn stop_loading_reader_removes_jslid() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();
        let jslid = execute_query(&state, json!({"sesid": sesid, "sql": "SELECT 1"})).unwrap()
            ["jslids"][0]
            .as_str()
            .unwrap()
            .to_string();

        let resp = stop_loading_reader(&state, json!({"jslid": jslid})).unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
        assert!(state.jsl_stats(&jslid).is_none());
        assert!(emitted
            .lock()
            .unwrap()
            .iter()
            .any(|(name, _)| name.starts_with("jsldata-stats-")));
    }

    #[test]
    fn stop_loading_reader_unknown_jslid_is_ok() {
        let state = test_state();
        let resp = stop_loading_reader(&state, json!({"jslid": "nope"})).unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
    }

    #[test]
    fn session_routes_dispatch() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);

        let created =
            crate::routes::dispatch(&state, "sessions_create", json!({"conid": "saved-sqlite"}))
                .unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let result = crate::routes::dispatch(
            &state,
            "sessions_execute_query",
            json!({"sesid": sesid, "sql": "SELECT 2 AS two"}),
        )
        .unwrap();
        let jslid = result["jslids"][0].as_str().unwrap();
        assert_eq!(state.jsl_rows(jslid, 0, 10).unwrap()[0]["two"], json!(2));

        let reader = crate::routes::dispatch(
            &state,
            "sessions_execute_reader",
            json!({"conid": "saved-sqlite", "sql": "SELECT 3 AS three"}),
        )
        .unwrap();
        let reader_jslid = reader["jslid"].as_str().unwrap();
        assert_eq!(
            state.jsl_rows(reader_jslid, 0, 10).unwrap()[0]["three"],
            json!(3)
        );

        let resp = crate::routes::dispatch(
            &state,
            "sessions_stop_loading_reader",
            json!({"jslid": reader_jslid}),
        )
        .unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
        assert!(state.jsl_stats(reader_jslid).is_none());
        assert!(emitted
            .lock()
            .unwrap()
            .iter()
            .any(|(name, _)| name.contains("session-")));
    }

    #[test]
    fn set_isolation_level_returns_ok_and_stores_level() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        let resp =
            set_isolation_level(&state, json!({"sesid": sesid, "level": "serializable"})).unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));

        let guard = state.sessions.lock().unwrap();
        assert_eq!(
            guard.get(&sesid).unwrap().isolation_level.as_deref(),
            Some("serializable")
        );
    }

    #[test]
    fn set_isolation_level_unknown_sesid_errors() {
        let state = test_state();
        let err = set_isolation_level(&state, json!({"sesid": "nope", "level": "serializable"}))
            .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid session nope"));
    }

    #[test]
    fn set_isolation_level_route_dispatches() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let resp = crate::routes::dispatch(
            &state,
            "sessions_set_isolation_level",
            json!({"sesid": sesid, "level": "read committed"}),
        )
        .unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
    }

    #[test]
    fn kill_removes_session_and_emits_closed() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let emitted = recording_emitter(&state);

        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();

        let resp = kill(&state, json!({"sesid": sesid})).unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
        assert!(!state.sessions.lock().unwrap().contains_key(&sesid));

        let events = emitted.lock().unwrap();
        assert!(events
            .iter()
            .any(|(name, _)| name == &format!("session-closed-{sesid}")));
    }

    #[test]
    fn kill_unknown_sesid_errors() {
        let state = test_state();
        let err = kill(&state, json!({"sesid": "nope"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid session nope"));
    }

    #[test]
    fn kill_route_dispatches() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let resp =
            crate::routes::dispatch(&state, "sessions_kill", json!({"sesid": sesid})).unwrap();
        assert_eq!(resp, json!({ "status": "ok" }));
    }

    #[test]
    fn ping_returns_ok_for_live_session() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let resp = ping(&state, json!({"sesid": sesid})).unwrap();
        assert_eq!(resp, json!({ "state": "ok" }));
    }

    #[test]
    fn ping_after_kill_returns_missing() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap().to_string();
        kill(&state, json!({"sesid": sesid})).unwrap();

        let resp = ping(&state, json!({"sesid": sesid})).unwrap();
        assert_eq!(resp, json!({ "state": "missing" }));
    }

    #[test]
    fn ping_route_dispatches() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");
        let created = create(&state, json!({"conid": "saved-sqlite"})).unwrap();
        let sesid = created["sesid"].as_str().unwrap();

        let resp =
            crate::routes::dispatch(&state, "sessions_ping", json!({"sesid": sesid})).unwrap();
        assert_eq!(resp, json!({ "state": "ok" }));
    }
}
