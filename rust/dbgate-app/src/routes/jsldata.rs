//! `jsldata/*` bridge routes.
//!
//! Serve the in-memory result sets stored under a `jslid` by
//! `sessions/execute-query` and `sessions/execute-reader`, mirroring the
//! Electron `jsldata` node (`packages/api/src/nodes/jsldata.js`): the data
//! grids paginate through `jsldata/get-rows` and track progress via
//! `jsldata/get-stats` plus the `jsldata-stats-{jslid}` events.

use serde_json::{json, Value};

use super::route_error;
use crate::DbgmState;

pub fn get_stats(state: &DbgmState, args: Value) -> Result<Value, String> {
    let jslid = args
        .get("jslid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("get_stats missing jslid"))?;
    state
        .jsl_stats(jslid)
        .ok_or_else(|| route_error(format!("Invalid jslid {jslid}")))
}

pub fn get_rows(state: &DbgmState, args: Value) -> Result<Value, String> {
    let jslid = args
        .get("jslid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("get_rows missing jslid"))?;
    let offset = args.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
    let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(1000) as usize;
    let rows = state
        .jsl_rows(jslid, offset, limit)
        .ok_or_else(|| route_error(format!("Invalid jslid {jslid}")))?;
    Ok(json!({ "rows": rows }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use dbgate_core::connection::ConnectionDefinition;
    use dbgate_core::drivers::sqlite::driver_ref;

    use crate::OpenConnection;

    fn open_sqlite(state: &DbgmState, conid: &str) {
        let driver = driver_ref();
        let def = ConnectionDefinition {
            engine: "sqlite@dbgate-plugin-sqlite".to_string(),
            name: "test".to_string(),
            database_file: Some(":memory:".to_string()),
            ..Default::default()
        };
        let handle = driver.connect(&def).unwrap();
        let mut guard = state.connections.lock().unwrap();
        guard.insert(conid.to_string(), OpenConnection { driver, handle });
    }

    fn seed(state: &DbgmState) -> String {
        crate::routes::dispatch(
            state,
            "sessions_execute_reader",
            json!({"conid": "sqlite-jsl", "sql": "SELECT 1 AS one UNION ALL SELECT 2 UNION ALL SELECT 3"}),
        )
        .unwrap()["jslid"]
            .as_str()
            .unwrap()
            .to_string()
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
    fn get_stats_paginates_rows_and_reports_finished() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsldata-1"));
        recording_emitter(&state);
        open_sqlite(&state, "sqlite-jsl");

        let jslid = seed(&state);

        let stats = get_stats(&state, json!({"jslid": jslid})).unwrap();
        assert_eq!(stats["rowCount"], json!(3));
        assert_eq!(stats["isFinished"], json!(true));

        let page1 = get_rows(&state, json!({"jslid": jslid, "offset": 0, "limit": 2})).unwrap();
        assert_eq!(page1["rows"].as_array().unwrap().len(), 2);
        assert_eq!(page1["rows"][0]["one"], json!(1));

        let page2 = get_rows(&state, json!({"jslid": jslid, "offset": 2, "limit": 2})).unwrap();
        assert_eq!(page2["rows"].as_array().unwrap().len(), 1);
        assert_eq!(page2["rows"][0]["one"], json!(3));
    }

    #[test]
    fn get_stats_unknown_jslid_errors() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsldata-2"));
        let err = get_stats(&state, json!({"jslid": "nope"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid jslid nope"));
    }

    #[test]
    fn get_rows_unknown_jslid_errors() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsldata-3"));
        let err = get_rows(&state, json!({"jslid": "nope"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Invalid jslid nope"));
    }

    #[test]
    fn jsldata_routes_dispatch() {
        let state = DbgmState::with_data_dir(std::env::temp_dir().join("dbgate-jsldata-4"));
        recording_emitter(&state);
        open_sqlite(&state, "sqlite-jsl");

        let jslid = crate::routes::dispatch(
            &state,
            "sessions_execute_reader",
            json!({"conid": "sqlite-jsl", "sql": "SELECT 1 AS one"}),
        )
        .unwrap()["jslid"]
            .as_str()
            .unwrap()
            .to_string();

        let stats =
            crate::routes::dispatch(&state, "jsldata_get_stats", json!({"jslid": jslid})).unwrap();
        assert_eq!(stats["rowCount"], json!(1));

        let rows = crate::routes::dispatch(
            &state,
            "jsldata_get_rows",
            json!({"jslid": jslid, "offset": 0, "limit": 100}),
        )
        .unwrap();
        assert_eq!(rows["rows"][0]["one"], json!(1));
    }
}
