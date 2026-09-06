//! `server-connections/*` bridge routes.
//!
//! Mirrors `packages/api/src/controllers/serverConnections.js`: this is the
//! multi-database path of the connection-open flow (a connection without a
//! pinned database first lists databases via `list-databases`, then the
//! frontend switches into one with `database-connections/*`). `refresh` is
//! a no-op marker matching `databaseConnections.js refresh`.

use serde_json::{json, Value};

use super::database_connections::ensure_connected;
use super::route_error;
use crate::DbgmState;

pub fn refresh(state: &DbgmState, _args: Value) -> Result<Value, String> {
    let _ = state;
    Ok(json!({ "status": "ok" }))
}

pub fn list_databases(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("list_databases missing conid"))?;

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    let databases = conn
        .driver
        .list_databases(&conn.handle)
        .map_err(|e| route_error(e.to_string()))?;
    serde_json::to_value(databases).map_err(|e| route_error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> DbgmState {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-srvconn-{nanos}"));
        DbgmState::with_data_dir(dir)
    }

    fn save_sqlite_connection(state: &DbgmState, conid: &str) {
        let store = crate::routes::connections::ConnectionsStore::new(
            crate::routes::connections::ConnectionsStore::default_path(state),
        );
        store
            .insert(json!({
                "_id": conid,
                "engine": "sqlite@dbgate-plugin-sqlite",
                "name": "saved",
                "databaseFile": ":memory:"
            }))
            .unwrap();
    }

    #[test]
    fn refresh_returns_ok() {
        let state = test_state();
        let result = refresh(&state, json!({})).unwrap();
        assert_eq!(result, json!({ "status": "ok" }));
    }

    #[test]
    fn list_databases_returns_main_for_sqlite() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let result = crate::routes::dispatch(
            &state,
            "server_connections_list_databases",
            json!({"conid": "saved-sqlite"}),
        )
        .unwrap();

        let dbs = result.as_array().unwrap();
        assert_eq!(dbs[0]["name"], json!("main"));
        assert!(state
            .connections
            .lock()
            .unwrap()
            .contains_key("saved-sqlite"));
    }

    #[test]
    fn list_databases_unknown_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "server_connections_list_databases",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }
}
