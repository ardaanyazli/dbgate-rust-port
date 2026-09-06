//! Database-connection bridge routes.
//!
//! Mirrors `packages/api/src/controllers/databaseConnections.js` for the
//! SQL execution surface (`sqlSelect`, `runScript`, `syncModel`, `refresh`)
//! against connections opened by the Tauri `open_connection` command.
//! Every handler auto-connects: when a `conid` is not yet open in
//! `DbgmState`, the definition is loaded from `connections.jsonl` and the
//! driver `.connect()` is invoked on the spot. Streaming, bulk export and
//! SSH tunnels are explicitly out of scope.

use serde_json::{json, Value};

use dbgate_core::connection::ConnectionDefinition;
use dbgate_core::driver::QueryOptions;

use super::connections::ConnectionsStore;
use super::route_error;
use crate::{DbgmState, OpenConnection};

/// Load the saved connection definition for `conid` from `connections.jsonl`.
pub(crate) fn load_definition(
    state: &DbgmState,
    conid: &str,
) -> Result<ConnectionDefinition, String> {
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    let saved = store.get(conid)?;
    if saved.is_null() {
        return Err(route_error(format!("Unknown connection {conid}")));
    }
    serde_json::from_value(saved)
        .map_err(|e| route_error(format!("Invalid connection definition: {e}")))
}

/// Bring the connection for `conid` into `DbgmState.connections`, loading
/// its definition from `connections.jsonl` and calling the driver when the
/// connection is not already open.
pub(crate) fn ensure_connected(state: &DbgmState, conid: &str) -> Result<(), String> {
    {
        let guard = state
            .connections
            .lock()
            .map_err(|_| route_error("connections lock poisoned"))?;
        if guard.contains_key(conid) {
            return Ok(());
        }
    }

    let def = load_definition(state, conid)?;
    let driver = state
        .drivers
        .get(&def.engine)
        .ok_or_else(|| route_error(format!("No driver registered for engine '{}'", def.engine)))?;
    let handle = driver
        .connect(&def)
        .map_err(|e| route_error(e.to_string()))?;

    state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?
        .insert(conid.to_string(), OpenConnection { driver, handle });
    Ok(())
}

/// `database_connections_sql_select` — run a SQL SELECT on a connection.
pub fn sql_select(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("sql_select missing conid"))?;
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("sql_select missing sql"))?;
    let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(1000);

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    let options = QueryOptions {
        range: Some((0, limit)),
        ..Default::default()
    };
    let result = conn
        .driver
        .query(&conn.handle, sql, &options)
        .map_err(|e| route_error(e.to_string()))?;
    serde_json::to_value(result).map_err(|e| route_error(e.to_string()))
}

/// `database_connections_run_script` — execute a SQL script on a connection.
pub fn run_script(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("run_script missing conid"))?;
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("run_script missing sql"))?;

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    let options = QueryOptions {
        discard_result: false,
        ..Default::default()
    };
    let result = conn
        .driver
        .query(&conn.handle, sql, &options)
        .map_err(|e| route_error(e.to_string()))?;
    serde_json::to_value(result).map_err(|e| route_error(e.to_string()))
}

/// Run `analyse_full` on the connected database and serialize the result.
/// Shared by `sync_model` and `structure`.
fn analyse(state: &DbgmState, conid: &str) -> Result<Value, String> {
    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    let version = conn
        .driver
        .get_version(&conn.handle)
        .map_err(|e| route_error(e.to_string()))?;
    let info = conn
        .driver
        .analyse_full(&conn.handle, &version.version)
        .map_err(|e| route_error(e.to_string()))?;
    serde_json::to_value(info).map_err(|e| route_error(e.to_string()))
}

/// `database_connections_sync_model` — analyse the connected database.
pub fn sync_model(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("sync_model missing conid"))?;
    analyse(state, conid)
}

/// `database_connections_structure` — full database structure, requested by
/// the `databaseInfoLoader` (`metadataLoaders.ts`) when a connection is
/// opened (the table tree in the navigator).
pub fn structure(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("structure missing conid"))?;
    analyse(state, conid)
}

/// `database_connections_refresh` — no-op marker; structure refresh is
/// already performed by `sync_model`.
pub fn refresh(state: &DbgmState, _args: Value) -> Result<Value, String> {
    let _ = state;
    Ok(json!({ "status": "ok" }))
}

/// `database_connections_call_method` — dispatch a driver metadata method.
pub fn call_method(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("call_method missing conid"))?;
    let method = args
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("call_method missing method"))?;
    let method_args = args.get("args").cloned().unwrap_or_else(|| json!({}));

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;

    match method {
        "getVersion" => {
            let version = conn
                .driver
                .get_version(&conn.handle)
                .map_err(|e| route_error(e.to_string()))?;
            serde_json::to_value(version).map_err(|e| route_error(e.to_string()))
        }
        "listDatabases" => {
            let databases = conn
                .driver
                .list_databases(&conn.handle)
                .map_err(|e| route_error(e.to_string()))?;
            serde_json::to_value(databases).map_err(|e| route_error(e.to_string()))
        }
        "analyseSingleTable" => {
            let name: dbgate_core::dbinfo::NamedObjectInfo = serde_json::from_value(method_args)
                .map_err(|e| route_error(format!("Invalid table name: {e}")))?;
            let table = conn
                .driver
                .analyse_single_table(&conn.handle, &name)
                .map_err(|e| route_error(e.to_string()))?;
            serde_json::to_value(table).map_err(|e| route_error(e.to_string()))
        }
        _ => Err(route_error(format!("Unknown method {method}"))),
    }
}

/// `database_connections_ping` — health-check a connection via `get_version`.
pub fn ping(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("ping missing conid"))?;

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    conn.driver
        .get_version(&conn.handle)
        .map_err(|e| route_error(e.to_string()))?;
    Ok(json!({ "status": "ok" }))
}

/// `database_connections_status` — health-check a connection via
/// `get_version`, mirroring the `connectionStatusLoader` usage in
/// `metadataLoaders.ts`.
pub fn status(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("status missing conid"))?;

    ensure_connected(state, conid)?;
    let guard = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?;
    let conn = guard
        .get(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    conn.driver
        .get_version(&conn.handle)
        .map_err(|e| route_error(e.to_string()))?;
    Ok(json!({ "status": "ok" }))
}

/// `database_connections_disconnect` — remove and close a connection.
pub fn disconnect(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("disconnect missing conid"))?;

    let conn = state
        .connections
        .lock()
        .map_err(|_| route_error("connections lock poisoned"))?
        .remove(conid)
        .ok_or_else(|| route_error(format!("Unknown connection {conid}")))?;
    conn.driver
        .close(conn.handle)
        .map_err(|e| route_error(e.to_string()))?;
    Ok(json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpenConnection;
    use dbgate_core::connection::ConnectionDefinition;
    use dbgate_core::drivers::sqlite::driver_ref;
    use serde_json::json;

    fn test_state() -> DbgmState {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-dbconn-{nanos}"));
        DbgmState::with_data_dir(dir)
    }

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

    #[test]
    fn sql_select_returns_rows_from_sqlite() {
        let state = test_state();
        open_sqlite(&state, "sqlite-test");

        run_script(
            &state,
            json!({"conid": "sqlite-test", "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"}),
        )
        .unwrap();
        run_script(
            &state,
            json!({"conid": "sqlite-test", "sql": "INSERT INTO t (name) VALUES ('a'), ('b')"}),
        )
        .unwrap();

        let result = sql_select(
            &state,
            json!({"conid": "sqlite-test", "sql": "SELECT * FROM t ORDER BY id", "limit": 1000}),
        )
        .unwrap();

        assert_eq!(result["rows"].as_array().unwrap().len(), 2);
        assert_eq!(result["columns"].as_array().unwrap().len(), 2);
        assert_eq!(result["columns"][0]["columnName"], json!("id"));
        assert_eq!(result["rows"][0]["name"], json!("a"));
    }

    #[test]
    fn sql_select_unknown_conid_errors() {
        let state = test_state();
        let err = sql_select(&state, json!({"conid": "nope", "sql": "SELECT 1"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn sql_select_route_dispatches() {
        let state = test_state();
        open_sqlite(&state, "sqlite-route");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_sql_select",
            json!({"conid": "sqlite-route", "sql": "SELECT 1 AS one"}),
        )
        .unwrap();

        assert_eq!(result["rows"][0]["one"], json!(1));
    }

    fn save_sqlite_connection(state: &DbgmState, conid: &str) {
        let path = ConnectionsStore::default_path(state);
        let store = ConnectionsStore::new(path);
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
    fn sql_select_auto_connects_from_saved_definition() {
        let state = test_state();
        save_sqlite_connection(&state, "saved-sqlite");

        let result = sql_select(
            &state,
            json!({"conid": "saved-sqlite", "sql": "SELECT 1 AS one"}),
        )
        .unwrap();

        assert_eq!(result["rows"][0]["one"], json!(1));
        assert!(state
            .connections
            .lock()
            .unwrap()
            .contains_key("saved-sqlite"));
    }

    #[test]
    fn sync_model_returns_database_info() {
        let state = test_state();
        open_sqlite(&state, "sqlite-sync");
        run_script(
            &state,
            json!({"conid": "sqlite-sync", "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"}),
        )
        .unwrap();

        let result = sync_model(&state, json!({"conid": "sqlite-sync"})).unwrap();

        let tables = result["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0]["object"]["pureName"], json!("t"));
        assert_eq!(tables[0]["columns"][0]["columnName"], json!("id"));
    }

    #[test]
    fn sync_model_route_dispatches() {
        let state = test_state();
        save_sqlite_connection(&state, "sync-route");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_sync_model",
            json!({"conid": "sync-route"}),
        )
        .unwrap();

        assert!(result["tables"].is_array());
    }

    #[test]
    fn sync_model_unknown_conid_errors() {
        let state = test_state();
        let err = sync_model(&state, json!({"conid": "nope"})).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn refresh_returns_ok() {
        let state = test_state();
        let result = refresh(&state, json!({})).unwrap();
        assert_eq!(result, json!({ "status": "ok" }));
    }

    #[test]
    fn call_method_get_version_returns_version_text() {
        let state = test_state();
        open_sqlite(&state, "sqlite-version");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_call_method",
            json!({"conid": "sqlite-version", "method": "getVersion"}),
        )
        .unwrap();

        assert!(result["version"].as_str().is_some());
        assert!(result["versionText"]
            .as_str()
            .unwrap()
            .starts_with("SQLite"));
    }

    #[test]
    fn call_method_list_databases_returns_main() {
        let state = test_state();
        open_sqlite(&state, "sqlite-dbs");

        let result = call_method(
            &state,
            json!({"conid": "sqlite-dbs", "method": "listDatabases"}),
        )
        .unwrap();

        let dbs = result.as_array().unwrap();
        assert_eq!(dbs[0]["name"], json!("main"));
    }

    #[test]
    fn call_method_analyse_single_table_returns_table() {
        let state = test_state();
        open_sqlite(&state, "sqlite-opts");
        run_script(
            &state,
            json!({"conid": "sqlite-opts", "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"}),
        )
        .unwrap();

        let result = call_method(
            &state,
            json!({"conid": "sqlite-opts", "method": "analyseSingleTable", "args": {"pureName": "t"}}),
        )
        .unwrap();

        assert_eq!(result["object"]["pureName"], json!("t"));
        assert!(result["columns"].is_array());
    }

    #[test]
    fn call_method_unknown_method_errors() {
        let state = test_state();
        open_sqlite(&state, "sqlite-bad");

        let err =
            call_method(&state, json!({"conid": "sqlite-bad", "method": "bogus"})).unwrap_err();

        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown method bogus"));
    }

    #[test]
    fn ping_returns_ok_for_open_connection() {
        let state = test_state();
        open_sqlite(&state, "sqlite-ping");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_ping",
            json!({"conid": "sqlite-ping"}),
        )
        .unwrap();

        assert_eq!(result, json!({ "status": "ok" }));
    }

    #[test]
    fn ping_unknown_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_ping",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn structure_returns_database_info() {
        let state = test_state();
        open_sqlite(&state, "sqlite-structure");
        run_script(
            &state,
            json!({"conid": "sqlite-structure", "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"}),
        )
        .unwrap();

        let result = crate::routes::dispatch(
            &state,
            "database_connections_structure",
            json!({"conid": "sqlite-structure"}),
        )
        .unwrap();

        let tables = result["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0]["object"]["pureName"], json!("t"));
    }

    #[test]
    fn structure_unknown_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_structure",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn status_returns_ok_for_open_connection() {
        let state = test_state();
        open_sqlite(&state, "sqlite-status");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_status",
            json!({"conid": "sqlite-status"}),
        )
        .unwrap();

        assert_eq!(result, json!({ "status": "ok" }));
    }

    #[test]
    fn disconnect_removes_connection() {
        let state = test_state();
        open_sqlite(&state, "sqlite-disc");

        let result = crate::routes::dispatch(
            &state,
            "database_connections_disconnect",
            json!({"conid": "sqlite-disc"}),
        )
        .unwrap();

        assert_eq!(result, json!({ "status": "ok" }));
        assert!(!state
            .connections
            .lock()
            .unwrap()
            .contains_key("sqlite-disc"));
    }

    #[test]
    fn disconnect_unknown_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_disconnect",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }
}
