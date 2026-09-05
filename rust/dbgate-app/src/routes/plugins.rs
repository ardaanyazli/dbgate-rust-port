//! Bridge routes for the plugins metadata surface
//! (`packages/api/src/controllers/plugins.js`).
//!
//! Only the `plugins_installed` route is in scope for this port: it lists
//! the engine metadata derived from the registered drivers. Frontend plugin
//! *script* loading (`plugins/script`) is explicitly out of scope.

use serde_json::{json, Value};

use crate::DbgmState;

/// `plugins_installed` — list installed engines as plugin metadata entries.
pub fn installed(state: &DbgmState, _args: Value) -> Result<Value, String> {
    let metadata = state.drivers.list_metadata();
    let entries = metadata
        .into_iter()
        .map(|m| {
            json!({
                "name": m.name,
                "displayName": m.display_name,
                "engine": m.engine,
                "defaultPort": m.default_port,
                "databaseEngine": m.database_engine,
                "icons": m.icons,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!(entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::dispatch;
    use std::path::PathBuf;

    fn test_state() -> DbgmState {
        DbgmState::with_data_dir(PathBuf::from("/tmp/dbgate-plugins-test"))
    }

    #[test]
    fn installed_returns_engine_metadata() {
        let state = test_state();
        let res = installed(&state, json!({})).unwrap();
        let arr = res.as_array().expect("array");
        assert_eq!(arr.len(), 8);

        let mysql = arr
            .iter()
            .find(|e| e["engine"] == "mysql@dbgate-plugin-mysql")
            .expect("mysql entry");
        assert_eq!(mysql["name"], "dbgate-plugin-mysql");
        assert_eq!(mysql["displayName"], "MySQL");
        assert_eq!(mysql["defaultPort"], 3306);
        assert_eq!(mysql["databaseEngine"], "mysql");
        assert!(mysql["icons"].is_object());

        let mariadb = arr
            .iter()
            .find(|e| e["engine"] == "mariadb@dbgate-plugin-mysql")
            .expect("mariadb entry");
        assert_eq!(mariadb["name"], "dbgate-plugin-mysql");
        assert_eq!(mariadb["databaseEngine"], "mariadb");

        let sqlite = arr
            .iter()
            .find(|e| e["engine"] == "sqlite@dbgate-plugin-sqlite")
            .expect("sqlite entry");
        assert_eq!(sqlite["defaultPort"], Value::Null);
    }

    #[test]
    fn plugins_installed_route_dispatches() {
        let state = test_state();
        let res = dispatch(&state, "plugins_installed", json!({})).unwrap();
        assert_eq!(res.as_array().expect("array").len(), 8);
    }
}