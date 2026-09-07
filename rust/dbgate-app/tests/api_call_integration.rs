//! Integration tests driving the `api_call` bridge dispatch end-to-end,
//! mirroring the flow the Svelte frontend performs on startup.

use dbgate_app_lib::routes::dispatch;
use dbgate_app_lib::DbgmState;
use serde_json::{json, Value};

fn test_state(tag: &str) -> DbgmState {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("dbgate-integration-{tag}-{nanos}"));
    DbgmState::with_data_dir(dir)
}

#[test]
fn connections_list_returns_empty_array_fresh() {
    let state = test_state("conns-empty");
    let res = dispatch(&state, "connections_list", json!({})).unwrap();
    assert_eq!(res, Value::Array(vec![]));
}

#[test]
fn plugins_installed_returns_one_row_per_package() {
    let state = test_state("plugins");
    let res = dispatch(&state, "plugins_installed", json!({})).unwrap();
    let arr = res.as_array().expect("array");
    // one row per uniquely named package; the mysql package (mysql + mariadb
    // engines) must not be listed twice, or the frontend eval-loop would
    // register both drivers twice and crash the engine dropdown keyed-each
    assert_eq!(arr.len(), 7);

    let mut names: Vec<&str> = arr.iter().map(|e| e["name"].as_str().unwrap()).collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 7);
    assert!(names.contains(&"dbgate-plugin-mysql"));
}

#[test]
fn plugins_script_returns_js_module_for_sqlite() {
    let state = test_state("plugins-script");
    let res = dispatch(
        &state,
        "plugins_script",
        json!({ "packageName": "dbgate-plugin-sqlite" }),
    )
    .unwrap();
    let js = res.as_str().expect("js string");
    assert!(js.starts_with("var plugin = "));
    assert!(js.contains("\"__esModule\": true"));
    assert!(js.contains("\"drivers\": ["));
    assert!(js.contains("engine: \"sqlite@dbgate-plugin-sqlite\""));
    assert!(js.contains("showConnectionField"));
    assert!(js.contains("beforeConnectionSave"));
}

#[test]
fn plugins_script_returns_empty_module_for_unknown_package() {
    let state = test_state("plugins-script-unknown");
    let res = dispatch(
        &state,
        "plugins_script",
        json!({ "packageName": "dbgate-plugin-does-not-exist" }),
    )
    .unwrap();
    let js = res.as_str().expect("js string");
    assert!(js.starts_with("var plugin = "));
    assert!(js.contains("\"__esModule\": true"));
    assert!(js.contains("\"drivers\": ["));
    // no engine produced any driver object
    assert!(!js.contains("showConnectionField"));
}

#[test]
fn apps_get_all_apps_returns_empty_array() {
    let state = test_state("apps");
    let res = dispatch(&state, "apps_get_all_apps", json!({})).unwrap();
    assert_eq!(res, Value::Array(vec![]));
}

#[test]
fn files_favorites_returns_empty_array() {
    let state = test_state("files");
    let res = dispatch(&state, "files_favorites", json!({})).unwrap();
    // must be an array so `OpenTabsOnStartup.svelte` can call list.filter;
    // an errorMessage object would throw "list.filter is not a function"
    assert_eq!(res, Value::Array(vec![]));
}

#[test]
fn config_get_returns_config_object() {
    let state = test_state("config");
    let res = dispatch(&state, "config_get", json!({})).unwrap();
    assert_eq!(res["isElectron"], json!(true));
    assert_eq!(res["isTauri"], json!(true));
    assert_eq!(res["skipAllAuth"], json!(true));
}

#[test]
fn sqlite_save_list_select_round_trip() {
    let state = test_state("sqlite-flow");

    let saved = dispatch(
        &state,
        "connections_save",
        json!({
            "_id": "saved-sqlite",
            "engine": "sqlite@dbgate-plugin-sqlite",
            "name": "saved",
            "databaseFile": ":memory:"
        }),
    )
    .unwrap();
    assert_eq!(saved["_id"], json!("saved-sqlite"));

    let list = dispatch(&state, "connections_list", json!({})).unwrap();
    let arr = list.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["_id"], json!("saved-sqlite"));

    let rows = dispatch(
        &state,
        "database_connections_sql_select",
        json!({ "conid": "saved-sqlite", "sql": "SELECT 1 AS one" }),
    )
    .unwrap();
    assert_eq!(rows["rows"][0]["one"], json!(1));
}
