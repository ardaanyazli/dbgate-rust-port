//! Bridge routes for the plugins metadata surface
//! (`packages/api/src/controllers/plugins.js`).
//!
//! Two routes are in scope for this port:
//! - `plugins_installed` — engine metadata derived from the registered
//!   drivers, consumed by `useInstalledPlugins` in the frontend.
//! - `plugins_script` — a generated JavaScript module for one plugin
//!   package. The frontend evals the module (`PluginsProvider.svelte`,
//!   `eval(\`${resp}; plugin\`)`) to build `$extensions.drivers`, so the
//!   driver list drives connection creation, the engine dropdown and the
//!   table browsing widgets.

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

/// JS field list for `showConnectionField` — file-based engines pick a file
/// path, everything else uses the classic server fields.
///
/// The URL-style field (`databaseUrl`) is deliberately omitted: this bridge
/// only registers SQL engines with concrete server/port/database settings.
fn show_connection_field(engine: &str) -> String {
    if engine == "sqlite" {
        "(field, values) => field == 'databaseFile' || field == 'isReadOnly'".to_string()
    } else {
        "(field, values) => ['server', 'port', 'user', 'password', 'database', 'defaultDatabase', 'singleDatabase', 'isReadOnly'].includes(field)"
            .to_string()
    }
}

/// JS splitter options literal for one engine family, matching the values in
/// `dbgate-query-splitter/lib/options.js`. QueryTab spreads the result of
/// `getQuerySplitterOptions('editor')` into the splitter configuration.
fn splitter_options(engine: &str) -> String {
    let base = match engine {
        "sqlite" => json!({
            "skipSeparatorBeginEnd": true,
            "stringsBegins": ["'", "\""],
            "stringsEnds": { "'": "'", "\"": "\"" },
            "stringEscapes": { "'": "'", "\"": "\"" }
        }),
        "mysql" | "mariadb" => json!({
            "allowCustomDelimiter": true,
            "stringsBegins": ["'", "`", "\""],
            "stringsEnds": { "'": "'", "`": "`", "\"": "\"" },
            "stringEscapes": { "'": "\\", "`": "`", "\"": "\\" }
        }),
        "postgres" => json!({
            "allowDollarDollarString": true,
            "stringsBegins": ["'", "\""],
            "stringsEnds": { "'": "'", "\"": "\"" },
            "stringEscapes": { "'": "'", "\"": "\"" }
        }),
        "mssql" => json!({
            "allowSemicolon": false,
            "allowGoDelimiter": true,
            "keepSemicolonInCommands": true,
            "stringsBegins": ["'", "["],
            "stringsEnds": { "'": "'", "[": "]" },
            "stringEscapes": { "'": "'" }
        }),
        "oracle" => json!({
            "allowCustomSqlTerminator": true,
            "allowSlashDelimiter": true,
            "stringsBegins": ["'", "\""],
            "stringsEnds": { "'": "'", "\"": "\"" },
            "stringEscapes": { "'": "'", "\"": "\"" }
        }),
        "firebird" => json!({
            "allowCustomSetTerm": true,
            "skipSeparatorBeginEnd": true,
            "queryParameterStyle": ":"
        }),
        _ => json!({
            "stringsBegins": ["'", "\""],
            "stringsEnds": { "'": "'", "\"": "\"" },
            "stringEscapes": { "'": "'", "\"": "\"" }
        }),
    };
    base.to_string()
}

/// JS `beforeConnectionSave` for file-based engines — always marks the
/// connection as a single database and derives the default database label
/// from the chosen file path, mirroring `dbgate-plugin-sqlite`.
fn sqlite_before_save() -> &'static str {
    "(connection) => {
      const databaseFile = connection && connection.databaseFile;
      const match = databaseFile && String(databaseFile).match(/[\\/]([^\\/]+)$/);
      return { ...connection, singleDatabase: true, defaultDatabase: match ? match[1] : databaseFile };
    }"
}

/// Build the JS driver object literal for one registered engine.
///
/// Spreading `driverBase` (available on `window['DBGATE_PACKAGES']`) gives
/// every default method the frontend calls on drivers (`createDumper`,
/// `getNewObjectTemplates`, `createSaveChangeSetScript`, the default
/// `dialect`, ...), exactly like the real plugin front-end modules do.
fn driver_js(m: &dbgate_core::registry::EngineMetadata) -> String {
    let mut s = String::from("  {\n    ...window['DBGATE_PACKAGES']['dbgate-tools'].driverBase,\n");
    s.push_str(&format!(
        "    engine: {},\n",
        serde_json::to_string(&m.engine).unwrap()
    ));
    s.push_str(&format!(
        "    title: {},\n",
        serde_json::to_string(&m.display_name).unwrap()
    ));
    s.push_str(&format!(
        "    defaultPort: {},\n",
        serde_json::to_string(&m.default_port).unwrap()
    ));
    s.push_str(&format!(
        "    databaseEngine: {},\n",
        serde_json::to_string(&m.database_engine).unwrap()
    ));
    s.push_str("    databaseEngineTypes: [\"sql\"],\n");
    s.push_str(&format!("    icons: {},\n", m.icons));
    s.push_str(&format!(
        "    showConnectionField: {},\n",
        show_connection_field(&m.database_engine)
    ));
    let splitter = splitter_options(&m.database_engine);
    s.push_str(&format!(
        "    getQuerySplitterOptions: (usage) => usage == 'editor'\n      ? {{ ...{}, ignoreComments: true, preventSingleLineSplit: true }}\n      : {},\n",
        splitter, splitter
    ));
    s.push_str("    showConnectionTab: (field) => false");
    if m.database_engine == "sqlite" {
        s.push_str(&format!(
            ",\n    beforeConnectionSave: {}",
            sqlite_before_save()
        ));
    }
    s.push_str("\n  }");
    s
}

/// `plugins_script` — generate the frontend module for one plugin package.
///
/// The response is a JS source snippet of the shape
/// `var plugin = {__esModule: true, default: {drivers: [...]}};` which the
/// frontend evals as `eval(\`${resp}; plugin\`)` and then unwraps via
/// `module.__esModule ? module.default : module`.
///
/// All engines sharing a package name are emitted together (e.g. the mysql
/// package registers both `mysql@` and `mariadb@` engines, and the frontend
/// only calls this route once per package).
pub fn script(state: &DbgmState, args: Value) -> Result<Value, String> {
    let package_name = args
        .get("packageName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let drivers = state
        .drivers
        .list_metadata()
        .into_iter()
        .filter(|m| m.name == package_name)
        .map(|m| driver_js(&m))
        .collect::<Vec<_>>()
        .join(",\n");
    Ok(Value::String(format!(
        "var plugin = {{\"__esModule\": true, \"default\": {{\"drivers\": [\n{}\n]}}}};",
        drivers
    )))
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

    #[test]
    fn script_generates_sqlite_module() {
        let state = test_state();
        let res = script(&state, json!({ "packageName": "dbgate-plugin-sqlite" })).unwrap();
        let js = res.as_str().expect("js string");
        assert!(js.starts_with("var plugin = "));
        assert!(js.contains("\"__esModule\": true"));
        assert!(js.contains("\"drivers\": ["));
        assert!(js.contains("engine: \"sqlite@dbgate-plugin-sqlite\""));
        assert!(js.contains("title: \"SQLite\""));
        assert!(js.contains("databaseEngine: \"sqlite\""));
        assert!(js.contains("databaseEngineTypes: [\"sql\"]"));
        assert!(js.contains("showConnectionField"));
        assert!(js.contains("showConnectionTab"));
        assert!(js.contains("beforeConnectionSave"));
        assert!(js.contains("singleDatabase: true"));
        assert!(js.contains("driverBase"));
    }

    #[test]
    fn script_generates_both_mysql_engines_for_shared_package() {
        let state = test_state();
        let res = script(&state, json!({ "packageName": "dbgate-plugin-mysql" })).unwrap();
        let js = res.as_str().expect("js string");
        assert!(js.contains("engine: \"mysql@dbgate-plugin-mysql\""));
        assert!(js.contains("engine: \"mariadb@dbgate-plugin-mysql\""));
        // two driver object literals
        assert_eq!(js.matches("showConnectionField").count(), 2);
        // sqlite-only method must not leak into the mysql package
        assert!(!js.contains("beforeConnectionSave"));
    }

    #[test]
    fn script_returns_empty_module_for_unknown_package() {
        let state = test_state();
        let res = script(&state, json!({ "packageName": "dbgate-plugin-missing" })).unwrap();
        let js = res.as_str().expect("js string");
        assert!(js.starts_with("var plugin = "));
        assert!(js.contains("\"__esModule\": true"));
        assert!(js.contains("\"drivers\": ["));
        // no engine produced any driver object
        assert!(!js.contains("showConnectionField"));
    }

    #[test]
    fn script_evals_in_node_when_available() {
        // The real contract is `eval(\`${resp}; plugin\`)` in a browser; if
        // node is installed, verify the generated module eval-round-trips.
        // Skipped silently in environments without node.
        let version = std::process::Command::new("node").arg("--version").output();
        match &version {
            Ok(out) if out.status.success() => {}
            _ => {
                eprintln!("node not available, skipping eval test");
                return;
            }
        }

        let state = test_state();
        let res = script(&state, json!({ "packageName": "dbgate-plugin-sqlite" })).unwrap();
        let js = res.as_str().expect("js string");
        // serde_json quoting is valid JS string escaping
        let harness = format!(
            r#"
const resp = {};
global.window = {{ DBGATE_PACKAGES: {{ 'dbgate-tools': {{ driverBase: {{ prefix: 1 }} }} }} }};
const module = eval(resp + '; plugin');
const content = module && module.__esModule ? module.default : module;
if (!content || !Array.isArray(content.drivers) || content.drivers.length !== 1) {{
  console.error('bad drivers', JSON.stringify(content));
  process.exit(1);
}}
const driver = content.drivers[0];
if (driver.engine !== 'sqlite@dbgate-plugin-sqlite') {{ console.error('engine'); process.exit(2); }}
if (typeof driver.showConnectionField !== 'function') {{ console.error('showConnectionField'); process.exit(3); }}
if (driver.showConnectionField('databaseFile', {{}}) !== true) {{ console.error('databaseFile'); process.exit(4); }}
if (driver.showConnectionField('server', {{}}) !== false) {{ console.error('server should be hidden'); process.exit(5); }}
if (typeof driver.getQuerySplitterOptions !== 'function') {{ console.error('splitter'); process.exit(6); }}
const opts = driver.getQuerySplitterOptions('editor');
if (!opts || opts.ignoreComments !== true || opts.preventSingleLineSplit !== true) {{ console.error('editor opts'); process.exit(7); }}
const saved = driver.beforeConnectionSave({{ databaseFile: '/tmp/x.db' }});
if (saved.singleDatabase !== true || saved.defaultDatabase !== 'x.db') {{ console.error('beforeSave', saved); process.exit(8); }}
console.log('OK');
"#,
            serde_json::to_string(js).unwrap()
        );
        let out = std::process::Command::new("node")
            .arg("-e")
            .arg(&harness)
            .output()
            .expect("run node harness");
        assert!(
            out.status.success(),
            "node eval harness failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
