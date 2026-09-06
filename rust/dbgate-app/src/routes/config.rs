use std::fs;

use serde_json::{json, Value};

use crate::DbgmState;

/// Path of the settings file inside the app data dir.
fn settings_path(state: &DbgmState) -> std::path::PathBuf {
    state.data_dir.join("settings.json")
}

/// Read the settings file, returning `{}` when missing or unparseable.
fn read_settings(state: &DbgmState) -> Value {
    fs::read_to_string(settings_path(state))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_else(|| json!({}))
}

/// `config_get` — startup config for the Svelte UI.
///
/// No auth, license, cloud or storage fields: the electron/Tauri shell is
/// always considered available and logged-in.
pub fn get(_state: &DbgmState, _args: Value) -> Result<Value, String> {
    Ok(json!({
        "isElectron": true,
        "isTauri": true,
        "singleConnection": null,
        "settingsValue": {},
        "skipAllAuth": true,
    }))
}

/// `config_get_settings` — the raw settings object.
pub fn get_settings(state: &DbgmState, _args: Value) -> Result<Value, String> {
    Ok(read_settings(state))
}

/// `config_update_settings` — shallow-merge new keys into `settings.json`.
///
/// The frontend sends a flat map of key/value pairs (e.g.
/// `{ "localization.language": "cs" }`), matching the Node controller.
/// A `{ "settings": {...} }` wrapper is also accepted.
pub fn update_settings(state: &DbgmState, args: Value) -> Result<Value, String> {
    let values = match args.get("settings") {
        Some(Value::Object(_)) => args["settings"].clone(),
        _ => args,
    };

    let mut current = read_settings(state);
    if let (Value::Object(cur), Value::Object(src)) = (&mut current, &values) {
        for (k, v) in src {
            cur.insert(k.clone(), v.clone());
        }
    }

    fs::create_dir_all(&state.data_dir)
        .map_err(|e| format!("DBGM-00000: cannot create data dir: {e}"))?;
    let text = serde_json::to_string_pretty(&current)
        .map_err(|e| format!("DBGM-00000: cannot serialize settings: {e}"))?;
    fs::write(settings_path(state), text)
        .map_err(|e| format!("DBGM-00000: cannot write settings: {e}"))?;

    Ok(json!({ "status": "ok" }))
}

/// `config_platform_info` — platform flags consumed by the connection form.
pub fn platform_info(_state: &DbgmState, _args: Value) -> Result<Value, String> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_default();
    Ok(json!({
        "isWindows": cfg!(target_os = "windows"),
        "isMac": cfg!(target_os = "macos"),
        "isLinux": cfg!(target_os = "linux"),
        "isElectron": true,
        "isTauri": true,
        "isDocker": false,
        "allowShellConnection": true,
        "allowShellScripting": true,
        "sshAuthSock": std::env::var("SSH_AUTH_SOCK").ok(),
        "defaultKeyfile": format!("{home}/.ssh/id_rsa"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_data_dir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("dbgate-config-test-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn get_returns_expected_structure() {
        let state = DbgmState::with_data_dir(temp_data_dir("get"));
        let res = get(&state, json!({})).unwrap();
        assert_eq!(res["isElectron"], true);
        assert_eq!(res["isTauri"], true);
        assert_eq!(res["singleConnection"], Value::Null);
        assert!(res["settingsValue"].is_object());
        assert_eq!(res["skipAllAuth"], true);
    }

    #[test]
    fn get_settings_returns_empty_when_missing() {
        let state = DbgmState::with_data_dir(temp_data_dir("empty"));
        let res = get_settings(&state, json!({})).unwrap();
        assert_eq!(res, json!({}));
    }

    #[test]
    fn update_settings_persists_flat_map() {
        let state = DbgmState::with_data_dir(temp_data_dir("persist"));
        let res = update_settings(&state, json!({ "localization.language": "cs" })).unwrap();
        assert_eq!(res["status"], "ok");
        let settings = get_settings(&state, json!({})).unwrap();
        assert_eq!(settings["localization.language"], "cs");
    }

    #[test]
    fn update_settings_merges_with_existing() {
        let state = DbgmState::with_data_dir(temp_data_dir("merge"));
        update_settings(&state, json!({ "a": 1 })).unwrap();
        update_settings(&state, json!({ "b": 2 })).unwrap();
        let settings = get_settings(&state, json!({})).unwrap();
        assert_eq!(settings["a"], 1);
        assert_eq!(settings["b"], 2);
    }

    #[test]
    fn update_settings_accepts_wrapper() {
        let state = DbgmState::with_data_dir(temp_data_dir("wrapper"));
        update_settings(&state, json!({ "settings": { "theme": "dark" } })).unwrap();
        let settings = get_settings(&state, json!({})).unwrap();
        assert_eq!(settings["theme"], "dark");
    }

    #[test]
    fn platform_info_reports_tauri() {
        let state = DbgmState::with_data_dir(temp_data_dir("plat"));
        let res = platform_info(&state, json!({})).unwrap();
        assert_eq!(res["isElectron"], true);
        assert_eq!(res["isTauri"], true);
        assert_eq!(res["isDocker"], false);
    }
}
