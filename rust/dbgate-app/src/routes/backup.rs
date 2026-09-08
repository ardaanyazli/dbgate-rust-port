//! Native backup/restore bridge routes.
//!
//! Mirrors the Electron `databaseConnections` controller's native
//! backup/restore shell-outs (`mysqldump`/`mysql`, `pg_dump`/`psql`) via
//! `std::process::Command`. Only engines advertising `supports_native_backup`
//! / `supports_native_restore` are accepted; a missing client binary surfaces
//! a `DBGM-00000` "not found in PATH" error. Live shell-outs against real
//! databases are integration-only (`#[ignore]` tests) — the committed unit
//! tests stub the binary.

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use dbgate_core::connection::ConnectionDefinition;
use dbgate_core::drivers::mysql::{MARIADB_ENGINE, MYSQL_ENGINE};
use dbgate_core::drivers::postgres::POSTGRES_ENGINE;

use super::database_connections::load_definition;
use super::route_error;
use crate::DbgmState;

/// Client family for a native backup/restore tool.
#[derive(Clone, Copy, Debug)]
enum ToolKind {
    Mysql,
    Postgres,
}

/// Resolved native tool for an engine: the client binary plus its arg style.
#[derive(Debug)]
struct NativeTool {
    bin: &'static str,
    kind: ToolKind,
}

/// Fully-resolved command ready to spawn (program + args + env).
#[derive(Debug)]
struct NativeCommand {
    bin: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

fn native_tool(def: &ConnectionDefinition, restore: bool) -> Result<NativeTool, String> {
    let (bin, kind) = match def.engine.as_str() {
        MYSQL_ENGINE | MARIADB_ENGINE => (
            if restore { "mysql" } else { "mysqldump" },
            ToolKind::Mysql,
        ),
        POSTGRES_ENGINE => (
            if restore { "psql" } else { "pg_dump" },
            ToolKind::Postgres,
        ),
        _ => {
            return Err(route_error(format!(
                "native {} not supported for {}",
                if restore { "restore" } else { "backup" },
                def.engine
            )))
        }
    };
    Ok(NativeTool { bin, kind })
}

/// Resolve the driver capabilities gate and the concrete tool/port for an
/// engine, mirroring the controller's `supportsNativeBackup/Restore` guard.
fn engine_context(
    state: &DbgmState,
    def: &ConnectionDefinition,
    restore: bool,
) -> Result<(NativeTool, u32), String> {
    let caps = state
        .drivers
        .get(&def.engine)
        .ok_or_else(|| route_error(format!("No driver registered for engine '{}'", def.engine)))?
        .capabilities();
    let supported = if restore {
        caps.supports_native_restore
    } else {
        caps.supports_native_backup
    };
    if !supported {
        return Err(route_error(format!(
            "native {} not supported for {}",
            if restore { "restore" } else { "backup" },
            def.engine
        )));
    }
    let tool = native_tool(def, restore)?;
    let port = def
        .port
        .or(caps.default_port)
        .ok_or_else(|| route_error(format!("No port for engine '{}'", def.engine)))?;
    Ok((tool, port))
}

fn connection_args(def: &ConnectionDefinition, tool: &NativeTool, port: u32) -> Vec<String> {
    let server = def.server.clone().unwrap_or_default();
    let user = def.user.clone().unwrap_or_default();
    let password = def.password.clone().unwrap_or_default();
    let database = def.database.clone().unwrap_or_default();
    match tool.kind {
        ToolKind::Mysql => vec![
            format!("-h{server}"),
            format!("-P{port}"),
            format!("-u{user}"),
            format!("-p{password}"),
            database,
        ],
        ToolKind::Postgres => vec![
            "-h".into(),
            server,
            "-p".into(),
            port.to_string(),
            "-U".into(),
            user,
            database,
        ],
    }
}

fn child_env(def: &ConnectionDefinition, tool: &NativeTool) -> Vec<(String, String)> {
    match tool.kind {
        ToolKind::Postgres => def
            .password
            .clone()
            .map(|password| vec![("PGPASSWORD".to_string(), password)])
            .unwrap_or_default(),
        ToolKind::Mysql => Vec::new(),
    }
}

fn spawn_error(bin: &str, err: std::io::Error) -> String {
    if err.kind() == std::io::ErrorKind::NotFound {
        route_error(format!("{bin} not found in PATH"))
    } else {
        route_error(format!("Failed to spawn {bin}: {err}"))
    }
}

/// Spawn a native client, capturing stdout/stderr and optionally feeding
/// `stdin_text` to the child (restore). A missing binary is surfaced as a
/// `DBGM-00000` "not found in PATH" error.
fn run_tool(command: &NativeCommand, stdin_text: Option<&str>) -> Result<Output, String> {
    use std::io::Write;
    let mut cmd = Command::new(&command.bin);
    cmd.args(&command.args);
    for (key, value) in &command.env {
        cmd.env(key, value);
    }
    match stdin_text {
        Some(text) => {
            let mut child = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| spawn_error(&command.bin, e))?;
            let mut child_stdin = child
                .stdin
                .take()
                .ok_or_else(|| route_error(format!("Cannot open stdin for {}", command.bin)))?;
            child_stdin
                .write_all(text.as_bytes())
                .map_err(|e| route_error(format!("Failed to write to {}: {e}", command.bin)))?;
            drop(child_stdin);
            child
                .wait_with_output()
                .map_err(|e| route_error(format!("Failed to run {}: {e}", command.bin)))
        }
        None => cmd.output().map_err(|e| spawn_error(&command.bin, e)),
    }
}

fn default_dump_path(state: &DbgmState, conid: &str) -> Result<PathBuf, String> {
    let dir = state.data_dir.join("dumps");
    std::fs::create_dir_all(&dir)
        .map_err(|e| route_error(format!("Cannot create dumps directory: {e}")))?;
    let safe = conid
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    Ok(dir.join(format!("{safe}-{millis}.sql")))
}

/// `database_connections_backup_native` — shell out to the engine's native
/// dump client, writing stdout to `path` (or `data_dir/dumps/` when absent).
pub fn backup_native(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("backup_native missing conid"))?;
    let path_arg = args.get("path").and_then(Value::as_str);
    let def = load_definition(state, conid)?;
    let (tool, port) = engine_context(state, &def, false)?;
    let command = NativeCommand {
        bin: tool.bin.to_string(),
        args: connection_args(&def, &tool, port),
        env: child_env(&def, &tool),
    };
    let output = run_tool(&command, None)?;
    let path = match path_arg {
        Some(path) => path.to_string(),
        None => default_dump_path(state, conid)?.to_string_lossy().into_owned(),
    };
    if output.status.success() {
        std::fs::write(&path, &output.stdout)
            .map_err(|e| route_error(format!("Cannot write dump to {path}: {e}")))?;
    }
    Ok(json!({
        "success": output.status.success(),
        "stdout": String::from_utf8_lossy(&output.stdout).into_owned(),
        "stderr": String::from_utf8_lossy(&output.stderr).into_owned(),
        "path": path,
    }))
}

/// `database_connections_restore_native` — feed a dump file into the engine's
/// native restore client via stdin.
pub fn restore_native(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("restore_native missing conid"))?;
    let path = args
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| route_error("restore_native missing path"))?;
    let def = load_definition(state, conid)?;
    let (tool, port) = engine_context(state, &def, true)?;
    let command = NativeCommand {
        bin: tool.bin.to_string(),
        args: connection_args(&def, &tool, port),
        env: child_env(&def, &tool),
    };
    let dump = std::fs::read_to_string(path)
        .map_err(|e| route_error(format!("Cannot read dump file {path}: {e}")))?;
    let output = run_tool(&command, Some(&dump))?;
    Ok(json!({
        "success": output.status.success(),
        "stdout": String::from_utf8_lossy(&output.stdout).into_owned(),
        "stderr": String::from_utf8_lossy(&output.stderr).into_owned(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::connections::ConnectionsStore;
    use dbgate_core::drivers::postgres::POSTGRES_ENGINE;
    use dbgate_core::drivers::sqlite::SQLITE_ENGINE;

    fn test_state() -> DbgmState {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-backup-{nanos}"));
        DbgmState::with_data_dir(dir)
    }

    fn mysql_def() -> ConnectionDefinition {
        ConnectionDefinition {
            engine: MYSQL_ENGINE.to_string(),
            name: "test".to_string(),
            server: Some("localhost".to_string()),
            port: Some(3306),
            user: Some("root".to_string()),
            password: Some("secret".to_string()),
            database: Some("mydb".to_string()),
            ..Default::default()
        }
    }

    fn postgres_def() -> ConnectionDefinition {
        ConnectionDefinition {
            engine: POSTGRES_ENGINE.to_string(),
            name: "test".to_string(),
            server: Some("localhost".to_string()),
            port: Some(5432),
            user: Some("root".to_string()),
            password: Some("secret".to_string()),
            database: Some("mydb".to_string()),
            ..Default::default()
        }
    }

    fn save_connection(state: &DbgmState, conid: &str, engine: &str) {
        let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
        store
            .insert(json!({
                "_id": conid,
                "engine": engine,
                "name": "saved",
                "server": "localhost",
                "port": 3306,
                "user": "root",
                "password": "secret",
                "database": "mydb"
            }))
            .unwrap();
    }

    fn fixture_bin() -> String {
        concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/fixtures/echo-args.sh").to_string()
    }

    #[test]
    fn mysql_backup_args_use_concatenated_flag_style() {
        let tool = native_tool(&mysql_def(), false).unwrap();
        assert_eq!(
            connection_args(&mysql_def(), &tool, 3306),
            vec!["-hlocalhost", "-P3306", "-uroot", "-psecret", "mydb"]
        );
    }

    #[test]
    fn postgres_backup_args_use_separated_flag_style() {
        let tool = native_tool(&postgres_def(), false).unwrap();
        assert_eq!(
            connection_args(&postgres_def(), &tool, 5432),
            vec!["-h", "localhost", "-p", "5432", "-U", "root", "mydb"]
        );
    }

    #[test]
    fn restore_binaries_are_mysql_and_psql() {
        assert_eq!(native_tool(&mysql_def(), true).unwrap().bin, "mysql");
        assert_eq!(native_tool(&postgres_def(), true).unwrap().bin, "psql");
    }

    #[test]
    fn postgres_env_carries_password_but_not_args() {
        let tool = native_tool(&postgres_def(), false).unwrap();
        assert_eq!(
            child_env(&postgres_def(), &tool),
            vec![("PGPASSWORD".to_string(), "secret".to_string())]
        );
        let command = NativeCommand {
            bin: tool.bin.to_string(),
            args: connection_args(&postgres_def(), &tool, 5432),
            env: child_env(&postgres_def(), &tool),
        };
        assert!(!command.args.iter().any(|a| a.contains("secret")));
    }

    #[test]
    fn missing_binary_reports_db_error() {
        let command = NativeCommand {
            bin: "definitely-not-a-real-dbgate-tool".to_string(),
            args: Vec::new(),
            env: Vec::new(),
        };
        let err = run_tool(&command, None).unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("definitely-not-a-real-dbgate-tool not found in PATH"));
    }

    #[test]
    fn fake_binary_backup_echoes_args_to_stdout() {
        let command = NativeCommand {
            bin: fixture_bin(),
            args: vec![
                "-hlocalhost".to_string(),
                "-P3306".to_string(),
                "-uroot".to_string(),
                "-psecret".to_string(),
                "mydb".to_string(),
            ],
            env: Vec::new(),
        };
        let output = run_tool(&command, None).unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("-hlocalhost"));
        assert!(stdout.contains("-P3306"));
        assert!(stdout.contains("-uroot"));
        assert!(stdout.contains("mydb"));
    }

    #[test]
    fn backup_native_rejects_non_capable_engine() {
        let state = test_state();
        save_connection(&state, "sqlite-backup", SQLITE_ENGINE);
        let err = crate::routes::dispatch(
            &state,
            "database_connections_backup_native",
            json!({"conid": "sqlite-backup"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains(&format!("native backup not supported for {SQLITE_ENGINE}")));
    }

    #[test]
    fn restore_native_rejects_non_capable_engine() {
        let state = test_state();
        save_connection(&state, "sqlite-restore", SQLITE_ENGINE);
        let err = crate::routes::dispatch(
            &state,
            "database_connections_restore_native",
            json!({"conid": "sqlite-restore", "path": "/tmp/dump.sql"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains(&format!("native restore not supported for {SQLITE_ENGINE}")));
    }

    #[test]
    fn backup_native_unknown_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_backup_native",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("Unknown connection nope"));
    }

    #[test]
    fn backup_native_missing_conid_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_backup_native",
            json!({}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("backup_native missing conid"));
    }

    #[test]
    fn restore_native_missing_path_errors() {
        let state = test_state();
        let err = crate::routes::dispatch(
            &state,
            "database_connections_restore_native",
            json!({"conid": "nope"}),
        )
        .unwrap_err();
        assert!(err.starts_with("DBGM-00000"));
        assert!(err.contains("restore_native missing path"));
    }

    #[test]
    fn default_dump_path_creates_dumps_directory() {
        let state = test_state();
        let path = default_dump_path(&state, "mysql/test").unwrap();
        assert!(path.starts_with(state.data_dir.join("dumps")));
        assert!(path.to_string_lossy().ends_with(".sql"));
        assert!(path.file_name().unwrap().to_string_lossy().starts_with("mysql_test"));
        assert!(path.parent().unwrap().is_dir());
    }

    #[test]
    fn postgres_driver_advertises_native_backup_and_restore() {
        let state = test_state();
        let caps = state.drivers.get(POSTGRES_ENGINE).unwrap().capabilities();
        assert!(caps.supports_native_backup);
        assert!(caps.supports_native_restore);
    }

    /// Integration-only: requires `mysqldump`/`pg_dump` on PATH and a live
    /// database. Run manually with `cargo test -- --ignored`.
    #[ignore]
    #[test]
    fn real_backup_shell_out_requires_live_database() {
        let state = test_state();
        save_connection(&state, "mysql-live", MYSQL_ENGINE);
        backup_native(&state, json!({"conid": "mysql-live", "path": "/tmp/dbgate-dump.sql"}))
            .unwrap();
    }
}