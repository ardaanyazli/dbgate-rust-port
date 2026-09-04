//! DbGate Tauri v2 application shell.
//!
//! This replaces the Electron main process (`app/src/electron.js`) with a
//! Tauri v2 backend. The Svelte 4 frontend (`packages/web`) is loaded
//! unchanged inside the Tauri webview.
//!
//! The app owns a [`DbgmState`] holding the driver registry and open
//! connections, and exposes Tauri commands that mirror both the Electron
//! IPC surface (window ops, menus, dialogs) and the database API.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dbgate_core::connection::ConnectionDefinition;
use dbgate_core::driver::{DbHandle, EngineDriver, QueryOptions};
use dbgate_core::query::QueryResult;
use dbgate_core::registry::DriverRegistry;

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// An open connection: its driver plus its type-erased handle.
struct OpenConnection {
    driver: Arc<dyn EngineDriver>,
    handle: DbHandle,
}

/// Application-wide state managed by Tauri.
pub struct DbgmState {
    /// All registered engine drivers.
    drivers: DriverRegistry,
    /// Open connections keyed by their connection id.
    connections: Mutex<HashMap<String, OpenConnection>>,
}

impl DbgmState {
    pub fn new() -> Self {
        let mut drivers = DriverRegistry::new();
        // Register built-in engines. Add more as drivers are ported.
        drivers.register(dbgate_core::drivers::sqlite::driver_ref());
        drivers.register(dbgate_core::drivers::mssql::driver_ref());
        drivers.register(dbgate_core::drivers::postgres::driver_ref());
        drivers.register(dbgate_core::drivers::mysql::driver_ref());
        drivers.register(dbgate_core::drivers::mysql::mariadb_driver_ref());
        drivers.register(dbgate_core::drivers::clickhouse::driver_ref());
        Self {
            drivers,
            connections: Mutex::new(HashMap::new()),
        }
    }

    fn next_conn_id(engine: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}-{}", engine.replace('@', "_"), nanos)
    }
}

impl Default for DbgmState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tauri commands — database operations
// ---------------------------------------------------------------------------

/// Open a new connection from a [`ConnectionDefinition`], returning a
/// connection id the frontend uses for later calls.
#[tauri::command]
fn open_connection(
    state: tauri::State<'_, Arc<DbgmState>>,
    definition: ConnectionDefinition,
) -> Result<String, String> {
    let driver = state
        .drivers
        .get(&definition.engine)
        .ok_or_else(|| format!("No driver registered for engine '{}'", definition.engine))?;

    let handle = driver.connect(&definition).map_err(|e| e.to_string())?;
    let conn_id = DbgmState::next_conn_id(&definition.engine);

    state.connections.lock().unwrap().insert(
        conn_id.clone(),
        OpenConnection { driver, handle },
    );
    Ok(conn_id)
}

/// Run a query on an open connection and return the full result set.
#[tauri::command]
fn run_query(
    state: tauri::State<'_, Arc<DbgmState>>,
    conn_id: String,
    sql: String,
) -> Result<QueryResult, String> {
    let guard = state.connections.lock().unwrap();
    let conn = guard
        .get(&conn_id)
        .ok_or_else(|| format!("Unknown connection {conn_id}"))?;
    let options = QueryOptions::default();
    conn.driver.query(&conn.handle, &sql, &options).map_err(|e| e.to_string())
}

/// Close an open connection.
#[tauri::command]
fn close_connection(state: tauri::State<'_, Arc<DbgmState>>, conn_id: String) -> Result<(), String> {
    let mut guard = state.connections.lock().unwrap();
    let conn = guard
        .remove(&conn_id)
        .ok_or_else(|| format!("Unknown connection {conn_id}"))?;
    conn.driver.close(conn.handle).map_err(|e| e.to_string())
}

/// List the ids of all open connections (for tests / diagnostics).
#[tauri::command]
fn list_connections(state: tauri::State<'_, Arc<DbgmState>>) -> Vec<String> {
    state.connections.lock().unwrap().keys().cloned().collect()
}

/// Report the server version for an open connection.
#[tauri::command]
fn get_version(
    state: tauri::State<'_, Arc<DbgmState>>,
    conn_id: String,
) -> Result<dbgate_core::driver::ServerVersion, String> {
    let guard = state.connections.lock().unwrap();
    let conn = guard
        .get(&conn_id)
        .ok_or_else(|| format!("Unknown connection {conn_id}"))?;
    conn.driver.get_version(&conn.handle).map_err(|e| e.to_string())
}

/// Analyse the full structure of the connected database.
#[tauri::command]
fn analyse_full(
    state: tauri::State<'_, Arc<DbgmState>>,
    conn_id: String,
) -> Result<dbgate_core::dbinfo::DatabaseInfo, String> {
    let guard = state.connections.lock().unwrap();
    let conn = guard
        .get(&conn_id)
        .ok_or_else(|| format!("Unknown connection {conn_id}"))?;
    let version = conn.driver.get_version(&conn.handle).map_err(|e| e.to_string())?;
    conn.driver
        .analyse_full(&conn.handle, &version.version)
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Tauri application entry
// ---------------------------------------------------------------------------

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "debug".into()),
            )
            .init();
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .manage(Arc::new(DbgmState::new()))
        .invoke_handler(tauri::generate_handler![
            open_connection,
            run_query,
            close_connection,
            list_connections,
            get_version,
            analyse_full,
        ])
        .setup(|_app| {
            tracing::info!("DbGate (Rust/Tauri v2 port) starting");
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
