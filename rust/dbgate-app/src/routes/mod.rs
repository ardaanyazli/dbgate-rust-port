//! Tauri bridge route dispatcher.
//!
//! The Svelte frontend calls a single generic Tauri command
//! (`api_call`) with a route name and JSON args; this module maps the
//! route name to a controller handler, mirroring the Node API surface
//! (`packages/api/src/controllers/*`).
//!
//! Wire format: the frontend `TauriApi.invoke` sends the route with
//! dashes replaced by underscores (e.g. `connections-list` ->
//! `connections_list`), so the arms below always match underscore names.

use serde_json::Value;

use crate::DbgmState;

/// Route error returned to the frontend, always carrying the `DBGM-00000`
/// sentinel code required for all newly written code.
pub fn route_error(message: impl Into<String>) -> String {
    format!("DBGM-00000: {}", message.into())
}

/// Dispatch a bridge route to its controller handler.
pub fn dispatch(
    state: &DbgmState,
    route: &str,
    args: Value,
) -> Result<Value, String> {
    match route {
        "connections_list" => connections::list(state, args),
        "connections_get" => connections::get(state, args),
        "connections_save" => connections::save(state, args),
        "connections_update" => connections::update(state, args),
        "connections_update_database" => connections::update_database(state, args),
        "connections_delete" => connections::delete(state, args),
        "config_get" => config::get(state, args),
        "config_get_settings" => config::get_settings(state, args),
        "config_update_settings" => config::update_settings(state, args),
        "config_platform_info" => config::platform_info(state, args),
        "plugins_installed" => plugins::installed(state, args),
        "database_connections_sql_select" => database_connections::sql_select(state, args),
        "database_connections_run_script" => database_connections::run_script(state, args),
        "database_connections_sync_model" => database_connections::sync_model(state, args),
        "database_connections_refresh" => database_connections::refresh(state, args),
        "database_connections_call_method" => database_connections::call_method(state, args),
        "database_connections_ping" => database_connections::ping(state, args),
        "database_connections_disconnect" => database_connections::disconnect(state, args),
        _ => Err(route_error(format!("Route not implemented: {route}"))),
    }
}

mod config;
mod connections;
mod database_connections;
mod plugins;