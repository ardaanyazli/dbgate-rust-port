//! Bridge route backing `apps/get-all-apps`.
//!
//! The Svelte frontend awaits this route during startup (`App.svelte`
//! `loadApi`), so it must always resolve successfully. The Node controller
//! (`packages/api/src/controllers/apps.js`) returns an empty array when no
//! applications directory exists; this port mirrors that default.

use serde_json::{json, Value};

use crate::DbgmState;

/// `apps_get_all_apps` — list installed applications.
///
/// Always returns an empty array: the Rust port ships no applications and
/// this mirrors the Node controller's behaviour when the applications
/// directory is absent.
pub fn get_all_apps(_state: &DbgmState, _args: Value) -> Result<Value, String> {
    Ok(json!([]))
}