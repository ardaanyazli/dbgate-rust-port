//! Bridge route backing `files/favorites`.
//!
//! The Svelte frontend reads favorites during startup
//! (`OpenTabsOnStartup.svelte` via `useFavorites`, which maps to the
//! `files/favorites` API route) and calls `list.filter(...)` on the result,
//! so the route must always resolve to an array, never an error envelope.
//! The Node controller (`packages/api/src/controllers/files.js`) returns an
//! empty array when no favorites directory exists or the read permission is
//! missing; this port mirrors that default.

use serde_json::{json, Value};

use crate::DbgmState;

/// `files_favorites` — list saved favorites.
///
/// Always returns an empty array: the Rust port ships no favorites storage
/// yet, mirroring the Node controller's behaviour when the favorites
/// directory is absent.
pub fn favorites(_state: &DbgmState, _args: Value) -> Result<Value, String> {
    Ok(json!([]))
}