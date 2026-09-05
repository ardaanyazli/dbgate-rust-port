//! Registry of registered engine drivers, addressable by engine id.
//!
//! Mirrors the plugin registry in the JS app: the app registers all 16
//! engines (SQLite, Postgres, MySQL, MongoDB, Redis, MSSQL, Oracle, ...) and
//! looks them up by dotted id string.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;

use crate::driver::EngineDriver;

/// Metadata describing a registered engine, as exposed to the Svelte UI via
/// the `plugins_installed` bridge route.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineMetadata {
    /// Plugin package name, e.g. `dbgate-plugin-mysql`.
    pub name: String,
    /// Human-readable engine title, e.g. "MySQL".
    pub display_name: String,
    /// Dotted engine id, e.g. `mysql@dbgate-plugin-mysql`.
    pub engine: String,
    /// Default TCP port for the engine, if any.
    pub default_port: Option<u32>,
    /// Engine family (the part before `@`), e.g. `mysql`.
    pub database_engine: String,
    /// Reserved for driver icons; always empty in this port.
    pub icons: serde_json::Value,
}

/// The plugin package name from an engine id (`mysql@dbgate-plugin-mysql` ->
/// `dbgate-plugin-mysql`).
fn plugin_name(engine: &str) -> String {
    engine.split('@').nth(1).unwrap_or(engine).to_string()
}

/// The engine family from an engine id (`mysql@dbgate-plugin-mysql` ->
/// `mysql`).
fn database_engine(engine: &str) -> String {
    engine.split('@').next().unwrap_or(engine).to_string()
}

/// Thread-safe registry of [`EngineDriver`]s.
#[derive(Default, Clone)]
pub struct DriverRegistry {
    by_engine: HashMap<String, Arc<dyn EngineDriver>>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, driver: Arc<dyn EngineDriver>) {
        self.by_engine.insert(driver.engine().to_string(), driver);
    }

    pub fn get(&self, engine: &str) -> Option<Arc<dyn EngineDriver>> {
        self.by_engine.get(engine).cloned()
    }

    pub fn engine_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.by_engine.keys().cloned().collect();
        ids.sort();
        ids
    }

    pub fn len(&self) -> usize {
        self.by_engine.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_engine.is_empty()
    }

    /// Metadata for every registered engine, sorted by engine id.
    pub fn list_metadata(&self) -> Vec<EngineMetadata> {
        let mut ids: Vec<&String> = self.by_engine.keys().collect();
        ids.sort();
        ids.into_iter()
            .map(|engine| {
                let driver = &self.by_engine[engine];
                EngineMetadata {
                    name: plugin_name(engine),
                    display_name: driver.title().to_string(),
                    engine: engine.clone(),
                    default_port: driver.capabilities().default_port,
                    database_engine: database_engine(engine),
                    icons: serde_json::json!({}),
                }
            })
            .collect()
    }
}
