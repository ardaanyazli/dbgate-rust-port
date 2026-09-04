//! Registry of registered engine drivers, addressable by engine id.
//!
//! Mirrors the plugin registry in the JS app: the app registers all 16
//! engines (SQLite, Postgres, MySQL, MongoDB, Redis, MSSQL, Oracle, ...) and
//! looks them up by dotted id string.

use std::collections::HashMap;
use std::sync::Arc;

use crate::driver::EngineDriver;

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
}
