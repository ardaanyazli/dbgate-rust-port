//! The `EngineDriver` trait: the heart of the DbGate Rust backend.
//!
//! This is the Rust port of `packages/types/engines.d.ts`. Every database
//! engine (MySQL, PostgreSQL, MongoDB, Redis, SQLite, ...) is implemented as
//! a driver that:
//!
//! 1. [`EngineDriver::connect`] — turns a [`ConnectionDefinition`] into a
//!    type-erased connection handle.
//! 2. [`EngineDriver::query`] / [`EngineDriver::stream`] /
//!    [`EngineDriver::read_query`] — run SQL and stream rows.
//! 3. Metadata/DDL methods — list databases, analyse structure, etc.
//!
//! Drivers are stored behind `Arc<dyn EngineDriver>` so the app holds a
//! registry of the 16 supported engines. The handle is type-erased
//! (`Box<dyn Any + Send + Sync>`) — each driver stores whatever client
//! object its underlying crate provides.

use std::any::Any;
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{DatabaseInfo, TableInfo};
use crate::error::DbgmResult;
use crate::query::{QueryResult, QueryResultColumn};

/// An open connection to a database, type-erased.
///
/// Drivers store their native client (e.g. `rusqlite::Connection`) inside.
pub type DbHandle = Box<dyn Any + Send + Sync>;

/// Callback sink used by [`EngineDriver::stream`] for row/recordset/info
/// push events.
pub struct StreamSink<'a> {
    pub on_recordset: &'a dyn Fn(&[QueryResultColumn]),
    pub on_row: &'a dyn Fn(&Value),
    pub on_info: &'a dyn Fn(&StreamInfo),
    pub on_done: &'a dyn Fn(),
}

/// A progress / info message emitted during stream execution.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub message: String,
    pub severity: StreamSeverity,
    pub rows_affected: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSeverity {
    Info,
    Warning,
    Error,
    Debug,
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub discard_result: bool,
    pub import_sql_dump: bool,
    pub range: Option<(u64, u64)>, // (offset, limit)
    pub readonly: bool,
    pub command_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct WriteTableOptions {
    pub drop_if_exists: bool,
    pub truncate: bool,
    pub create_if_not_exists: bool,
    pub commit_after_insert: bool,
    pub target_table_structure: Option<TableInfo>,
}

/// A version reported by a database server.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerVersion {
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_text: Option<String>,
}

/// A database listed on a server.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_on_disk: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
}

/// Driver capability flags mirroring the boolean capability fields on the
/// JS `EngineDriver`.
#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    pub read_only_sessions: bool,
    pub supports_transactions: bool,
    pub supports_native_backup: bool,
    pub supports_native_restore: bool,
    pub supports_server_summary: bool,
    pub default_port: Option<u32>,
}

/// The engine driver contract. See module docs.
///
/// # Lifetime / threading note
///
/// Methods take `&self` and the handle is `Send + Sync`, matching the
/// concurrent, shared-connection usage patterns of the original app.
pub trait EngineDriver: Send + Sync {
    /// Stable dotted engine id, e.g. `mysql@dbgate-plugin-mysql`.
    fn engine(&self) -> &str;

    /// Human-readable title, e.g. "MySQL".
    fn title(&self) -> &str;

    /// Capability flags.
    fn capabilities(&self) -> Capabilities;

    /// Open a connection from a definition, returning an opaque handle.
    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle>;

    /// Close a connection handle.
    fn close(&self, handle: DbHandle) -> DbgmResult<()>;

    /// Run a query and collect the full result set in memory.
    fn query(&self, handle: &DbHandle, sql: &str, options: &QueryOptions) -> DbgmResult<QueryResult>;

    /// Stream a script/query result through a callback sink.
    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()>;

    /// Metadata: server version.
    fn get_version(&self, handle: &DbHandle) -> DbgmResult<ServerVersion>;

    /// Metadata: list databases on the server.
    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>>;

    /// Metadata: full structural analysis of the connected database.
    fn analyse_full(&self, handle: &DbHandle, server_version: &str) -> DbgmResult<DatabaseInfo>;

    /// Read a table's full metadata.
    fn analyse_single_table(&self, handle: &DbHandle, name: &crate::dbinfo::NamedObjectInfo)
        -> DbgmResult<TableInfo>;

    /// Perform a bulk write to a table from a stream of rows.
    fn write_table(
        &self,
        handle: &DbHandle,
        name: &crate::dbinfo::NamedObjectInfo,
        options: &WriteTableOptions,
    ) -> DbgmResult<()>;

    /// Downcast hook so the app can reach driver-specific state if needed.
    fn as_any(&self) -> &dyn Any;
}

/// Helper to get an `Arc<dyn EngineDriver>` from a concrete driver.
pub fn driver_ref<D: EngineDriver + 'static>(driver: D) -> Arc<dyn EngineDriver> {
    Arc::new(driver)
}
