//! SQLite engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-sqlite/src/backend/driver.sqlite.js`
//! on top of [`rusqlite`]. This is the reference implementation used as the
//! template for the other 15 engine drivers.

use std::any::Any;
use std::sync::Mutex;

use rusqlite::{Connection, OpenFlags, Row};
use serde_json::{Map, Value};

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{ColumnInfo, DatabaseInfo, NamedObjectInfo, TableInfo};
use crate::driver::{
    Capabilities, DatabaseEntry, DbHandle, EngineDriver, QueryOptions, ServerVersion, StreamSink,
    WriteTableOptions,
};
use crate::error::{DbgmError, DbgmResult};
use crate::query::{QueryResult, QueryResultColumn};

/// Dotted engine id for SQLite.
pub const SQLITE_ENGINE: &str = "sqlite@dbgate-plugin-sqlite";

/// The SQLite driver.
pub struct SqliteDriver;

impl SqliteDriver {
    pub fn new() -> Self {
        Self
    }

    fn downcast(handle: &DbHandle) -> DbgmResult<&Mutex<Connection>> {
        handle
            .downcast_ref::<Mutex<Connection>>()
            .ok_or_else(|| DbgmError::new("handle is not a SQLite connection"))
    }

    fn open_connection(def: &ConnectionDefinition) -> DbgmResult<Connection> {
        let db_file = def
            .database_file
            .as_deref()
            .or_else(|| {
                def.extra
                    .as_ref()
                    .and_then(|m| m.get("databaseFile").and_then(|v| v.as_str()))
            })
            .ok_or_else(|| DbgmError::new("SQLite connection requires a database_file"))?;

        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        if def.is_read_only.unwrap_or(false) {
            flags = OpenFlags::SQLITE_OPEN_READ_ONLY;
        }

        let conn = Connection::open_with_flags(db_file, flags)
            .map_err(|e| DbgmError::with_source(format!("Cannot open SQLite database {db_file}"), e))?;
        Ok(conn)
    }

    fn prepare_handles(conn: &Connection, sql: &str) -> DbgmResult<QueryResult> {
        let mut stmt = conn.prepare_cached(sql)?;
        let column_count = stmt.column_count();
        if column_count > 0 {
            let columns: Vec<QueryResultColumn> = (0..column_count)
                .map(|i| QueryResultColumn {
                    column_name: stmt
                        .column_name(i)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|_| format!("col{i}")),
                    table_name: None,
                    table_schema: None,
                    source_column_name: None,
                    data_type: None,
                    display: None,
                    is_primary_key: None,
                })
                .collect();

            let mut rows = Vec::new();
            {
                let mut qrows = stmt.query([])?;
                while let Some(row) = qrows.next()? {
                    rows.push(row_to_value(row)?);
                }
            }

            Ok(QueryResult { rows, columns })
        } else {
            stmt.execute([])?;
            Ok(QueryResult::empty())
        }
    }
}

impl Default for SqliteDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(SqliteDriver::new())
}

fn row_to_value(row: &Row) -> DbgmResult<Value> {
    let mut map = Map::new();
    let column_count = row.as_ref().column_count();
    for idx in 0..column_count {
        let value = row
            .get_ref(idx)
            .map(value_ref_to_json)
            .map_err(|e| DbgmError::with_source("Error reading SQLite cell", e))?;
        let name = row
            .as_ref()
            .column_name(idx)
            .map_err(|e| DbgmError::with_source("Error reading SQLite column name", e))?
            .to_string();
        map.insert(name, value);
    }
    Ok(Value::Object(map))
}

fn value_ref_to_json(v: rusqlite::types::ValueRef) -> Value {
    match v {
        rusqlite::types::ValueRef::Null => Value::Null,
        rusqlite::types::ValueRef::Integer(i) => Value::Number(i.into()),
        rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        rusqlite::types::ValueRef::Text(s) => {
            Value::String(String::from_utf8_lossy(s).into_owned())
        }
        rusqlite::types::ValueRef::Blob(b) => {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(b);
            let mut obj = Map::new();
            obj.insert("$binary".into(), Value::Object(Map::from_iter([(
                "base64".into(),
                Value::String(b64),
            )])));
            Value::Object(obj)
        }
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// One grouped SQLite foreign key: referenced table, its (from, to) column
/// pairs, and the update/delete actions.
struct FkGroup {
    ref_table: String,
    columns: Vec<(String, String)>,
    on_update: String,
    on_delete: String,
}

impl EngineDriver for SqliteDriver {
    fn engine(&self) -> &str {
        SQLITE_ENGINE
    }

    fn title(&self) -> &str {
        "SQLite"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: false,
            supports_transactions: true,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: false,
            default_port: None,
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let conn = Self::open_connection(def)?;
        conn.pragma_update(None, "foreign_keys", true)?;
        Ok(Box::new(Mutex::new(conn)))
    }

    fn close(&self, handle: DbHandle) -> DbgmResult<()> {
        drop(handle);
        Ok(())
    }

    fn query(&self, handle: &DbHandle, sql: &str, _options: &QueryOptions) -> DbgmResult<QueryResult> {
        let conn = Self::downcast(handle)?;
        let guard = conn.lock().map_err(|_| DbgmError::new("SQLite connection lock poisoned"))?;
        Self::prepare_handles(&guard, sql)
    }

    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
        let conn = Self::downcast(handle)?;
        let guard = conn.lock().map_err(|_| DbgmError::new("SQLite connection lock poisoned"))?;

        let statements = split_sql(sql);
        let mut rows_affected: u64 = 0;

        guard.execute_batch("BEGIN")?;
        for stmt_sql in &statements {
            if stmt_sql.trim().is_empty() {
                continue;
            }
            let result = Self::prepare_handles(&guard, stmt_sql)?;
            if result.columns.is_empty() {
                rows_affected += guard.changes();
            } else {
                (sink.on_recordset)(&result.columns);
                for row in &result.rows {
                    (sink.on_row)(row);
                }
            }
        }
        guard.execute_batch("COMMIT")?;

        if rows_affected > 0 {
            (sink.on_info)(&crate::driver::StreamInfo {
                message: format!("{rows_affected} rows affected"),
                severity: crate::driver::StreamSeverity::Info,
                rows_affected: Some(rows_affected),
            });
        }

        (sink.on_done)();
        Ok(())
    }

    fn get_version(&self, handle: &DbHandle) -> DbgmResult<ServerVersion> {
        let conn = Self::downcast(handle)?;
        let guard = conn.lock().map_err(|_| DbgmError::new("SQLite connection lock poisoned"))?;
        let version: String = guard.query_row("select sqlite_version()", [], |r| r.get(0))?;
        Ok(ServerVersion {
            version: version.clone(),
            version_text: Some(format!("SQLite {version}")),
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let _ = handle;
        Ok(vec![DatabaseEntry {
            name: "main".to_string(),
            size_on_disk: None,
            empty: None,
        }])
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_sqlite_full(handle)
    }

    fn analyse_single_table(&self, handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
        analyse_sqlite_table(handle, &name.pure_name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new("SQLite write_table streaming is not yet ported"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn split_sql(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();

        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
            current.push(c);
            i += 1;
            continue;
        }
        if in_block_comment {
            if c == '*' && next == Some('/') {
                in_block_comment = false;
                current.push(c);
                current.push('/');
                i += 2;
                continue;
            }
            current.push(c);
            i += 1;
            continue;
        }
        if let Some(q) = in_quote {
            current.push(c);
            if c == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match c {
            '\'' | '"' | '`' => {
                in_quote = Some(c);
                current.push(c);
                i += 1;
            }
            '-' if next == Some('-') => {
                in_line_comment = true;
                current.push(c);
                current.push('-');
                i += 2;
            }
            '/' if next == Some('*') => {
                in_block_comment = true;
                current.push(c);
                current.push('*');
                i += 2;
            }
            ';' => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    out.push(trimmed);
                }
                current.clear();
                i += 1;
            }
            _ => {
                current.push(c);
                i += 1;
            }
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        out.push(trimmed);
    }

    out
}

fn analyse_sqlite_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let conn = SqliteDriver::downcast(handle)?;
    let guard = conn.lock().map_err(|_| DbgmError::new("SQLite connection lock poisoned"))?;

    let mut stmt = guard.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let table_names: Vec<(String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))?
        .collect::<Result<_, _>>()?;

    let mut tables = Vec::new();
    for (name, create_sql) in &table_names {
        tables.push(analyse_sqlite_table_by_conn(&guard, name, create_sql.as_deref())?);
    }

    Ok(DatabaseInfo {
        tables,
        ..Default::default()
    })
}

fn analyse_sqlite_table(handle: &DbHandle, pure_name: &str) -> DbgmResult<TableInfo> {
    let conn = SqliteDriver::downcast(handle)?;
    let guard = conn.lock().map_err(|_| DbgmError::new("SQLite connection lock poisoned"))?;
    analyse_sqlite_table_by_conn(&guard, pure_name, None)
}

fn analyse_sqlite_table_by_conn(
    conn: &Connection,
    pure_name: &str,
    create_sql: Option<&str>,
) -> DbgmResult<TableInfo> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(pure_name)))?;
    let column_rows: Vec<(i64, String, String, i64, Option<String>, i64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<_, _>>()?;

    let columns: Vec<ColumnInfo> = column_rows
        .iter()
        .map(|(_, name, data_type, not_null, default_value, _)| ColumnInfo {
            column_name: name.clone(),
            data_type: if data_type.is_empty() {
                "TEXT".to_string()
            } else {
                data_type.clone()
            },
            not_null: Some(*not_null != 0),
            default_value: default_value.clone(),
            ..Default::default()
        })
        .collect();

    let primary_keys: Vec<String> = column_rows
        .iter()
        .filter(|(_, _, _, _, _, pk)| *pk > 0)
        .map(|(_, name, _, _, _, _)| name.clone())
        .collect();

    let primary_key = if primary_keys.is_empty() {
        None
    } else {
        Some(crate::dbinfo::PrimaryKeyInfo {
            columns_constraint: crate::dbinfo::ColumnsConstraintInfo {
                constraint: crate::dbinfo::ConstraintInfo {
                    pairing_id: None,
                    constraint_name: Some(format!("PK_{pure_name}")),
                    constraint_type: crate::dbinfo::ConstraintType::PrimaryKey,
                },
                columns: primary_keys
                    .iter()
                    .map(|c| crate::dbinfo::ColumnReference {
                        column_name: c.clone(),
                        ref_column_name: None,
                        is_included_column: None,
                        is_descending: None,
                    })
                    .collect(),
            },
        })
    };

    let mut fk_stmt = conn.prepare(&format!("PRAGMA foreign_key_list({})", quote_ident(pure_name)))?;
    let fk_rows: Vec<(i64, i64, String, String, String, String, String)> = fk_stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
            ))
        })?
        .collect::<Result<_, _>>()?;

    let mut fk_groups: Vec<FkGroup> = Vec::new();
    for (_, seq, table, from_col, to_col, on_update, on_delete) in &fk_rows {
        if *seq == 0 {
            fk_groups.push(FkGroup {
                ref_table: table.clone(),
                columns: vec![(from_col.clone(), to_col.clone())],
                on_update: on_update.clone(),
                on_delete: on_delete.clone(),
            });
        } else if let Some(last) = fk_groups.last_mut() {
            last.columns.push((from_col.clone(), to_col.clone()));
        }
    }

    let dependencies = fk_groups
        .iter()
        .map(|fk| crate::dbinfo::ForeignKeyInfo {
            columns_constraint: crate::dbinfo::ColumnsConstraintInfo {
                constraint: crate::dbinfo::ConstraintInfo {
                    pairing_id: None,
                    constraint_name: None,
                    constraint_type: crate::dbinfo::ConstraintType::ForeignKey,
                },
                columns: fk
                    .columns
                    .iter()
                    .map(|(from, _)| crate::dbinfo::ColumnReference {
                        column_name: from.clone(),
                        ref_column_name: None,
                        is_included_column: None,
                        is_descending: None,
                    })
                    .collect(),
            },
            ref_schema_name: None,
            ref_table_name: fk.ref_table.clone(),
            update_action: if fk.on_update.is_empty() {
                None
            } else {
                Some(fk.on_update.clone())
            },
            delete_action: if fk.on_delete.is_empty() {
                None
            } else {
                Some(fk.on_delete.clone())
            },
        })
        .collect();

    Ok(TableInfo {
        object: crate::dbinfo::DatabaseObjectInfo {
            pure_name: pure_name.to_string(),
            schema_name: None,
            pairing_id: None,
            object_id: None,
            create_date: None,
            modify_date: None,
            hash_code: None,
            object_type_field: None,
            object_comment: create_sql.map(|s| s.to_string()),
        },
        columns,
        primary_key,
        sorting_key: None,
        foreign_keys: Some(dependencies),
        dependencies: None,
        indexes: None,
        uniques: None,
        checks: None,
        table_row_count: None,
        table_engine: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_def(db_path: &str) -> ConnectionDefinition {
        ConnectionDefinition {
            engine: SQLITE_ENGINE.to_string(),
            name: "test".to_string(),
            database_file: Some(db_path.to_string()),
            ..Default::default()
        }
    }

    fn tempfile_path() -> String {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("dbgate_test_{}_{}", std::process::id(), nanos));
        // Create a unique directory so SQLite journal/WAL temp files cannot
        // collide across concurrently-running tests in the shared temp dir.
        let _ = std::fs::create_dir_all(&dir);
        dir.push("test.sqlite");
        dir.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let dir = std::path::Path::new(path).parent().map(|p| p.to_path_buf());
        let _ = std::fs::remove_file(path);
        if let Some(dir) = dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn query_select_returns_rows_and_columns() {
        let driver = SqliteDriver::new();
        let tmp = tempfile_path();
        let handle = driver.connect(&make_def(&tmp)).unwrap();

        driver
            .query(&handle, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &QueryOptions::default())
            .unwrap();
        driver
            .query(&handle, "INSERT INTO t (name) VALUES ('a'), ('b')", &QueryOptions::default())
            .unwrap();

        let result = driver
            .query(&handle, "SELECT * FROM t ORDER BY id", &QueryOptions::default())
            .unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["name"], Value::String("a".to_string()));

        driver.close(handle).unwrap();
        cleanup(&tmp);
    }

    #[test]
    fn version_reports_sqlite() {
        let driver = SqliteDriver::new();
        let tmp = tempfile_path();
        let handle = driver.connect(&make_def(&tmp)).unwrap();
        let version = driver.get_version(&handle).unwrap();
        assert!(version.version.starts_with('3'));
        assert!(version.version_text.unwrap().starts_with("SQLite"));
        driver.close(handle).unwrap();
        cleanup(&tmp);
    }

    #[test]
    fn analyse_full_discovers_table() {
        let driver = SqliteDriver::new();
        let tmp = tempfile_path();
        let handle = driver.connect(&make_def(&tmp)).unwrap();
        driver
            .query(
                &handle,
                "CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                &QueryOptions::default(),
            )
            .unwrap();

        let info = driver.analyse_full(&handle, "3.x").unwrap();
        assert_eq!(info.tables.len(), 1);
        assert_eq!(info.tables[0].object.pure_name, "customer");
        assert_eq!(info.tables[0].columns.len(), 2);
        assert!(info.tables[0].primary_key.is_some());

        driver.close(handle).unwrap();
        cleanup(&tmp);
    }
}
