//! Oracle engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-oracle/src/backend/` on top of the
//! official [`oracledb`] pure-Rust client. The `oracledb` crate spawns its own
//! background transport and exposes a blocking (synchronous) [`oracledb::Connection`]
//! which is `Send + Sync`, so unlike the tokio-based drivers there is no
//! per-connection runtime here — method calls are synchronous.
//!
//! Oracle has no separate databases: a connection operates within a single
//! `CURRENT_SCHEMA`. The analyser's `$owner` placeholder is replaced with that
//! schema and `=OBJECT_ID_CONDITION` with either `= 'tables:NAME'` (single
//! table) or `is not null` (full analysis). All catalog queries read from the
//! `all_*` data-dictionary views (objects visible to the current user).
//!
//! Value mapping mirrors the `oracledb` Node driver options used by the
//! original plugin: DATE/TIMESTAMP become `YYYY-MM-DDTHH:MM:SS` ISO strings,
//! BLOB/RAW become `$binary` (base64), and CLOB/NCLOB are fetched as strings.

use std::any::Any;
use std::collections::HashSet;

use oracledb::{
    Config, Connection, DbType, Metadata, OracleNumber, OracleTimestamp, Row,
};
use serde_json::{Map, Value};

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{
    CallableObjectInfo, ColumnInfo, ColumnReference, ColumnsConstraintInfo, ConstraintInfo,
    ConstraintType, DatabaseInfo, DatabaseObjectInfo, ForeignKeyInfo, FunctionInfo, IndexInfo,
    NamedObjectInfo, ParameterInfo, ParameterMode, PrimaryKeyInfo, ProcedureInfo, SqlObjectInfo,
    TableInfo, TriggerEventType, TriggerInfo, TriggerTiming, UniqueInfo, ViewInfo,
};
use crate::driver::{
    Capabilities, DatabaseEntry, DbHandle, EngineDriver, QueryOptions, ServerVersion, StreamSink,
    StreamSeverity, WriteTableOptions,
};
use crate::error::{DbgmError, DbgmResult};
use crate::query::{QueryResult, QueryResultColumn};

/// Dotted engine id for Oracle.
pub const ORACLE_ENGINE: &str = "oracle@dbgate-plugin-oracle";

/// An open Oracle connection: the blocking [`Connection`] (Send + Sync, so no
/// mutex) and the current schema used for `$owner` substitution.
struct OracleConnection {
    conn: Connection,
    database: Option<String>,
}

/// The Oracle driver.
pub struct OracleDriver;

impl OracleDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for OracleDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(OracleDriver::new())
}

fn downcast(handle: &DbHandle) -> DbgmResult<&OracleConnection> {
    handle
        .downcast_ref::<OracleConnection>()
        .ok_or_else(|| DbgmError::new("handle is not an Oracle connection"))
}

fn out(err: oracledb::Error) -> DbgmError {
    DbgmError::new(format!("Oracle error: {err}"))
}

/// Build an Oracle connect string from a connection definition. The generic
/// [`ConnectionDefinition`] does not carry a dedicated Oracle service name, so
/// the `server:port` form is used; the `database` field is applied afterwards
/// as the `CURRENT_SCHEMA`. When `auth_type` is `url`, `database` is treated
/// as a full connect string (host[:port]/service) instead.
fn connect_string(def: &ConnectionDefinition) -> String {
    let server = def.server.clone().unwrap_or_default();
    let port = def.port.unwrap_or(1521);
    if def.auth_type.as_deref() == Some("url") {
        return def.database.clone().unwrap_or_else(|| format!("{server}:{port}"));
    }
    format!("{server}:{port}")
}

impl EngineDriver for OracleDriver {
    fn engine(&self) -> &str {
        ORACLE_ENGINE
    }

    fn title(&self) -> &str {
        "OracleDB"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: true,
            supports_transactions: true,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: true,
            default_port: Some(1521),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let config = Config::default()
            .set_credentials(&def.user.clone().unwrap_or_default(), &def.password.clone().unwrap_or_default())
            .set_connect_string(&connect_string(def))
            .map_err(out)?;
        let conn = oracledb::connect(config).map_err(out)?;

        let database = def.database.clone();
        let conn = conn;
        if let Some(db) = &database {
            conn.execute(
                &format!("ALTER SESSION SET CURRENT_SCHEMA = \"{db}\""),
                &[],
            )
            .map_err(out)?;
        }

        Ok(Box::new(OracleConnection { conn, database }))
    }

    fn close(&self, handle: DbHandle) -> DbgmResult<()> {
        drop(handle);
        Ok(())
    }

    fn query(&self, handle: &DbHandle, sql: &str, _options: &QueryOptions) -> DbgmResult<QueryResult> {
        let conn = downcast(handle)?;
        blocking_query(conn, sql)
    }

    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
        let conn = downcast(handle)?;
        let result = blocking_query(conn, sql)?;
        let (columns, rows) = (result.columns, result.rows);

        let mut rows_affected: u64 = 0;
        if columns.is_empty() {
            rows_affected += rows.len() as u64;
        } else {
            (sink.on_recordset)(&columns);
            for row in &rows {
                (sink.on_row)(row);
            }
            rows_affected += rows.len() as u64;
        }

        if rows_affected > 0 {
            (sink.on_info)(&crate::driver::StreamInfo {
                message: format!("{rows_affected} rows affected"),
                severity: StreamSeverity::Info,
                rows_affected: Some(rows_affected),
            });
        }
        (sink.on_done)();
        Ok(())
    }

    fn get_version(&self, handle: &DbHandle) -> DbgmResult<ServerVersion> {
        let conn = downcast(handle)?;
        let version = conn.conn.version().map_err(out)?;
        // OracleVersion is a 5-tuple: (major, minor, update, patch, ...).
        let version_text = format!("Oracle {}c", version.0);
        let full = version.to_string();
        Ok(ServerVersion {
            version: full,
            version_text: Some(version_text),
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = downcast(handle)?;
        let result = blocking_query(conn, "SELECT username AS name FROM all_users ORDER BY username")?;
        Ok(result
            .rows
            .iter()
            .filter_map(|r| r.get("name").and_then(|v| v.as_str()).map(String::from))
            .map(|name| DatabaseEntry {
                name,
                size_on_disk: None,
                empty: None,
            })
            .collect())
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_oracle_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_oracle_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new("Oracle write_table streaming is not yet ported"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn blocking_query(conn: &OracleConnection, sql: &str) -> DbgmResult<QueryResult> {
    let sql = trim_trailing_semicolon(sql);
    let cursor = conn.conn.query(&sql, &[]).map_err(out)?;
    let columns = cursor.columns().clone();
    let mut rows = Vec::new();
    for row_result in cursor {
        let row = row_result.map_err(out)?;
        rows.push(row_to_value(&row, &columns));
    }
    let metadata: Vec<QueryResultColumn> = columns
        .iter()
        .map(|c| QueryResultColumn {
            column_name: c.name().to_string(),
            data_type: Some(c.db_type().name().to_string()),
            ..Default::default()
        })
        .collect();
    Ok(QueryResult { rows, columns: metadata })
}

/// The original driver strips a single trailing semicolon before executing.
fn trim_trailing_semicolon(sql: &str) -> String {
    match sql.trim_end().strip_suffix(';') {
        Some(trimmed) => trimmed.to_string(),
        None => sql.trim_end().to_string(),
    }
}

// ---------------------------------------------------------------------------
// Value mapping: oracledb cell -> DbGate JSON Value
// ---------------------------------------------------------------------------

fn binary_value(bytes: &[u8]) -> Value {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    Value::Object(Map::from_iter([(
        "$binary".into(),
        Value::Object(Map::from_iter([("base64".into(), Value::String(b64))])),
    )]))
}

/// Port of the original `nativeDateToIsoString`: `YYYY-MM-DDTHH:MM:SS`.
fn timestamp_to_iso(ts: &oracledb::OracleTimestamp) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second()
    )
}

/// Decode a single cell to a DbGate JSON [`Value`] based on its Oracle type.
/// Dispatch happens on the column's [`DbType`], using the crate's public typed
/// `Row::get` API (the internal `DbValue` enum is crate-private).
fn cell_value(row: &Row, index: usize, ty: &'static DbType) -> Value {
    use oracledb::DB_TYPE_BINARY_DOUBLE;
    use oracledb::DB_TYPE_BINARY_FLOAT;
    use oracledb::DB_TYPE_BLOB;
    use oracledb::DB_TYPE_BOOLEAN;
    use oracledb::DB_TYPE_DATE;
    use oracledb::DB_TYPE_LONG_RAW;
    use oracledb::DB_TYPE_NUMBER;
    use oracledb::DB_TYPE_RAW;
    use oracledb::DB_TYPE_TIMESTAMP;
    use oracledb::DB_TYPE_TIMESTAMP_LTZ;
    use oracledb::DB_TYPE_TIMESTAMP_TZ;

    let num = ty.num();

    // Binary (RAW / LONG RAW / BLOB) -> $binary base64.
    if num == DB_TYPE_RAW.num()
        || num == DB_TYPE_LONG_RAW.num()
        || num == DB_TYPE_BLOB.num()
    {
        if let Ok(Some(bytes)) = row.get::<Option<Vec<u8>>>(index) {
            return binary_value(&bytes);
        }
        return Value::Null;
    }

    // Date/time -> ISO `YYYY-MM-DDTHH:MM:SS`, mirroring `nativeDateToIsoString`.
    if num == DB_TYPE_DATE.num()
        || num == DB_TYPE_TIMESTAMP.num()
        || num == DB_TYPE_TIMESTAMP_LTZ.num()
        || num == DB_TYPE_TIMESTAMP_TZ.num()
    {
        if let Ok(Some(ts)) = row.get::<Option<OracleTimestamp>>(index) {
            return Value::String(timestamp_to_iso(&ts));
        }
        return Value::Null;
    }

    // String types (VARCHAR2 / CHAR / CLOB / NCLOB / ROWID / UROWID) -> string.
    if ty.is_string_type() {
        if let Ok(Some(s)) = row.get::<Option<String>>(index) {
            return Value::String(s);
        }
        return Value::Null;
    }

    // NUMBER -> string to preserve full decimal precision on large values.
    if num == DB_TYPE_NUMBER.num() {
        if let Ok(Some(n)) = row.get::<Option<OracleNumber>>(index) {
            return Value::String(n.to_string());
        }
        return Value::Null;
    }

    // BINARY_DOUBLE / BINARY_FLOAT -> JSON number.
    if num == DB_TYPE_BINARY_DOUBLE.num() {
        if let Ok(Some(d)) = row.get::<Option<f64>>(index) {
            return serde_json::Number::from_f64(d).map(Value::Number).unwrap_or(Value::Null);
        }
        return Value::Null;
    }
    if num == DB_TYPE_BINARY_FLOAT.num() {
        if let Ok(Some(f)) = row.get::<Option<f32>>(index) {
            return serde_json::Number::from_f64(f64::from(f))
                .map(Value::Number)
                .unwrap_or(Value::Null);
        }
        return Value::Null;
    }

    // BOOLEAN -> JSON bool.
    if num == DB_TYPE_BOOLEAN.num() {
        if let Ok(Some(b)) = row.get::<Option<bool>>(index) {
            return Value::Bool(b);
        }
        return Value::Null;
    }

    Value::Null
}

fn row_to_value(row: &Row, columns: &[Metadata]) -> Value {
    let mut map = Map::new();
    for (i, col) in columns.iter().enumerate() {
        map.insert(col.name().to_string(), cell_value(row, i, col.db_type()));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Analysis helpers (ported from Analyser.js / sql/*.js)
// ---------------------------------------------------------------------------

fn get_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn get_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| v.as_i64())
}

fn object_id(kind: &str, pure: &str) -> String {
    format!("{kind}:{pure}")
}

/// Substitute the analyser `=OBJECT_ID_CONDITION` / `$owner` placeholders.
/// `object_id` is `None` in full-analysis mode (all objects). Oracle has no
/// separate schema placeholder beyond `$owner`.
fn substitute_conditions(template: &str, database: &str, object_id: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!("'tables:{id}'"),
        None => "is not null".to_string(),
    };
    template
        .replace("$owner", database)
        .replace("OBJECT_ID_CONDITION", &object_cond)
}

fn normalize_type_name(data_type: &str) -> String {
    match data_type {
        "character varying" => "varchar".to_string(),
        "timestamp without time zone" => "timestamp".to_string(),
        other => other.to_string(),
    }
}

fn is_type_string(t: &str) -> bool {
    matches!(
        t.to_lowercase().as_str(),
        "varchar" | "varchar2" | "char" | "nchar" | "nvarchar2" | "clob" | "nclob" | "raw"
    )
}

fn is_type_numeric(t: &str) -> bool {
    matches!(t.to_lowercase().as_str(), "number" | "decimal" | "numeric")
}

/// Port of `getColumnInfo`'s full data-type construction. Oracle's `data_length`
/// is a byte length, not a character count; the original driver uses it for
/// string columns, so we mirror that. The original's `numeric_ccale` typo
/// (which dropped the scale) is fixed here by honouring `numeric_scale`.
fn get_column_info(
    row: &Value,
    column_name: &str,
    quote_defaults: bool,
) -> ColumnInfo {
    let data_type = normalize_type_name(&get_str(row, "data_type").unwrap_or_default());
    let char_max_length = get_i64(row, "char_max_length");
    let numeric_precision = get_i64(row, "numeric_precision");
    let numeric_scale = get_i64(row, "numeric_scale");

    let mut full_type = data_type.clone();
    if let Some(len) = char_max_length {
        if is_type_string(&data_type) {
            full_type = format!("{data_type}({len})");
        }
    }
    if let (Some(p), Some(s)) = (numeric_precision, numeric_scale) {
        if is_type_numeric(&data_type) {
            full_type = format!("{data_type}({p},{s})");
        }
    }

    let is_nullable = get_str(row, "is_nullable");
    let raw_default = get_str(row, "default_value");
    let auto_increment = raw_default
        .as_deref()
        .map(|d| d.ends_with(".nextval") || d.ends_with(".NEXTVAL"))
        .unwrap_or(false);
    let default_value = if auto_increment || !quote_defaults {
        raw_default
    } else {
        raw_default.as_deref().map(quote_default_value)
    };

    ColumnInfo {
        column_name: column_name.to_string(),
        data_type: full_type,
        not_null: Some(!(is_nullable.as_deref() == Some("Y")
            || is_nullable.as_deref() == Some("YES")
            || is_nullable.as_deref() == Some("y"))),
        auto_increment: Some(auto_increment),
        default_value: if auto_increment { None } else { default_value },
        ..Default::default()
    }
}

/// Port of `quoteDefaultValue`: numeric and `NULL` defaults stay bare; other
/// string defaults are single-quoted.
fn quote_default_value(value: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    if value.parse::<f64>().is_ok() {
        return value.to_string();
    }
    if value.eq_ignore_ascii_case("NULL") {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "''"))
}

fn build_primary_key(pure: &str, pk_rows: &[Value]) -> Option<PrimaryKeyInfo> {
    let filtered: Vec<&Value> = pk_rows
        .iter()
        .filter(|r| get_str(r, "pure_name").as_deref() == Some(pure))
        .collect();
    if filtered.is_empty() {
        return None;
    }
    let constraint_name = filtered[0]
        .get("constraint_name")
        .and_then(|v| v.as_str())
        .map(String::from);
    Some(PrimaryKeyInfo {
        columns_constraint: ColumnsConstraintInfo {
            constraint: ConstraintInfo {
                pairing_id: None,
                constraint_name,
                constraint_type: ConstraintType::PrimaryKey,
            },
            columns: filtered
                .iter()
                .map(|r| ColumnReference {
                    column_name: get_str(r, "column_name").unwrap_or_default(),
                    ref_column_name: None,
                    is_included_column: None,
                    is_descending: None,
                })
                .collect(),
        },
    })
}

fn build_foreign_keys(pure: &str, fk_rows: &[Value]) -> Vec<ForeignKeyInfo> {
    let mut grouped: Vec<(String, Vec<&Value>)> = Vec::new();
    for r in fk_rows {
        if get_str(r, "pure_name").as_deref() != Some(pure) {
            continue;
        }
        let name = get_str(r, "constraint_name").unwrap_or_default();
        if let Some((_, group)) = grouped.iter_mut().find(|(n, _)| n == &name) {
            group.push(r);
        } else {
            grouped.push((name.clone(), vec![r]));
        }
    }
    grouped
        .into_iter()
        .map(|(name, group)| {
            let first = group[0];
            ForeignKeyInfo {
                columns_constraint: ColumnsConstraintInfo {
                    constraint: ConstraintInfo {
                        pairing_id: None,
                        constraint_name: Some(name),
                        constraint_type: ConstraintType::ForeignKey,
                    },
                    columns: group
                        .iter()
                        .map(|r| ColumnReference {
                            column_name: get_str(r, "column_name").unwrap_or_default(),
                            ref_column_name: get_str(r, "ref_column_name"),
                            is_included_column: None,
                            is_descending: None,
                        })
                        .collect(),
                },
                ref_schema_name: None,
                ref_table_name: get_str(first, "ref_table_name").unwrap_or_default(),
                update_action: Some(get_str(first, "update_action").unwrap_or_default()),
                delete_action: Some(get_str(first, "delete_action").unwrap_or_default()),
            }
        })
        .collect()
}

fn build_indexes_and_uniques(
    pure: &str,
    index_rows: &[Value],
    unique_names: &HashSet<String>,
) -> (Vec<IndexInfo>, Vec<UniqueInfo>) {
    let mut indexes = Vec::new();
    let mut uniques = Vec::new();

    for idx in index_rows
        .iter()
        .filter(|r| get_str(r, "tableName").as_deref() == Some(pure))
    {
        let index_name = get_str(idx, "constraintName").unwrap_or_default();

        // Skip system-generated constraint-backed indexes (SYS_C...).
        if index_name.starts_with("SYS_C") {
            continue;
        }

        let is_unique = get_str(idx, "Unique").as_deref() == Some("UNIQUE");
        let is_descending = get_str(idx, "descending").as_deref() == Some("DESC");

        let column = ColumnReference {
            column_name: get_str(idx, "columnName").unwrap_or_default(),
            ref_column_name: None,
            is_included_column: None,
            is_descending: Some(is_descending),
        };

        let constraint = ConstraintInfo {
            pairing_id: None,
            constraint_name: Some(index_name.clone()),
            constraint_type: if unique_names.contains(&index_name) {
                ConstraintType::Unique
            } else {
                ConstraintType::Index
            },
        };

        let columns_constraint = ColumnsConstraintInfo {
            constraint,
            columns: vec![column],
        };

        if unique_names.contains(&index_name) {
            uniques.push(UniqueInfo { columns_constraint });
        } else {
            indexes.push(IndexInfo {
                columns_constraint,
                is_unique,
                index_type: get_str(idx, "indexType"),
                filter_definition: None,
            });
        }
    }
    (indexes, uniques)
}

fn build_parameters(pure: &str, param_rows: &[Value]) -> Vec<ParameterInfo> {
    param_rows
        .iter()
        .filter(|r| get_str(r, "pure_name").as_deref() == Some(pure))
        .map(|r| {
            let mode = match get_str(r, "parameter_mode").as_deref() {
                Some("OUT") => ParameterMode::Out,
                Some("INOUT") => ParameterMode::InOut,
                Some("RETURN") => ParameterMode::Return,
                _ => ParameterMode::In,
            };
            ParameterInfo {
                parameter_name: get_str(r, "parameter_name").unwrap_or_default(),
                data_type: normalize_type_name(&get_str(r, "data_type").unwrap_or_default()),
                parameter_mode: Some(mode),
                position: get_i64(r, "ordinal_position"),
            }
        })
        .collect()
}

fn db_table_object(value: &Value, object_id: &str, content_hash: Option<&str>) -> DatabaseObjectInfo {
    DatabaseObjectInfo {
        pure_name: get_str(value, "pure_name").unwrap_or_default(),
        schema_name: None,
        pairing_id: None,
        object_id: Some(object_id.to_string()),
        create_date: None,
        modify_date: get_str(value, "modify_date"),
        hash_code: content_hash.map(String::from),
        object_type_field: None,
        object_comment: get_str(value, "object_comment"),
    }
}

fn analyser_query(
    handle: &DbHandle,
    template: &str,
    object_id: Option<&str>,
    database: &str,
) -> DbgmResult<QueryResult> {
    let conn = downcast(handle)?;
    let sql = substitute_conditions(template, database, object_id);
    blocking_query(conn, &sql)
}

// ---------------------------------------------------------------------------
// analyse_full / analyse_single_table
// ---------------------------------------------------------------------------

fn collect_columns(columns_rows: &[Value], pure: &str, quote_defaults: bool) -> Vec<ColumnInfo> {
    columns_rows
        .iter()
        .filter(|c| get_str(c, "pure_name").as_deref() == Some(pure))
        .map(|c| get_column_info(c, &get_str(c, "column_name").unwrap_or_default(), quote_defaults))
        .collect()
}

fn analyse_oracle_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let conn = downcast(handle)?;
    let database = conn.database.clone().unwrap_or_default();

    let tables = analyser_query(handle, SQL_TABLES, None, &database)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, None, &database)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, None, &database)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, None, &database)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, None, &database)?.rows;
    let views_rows = analyser_query(handle, SQL_VIEWS, None, &database)?.rows;
    let routines_rows = analyser_query(handle, SQL_ROUTINES, None, &database)?.rows;
    let param_rows = analyser_query(handle, SQL_PARAMETERS, None, &database)?.rows;
    let trigger_rows = analyser_query(handle, SQL_TRIGGERS, None, &database)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, &database)?.rows;

    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraintName"))
        .collect();

    let mut result_tables = Vec::new();
    for table in &tables {
        let pure = get_str(table, "pure_name").unwrap_or_default();
        let key = object_id("tables", &pure);
        let content_hash = get_str(table, "hash_code");

        let columns = collect_columns(&columns_rows, &pure, true);
        let (indexes, uniques) = build_indexes_and_uniques(&pure, &indexes_rows, &unique_names);

        result_tables.push(TableInfo {
            object: db_table_object(table, &key, content_hash.as_deref()),
            columns,
            primary_key: build_primary_key(&pure, &pk_rows),
            sorting_key: None,
            foreign_keys: Some(build_foreign_keys(&pure, &fk_rows)),
            dependencies: None,
            indexes: Some(indexes),
            uniques: Some(uniques),
            checks: None,
            table_row_count: get_i64(table, "table_row_count"),
            table_engine: None,
        });
    }

    let mut views = Vec::new();
    for view in &views_rows {
        let pure = get_str(view, "pure_name").unwrap_or_default();
        let create_sql = get_str(view, "create_sql").map(|v| {
            format!("CREATE VIEW \"{pure}\"\nAS\n{v}")
        });
        views.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_table_object(
                    view,
                    &object_id("views", &pure),
                    get_str(view, "hash_code").as_deref(),
                ),
                create_sql,
                requires_format: None,
            },
            columns: collect_columns(&columns_rows, &pure, true),
        });
    }

    let mut procedures = Vec::new();
    let mut functions = Vec::new();
    for routine in &routines_rows {
        let pure = get_str(routine, "pure_name").unwrap_or_default();
        let params = build_parameters(&pure, &param_rows);
        let is_procedure = get_str(routine, "object_type").as_deref() == Some("PROCEDURE");
        let source_code = get_str(routine, "source_code").unwrap_or_default();
        // Original wraps routines with SQL*Plus replaceable terminator.
        let create_sql = format!("SET SQLTERMINATOR \"/\"\n{source_code}\n/\n");
        let callable = CallableObjectInfo {
            object: SqlObjectInfo {
                object: db_table_object(
                    routine,
                    &object_id(if is_procedure { "procedures" } else { "functions" }, &pure),
                    get_str(routine, "hash_code").as_deref(),
                ),
                create_sql: Some(create_sql),
                requires_format: None,
            },
            parameters: if params.is_empty() { None } else { Some(params) },
        };
        if is_procedure {
            procedures.push(ProcedureInfo { callable });
        } else {
            functions.push(FunctionInfo {
                callable,
                return_type: None,
            });
        }
    }

    let mut triggers = Vec::new();
    for trg in &trigger_rows {
        let trigger_name = get_str(trg, "trigger_name").unwrap_or_default();
        let table_name = get_str(trg, "table_name").unwrap_or_default();
        let trigger_timing = get_str(trg, "trigger_timing").unwrap_or_default();
        let event_type = get_str(trg, "event_type").unwrap_or_default();
        let definition = get_str(trg, "definition").unwrap_or_default();
        let create_sql = format!(
            "SET SQLTERMINATOR \"/\"\nCREATE TRIGGER \"{trigger_name}\" {trigger_timing} {event_type} ON \"{table_name}\" FOR EACH ROW {definition}\n/\n"
        );
        let object = DatabaseObjectInfo {
            pure_name: trigger_name.clone(),
            schema_name: None,
            pairing_id: None,
            object_id: Some(format!("triggers:{trigger_name}")),
            create_date: None,
            modify_date: None,
            hash_code: Some(format!("triggers:{trigger_name}")),
            object_type_field: None,
            object_comment: None,
        };
        let timing = match trigger_timing.to_uppercase().as_str() {
            "AFTER" => Some(TriggerTiming::After),
            "INSTEAD OF" => Some(TriggerTiming::InsteadOf),
            _ => Some(TriggerTiming::Before),
        };
        let event = match event_type.to_uppercase().as_str() {
            "INSERT" => Some(TriggerEventType::Insert),
            "UPDATE" => Some(TriggerEventType::Update),
            "DELETE" => Some(TriggerEventType::Delete),
            _ => None,
        };
        triggers.push(TriggerInfo {
            object: SqlObjectInfo {
                object,
                create_sql: Some(create_sql),
                requires_format: None,
            },
            function_name: None,
            table_name: Some(table_name),
            trigger_timing: timing,
            event_type: event,
        });
    }

    Ok(DatabaseInfo {
        tables: result_tables,
        views,
        matviews: None,
        procedures,
        functions,
        triggers,
        ..Default::default()
    })
}

fn analyse_oracle_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let conn = downcast(handle)?;
    let database = conn.database.clone().unwrap_or_default();
    let pure = name.pure_name.clone();
    let key = object_id("tables", &pure);
    let object_id_some = Some(key.as_str());

    let tables = analyser_query(handle, SQL_TABLES, object_id_some, &database)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, object_id_some, &database)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, object_id_some, &database)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, object_id_some, &database)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, object_id_some, &database)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, &database)?.rows;
    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraintName"))
        .collect();

    let table = tables
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table not found: {key}")))?;
    let pure_ref = get_str(table, "pure_name").unwrap_or_default();
    let content_hash = get_str(table, "hash_code");

    let columns = collect_columns(&columns_rows, &pure_ref, true);
    let (indexes, uniques) = build_indexes_and_uniques(&pure_ref, &indexes_rows, &unique_names);

    Ok(TableInfo {
        object: db_table_object(table, &key, content_hash.as_deref()),
        columns,
        primary_key: build_primary_key(&pure_ref, &pk_rows),
        sorting_key: None,
        foreign_keys: Some(build_foreign_keys(&pure_ref, &fk_rows)),
        dependencies: None,
        indexes: Some(indexes),
        uniques: Some(uniques),
        checks: None,
        table_row_count: get_i64(table, "table_row_count"),
        table_engine: None,
    })
}

// ---------------------------------------------------------------------------
// Catalog query templates (ported from plugins/dbgate-plugin-oracle/.../sql)
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = "
select
    table_name as \"pure_name\",
    num_rows * avg_row_len as \"size_bytes\",
    num_rows as \"table_row_count\"
  from
    all_tables
  where OWNER='$owner' AND 'tables:' || TABLE_NAME =OBJECT_ID_CONDITION
";

const SQL_COLUMNS: &str = "
select
  table_name as \"pure_name\",
  column_name as \"column_name\",
  nullable as \"is_nullable\",
  data_type as \"data_type\",
  data_length as \"char_max_length\",
  data_precision as \"numeric_precision\",
  data_scale as \"numeric_scale\",
  data_default as \"default_value\"
  FROM all_tab_columns av
  where OWNER='$owner' AND 'tables:' || TABLE_NAME =OBJECT_ID_CONDITION
order by column_id
";

const SQL_PRIMARY_KEYS: &str = "
select
  pk.constraint_name as \"constraint_name\",
  pk.table_name as \"pure_name\",
  basecol.column_name as \"column_name\"
from all_cons_columns basecol,
 all_constraints pk
where constraint_type = 'P'
and basecol.owner = pk.owner
and basecol.constraint_name = pk.constraint_name
and basecol.table_name = pk.table_name
and 'tables:' || basecol.table_name =OBJECT_ID_CONDITION
and pk.owner = '$owner'
order by basecol.position
;

-- Oracle stores a constraint's column positions in ALL_CONS_COLUMNS.POSITION.
";

const SQL_FOREIGN_KEYS: &str = "
select  fk.constraint_name as \"constraint_name\",
  fk.table_name as \"pure_name\",
  fk.delete_rule as \"update_action\",
  fk.delete_rule as \"delete_action\",
  ref.table_name as \"ref_table_name\",
  basecol.column_name as \"column_name\",
  refcol.column_name as \"ref_column_name\"
from all_cons_columns refcol, all_cons_columns basecol, all_constraints ref, all_constraints fk
where fk.OWNER = '$owner' AND fk.constraint_type = 'R'
and ref.owner = fk.r_owner
and ref.constraint_name = fk.r_constraint_name
and basecol.owner = fk.owner
and basecol.constraint_name = fk.constraint_name
and basecol.table_name = fk.table_name
and refcol.owner = ref.owner
and refcol.constraint_name = ref.constraint_name
and refcol.table_name = ref.table_name
AND 'tables:' || fk.table_name =OBJECT_ID_CONDITION
order by basecol.position
";

const SQL_INDEXES: &str = "
select  i.table_name as \"tableName\",
        i.index_name as \"constraintName\",
        i.index_type as \"indexType\",
        i.uniqueness as \"Unique\",
        ic.column_name as \"columnName\",
        ic.descend as \"descending\"
from all_ind_columns ic, all_indexes i
where INDEX_OWNER = '$owner' AND ic.index_owner = i.owner
and ic.index_name = i.index_name
and 'tables:' || i.table_name =OBJECT_ID_CONDITION
order by i.table_owner,
         i.table_name,
         i.index_name,
         ic.column_position
";

const SQL_VIEWS: &str = "
select
  view_name as \"pure_name\",
  text as \"create_sql\"
  from all_views av
  where owner = '$owner' and text is not null
";

const SQL_ROUTINES: &str = "
SELECT
  name as \"pure_name\",
  type as \"object_type\",
  LISTAGG(text, '') WITHIN GROUP (ORDER BY line) AS \"source_code\",
  ora_hash(LISTAGG(text, '') WITHIN GROUP (ORDER BY line)) AS \"hash_code\"
FROM all_source
WHERE type in ('FUNCTION', 'PROCEDURE') AND OWNER = '$owner'
GROUP BY name, type
";

const SQL_PARAMETERS: &str = "
SELECT
    o.OBJECT_NAME AS \"pure_name\",
    a.ARGUMENT_NAME AS \"parameter_name\",
    a.POSITION AS \"ordinal_position\",
    a.DATA_TYPE AS \"data_type\",
    a.CHAR_LENGTH AS \"char_max_length\",
    a.DATA_PRECISION AS \"numeric_precision\",
    a.DATA_SCALE AS \"numeric_scale\",
    a.IN_OUT AS \"parameter_mode\"
FROM
    all_objects o
LEFT JOIN
    all_arguments a
    ON o.object_id = a.object_id
WHERE
    o.object_type IN ('FUNCTION', 'PROCEDURE')
    AND o.OWNER = '$owner'
ORDER BY
    a.POSITION
";

const SQL_TRIGGERS: &str = "
SELECT
    TRIGGER_TYPE AS \"trigger_timing\",
    TRIGGERING_EVENT AS \"event_type\",
    TRIGGER_BODY AS \"definition\",
    TRIGGER_NAME AS \"trigger_name\",
    TABLE_NAME AS \"table_name\"
FROM
    all_triggers
WHERE
    OWNER='$owner'
    AND 'tables:' || TABLE_NAME =OBJECT_ID_CONDITION
";

const SQL_UNIQUE_NAMES: &str = "
select constraint_name as \"constraintName\"
from all_constraints
where owner='$owner' and constraint_type = 'U'
  and 'tables:' || table_name =OBJECT_ID_CONDITION
";

// ---------------------------------------------------------------------------
// Unit tests (pure helpers only; no live server required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_object_conditions() {
        let sql = substitute_conditions(
            "where OWNER='$owner' AND 'tables:' || TABLE_NAME =OBJECT_ID_CONDITION",
            "HR",
            Some("users"),
        );
        assert!(sql.contains("OWNER='HR'"));
        assert!(sql.contains("TABLE_NAME ='tables:users'"));
        let sql_all = substitute_conditions(
            "where OWNER='$owner' AND 'tables:' || TABLE_NAME =OBJECT_ID_CONDITION",
            "HR",
            None,
        );
        assert!(sql_all.contains("OWNER='HR'"));
        assert!(sql_all.contains("is not null"));
    }

    #[test]
    fn quotes_default_values() {
        assert_eq!(quote_default_value("42"), "42");
        assert_eq!(quote_default_value("NULL"), "NULL");
        assert_eq!(quote_default_value("hello"), "'hello'");
    }

    #[test]
    fn trims_trailing_semicolon() {
        assert_eq!(trim_trailing_semicolon("select 1;"), "select 1");
        assert_eq!(trim_trailing_semicolon("select 1"), "select 1");
        assert_eq!(trim_trailing_semicolon("  select 1;  "), "  select 1");
    }

    #[test]
    fn builds_primary_key_from_rows() {
        let rows = serde_json::json!([
            {"pure_name": "users", "constraint_name": "PK_USERS", "column_name": "id"},
            {"pure_name": "users", "constraint_name": "PK_USERS", "column_name": "tenant"},
        ])
        .as_array()
        .cloned()
        .unwrap_or_default();
        let pk = build_primary_key("users", &rows).unwrap();
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
        assert_eq!(pk.columns_constraint.columns[1].column_name, "tenant");
    }

    #[test]
    fn computes_full_data_type_names() {
        let row = serde_json::json!({
            "data_type": "VARCHAR2",
            "char_max_length": 250,
        });
        let info = get_column_info(&row, "name", true);
        assert_eq!(info.data_type, "VARCHAR2(250)");

        let row2 = serde_json::json!({
            "data_type": "NUMBER",
            "numeric_precision": 10,
            "numeric_scale": 2,
        });
        let info2 = get_column_info(&row2, "price", true);
        assert_eq!(info2.data_type, "NUMBER(10,2)");
    }

    #[test]
    fn detects_auto_increment_sequence_default() {
        let row = serde_json::json!({
            "data_type": "NUMBER",
            "default_value": "MY_SEQ.NEXTVAL",
        });
        let info = get_column_info(&row, "id", true);
        assert_eq!(info.auto_increment, Some(true));
        assert_eq!(info.default_value, None);
    }
}
