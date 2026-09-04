//! MySQL / MariaDB engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-mysql/src/backend/` on top of the
//! [`mysql_async`] client. Like the PostgreSQL and SQL Server drivers, the
//! synchronous [`EngineDriver`] trait is bridged to tokio by giving each
//! connection its own runtime plus a `Mutex<Conn>` and driving async calls
//! with `runtime.block_on(...)`.
//!
//! Value mapping mirrors the `mysql2` options used by the original driver:
//! binary / blob / geometry columns become `$binary` (base64), integer and
//! decimal columns keep full precision by being emitted as JSON strings, and
//! date/time columns are emitted as strings.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row};
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

/// Dotted engine id for MySQL.
pub const MYSQL_ENGINE: &str = "mysql@dbgate-plugin-mysql";
/// Dotted engine id for MariaDB.
pub const MARIADB_ENGINE: &str = "mariadb@dbgate-plugin-mysql";

/// An open MySQL/MariaDB connection: the runtime driving the tokio client and
/// the client itself (behind a mutex for serialized access).
struct MySqlConnection {
    runtime: tokio::runtime::Runtime,
    conn: Mutex<Conn>,
    database: String,
}

/// The MySQL driver.
pub struct MySqlDriver;

/// The MariaDB driver (same wire protocol, distinct engine id).
pub struct MariaDbDriver;

trait MySqlCommon {
    fn engine() -> &'static str;
    fn title() -> &'static str;
}

impl MySqlCommon for MySqlDriver {
    fn engine() -> &'static str {
        MYSQL_ENGINE
    }
    fn title() -> &'static str {
        "MySQL"
    }
}

impl MySqlCommon for MariaDbDriver {
    fn engine() -> &'static str {
        MARIADB_ENGINE
    }
    fn title() -> &'static str {
        "MariaDB"
    }
}

impl MySqlDriver {
    pub fn new() -> Self {
        Self
    }
}

impl MariaDbDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MySqlDriver {
    fn default() -> Self {
        Self::new()
    }
}
impl Default for MariaDbDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(MySqlDriver::new())
}

pub fn mariadb_driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(MariaDbDriver::new())
}

fn downcast(handle: &DbHandle) -> DbgmResult<&MySqlConnection> {
    handle
        .downcast_ref::<MySqlConnection>()
        .ok_or_else(|| DbgmError::new("handle is not a MySQL/MariaDB connection"))
}

fn out(err: mysql_async::Error) -> DbgmError {
    DbgmError::with_source("MySQL error", err)
}

/// Build a [`Conn`] from a connection definition. The database defaults to
/// `mysql` when not supplied.
fn build_opts(def: &ConnectionDefinition) -> DbgmResult<OptsBuilder> {
    let server = def
        .server
        .clone()
        .ok_or_else(|| DbgmError::new("MySQL connection requires a server host"))?;
    let database = def
        .database
        .clone()
        .unwrap_or_else(|| "mysql".to_string());
    let builder = OptsBuilder::default()
        .ip_or_hostname(server)
        .tcp_port(def.port.unwrap_or(3306) as u16)
        .user(def.user.clone())
        .pass(def.password.clone())
        .db_name(Some(database));
    Ok(builder)
}

// ---------------------------------------------------------------------------
// Value mapping: mysql_async Value -> DbGate JSON Value
// ---------------------------------------------------------------------------

/// Alias used throughout the value-mapping helpers so `mysql_async::Value` and
/// `serde_json::Value` do not collide.
type Json = serde_json::Value;

fn binary_value(bytes: &[u8]) -> Json {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    Json::Object(Map::from_iter([(
        "$binary".into(),
        Json::Object(Map::from_iter([("base64".into(), Json::String(b64))])),
    )]))
}

/// Whether a column type carries binary bytes that must be `$binary` encoded.
fn is_binary_type(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_BIT
            | ColumnType::MYSQL_TYPE_GEOMETRY
    )
}

/// Whether a column type holds an integer/decimal that must keep string
/// precision (mirrors `supportBigNumbers`/`bigNumberStrings` in mysql2).
fn is_precision_type(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::MYSQL_TYPE_LONGLONG
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_DECIMAL
    )
}

fn value_to_json(val: &mysql_async::Value, ty: ColumnType) -> Json {
    use mysql_async::Value;
    if is_binary_type(ty) {
        match val {
            Value::Bytes(b) => return binary_value(b),
            Value::NULL => return Json::Null,
            _ => {}
        }
    }
    if is_precision_type(ty) {
        return match val {
            Value::Int(i) => Json::String(i.to_string()),
            Value::UInt(u) => Json::String(u.to_string()),
            Value::Bytes(b) => Json::String(String::from_utf8_lossy(b).into_owned()),
            Value::NULL => Json::Null,
            _ => Json::Null,
        };
    }
    match val {
        Value::NULL => Json::Null,
        Value::Int(i) => Json::from(*i),
        Value::UInt(u) => Json::from(*u),
        Value::Float(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => Json::String(s),
            Err(_) => binary_value(b),
        },
        Value::Date(y, mo, d, h, mi, s, mic) => {
            let base = format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}");
            if *mic > 0 {
                Json::String(format!("{base}.{mic:06}"))
            } else {
                Json::String(base)
            }
        }
        Value::Time(neg, days, h, mi, s, _mic) => {
            let sign = if *neg { "-" } else { "" };
            Json::String(format!("{sign}{days} {h:02}:{mi:02}:{s:02}"))
        }
    }
}

fn row_to_value(row: &Row, columns: &[mysql_async::Column]) -> Json {
    let mut map = Map::new();
    for (i, col) in columns.iter().enumerate() {
        let name = col.name_str().to_string();
        let ty = col.column_type();
        let val = row
            .as_ref(i)
            .map(|v| value_to_json(v, ty))
            .unwrap_or(Json::Null);
        map.insert(name, val);
    }
    Json::Object(map)
}

fn columns_metadata(columns: &[mysql_async::Column]) -> Vec<QueryResultColumn> {
    columns
        .iter()
        .map(|c| QueryResultColumn {
            column_name: c.name_str().to_string(),
            data_type: Some(format!("{:?}", c.column_type())),
            ..Default::default()
        })
        .collect()
}

async fn run_query(
    conn: &mut Conn,
    sql: &str,
) -> Result<(Vec<QueryResultColumn>, Vec<Value>), mysql_async::Error> {
    let mut result = conn.query_iter(sql).await?;
    let columns: Vec<mysql_async::Column> = result.columns_ref().to_vec();
    let metadata: Vec<QueryResultColumn> = columns_metadata(&columns);
    let rows: Vec<Row> = result.collect::<Row>().await?;
    let values = rows.iter().map(|r| row_to_value(r, &columns)).collect();
    Ok((metadata, values))
}

// ---------------------------------------------------------------------------
// EngineDriver implementation
// ---------------------------------------------------------------------------

macro_rules! impl_driver {
    ($ty:ty) => {
        impl EngineDriver for $ty {
            fn engine(&self) -> &str {
                <$ty as MySqlCommon>::engine()
            }

            fn title(&self) -> &str {
                <$ty as MySqlCommon>::title()
            }

            fn capabilities(&self) -> Capabilities {
                Capabilities {
                    read_only_sessions: true,
                    supports_transactions: true,
                    supports_native_backup: true,
                    supports_native_restore: true,
                    supports_server_summary: true,
                    default_port: Some(3306),
                }
            }

            fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
                let opts = build_opts(def)?;
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        DbgmError::with_source("Cannot start tokio runtime for MySQL", e)
                    })?;
                let database = def
                    .database
                    .clone()
                    .unwrap_or_else(|| "mysql".to_string());

                let mut conn = runtime.block_on(async move {
                    Conn::new(opts)
                        .await
                        .map_err(|e| DbgmError::with_source("Cannot connect to MySQL", e))
                })?;

                if def.is_read_only.unwrap_or(false) {
                    runtime
                        .block_on(async {
                            conn.query_drop("SET SESSION TRANSACTION READ ONLY").await
                        })
                        .map_err(out)?;
                }

                Ok(Box::new(MySqlConnection {
                    runtime,
                    conn: Mutex::new(conn),
                    database,
                }))
            }

            fn close(&self, handle: DbHandle) -> DbgmResult<()> {
                drop(handle);
                Ok(())
            }

            fn query(
                &self,
                handle: &DbHandle,
                sql: &str,
                _options: &QueryOptions,
            ) -> DbgmResult<QueryResult> {
                let conn = downcast(handle)?;
                Self::self_blocking_query(conn, sql)
            }

            fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
                let conn = downcast(handle)?;
                let result = Self::self_blocking_query(conn, sql)?;
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
                let result = Self::self_blocking_query(conn, "show variables like 'version'")?;
                let version = result
                    .rows
                    .first()
                    .and_then(|r| r.get("Value").and_then(|v| v.as_str()))
                    .unwrap_or("unknown")
                    .to_string();
                let version_text = match version.find('-') {
                    Some(i) if version[i..].to_lowercase().contains("mariadb") => {
                        Some(format!("MariaDB {}", &version[..i]))
                    }
                    _ => Some(format!("MySQL {version}")),
                };
                Ok(ServerVersion {
                    version,
                    version_text,
                })
            }

            fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
                let conn = downcast(handle)?;
                let result = Self::self_blocking_query(conn, "show databases")?;
                Ok(result
                    .rows
                    .iter()
                    .filter_map(|r| r.get("Database").and_then(|v| v.as_str()).map(String::from))
                    .map(|name| DatabaseEntry {
                        name,
                        size_on_disk: None,
                        empty: None,
                    })
                    .collect())
            }

            fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
                analyse_myql_full(handle)
            }

            fn analyse_single_table(
                &self,
                handle: &DbHandle,
                name: &NamedObjectInfo,
            ) -> DbgmResult<TableInfo> {
                analyse_myql_table(handle, name)
            }

            fn write_table(
                &self,
                _handle: &DbHandle,
                _name: &NamedObjectInfo,
                _options: &WriteTableOptions,
            ) -> DbgmResult<()> {
                Err(DbgmError::new(
                    "MySQL write_table streaming is not yet ported",
                ))
            }

            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    };
}

impl_driver!(MySqlDriver);
impl_driver!(MariaDbDriver);

// `Self::self_blocking_query` in the driver macro resolves through this trait.
trait MySqlBlocking {
    fn self_blocking_query(handle: &MySqlConnection, sql: &str) -> DbgmResult<QueryResult>;
}

impl MySqlBlocking for MySqlDriver {
    fn self_blocking_query(handle: &MySqlConnection, sql: &str) -> DbgmResult<QueryResult> {
        let mut conn = handle
            .conn
            .lock()
            .map_err(|_| DbgmError::new("MySQL connection lock poisoned"))?;
        let (columns, rows) = handle
            .runtime
            .block_on(run_query(&mut conn, sql))
            .map_err(out)?;
        Ok(QueryResult { rows, columns })
    }
}
impl MySqlBlocking for MariaDbDriver {
    fn self_blocking_query(handle: &MySqlConnection, sql: &str) -> DbgmResult<QueryResult> {
        MySqlDriver::self_blocking_query(handle, sql)
    }
}

// ---------------------------------------------------------------------------
// Analysis helpers
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

/// Substitute the analyser `=OBJECT_ID_CONDITION` placeholder. `object_id` is
/// `None` in full-analysis mode (all objects).
fn substitute_condition(template: &str, database: &str, object_id: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!(" = '{id}'"),
        None => " is not null".to_string(),
    };
    template
        .replace("#DATABASE#", database)
        .replace("=OBJECT_ID_CONDITION", &object_cond)
}

fn is_type_string(t: &str) -> bool {
    matches!(t, "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" | "binary" | "varbinary")
}

fn is_type_numeric(t: &str) -> bool {
    matches!(t, "decimal" | "numeric")
}

/// Port of `getColumnInfo`'s full data-type construction and the column flags
/// surfaced by the original driver.
fn get_column_info(
    row: &Value,
    column_name: &str,
    quote_defaults: bool,
) -> ColumnInfo {
    let data_type = get_str(row, "dataType").unwrap_or_default();
    let char_max_length = get_i64(row, "charMaxLength");
    let numeric_precision = get_i64(row, "numericPrecision");
    let numeric_scale = get_i64(row, "numericScale");
    let column_type = get_str(row, "columnType").unwrap_or_default();

    let mut full_type = if let Some(len) = char_max_length {
        if is_type_string(&data_type) {
            format!("{data_type}({len})")
        } else {
            data_type.clone()
        }
    } else {
        data_type.clone()
    };
    if let (Some(p), Some(s)) = (numeric_precision, numeric_scale) {
        if is_type_numeric(&data_type) {
            full_type = format!("{data_type}({p},{s})");
        }
    }
    // enum/set columns carry their options inline in column_type.
    if column_type.to_lowercase().starts_with("enum(")
        || column_type.to_lowercase().starts_with("set(")
    {
        full_type = column_type.clone();
    }

    let is_nullable = get_str(row, "isNullable");
    let extra = get_str(row, "extra").unwrap_or_default();
    let auto_increment = extra
        .to_lowercase()
        .contains("auto_increment");

    let raw_default = get_str(row, "defaultValue");
    let default_value = if quote_defaults {
        raw_default.as_deref().map(quote_default_value)
    } else {
        raw_default
    };

    let on_update_expression = extra
        .to_lowercase()
        .find("on update")
        .map(|i| extra[i + "on update".len()..].trim().to_string());

    let is_unsigned = column_type.split_whitespace().any(|t| t == "unsigned");
    let is_zerofill = column_type.split_whitespace().any(|t| t == "zerofill");

    ColumnInfo {
        column_name: column_name.to_string(),
        data_type: full_type,
        not_null: Some(
            !(is_nullable.as_deref() == Some("NO") || is_nullable.as_deref() == Some("no")),
        ),
        auto_increment: Some(auto_increment),
        default_value: if auto_increment { None } else { default_value },
        on_update_expression,
        is_unsigned: Some(is_unsigned),
        is_zerofill: Some(is_zerofill),
        column_comment: get_str(row, "columnComment"),
        ..Default::default()
    }
}

/// Port of `quoteDefaultValue`: numeric and `CURRENT_*` defaults stay bare;
/// other string defaults are single-quoted.
fn quote_default_value(value: &str) -> String {
    if value.is_empty() {
        return value.to_string();
    }
    if value.parse::<f64>().is_ok() {
        return value.to_string();
    }
    if value.starts_with("CURRENT_") {
        return value.to_string();
    }
    if value.eq_ignore_ascii_case("NULL") {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "\\'"))
}

fn build_primary_key(pure: &str, pk_rows: &[Value]) -> Option<PrimaryKeyInfo> {
    let filtered: Vec<&Value> = pk_rows
        .iter()
        .filter(|r| get_str(r, "pureName").as_deref() == Some(pure))
        .collect();
    if filtered.is_empty() {
        return None;
    }
    let constraint_name = filtered[0].get("constraintName").and_then(|v| v.as_str()).map(String::from);
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
                    column_name: get_str(r, "columnName").unwrap_or_default(),
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
        if get_str(r, "pureName").as_deref() != Some(pure) {
            continue;
        }
        let name = get_str(r, "constraintName").unwrap_or_default();
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
                            column_name: get_str(r, "columnName").unwrap_or_default(),
                            ref_column_name: get_str(r, "refColumnName"),
                            is_included_column: None,
                            is_descending: None,
                        })
                        .collect(),
                },
                ref_schema_name: None,
                ref_table_name: get_str(first, "refTableName").unwrap_or_default(),
                update_action: get_str(first, "updateAction"),
                delete_action: get_str(first, "deleteAction"),
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

    for idx in index_rows.iter().filter(|r| {
        get_str(r, "tableName").as_deref() == Some(pure)
    }) {
        let index_name = get_str(idx, "constraintName").unwrap_or_default();
        let index_type = get_str(idx, "indexType").unwrap_or_else(|| "index".to_string());
        let non_unique = idx.get("nonUnique").and_then(|v| v.as_i64()).unwrap_or(1) != 0;
        let is_descending = idx.get("isDescending").and_then(|v| v.as_bool()).unwrap_or(false);

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

        if unique_names.contains(&index_name) {
            uniques.push(UniqueInfo {
                columns_constraint: ColumnsConstraintInfo {
                    constraint,
                    columns: vec![column],
                },
            });
        } else {
            indexes.push(IndexInfo {
                columns_constraint: ColumnsConstraintInfo {
                    constraint,
                    columns: vec![column],
                },
                is_unique: !non_unique,
                index_type: Some(index_type),
                filter_definition: None,
            });
        }
    }
    (indexes, uniques)
}

fn build_parameters(pure: &str, param_rows: &[Value]) -> Vec<ParameterInfo> {
    param_rows
        .iter()
        .filter(|r| get_str(r, "pureName").as_deref() == Some(pure))
        .map(|r| {
            let mode = match get_str(r, "parameterMode").as_deref() {
                Some("OUT") => ParameterMode::Out,
                Some("INOUT") => ParameterMode::InOut,
                Some("RETURN") => ParameterMode::Return,
                _ => ParameterMode::In,
            };
            // RETURN pseudo-parameter is synthesized as a bare return value.
            let name = get_str(r, "parameterName").unwrap_or_default();
            ParameterInfo {
                parameter_name: name,
                data_type: get_str(r, "dataType").unwrap_or_default(),
                parameter_mode: Some(mode),
                position: get_i64(r, "ordinalPosition"),
            }
        })
        .collect()
}

fn parameters_sql_string(parameters: &[ParameterInfo]) -> String {
    parameters
        .iter()
        .map(|p| {
            let mode_prefix = match p.parameter_mode {
                Some(ParameterMode::Out) => "OUT ",
                Some(ParameterMode::InOut) => "INOUT ",
                Some(ParameterMode::In) => "IN ",
                _ => "",
            };
            let type_suffix = if p.data_type.is_empty() {
                String::new()
            } else {
                format!(" {}", p.data_type.to_uppercase())
            };
            format!("{mode_prefix}{}{type_suffix}", p.parameter_name)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn db_table_object(
    value: &Value,
    object_id: &str,
    content_hash: Option<&str>,
) -> DatabaseObjectInfo {
    DatabaseObjectInfo {
        pure_name: get_str(value, "pureName").unwrap_or_default(),
        schema_name: None,
        pairing_id: None,
        object_id: Some(object_id.to_string()),
        create_date: None,
        modify_date: get_str(value, "modifyDate"),
        hash_code: content_hash.map(String::from),
        object_type_field: None,
        object_comment: get_str(value, "objectComment"),
    }
}

fn analyser_query(
    handle: &DbHandle,
    template: &str,
    object_id: Option<&str>,
    database: &str,
) -> DbgmResult<QueryResult> {
    let conn = downcast(handle)?;
    let sql = substitute_condition(template, database, object_id);
    MySqlDriver::self_blocking_query(conn, &sql)
}

// ---------------------------------------------------------------------------
// analyse_full / analyse_single_table
// ---------------------------------------------------------------------------

fn analyse_myql_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let conn = downcast(handle)?;
    let database = &conn.database;

    let tables = analyser_query(handle, SQL_TABLES, None, database)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, None, database)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, None, database)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, None, database)?.rows;
    let views_rows = analyser_query(handle, SQL_VIEWS, None, database)?.rows;
    let routines_rows = analyser_query(handle, SQL_PROGRAMMABLES, None, database)?.rows;
    let param_rows = analyser_query(handle, SQL_PARAMETERS, None, database)?.rows;
    let view_text_rows = analyser_query(handle, SQL_VIEW_TEXTS, None, database)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, None, database)?.rows;
    let trigger_rows = analyser_query(handle, SQL_TRIGGERS, None, database)?.rows;
    let _event_rows = analyser_query(handle, SQL_SCHEDULER_EVENTS, None, database)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, database)?.rows;

    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraintName"))
        .collect();

    let mut result_tables = Vec::new();
    for table in &tables {
        let pure = get_str(table, "pureName").unwrap_or_default();
        let key = object_id("tables", &pure);
        let content_hash = get_str(table, "modifyDate");

        let columns: Vec<ColumnInfo> = columns_rows
            .iter()
            .filter(|c| get_str(c, "pureName").as_deref() == Some(pure.as_str()))
            .map(|c| {
                get_column_info(
                    c,
                    &get_str(c, "columnName").unwrap_or_default(),
                    true,
                )
            })
            .collect();

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
            table_row_count: get_i64(table, "tableRowCount"),
            table_engine: get_str(table, "tableEngine"),
        });
    }

    let view_text_by_name: HashMap<String, String> = view_text_rows
        .iter()
        .filter_map(|v| {
            get_str(v, "pureName")
                .map(|n| (n, get_str(v, "viewDefinition").unwrap_or_default()))
        })
        .collect();

    let mut views = Vec::new();
    for view in &views_rows {
        let pure = get_str(view, "pureName").unwrap_or_default();
        let definition = view_text_by_name.get(&pure).cloned().unwrap_or_default();
        let create_sql = format!("CREATE VIEW `{pure}` AS {definition}");
        views.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_table_object(
                    view,
                    &object_id("views", &pure),
                    get_str(view, "modifyDate").as_deref(),
                ),
                create_sql: Some(create_sql),
                requires_format: Some(true),
            },
            columns: columns_rows
                .iter()
                .filter(|c| get_str(c, "pureName").as_deref() == Some(pure.as_str()))
                .map(|c| get_column_info(c, &get_str(c, "columnName").unwrap_or_default(), true))
                .collect(),
        });
    }

    let mut procedures = Vec::new();
    let mut functions = Vec::new();
    for routine in &routines_rows {
        let pure = get_str(routine, "pureName").unwrap_or_default();
        let params = build_parameters(&pure, &param_rows);
        let is_procedure = get_str(routine, "objectType").as_deref() == Some("PROCEDURE");
        let definition = get_str(routine, "routineDefinition").unwrap_or_default();
        let (kind, create_sql) = if is_procedure {
            (
                "procedures",
                format!(
                    "DELIMITER //\n\nCREATE PROCEDURE `{pure}`({})\n{definition}\n\nDELIMITER ;\n",
                    parameters_sql_string(&params)
                ),
            )
        } else {
            let return_type = get_str(routine, "returnDataType").unwrap_or_default();
            let deterministic = if get_str(routine, "isDeterministic").as_deref() == Some("YES") {
                "DETERMINISTIC"
            } else {
                "NOT DETERMINISTIC"
            };
            let callable_params: Vec<ParameterInfo> = params
                .iter()
                .filter(|p| !matches!(p.parameter_mode, Some(ParameterMode::Return)))
                .cloned()
                .collect();
            (
                "functions",
                format!(
                    "CREATE FUNCTION `{pure}`({})\nRETURNS {return_type} {deterministic}\n{definition}",
                    parameters_sql_string(&callable_params)
                ),
            )
        };
        let callable = CallableObjectInfo {
            object: SqlObjectInfo {
                object: db_table_object(
                    routine,
                    &object_id(kind, &pure),
                    get_str(routine, "modifyDate").as_deref(),
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
                return_type: get_str(routine, "returnDataType"),
            });
        }
    }

    let mut triggers = Vec::new();
    for trg in &trigger_rows {
        let trigger_name = get_str(trg, "triggerName").unwrap_or_default();
        let table_name = get_str(trg, "tableName").unwrap_or_default();
        let trigger_timing = get_str(trg, "triggerTiming").unwrap_or_default();
        let event_type = get_str(trg, "eventType").unwrap_or_default();
        let definition = get_str(trg, "definition").unwrap_or_default();
        let create_sql = format!(
            "CREATE TRIGGER {trigger_name} {trigger_timing} {event_type} ON {table_name} FOR EACH ROW {definition}"
        );
        let object = DatabaseObjectInfo {
            pure_name: trigger_name.clone(),
            schema_name: get_str(trg, "schemaName"),
            pairing_id: None,
            object_id: Some(format!("triggers:{trigger_name}")),
            create_date: None,
            modify_date: get_str(trg, "modifyDate"),
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
            "TRUNCATE" => Some(TriggerEventType::Truncate),
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

fn analyse_myql_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let conn = downcast(handle)?;
    let database = &conn.database;
    let pure = name.pure_name.clone();
    let key = object_id("tables", &pure);
    let object_id_some = Some(key.as_str());

    let tables = analyser_query(handle, SQL_TABLES, object_id_some, database)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, object_id_some, database)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, object_id_some, database)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, object_id_some, database)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, object_id_some, database)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, database)?.rows;
    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraintName"))
        .collect();

    let table = tables
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table not found: {key}")))?;
    let pure_ref = get_str(table, "pureName").unwrap_or_default();
    let content_hash = get_str(table, "modifyDate");

    let columns: Vec<ColumnInfo> = columns_rows
        .iter()
        .filter(|c| get_str(c, "pureName").as_deref() == Some(pure_ref.as_str()))
        .map(|c| get_column_info(c, &get_str(c, "columnName").unwrap_or_default(), true))
        .collect();

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
        table_row_count: get_i64(table, "tableRowCount"),
        table_engine: get_str(table, "tableEngine"),
    })
}

// ---------------------------------------------------------------------------
// Catalog query templates (ported from plugins/dbgate-plugin-mysql/.../sql)
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = "
select
\tTABLE_NAME as pureName,
\tTABLE_ROWS as tableRowCount,
\tENGINE as tableEngine,
\tTABLE_COMMENT as objectComment,
\tDATA_LENGTH + INDEX_LENGTH as sizeBytes,
\tcase when ENGINE='InnoDB' then CREATE_TIME else coalesce(UPDATE_TIME, CREATE_TIME) end as modifyDate
from information_schema.tables
where TABLE_SCHEMA = '#DATABASE#' and TABLE_TYPE in ('BASE TABLE', 'SYSTEM VIEW', 'SYSTEM VERSIONED') and TABLE_NAME =OBJECT_ID_CONDITION;
";

const SQL_COLUMNS: &str = "
select
\tTABLE_NAME as pureName,
\tCOLUMN_NAME as columnName,
\tIS_NULLABLE as isNullable,
\tDATA_TYPE as dataType,
\tCHARACTER_MAXIMUM_LENGTH as charMaxLength,
\tNUMERIC_PRECISION as numericPrecision,
\tNUMERIC_SCALE as numericScale,
\tCOLUMN_DEFAULT as defaultValue,
\tCOLUMN_COMMENT as columnComment,
\tCOLUMN_TYPE as columnType,
\tEXTRA as extra
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = '#DATABASE#' and TABLE_NAME =OBJECT_ID_CONDITION
order by ORDINAL_POSITION
";

const SQL_PRIMARY_KEYS: &str = "
select
\tKEY_COLUMN_USAGE.CONSTRAINT_NAME as constraintName,
\tKEY_COLUMN_USAGE.TABLE_NAME as pureName,
\tKEY_COLUMN_USAGE.COLUMN_NAME as columnName
from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
where KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA = '#DATABASE#' and KEY_COLUMN_USAGE.TABLE_NAME =OBJECT_ID_CONDITION AND KEY_COLUMN_USAGE.CONSTRAINT_NAME = 'PRIMARY'
order by KEY_COLUMN_USAGE.ORDINAL_POSITION
";

const SQL_FOREIGN_KEYS: &str = "
select
\tREFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME as constraintName,
\tREFERENTIAL_CONSTRAINTS.TABLE_NAME as pureName,
\tREFERENTIAL_CONSTRAINTS.UPDATE_RULE as updateAction,
\tREFERENTIAL_CONSTRAINTS.DELETE_RULE as deleteAction,
\tREFERENTIAL_CONSTRAINTS.REFERENCED_TABLE_NAME as refTableName,
\tKEY_COLUMN_USAGE.COLUMN_NAME as columnName,
\tKEY_COLUMN_USAGE.REFERENCED_COLUMN_NAME as refColumnName
from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE
\ton REFERENTIAL_CONSTRAINTS.TABLE_NAME = KEY_COLUMN_USAGE.TABLE_NAME
\tand REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME = KEY_COLUMN_USAGE.CONSTRAINT_NAME
\tand REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA = KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA
where REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA = '#DATABASE#' and REFERENTIAL_CONSTRAINTS.TABLE_NAME =OBJECT_ID_CONDITION
order by KEY_COLUMN_USAGE.ORDINAL_POSITION
";

const SQL_INDEXES: &str = "
SELECT
\tINDEX_NAME AS constraintName,
\tTABLE_NAME AS tableName,
\tCOLUMN_NAME AS columnName,
\tINDEX_TYPE AS indexType,
\tNON_UNIQUE AS nonUnique,
\tCASE COLLATION
\t\tWHEN 'D' THEN 1
\t\tELSE 0
\tEND AS isDescending
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = '#DATABASE#' AND TABLE_NAME =OBJECT_ID_CONDITION AND INDEX_NAME != 'PRIMARY'
ORDER BY SEQ_IN_INDEX
";

const SQL_UNIQUE_NAMES: &str = "
select CONSTRAINT_NAME as constraintName
from information_schema.TABLE_CONSTRAINTS
where CONSTRAINT_SCHEMA = '#DATABASE#' and constraint_type = 'UNIQUE'
";

const SQL_VIEWS: &str = "
select
\tTABLE_NAME as pureName,
\tcoalesce(UPDATE_TIME, CREATE_TIME) as modifyDate
from information_schema.tables
where TABLE_SCHEMA = '#DATABASE#' and TABLE_NAME =OBJECT_ID_CONDITION and TABLE_TYPE = 'VIEW';
";

const SQL_VIEW_TEXTS: &str = "
select
\tTABLE_NAME as pureName,
\tVIEW_DEFINITION as viewDefinition
from information_schema.views
where TABLE_SCHEMA = '#DATABASE#' and TABLE_NAME =OBJECT_ID_CONDITION;
";

const SQL_PROGRAMMABLES: &str = "
select
\tROUTINE_NAME as pureName,
\tROUTINE_TYPE as objectType,
\tCOALESCE(LAST_ALTERED, CREATED) as modifyDate,
\tDATA_TYPE AS returnDataType,
\tROUTINE_DEFINITION as routineDefinition,
\tIS_DETERMINISTIC as isDeterministic
from information_schema.routines
where ROUTINE_SCHEMA = '#DATABASE#' and ROUTINE_NAME =OBJECT_ID_CONDITION
";

const SQL_PARAMETERS: &str = "
SELECT
\tr.ROUTINE_SCHEMA AS schemaName,
\tr.SPECIFIC_NAME AS pureName,
\tCASE
\t\tWHEN COALESCE(NULLIF(PARAMETER_MODE, ''), 'RETURN') = 'RETURN' THEN 'Return'
\t\tELSE PARAMETER_NAME
\tEND AS parameterName,
\tp.CHARACTER_MAXIMUM_LENGTH AS charMaxLength,
\tp.NUMERIC_PRECISION AS numericPrecision,
\tp.NUMERIC_SCALE AS numericScale,
\tp.DTD_IDENTIFIER AS dataType,
\tCOALESCE(NULLIF(PARAMETER_MODE, ''), 'RETURN') AS parameterMode,
\tr.ROUTINE_TYPE AS routineType,
\tp.ORDINAL_POSITION AS ordinalPosition
FROM
\tinformation_schema.PARAMETERS p
JOIN
\tinformation_schema.ROUTINES r
ON
\tp.SPECIFIC_NAME = r.SPECIFIC_NAME AND r.ROUTINE_SCHEMA = p.SPECIFIC_SCHEMA
WHERE
\tr.ROUTINE_SCHEMA = '#DATABASE#' AND r.ROUTINE_NAME =OBJECT_ID_CONDITION
ORDER BY
\tr.ROUTINE_SCHEMA, r.SPECIFIC_NAME, p.ORDINAL_POSITION
";

const SQL_TRIGGERS: &str = "
SELECT
\tTRIGGER_NAME AS triggerName,
\tEVENT_MANIPULATION AS eventType,
\tACTION_TIMING AS triggerTiming,
\tEVENT_OBJECT_SCHEMA AS schemaName,
\tEVENT_OBJECT_TABLE AS tableName,
\tACTION_STATEMENT AS definition,
\tCREATED as modifyDate
FROM
\tINFORMATION_SCHEMA.TRIGGERS
\tWHERE EVENT_OBJECT_SCHEMA = '#DATABASE#' AND TRIGGER_NAME =OBJECT_ID_CONDITION
";

const SQL_SCHEDULER_EVENTS: &str = "
SELECT
\tEVENT_SCHEMA,
\tEVENT_NAME,
\tDEFINER,
\tEVENT_TYPE,
\tEXECUTE_AT,
\tINTERVAL_VALUE,
\tINTERVAL_FIELD,
\tCREATED,
\tLAST_EXECUTED,
\tLAST_ALTERED,
\tSTARTS,
\tENDS,
\tSTATUS,
\tON_COMPLETION,
\tCONCAT('CREATE EVENT \\\\`', EVENT_NAME, '\\\\` ',
\t\tCASE WHEN EVENT_TYPE = 'RECURRING' THEN 'ON SCHEDULE EVERY ' ELSE 'ON SCHEDULE AT ' END,
\t\tCASE WHEN EVENT_TYPE = 'RECURRING' THEN CONCAT(INTERVAL_VALUE, ' ', INTERVAL_FIELD)
\t\t\tELSE DATE_FORMAT(EXECUTE_AT, '%Y-%m-%d %H:%i:%s') END,
\t\t' DO ', EVENT_DEFINITION) AS CREATE_SQL
FROM INFORMATION_SCHEMA.EVENTS
WHERE EVENT_SCHEMA = '#DATABASE#' AND EVENT_NAME =OBJECT_ID_CONDITION
";

// ---------------------------------------------------------------------------
// Unit tests (pure helpers only; no live server required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_object_conditions() {
        let sql = substitute_condition(
            "where TABLE_SCHEMA = '#DATABASE#' and TABLE_NAME =OBJECT_ID_CONDITION",
            "mydb",
            Some("users"),
        );
        assert!(sql.contains("TABLE_SCHEMA = 'mydb'"));
        assert!(sql.contains("TABLE_NAME = 'users'") || sql.contains("TABLE_NAME  = 'users'"));
        let sql_all = substitute_condition(
            "where TABLE_SCHEMA = '#DATABASE#' and TABLE_NAME =OBJECT_ID_CONDITION",
            "mydb",
            None,
        );
        assert!(sql_all.contains("TABLE_SCHEMA = 'mydb'"));
        assert!(sql_all.contains("is not null"));
    }

    #[test]
    fn builds_primary_key_from_rows() {
        let rows = vec![
            serde_json::json!({"pureName": "users", "constraintName": "PRIMARY", "columnName": "id"}),
            serde_json::json!({"pureName": "users", "constraintName": "PRIMARY", "columnName": "tenant"}),
        ];
        for r in &rows {
            let _ = r;
        }
        let pk = build_primary_key("users", &rows).unwrap();
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
        assert_eq!(pk.columns_constraint.columns[1].column_name, "tenant");
    }

    #[test]
    fn quotes_default_values() {
        assert_eq!(quote_default_value("42"), "42");
        assert_eq!(quote_default_value("CURRENT_TIMESTAMP"), "CURRENT_TIMESTAMP");
        assert_eq!(quote_default_value("hello"), "'hello'");
        assert_eq!(quote_default_value("NULL"), "NULL");
    }

    #[test]
    fn maps_binary_and_precision_types() {
        use mysql_async::Value;
        let j = value_to_json(&Value::Bytes(vec![1, 2, 3]), ColumnType::MYSQL_TYPE_BLOB);
        assert_eq!(
            j,
            serde_json::json!({"$binary": {"base64": "AQID"}})
        );
        let j = value_to_json(&Value::Int(123456789012345678), ColumnType::MYSQL_TYPE_LONGLONG);
        assert_eq!(j, serde_json::json!("123456789012345678"));
        let j = value_to_json(&Value::Int(42), ColumnType::MYSQL_TYPE_LONG);
        assert_eq!(j, serde_json::json!(42));
    }

    #[test]
    fn computes_full_data_type_names() {
        let row = serde_json::json!({
            "dataType": "varchar",
            "charMaxLength": 250,
            "columnType": "varchar(250)",
        });
        let info = get_column_info(&row, "name", true);
        assert_eq!(info.data_type, "varchar(250)");

        let row2 = serde_json::json!({
            "dataType": "decimal",
            "numericPrecision": 10,
            "numericScale": 2,
            "columnType": "decimal(10,2)",
        });
        let info2 = get_column_info(&row2, "price", true);
        assert_eq!(info2.data_type, "decimal(10,2)");
    }
}
