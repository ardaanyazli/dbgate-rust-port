//! Firebird engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-firebird/src/backend/` on top of the
//! [`rsfbclient`] crate (`pure_rust` feature — the Firebird wire protocol, so
//! no `fbclient` native library is required). The client is blocking and its
//! query/execute methods take `&mut self`, so the connection is shared behind a
//! [`Mutex`]; the `EngineDriver` trait methods are synchronous, so no per
//! connection tokio runtime is needed (unlike the async PostgreSQL/MSSQL
//! drivers).
//!
//! Firebird has no separate databases: a connection operates against a single
//! database file/alias. The analyser's `=OBJECT_ID_CONDITION` placeholder is
//! replaced with `= 'tables:NAME'` (single object) or `= is not null` (full
//! analysis). Object ids use the prefixes `tables:`, `triggers:`, `functions:`
//! and `procedures:`; views share the `tables:` prefix (distinguished by
//! `RDB$RELATION_TYPE = 1`). All catalog metadata is read from the `RDB$*`
//! system tables.
//!
//! Value mapping mirrors the `node-firebird` options the original plugin uses:
//! with `blobAsText: true` blobs are returned as text, DATE/TIME/TIMESTAMP as
//! `YYYY-MM-DD HH:MM:SS` strings, numbers as JSON numbers, and BOOLEAN as JSON
//! booleans.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Mutex;

use rsfbclient::{builder_pure_rust, Queryable, Row, SimpleConnection, SqlType};
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

/// Dotted engine id for Firebird.
pub const FIREBIRD_ENGINE: &str = "firebird@dbgate-plugin-firebird";

/// An open Firebird connection. The blocking [`SimpleConnection`] is shared
/// behind a [`Mutex`] because the crate's query/execute methods take `&mut
/// self`. `database` stores the attached database path, used to report the
/// single attached database in `list_databases`.
struct FirebirdConnection {
    conn: Mutex<SimpleConnection>,
    database: String,
}

/// The Firebird driver.
pub struct FirebirdDriver;

impl FirebirdDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FirebirdDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(FirebirdDriver::new())
}

fn downcast(handle: &DbHandle) -> DbgmResult<&FirebirdConnection> {
    handle
        .downcast_ref::<FirebirdConnection>()
        .ok_or_else(|| DbgmError::new("handle is not a Firebird connection"))
}

fn out(err: rsfbclient::FbError) -> DbgmError {
    DbgmError::new(format!("Firebird error: {err}"))
}

fn lock(conn: &FirebirdConnection) -> DbgmResult<std::sync::MutexGuard<'_, SimpleConnection>> {
    conn.conn
        .lock()
        .map_err(|_| DbgmError::new("Firebird connection lock poisoned"))
}

/// Build an `rsfbclient` connection from a definition. The Firebird plugin maps
/// its connection fields onto `node-firebird` options: `server` -> host,
/// `port`, `user`, `password`, `databaseFile` -> database path.
fn build_connection(def: &ConnectionDefinition) -> DbgmResult<SimpleConnection> {
    let server = def.server.clone().unwrap_or_else(|| "localhost".to_string());
    let port = def.port.unwrap_or(3050) as u16;
    let user = def.user.clone().unwrap_or_else(|| "SYSDBA".to_string());
    let pass = def.password.clone().unwrap_or_else(|| "masterkey".to_string());
    let database = def
        .database_file
        .clone()
        .or_else(|| def.database.clone())
        .ok_or_else(|| DbgmError::new("Firebird connection requires a database file path"))?;

    let conn = builder_pure_rust()
        .host(server)
        .port(port)
        .db_name(database)
        .user(user)
        .pass(pass)
        .connect()
        .map_err(out)?;
    Ok(conn.into())
}

impl EngineDriver for FirebirdDriver {
    fn engine(&self) -> &str {
        FIREBIRD_ENGINE
    }

    fn title(&self) -> &str {
        "Firebird"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: true,
            supports_transactions: true,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: true,
            default_port: Some(3050),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let database = def
            .database_file
            .clone()
            .or_else(|| def.database.clone())
            .unwrap_or_default();
        let conn = build_connection(def)?;
        Ok(Box::new(FirebirdConnection {
            conn: Mutex::new(conn),
            database,
        }))
    }

    fn close(&self, handle: DbHandle) -> DbgmResult<()> {
        drop(handle);
        Ok(())
    }

    fn query(&self, handle: &DbHandle, sql: &str, _options: &QueryOptions) -> DbgmResult<QueryResult> {
        let conn = downcast(handle)?;
        let mut client = lock(conn)?;
        query_rows(&mut client, sql)
    }

    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
        let conn = downcast(handle)?;
        let mut client = lock(conn)?;
        let result = query_rows(&mut client, sql)?;
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
        let mut client = lock(conn)?;
        let rows = query_rows(&mut client, SQL_VERSION)?.rows;
        let version = rows
            .first()
            .and_then(|r| r.get("VERSION").and_then(|v| v.as_str()).map(String::from))
            .unwrap_or_else(|| "unknown".to_string());
        Ok(ServerVersion {
            version: version.clone(),
            version_text: Some(format!("Firebird {version}")),
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = downcast(handle)?;
        Ok(vec![DatabaseEntry {
            name: conn.database.clone(),
            size_on_disk: None,
            empty: None,
        }])
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_firebird_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_firebird_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new("Firebird write_table streaming is not yet ported"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn query_rows(client: &mut SimpleConnection, sql: &str) -> DbgmResult<QueryResult> {
    let rows = client.query::<_, Row>(sql, ()).map_err(out)?;
    if rows.is_empty() {
        return Ok(QueryResult::empty());
    }
    let columns: Vec<QueryResultColumn> = rows[0]
        .cols
        .iter()
        .map(|c| QueryResultColumn {
            column_name: c.name.clone(),
            data_type: raw_type_to_name(c.raw_type),
            ..Default::default()
        })
        .collect();
    let values = rows.iter().map(row_to_value).collect();
    Ok(QueryResult {
        rows: values,
        columns,
    })
}

/// Map a Firebird `XLONG` SQL type code (see `ibase.h`) to a display name used
/// for query result column metadata.
fn raw_type_to_name(raw: u32) -> Option<String> {
    Some(match raw & 0xff {
        7 => "SMALLINT".to_string(),
        8 => "INTEGER".to_string(),
        9 => "BIGINT".to_string(),
        10 => "FLOAT".to_string(),
        11 | 27 => "DOUBLE PRECISION".to_string(),
        12 => "DATE".to_string(),
        13 => "TIME".to_string(),
        14 => "CHAR".to_string(),
        16 => "NUMERIC".to_string(),
        35 => "TIMESTAMP".to_string(),
        37 => "VARCHAR".to_string(),
        40 => "CSTRING".to_string(),
        261 => "BLOB".to_string(),
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Value mapping: rsfbclient SqlType -> DbGate JSON Value
// ---------------------------------------------------------------------------

fn binary_value(bytes: &[u8]) -> Value {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    Value::Object(Map::from_iter([(
        "$binary".into(),
        Value::Object(Map::from_iter([("base64".into(), Value::String(b64))])),
    )]))
}

/// Format a timestamp as `YYYY-MM-DD HH:MM:SS` (space separator), matching the
/// `transformRow` output the original driver produces for time values.
fn timestamp_to_string(ts: &chrono::NaiveDateTime) -> String {
    format!("{}", ts.format("%Y-%m-%d %H:%M:%S"))
}

/// Decode a single `SqlType` cell to a DbGate JSON [`Value`]. The plugin sets
/// `blobAsText: true`, so text blobs are decoded to strings; binary data that
/// is not valid UTF-8 falls back to a `$binary` base64 object.
fn cell_value(sql: &SqlType) -> Value {
    match sql {
        SqlType::Text(s) => Value::String(s.clone()),
        SqlType::Integer(i) => Value::Number((*i).into()),
        SqlType::Floating(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        SqlType::Timestamp(ts) => Value::String(timestamp_to_string(ts)),
        SqlType::Binary(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => binary_value(bytes),
        },
        SqlType::Boolean(b) => Value::Bool(*b),
        SqlType::Null => Value::Null,
    }
}

fn row_to_value(row: &Row) -> Value {
    let mut map = Map::new();
    for col in &row.cols {
        map.insert(col.name.clone(), cell_value(&col.value));
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Analysis helpers (ported from Analyser.js / helpers.js / sql/*.js)
// ---------------------------------------------------------------------------

fn get_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn get_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| v.as_i64())
}

fn get_bool(value: &Value, key: &str) -> bool {
    // Firebird returns 1/0 for the boolean-flag columns; the original driver
    // treats both `true` and `1` as true.
    value.get(key).map(|v| v == &Value::Bool(true) || v.as_i64() == Some(1)).unwrap_or(false)
}

fn object_id(kind: &str, pure: &str) -> String {
    format!("{kind}:{pure}")
}

/// Substitute the analyser `=OBJECT_ID_CONDITION` placeholder. Firebird
/// catalog templates spell it as `... =OBJECT_ID_CONDITION`; we replace the
/// token (leaving the leading `=`) with `'tables:NAME'` when analysing a single
/// table or `is not null` for a full analysis.
fn substitute_conditions(template: &str, object_id: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!("'tables:{id}'"),
        None => "is not null".to_string(),
    };
    template.replace("OBJECT_ID_CONDITION", &object_cond)
}

/// Port of `getDataTypeString` from `helpers.js`.
fn data_type_string(data_type_code: Option<i64>, sub_type: Option<i64>, scale: Option<i64>, length: Option<i64>, precision: Option<i64>) -> String {
    let exact = |def: &str| match sub_type {
        Some(1) => format!("numeric({}, {})", precision.unwrap_or(0), scale.map(|s| s.abs()).unwrap_or(0)),
        Some(2) => format!("decimal({}, {})", precision.unwrap_or(0), scale.map(|s| s.abs()).unwrap_or(0)),
        _ => def.to_string(),
    };
    match data_type_code {
        Some(7) => exact("smallint"),
        Some(8) => exact("integer"),
        Some(9) => "bigint".to_string(),
        Some(10) => "float".to_string(),
        Some(11) => "DOUBLE precision".to_string(),
        Some(12) => "date".to_string(),
        Some(13) => "time".to_string(),
        Some(14) => format!("char({})", length.unwrap_or(0)),
        Some(16) => exact("bigint"),
        Some(27) => "double precision".to_string(),
        Some(35) => "timestamp".to_string(),
        Some(40) => format!("cstring({})", length.unwrap_or(0)),
        Some(37) => format!("varchar({})", length.unwrap_or(0)),
        Some(261) => "blob".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

/// Port of the `getColumnInfo` builder: computes the full data type string from
/// the Firebird type code / sub-type / scale / length, and flags nullability,
/// primary-key membership and identity (auto-increment) from the `DEFAULT_SOURCE`.
fn get_column_info(row: &Value, column_name: &str) -> ColumnInfo {
    let data_type = data_type_string(
        get_i64(row, "dataTypeCode"),
        get_i64(row, "subType"),
        get_i64(row, "scale"),
        get_i64(row, "length"),
        get_i64(row, "precision"),
    );
    let raw_default = get_str(row, "defaultValue");
    let is_identity = raw_default
        .as_deref()
        .map(|d| d.to_uppercase().contains("IDENTITY"))
        .unwrap_or(false);
    let not_null = get_bool(row, "notNull");
    let default_value = raw_default.map(|d| d.replace("default ", "").replace("DEFAULT ", ""));
    ColumnInfo {
        column_name: column_name.to_string(),
        data_type,
        not_null: Some(not_null),
        auto_increment: Some(is_identity),
        default_value: if is_identity { None } else { default_value },
        ..Default::default()
    }
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
                    is_descending: get_bool_opt(r, "is_descending"),
                })
                .collect(),
        },
    })
}

fn get_bool_opt(value: &Value, key: &str) -> Option<bool> {
    Some(get_bool(value, key))
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
                            is_descending: get_bool_opt(r, "is_descending"),
                        })
                        .collect(),
                },
                ref_schema_name: None,
                ref_table_name: get_str(first, "ref_table_name").unwrap_or_default(),
                update_action: None,
                delete_action: None,
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
        .filter(|r| get_str(r, "pure_name").as_deref() == Some(pure))
    {
        let index_name = get_str(idx, "constraint_name").unwrap_or_default();
        let is_unique = get_bool(idx, "is_unique");
        let index_type = get_str(idx, "index_type");
        let is_descending = get_bool_opt(idx, "is_descending");

        let column = ColumnReference {
            column_name: get_str(idx, "column_name").unwrap_or_default(),
            ref_column_name: None,
            is_included_column: None,
            is_descending,
        };

        let constraint = ConstraintInfo {
            pairing_id: None,
            constraint_name: Some(index_name.clone()),
            constraint_type: if is_unique {
                ConstraintType::Unique
            } else {
                ConstraintType::Index
            },
        };
        let columns_constraint = ColumnsConstraintInfo {
            constraint,
            columns: vec![column],
        };

        if is_unique {
            uniques.push(UniqueInfo { columns_constraint });
        } else {
            indexes.push(IndexInfo {
                columns_constraint,
                is_unique,
                index_type,
                filter_definition: None,
            });
        }
    }
    // `unique_names` is unused in this port because a dedicated uniques query
    // is used; keep the parameter-free signature for parity with the analyser.
    let _ = unique_names;
    (indexes, uniques)
}

fn build_parameters(pure: &str, param_rows: &[Value]) -> Vec<ParameterInfo> {
    param_rows
        .iter()
        .filter(|r| get_str(r, "owning_object_name").as_deref() == Some(pure))
        .map(|r| {
            let mode = match get_str(r, "parameter_mode").as_deref() {
                Some("OUT") => ParameterMode::Out,
                Some("INOUT") => ParameterMode::InOut,
                Some("RETURN") => ParameterMode::Return,
                _ => ParameterMode::In,
            };
            ParameterInfo {
                parameter_name: get_str(r, "parameter_name").unwrap_or_default(),
                data_type: data_type_string(
                    get_i64(r, "dataTypeCode"),
                    get_i64(r, "subType"),
                    get_i64(r, "scale"),
                    get_i64(r, "length"),
                    get_i64(r, "precision"),
                ),
                parameter_mode: Some(mode),
                position: get_i64(r, "position"),
            }
        })
        .collect()
}

fn db_table_object(value: &Value, object_id: &str) -> DatabaseObjectInfo {
    DatabaseObjectInfo {
        pure_name: get_str(value, "pure_name").unwrap_or_default(),
        schema_name: None,
        pairing_id: None,
        object_id: Some(object_id.to_string()),
        create_date: None,
        modify_date: None,
        hash_code: None,
        object_type_field: Some(get_str(value, "object_type_field").unwrap_or_default()),
        object_comment: get_str(value, "object_comment"),
    }
}

fn analyser_query(
    handle: &DbHandle,
    template: &str,
    object_id: Option<&str>,
) -> DbgmResult<QueryResult> {
    let conn = downcast(handle)?;
    let mut client = lock(conn)?;
    let sql = substitute_conditions(template, object_id);
    query_rows(&mut client, &sql)
}

/// Map a Firebird trigger type code to the `(timing, event)` pair, porting the
/// `eventMap` in `helpers.js`.
fn trigger_event(trigger_type: i64) -> (Option<TriggerTiming>, Option<TriggerEventType>) {
    match trigger_type {
        1 | 17 | 25 => (Some(TriggerTiming::Before), Some(TriggerEventType::Insert)),
        2 | 18 | 26 => (Some(TriggerTiming::After), Some(TriggerEventType::Insert)),
        3 | 27 => (Some(TriggerTiming::Before), Some(TriggerEventType::Update)),
        4 | 28 => (Some(TriggerTiming::After), Some(TriggerEventType::Update)),
        5 => (Some(TriggerTiming::Before), Some(TriggerEventType::Delete)),
        6 => (Some(TriggerTiming::After), Some(TriggerEventType::Delete)),
        113 => (Some(TriggerTiming::Before), None),
        114 => (Some(TriggerTiming::After), None),
        8192 | 8193 => (Some(TriggerTiming::BeforeEvent), None),
        8194..=8196 => (Some(TriggerTiming::AfterStatement), None),
        _ => (None, None),
    }
}

// ---------------------------------------------------------------------------
// analyse_full / analyse_single_table
// ---------------------------------------------------------------------------

fn collect_columns(columns_rows: &[Value], pure: &str) -> Vec<ColumnInfo> {
    columns_rows
        .iter()
        .filter(|c| get_str(c, "pure_name").as_deref() == Some(pure))
        .map(|c| get_column_info(c, &get_str(c, "column_name").unwrap_or_default()))
        .collect()
}

fn analyse_firebird_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    // capabilities determines which function template variant to use.
    let capabilities = analyser_query(handle, SQL_CAPABILITIES, None)?.rows;
    let first = capabilities.first();
    let function_source_count = first.and_then(|r| get_i64(r, "functionSourceCount")).unwrap_or(0);
    let function_arg_source_count = first
        .and_then(|r| get_i64(r, "functionArgumentSourceCount"))
        .unwrap_or(0);
    let functions_sql = if function_source_count > 0 { SQL_FUNCTIONS } else { SQL_FUNCTIONS_LEGACY };
    let function_params_sql = if function_arg_source_count > 0 {
        SQL_FUNCTION_PARAMETERS
    } else {
        SQL_FUNCTION_PARAMETERS_LEGACY
    };

    let tables = analyser_query(handle, SQL_TABLES, None)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, None)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, None)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, None)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, None)?.rows;
    let views_rows = analyser_query(handle, SQL_VIEWS, None)?.rows;
    let procedure_rows = analyser_query(handle, SQL_PROCEDURES, None)?.rows;
    let procedure_param_rows = analyser_query(handle, SQL_PROCEDURE_PARAMETERS, None)?.rows;
    let function_rows = analyser_query(handle, functions_sql, None)?.rows;
    let function_param_rows = analyser_query(handle, function_params_sql, None)?.rows;
    let trigger_rows = analyser_query(handle, SQL_TRIGGERS, None)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUES, None)?.rows;

    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraint_name"))
        .collect();

    let mut result_tables = Vec::new();
    for table in &tables {
        let pure = get_str(table, "pure_name").unwrap_or_default();
        let key = object_id("tables", &pure);
        let columns = collect_columns(&columns_rows, &pure);
        let (indexes, uniques) = build_indexes_and_uniques(&pure, &indexes_rows, &unique_names);

        result_tables.push(TableInfo {
            object: db_table_object(table, &key),
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
        views.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_table_object(view, &object_id("tables", &pure)),
                create_sql: None,
                requires_format: None,
            },
            columns: collect_columns(&columns_rows, &pure),
        });
    }

    let mut procedures = Vec::new();
    for proc in &procedure_rows {
        let pure = get_str(proc, "pure_name").unwrap_or_default();
        let params = build_parameters(&pure, &procedure_param_rows);
        let create_sql = get_str(proc, "create_sql");
        procedures.push(ProcedureInfo {
            callable: CallableObjectInfo {
                object: SqlObjectInfo {
                    object: db_table_object(proc, &object_id("procedures", &pure)),
                    create_sql,
                    requires_format: None,
                },
                parameters: if params.is_empty() { None } else { Some(params) },
            },
        });
    }

    let mut functions = Vec::new();
    for func in &function_rows {
        let pure = get_str(func, "pure_name").unwrap_or_default();
        let params = build_parameters(&pure, &function_param_rows);
        let create_sql = get_str(func, "create_sql");
        let legacy = get_bool(func, "legacy_flag");
        functions.push(FunctionInfo {
            callable: CallableObjectInfo {
                object: SqlObjectInfo {
                    object: db_table_object(func, &object_id("functions", &pure)),
                    create_sql: if legacy { None } else { create_sql },
                    requires_format: None,
                },
                parameters: if params.is_empty() { None } else { Some(params) },
            },
            return_type: None,
        });
    }

    let mut triggers = Vec::new();
    for trg in &trigger_rows {
        let trigger_name = get_str(trg, "pure_name").unwrap_or_default();
        let table_name = get_str(trg, "table_name").unwrap_or_default();
        let trigger_type = get_i64(trg, "trigger_type").unwrap_or(0);
        let body = get_str(trg, "trigger_body_sql").unwrap_or_default();
        let (timing, event) = trigger_event(trigger_type);
        let create_sql = get_trigger_create_sql(
            &trigger_name,
            &table_name,
            &body,
            timing,
            event,
        );
        triggers.push(TriggerInfo {
            object: SqlObjectInfo {
                object: DatabaseObjectInfo {
                    pure_name: trigger_name,
                    schema_name: None,
                    pairing_id: None,
                    object_id: Some(format!("triggers:{}", get_str(trg, "pure_name").unwrap_or_default())),
                    create_date: None,
                    modify_date: None,
                    hash_code: None,
                    object_type_field: None,
                    object_comment: None,
                },
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

/// Build the `CREATE OR ALTER TRIGGER` SQL, mirroring `getTriggerCreateSql`.
fn get_trigger_create_sql(
    name: &str,
    table_name: &str,
    body: &str,
    timing: Option<TriggerTiming>,
    event: Option<TriggerEventType>,
) -> String {
    let timing_str = match timing {
        Some(TriggerTiming::Before) => "BEFORE",
        Some(TriggerTiming::After) => "AFTER",
        _ => "BEFORE",
    };
    let event_str = match event {
        Some(TriggerEventType::Insert) => "INSERT",
        Some(TriggerEventType::Update) => "UPDATE",
        Some(TriggerEventType::Delete) => "DELETE",
        _ => "INSERT OR UPDATE OR DELETE",
    };
    format!("CREATE OR ALTER TRIGGER \"{name}\" {timing_str} {event_str} ON \"{table_name}\" {body};")
}

fn analyse_firebird_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let pure = name.pure_name.clone();
    let key = object_id("tables", &pure);
    let object_id_some = Some(key.as_str());

    let tables = analyser_query(handle, SQL_TABLES, object_id_some)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, object_id_some)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, object_id_some)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, object_id_some)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, object_id_some)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUES, object_id_some)?.rows;
    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraint_name"))
        .collect();

    let table = tables
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table not found: {key}")))?;
    let pure_ref = get_str(table, "pure_name").unwrap_or_default();
    let columns = collect_columns(&columns_rows, &pure_ref);
    let (indexes, uniques) = build_indexes_and_uniques(&pure_ref, &indexes_rows, &unique_names);

    Ok(TableInfo {
        object: db_table_object(table, &key),
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
// Catalog query templates (ported from plugins/dbgate-plugin-firebird/...)
// ---------------------------------------------------------------------------

const SQL_VERSION: &str = "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') as \"VERSION\" from rdb$database";

const SQL_CAPABILITIES: &str = "
SELECT
    (
        SELECT COUNT(*)
        FROM RDB$RELATION_FIELDS
        WHERE RDB$RELATION_NAME = 'RDB$FUNCTIONS'
          AND RDB$FIELD_NAME = 'RDB$FUNCTION_SOURCE'
    ) AS \"functionSourceCount\",
    (
        SELECT COUNT(*)
        FROM RDB$RELATION_FIELDS
        WHERE RDB$RELATION_NAME = 'RDB$FUNCTION_ARGUMENTS'
          AND RDB$FIELD_NAME = 'RDB$FIELD_SOURCE'
    ) AS \"functionArgumentSourceCount\"
FROM RDB$DATABASE
";

const SQL_TABLES: &str = "
SELECT
    TRIM(RDB$RELATION_NAME) AS \"pureName\",
    RDB$DESCRIPTION AS \"objectComment\",
    RDB$FORMAT AS \"objectTypeField\"
FROM
    RDB$RELATIONS
WHERE
    RDB$SYSTEM_FLAG = 0
AND
    COALESCE(RDB$RELATION_TYPE, 0) = 0
AND
    ('tables:' || TRIM(RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\"
";

const SQL_COLUMNS: &str = "
SELECT DISTINCT
    CAST(TRIM(rf.rdb$relation_name) AS VARCHAR(255)) AS \"pureName\",
    CAST(TRIM(rf.rdb$field_name) AS VARCHAR(255)) AS \"columnName\",
    CASE rf.rdb$null_flag WHEN 1 THEN 1 ELSE 0 END AS \"notNull\",
    f.rdb$field_type AS \"dataTypeCode\",
    f.rdb$field_sub_type AS \"subType\",
    f.rdb$field_precision AS \"precision\",
    f.rdb$field_scale AS \"scale\",
    f.rdb$field_length / 4 AS \"length\",
    rf.RDB$DEFAULT_SOURCE AS \"defaultValue\"
FROM
    rdb$relation_fields rf
JOIN
    rdb$relations r ON rf.rdb$relation_name = r.rdb$relation_name
LEFT JOIN
    rdb$fields f ON rf.rdb$field_source = f.rdb$field_name
WHERE
    r.rdb$system_flag = 0
AND
    ('tables:' || CAST(TRIM(rf.rdb$relation_name) AS VARCHAR(255))) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\", rf.rdb$field_position
";

const SQL_PRIMARY_KEYS: &str = "
SELECT
    TRIM(rc.RDB$RELATION_NAME) AS \"pureName\",
    TRIM(rc.RDB$CONSTRAINT_NAME) AS \"constraintName\",
    TRIM(iseg.RDB$FIELD_NAME) AS \"columnName\",
    CAST(NULL AS VARCHAR(63)) AS \"refColumnName\",
    0 AS \"isIncludedColumn\",
    CASE COALESCE(idx.RDB$INDEX_TYPE, 0)
        WHEN 1 THEN 1
        ELSE 0
    END AS \"isDescending\"
FROM
    RDB$RELATION_CONSTRAINTS rc
JOIN
    RDB$RELATIONS rel ON rc.RDB$RELATION_NAME = rel.RDB$RELATION_NAME
JOIN
    RDB$INDICES idx ON rc.RDB$INDEX_NAME = idx.RDB$INDEX_NAME
JOIN
    RDB$INDEX_SEGMENTS iseg ON idx.RDB$INDEX_NAME = iseg.RDB$INDEX_NAME
WHERE
    rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
    AND COALESCE(rel.RDB$SYSTEM_FLAG, 0) = 0
    AND ('tables:' || TRIM(rc.RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\",
    \"constraintName\",
    iseg.RDB$FIELD_POSITION
";

const SQL_UNIQUES: &str = "
SELECT
    TRIM(rc.RDB$CONSTRAINT_NAME) AS \"constraintName\",
    TRIM('unique') AS \"constraintType\",
    TRIM(rc.RDB$RELATION_NAME) AS \"pureName\",
    TRIM(s.RDB$FIELD_NAME) AS \"columnName\",
    CASE COALESCE(i.RDB$INDEX_TYPE, 0)
        WHEN 1 THEN 1
        ELSE 0
    END AS \"isDescending\"
FROM
    RDB$RELATION_CONSTRAINTS rc
JOIN
    RDB$INDICES i ON rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME
JOIN
    RDB$INDEX_SEGMENTS s ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
WHERE
    rc.RDB$CONSTRAINT_TYPE = 'UNIQUE'
    AND COALESCE(i.RDB$SYSTEM_FLAG, 0) = 0
    AND
        ('tables:' || TRIM(rc.RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
";

const SQL_INDEXES: &str = "
SELECT
    TRIM(I.RDB$INDEX_NAME) AS \"constraintName\",
    TRIM('index') AS \"constraintType\",
    TRIM(I.RDB$RELATION_NAME) AS \"pureName\",
    CASE COALESCE(I.RDB$UNIQUE_FLAG, 0)
        WHEN 1 THEN 1
        ELSE 0
    END AS \"isUnique\",
    CASE
        WHEN I.RDB$EXPRESSION_SOURCE IS NOT NULL THEN TRIM('expression')
        ELSE TRIM('normal')
    END AS \"indexType\",
    TRIM(S.RDB$FIELD_NAME) AS \"columnName\",
    CASE COALESCE(I.RDB$INDEX_TYPE, 0)
        WHEN 1 THEN 1
        ELSE 0
    END AS \"isDescending\"
FROM
    RDB$INDICES I
JOIN
    RDB$INDEX_SEGMENTS S ON I.RDB$INDEX_NAME = S.RDB$INDEX_NAME
WHERE
    COALESCE(I.RDB$SYSTEM_FLAG, 0) = 0
    AND I.RDB$FOREIGN_KEY IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM RDB$RELATION_CONSTRAINTS rc
        WHERE rc.RDB$INDEX_NAME = I.RDB$INDEX_NAME
          AND rc.RDB$CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
    )
    AND
        ('tables:' || TRIM(I.RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
";

const SQL_FOREIGN_KEYS: &str = "
SELECT
    TRIM(rc_fk.RDB$RELATION_NAME) AS \"pureName\",
    TRIM(rc_fk.RDB$CONSTRAINT_NAME) AS \"constraintName\",
    TRIM(iseg_fk.RDB$FIELD_NAME) AS \"columnName\",
    TRIM(iseg_pk.RDB$FIELD_NAME) AS \"refColumnName\",
    TRIM(rc_pk.RDB$RELATION_NAME) AS \"refTableName\",
    0 AS \"isIncludedColumn\",
    CASE COALESCE(idx_fk.RDB$INDEX_TYPE, 0)
        WHEN 1 THEN 1
        ELSE 0
    END AS \"isDescending\"
FROM
    RDB$RELATION_CONSTRAINTS rc_fk
JOIN
    RDB$RELATIONS rel ON rc_fk.RDB$RELATION_NAME = rel.RDB$RELATION_NAME
JOIN
    RDB$INDEX_SEGMENTS iseg_fk ON rc_fk.RDB$INDEX_NAME = iseg_fk.RDB$INDEX_NAME
JOIN
    RDB$INDICES idx_fk ON rc_fk.RDB$INDEX_NAME = idx_fk.RDB$INDEX_NAME
JOIN
    RDB$REF_CONSTRAINTS refc ON rc_fk.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
JOIN
    RDB$RELATION_CONSTRAINTS rc_pk ON refc.RDB$CONST_NAME_UQ = rc_pk.RDB$CONSTRAINT_NAME
JOIN
    RDB$INDEX_SEGMENTS iseg_pk ON rc_pk.RDB$INDEX_NAME = iseg_pk.RDB$INDEX_NAME
                               AND iseg_fk.RDB$FIELD_POSITION = iseg_pk.RDB$FIELD_POSITION
WHERE
    rc_fk.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
AND
    ('tables:' || TRIM(rc_fk.RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\",
    \"constraintName\",
    iseg_fk.RDB$FIELD_POSITION
";

const SQL_VIEWS: &str = "
SELECT
    TRIM(RDB$RELATION_NAME) AS \"pureName\",
    RDB$DESCRIPTION AS \"objectComment\",
    RDB$FORMAT AS \"objectTypeField\"
FROM
    RDB$RELATIONS
WHERE
    RDB$SYSTEM_FLAG = 0
AND
    RDB$RELATION_TYPE = 1
AND
    ('tables:' || TRIM(RDB$RELATION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\"
";

const SQL_TRIGGERS: &str = "
SELECT
    TRIM(rtr.RDB$TRIGGER_NAME) as \"pureName\",
    TRIM(rtr.RDB$RELATION_NAME) as \"tableName\",
    rtr.RDB$TRIGGER_TYPE as \"triggerType\",
    CAST(rtr.RDB$TRIGGER_SOURCE AS VARCHAR(8191)) AS \"triggerBodySql\"
FROM
    RDB$TRIGGERS rtr
JOIN RDB$RELATIONS rel ON rtr.RDB$RELATION_NAME = rel.RDB$RELATION_NAME
WHERE rtr.RDB$SYSTEM_FLAG = 0
AND ('triggers:' || TRIM(rtr.RDB$TRIGGER_NAME)) =OBJECT_ID_CONDITION
ORDER BY rtr.RDB$TRIGGER_NAME
";

const SQL_PROCEDURES: &str = "
SELECT
    TRIM(P.RDB$PROCEDURE_NAME) AS \"pureName\",
    TRIM('PROCEDURE') AS \"objectTypeField\",
    TRIM(P.RDB$DESCRIPTION) AS \"objectComment\",
    CAST(SUBSTRING(P.RDB$PROCEDURE_SOURCE FROM 1 FOR 5000) AS VARCHAR(5000)) AS \"createSql\"
FROM
    RDB$PROCEDURES P
WHERE
    COALESCE(P.RDB$SYSTEM_FLAG, 0) = 0
    AND ('procedures:' || TRIM(P.RDB$PROCEDURE_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\"
";

const SQL_PROCEDURE_PARAMETERS: &str = "
SELECT
    TRIM(PP.RDB$PROCEDURE_NAME) AS \"owningObjectName\",
    TRIM(PP.RDB$PARAMETER_NAME) AS \"parameterName\",
    FFLDS.RDB$FIELD_TYPE AS \"dataTypeCode\",
    FFLDS.RDB$FIELD_SUB_TYPE AS \"subType\",
    FFLDS.rdb$field_precision AS \"precision\",
    FFLDS.rdb$field_scale AS \"scale\",
    FFLDS.rdb$field_length AS \"length\",
    CASE PP.RDB$PARAMETER_TYPE
        WHEN 0 THEN 'IN'
        WHEN 1 THEN 'OUT'
        ELSE CAST(PP.RDB$PARAMETER_TYPE AS VARCHAR(10))
    END AS \"parameterMode\",
    PP.RDB$PARAMETER_NUMBER AS \"position\",
    TRIM(PP.RDB$PARAMETER_NAME) AS \"pureName\"
FROM
    RDB$PROCEDURE_PARAMETERS PP
JOIN
    RDB$PROCEDURES P ON PP.RDB$PROCEDURE_NAME = P.RDB$PROCEDURE_NAME
JOIN
    RDB$FIELDS FFLDS ON PP.RDB$FIELD_SOURCE = FFLDS.RDB$FIELD_NAME
WHERE
    COALESCE(P.RDB$SYSTEM_FLAG, 0) = 0
ORDER BY
    \"owningObjectName\", PP.RDB$PARAMETER_TYPE, \"position\"
";

const SQL_FUNCTIONS: &str = "
SELECT
    TRIM(F.RDB$FUNCTION_NAME) AS \"pureName\",
    TRIM('FUNCTION') AS \"objectTypeField\",
    TRIM(F.RDB$DESCRIPTION) AS \"objectComment\",
    F.RDB$LEGACY_FLAG AS \"legacyFlag\",
    TRIM(F.RDB$ENTRYPOINT) AS \"entryPoint\",
    TRIM(F.RDB$MODULE_NAME) AS \"moduleName\",
    CAST(SUBSTRING(F.RDB$FUNCTION_SOURCE FROM 1 FOR 5000) AS VARCHAR(5000)) AS \"createSql\"
FROM
    RDB$FUNCTIONS F
WHERE
    COALESCE(F.RDB$SYSTEM_FLAG, 0) = 0
    AND ('functions:' || TRIM(F.RDB$FUNCTION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\"
";

const SQL_FUNCTIONS_LEGACY: &str = "
SELECT
    TRIM(F.RDB$FUNCTION_NAME) AS \"pureName\",
    TRIM('FUNCTION') AS \"objectTypeField\",
    TRIM(F.RDB$DESCRIPTION) AS \"objectComment\",
    1 AS \"legacyFlag\",
    TRIM(F.RDB$ENTRYPOINT) AS \"entryPoint\",
    TRIM(F.RDB$MODULE_NAME) AS \"moduleName\",
    CAST(NULL AS VARCHAR(5000)) AS \"createSql\"
FROM
    RDB$FUNCTIONS F
WHERE
    COALESCE(F.RDB$SYSTEM_FLAG, 0) = 0
    AND ('functions:' || TRIM(F.RDB$FUNCTION_NAME)) =OBJECT_ID_CONDITION
ORDER BY
    \"pureName\"
";

const SQL_FUNCTION_PARAMETERS: &str = "
SELECT
    TRIM(FA.RDB$FUNCTION_NAME) AS \"owningObjectName\",
    TRIM(FA.RDB$ARGUMENT_NAME) AS \"parameterName\",
    COALESCE(FFLDS.RDB$FIELD_TYPE, FA.RDB$FIELD_TYPE) AS \"dataTypeCode\",
    COALESCE(FFLDS.RDB$FIELD_SUB_TYPE, FA.RDB$FIELD_SUB_TYPE) AS \"subType\",
    COALESCE(FFLDS.RDB$FIELD_PRECISION, FA.RDB$FIELD_PRECISION) AS \"precision\",
    COALESCE(FFLDS.RDB$FIELD_SCALE, FA.RDB$FIELD_SCALE) AS \"scale\",
    COALESCE(FFLDS.RDB$FIELD_LENGTH, FA.RDB$FIELD_LENGTH) AS \"length\",
    TRIM(CASE
        WHEN FA.RDB$ARGUMENT_POSITION = F.RDB$RETURN_ARGUMENT THEN 'RETURN'
        ELSE 'IN'
    END) AS \"parameterMode\",
    FA.RDB$ARGUMENT_POSITION AS \"position\",
    TRIM(FA.RDB$FUNCTION_NAME) AS \"pureName\"
FROM
    RDB$FUNCTION_ARGUMENTS FA
JOIN
    RDB$FUNCTIONS F ON FA.RDB$FUNCTION_NAME = F.RDB$FUNCTION_NAME
LEFT JOIN
    RDB$FIELDS FFLDS ON FA.RDB$FIELD_SOURCE = FFLDS.RDB$FIELD_NAME
WHERE
    COALESCE(F.RDB$SYSTEM_FLAG, 0) = 0
ORDER BY
    \"owningObjectName\", \"position\"
";

const SQL_FUNCTION_PARAMETERS_LEGACY: &str = "
SELECT
    TRIM(FA.RDB$FUNCTION_NAME) AS \"owningObjectName\",
    CAST(NULL AS VARCHAR(63)) AS \"parameterName\",
    FA.RDB$FIELD_TYPE AS \"dataTypeCode\",
    FA.RDB$FIELD_SUB_TYPE AS \"subType\",
    FA.RDB$FIELD_PRECISION AS \"precision\",
    FA.RDB$FIELD_SCALE AS \"scale\",
    FA.RDB$FIELD_LENGTH AS \"length\",
    TRIM(CASE
        WHEN FA.RDB$ARGUMENT_POSITION = F.RDB$RETURN_ARGUMENT THEN 'RETURN'
        ELSE 'IN'
    END) AS \"parameterMode\",
    FA.RDB$ARGUMENT_POSITION AS \"position\",
    TRIM(FA.RDB$FUNCTION_NAME) AS \"pureName\"
FROM
    RDB$FUNCTION_ARGUMENTS FA
JOIN
    RDB$FUNCTIONS F ON FA.RDB$FUNCTION_NAME = F.RDB$FUNCTION_NAME
WHERE
    COALESCE(F.RDB$SYSTEM_FLAG, 0) = 0
ORDER BY
    \"owningObjectName\", \"position\"
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
            "WHERE ('tables:' || TRIM(RDB$RELATION_NAME)) =OBJECT_ID_CONDITION",
            Some("USERS"),
        );
        assert!(sql.contains("='tables:USERS'"));
        let sql_all =
            substitute_conditions("WHERE ('tables:' || TRIM(RDB$RELATION_NAME)) =OBJECT_ID_CONDITION", None);
        assert!(sql_all.contains("=is not null"));
    }

    #[test]
    fn maps_data_type_codes() {
        assert_eq!(data_type_string(Some(8), None, None, None, None), "integer");
        assert_eq!(data_type_string(Some(37), None, None, Some(100), None), "varchar(100)");
        assert_eq!(data_type_string(Some(7), Some(1), Some(-2), None, Some(8)), "numeric(8, 2)");
        assert_eq!(data_type_string(Some(35), None, None, None, None), "timestamp");
    }

    #[test]
    fn decodes_sql_types() {
        assert_eq!(cell_value(&SqlType::Text("abc".into())), Value::String("abc".into()));
        assert_eq!(cell_value(&SqlType::Integer(5)), Value::Number(5.into()));
        assert_eq!(cell_value(&SqlType::Boolean(true)), Value::Bool(true));
        assert_eq!(cell_value(&SqlType::Null), Value::Null);
        assert!(matches!(cell_value(&SqlType::Floating(1.5)), Value::Number(n) if n.as_f64() == Some(1.5)));
    }

    #[test]
    fn maps_trigger_types() {
        let (t1, e1) = trigger_event(2);
        assert!(matches!(t1, Some(TriggerTiming::After)));
        assert!(matches!(e1, Some(TriggerEventType::Insert)));
        let (t3, e3) = trigger_event(5);
        assert!(matches!(t3, Some(TriggerTiming::Before)));
        assert!(matches!(e3, Some(TriggerEventType::Delete)));
    }

    #[test]
    fn builds_primary_key_from_rows() {
        let rows = serde_json::json!([
            {"pure_name": "users", "constraint_name": "INTEG_1", "column_name": "id"},
            {"pure_name": "users", "constraint_name": "INTEG_1", "column_name": "tenant"},
        ])
        .as_array()
        .cloned()
        .unwrap_or_default();
        let pk = build_primary_key("users", &rows).unwrap();
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
        assert_eq!(pk.columns_constraint.columns[1].column_name, "tenant");
    }
}
