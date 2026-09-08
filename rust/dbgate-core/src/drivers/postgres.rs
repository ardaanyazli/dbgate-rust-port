//! PostgreSQL engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-postgres/src/backend/` on top of the
//! [`tokio_postgres`] client. Like the SQL Server driver, the synchronous
//! [`EngineDriver`] trait is bridged to tokio by giving each connection its
//! own runtime plus a `Mutex<Client>` and driving async calls with
//! `runtime.block_on(...)`.
//!
//! Value mapping mirrors the `pg` type parsers in the original driver: text
//! family types stay strings, `bytea` becomes `$binary`, and geometry /
//! geography columns (reported through the driver's OID->name map) become
//! `$binary` as well. Numeric precision is not yet preserved.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use serde_json::{Map, Value};
use tokio_postgres::{Client, Column, Error, IsolationLevel, NoTls, Row};

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

/// Dotted engine id for PostgreSQL.
pub const POSTGRES_ENGINE: &str = "postgres@dbgate-plugin-postgres";

/// An open PostgreSQL connection: the runtime driving the tokio client, the
/// client itself (behind a mutex for serialized access), and the OID -> type-name
/// map loaded at connect time (used to detect `bytea` / PostGIS geography and
/// geometry columns).
struct PostgresConnection {
    runtime: tokio::runtime::Runtime,
    client: Mutex<Client>,
    type_id_to_name: HashMap<u32, String>,
}

/// The PostgreSQL driver.
pub struct PostgresDriver;

impl PostgresDriver {
    pub fn new() -> Self {
        Self
    }

    fn downcast(handle: &DbHandle) -> DbgmResult<&PostgresConnection> {
        handle
            .downcast_ref::<PostgresConnection>()
            .ok_or_else(|| DbgmError::new("handle is not a PostgreSQL connection"))
    }

    fn out(err: tokio_postgres::Error) -> DbgmError {
        DbgmError::with_source("PostgreSQL error", err)
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(PostgresDriver::new())
}

/// Build a tokio_postgres [`Client`] configuration from a connection
/// definition. The database defaults to `postgres` when not supplied.
fn build_config(def: &ConnectionDefinition) -> DbgmResult<tokio_postgres::Config> {
    let server = def
        .server
        .clone()
        .ok_or_else(|| DbgmError::new("PostgreSQL connection requires a server host"))?;

    let mut config = tokio_postgres::Config::new();
    config.host(&server);
    config.port(def.port.unwrap_or(5432) as u16);
    if let Some(user) = &def.user {
        config.user(user);
    }
    if let Some(password) = &def.password {
        config.password(password);
    }
    let database = def
        .database
        .clone()
        .unwrap_or_else(|| "postgres".to_string());
    config.dbname(&database);
    config.application_name("DbGate");
    Ok(config)
}

// ---------------------------------------------------------------------------
// Value mapping: tokio_postgres column -> DbGate JSON Value
// ---------------------------------------------------------------------------

fn number_or_null(f: f64) -> Value {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn binary_value(bytes: &[u8]) -> Value {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    let mut obj = Map::new();
    obj.insert(
        "$binary".into(),
        Value::Object(Map::from_iter([("base64".into(), Value::String(b64))])),
    );
    Value::Object(obj)
}

/// Convert a full row to a JSON object keyed by column name.
fn row_to_value(row: &Row, type_id_to_name: &HashMap<u32, String>) -> Value {
    let mut map = Map::new();
    for (idx, column) in row.columns().iter().enumerate() {
        map.insert(column.name().to_string(), column_value(row, idx, column, type_id_to_name));
    }
    Value::Object(map)
}

/// Convert a single cell to JSON, dispatching on the column type name.
fn column_value(
    row: &Row,
    idx: usize,
    column: &Column,
    type_id_to_name: &HashMap<u32, String>,
) -> Value {
    let ty = column.type_();
    match ty.name() {
        "bool" => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| Value::from(i64::from(v)))
            .unwrap_or(Value::Null),
        "int4" => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| Value::from(i64::from(v)))
            .unwrap_or(Value::Null),
        "int8" => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(Value::from)
            .unwrap_or(Value::Null),
        "float4" => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(|v| number_or_null(f64::from(v)))
            .unwrap_or(Value::Null),
        "float8" => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(number_or_null)
            .unwrap_or(Value::Null),
        "text" | "varchar" | "bpchar" | "name" | "citext" | "char" => row
            .try_get::<_, Option<&str>>(idx)
            .ok()
            .flatten()
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        "bytea" => row
            .try_get::<_, Option<&[u8]>>(idx)
            .ok()
            .flatten()
            .map(binary_value)
            .unwrap_or(Value::Null),
        "date" => row
            .try_get::<_, Option<chrono::NaiveDate>>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or(Value::Null),
        "timestamp" => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or(Value::Null),
        "timestamptz" => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or(Value::Null),
        // PostGIS geometry/geography come back as binary; the original driver
        // converts them to WKT via wkx, which this port does not yet ship so
        // the raw bytes are exposed instead.
        oid_name
            if type_id_to_name
                .get(&ty.oid())
                .map(|n| n == "geometry" || n == "geography")
                .unwrap_or(false) =>
        {
            let _ = oid_name;
            row.try_get::<_, Option<&[u8]>>(idx)
                .ok()
                .flatten()
                .map(binary_value)
                .unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Build the DbGate column metadata for a result set's columns.
fn columns_metadata(columns: &[Column]) -> Vec<QueryResultColumn> {
    columns
        .iter()
        .map(|c| QueryResultColumn {
            column_name: c.name().to_string(),
            data_type: Some(c.type_().name().to_string()),
            ..Default::default()
        })
        .collect()
}

async fn run_query(
    client: &Client,
    sql: &str,
    type_id_to_name: &HashMap<u32, String>,
) -> Result<(Vec<QueryResultColumn>, Vec<Value>), Error> {
    let rows = client.query(sql, &[]).await?;
    let columns: Vec<QueryResultColumn> = rows
        .first()
        .map(|r| columns_metadata(r.columns()))
        .unwrap_or_default();
    let values = rows.iter().map(|r| row_to_value(r, type_id_to_name)).collect();
    Ok((columns, values))
}

// ---------------------------------------------------------------------------
// EngineDriver implementation
// ---------------------------------------------------------------------------

impl EngineDriver for PostgresDriver {
    fn engine(&self) -> &str {
        POSTGRES_ENGINE
    }

    fn title(&self) -> &str {
        "PostgreSQL"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: false,
            supports_transactions: true,
            supports_native_backup: true,
            supports_native_restore: true,
            supports_server_summary: true,
            default_port: Some(5432),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let config = build_config(def)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| DbgmError::with_source("Cannot start tokio runtime for PostgreSQL", e))?;

        let client = runtime.block_on(async move {
            let (client, connection) = config
                .connect(NoTls)
                .await
                .map_err(|e| DbgmError::with_source("Cannot connect to PostgreSQL", e))?;
            // Drive the connection task on this runtime for the client's lifetime.
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres connection closed: {e}");
                }
            });
            Ok::<_, DbgmError>(client)
        })?;

        let conn = PostgresConnection {
            runtime,
            client: Mutex::new(client),
            type_id_to_name: HashMap::new(),
        };

        // Load the OID -> type-name map (bytea / geography / geometry).
        let type_result = Self::blocking_query(&conn, "SELECT oid, typname FROM pg_type WHERE typname in ('geography', 'geometry', 'bytea')")?;
        let type_id_to_name = type_result
            .rows
            .iter()
            .filter_map(|r| {
                let oid = r.get("oid")?.as_i64()? as u32;
                let name = r.get("typname")?.as_str()?.to_string();
                Some((oid, name))
            })
            .collect();
        // Borrow the map into the connection immutably for subsequent queries
        // by observing it lives inside `conn`.
        let mut conn = conn;
        conn.type_id_to_name = type_id_to_name;

        if def.is_read_only.unwrap_or(false) {
            Self::blocking_query(&conn, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")?;
        }

        Ok(Box::new(conn))
    }

    fn close(&self, handle: DbHandle) -> DbgmResult<()> {
        drop(handle);
        Ok(())
    }

    fn query(&self, handle: &DbHandle, sql: &str, _options: &QueryOptions) -> DbgmResult<QueryResult> {
        let conn = Self::downcast(handle)?;
        let result = Self::blocking_query(conn, sql)?;
        Ok(QueryResult {
            rows: result.rows,
            columns: result.columns,
        })
    }

    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
        let conn = Self::downcast(handle)?;
        let result = Self::blocking_query(conn, sql)?;
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
        let conn = Self::downcast(handle)?;
        let result = Self::blocking_query(conn, "SELECT version()")?;
        let rows = &result.rows;
        let version = rows
            .first()
            .and_then(|r| r.get("version").and_then(|v| v.as_str()))
            .unwrap_or("unknown")
            .to_string();

        let mut version_text = None;
        if let Some(m) = version.split(' ').find(|s| s.split('.').count() >= 2) {
            let is_cockroach = version.to_lowercase().contains("cockroach");
            let is_redshift = version.to_lowercase().contains("redshift");
            let prefix = if is_cockroach {
                "CockroachDB"
            } else if is_redshift {
                "Redshift"
            } else {
                "PostgreSQL"
            };
            version_text = Some(format!("{prefix} {m}"));
        }

        Ok(ServerVersion {
            version,
            version_text,
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = Self::downcast(handle)?;
        let result = Self::blocking_query(
            conn,
            "SELECT datname AS name FROM pg_database WHERE datistemplate = false",
        )?;
        Ok(result
            .rows
            .iter()
            .filter_map(|r| r.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .map(|name| DatabaseEntry {
                name,
                size_on_disk: None,
                empty: None,
            })
            .collect())
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_postgres_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_postgres_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new("PostgreSQL write_table streaming is not yet ported"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PostgresDriver {
    fn blocking_query(conn: &PostgresConnection, sql: &str) -> DbgmResult<QueryResult> {
        let client = conn
            .client
            .lock()
            .map_err(|_| DbgmError::new("PostgreSQL connection lock poisoned"))?;
        let (columns, rows) = conn
            .runtime
            .block_on(run_query(&client, sql, &conn.type_id_to_name))
            .map_err(Self::out)?;
        Ok(QueryResult { rows, columns })
    }
}

// Don't warn that IsolationLevel / transaction helpers are unused; kept for
// parity with future transaction work.
#[allow(dead_code)]
fn _unused_isolation(_: IsolationLevel) {}

// ---------------------------------------------------------------------------
// Analysis helpers (ported from Analyser.js / sql/*.js)
// ---------------------------------------------------------------------------

fn get_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn get_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| v.as_i64())
}

fn object_id(kind: &str, schema: &str, pure: &str) -> String {
    format!("{kind}:{schema}.{pure}")
}

fn schema_pure(value: &Value) -> (String, String) {
    (
        get_str(value, "schema_name").unwrap_or_default(),
        get_str(value, "pure_name").unwrap_or_default(),
    )
}

/// Substitute the analyser `=OBJECT_ID_CONDITION` / `=SCHEMA_NAME_CONDITION`
/// placeholders. `object_id` is `None` in full-analysis mode (all objects)
/// and `schema` is `None` for the default schema-name exclusion.
fn substitute_conditions(template: &str, object_id: Option<&str>, schema: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!(" = '{id}'"),
        None => " is not null".to_string(),
    };
    let schema_cond = match schema {
        Some(s) => format!(" = '{s}'"),
        None => " not in ('pg_catalog', 'pg_toast', 'information_schema')".to_string(),
    };
    template
        .replace("=OBJECT_ID_CONDITION", &object_cond)
        .replace("=SCHEMA_NAME_CONDITION", &schema_cond)
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
        t,
        "varchar" | "char" | "bpchar" | "character" | "text" | "name" | "citext"
    )
}

fn is_type_numeric(t: &str) -> bool {
    matches!(t, "numeric" | "decimal")
}

/// Port of `getColumnInfo`'s full data-type construction.
fn full_data_type_name(
    data_type: &str,
    char_max_length: Option<i64>,
    precision: Option<i64>,
    scale: Option<i64>,
) -> String {
    let norm = normalize_type_name(data_type);
    let mut full = norm.clone();
    if let Some(len) = char_max_length {
        if is_type_string(&norm) {
            full = format!("{norm}({len})");
        }
    }
    if let (Some(p), Some(s)) = (precision, scale) {
        if is_type_numeric(&norm) {
            full = format!("{norm}({p},{s})");
        }
    }
    full
}

/// Port of `Analyser.getColumnInfo`.
fn get_column_info(
    row: &Value,
    schema: &str,
    pure: &str,
    column_name: &str,
    geometry_columns: &[Value],
    geography_columns: &[Value],
) -> ColumnInfo {
    let data_type = get_str(row, "data_type").unwrap_or_default();
    let char_max_length = get_i64(row, "char_max_length");
    let precision = get_i64(row, "numeric_precision");
    let scale = get_i64(row, "numeric_scale");

    let mut full_type = full_data_type_name(&data_type, char_max_length, precision, scale);

    let is_geom = geometry_columns.iter().any(|g| {
        get_str(g, "schema_name").as_deref() == Some(schema)
            && get_str(g, "pure_name").as_deref() == Some(pure)
            && get_str(g, "column_name").as_deref() == Some(column_name)
    });
    let is_geog = geography_columns.iter().any(|g| {
        get_str(g, "schema_name").as_deref() == Some(schema)
            && get_str(g, "pure_name").as_deref() == Some(pure)
            && get_str(g, "column_name").as_deref() == Some(column_name)
    });
    if is_geom {
        full_type = "geometry".to_string();
    } else if is_geog {
        full_type = "geography".to_string();
    }

    let default_value = get_str(row, "default_value");
    let auto_increment = default_value
        .as_deref()
        .map(|v| v.starts_with("nextval("))
        .unwrap_or(false);
    let is_nullable = get_str(row, "is_nullable");

    ColumnInfo {
        column_name: column_name.to_string(),
        data_type: full_type,
        not_null: Some(!(is_nullable.as_deref() == Some("YES") || is_nullable.as_deref() == Some("yes"))),
        auto_increment: Some(auto_increment),
        default_value: if auto_increment { None } else { default_value },
        ..Default::default()
    }
}

fn build_primary_key(
    schema: &str,
    pure: &str,
    pk_rows: &[Value],
) -> Option<PrimaryKeyInfo> {
    let filtered: Vec<&Value> = pk_rows
        .iter()
        .filter(|r| {
            get_str(r, "schema_name").as_deref() == Some(schema)
                && get_str(r, "pure_name").as_deref() == Some(pure)
        })
        .collect();
    if filtered.is_empty() {
        return None;
    }
    let constraint_name = filtered[0].get("constraint_name").and_then(|v| v.as_str()).map(String::from);
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

fn build_foreign_keys(schema: &str, pure: &str, fk_rows: &[Value]) -> Vec<ForeignKeyInfo> {
    let mut grouped: Vec<(String, Vec<&Value>)> = Vec::new();
    for r in fk_rows {
        if get_str(r, "table_schema").as_deref() != Some(schema)
            || get_str(r, "table_name").as_deref() != Some(pure)
        {
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
                ref_schema_name: get_str(first, "ref_table_schema"),
                ref_table_name: get_str(first, "ref_table_name").unwrap_or_default(),
                update_action: get_str(first, "update_action"),
                delete_action: get_str(first, "delete_action"),
            }
        })
        .collect()
}

fn build_indexes_and_uniques(
    schema: &str,
    pure: &str,
    index_rows: &[Value],
    indexcols_rows: &[Value],
    unique_names: &HashSet<String>,
) -> (Vec<IndexInfo>, Vec<UniqueInfo>) {
    let mut indexes = Vec::new();
    let mut uniques = Vec::new();

    for idx in index_rows.iter().filter(|r| {
        get_str(r, "schema_name").as_deref() == Some(schema)
            && get_str(r, "table_name").as_deref() == Some(pure)
    }) {
        let index_name = get_str(idx, "index_name").unwrap_or_default();
        let oid = get_i64(idx, "oid");
        let is_unique = idx.get("is_unique").and_then(|v| v.as_bool()).unwrap_or(false);
        let indkey: Vec<&str> = idx
            .get("indkey")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        let indoption: Vec<&str> = idx
            .get("indoption")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .split_whitespace()
            .collect();

        let columns: Vec<ColumnReference> = indkey
            .iter()
            .enumerate()
            .filter_map(|(col_index, colid)| {
                let col = indexcols_rows.iter().find(|c| {
                    get_i64(c, "oid") == oid && get_str(c, "attnum") == Some(colid.to_string())
                });
                col.map(|c| ColumnReference {
                    column_name: get_str(c, "column_name").unwrap_or_default(),
                    ref_column_name: None,
                    is_included_column: None,
                    is_descending: indoption
                        .get(col_index)
                        .and_then(|o| o.parse::<i64>().ok())
                        .map(|v| v > 0),
                })
            })
            .collect();

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
                columns_constraint: ColumnsConstraintInfo { constraint, columns },
            });
        } else {
            indexes.push(IndexInfo {
                columns_constraint: ColumnsConstraintInfo { constraint, columns },
                is_unique,
                index_type: Some("index".to_string()),
                filter_definition: None,
            });
        }
    }
    (indexes, uniques)
}

fn build_parameters(schema: &str, pure: &str, param_rows: &[Value]) -> Vec<ParameterInfo> {
    param_rows
        .iter()
        .filter(|r| {
            get_str(r, "schema_name").as_deref() == Some(schema)
                && get_str(r, "pure_name").as_deref() == Some(pure)
        })
        .map(|r| {
            let mode = match get_str(r, "parameter_mode").as_deref() {
                Some("OUT") => ParameterMode::Out,
                Some("INOUT") => ParameterMode::InOut,
                _ => ParameterMode::In,
            };
            ParameterInfo {
                parameter_name: get_str(r, "parameter_name").unwrap_or_default(),
                data_type: normalize_type_name(&get_str(r, "data_type").unwrap_or_default()),
                parameter_mode: Some(mode),
                position: get_i64(r, "parameter_index"),
            }
        })
        .collect()
}

/// Port of `getParametersSqlString` used to build procedure/function DDL.
fn parameters_sql_string(parameters: &[ParameterInfo]) -> String {
    parameters
        .iter()
        .map(|p| {
            let mode_prefix = match p.parameter_mode {
                Some(ParameterMode::Out) => "OUT ",
                Some(ParameterMode::InOut) => "INOUT ",
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

fn analyser_query(
    handle: &DbHandle,
    template: &str,
    object_id: Option<&str>,
    schema: Option<&str>,
) -> DbgmResult<QueryResult> {
    let conn = PostgresDriver::downcast(handle)?;
    let sql = substitute_conditions(template, object_id, schema);
    PostgresDriver::blocking_query(conn, &sql)
}

fn db_table_object(value: &Value, object_id: &str, content_hash: Option<&str>) -> DatabaseObjectInfo {
    DatabaseObjectInfo {
        pure_name: get_str(value, "pure_name").unwrap_or_default(),
        schema_name: get_str(value, "schema_name"),
        pairing_id: None,
        object_id: Some(object_id.to_string()),
        create_date: None,
        modify_date: None,
        hash_code: content_hash.map(String::from),
        object_type_field: None,
        object_comment: None,
    }
}

// ---------------------------------------------------------------------------
// analyse_full / analyse_single_table
// ---------------------------------------------------------------------------

fn analyse_postgres_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let tables = analyser_query(handle, SQL_TABLES, None, None)?.rows;
    let views_rows = analyser_query(handle, SQL_VIEWS, None, None)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, None, None)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, None, None)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, None, None)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, None)?.rows;
    let routines_rows = analyser_query(handle, SQL_ROUTINES, None, None)?.rows;
    let param_rows = analyser_query(handle, SQL_PROCEDURES_PARAMETERS, None, None)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, None, None)?.rows;
    let indexcols_rows = analyser_query(handle, SQL_INDEXCOLS, None, None)?.rows;
    let matviews_rows = analyser_query(handle, SQL_MATVIEWS, None, None)?.rows;
    let matview_columns_rows = analyser_query(handle, SQL_MATVIEW_COLUMNS, None, None)?.rows;
    let trigger_rows = analyser_query(handle, SQL_TRIGGERS, None, None)?.rows;

    let geometry_columns = analyser_query(handle, SQL_GEOMETRY_COLUMNS, None, None)?.rows;
    let geography_columns = analyser_query(handle, SQL_GEOGRAPHY_COLUMNS, None, None)?.rows;

    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraint_name"))
        .collect();

    let mut result_tables = Vec::new();
    for table in &tables {
        let (schema, pure) = schema_pure(table);
        let key = object_id("tables", &schema, &pure);
        let content_hash = match (
            get_str(table, "hash_code_columns"),
            get_str(table, "hash_code_constraints"),
        ) {
            (Some(c), Some(k)) => Some(format!("{c}-{k}")),
            _ => None,
        };
        let table_obj = DatabaseObjectInfo {
            pure_name: pure.clone(),
            schema_name: Some(schema.clone()),
            pairing_id: None,
            object_id: Some(key.clone()),
            create_date: None,
            modify_date: None,
            hash_code: content_hash,
            object_type_field: None,
            object_comment: None,
        };

        let columns: Vec<ColumnInfo> = columns_rows
            .iter()
            .filter(|c| {
                get_str(c, "schema_name").as_deref() == Some(schema.as_str())
                    && get_str(c, "pure_name").as_deref() == Some(pure.as_str())
            })
            .map(|c| {
                get_column_info(
                    c,
                    &schema,
                    &pure,
                    &get_str(c, "column_name").unwrap_or_default(),
                    &geometry_columns,
                    &geography_columns,
                )
            })
            .collect();

        let (indexes, uniques) =
            build_indexes_and_uniques(&schema, &pure, &indexes_rows, &indexcols_rows, &unique_names);

        result_tables.push(TableInfo {
            object: table_obj,
            columns,
            primary_key: build_primary_key(&schema, &pure, &pk_rows),
            sorting_key: None,
            foreign_keys: Some(build_foreign_keys(&schema, &pure, &fk_rows)),
            dependencies: None,
            indexes: Some(indexes),
            uniques: Some(uniques),
            checks: None,
            table_row_count: get_i64(table, "size_bytes"),
            table_engine: None,
        });
    }

    let mut views = Vec::new();
    for view in &views_rows {
        let (schema, pure) = schema_pure(view);
        let create_sql = get_str(view, "create_sql").map(|v| {
            format!("CREATE VIEW \"{schema}\".\"{pure}\"\nAS\n{v}")
        });
        views.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_table_object(view, &object_id("views", &schema, &pure), get_str(view, "hash_code").as_deref()),
                create_sql,
                requires_format: None,
            },
            columns: columns_rows
                .iter()
                .filter(|c| {
                    get_str(c, "schema_name").as_deref() == Some(schema.as_str())
                        && get_str(c, "pure_name").as_deref() == Some(pure.as_str())
                })
                .map(|c| get_column_info(c, &schema, &pure, &get_str(c, "column_name").unwrap_or_default(), &[], &[]))
                .collect(),
        });
    }

    let mut matviews = Vec::new();
    for mv in &matviews_rows {
        let (schema, pure) = schema_pure(mv);
        let create_sql = get_str(mv, "definition").map(|d| {
            format!("CREATE MATERIALIZED VIEW \"{schema}\".\"{pure}\"\nAS\n{d}")
        });
        matviews.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_table_object(mv, &object_id("matviews", &schema, &pure), get_str(mv, "hash_code").as_deref()),
                create_sql,
                requires_format: None,
            },
            columns: matview_columns_rows
                .iter()
                .filter(|c| {
                    get_str(c, "schema_name").as_deref() == Some(schema.as_str())
                        && get_str(c, "pure_name").as_deref() == Some(pure.as_str())
                })
                .map(|c| get_column_info(c, &schema, &pure, &get_str(c, "column_name").unwrap_or_default(), &[], &[]))
                .collect(),
        });
    }

    let mut procedures = Vec::new();
    let mut functions = Vec::new();
    for routine in &routines_rows {
        let (schema, pure) = schema_pure(routine);
        let params = build_parameters(&schema, &pure, &param_rows);
        let is_procedure = get_str(routine, "object_type").as_deref() == Some("PROCEDURE");
        let (kind, header) = if is_procedure {
            (
                "procedures",
                format!(
                    "CREATE PROCEDURE \"{schema}\".\"{pure}\"({}) LANGUAGE {}",
                    parameters_sql_string(&params),
                    get_str(routine, "language").unwrap_or_default()
                ),
            )
        } else {
            (
                "functions",
                format!(
                    "CREATE FUNCTION \"{schema}\".\"{pure}\"({}) RETURNS {} LANGUAGE {}",
                    parameters_sql_string(&params),
                    get_str(routine, "data_type").unwrap_or_default().to_uppercase(),
                    get_str(routine, "language").unwrap_or_default()
                ),
            )
        };
        let definition = get_str(routine, "definition").unwrap_or_default();
        let create_sql = format!("{header}\nAS\n$$\n{definition}\n$$");
        let callable = CallableObjectInfo {
            object: SqlObjectInfo {
                object: db_table_object(
                    routine,
                    &object_id(kind, &schema, &pure),
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
                return_type: get_str(routine, "data_type"),
            });
        }
    }

    let mut triggers = Vec::new();
    for trg in &trigger_rows {
        let trigger_id = get_i64(trg, "trigger_id").unwrap_or_default();
        let object = DatabaseObjectInfo {
            pure_name: get_str(trg, "trigger_name").unwrap_or_default(),
            schema_name: get_str(trg, "schema_name"),
            pairing_id: None,
            object_id: Some(format!("triggers:{trigger_id}")),
            create_date: None,
            modify_date: None,
            hash_code: Some(format!("triggers:{trigger_id}")),
            object_type_field: None,
            object_comment: None,
        };
        let timing = match get_str(trg, "trigger_timing").as_deref() {
            Some("AFTER") => Some(TriggerTiming::After),
            Some("INSTEAD OF") => Some(TriggerTiming::InsteadOf),
            _ => Some(TriggerTiming::Before),
        };
        let event = match get_str(trg, "event_type").as_deref() {
            Some(e) if e.contains("INSERT") => Some(TriggerEventType::Insert),
            Some(e) if e.contains("UPDATE") => Some(TriggerEventType::Update),
            Some(e) if e.contains("DELETE") => Some(TriggerEventType::Delete),
            Some(e) if e.contains("TRUNCATE") => Some(TriggerEventType::Truncate),
            _ => None,
        };
        triggers.push(TriggerInfo {
            object: SqlObjectInfo {
                object,
                create_sql: get_str(trg, "definition"),
                requires_format: None,
            },
            function_name: get_str(trg, "function_name"),
            table_name: get_str(trg, "table_name"),
            trigger_timing: timing,
            event_type: event,
        });
    }

    Ok(DatabaseInfo {
        tables: result_tables,
        views,
        matviews: Some(matviews),
        procedures,
        functions,
        triggers,
        ..Default::default()
    })
}

fn analyse_postgres_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let schema = name
        .schema_name
        .clone()
        .unwrap_or_else(|| "public".to_string());
    let pure = name.pure_name.clone();
    let key = object_id("tables", &schema, &pure);
    let object_id_some = Some(key.as_str());
    let schema_some = Some(schema.as_str());

    let tables = analyser_query(handle, SQL_TABLES, object_id_some, schema_some)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, object_id_some, schema_some)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, object_id_some, schema_some)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, object_id_some, schema_some)?.rows;
    let unique_rows = analyser_query(handle, SQL_UNIQUE_NAMES, None, schema_some)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, object_id_some, schema_some)?.rows;
    let indexcols_rows = analyser_query(handle, SQL_INDEXCOLS, object_id_some, schema_some)?.rows;
    let geometry_columns = analyser_query(handle, SQL_GEOMETRY_COLUMNS, object_id_some, schema_some)?.rows;
    let geography_columns = analyser_query(handle, SQL_GEOGRAPHY_COLUMNS, object_id_some, schema_some)?.rows;
    let unique_names: HashSet<String> = unique_rows
        .iter()
        .filter_map(|r| get_str(r, "constraint_name"))
        .collect();

    let table = tables
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table not found: {key}")))?;
    let (schema_ref, pure_ref) = schema_pure(table);

    let content_hash = match (
        get_str(table, "hash_code_columns"),
        get_str(table, "hash_code_constraints"),
    ) {
        (Some(c), Some(k)) => Some(format!("{c}-{k}")),
        _ => None,
    };

    let columns: Vec<ColumnInfo> = columns_rows
        .iter()
        .filter(|c| {
            get_str(c, "schema_name").as_deref() == Some(schema_ref.as_str())
                && get_str(c, "pure_name").as_deref() == Some(pure_ref.as_str())
        })
        .map(|c| {
            get_column_info(
                c,
                &schema_ref,
                &pure_ref,
                &get_str(c, "column_name").unwrap_or_default(),
                &geometry_columns,
                &geography_columns,
            )
        })
        .collect();

    let (indexes, uniques) =
        build_indexes_and_uniques(&schema_ref, &pure_ref, &indexes_rows, &indexcols_rows, &unique_names);

    Ok(TableInfo {
        object: db_table_object(table, &key, content_hash.as_deref()),
        columns,
        primary_key: build_primary_key(&schema_ref, &pure_ref, &pk_rows),
        sorting_key: None,
        foreign_keys: Some(build_foreign_keys(&schema_ref, &pure_ref, &fk_rows)),
        dependencies: None,
        indexes: Some(indexes),
        uniques: Some(uniques),
        checks: None,
        table_row_count: get_i64(table, "size_bytes"),
        table_engine: None,
    })
}

// ---------------------------------------------------------------------------
// Catalog query templates (ported from plugins/dbgate-plugin-postgres/.../sql)
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = "
SELECT
    n.nspname AS \"schema_name\",
    c.relname AS \"pure_name\",
    pg_relation_size(c.oid) AS \"size_bytes\",
    MD5(
        COALESCE(
            (SELECT string_agg(
                a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull::text
                , ',' ORDER BY a.attnum
            )
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
            ''
        )
    ) AS \"hash_code_columns\",
    MD5(
        COALESCE(
            (SELECT string_agg(
                con.conname || ':' || con.contype::text
                , ',' ORDER BY con.conname
            )
            FROM pg_catalog.pg_constraint con
            WHERE con.conrelid = c.oid),
            ''
        )
    ) AS \"hash_code_constraints\"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'f')
    AND ('tables:' || n.nspname || '.' || c.relname) =OBJECT_ID_CONDITION
    AND n.nspname <> 'pg_internal'
    AND n.nspname !~ '^_timescaledb_'
    AND n.nspname =SCHEMA_NAME_CONDITION
";

const SQL_COLUMNS: &str = "
SELECT
    n.nspname AS \"schema_name\",
    c.relname AS \"pure_name\",
    c.oid AS \"postgres_table_id\",
    a.attname AS \"column_name\",
    a.attnum AS \"postgres_column_id\",
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS \"is_nullable\",
    format_type(a.atttypid, NULL) AS \"data_type\",
    CASE
        WHEN a.atttypmod > 0 AND t.typname IN ('varchar', 'bpchar', 'char') THEN a.atttypmod - 4
        WHEN a.atttypmod > 0 AND t.typname IN ('bit', 'varbit') THEN a.atttypmod
        ELSE NULL
    END AS \"char_max_length\",
    CASE
        WHEN a.atttypmod > 0 AND t.typname = 'numeric' THEN ((a.atttypmod - 4) >> 16) & 65535
        ELSE NULL
    END AS \"numeric_precision\",
    CASE
        WHEN a.atttypmod > 0 AND t.typname = 'numeric' THEN (a.atttypmod - 4) & 65535
        ELSE NULL
    END AS \"numeric_scale\",
    pg_get_expr(d.adbin, d.adrelid) AS \"default_value\"
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attnum > 0
    AND NOT a.attisdropped
    AND c.relkind IN ('r', 'v', 'p', 'f')
    AND n.nspname !~ '^_timescaledb_'
    AND (
        ('tables:' || n.nspname || '.' || c.relname) =OBJECT_ID_CONDITION
        OR
        ('views:' || n.nspname || '.' || c.relname) =OBJECT_ID_CONDITION
    )
    AND n.nspname =SCHEMA_NAME_CONDITION
ORDER BY a.attnum
";

const SQL_PRIMARY_KEYS: &str = "
SELECT
    n.nspname        AS \"constraint_schema\",
    c.conname        AS \"constraint_name\",
    n.nspname        AS \"schema_name\",
    t.relname        AS \"pure_name\",
    a.attname        AS \"column_name\"
FROM pg_catalog.pg_constraint AS c
JOIN pg_catalog.pg_class AS t
  ON t.oid = c.conrelid
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = t.relnamespace
JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ordinal_position)
  ON TRUE
JOIN pg_catalog.pg_attribute AS a
  ON a.attrelid = t.oid
 AND a.attnum   = cols.attnum
WHERE
    c.contype = 'p'
    AND n.nspname !~ '^_timescaledb_'
    AND ('tables:' || n.nspname || '.' || t.relname) =OBJECT_ID_CONDITION
    AND n.nspname =SCHEMA_NAME_CONDITION
ORDER BY cols.ordinal_position
";

const SQL_FOREIGN_KEYS: &str = "
SELECT
    nsp.nspname AS \"table_schema\",
    rel.relname AS \"table_name\",
    con.conname AS \"constraint_name\",
    nsp2.nspname AS \"ref_table_schema\",
    rel2.relname AS \"ref_table_name\",
    att.attname AS \"column_name\",
    att2.attname AS \"ref_column_name\",
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
        ELSE con.confupdtype::text
    END AS \"update_action\",
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
        ELSE con.confdeltype::text
    END AS \"delete_action\"
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
JOIN pg_class rel2 ON rel2.oid = con.confrelid
JOIN pg_namespace nsp2 ON nsp2.oid = rel2.relnamespace
JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS cols(attnum, ref_attnum, ordinal_position) ON TRUE
JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = cols.attnum
JOIN pg_attribute att2 ON att2.attrelid = con.confrelid AND att2.attnum = cols.ref_attnum
WHERE con.contype = 'f'
    AND ('tables:' || nsp.nspname || '.' || rel.relname) =OBJECT_ID_CONDITION
    AND nsp.nspname =SCHEMA_NAME_CONDITION
ORDER BY con.conname, cols.ordinal_position
";

const SQL_UNIQUE_NAMES: &str = "
    select cnt.conname as \"constraint_name\" from pg_constraint cnt
    inner join pg_namespace c on c.oid = cnt.connamespace
     where cnt.contype = 'u' and c.nspname =SCHEMA_NAME_CONDITION
";

const SQL_INDEXES: &str = "
    select
        t.relname as \"table_name\",
        c.nspname as \"schema_name\",
        i.relname as \"index_name\",
        ix.indisprimary as \"is_primary\",
        ix.indisunique as \"is_unique\",
        ix.indkey as \"indkey\",
        ix.indoption as \"indoption\",
        t.oid as \"oid\"
    from
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_namespace c
    where
        t.oid = ix.indrelid
        and i.oid = ix.indexrelid
        and t.relkind = 'r'
        and ix.indisprimary = false
        and t.relnamespace = c.oid
        and c.nspname != 'pg_catalog'
        and ('tables:' || c.nspname || '.' ||  t.relname) =OBJECT_ID_CONDITION
        and c.nspname =SCHEMA_NAME_CONDITION
    order by
        t.relname
";

const SQL_INDEXCOLS: &str = "
    select
        a.attname as \"column_name\",
        a.attnum as \"attnum\",
        a.attrelid as \"oid\"
    from
        pg_class t,
        pg_class i,
        pg_attribute a,
        pg_index ix,
        pg_namespace c
    where
        t.oid = ix.indrelid
        and a.attnum = ANY(ix.indkey)
        and a.attrelid = t.oid
        and i.oid = ix.indexrelid
        and t.relkind = 'r'
        and ix.indisprimary = false
        and t.relnamespace = c.oid
        and c.nspname != 'pg_catalog'
        and ('tables:' || c.nspname || '.' ||  t.relname) =OBJECT_ID_CONDITION
        and c.nspname =SCHEMA_NAME_CONDITION
    order by
        t.relname
";

const SQL_VIEWS: &str = "
WITH view_defs AS (
  SELECT
    c.relname AS pure_name,
    n.nspname AS schema_name,
    pg_get_viewdef(c.oid, true) AS viewdef
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'v'
    AND n.nspname !~ '^_timescaledb_'
    AND n.nspname =SCHEMA_NAME_CONDITION
    AND ('views:' || n.nspname || '.' || c.relname) =OBJECT_ID_CONDITION
)
SELECT
  pure_name AS \"pure_name\",
  schema_name AS \"schema_name\",
  viewdef AS \"create_sql\",
  MD5(viewdef) AS \"hash_code\"
FROM view_defs
";

const SQL_MATVIEWS: &str = "
select
  matviewname as \"pure_name\",
  schemaname as \"schema_name\",
  definition as \"definition\",
  MD5(definition) as \"hash_code\"
from
  pg_catalog.pg_matviews WHERE schemaname NOT LIKE 'pg_%'
  and ('matviews:' || schemaname || '.' ||  matviewname) =OBJECT_ID_CONDITION
  and schemaname =SCHEMA_NAME_CONDITION
";

const SQL_MATVIEW_COLUMNS: &str = "
SELECT pg_namespace.nspname AS \"schema_name\"
    , pg_class.relname AS \"pure_name\"
    , pg_class.oid AS \"postgres_table_id\"
    , pg_attribute.attname AS \"column_name\"
    , pg_attribute.attnum AS \"postgres_column_id\"
    , pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS \"data_type\"
FROM pg_catalog.pg_class
    INNER JOIN pg_catalog.pg_namespace
        ON pg_class.relnamespace = pg_namespace.oid
    INNER JOIN pg_catalog.pg_attribute
        ON pg_class.oid = pg_attribute.attrelid
WHERE pg_class.relkind = 'm'
    AND pg_attribute.attnum >= 1
    AND ('matviews:' || pg_namespace.nspname || '.' || pg_class.relname) =OBJECT_ID_CONDITION
    AND pg_namespace.nspname =SCHEMA_NAME_CONDITION
ORDER BY pg_attribute.attnum
";

const SQL_ROUTINES: &str = "
SELECT
    p.proname AS \"pure_name\",
    n.nspname AS \"schema_name\",
    max(p.prosrc) AS \"definition\",
    max(MD5(p.prosrc)) AS \"hash_code\",
    CASE max(p.prokind) WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS \"object_type\",
    string_agg(pg_catalog.format_type(p.prorettype, NULL), '|') AS \"data_type\",
    max(l.lanname) AS \"language\"
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE p.prokind IN ('f', 'p')
    AND n.nspname !~ '^_timescaledb_'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname =SCHEMA_NAME_CONDITION
    AND (
        (p.prokind = 'p' AND ('procedures:' || n.nspname || '.' || p.proname) =OBJECT_ID_CONDITION)
        OR
        (p.prokind != 'p' AND ('functions:' || n.nspname || '.' || p.proname) =OBJECT_ID_CONDITION)
    )
GROUP BY p.proname, n.nspname, p.prokind
";

const SQL_PROCEDURES_PARAMETERS: &str = "
SELECT
    n.nspname AS \"schema_name\",
    p.proname AS \"pure_name\",
    CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS \"routine_type\",
    a.parameter_name AS \"parameter_name\",
    CASE (p.proargmodes::text[])[a.ordinal_position]
        WHEN 'o' THEN 'OUT'
        WHEN 'b' THEN 'INOUT'
        WHEN 'v' THEN 'VARIADIC'
        WHEN 't' THEN 'TABLE'
        ELSE 'IN'
    END AS \"parameter_mode\",
    pg_catalog.format_type(a.parameter_type, NULL) AS \"data_type\",
    a.ordinal_position AS \"parameter_index\"
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
CROSS JOIN LATERAL unnest(
    COALESCE(p.proallargtypes, p.proargtypes::oid[]),
    p.proargnames
) WITH ORDINALITY AS a(parameter_type, parameter_name, ordinal_position)
WHERE p.prokind IN ('f', 'p')
    AND p.proargnames IS NOT NULL
    AND a.parameter_name IS NOT NULL
    AND n.nspname !~ '^_timescaledb_'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname =SCHEMA_NAME_CONDITION
    AND (
        (p.prokind = 'p' AND ('procedures:' || n.nspname || '.' || p.proname) =OBJECT_ID_CONDITION)
        OR
        (p.prokind != 'p' AND ('functions:' || n.nspname || '.' || p.proname) =OBJECT_ID_CONDITION)
    )
ORDER BY n.nspname, a.ordinal_position
";

const SQL_TRIGGERS: &str = "
SELECT
    t.oid AS \"trigger_id\",
    t.tgname AS \"trigger_name\",
    n.nspname AS \"schema_name\",
    c.relname AS \"table_name\",
    p.proname AS \"function_name\",
    t.tgtype AS \"original_tgtype\",
    CASE
        WHEN t.tgtype & 1 = 1 THEN 'ROW'
        ELSE 'STATEMENT'
    END AS \"trigger_level\",
    COALESCE(
    CASE WHEN (tgtype::int::bit(7) & b'0000010')::int = 0 THEN NULL ELSE 'BEFORE' END,
    CASE WHEN (tgtype::int::bit(7) & b'0000010')::int = 0 THEN 'AFTER' ELSE NULL END,
    CASE WHEN (tgtype::int::bit(7) & b'1000000')::int = 0 THEN NULL ELSE 'INSTEAD OF' END,
    ''
  )::text as \"trigger_timing\",
    (CASE WHEN (tgtype::int::bit(7) & b'0000100')::int = 0 THEN '' ELSE 'INSERT' END) ||
    (CASE WHEN (tgtype::int::bit(7) & b'0001000')::int = 0 THEN '' ELSE 'DELETE' END) ||
    (CASE WHEN (tgtype::int::bit(7) & b'0010000')::int = 0 THEN '' ELSE 'UPDATE' END) ||
    (CASE WHEN (tgtype::int::bit(7) & b'0100000')::int = 0 THEN '' ELSE 'TRUNCATE' END)
  as \"event_type\",
    pg_get_triggerdef(t.oid) AS \"definition\"
FROM
    pg_trigger t
JOIN
    pg_class c ON c.oid = t.tgrelid
JOIN
    pg_namespace n ON n.oid = c.relnamespace
JOIN
    pg_proc p ON p.oid = t.tgfoid
WHERE
    NOT t.tgisinternal AND n.nspname =SCHEMA_NAME_CONDITION
";

const SQL_GEOMETRY_COLUMNS: &str = "
select
	f_table_schema as \"schema_name\",
	f_table_name as \"pure_name\",
	f_geometry_column as \"column_name\"
from public.geometry_columns
where ('tables:' || f_table_schema || '.' || f_table_name) =OBJECT_ID_CONDITION and f_table_schema =SCHEMA_NAME_CONDITION
";

const SQL_GEOGRAPHY_COLUMNS: &str = "
select
	f_table_schema as \"schema_name\",
	f_table_name as \"pure_name\",
	f_geography_column as \"column_name\"
from public.geography_columns
where ('tables:' || f_table_schema || '.' || f_table_name) =OBJECT_ID_CONDITION and f_table_schema =SCHEMA_NAME_CONDITION
";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_postgres_type_names() {
        assert_eq!(normalize_type_name("character varying"), "varchar");
        assert_eq!(normalize_type_name("timestamp without time zone"), "timestamp");
        assert_eq!(normalize_type_name("integer"), "integer");
    }

    #[test]
    fn computes_full_data_type_names() {
        let s = |t: &str, len, p, sc| full_data_type_name(t, len, p, sc);
        assert_eq!(s("varchar", Some(250), None, None), "varchar(250)");
        assert_eq!(s("numeric", None, Some(10), Some(2)), "numeric(10,2)");
        assert_eq!(s("timestamp", None, None, None), "timestamp");
    }

    #[test]
    fn substitutes_object_and_schema_conditions() {
        let sql = "AND c.relname =OBJECT_ID_CONDITION AND n.nspname =SCHEMA_NAME_CONDITION";
        let full = substitute_conditions(sql, None, None);
        assert!(full.contains("is not null"));
        assert!(full.contains("not in ('pg_catalog', 'pg_toast', 'information_schema')"));

        let single = substitute_conditions(sql, Some("tables:public.customer"), Some("public"));
        assert!(single.contains("= 'tables:public.customer'"));
        assert!(single.contains("= 'public'"));
    }

    #[test]
    fn builds_parameters_sql_string() {
        let params = vec![
            ParameterInfo {
                parameter_name: "id".to_string(),
                data_type: "int4".to_string(),
                parameter_mode: Some(ParameterMode::In),
                position: Some(1),
            },
            ParameterInfo {
                parameter_name: "name".to_string(),
                data_type: "varchar".to_string(),
                parameter_mode: None,
                position: Some(2),
            },
        ];
        assert_eq!(parameters_sql_string(&params), "id INT4, name VARCHAR");
    }

    #[test]
    fn detects_geometry_and_geography_columns() {
        let row = Value::Object(Map::from_iter([
            ("schema_name".into(), Value::String("public".into())),
            ("pure_name".into(), Value::String("places".into())),
            ("column_name".into(), Value::String("geom".into())),
            ("data_type".into(), Value::String("bytea".into())),
            ("is_nullable".into(), Value::String("YES".into())),
        ]));
        let geom = vec![Value::Object(Map::from_iter([
            ("schema_name".into(), Value::String("public".into())),
            ("pure_name".into(), Value::String("places".into())),
            ("column_name".into(), Value::String("geom".into())),
        ]))];
        let info = get_column_info(&row, "public", "places", "geom", &geom, &[]);
        assert_eq!(info.data_type, "geometry");
    }
}
