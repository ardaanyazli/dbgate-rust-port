//! Cassandra engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-cassandra/src/backend/` on top of the
//! [`scylla`] crate (CQL native protocol). The async [`Session`] is bridged to
//! the synchronous [`EngineDriver`] trait by giving each connection its own
//! tokio runtime and driving async calls with `runtime.block_on(...)`.
//!
//! Value mapping mirrors the Node `cassandra-driver` behaviour: numbers as
//! JSON numbers, booleans as JSON booleans, timestamps/dates as ISO-8601
//! strings, blobs as base64, collections as JSON arrays/objects.
//!
//! Cassandra has no traditional SQL transactions, stored procedures, or
//! foreign keys. The analyser reads `system_schema.tables` and
//! `system_schema.columns` to build the structural metadata.

use std::any::Any;
use std::sync::Mutex;

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::response::result::{CollectionType, ColumnType, NativeType};
use scylla::value::{CqlValue, Row};
use serde_json::{Map, Value};

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{
    ColumnInfo, ColumnReference, ColumnsConstraintInfo, ConstraintInfo, ConstraintType,
    DatabaseInfo, DatabaseObjectInfo, NamedObjectInfo, PrimaryKeyInfo, TableInfo,
};
use crate::driver::{
    Capabilities, DatabaseEntry, DbHandle, EngineDriver, QueryOptions, ServerVersion, StreamSink,
    StreamSeverity, WriteTableOptions,
};
use crate::error::{DbgmError, DbgmResult};
use crate::query::{QueryResult, QueryResultColumn};

/// Dotted engine id for Cassandra.
pub const CASSANDRA_ENGINE: &str = "cassandra@dbgate-plugin-cassandra";

/// An open Cassandra connection: the tokio runtime driving the scylla session,
/// the session itself (behind a mutex for serialized access), and the
/// connected keyspace name (for `list_databases` reporting).
struct CassandraConnection {
    runtime: tokio::runtime::Runtime,
    session: Mutex<Session>,
    keyspace: String,
}

/// The Cassandra driver.
pub struct CassandraDriver;

impl CassandraDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CassandraDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(CassandraDriver::new())
}

fn downcast(handle: &DbHandle) -> DbgmResult<&CassandraConnection> {
    handle
        .downcast_ref::<CassandraConnection>()
        .ok_or_else(|| DbgmError::new("handle is not a Cassandra connection"))
}

fn out(e: impl std::fmt::Display) -> DbgmError {
    DbgmError::new(e.to_string())
}

/// Parse the `server` field from a [`ConnectionDefinition`] into a list of
/// contact-point host:port strings. The Node driver does
/// `server.split(',')` and trims each entry; we also strip a `cassandra://`
/// scheme prefix and append `:port` when the entry has no port.
fn parse_contact_points(def: &ConnectionDefinition) -> DbgmResult<Vec<String>> {
    let raw = def
        .server
        .clone()
        .ok_or_else(|| DbgmError::new("Cassandra connection requires a server host"))?;
    let port = def.port.unwrap_or(9042);
    let points: Vec<String> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            let host = s.strip_prefix("cassandra://").unwrap_or(s);
            if host.contains(':') {
                host.to_string()
            } else {
                format!("{host}:{port}")
            }
        })
        .collect();
    if points.is_empty() {
        return Err(DbgmError::new(
            "Cassandra connection requires at least one contact point",
        ));
    }
    Ok(points)
}

/// Substitute `#DATABASE#` in a CQL template string with the given keyspace.
/// Rejects keyspace names containing a single quote to prevent CQL injection.
fn hydrate_keyspace_query(template: &str, keyspace: &str) -> DbgmResult<String> {
    if keyspace.contains('\'') {
        return Err(DbgmError::new(
            "Cassandra keyspace name contains invalid character",
        ));
    }
    Ok(template.replace("#DATABASE#", keyspace))
}

// ---------------------------------------------------------------------------
// Blocking query helpers
// ---------------------------------------------------------------------------

async fn execute_query(
    session: &Session,
    sql: &str,
) -> Result<(Vec<QueryResultColumn>, Vec<Value>), DbgmError> {
    let result = session
        .query_unpaged(sql, ())
        .await
        .map_err(out)?;

    let rows_result = match result.into_rows_result() {
        Ok(rr) => rr,
        Err(scylla::response::query_result::IntoRowsResultError::ResultNotRows(_)) => {
            return Ok((Vec::new(), Vec::new()));
        }
        Err(scylla::response::query_result::IntoRowsResultError::ResultMetadataLazyDeserializationError(err)) => {
            return Err(DbgmError::new(format!("{err:?}")));
        }
    };

    let column_specs = rows_result.column_specs();
    let columns: Vec<QueryResultColumn> = column_specs
        .iter()
        .map(|spec| {
            let type_name = column_type_string(spec.typ());
            QueryResultColumn {
                column_name: spec.name().to_string(),
                data_type: Some(type_name),
                ..Default::default()
            }
        })
        .collect();

    let row_iter = rows_result.rows::<Row>().map_err(out)?;
    let mut values = Vec::new();
    for row in row_iter {
        let val = row_to_value(&row.map_err(out)?, &columns);
        values.push(val);
    }
    Ok((columns, values))
}

fn blocking_query(conn: &CassandraConnection, sql: &str) -> DbgmResult<QueryResult> {
    let session = conn
        .session
        .lock()
        .map_err(|_| DbgmError::new("Cassandra connection lock poisoned"))?;
    let (columns, rows) = conn
        .runtime
        .block_on(execute_query(&session, sql))?;
    Ok(QueryResult { rows, columns })
}

// ---------------------------------------------------------------------------
// Value mapping: scylla CqlValue -> DbGate JSON Value
// ---------------------------------------------------------------------------

fn cql_value_to_json(val: &CqlValue) -> Value {
    match val {
        CqlValue::Boolean(b) => Value::Bool(*b),
        CqlValue::Int(i) => Value::Number((*i).into()),
        CqlValue::BigInt(i) => Value::Number((*i).into()),
        CqlValue::SmallInt(i) => Value::Number((*i).into()),
        CqlValue::TinyInt(i) => Value::Number((*i).into()),
        CqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        CqlValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        CqlValue::Text(s) | CqlValue::Ascii(s) => Value::String(s.clone()),
        CqlValue::Timestamp(dt) => {
            let chrono_dt: Result<DateTime<Utc>, _> = (*dt).try_into();
            match chrono_dt {
                Ok(d) => Value::String(d.to_rfc3339()),
                Err(_) => Value::String(val.to_string()),
            }
        }
        CqlValue::Date(d) => {
            let naive: Result<NaiveDate, _> = (*d).try_into();
            match naive {
                Ok(d) => Value::String(d.to_string()),
                Err(_) => Value::String(val.to_string()),
            }
        }
        CqlValue::Time(t) => {
            let naive: Result<NaiveTime, _> = (*t).try_into();
            match naive {
                Ok(t) => Value::String(t.format("%H:%M:%S%.f").to_string()),
                Err(_) => Value::String(val.to_string()),
            }
        }
        CqlValue::Inet(addr) => Value::String(addr.to_string()),
        CqlValue::Uuid(uuid) => Value::String(uuid.to_string()),
        CqlValue::Timeuuid(uuid) => Value::String(uuid.to_string()),
        CqlValue::Counter(c) => Value::Number(c.0.into()),
        CqlValue::List(items) | CqlValue::Set(items) => {
            Value::Array(items.iter().map(cql_value_to_json).collect())
        }
        CqlValue::Map(pairs) => {
            let obj: Map<String, Value> = pairs
                .iter()
                .map(|(k, v)| (cql_map_key(k), cql_value_to_json(v)))
                .collect();
            Value::Object(obj)
        }
        CqlValue::Tuple(items) => {
            Value::Array(
                items
                    .iter()
                    .map(|item| item.as_ref().map(cql_value_to_json).unwrap_or(Value::Null))
                    .collect(),
            )
        }
        CqlValue::Empty => Value::Null,
        CqlValue::Blob(bytes) => {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
            Value::Object(Map::from_iter([(
                "$binary".into(),
                Value::Object(Map::from_iter([("base64".into(), Value::String(b64))])),
            )]))
        }
        CqlValue::UserDefinedType { .. } => {
            // UDTs are complex; fall back to string representation.
            Value::String(val.to_string())
        }
        _ => Value::String(val.to_string()),
    }
}

fn cql_map_key(val: &CqlValue) -> String {
    match val {
        CqlValue::Text(s) | CqlValue::Ascii(s) => s.clone(),
        _ => val.to_string(),
    }
}

/// Map a scylla [`ColumnType`] to a human-readable type name string.
fn column_type_string(ct: &ColumnType<'_>) -> String {
    match ct {
        ColumnType::Native(nt) => native_type_string(nt),
        ColumnType::Collection { frozen, typ } => {
            let inner = collection_type_string(typ);
            if *frozen {
                format!("frozen<{inner}>")
            } else {
                inner
            }
        }
        ColumnType::Vector { typ, dimensions } => {
            format!("vector<{}, {}>", column_type_string(typ), dimensions)
        }
        ColumnType::Tuple(items) => {
            let parts: Vec<String> = items.iter().map(column_type_string).collect();
            format!("tuple<{}>", parts.join(", "))
        }
        ColumnType::UserDefinedType { definition, .. } => {
            format!("{}.{}", definition.keyspace, definition.name)
        }
        _ => "unknown".to_string(),
    }
}

/// Map a scylla [`CollectionType`] to a human-readable type name string.
fn collection_type_string(ct: &CollectionType<'_>) -> String {
    match ct {
        CollectionType::List(inner) => format!("list<{}>", column_type_string(inner)),
        CollectionType::Map(k, v) => {
            format!("map<{}, {}>", column_type_string(k), column_type_string(v))
        }
        CollectionType::Set(inner) => format!("set<{}>", column_type_string(inner)),
        _ => "unknown".to_string(),
    }
}

/// Map a scylla [`NativeType`] to a display name.
fn native_type_string(nt: &NativeType) -> String {
    match nt {
        NativeType::Ascii => "ascii".to_string(),
        NativeType::BigInt => "bigint".to_string(),
        NativeType::Blob => "blob".to_string(),
        NativeType::Boolean => "boolean".to_string(),
        NativeType::Counter => "counter".to_string(),
        NativeType::Date => "date".to_string(),
        NativeType::Decimal => "decimal".to_string(),
        NativeType::Double => "double".to_string(),
        NativeType::Float => "float".to_string(),
        NativeType::Int => "int".to_string(),
        NativeType::Text => "text".to_string(),
        NativeType::Timestamp => "timestamp".to_string(),
        NativeType::Timeuuid => "timeuuid".to_string(),
        NativeType::Inet => "inet".to_string(),
        NativeType::SmallInt => "smallint".to_string(),
        NativeType::TinyInt => "tinyint".to_string(),
        NativeType::Time => "time".to_string(),
        NativeType::Duration => "duration".to_string(),
        NativeType::Uuid => "uuid".to_string(),
        NativeType::Varint => "varint".to_string(),
        _ => "unknown".to_string(),
    }
}

/// Convert a scylla `Row` to a JSON object, zipping column names from the
/// result metadata with the row's cell values.
fn row_to_value(row: &Row, columns: &[QueryResultColumn]) -> Value {
    let mut map = Map::new();
    for (idx, cell) in row.columns.iter().enumerate() {
        let name = columns
            .get(idx)
            .map(|c| c.column_name.clone())
            .unwrap_or_else(|| format!("col_{idx}"));
        let value = match cell {
            Some(cql_val) => cql_value_to_json(cql_val),
            None => Value::Null,
        };
        map.insert(name, value);
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Catalog query templates (ported from sql/tables.js, sql/columns.js)
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = r#"
SELECT table_name as "pureName"
FROM system_schema.tables
WHERE keyspace_name='#DATABASE#'
"#;

const SQL_COLUMNS: &str = r#"
SELECT
  table_name as "pureName",
  column_name as "columnName",
  type as "dataType",
  kind as "kind"
FROM system_schema.columns
WHERE keyspace_name = '#DATABASE#'
"#;

// ---------------------------------------------------------------------------
// Analysis helpers (ported from Analyser.js)
// ---------------------------------------------------------------------------

fn get_str(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|v| v.as_str()).map(String::from)
}

fn db_object_info(pure_name: &str) -> DatabaseObjectInfo {
    DatabaseObjectInfo {
        pure_name: pure_name.to_string(),
        schema_name: None,
        pairing_id: None,
        object_id: None,
        create_date: None,
        modify_date: None,
        hash_code: None,
        object_type_field: None,
        object_comment: None,
    }
}

/// Build a primary key from column rows whose `kind` is `partition_key` or
/// `clustering`, mirroring the Analyser logic.
fn build_primary_key(pure: &str, columns_rows: &[Value]) -> Option<PrimaryKeyInfo> {
    let pk_columns: Vec<&Value> = columns_rows
        .iter()
        .filter(|c| {
            get_str(c, "pureName").as_deref() == Some(pure)
                && matches!(
                    get_str(c, "kind").as_deref(),
                    Some("partition_key") | Some("clustering")
                )
        })
        .collect();
    if pk_columns.is_empty() {
        return None;
    }
    Some(PrimaryKeyInfo {
        columns_constraint: ColumnsConstraintInfo {
            constraint: ConstraintInfo {
                pairing_id: None,
                constraint_name: None,
                constraint_type: ConstraintType::PrimaryKey,
            },
            columns: pk_columns
                .into_iter()
                .map(|c| ColumnReference {
                    column_name: get_str(c, "columnName").unwrap_or_default(),
                    ref_column_name: None,
                    is_included_column: None,
                    is_descending: None,
                })
                .collect(),
        },
    })
}

/// Collect columns for a specific table from the flat columns query result.
fn collect_columns(columns_rows: &[Value], pure: &str) -> Vec<ColumnInfo> {
    columns_rows
        .iter()
        .filter(|c| get_str(c, "pureName").as_deref() == Some(pure))
        .map(|c| ColumnInfo {
            column_name: get_str(c, "columnName").unwrap_or_default(),
            data_type: get_str(c, "dataType").unwrap_or_default(),
            not_null: None,
            ..Default::default()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// analyse_full / analyse_single_table
// ---------------------------------------------------------------------------

fn analyse_cassandra_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let conn = downcast(handle)?;
    let keyspace = &conn.keyspace;

    let tables = blocking_query(conn, &hydrate_keyspace_query(SQL_TABLES, keyspace)?)?;
    let columns = blocking_query(conn, &hydrate_keyspace_query(SQL_COLUMNS, keyspace)?)?;

    let mut result_tables = Vec::new();
    for table in &tables.rows {
        let pure = get_str(table, "pureName").unwrap_or_default();
        let table_columns = collect_columns(&columns.rows, &pure);
        let pk = build_primary_key(&pure, &columns.rows);

        result_tables.push(TableInfo {
            object: db_object_info(&pure),
            columns: table_columns,
            primary_key: pk,
            sorting_key: None,
            foreign_keys: Some(vec![]),
            dependencies: None,
            indexes: None,
            uniques: None,
            checks: None,
            table_row_count: None,
            table_engine: None,
        });
    }

    Ok(DatabaseInfo {
        tables: result_tables,
        views: vec![],
        ..Default::default()
    })
}

fn analyse_cassandra_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let conn = downcast(handle)?;
    let keyspace = &conn.keyspace;

    let tables = blocking_query(conn, &hydrate_keyspace_query(SQL_TABLES, keyspace)?)?;
    let columns = blocking_query(conn, &hydrate_keyspace_query(SQL_COLUMNS, keyspace)?)?;

    let pure = &name.pure_name;
    let table_columns = collect_columns(&columns.rows, pure);
    let pk = build_primary_key(pure, &columns.rows);

    // Verify the table exists.
    let _ = tables
        .rows
        .iter()
        .find(|t| get_str(t, "pureName").as_deref() == Some(pure.as_str()))
        .ok_or_else(|| DbgmError::new(format!("Cassandra table not found: {pure}")))?;

    Ok(TableInfo {
        object: db_object_info(pure),
        columns: table_columns,
        primary_key: pk,
        sorting_key: None,
        foreign_keys: Some(vec![]),
        dependencies: None,
        indexes: None,
        uniques: None,
        checks: None,
        table_row_count: None,
        table_engine: None,
    })
}

// ---------------------------------------------------------------------------
// EngineDriver implementation
// ---------------------------------------------------------------------------

impl EngineDriver for CassandraDriver {
    fn engine(&self) -> &str {
        CASSANDRA_ENGINE
    }

    fn title(&self) -> &str {
        "Cassandra"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: false,
            supports_transactions: false,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: false,
            default_port: Some(9042),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let contact_points = parse_contact_points(def)?;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| DbgmError::with_source("Cannot start tokio runtime for Cassandra", e))?;

        let keyspace = def.database.clone().unwrap_or_default();
        let local_dc = def
            .extra
            .as_ref()
            .and_then(|m| m.get("localDataCenter"))
            .and_then(|v| v.as_str())
            .unwrap_or("datacenter1")
            .to_string();

        let session = runtime.block_on(async {
            let mut builder = SessionBuilder::new();
            for point in &contact_points {
                builder = builder.known_node(point);
            }
            builder = builder.prefer_datacenter(local_dc);

            if let Some(user) = &def.user {
                let pass = def.password.clone().unwrap_or_default();
                builder = builder.user(user, pass);
            }

            let session = builder
                .build()
                .await
                .map_err(|e| DbgmError::with_source("Cannot connect to Cassandra", e))?;

            if !keyspace.is_empty() {
                session
                    .use_keyspace(&keyspace, false)
                    .await
                    .map_err(|e| {
                        DbgmError::with_source(
                            format!("Cannot use Cassandra keyspace '{keyspace}'"),
                            e,
                        )
                    })?;
            }

            Ok::<_, DbgmError>(session)
        })?;

        Ok(Box::new(CassandraConnection {
            runtime,
            session: Mutex::new(session),
            keyspace,
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
        let result = blocking_query(conn, "SELECT release_version FROM system.local")?;
        let version = result
            .rows
            .first()
            .and_then(|r| r.get("release_version").and_then(|v| v.as_str()))
            .unwrap_or("unknown")
            .to_string();
        Ok(ServerVersion {
            version: version.clone(),
            version_text: Some(format!("Cassandra/Scylla {version}")),
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = downcast(handle)?;
        let result =
            blocking_query(conn, "SELECT keyspace_name FROM system_schema.keyspaces")?;
        Ok(result
            .rows
            .iter()
            .filter_map(|r| r.get("keyspace_name").and_then(|v| v.as_str()).map(String::from))
            .map(|name| DatabaseEntry {
                name,
                size_on_disk: None,
                empty: None,
            })
            .collect())
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_cassandra_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_cassandra_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new(
            "Cassandra write_table streaming is not yet ported",
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// Unit tests (pure helpers only; no live server required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_contact_point() {
        let def = ConnectionDefinition {
            server: Some("127.0.0.1".to_string()),
            port: Some(9042),
            ..Default::default()
        };
        let points = parse_contact_points(&def).unwrap();
        assert_eq!(points, vec!["127.0.0.1:9042"]);
    }

    #[test]
    fn parses_multiple_contact_points() {
        let def = ConnectionDefinition {
            server: Some("host1, host2".to_string()),
            port: Some(9043),
            ..Default::default()
        };
        let points = parse_contact_points(&def).unwrap();
        assert_eq!(points, vec!["host1:9043", "host2:9043"]);
    }

    #[test]
    fn strips_cassandra_scheme_prefix() {
        let def = ConnectionDefinition {
            server: Some("cassandra://10.0.0.1".to_string()),
            ..Default::default()
        };
        let points = parse_contact_points(&def).unwrap();
        assert_eq!(points, vec!["10.0.0.1:9042"]);
    }

    #[test]
    fn preserves_existing_port() {
        let def = ConnectionDefinition {
            server: Some("host1:19042".to_string()),
            port: Some(9042),
            ..Default::default()
        };
        let points = parse_contact_points(&def).unwrap();
        assert_eq!(points, vec!["host1:19042"]);
    }

    #[test]
    fn returns_error_for_empty_server() {
        let def = ConnectionDefinition {
            server: None,
            ..Default::default()
        };
        assert!(parse_contact_points(&def).is_err());
    }

    #[test]
    fn hydrates_keyspace_query() {
        let sql = hydrate_keyspace_query(SQL_TABLES, "my_keyspace").unwrap();
        assert!(sql.contains("keyspace_name='my_keyspace'"));
    }

    #[test]
    fn rejects_keyspace_with_single_quote() {
        assert!(hydrate_keyspace_query(SQL_TABLES, "key'space").is_err());
    }

    #[test]
    fn maps_native_type_to_string() {
        assert_eq!(native_type_string(&NativeType::Int), "int");
        assert_eq!(native_type_string(&NativeType::Text), "text");
        assert_eq!(native_type_string(&NativeType::Boolean), "boolean");
        assert_eq!(native_type_string(&NativeType::Timestamp), "timestamp");
    }

    #[test]
    fn builds_primary_key_from_analyser_rows() {
        let columns = serde_json::json!([
            {"pureName": "users", "columnName": "id", "kind": "partition_key"},
            {"pureName": "users", "columnName": "name", "kind": "clustering"},
            {"pureName": "users", "columnName": "email", "kind": "regular"},
            {"pureName": "other", "columnName": "x", "kind": "partition_key"},
        ])
        .as_array()
        .cloned()
        .unwrap_or_default();
        let pk = build_primary_key("users", &columns).unwrap();
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
        assert_eq!(pk.columns_constraint.columns[1].column_name, "name");
    }

    #[test]
    fn returns_none_for_no_pk_columns() {
        let columns = serde_json::json!([
            {"pureName": "t", "columnName": "a", "kind": "regular"},
        ])
        .as_array()
        .cloned()
        .unwrap_or_default();
        assert!(build_primary_key("t", &columns).is_none());
    }

    #[test]
    fn collects_columns_for_table() {
        let columns = serde_json::json!([
            {"pureName": "t1", "columnName": "a", "dataType": "int"},
            {"pureName": "t1", "columnName": "b", "dataType": "text"},
            {"pureName": "t2", "columnName": "x", "dataType": "boolean"},
        ])
        .as_array()
        .cloned()
        .unwrap_or_default();
        let cols = collect_columns(&columns, "t1");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].column_name, "a");
        assert_eq!(cols[1].column_name, "b");
    }

    #[test]
    fn cql_value_boolean_to_json() {
        assert_eq!(cql_value_to_json(&CqlValue::Boolean(true)), Value::Bool(true));
        assert_eq!(cql_value_to_json(&CqlValue::Boolean(false)), Value::Bool(false));
    }

    #[test]
    fn cql_value_int_to_json() {
        assert_eq!(
            cql_value_to_json(&CqlValue::Int(42)),
            Value::Number(42.into())
        );
    }

    #[test]
    fn cql_value_text_to_json() {
        assert_eq!(
            cql_value_to_json(&CqlValue::Text("hello".into())),
            Value::String("hello".into())
        );
    }

    #[test]
    fn cql_value_empty_to_json() {
        assert_eq!(cql_value_to_json(&CqlValue::Empty), Value::Null);
    }

    #[test]
    fn cql_value_list_to_json() {
        let val = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]);
        let json = cql_value_to_json(&val);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[test]
    fn cql_value_map_to_json() {
        let val = CqlValue::Map(vec![
            (CqlValue::Text("a".into()), CqlValue::Int(1)),
            (CqlValue::Text("b".into()), CqlValue::Int(2)),
        ]);
        let json = cql_value_to_json(&val);
        assert!(json.is_object());
        assert_eq!(json["a"], Value::Number(1.into()));
        assert_eq!(json["b"], Value::Number(2.into()));
    }

    #[test]
    fn cql_value_counter_to_json() {
        use scylla::value::Counter;
        assert_eq!(
            cql_value_to_json(&CqlValue::Counter(Counter(100))),
            Value::Number(100.into())
        );
    }

    #[test]
    fn column_type_string_native() {
        assert_eq!(column_type_string(&ColumnType::Native(NativeType::Int)), "int");
    }

    #[test]
    fn column_type_string_list() {
        let ct = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Text))),
        };
        assert_eq!(column_type_string(&ct), "list<text>");
    }

    #[test]
    fn column_type_string_map() {
        let ct = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Int)),
            ),
        };
        assert_eq!(column_type_string(&ct), "map<text, int>");
    }

    #[test]
    fn engine_and_title() {
        let driver = CassandraDriver::new();
        assert_eq!(driver.engine(), "cassandra@dbgate-plugin-cassandra");
        assert_eq!(driver.title(), "Cassandra");
    }

    #[test]
    fn capabilities_match_spec() {
        let driver = CassandraDriver::new();
        let caps = driver.capabilities();
        assert!(!caps.supports_transactions);
        assert_eq!(caps.default_port, Some(9042));
    }
}
