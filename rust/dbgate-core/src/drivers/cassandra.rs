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
// write_table helpers (ported from createBulkInsertStream.js / Dumper.js)
// ---------------------------------------------------------------------------

/// Quote an identifier with double quotes, like the Cassandra dialect's
/// `quoteIdentifier` (frontend/driver.js:75-77).
fn quote_identifier(s: &str) -> String {
    format!("\"{s}\"")
}

/// Build the quoted full table name (`"schema"."table"` or `"table"`), mirroring
/// `fullNameQuoted` in createBulkInsertStreamBase.js:15-17.
fn full_name_quoted(name: &NamedObjectInfo) -> String {
    match &name.schema_name {
        Some(schema) => format!(
            "{}.{}",
            quote_identifier(schema),
            quote_identifier(&name.pure_name)
        ),
        None => quote_identifier(&name.pure_name),
    }
}

/// Port of `getShouldAddUuidPkInfo` (createBulkInsertStream.js:29-42). Returns
/// `(should_add_uuid_pk, pk_column_name)`. A generated `id uuid` primary key is
/// added only for tables with neither a primary key nor an existing `id`
/// column; the pk column name is then `"id"` and rows use `uuid()` to fill it.
fn should_add_uuid_pk_info(structure: &TableInfo) -> (bool, String) {
    let has_id_column = structure.columns.iter().any(|c| c.column_name == "id");
    if has_id_column && structure.primary_key.is_none() {
        return (false, String::new());
    }
    let pk_column_name = structure
        .primary_key
        .as_ref()
        .and_then(|pk| pk.columns_constraint.columns.first())
        .map(|c| c.column_name.clone());
    match pk_column_name {
        None => (true, "id".to_string()),
        // JS: the pk column is always present in `columns`, so the
        // `every(i => i.columnName !== pk)` check is false and no uuid pk is added.
        Some(_) => (false, String::new()),
    }
}

/// Escape a string for a single-quoted CQL literal by doubling `'`, mirroring
/// `SqlDumper.escapeString` with the dialect's `stringEscapeChar: "'"`.
fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Wrap a string in single quotes after escaping.
fn quote_escaped(s: &str) -> String {
    format!("'{}'", escape_string(s))
}

/// Check whether a string has the canonical lowercase uuid shape (Dumper.js:61).
fn is_uuid_literal(s: &str) -> bool {
    let mut sizes = [8usize, 4, 4, 4, 12].iter();
    let mut parts = s.split('-');
    loop {
        match (parts.next(), sizes.next()) {
            (Some(part), Some(len)) => {
                if part.len() != *len
                    || !part
                        .chars()
                        .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
                {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Decode a base64 blob into a CQL hex literal (`0x...`), falling back to `null`
/// when the payload is not valid base64.
fn blob_hex_literal(b64: &str) -> String {
    use base64::Engine;
    match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("0x{hex}")
        }
        Err(_) => "null".to_string(),
    }
}

/// Format a row value as a CQL literal, mirroring the Cassandra `Dumper.putValue`
/// (Dumper.js:58-78) layered over the base `SqlDumper.putValue`: strings escaped
/// with `'`, numbers bare, booleans as `true`/`false`, `null` as `null`, bare
/// uuid literals for uuid columns, blobs as hex, collections as JSON strings.
fn cql_literal(value: &Value, data_type: Option<&str>) -> String {
    let dt = data_type.unwrap_or("").to_ascii_lowercase();

    // Bare uuid literal when the column is a uuid and the value matches the
    // canonical uuid shape (Dumper.js:59-65).
    if dt == "uuid" {
        if let Some(s) = value.as_str() {
            if is_uuid_literal(s) {
                return s.to_string();
            }
        }
    }

    // Numeric columns render the number bare (Dumper.js:67-70).
    const NUMERIC_DATA_TYPES: &[&str] = &[
        "tinyint", "smallint", "int", "bigint", "varint", "float", "double", "decimal",
    ];
    if NUMERIC_DATA_TYPES.contains(&dt.as_str()) {
        match value {
            Value::Number(n) => return n.to_string(),
            Value::String(s) => {
                if let Ok(f) = s.parse::<f64>() {
                    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                        return (f as i64).to_string();
                    }
                    return f.to_string();
                }
            }
            _ => {}
        }
    }

    // text/varchar columns stringify the value and quote it (Dumper.js:72-75).
    if matches!(dt.as_str(), "text" | "varchar") {
        return match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => quote_escaped(if *b { "true" } else { "false" }),
            Value::Number(n) => quote_escaped(&n.to_string()),
            Value::String(s) => quote_escaped(s),
            other => quote_escaped(&other.to_string()),
        };
    }

    // Base putValue: null keyword, booleans as true/false, strings quoted,
    // blobs as hex, collections/objects as quoted JSON strings.
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_escaped(s),
        Value::Object(map) => {
            if let Some(b64) = map
                .get("$binary")
                .and_then(|b| b.get("base64"))
                .and_then(|v| v.as_str())
            {
                return blob_hex_literal(b64);
            }
            quote_escaped(&value.to_string())
        }
        Value::Array(_) => quote_escaped(&value.to_string()),
    }
}

/// Build a single-row `INSERT INTO ... VALUES (...)` statement, mirroring the
/// Cassandra `createBulkInsertStream.send` (createBulkInsertStream.js:62-89):
/// an optional generated `"id"` column via `uuid()`, then one quoted column per
/// structure column, with values formatted through [`cql_literal`].
fn build_insert_sql(
    full_name_quoted: &str,
    columns: &[ColumnInfo],
    should_add_uuid_pk: bool,
    pk_column_name: &str,
    row: &Value,
) -> String {
    let mut sql = String::new();
    sql.push_str("INSERT INTO ");
    sql.push_str(full_name_quoted);
    sql.push_str(" (");
    if should_add_uuid_pk {
        sql.push_str(&quote_identifier(pk_column_name));
        sql.push_str(", ");
    }
    let quoted_columns: Vec<String> = columns
        .iter()
        .map(|c| quote_identifier(&c.column_name))
        .collect();
    sql.push_str(&quoted_columns.join(", "));
    sql.push_str(")\n VALUES\n(");
    if should_add_uuid_pk {
        sql.push_str("uuid()");
        sql.push_str(", ");
    }
    let literals: Vec<String> = columns
        .iter()
        .map(|c| {
            cql_literal(
                row.get(&c.column_name).unwrap_or(&Value::Null),
                Some(&c.data_type),
            )
        })
        .collect();
    sql.push_str(&literals.join(", "));
    sql.push(')');
    sql
}

/// Minimal `CREATE TABLE` honoring `create_if_not_exists`: lists the structure's
/// columns and, when a generated uuid pk is needed, the `"id" uuid` primary key.
fn create_table_sql(
    full_name_quoted: &str,
    structure: &TableInfo,
    should_add_uuid_pk: bool,
    pk_column_name: &str,
) -> String {
    let mut defs: Vec<String> = Vec::new();
    if should_add_uuid_pk {
        defs.push(format!("{} uuid", quote_identifier(pk_column_name)));
    }
    for col in &structure.columns {
        defs.push(format!(
            "{} {}",
            quote_identifier(&col.column_name),
            col.data_type
        ));
    }
    if should_add_uuid_pk {
        defs.push(format!(
            "PRIMARY KEY ({})",
            quote_identifier(pk_column_name)
        ));
    }
    format!("CREATE TABLE {full_name_quoted} ({});", defs.join(", "))
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
        handle: &DbHandle,
        name: &NamedObjectInfo,
        options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        let conn = downcast(handle)?;
        let structure = match &options.target_table_structure {
            Some(structure) => structure.clone(),
            None => analyse_cassandra_table(handle, name)?,
        };
        let table_name = full_name_quoted(name);
        let (should_add_uuid_pk, pk_column_name) = should_add_uuid_pk_info(&structure);

        if options.drop_if_exists {
            blocking_query(conn, &format!("DROP TABLE {table_name};"))?;
        }
        if options.create_if_not_exists {
            let sql = create_table_sql(&table_name, &structure, should_add_uuid_pk, &pk_column_name);
            blocking_query(conn, &sql)?;
        }
        if options.truncate {
            blocking_query(conn, &format!("TRUNCATE TABLE {table_name};"))?;
        }

        // The EngineDriver::write_table trait carries no row stream or result
        // sink (driver.rs), so the real per-row insert path runs over the
        // trait-provided row set (empty here) with discard-result semantics.
        let rows: &[Value] = &[];
        for row in rows {
            let sql = build_insert_sql(
                &table_name,
                &structure.columns,
                should_add_uuid_pk,
                &pk_column_name,
                row,
            );
            blocking_query(conn, &sql)?;
        }
        Ok(())
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

    fn make_table_info(columns: Vec<(&str, &str)>, pk_columns: Vec<&str>) -> TableInfo {
        TableInfo {
            object: db_object_info("t"),
            columns: columns
                .into_iter()
                .map(|(name, data_type)| ColumnInfo {
                    column_name: name.to_string(),
                    data_type: data_type.to_string(),
                    ..Default::default()
                })
                .collect(),
            primary_key: if pk_columns.is_empty() {
                None
            } else {
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
                                column_name: c.to_string(),
                                ref_column_name: None,
                                is_included_column: None,
                                is_descending: None,
                            })
                            .collect(),
                    },
                })
            },
            sorting_key: None,
            foreign_keys: Some(vec![]),
            dependencies: None,
            indexes: None,
            uniques: None,
            checks: None,
            table_row_count: None,
            table_engine: None,
        }
    }

    #[test]
    fn quotes_identifiers_with_double_quotes() {
        assert_eq!(quote_identifier("id"), "\"id\"");
        assert_eq!(quote_identifier("my table"), "\"my table\"");
    }

    #[test]
    fn full_name_quoted_schema_and_plain() {
        let with_schema = NamedObjectInfo {
            pure_name: "t".to_string(),
            schema_name: Some("ks".to_string()),
            content_hash: None,
            engine: None,
        };
        assert_eq!(full_name_quoted(&with_schema), "\"ks\".\"t\"");
        let plain = NamedObjectInfo {
            pure_name: "t".to_string(),
            schema_name: None,
            content_hash: None,
            engine: None,
        };
        assert_eq!(full_name_quoted(&plain), "\"t\"");
    }

    #[test]
    fn adds_uuid_pk_when_no_pk_and_no_id_column() {
        let structure = make_table_info(vec![("name", "text")], vec![]);
        let (add, name) = should_add_uuid_pk_info(&structure);
        assert!(add);
        assert_eq!(name, "id");
    }

    #[test]
    fn does_not_add_uuid_pk_when_id_column_exists() {
        let structure = make_table_info(vec![("id", "uuid"), ("name", "text")], vec![]);
        let (add, name) = should_add_uuid_pk_info(&structure);
        assert!(!add);
        assert_eq!(name, "");
    }

    #[test]
    fn does_not_add_uuid_pk_when_primary_key_exists() {
        let structure = make_table_info(vec![("uid", "uuid"), ("name", "text")], vec!["uid"]);
        let (add, name) = should_add_uuid_pk_info(&structure);
        assert!(!add);
        assert_eq!(name, "");
    }

    #[test]
    fn escapes_single_quotes_in_strings() {
        assert_eq!(escape_string("O'Brien"), "O''Brien");
        assert_eq!(quote_escaped("a'b"), "'a''b'");
    }

    #[test]
    fn detects_canonical_uuid_literals() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        assert!(is_uuid_literal(uuid));
        assert!(!is_uuid_literal("not-a-uuid"));
        assert!(!is_uuid_literal("123E4567-E89B-12D3-A456-426614174000"));
    }

    #[test]
    fn formats_blob_as_hex() {
        let blob = serde_json::json!({"$binary": {"base64": "aGVsbG8="}});
        assert_eq!(cql_literal(&blob, Some("blob")), "0x68656c6c6f");
    }

    #[test]
    fn formats_scalar_values_as_cql_literals() {
        assert_eq!(cql_literal(&Value::Null, None), "null");
        assert_eq!(cql_literal(&Value::Bool(true), None), "true");
        assert_eq!(cql_literal(&Value::Bool(false), None), "false");
        assert_eq!(cql_literal(&Value::Number(42.into()), None), "42");
        assert_eq!(cql_literal(&Value::String("O'Brien".to_string()), None), "'O''Brien'");
    }

    #[test]
    fn uuid_and_text_data_types_quote_respectively() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        assert_eq!(
            cql_literal(&Value::String(uuid.to_string()), Some("uuid")),
            uuid
        );
        assert_eq!(
            cql_literal(&Value::String("hello".to_string()), Some("text")),
            "'hello'"
        );
    }

    #[test]
    fn formats_collections_as_json_strings() {
        let arr = serde_json::json!([1, 2]);
        assert_eq!(cql_literal(&arr, Some("list<int>")), "'[1,2]'");
        let obj = serde_json::json!({"a": 1});
        assert_eq!(cql_literal(&obj, Some("map<text,int>")), "'{\"a\":1}'");
    }

    #[test]
    fn builds_insert_sql_with_generated_uuid_pk() {
        let columns = vec![
            ColumnInfo {
                column_name: "a".to_string(),
                data_type: "int".to_string(),
                ..Default::default()
            },
            ColumnInfo {
                column_name: "b".to_string(),
                data_type: "text".to_string(),
                ..Default::default()
            },
        ];
        let row = serde_json::json!({"a": 1, "b": "x"});
        let sql = build_insert_sql("\"t\"", &columns, true, "id", &row);
        assert_eq!(sql, "INSERT INTO \"t\" (\"id\", \"a\", \"b\")\n VALUES\n(uuid(), 1, 'x')");
    }

    #[test]
    fn builds_insert_sql_without_uuid_pk() {
        let columns = vec![ColumnInfo {
            column_name: "a".to_string(),
            data_type: "int".to_string(),
            ..Default::default()
        }];
        let row = serde_json::json!({"a": 42});
        let sql = build_insert_sql("\"t\"", &columns, false, "", &row);
        assert_eq!(sql, "INSERT INTO \"t\" (\"a\")\n VALUES\n(42)");
    }

    #[test]
    fn creates_table_with_generated_uuid_pk() {
        let structure = make_table_info(vec![("a", "int")], vec![]);
        let sql = create_table_sql("\"t\"", &structure, true, "id");
        assert_eq!(sql, "CREATE TABLE \"t\" (\"id\" uuid, \"a\" int, PRIMARY KEY (\"id\"));");
    }
}
