//! Microsoft SQL Server engine driver.
//!
//! Rust port of `plugins/dbgate-plugin-mssql/src/backend/` on top of the
//! [`tiberius`] TDS client. This is the "native" TDS path (SQL Server
//! user/password authentication; Windows integrated auth is exposed by
//! tiberius on Windows but is out of scope for this port).
//!
//! The `EngineDriver` trait is synchronous, while `tiberius` is async. Each
//! connection therefore owns a dedicated tokio runtime plus a `Mutex<Client>`
//! so the sync trait methods can drive async work via `runtime.block_on(...)`.

use std::any::Any;
use std::sync::Mutex;

use futures_util::StreamExt;
use serde_json::{Map, Value};
use tiberius::{AuthMethod, Column, ColumnData, ColumnType, Config, EncryptionLevel, QueryItem, Row};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

/// The concrete TDS client stream type produced by `tcp.compat_write()`.
type ClientStream = Compat<tokio::net::TcpStream>;
type TdsClient = tiberius::Client<ClientStream>;

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{
    CallableObjectInfo, ColumnInfo, ConstraintInfo, ConstraintType, ColumnsConstraintInfo,
    DatabaseInfo, DatabaseObjectInfo, ForeignKeyInfo, FunctionInfo, IndexInfo, NamedObjectInfo,
    ParameterInfo, ParameterMode, PrimaryKeyInfo, ProcedureInfo, SqlObjectInfo, TableInfo,
    TriggerEventType, TriggerInfo, TriggerTiming, UniqueInfo, ViewInfo,
};
use crate::driver::{
    Capabilities, DatabaseEntry, DbHandle, EngineDriver, QueryOptions, ServerVersion, StreamSink,
    StreamSeverity, WriteTableOptions,
};
use crate::error::{DbgmError, DbgmResult};
use crate::query::{QueryResult, QueryResultColumn};
use crate::ssh_tunnel::SshTunnel;

/// Dotted engine id for SQL Server.
pub const MSSQL_ENGINE: &str = "mssql@dbgate-plugin-mssql";

/// An open SQL Server connection: the tokio runtime that drives the TDS
/// client plus the client itself. Both are `Send + Sync`, so this plugs
/// directly into a [`DbHandle`].
struct MssqlConnection {
    runtime: tokio::runtime::Runtime,
    client: Mutex<TdsClient>,
    database: Option<String>,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
}

/// The SQL Server driver.
pub struct MssqlDriver;

impl MssqlDriver {
    pub fn new() -> Self {
        Self
    }

    fn downcast(handle: &DbHandle) -> DbgmResult<&MssqlConnection> {
        handle
            .downcast_ref::<MssqlConnection>()
            .ok_or_else(|| DbgmError::new("handle is not an MSSQL connection"))
    }

    fn out(err: tiberius::error::Error) -> DbgmError {
        DbgmError::with_source("MSSQL error", err)
    }
}

impl Default for MssqlDriver {
    fn default() -> Self {
        Self::new()
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(MssqlDriver::new())
}

/// Build a tiberius [`Config`] from a connection definition.
fn build_config(def: &ConnectionDefinition) -> DbgmResult<Config> {
    let server = def
        .server
        .clone()
        .ok_or_else(|| DbgmError::new("MSSQL connection requires a server host"))?;

    let mut config = Config::new();
    config.host(server);
    config.port(def.port.unwrap_or(1433) as u16);
    if let Some(database) = &def.database {
        config.database(database.clone());
    }
    if let Some(user) = &def.user {
        let password = def.password.clone().unwrap_or_default();
        config.authentication(AuthMethod::sql_server(user, password));
    }
    config.application_name("DbGate");

    // Mirror the tedious `ssl`/`trustServerCertificate` handling: if a trust
    // flag is present, trust the (possibly self-signed) server cert and
    // require encryption; otherwise default to requiring encryption too.
    let trust = def
        .extra
        .as_ref()
        .and_then(|m| m.get("trustServerCertificate"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || def
            .extra
            .as_ref()
            .and_then(|m| m.get("ssl"))
            .is_some();
    if trust {
        config.trust_cert();
    }
    config.encryption(EncryptionLevel::Required);

    Ok(config)
}

// ---------------------------------------------------------------------------
// Value mapping: tiberius ColumnData -> DbGate JSON Value
// ---------------------------------------------------------------------------

/// Convert a tiberius column type to a lowercase SQL type-name string.
fn column_type_name(ty: ColumnType) -> String {
    use ColumnType::*;
    let name = match ty {
        Null => "null",
        Bit | Bitn => "bit",
        Int1 => "tinyint",
        Int2 => "smallint",
        Int4 => "int",
        Int8 | Intn => "bigint",
        Float4 => "real",
        Float8 | Floatn => "float",
        Money | Money4 => "money",
        Datetime4 => "smalldatetime",
        Datetime | Datetimen => "datetime",
        Guid => "uniqueidentifier",
        Decimaln | Numericn => "decimal",
        Daten => "date",
        Timen => "time",
        Datetime2 => "datetime2",
        DatetimeOffsetn => "datetimeoffset",
        BigVarBin | BigBinary | Image => "binary",
        BigVarChar | BigChar | Text => "varchar",
        NVarchar | NChar | NText => "nvarchar",
        Xml => "xml",
        Udt => "udt",
        SSVariant => "sql_variant",
    };
    name.to_string()
}

/// Map a single tiberius cell to a JSON value, mirroring the JS
/// `modifyRow`/value coercion in `tediousDriver.js` (binary -> `$binary`).
fn cell_to_json(col_data: &ColumnData) -> Value {
    match col_data {
        ColumnData::U8(v) => v.map(Value::from).unwrap_or(Value::Null),
        ColumnData::I16(v) => v.map(Value::from).unwrap_or(Value::Null),
        ColumnData::I32(v) => v.map(Value::from).unwrap_or(Value::Null),
        ColumnData::I64(v) => v.map(Value::from).unwrap_or(Value::Null),
        ColumnData::F32(v) => v.map(|f| number_or_null(f as f64)).unwrap_or(Value::Null),
        ColumnData::F64(v) => v.map(number_or_null).unwrap_or(Value::Null),
        ColumnData::Bit(v) => v.map(Value::Bool).unwrap_or(Value::Null),
        ColumnData::String(v) => v
            .as_ref()
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        ColumnData::Guid(v) => v.map(|u| Value::String(u.to_string())).unwrap_or(Value::Null),
        ColumnData::Binary(v) => v
            .as_ref()
            .map(|bytes| binary_value(bytes))
            .unwrap_or(Value::Null),
        ColumnData::Numeric(v) => v
            .as_ref()
            .map(|n| number_or_null(f64::from(*n)))
            .unwrap_or(Value::Null),
        ColumnData::Xml(v) => v
            .as_ref()
            .map(|x| Value::String(x.to_string()))
            .unwrap_or(Value::Null),
        // Date/time cells are converted to chrono values in `cell_value`;
        // they never reach this fallback.
        ColumnData::DateTime(_)
        | ColumnData::SmallDateTime(_)
        | ColumnData::Time(_)
        | ColumnData::Date(_)
        | ColumnData::DateTime2(_)
        | ColumnData::DateTimeOffset(_) => Value::Null,
    }
}

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

/// Convert a full tiberius row to a JSON object keyed by column name.
fn row_to_value(row: &Row) -> Value {
    let mut map = Map::new();
    for (idx, (column, cell)) in row.cells().enumerate() {
        map.insert(column.name().to_string(), cell_value(row, idx, column.column_type(), cell));
    }
    Value::Object(map)
}

/// Convert one cell, using `Row::try_get` for typed date/time values (tiberius
/// exposes those through its `chrono` feature) and the raw `ColumnData` match
/// for everything else.
fn cell_value(row: &Row, idx: usize, ty: ColumnType, cell: &ColumnData) -> Value {
    use ColumnType::*;
    match ty {
        Daten => row
            .try_get::<chrono::NaiveDate, _>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or_else(|| cell_to_json(cell)),
        Timen => row
            .try_get::<chrono::NaiveTime, _>(idx)
            .ok()
            .flatten()
            .map(|t| Value::String(t.to_string()))
            .unwrap_or_else(|| cell_to_json(cell)),
        Datetime4 | Datetimen | Datetime2 | Datetime => row
            .try_get::<chrono::NaiveDateTime, _>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or_else(|| cell_to_json(cell)),
        DatetimeOffsetn => row
            .try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx)
            .ok()
            .flatten()
            .map(|d| Value::String(d.to_string()))
            .unwrap_or_else(|| cell_to_json(cell)),
        _ => cell_to_json(cell),
    }
}

/// Build the DbGate column metadata for a result set's columns. Optionally
/// enrich each with its primary-key status from a `dbinfo` structure.
fn columns_metadata(columns: &[Column]) -> Vec<QueryResultColumn> {
    columns
        .iter()
        .map(|c| QueryResultColumn {
            column_name: c.name().to_string(),
            data_type: Some(column_type_name(c.column_type())),
            ..Default::default()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Query helpers (run on the connection's runtime)
// ---------------------------------------------------------------------------

/// Split a SQL script on statement terminators, honouring quoted strings.
fn split_sql(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    let mut in_square = false;
    let mut prev: Option<char> = None;
    for c in sql.chars() {
        match in_quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    in_quote = None;
                }
            }
            None if in_square => {
                current.push(c);
                if c == ']' {
                    in_square = false;
                }
            }
            None => match c {
                '\'' => {
                    in_quote = Some('\'');
                    current.push(c);
                }
                '"' => {
                    in_quote = Some('"');
                    current.push(c);
                }
                '[' => {
                    in_square = true;
                    current.push(c);
                }
                ';' => {
                    let trimmed = current.trim().to_string();
                    if !trimmed.is_empty() {
                        out.push(trimmed);
                    }
                    current.clear();
                }
                _ => {
                    // Avoid splitting on `--` line comments freely; MSSQL
                    // scripts use `;` as the separator which is safe here.
                    let _ = prev;
                    current.push(c);
                }
            },
        }
        prev = Some(c);
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        out.push(trimmed);
    }
    out
}

/// Run one statement and return (columns, rows). For SELECTs this collects
/// the returned rows; for DML/DDL it returns an empty result set.
async fn run_statement(
    client: &mut TdsClient,
    sql: &str,
) -> tiberius::Result<(Vec<QueryResultColumn>, Vec<Value>)> {
    let mut stream = client.query(sql, &[]).await?;
    let mut columns: Vec<QueryResultColumn> = Vec::new();
    let mut rows: Vec<Value> = Vec::new();

    while let Some(item) = stream.next().await {
        let item = item?;
        match item {
            QueryItem::Metadata(meta) => {
                if columns.is_empty() {
                    columns = columns_metadata(meta.columns());
                }
            }
            QueryItem::Row(row) => rows.push(row_to_value(&row)),
        }
    }
    Ok((columns, rows))
}

// ---------------------------------------------------------------------------
// EngineDriver implementation
// ---------------------------------------------------------------------------

impl EngineDriver for MssqlDriver {
    fn engine(&self) -> &str {
        MSSQL_ENGINE
    }

    fn title(&self) -> &str {
        "SQL Server"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: false,
            supports_transactions: true,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: false,
            default_port: Some(1433),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let server = def
            .server
            .clone()
            .ok_or_else(|| DbgmError::new("MSSQL connection requires a server host"))?;
        let port = def.port.unwrap_or(1433) as u16;
        let tunnel = SshTunnel::open(def, &server, port)?;
        let mut config = build_config(def)?;
        if let Some(t) = &tunnel {
            let (host, port) = t.local_endpoint();
            config.host(host);
            config.port(port);
        }
        let addr = config.get_addr();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| DbgmError::with_source("Cannot start tokio runtime for MSSQL", e))?;

        let client = runtime.block_on(async move {
            let tcp = tokio::net::TcpStream::connect(&addr)
                .await
                .map_err(|e| DbgmError::with_source(format!("Cannot connect to MSSQL at {addr}"), e))?;
            tcp.set_nodelay(true)
                .map_err(|e| DbgmError::with_source("Cannot set nodelay on MSSQL socket", e))?;
            tiberius::Client::connect(config, tcp.compat_write())
                .await
                .map_err(Self::out)
        })?;

        Ok(Box::new(MssqlConnection {
            runtime,
            client: Mutex::new(client),
            database: def.database.clone(),
            ssh_tunnel: tunnel,
        }))
    }

    fn close(&self, handle: DbHandle) -> DbgmResult<()> {
        drop(handle);
        Ok(())
    }

    fn query(&self, handle: &DbHandle, sql: &str, _options: &QueryOptions) -> DbgmResult<QueryResult> {
        let conn = Self::downcast(handle)?;
        let mut client = conn
            .client
            .lock()
            .map_err(|_| DbgmError::new("MSSQL connection lock poisoned"))?;
        let (columns, rows) = conn
            .runtime
            .block_on(run_statement(&mut client, sql))
            .map_err(Self::out)?;
        Ok(QueryResult { rows, columns })
    }

    fn stream(&self, handle: &DbHandle, sql: &str, sink: &StreamSink) -> DbgmResult<()> {
        let conn = Self::downcast(handle)?;
        let mut client = conn
            .client
            .lock()
            .map_err(|_| DbgmError::new("MSSQL connection lock poisoned"))?;

        let statements = split_sql(sql);
        let mut rows_affected: u64 = 0;
        for stmt in &statements {
            if stmt.trim().is_empty() {
                continue;
            }
            let (columns, rows) = conn
                .runtime
                .block_on(run_statement(&mut client, stmt))
                .map_err(Self::out)?;
            if columns.is_empty() {
                rows_affected += rows.len() as u64;
            } else {
                (sink.on_recordset)(&columns);
                for row in &rows {
                    (sink.on_row)(row);
                }
                rows_affected += rows.len() as u64;
            }
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
        let mut client = conn
            .client
            .lock()
            .map_err(|_| DbgmError::new("MSSQL connection lock poisoned"))?;
        let (_, rows) = conn
            .runtime
            .block_on(run_statement(&mut client, VERSION_QUERY))
            .map_err(Self::out)?;
        let row = rows.first().cloned().unwrap_or_else(|| Value::Object(Map::new()));
        let version = row
            .get("productVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| row.get("version").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .unwrap_or_else(|| "unknown".to_string());
        let version_text = row
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(ServerVersion {
            version,
            version_text,
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = Self::downcast(handle)?;
        let mut client = conn
            .client
            .lock()
            .map_err(|_| DbgmError::new("MSSQL connection lock poisoned"))?;
        let (_, rows) = conn
            .runtime
            .block_on(run_statement(&mut client, "SELECT name FROM sys.databases ORDER BY name"))
            .map_err(Self::out)?;
        Ok(rows
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
        analyse_mssql_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_mssql_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new("MSSQL write_table streaming is not yet ported"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// get_version / analysis catalog SQL (ported from the mssql plugin)
// ---------------------------------------------------------------------------

const VERSION_QUERY: &str = "
SELECT
  @@VERSION AS version,
  SERVERPROPERTY('productversion') as productVersion,
  CONVERT(INT, SERVERPROPERTY('EngineEdition')) as engineEdition,
  CASE
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '8%' THEN 'SQL Server 2000'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '9%' THEN 'SQL Server 2005'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '10.0%' THEN 'SQL Server 2008'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '10.5%' THEN 'SQL Server 2008 R2'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '11%' THEN 'SQL Server 2012'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '12%' THEN 'SQL Server 2014'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '13%' THEN 'SQL Server 2016'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '14%' THEN 'SQL Server 2017'
  WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('productversion')) like '15%' THEN 'SQL Server 2019'
  ELSE 'Unknown'
  END AS versionText
";

/// Replace the analyser's `OBJECT_ID_CONDITION` / `SCHEMA_NAME_CONDITION`
/// placeholders. `object_id` is the substituted condition; when `None` it
/// becomes ` is not null` (all objects), matching DatabaseAnalyser.
fn substitute_conditions(template: &str, object_id: Option<&str>, schema: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!(" = '{id}'"),
        None => " is not null".to_string(),
    };
    let schema_cond = match schema {
        Some(s) => format!(" = '{s}'"),
        None => " is not null".to_string(),
    };
    template
        .replace("=OBJECT_ID_CONDITION", &object_cond)
        .replace("=SCHEMA_NAME_CONDITION", &schema_cond)
}

/// Port of `getFullDataTypeName` from MsSqlAnalyser.js.
fn full_data_type_name(data_type: &str, char_max_length: Option<i64>, precision: Option<i64>, scale: Option<i64>) -> String {
    let mut full = data_type.to_string();
    if let Some(len) = char_max_length {
        if is_type_string(data_type) {
            full = format!("{data_type}({})", if len < 0 { "MAX".to_string() } else { len.to_string() });
        }
    }
    if let (Some(p), Some(s)) = (precision, scale) {
        if is_type_numeric(data_type) {
            full = format!("{data_type}({p},{s})");
        }
    }
    full
}

fn is_type_string(t: &str) -> bool {
    matches!(
        t.to_ascii_lowercase().as_str(),
        "char" | "varchar" | "nchar" | "nvarchar" | "binary" | "varbinary" | "text" | "ntext"
            | "xml" | "sysname"
    )
}

fn is_type_numeric(t: &str) -> bool {
    matches!(t.to_ascii_lowercase().as_str(), "decimal" | "numeric")
}

/// Port of `simplifyComutedExpression` / default-value paren stripping.
fn strip_outer_parens(mut s: String) -> String {
    loop {
        let trimmed = s.trim().to_string();
        if trimmed.starts_with('(') && trimmed.ends_with(')') {
            s = trimmed[1..trimmed.len() - 1].to_string();
        } else {
            return s;
        }
    }
}

/// Port of `getColumnInfo` from MsSqlAnalyser.js.
fn build_column_info(row: &Value, _object_id: &str) -> ColumnInfo {
    let get = |k: &str| row.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
    let get_i = |k: &str| row.get(k).and_then(|v| v.as_i64());
    let get_b = |k: &str| row.get(k).and_then(|v| v.as_bool());

    let data_type = get("dataType").unwrap_or_default();
    let char_max_length = get_i("charMaxLength");
    let precision = get_i("numericPrecision");
    let scale = get_i("numericScale");

    let full_type = full_data_type_name(&data_type, char_max_length, precision, scale);

    let mut default_value = get("defaultValue");
    if let Some(dv) = default_value.as_mut() {
        *dv = strip_outer_parens(dv.clone());
    }
    let computed = get("computedExpression").map(strip_outer_parens);

    ColumnInfo {
        column_name: get("columnName").unwrap_or_default(),
        data_type: full_type,
        not_null: Some(!get_b("isNullable").unwrap_or(true)),
        auto_increment: Some(get_b("isIdentity").unwrap_or(false)),
        default_value,
        default_constraint: get("defaultConstraint"),
        computed_expression: computed,
        column_comment: get("columnComment"),
        ..Default::default()
    }
}

/// Build the table's primary-key info from primaryKeys query rows.
fn build_primary_key(table: &Value, pk_rows: &[Value]) -> Option<PrimaryKeyInfo> {
    let pure_name = table.get("pureName").and_then(|v| v.as_str()).unwrap_or_default();
    let schema_name = table.get("schemaName");
    let filtered = pk_rows.iter().filter(|r| {
        r.get("pureName").and_then(|v| v.as_str()) == Some(pure_name)
            && r.get("schemaName") == schema_name
    });
    let mut cols: Vec<String> = Vec::new();
    let mut constraint_name = None;
    for r in filtered {
        if constraint_name.is_none() {
            constraint_name = r.get("constraintName").and_then(|v| v.as_str()).map(String::from);
        }
        if let Some(c) = r.get("columnName").and_then(|v| v.as_str()) {
            cols.push(c.to_string());
        }
    }
    if cols.is_empty() {
        return None;
    }
    Some(PrimaryKeyInfo {
        columns_constraint: ColumnsConstraintInfo {
            constraint: ConstraintInfo {
                pairing_id: None,
                constraint_name,
                constraint_type: ConstraintType::PrimaryKey,
            },
            columns: cols
                .into_iter()
                .map(|c| crate::dbinfo::ColumnReference {
                    column_name: c,
                    ref_column_name: None,
                    is_included_column: None,
                    is_descending: None,
                })
                .collect(),
        },
    })
}

/// Build foreign keys grouped by constraint name (port of extractForeignKeys).
fn build_foreign_keys(table: &Value, fk_rows: &[Value]) -> Vec<ForeignKeyInfo> {
    let pure_name = table.get("pureName").and_then(|v| v.as_str()).unwrap_or_default();
    let schema_name = table.get("schemaName");
    let mut grouped: Vec<(String, Vec<&Value>)> = Vec::new();
    for r in fk_rows {
        if r.get("pureName").and_then(|v| v.as_str()) != Some(pure_name)
            || r.get("schemaName") != schema_name
        {
            continue;
        }
        let name = r.get("constraintName").and_then(|v| v.as_str()).unwrap_or_default().to_string();
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
            let get = |k: &str| first.get(k).and_then(|v| v.as_str()).map(String::from);
            let columns = group
                .iter()
                .map(|r| crate::dbinfo::ColumnReference {
                    column_name: r
                        .get("columnName")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    ref_column_name: r.get("refColumnName").and_then(|v| v.as_str()).map(String::from),
                    is_included_column: None,
                    is_descending: None,
                })
                .collect();
            ForeignKeyInfo {
                columns_constraint: ColumnsConstraintInfo {
                    constraint: ConstraintInfo {
                        pairing_id: None,
                        constraint_name: Some(name),
                        constraint_type: ConstraintType::ForeignKey,
                    },
                    columns,
                },
                ref_schema_name: get("refSchemaName"),
                ref_table_name: get("refTableName").unwrap_or_default(),
                update_action: get("updateAction"),
                delete_action: get("deleteAction"),
            }
        })
        .collect()
}

/// Build indexes / uniques from indexes + indexcols query rows.
fn build_indexes_and_uniques(
    table: &Value,
    index_rows: &[Value],
    indexcols_rows: &[Value],
) -> (Vec<IndexInfo>, Vec<UniqueInfo>) {
    let object_id = table.get("objectId").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let obj_cond = |r: &Value| r.get("object_id").and_then(|v| v.as_str()) == Some(object_id.as_str());
    let mut indexes = Vec::new();
    let mut uniques = Vec::new();

    for idx in index_rows.iter().filter(|r| obj_cond(r)) {
        let index_id = idx.get("index_id").and_then(|v| v.as_i64());
        let is_unique_constraint = idx.get("is_unique_constraint").and_then(|v| v.as_bool()).unwrap_or(false);
        let columns: Vec<crate::dbinfo::ColumnReference> = indexcols_rows
            .iter()
            .filter(|c| {
                c.get("object_id").and_then(|v| v.as_str()) == Some(object_id.as_str())
                    && c.get("index_id").and_then(|v| v.as_i64()) == index_id
            })
            .map(|c| crate::dbinfo::ColumnReference {
                column_name: c.get("columnName").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                ref_column_name: None,
                is_included_column: c.get("isIncludedColumn").and_then(|v| v.as_bool()),
                is_descending: c.get("isDescending").and_then(|v| v.as_bool()),
            })
            .collect();

        let constraint = ConstraintInfo {
            pairing_id: None,
            constraint_name: idx.get("constraintName").and_then(|v| v.as_str()).map(String::from),
            constraint_type: if is_unique_constraint {
                ConstraintType::Unique
            } else {
                ConstraintType::Index
            },
        };

        if is_unique_constraint {
            uniques.push(UniqueInfo {
                columns_constraint: ColumnsConstraintInfo { constraint, columns },
            });
        } else {
            indexes.push(IndexInfo {
                columns_constraint: ColumnsConstraintInfo { constraint, columns },
                is_unique: idx.get("isUnique").and_then(|v| v.as_bool()).unwrap_or(false),
                index_type: idx.get("indexType").and_then(|v| v.as_str()).map(String::from),
                filter_definition: idx.get("filterDefinition").and_then(|v| v.as_str()).map(String::from),
            });
        }
    }
    (indexes, uniques)
}

/// Build parameter list for a procedure/function from parameter rows.
fn build_parameters(parent_object_id: &str, param_rows: &[Value]) -> Vec<ParameterInfo> {
    param_rows
        .iter()
        .filter(|r| r.get("parentObjectId").and_then(|v| v.as_str()) == Some(parent_object_id))
        .map(|r| {
            let get = |k: &str| r.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
            let mode = match get("parameterMode").as_deref() {
                Some("OUT") => ParameterMode::Out,
                _ => ParameterMode::In,
            };
            ParameterInfo {
                parameter_name: get("parameterName").unwrap_or_default(),
                data_type: full_data_type_name(
                    &get("dataType").unwrap_or_default(),
                    r.get("charMaxLength").and_then(|v| v.as_i64()),
                    r.get("numericPrecision").and_then(|v| v.as_i64()),
                    r.get("numericScale").and_then(|v| v.as_i64()),
                ),
                parameter_mode: Some(mode),
                position: r.get("parameterIndex").and_then(|v| v.as_i64()),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// analyse_full
// ---------------------------------------------------------------------------

fn db_object(value: &Value) -> DatabaseObjectInfo {
    let get = |k: &str| value.get(k).and_then(|v| v.as_str()).map(String::from);
    DatabaseObjectInfo {
        pure_name: get("pureName").unwrap_or_default(),
        schema_name: get("schemaName"),
        pairing_id: None,
        object_id: get("objectId"),
        create_date: get("createDate"),
        modify_date: get("modifyDate"),
        hash_code: get("contentHash"),
        object_type_field: get("sqlObjectType"),
        object_comment: get("objectComment"),
    }
}

fn analyser_query(
    handle: &DbHandle,
    template: &str,
    object_id: Option<&str>,
    schema: Option<&str>,
) -> DbgmResult<QueryResult> {
    let conn = MssqlDriver::downcast(handle)?;
    let mut client = conn
        .client
        .lock()
        .map_err(|_| DbgmError::new("MSSQL connection lock poisoned"))?;
    let sql = substitute_conditions(template, object_id, schema);
    let (columns, rows) = conn
        .runtime
        .block_on(run_statement(&mut client, &sql))
        .map_err(MssqlDriver::out)?;
    Ok(QueryResult { rows, columns })
}

fn analyse_mssql_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    // Full analysis: no object filter -> both conditions become "is not null".
    let tables = analyser_query(handle, SQL_TABLES, None, None)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, None, None)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, None, None)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, None, None)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, None, None)?.rows;
    let indexcols_rows = analyser_query(handle, SQL_INDEXCOLS, None, None)?.rows;
    let table_sizes = analyser_query(handle, SQL_TABLE_SIZES, None, None)?.rows;
    let sql_code_rows = analyser_query(handle, SQL_LOAD_SQL_CODE, None, None)?.rows;
    let views_rows = analyser_query(handle, SQL_VIEWS, None, None)?.rows;
    let view_columns_rows = analyser_query(handle, SQL_VIEW_COLUMNS, None, None)?.rows;
    let programmables_rows = analyser_query(handle, SQL_PROGRAMMABLES, None, None)?.rows;
    let proc_param_rows = analyser_query(handle, SQL_PROCEDURES_PARAMETERS, None, None)?.rows;
    let fn_param_rows = analyser_query(handle, SQL_FUNCTION_PARAMETERS, None, None)?.rows;
    let trigger_rows = analyser_query(handle, SQL_TRIGGERS, None, None)?.rows;

    let get_create_sql = |pure_name: &str, schema_name: &Option<String>| -> Option<String> {
        let parts: Vec<&str> = sql_code_rows
            .iter()
            .filter(|r| {
                r.get("pureName").and_then(|v| v.as_str()) == Some(pure_name)
                    && r.get("schemaName").and_then(|v| v.as_str())
                        == schema_name.as_deref()
            })
            .filter_map(|r| r.get("codeText").and_then(|v| v.as_str()))
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.concat())
        }
    };

    let mut result_tables = Vec::new();
    for table in &tables {
        let object_id = table.get("objectId").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let columns: Vec<ColumnInfo> = columns_rows
            .iter()
            .filter(|c| {
                c.get("objectId").and_then(|v| v.as_str()) == Some(object_id.as_str())
            })
            .map(|c| build_column_info(c, &object_id))
            .collect();

        let row_count = table_sizes
            .iter()
            .find(|r| r.get("objectId").and_then(|v| v.as_str()) == Some(object_id.as_str()))
            .and_then(|r| r.get("tableRowCount").and_then(|v| v.as_i64()));

        let (indexes, uniques) = build_indexes_and_uniques(table, &indexes_rows, &indexcols_rows);

        result_tables.push(TableInfo {
            object: db_object(table),
            columns,
            primary_key: build_primary_key(table, &pk_rows),
            sorting_key: None,
            foreign_keys: Some(build_foreign_keys(table, &fk_rows)),
            dependencies: None,
            indexes: Some(indexes),
            uniques: Some(uniques),
            checks: None,
            table_row_count: row_count,
            table_engine: None,
        });
    }

    let mut views = Vec::new();
    for view in &views_rows {
        let object_id = view.get("objectId").and_then(|v| v.as_str()).unwrap_or_default();
        let pure_name = view.get("pureName").and_then(|v| v.as_str()).unwrap_or_default();
        let schema_name = view.get("schemaName").and_then(|v| v.as_str()).map(String::from);
        let columns: Vec<ColumnInfo> = view_columns_rows
            .iter()
            .filter(|c| {
                c.get("objectId").and_then(|v| v.as_str()) == Some(object_id)
            })
            .map(|c| build_column_info(c, object_id))
            .collect();
        views.push(ViewInfo {
            object: SqlObjectInfo {
                object: db_object(view),
                create_sql: get_create_sql(pure_name, &schema_name),
                requires_format: None,
            },
            columns,
        });
    }

    let mut procedures = Vec::new();
    let mut functions = Vec::new();
    for prog in &programmables_rows {
        let object_id = prog.get("objectId").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let object_type = prog
            .get("sqlObjectType")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();
        let pure_name = prog.get("pureName").and_then(|v| v.as_str()).unwrap_or_default();
        let schema_name = prog.get("schemaName").and_then(|v| v.as_str()).map(String::from);
        let params = if matches!(object_type.as_str(), "FN" | "IF" | "TF") {
            build_parameters(&object_id, &fn_param_rows)
        } else {
            build_parameters(&object_id, &proc_param_rows)
        };
        let callable = CallableObjectInfo {
            object: SqlObjectInfo {
                object: db_object(prog),
                create_sql: get_create_sql(pure_name, &schema_name),
                requires_format: None,
            },
            parameters: if params.is_empty() { None } else { Some(params) },
        };
        if object_type == "P" {
            procedures.push(ProcedureInfo { callable });
        } else if matches!(object_type.as_str(), "FN" | "IF" | "TF") {
            functions.push(FunctionInfo {
                callable,
                return_type: None,
            });
        }
    }

    let mut triggers = Vec::new();
    for trg in &trigger_rows {
        let get = |k: &str| trg.get(k).and_then(|v| v.as_str()).map(String::from);
        let timing = match get("triggerTiming").as_deref() {
            Some("AFTER") => Some(TriggerTiming::After),
            Some("INSTEAD OF") => Some(TriggerTiming::InsteadOf),
            _ => Some(TriggerTiming::Before),
        };
        let event = match get("eventType").as_deref() {
            Some("INSERT") => Some(TriggerEventType::Insert),
            Some("UPDATE") => Some(TriggerEventType::Update),
            Some("DELETE") => Some(TriggerEventType::Delete),
            _ => None,
        };
        let object = DatabaseObjectInfo {
            pure_name: get("triggerName").unwrap_or_default(),
            schema_name: get("schemaName"),
            pairing_id: None,
            object_id: get("objectId"),
            create_date: None,
            modify_date: get("modifyDate"),
            hash_code: None,
            object_type_field: None,
            object_comment: None,
        };
        triggers.push(TriggerInfo {
            object: SqlObjectInfo {
                object,
                create_sql: get("definition"),
                requires_format: None,
            },
            function_name: None,
            table_name: get("tableName"),
            trigger_timing: timing,
            event_type: event,
        });
    }

    Ok(DatabaseInfo {
        tables: result_tables,
        views,
        procedures,
        functions,
        triggers,
        ..Default::default()
    })
}

fn analyse_mssql_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let conn = MssqlDriver::downcast(handle)?;
    let schema = name
        .schema_name
        .clone()
        .or_else(|| conn.database.clone())
        .unwrap_or_else(|| "dbo".to_string());
    let full_name = format!("[{schema}].[{}]", name.pure_name);

    // Resolve the object id for the single-object filter.
    let id_rows = analyser_query(handle, &format!("SELECT OBJECT_ID('{full_name}') AS id"), None, None)?;
    let object_id = id_rows
        .rows
        .first()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| DbgmError::new(format!("Object not found: {full_name}")))?;

    let object_id_some = Some(object_id.as_str());
    let schema_some = Some(schema.as_str());
    let tables = analyser_query(handle, SQL_TABLES, object_id_some, schema_some)?.rows;
    let columns_rows = analyser_query(handle, SQL_COLUMNS, object_id_some, schema_some)?.rows;
    let pk_rows = analyser_query(handle, SQL_PRIMARY_KEYS, object_id_some, schema_some)?.rows;
    let fk_rows = analyser_query(handle, SQL_FOREIGN_KEYS, object_id_some, schema_some)?.rows;
    let indexes_rows = analyser_query(handle, SQL_INDEXES, object_id_some, schema_some)?.rows;
    let indexcols_rows = analyser_query(handle, SQL_INDEXCOLS, object_id_some, schema_some)?.rows;
    let table_sizes = analyser_query(handle, SQL_TABLE_SIZES, None, schema_some)?.rows;

    let table = tables
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table not found: {full_name}")))?;
    let object_id_owned = object_id.clone();
    let columns: Vec<ColumnInfo> = columns_rows
        .iter()
        .filter(|c| c.get("objectId").and_then(|v| v.as_str()) == Some(object_id_owned.as_str()))
        .map(|c| build_column_info(c, &object_id_owned))
        .collect();
    let row_count = table_sizes
        .iter()
        .find(|r| r.get("objectId").and_then(|v| v.as_str()) == Some(object_id_owned.as_str()))
        .and_then(|r| r.get("tableRowCount").and_then(|v| v.as_i64()));
    let (indexes, uniques) = build_indexes_and_uniques(table, &indexes_rows, &indexcols_rows);

    Ok(TableInfo {
        object: db_object(table),
        columns,
        primary_key: build_primary_key(table, &pk_rows),
        sorting_key: None,
        foreign_keys: Some(build_foreign_keys(table, &fk_rows)),
        dependencies: None,
        indexes: Some(indexes),
        uniques: Some(uniques),
        checks: None,
        table_row_count: row_count,
        table_engine: None,
    })
}

// ---------------------------------------------------------------------------
// Catalog query templates (ported verbatim from the mssql plugin sql/ files)
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = "
select
	o.name as pureName,
	s.name as schemaName,
	o.object_id as objectId,
	o.create_date as createDate,
	o.modify_date as modifyDate,
	ep.value as objectComment
from sys.tables o
inner join sys.schemas s on o.schema_id = s.schema_id
left join sys.extended_properties ep on ep.major_id = o.object_id
	and ep.minor_id = 0
	and ep.name = 'MS_Description'
  and ep.class = 1
where o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION
";

const SQL_COLUMNS: &str = "
select c.name as columnName, t.name as dataType, c.object_id as objectId, c.is_identity as isIdentity,
    c.max_length as maxLength, c.precision, c.scale, c.is_nullable as isNullable,
    col.CHARACTER_MAXIMUM_LENGTH as charMaxLength,
    d.definition as defaultValue, d.name as defaultConstraint,
    m.definition as computedExpression, m.is_persisted as isPersisted, c.column_id as columnId,
    col.NUMERIC_PRECISION as numericPrecision,
    col.NUMERIC_SCALE as numericScale,
    c.is_sparse as isSparse,
    ep.value as columnComment
from sys.columns c
inner join sys.types t on c.system_type_id = t.system_type_id and c.user_type_id = t.user_type_id
inner join sys.objects o on c.object_id = o.object_id
INNER JOIN sys.schemas u ON u.schema_id=o.schema_id
INNER JOIN INFORMATION_SCHEMA.COLUMNS col ON col.TABLE_NAME = o.name AND col.TABLE_SCHEMA = u.name and col.COLUMN_NAME = c.name
left join sys.default_constraints d on c.default_object_id = d.object_id
left join sys.computed_columns m on m.object_id = c.object_id and m.column_id = c.column_id
left join sys.extended_properties ep on ep.major_id = c.object_id
    and ep.minor_id = c.column_id
    and ep.name = 'MS_Description'
    and ep.class = 1
where o.type = 'U' and o.object_id =OBJECT_ID_CONDITION and u.name =SCHEMA_NAME_CONDITION
order by c.column_id
";

const SQL_PRIMARY_KEYS: &str = "
SELECT
    i.object_id AS objectId,
    o.name AS pureName,
    s.name AS schemaName,
    c.name AS columnName,
    i.name AS constraintName
FROM
    sys.indexes i
INNER JOIN
    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN
    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
INNER JOIN
    sys.objects o ON i.object_id = o.object_id
INNER JOIN
    sys.schemas s ON o.schema_id = s.schema_id
WHERE
    i.is_primary_key = 1
	and o.object_id =OBJECT_ID_CONDITION
    and s.name =SCHEMA_NAME_CONDITION
ORDER BY
    ic.key_ordinal
";

const SQL_FOREIGN_KEYS: &str = "
SELECT
    schemaName = FK.TABLE_SCHEMA,
    pureName = FK.TABLE_NAME,
    columnName = CU.COLUMN_NAME,

    refSchemaName = PK.TABLE_SCHEMA,
    refTableName = PK.TABLE_NAME,
    refColumnName = RCU.COLUMN_NAME,

    constraintName = C.CONSTRAINT_NAME,
    updateAction = rc.UPDATE_RULE,
    deleteAction = rc.DELETE_RULE,

    objectId = o.object_id
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
    ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
    AND C.CONSTRAINT_SCHEMA = FK.CONSTRAINT_SCHEMA

LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
    ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
    AND C.UNIQUE_CONSTRAINT_SCHEMA = PK.CONSTRAINT_SCHEMA

LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
    ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
    AND C.CONSTRAINT_SCHEMA = CU.CONSTRAINT_SCHEMA

LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE RCU
    ON C.UNIQUE_CONSTRAINT_NAME = RCU.CONSTRAINT_NAME
    AND C.UNIQUE_CONSTRAINT_SCHEMA = RCU.CONSTRAINT_SCHEMA
    AND CU.ORDINAL_POSITION = RCU.ORDINAL_POSITION

INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    ON FK.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    AND FK.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA

INNER JOIN sys.objects o
    ON o.name = FK.TABLE_NAME
    AND SCHEMA_NAME(o.schema_id) = FK.TABLE_SCHEMA

INNER JOIN sys.schemas s
    ON o.schema_id = s.schema_id
    AND s.name = FK.TABLE_SCHEMA

where o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION
ORDER BY CU.ORDINAL_POSITION
";

const SQL_INDEXES: &str = "
select i.object_id, i.name as constraintName, i.type_desc as indexType, i.is_unique as isUnique,i.index_id, i.is_unique_constraint, i.filter_definition AS filterDefinition
from sys.indexes i
inner join sys.objects o on i.object_id = o.object_id
INNER JOIN sys.schemas u ON u.schema_id=o.schema_id
where i.is_primary_key=0
and i.is_hypothetical=0 and indexproperty(i.object_id, i.name, 'IsStatistics') = 0
and objectproperty(i.object_id, 'IsUserTable') = 1
and i.index_id between 1 and 254
 and i.object_id =OBJECT_ID_CONDITION and u.name =SCHEMA_NAME_CONDITION
";

const SQL_INDEXCOLS: &str = "
select
    c.object_id, c.index_id, c.column_id,
    col.name as columnName,
    c.is_descending_key as isDescending, c.is_included_column as isIncludedColumn
from sys.index_columns c
inner join sys.columns col on c.object_id = col.object_id and c.column_id = col.column_id
inner join sys.objects o on c.object_id = o.object_id
INNER JOIN sys.schemas u ON u.schema_id=o.schema_id
where c.object_id =OBJECT_ID_CONDITION and u.name =SCHEMA_NAME_CONDITION
order by c.key_ordinal
";

const SQL_TABLE_SIZES: &str = "
SELECT distinct
    t.object_id as objectId,
    p.rows AS tableRowCount
FROM
    sys.tables t
INNER JOIN
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN
    sys.schemas s ON t.schema_id = s.schema_id
WHERE
    t.NAME NOT LIKE 'dt%'
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255
    AND s.name =SCHEMA_NAME_CONDITION
";

const SQL_LOAD_SQL_CODE: &str = "
select s.name as pureName, u.name as schemaName, c.text AS codeText
    from sys.objects s
    inner join sys.syscomments c on s.object_id = c.id
    inner join sys.schemas u on u.schema_id = s.schema_id
where (s.object_id =OBJECT_ID_CONDITION) and u.name =SCHEMA_NAME_CONDITION
order by u.name, s.name, c.colid
";

const SQL_VIEWS: &str = "
SELECT
	o.name as pureName,
	u.name as schemaName,
	o.object_id as objectId,
	o.create_date as createDate,
	o.modify_date as modifyDate
FROM sys.objects o INNER JOIN sys.schemas u ON u.schema_id=o.schema_id
WHERE type in ('V') and o.object_id =OBJECT_ID_CONDITION and u.name =SCHEMA_NAME_CONDITION
";

const SQL_VIEW_COLUMNS: &str = "
select
    o.object_id AS objectId,
    col.TABLE_SCHEMA as schemaName,
    col.TABLE_NAME as pureName,
	col.COLUMN_NAME as columnName,
	col.IS_NULLABLE as isNullable,
	col.DATA_TYPE as dataType,
	col.CHARACTER_MAXIMUM_LENGTH as charMaxLength,
	col.NUMERIC_PRECISION as precision,
	col.NUMERIC_SCALE as scale,
	col.COLUMN_DEFAULT
FROM sys.objects o
INNER JOIN sys.schemas u ON u.schema_id=o.schema_id
INNER JOIN INFORMATION_SCHEMA.COLUMNS col ON col.TABLE_NAME = o.name AND col.TABLE_SCHEMA = u.name
WHERE o.type in ('V') and o.object_id =OBJECT_ID_CONDITION and u.name =SCHEMA_NAME_CONDITION
order by col.ORDINAL_POSITION
";

const SQL_PROGRAMMABLES: &str = "
select o.name as pureName, s.name as schemaName, o.object_id as objectId, o.create_date as createDate, o.modify_date as modifyDate, o.type as sqlObjectType
from sys.objects o
inner join sys.schemas s on o.schema_id = s.schema_id
where o.type in ('P', 'IF', 'FN', 'TF') and o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION
";

const SQL_PROCEDURES_PARAMETERS: &str = "
SELECT
    o.object_id as parentObjectId,
    p.object_id as objectId,
    o.name as pureName,
    p.name AS parameterName,
    TYPE_NAME(p.user_type_id) AS dataType,
    CASE
        WHEN TYPE_NAME(p.user_type_id) = 'nvarchar' THEN p.max_length / 2
        ELSE p.max_length
    END AS charMaxLength,
    CASE
        WHEN p.is_output = 1 THEN 'OUT'
        ELSE 'IN'
    END AS parameterMode,
    CASE
        WHEN TYPE_NAME(p.user_type_id) IN ('numeric', 'decimal') THEN p.precision
        ELSE NULL
    END AS numericPrecision,
    CASE
        WHEN TYPE_NAME(p.user_type_id) IN ('numeric', 'decimal') THEN p.scale
        ELSE NULL
    END AS numericScale,
    p.parameter_id AS parameterIndex,
    s.name as schemaName
FROM
    sys.objects o
JOIN
    sys.parameters p ON o.object_id = p.object_id
INNER JOIN
    sys.schemas s ON s.schema_id=o.schema_id
WHERE
    o.type = 'P'
    and o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION
ORDER BY
    o.object_id,
    p.parameter_id;
";

const SQL_FUNCTION_PARAMETERS: &str = "
SELECT
    o.object_id as parentObjectId,
    p.object_id AS parameterObjectId,
    o.name as pureName,
    CASE
        WHEN p.name IS NULL OR LTRIM(RTRIM(p.name)) = '' THEN
            '@Output'
        ELSE
            p.name
    END AS parameterName,
    TYPE_NAME(p.user_type_id) AS dataType,
    CASE
        WHEN TYPE_NAME(p.user_type_id) = 'nvarchar' THEN p.max_length / 2
        ELSE p.max_length
    END AS charMaxLength,
    CASE
        WHEN p.is_output = 1 THEN 'OUT'
        ELSE 'IN'
    END AS parameterMode,
    CASE
        WHEN TYPE_NAME(p.user_type_id) IN ('numeric', 'decimal') THEN p.precision
        ELSE NULL
    END AS numericPrecision,
    CASE
        WHEN TYPE_NAME(p.user_type_id) IN ('numeric', 'decimal') THEN p.scale
        ELSE NULL
    END AS numericScale,
    p.parameter_id AS parameterIndex,
    s.name as schemaName
FROM
    sys.objects o
JOIN
    sys.parameters p ON o.object_id = p.object_id
INNER JOIN
    sys.schemas s ON s.schema_id=o.schema_id
WHERE
    o.type IN ('FN', 'IF', 'TF')
    and o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION
ORDER BY
    p.object_id,
    p.parameter_id;
";

const SQL_TRIGGERS: &str = "
SELECT
   o.modify_date as modifyDate,
   o.object_id as objectId,
   o.name AS triggerName,
   s.name AS schemaName,
   OBJECT_NAME(o.parent_object_id) AS tableName,
   CASE
       WHEN OBJECTPROPERTY(o.object_id, 'ExecIsAfterTrigger') = 1 THEN 'AFTER'
       WHEN OBJECTPROPERTY(o.object_id, 'ExecIsInsteadOfTrigger') = 1 THEN 'INSTEAD OF'
       ELSE 'BEFORE'
   END AS triggerTiming,
   CASE
       WHEN OBJECTPROPERTY(o.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT'
       WHEN OBJECTPROPERTY(o.object_id, 'ExecIsUpdateTrigger') = 1 THEN 'UPDATE'
       WHEN OBJECTPROPERTY(o.object_id, 'ExecIsDeleteTrigger') = 1 THEN 'DELETE'
   END AS eventType,
   OBJECT_DEFINITION(o.object_id) AS definition
FROM sys.objects o
INNER JOIN sys.tables t
   ON o.parent_object_id = t.object_id
INNER JOIN sys.schemas s
   ON t.schema_id = s.schema_id
WHERE o.type = 'TR'
  AND o.is_ms_shipped = 0
  AND o.object_id =OBJECT_ID_CONDITION
  AND s.name =SCHEMA_NAME_CONDITION
";

// ---------------------------------------------------------------------------
// Tests (pure helpers only; no live server)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_object_and_schema_conditions() {
        let tpl = "where o.object_id =OBJECT_ID_CONDITION and s.name =SCHEMA_NAME_CONDITION";
        // Full analysis: no filter -> is not null
        assert_eq!(
            substitute_conditions(tpl, None, None),
            "where o.object_id  is not null and s.name  is not null"
        );
        // Single object
        assert_eq!(
            substitute_conditions(tpl, Some("55"), Some("dbo")),
            "where o.object_id  = '55' and s.name  = 'dbo'"
        );
    }

    #[test]
    fn computes_full_data_type_names() {
        assert_eq!(
            full_data_type_name("nvarchar", Some(50), None, None),
            "nvarchar(50)"
        );
        assert_eq!(
            full_data_type_name("varchar", Some(-1), None, None),
            "varchar(MAX)"
        );
        assert_eq!(
            full_data_type_name("decimal", None, Some(18), Some(2)),
            "decimal(18,2)"
        );
        assert_eq!(full_data_type_name("int", None, None, None), "int");
    }

    #[test]
    fn strips_outer_parens_recursively() {
        assert_eq!(strip_outer_parens("((5))".to_string()), "5");
        assert_eq!(strip_outer_parens("(getdate())".to_string()), "getdate()");
        assert_eq!(strip_outer_parens("plain".to_string()), "plain");
    }

    #[test]
    fn splits_sql_on_semicolons_respecting_quotes_and_brackets() {
        let sql = "SELECT 'a;b' AS x; SELECT * FROM [t;bl];";
        let parts = split_sql(sql);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].contains("'a;b'"));
        assert!(parts[1].contains("[t;bl]"));
    }

    #[test]
    fn maps_column_types_to_sql_names() {
        assert_eq!(column_type_name(ColumnType::Int4), "int");
        assert_eq!(column_type_name(ColumnType::NVarchar), "nvarchar");
        assert_eq!(column_type_name(ColumnType::Decimaln), "decimal");
        assert_eq!(column_type_name(ColumnType::Daten), "date");
    }

    #[test]
    fn maps_cells_to_json() {
        assert_eq!(cell_to_json(&ColumnData::I32(Some(42))), Value::from(42));
        assert_eq!(cell_to_json(&ColumnData::I32(None)), Value::Null);
        assert_eq!(cell_to_json(&ColumnData::Bit(Some(true))), Value::Bool(true));
        assert_eq!(
            cell_to_json(&ColumnData::String(Some(std::borrow::Cow::Borrowed("hi")))),
            Value::String("hi".into())
        );
        // binary -> $binary.base64
        let bin = ColumnData::Binary(Some(std::borrow::Cow::Borrowed(&[0u8, 1, 2, 255])));
        let v = cell_to_json(&bin);
        assert_eq!(
            v["$binary"]["base64"],
            Value::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8, 1, 2, 255]))
        );
    }

    #[test]
    fn groups_foreign_keys_by_constraint() {
        let mk_row = |name: &str, col: &str, ref_col: &str| {
            let mut m = Map::new();
            m.insert("pureName".into(), Value::String("T".into()));
            m.insert("schemaName".into(), Value::String("dbo".into()));
            m.insert("constraintName".into(), Value::String(name.into()));
            m.insert("columnName".into(), Value::String(col.into()));
            m.insert("refColumnName".into(), Value::String(ref_col.into()));
            m.insert("refTableName".into(), Value::String("R".into()));
            m.insert("refSchemaName".into(), Value::String("dbo".into()));
            Value::Object(m)
        };
        let rows = vec![
            mk_row("FK1", "a", "x"),
            mk_row("FK1", "b", "y"),
            mk_row("FK_OTHER", "c", "z"),
        ];
        let table = serde_json::json!({ "pureName": "T", "schemaName": "dbo" });
        let fks = build_foreign_keys(&table, &rows);
        assert_eq!(fks.len(), 2);
        let fk1 = fks.iter().find(|f| f.columns_constraint.constraint.constraint_name.as_deref() == Some("FK1")).unwrap();
        assert_eq!(fk1.columns_constraint.columns.len(), 2);
        assert_eq!(fk1.columns_constraint.columns[0].column_name, "a");
        assert_eq!(fk1.columns_constraint.columns[0].ref_column_name.as_deref(), Some("x"));
    }

    #[test]
    fn builds_primary_key_from_rows() {
        let mk_row = |col: &str| {
            serde_json::json!({
                "pureName": "T", "schemaName": "dbo",
                "columnName": col, "constraintName": "PK_T"
            })
        };
        let rows = vec![mk_row("id"), mk_row("name")];
        let table = serde_json::json!({ "pureName": "T", "schemaName": "dbo" });
        let pk = build_primary_key(&table, &rows).expect("pk");
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
    }
}
