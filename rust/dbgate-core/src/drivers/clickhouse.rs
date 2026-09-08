use std::any::Any;
use std::collections::HashMap;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::connection::ConnectionDefinition;
use crate::dbinfo::{
    ColumnInfo, ColumnsConstraintInfo, ConstraintInfo, ConstraintType, DatabaseInfo,
    DatabaseObjectInfo, NamedObjectInfo, PrimaryKeyInfo, SqlObjectInfo, TableInfo, ViewInfo,
};
use crate::driver::{
    Capabilities, DatabaseEntry, DbHandle, EngineDriver, QueryOptions, ServerVersion, StreamSink,
    StreamSeverity, WriteTableOptions,
};
use crate::error::{DbgmError, DbgmResult};
use crate::query::{QueryResult, QueryResultColumn};
use crate::ssh_tunnel::SshTunnel;

pub const CLICKHOUSE_ENGINE: &str = "clickhouse@dbgate-plugin-clickhouse";

struct ClickHouseConnection {
    runtime: tokio::runtime::Runtime,
    client: clickhouse::Client,
    database: String,
    #[allow(dead_code)]
    ssh_tunnel: Option<SshTunnel>,
}

pub struct ClickHouseDriver;

impl ClickHouseDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClickHouseDriver {
    fn default() -> Self {
        Self::new()
    }
}

fn downcast(handle: &DbHandle) -> DbgmResult<&ClickHouseConnection> {
    handle
        .downcast_ref::<ClickHouseConnection>()
        .ok_or_else(|| DbgmError::new("Invalid ClickHouse connection handle"))
}

fn out(e: impl std::fmt::Display) -> DbgmError {
    DbgmError::new(e.to_string())
}

fn build_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}")
}

fn substitute_condition(template: &str, database: &str, object_id: Option<&str>) -> String {
    let object_cond = match object_id {
        Some(id) => format!(" = '{id}'"),
        None => " is not null".to_string(),
    };
    template
        .replace("#DATABASE#", database)
        .replace("=OBJECT_ID_CONDITION", &object_cond)
}

fn extract_data_type(data_type: &str) -> (String, bool) {
    if let Some(inner) = data_type
        .strip_prefix("Nullable(")
        .and_then(|s| s.strip_suffix(')'))
    {
        (inner.to_string(), false)
    } else {
        (data_type.to_string(), true)
    }
}

fn parse_comma_separated(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn get_str<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(|v| v.as_str())
}

fn get_i64(row: &Value, key: &str) -> Option<i64> {
    row.get(key).and_then(|v| v.as_i64())
}

fn build_primary_key_from_comma(raw: &str) -> Option<PrimaryKeyInfo> {
    let columns = parse_comma_separated(raw);
    if columns.is_empty() {
        return None;
    }
    Some(PrimaryKeyInfo {
        columns_constraint: ColumnsConstraintInfo {
            constraint: ConstraintInfo {
                pairing_id: None,
                constraint_name: None,
                constraint_type: ConstraintType::PrimaryKey,
            },
            columns: columns
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

fn build_sorting_key_from_comma(raw: &str) -> Option<ColumnsConstraintInfo> {
    let columns = parse_comma_separated(raw);
    if columns.is_empty() {
        return None;
    }
    Some(ColumnsConstraintInfo {
        constraint: ConstraintInfo {
            pairing_id: None,
            constraint_name: None,
            constraint_type: ConstraintType::SortingKey,
        },
        columns: columns
            .into_iter()
            .map(|c| crate::dbinfo::ColumnReference {
                column_name: c,
                ref_column_name: None,
                is_included_column: None,
                is_descending: None,
            })
            .collect(),
    })
}

fn get_column_info_from_ch(row: &Value, column_name_field: &str) -> ColumnInfo {
    let raw_type = get_str(row, "dataType").unwrap_or_default();
    let (displayed, not_null) = extract_data_type(raw_type);
    ColumnInfo {
        column_name: get_str(row, column_name_field)
            .unwrap_or_default()
            .to_string(),
        data_type: raw_type.to_string(),
        not_null: Some(not_null),
        displayed_data_type: Some(displayed),
        default_value: get_str(row, "defaultValue").map(String::from),
        column_comment: get_str(row, "columnComment").map(String::from),
        ..Default::default()
    }
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

fn build_create_view_sql(pure_name: &str, view_def: &str) -> String {
    format!("CREATE VIEW \"{pure_name}\"\nAS\n{view_def}")
}

// ---------------------------------------------------------------------------
// Blocking query helpers
// ---------------------------------------------------------------------------

async fn query_rows_as_json(
    client: &clickhouse::Client,
    sql: &str,
) -> Result<(Vec<QueryResultColumn>, Vec<Value>), DbgmError> {
    let bytes = client
        .query(sql)
        .fetch_bytes("JSONEachRow")
        .map_err(out)?;

    let mut rows = Vec::new();
    let mut columns: Vec<QueryResultColumn> = Vec::new();
    let mut columns_extracted = false;

    let mut lines = BufReader::new(bytes).lines();
    while let Some(line_result) = lines.next_line().await.map_err(out)? {
        if line_result.is_empty() {
            continue;
        }
        let val: Value = serde_json::from_str(&line_result).map_err(out)?;
        if !columns_extracted {
            if let Value::Object(map) = &val {
                columns = map
                    .keys()
                    .map(|k| QueryResultColumn {
                        column_name: k.clone(),
                        ..Default::default()
                    })
                    .collect();
            }
            columns_extracted = true;
        }
        rows.push(val);
    }
    Ok((columns, rows))
}

fn blocking_query(conn: &ClickHouseConnection, sql: &str) -> DbgmResult<QueryResult> {
    let (columns, rows) = conn
        .runtime
        .block_on(query_rows_as_json(&conn.client, sql))?;
    Ok(QueryResult { rows, columns })
}

// ---------------------------------------------------------------------------
// SQL catalog queries
// ---------------------------------------------------------------------------

const SQL_TABLES: &str = "\
select name as \"pureName\", metadata_modification_time as \"contentHash\",
total_rows as \"tableRowCount\", uuid as \"objectId\", comment as \"objectComment\",
engine as \"tableEngine\", primary_key as \"primaryKeyColumns\",
sorting_key as \"sortingKeyColumns\"
from system.tables
where database='#DATABASE#' and uuid =OBJECT_ID_CONDITION and engine != 'View'";

const SQL_COLUMNS: &str = "\
select
    columns.table as \"pureName\",
    tables.uuid as \"objectId\",
    columns.name as \"columnName\",
    columns.type as \"dataType\",
    columns.comment as \"columnComment\",
    columns.default_expression as \"defaultValue\"
from system.columns
inner join system.tables on columns.table = tables.name and columns.database = tables.database
where columns.database='#DATABASE#' and tables.uuid =OBJECT_ID_CONDITION
order by toInt32(columns.position)";

const SQL_VIEWS: &str = "\
select
    tables.name as \"pureName\",
    tables.uuid as \"objectId\",
    views.view_definition as \"viewDefinition\",
    tables.metadata_modification_time as \"contentHash\"
from information_schema.views
inner join system.tables on views.table_name = tables.name and views.table_schema = tables.database
where views.table_schema='#DATABASE#' and tables.uuid =OBJECT_ID_CONDITION";

const SQL_VIEW_TEXTS: &str = "\
select
    tables.name as \"pureName\",
    tables.uuid as \"objectId\",
    tables.metadata_modification_time as \"contentHash\"
from system.tables
where tables.database='#DATABASE#' and tables.uuid =OBJECT_ID_CONDITION and tables.engine = 'View'";

// ---------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------

fn analyse_clickhouse_full(handle: &DbHandle) -> DbgmResult<DatabaseInfo> {
    let conn = downcast(handle)?;
    let database = &conn.database;

    let tables = blocking_query(conn, &substitute_condition(SQL_TABLES, database, None))?;
    let columns = blocking_query(conn, &substitute_condition(SQL_COLUMNS, database, None))?;

    let views_result = blocking_query(conn, &substitute_condition(SQL_VIEWS, database, None));
    let views = match views_result {
        Ok(v) if !v.rows.is_empty() => v,
        _ => blocking_query(conn, &substitute_condition(SQL_VIEW_TEXTS, database, None))?,
    };

    let mut table_columns: HashMap<String, Vec<ColumnInfo>> = HashMap::new();
    for row in &columns.rows {
        let pure_name = get_str(row, "pureName").unwrap_or_default().to_string();
        table_columns
            .entry(pure_name)
            .or_default()
            .push(get_column_info_from_ch(row, "columnName"));
    }

    let mut result_tables = Vec::new();
    for row in &tables.rows {
        let pure_name = get_str(row, "pureName").unwrap_or_default().to_string();
        let pk = get_str(row, "primaryKeyColumns")
            .and_then(build_primary_key_from_comma);
        let sk = get_str(row, "sortingKeyColumns")
            .and_then(build_sorting_key_from_comma);

        let mut obj = db_object_info(&pure_name);
        obj.object_comment = get_str(row, "objectComment").map(String::from);
        obj.hash_code = get_str(row, "contentHash").map(String::from);

        result_tables.push(TableInfo {
            object: obj,
            columns: table_columns.remove(&pure_name).unwrap_or_default(),
            primary_key: pk,
            sorting_key: sk,
            foreign_keys: None,
            dependencies: None,
            indexes: None,
            uniques: None,
            checks: None,
            table_row_count: get_i64(row, "tableRowCount"),
            table_engine: get_str(row, "tableEngine").map(String::from),
        });
    }

    let mut result_views = Vec::new();
    for row in &views.rows {
        let pure_name = get_str(row, "pureName").unwrap_or_default().to_string();
        let view_def = get_str(row, "viewDefinition").unwrap_or("");
        let create_sql = if view_def.is_empty() {
            None
        } else {
            Some(build_create_view_sql(&pure_name, view_def))
        };

        let obj = db_object_info(&pure_name);
        result_views.push(ViewInfo {
            object: SqlObjectInfo {
                object: obj,
                create_sql,
                requires_format: None,
            },
            columns: table_columns.remove(&pure_name).unwrap_or_default(),
        });
    }

    Ok(DatabaseInfo {
        tables: result_tables,
        views: result_views,
        ..Default::default()
    })
}

fn analyse_clickhouse_table(handle: &DbHandle, name: &NamedObjectInfo) -> DbgmResult<TableInfo> {
    let conn = downcast(handle)?;
    let database = &conn.database;

    let uuid_sql = format!(
        "SELECT uuid as id FROM system.tables WHERE database = '{database}' AND name='{}'",
        name.pure_name.replace('\'', "''")
    );
    let uuid_result = blocking_query(conn, &uuid_sql)?;
    let uuid = uuid_result
        .rows
        .first()
        .and_then(|r| get_str(r, "id"))
        .unwrap_or("")
        .to_string();

    let object_id = Some(uuid.as_str());
    let tables = blocking_query(conn, &substitute_condition(SQL_TABLES, database, object_id))?;
    let columns = blocking_query(conn, &substitute_condition(SQL_COLUMNS, database, object_id))?;

    let table_row = tables
        .rows
        .first()
        .ok_or_else(|| DbgmError::new(format!("Table '{}' not found", name.pure_name)))?;

    let pk = get_str(table_row, "primaryKeyColumns")
        .and_then(build_primary_key_from_comma);
    let sk = get_str(table_row, "sortingKeyColumns")
        .and_then(build_sorting_key_from_comma);

    let cols: Vec<ColumnInfo> = columns
        .rows
        .iter()
        .map(|r| get_column_info_from_ch(r, "columnName"))
        .collect();

    let mut obj = db_object_info(&name.pure_name);
    obj.object_comment = get_str(table_row, "objectComment").map(String::from);
    obj.hash_code = get_str(table_row, "contentHash").map(String::from);

    Ok(TableInfo {
        object: obj,
        columns: cols,
        primary_key: pk,
        sorting_key: sk,
        foreign_keys: None,
        dependencies: None,
        indexes: None,
        uniques: None,
        checks: None,
        table_row_count: get_i64(table_row, "tableRowCount"),
        table_engine: get_str(table_row, "tableEngine").map(String::from),
    })
}

// ---------------------------------------------------------------------------
// EngineDriver implementation
// ---------------------------------------------------------------------------

impl EngineDriver for ClickHouseDriver {
    fn engine(&self) -> &str {
        CLICKHOUSE_ENGINE
    }

    fn title(&self) -> &str {
        "ClickHouse"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read_only_sessions: true,
            supports_transactions: false,
            supports_native_backup: false,
            supports_native_restore: false,
            supports_server_summary: false,
            default_port: Some(8123),
        }
    }

    fn connect(&self, def: &ConnectionDefinition) -> DbgmResult<DbHandle> {
        let server = def
            .server
            .clone()
            .ok_or_else(|| DbgmError::new("ClickHouse connection requires a server host"))?;
        let port = def.port.unwrap_or(8123) as u16;
        let tunnel = SshTunnel::open(def, &server, port)?;
        let (host, port) = match &tunnel {
            Some(t) => t.local_endpoint(),
            None => (server, port),
        };
        let url = build_url(&host, port);
        let database = def
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                DbgmError::with_source("Cannot start tokio runtime for ClickHouse", e)
            })?;

        let mut client = clickhouse::Client::default().with_url(&url);
        if let Some(user) = &def.user {
            client = client.with_user(user);
        }
        if let Some(password) = &def.password {
            client = client.with_password(password);
        }
        client = client.with_database(&database);

        let test_client = client.clone();
        let db_clone = database.clone();
        runtime.block_on(async {
            test_client
                .query("SELECT 1")
                .execute()
                .await
                .map_err(|e| DbgmError::with_source("Cannot connect to ClickHouse", e))
        })?;

        Ok(Box::new(ClickHouseConnection {
            runtime,
            client,
            database: db_clone,
            ssh_tunnel: tunnel,
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
        let columns = &result.columns;
        let rows = &result.rows;
        let mut rows_affected: u64 = 0;
        if columns.is_empty() {
            rows_affected += rows.len() as u64;
        } else {
            (sink.on_recordset)(columns);
            for row in rows {
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
        let result = blocking_query(conn, "SELECT version() as version")?;
        let version = result
            .rows
            .first()
            .and_then(|r| get_str(r, "version"))
            .unwrap_or("unknown")
            .to_string();
        Ok(ServerVersion {
            version_text: Some(format!("ClickHouse {version}")),
            version,
        })
    }

    fn list_databases(&self, handle: &DbHandle) -> DbgmResult<Vec<DatabaseEntry>> {
        let conn = downcast(handle)?;
        let result = blocking_query(
            conn,
            "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'information_schema_ro', 'INFORMATION_SCHEMA')",
        )?;
        Ok(result
            .rows
            .iter()
            .filter_map(|r| get_str(r, "name").map(String::from))
            .map(|name| DatabaseEntry {
                name,
                size_on_disk: None,
                empty: None,
            })
            .collect())
    }

    fn analyse_full(&self, handle: &DbHandle, _server_version: &str) -> DbgmResult<DatabaseInfo> {
        analyse_clickhouse_full(handle)
    }

    fn analyse_single_table(
        &self,
        handle: &DbHandle,
        name: &NamedObjectInfo,
    ) -> DbgmResult<TableInfo> {
        analyse_clickhouse_table(handle, name)
    }

    fn write_table(
        &self,
        _handle: &DbHandle,
        _name: &NamedObjectInfo,
        _options: &WriteTableOptions,
    ) -> DbgmResult<()> {
        Err(DbgmError::new(
            "ClickHouse write_table streaming is not yet ported",
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn driver_ref() -> std::sync::Arc<dyn EngineDriver> {
    std::sync::Arc::new(ClickHouseDriver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_object_conditions() {
        let sql = substitute_condition(
            "where database = '#DATABASE#' and uuid =OBJECT_ID_CONDITION",
            "mydb",
            Some("abc-123"),
        );
        assert!(sql.contains("database = 'mydb'"));
        assert!(sql.contains("uuid = 'abc-123'") || sql.contains("uuid  = 'abc-123'"));

        let sql_all = substitute_condition(
            "where database = '#DATABASE#' and uuid =OBJECT_ID_CONDITION",
            "mydb",
            None,
        );
        assert!(sql_all.contains("database = 'mydb'"));
        assert!(sql_all.contains("is not null"));
    }

    #[test]
    fn extracts_nullable_data_type() {
        let (displayed, not_null) = extract_data_type("Nullable(String)");
        assert_eq!(displayed, "String");
        assert!(!not_null);

        let (displayed, not_null) = extract_data_type("UInt32");
        assert_eq!(displayed, "UInt32");
        assert!(not_null);

        let (displayed, not_null) = extract_data_type("Nullable(DateTime64(3))");
        assert_eq!(displayed, "DateTime64(3)");
        assert!(!not_null);
    }

    #[test]
    fn parses_comma_separated_primary_key() {
        let pk = build_primary_key_from_comma("id, tenant_id").unwrap();
        assert_eq!(pk.columns_constraint.columns.len(), 2);
        assert_eq!(pk.columns_constraint.columns[0].column_name, "id");
        assert_eq!(pk.columns_constraint.columns[1].column_name, "tenant_id");
    }

    #[test]
    fn returns_none_for_empty_primary_key() {
        assert!(build_primary_key_from_comma("").is_none());
        assert!(build_primary_key_from_comma("  ").is_none());
    }

    #[test]
    fn builds_column_info_from_clickhouse_row() {
        let row = serde_json::json!({
            "columnName": "created_at",
            "dataType": "Nullable(DateTime64(3))",
            "columnComment": "creation timestamp",
            "defaultValue": "now64()"
        });
        let info = get_column_info_from_ch(&row, "columnName");
        assert_eq!(info.column_name, "created_at");
        assert_eq!(info.data_type, "Nullable(DateTime64(3))");
        assert_eq!(info.displayed_data_type.as_deref(), Some("DateTime64(3)"));
        assert_eq!(info.not_null, Some(false));
        assert_eq!(info.default_value.as_deref(), Some("now64()"));
        assert_eq!(
            info.column_comment.as_deref(),
            Some("creation timestamp")
        );
    }

    #[test]
    fn builds_create_view_sql_correctly() {
        let sql = build_create_view_sql("my_view", "SELECT id, name FROM users");
        assert_eq!(
            sql,
            "CREATE VIEW \"my_view\"\nAS\nSELECT id, name FROM users"
        );
    }
}
