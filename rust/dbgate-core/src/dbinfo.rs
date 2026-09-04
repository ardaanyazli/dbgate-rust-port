//! DbGate database-object model.
//!
//! Rust port of `packages/types/dbinfo.d.ts`. These types describe the
//! in-memory "dbinfo" structure that the app uses to represent a connected
//! database: tables, views, procedures, functions, triggers, columns,
//! primary/foreign keys, indexes, uniques, checks.

use serde::{Deserialize, Serialize};

/// A named object (table / view / column / etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedObjectInfo {
    pub pure_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
}

/// Reference to a column, possibly within an index / key definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnReference {
    pub column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_column_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_included_column: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_descending: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    SortingKey,
    Index,
    Check,
    Unique,
}

/// Base for all constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_name: Option<String>,
    pub constraint_type: ConstraintType,
}

/// Constraint that references one or more columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnsConstraintInfo {
    pub constraint: ConstraintInfo,
    pub columns: Vec<ColumnReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyInfo {
    pub columns_constraint: ColumnsConstraintInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub columns_constraint: ColumnsConstraintInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_schema_name: Option<String>,
    pub ref_table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_action: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub columns_constraint: ColumnsConstraintInfo,
    pub is_unique: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueInfo {
    pub columns_constraint: ColumnsConstraintInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckInfo {
    pub constraint: ConstraintInfo,
    pub definition: String,
}

/// A single table/view column.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub column_name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub not_null: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub auto_increment: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub displayed_data_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_persisted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_sparse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_update_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_unsigned: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_zerofill: Option<bool>,
}

/// Common fields for any database object (table/view/procedure/...).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseObjectInfo {
    pub pure_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modify_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_type_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_comment: Option<String>,
}

/// SQL-defined object (view/procedure/function/trigger) carrying its DDL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlObjectInfo {
    pub object: DatabaseObjectInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub requires_format: Option<bool>,
}

/// A table (full metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub object: DatabaseObjectInfo,
    pub columns: Vec<ColumnInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<PrimaryKeyInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sorting_key: Option<ColumnsConstraintInfo>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub foreign_keys: Option<Vec<ForeignKeyInfo>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dependencies: Option<Vec<ForeignKeyInfo>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub indexes: Option<Vec<IndexInfo>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub uniques: Option<Vec<UniqueInfo>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub checks: Option<Vec<CheckInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_row_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_engine: Option<String>,
}

/// A collection (Mongo / Cassandra style).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub object: DatabaseObjectInfo,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub known_columns: Option<Vec<ColumnInfo>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique_key: Option<Vec<ColumnReference>>,
}

/// A view (columns + create SQL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub object: SqlObjectInfo,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Return,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterInfo {
    pub parameter_name: String,
    pub data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter_mode: Option<ParameterMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallableObjectInfo {
    pub object: SqlObjectInfo,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parameters: Option<Vec<ParameterInfo>>,
}

/// Stored procedure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureInfo {
    pub callable: CallableObjectInfo,
}

/// Stored function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub callable: CallableObjectInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_type: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TriggerTiming {
    Before,
    After,
    #[serde(rename = "INSTEAD OF")]
    InsteadOf,
    #[serde(rename = "BEFORE EACH ROW")]
    BeforeEachRow,
    #[serde(rename = "AFTER EACH ROW")]
    AfterEachRow,
    #[serde(rename = "AFTER STATEMENT")]
    AfterStatement,
    #[serde(rename = "BEFORE STATEMENT")]
    BeforeStatement,
    #[serde(rename = "AFTER EVENT")]
    AfterEvent,
    #[serde(rename = "BEFORE EVENT")]
    BeforeEvent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TriggerEventType {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub object: SqlObjectInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_timing: Option<TriggerTiming>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<TriggerEventType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub schema_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub is_default: Option<bool>,
}

/// Aggregate of all object types in a database ("DatabaseInfo").
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub tables: Vec<TableInfo>,
    pub collections: Vec<CollectionInfo>,
    pub views: Vec<ViewInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matviews: Option<Vec<ViewInfo>>,
    pub procedures: Vec<ProcedureInfo>,
    pub functions: Vec<FunctionInfo>,
    pub triggers: Vec<TriggerInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
}
