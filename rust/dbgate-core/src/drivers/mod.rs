//! Built-in engine drivers.
//!
//! Each submodule port a corresponding `dbgate-plugin-*` backend to a native
//! Rust database crate. New drivers are added one database at a time;
//! `sqlite` is the reference implementation every other driver follows.

pub mod clickhouse;
pub mod mssql;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
