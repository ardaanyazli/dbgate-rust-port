//! DbGate core — the Rust backend foundation for the DbGate → Rust/Tauri v2
//! port.
//!
//! This crate provides:
//!
//! - The DbGate data model ([`dbinfo`], [`query`], [`connection`]).
//! - The [`driver`] trait — the engine-driver contract every database
//!   engine implements (Rust port of `packages/types/engines.d.ts`).
//! - A [`registry`] of registered drivers.
//! - A shared error type ([`error`]) using the `DBGM-00000` code convention.
//!
//! Drivers live in `dbgate-core::drivers` and the Tauri application lives in
//! the `dbgate-app` workspace member.

pub mod connection;
pub mod dbinfo;
pub mod driver;
pub mod drivers;
pub mod error;
pub mod query;
pub mod query_splitter;
pub mod registry;
pub mod security;

pub use error::{DbgmError, DbgmResult};
