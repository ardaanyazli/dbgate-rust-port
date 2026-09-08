# DbGate → Rust + Tauri v2: Backend Migration Blueprint

This document is the decision-complete roadmap for rewriting DbGate's Node.js
backend (`packages/api` + plugin drivers) in Rust, keeping the Svelte 4
frontend (`packages/web`) running unchanged inside a Tauri v2 webview.

## What already exists (this repo, `rust/`)

| Crate | Purpose | Status |
|---|---|---|
| `dbgate-core` | Data model (`dbinfo`, `query`, `connection`), `EngineDriver` trait, driver registry, `DBGM-00000` error type | ✅ Compiles, tested |
| `dbgate-core::drivers::sqlite` | **Reference** SQLite driver on `rusqlite` (connect/query/stream/version/analyse) | ✅ Compiles, 3 tests pass |
| `dbgate-app` | Tauri v2 shell + `run_query`/`open_connection`/`analyse_full` commands | ✅ Compiles |

The SQLite driver is the **template**. Every other engine follows the same
shape: a `struct` implementing `EngineDriver`, storing its native client in a
thread-safe `DbHandle`, and mapping driver rows to the DbGate JSON value model.

## How to add a new driver (follow the sqlite template)

1. Add a module under `dbgate-core/src/drivers/<engine>.rs`.
2. Implement `EngineDriver`:
   - `connect`: turn `ConnectionDefinition` into `DbHandle` (wrap the native
     client, adding `Mutex`/pooling where the client is not `Sync`).
   - `query`: prepare + collect `QueryResult { rows, columns }`.
   - `stream`: split SQL, run each statement transactionally, emit via
     `StreamSink` (recordset/row/info/done).
   - `get_version`, `list_databases`, `analyse_full`, `analyse_single_table`.
3. `analyse_full` must populate `DatabaseInfo` (tables, views, procedures,
   functions, triggers, columns, PK/FK/index/unique/check) by querying the
   engine's system catalog.
4. Register in `DbgmState::new()` (dbgate-app/src/lib.rs) via `drivers.register(...)`.
5. Add unit tests for query/version/analyse, following the sqlite test module.
6. Gate UDP/TCP networking drivers behind the driver's async runtime.

## Engine → Rust crate mapping (the 15 remaining)

| # | Engine | Node plugin | Rust crate | Notes |
|---|---|---|---|---|
| 1 | PostgreSQL | dbgate-plugin-postgres | `postgres` (tokio-postgres) | Native async, `Send+Sync` handles; no Mutex needed. |
| 2 | MySQL | dbgate-plugin-mysql | `mysql` | Async `Conn`/pool. |
| 3 | MariaDB | dbgate-plugin-mysql | `mysql` (MariaDB wire) | Same crate, different flags/dialect. |
| 4 | SQL Server | dbgate-plugin-mssql | `tiberius` | TDS protocol. |
| 5 | Oracle | dbgate-plugin-oracle | `oracledb` | ✅ **DONE** — thin/blocking, Send+Sync. |
| 6 | MongoDB | dbgate-plugin-mongo | `mongodb` | Document model, not SQL. |
| 7 | Redis | dbgate-plugin-redis | `redis` | Key/command model, not SQL. |
| 8 | SQLite | dbgate-plugin-sqlite | `rusqlite` | ✅ **DONE** (reference). |
| 9 | DuckDB | dbgate-plugin-duckdb | `duckdb` | QL / arrow. |
| 10 | ClickHouse | dbgate-plugin-clickhouse | `clickhouse` | HTTP/TCK. |
| 11 | Cassandra | dbgate-plugin-cassandra | `scylla` | ✅ **DONE** — CQL native protocol via scylla 1.8.0. |
| 12 | Firebird | dbgate-plugin-firebird | `rsfbclient` | ✅ **DONE** — `pure_rust`, blocking `&mut self` → `Mutex<SimpleConnection>`. |
| 13 | CockroachDB | (postgres plugin) | `postgres` | Postgres wire + cluster queries. |
| 14 | Redshift (Premium) | dbgate-plugin-postgres | `postgres` | Postgres wire. |
| 15 | CosmosDB / Firestore (Premium) | REST/HTTP | reqwest | REST/OData drivers already in `packages/rest`. |
| 16 | libSQL/Turso (Premium) | dbgate-plugin-sqlite (libsql) | `libsql` | **Reuses the SQLite driver shape.** |

**Non-SQL engines (Mongo, Redis, Cassandra)** do not implement the SQL path;
they use the collection/key branches of `EngineDriver`. Their drivers are the
most work because data access differs fundamentally.

**File-format plugins** (csv, excel, dbf, xml, json/ndjson) are not database
engines; they stay as `packages/datalib` logic. Decide later whether to port
them or bridge to the sidecar. They are out of scope for the backend rewrite.

## Cross-cutting pieces still to port

After the drivers, the remaining Node backend modules need Rust equivalents:

| Node module | Rust crate / approach | Status |
|---|---|---|
| SQL splitter (`dbgate-query-splitter`) | hand-rolled splitter extracted to `query_splitter.rs` | ✅ done (`run_script` splits + transaction window) |
| SQL tree / query designer (`dbgate-sqltree`) | stays JS-side, consumed via `plugins_script` eval-loop | frontend-only, no Rust port |
| SQL dumper / DDL generation (`dbgate-tools`) | custom, per-dialect | pending |
| Filter parser & data library (`dbgate-datalib`) | `polars` / `arrow` | pending |
| SSH tunneling (`ssh2`) | `openssh` 0.11.6 (request_port_forward) | pending (workstream B) |
| Config persistence (`config-root.json`, `settings.json`) | `connections.jsonl` canonical + `keyring` 4.2.0 credential vault | ✅ done (secrets) |
| Native backup/restore CLI wrappers | `std::process::Command` (`mysqldump`/`pg_dump`) | pending (workstream C) |
| HTTP/REST API (web mode) | `axum` (only needed for web mode) | skipped (M6 out of scope) |
| Auth / JWT / license | `jsonwebtoken`, permissive/classic licensing | out of scope |

## Delivery roadmap (recommended order)

1. **Milestone 1 — Foundation** ✅ *done in this repo*: workspace, `dbgate-core`,
   `EngineDriver` trait, SQLite reference driver, Tauri v2 shell.
2. **Milestone 2 — Native-lib/embedded engines** (lowest friction): DuckDB,
   libSQL/Turso. These mirror SQLite almost exactly.
3. **Milestone 3 — Network SQL engines**: Postgres → MySQL/MariaDB → SQL Server
   → ClickHouse → Oracle → Firebird. Each is the same `EngineDriver` shape;
   only the catalog queries and value mapping differ.
4. **Milestone 4 — Non-SQL engines**: MongoDB → Redis → Cassandra.
   Cassandra driver **done** (scylla 1.8.0).
5. **Milestone 5 — Cross-cutting**: SQL splitter + keyring credential vault
   **done** (query_splitter.rs generalizes sqlite's hand-rolled splitter into the
   generic `run_script`; connections store secrets as `keyring:<conid>`
   placeholders with plaintext fallback). SSH tunnels, dump/restore pending.
6. **Milestone 6 — Web mode**: axum HTTP server for the browser/Docker target.

Each milestone is shippable and independently testable. Do **not** attempt to
port all drivers in one pass — that is how broken, unverifiable code is
produced.

**Milestone 6 (Web mode / axum) is explicitly out of scope** and skipped
entirely. The Rust backend targets the desktop (Tauri v2) application only.

## Working workflow (commits & pushes)

- **Commit after every driver implementation** (each new `dbgate-core/src/drivers/<engine>.rs`
  plus its registration and tests) — one focused commit per driver.
- **Push to `origin` after each milestone completes** (Milestones 1-5; 6 is skipped),
  not after every individual driver.
- Milestone 1 is committed and pushed. Milestone 3 (network SQL engines) is
  **complete and pushed to origin** — SQL Server, PostgreSQL, MySQL/MariaDB,
  ClickHouse, Oracle, and Firebird drivers are all done.
- Milestone 4 (Cassandra) is committed and pushed. Milestone 5 workstreams A
  (SQL splitter) and D (keyring credential vault) are committed and pushed;
  workstreams C (backup/restore) and B (SSH tunnels) remain.

## Verification discipline (carried into every driver)

- `cargo test` — unit tests per driver (query, version, analyse).
- `cargo clippy` — must be clean (no warnings).
- Use `DBGM-00000` for all new error codes; never invent numbered `DBGM-xxxxx`.
- Keep the Svelte frontend untouched; the Tauri webview loads
  `packages/web/public` directly.
