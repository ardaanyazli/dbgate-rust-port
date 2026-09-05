//! Connection CRUD bridge routes.
//!
//! Stores connections in a JSON-lines file exactly like the Node
//! `JsonLinesDatabase` (`packages/api/src/utility/JsonLinesDatabase.js`):
//! one raw JSON object per line, keys kept verbatim as sent by the
//! frontend (camelCase: `databaseFile`, `databaseUrl`, `authType`, ...).
//! Round-tripping raw values preserves field fidelity for the Svelte UI.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::DbgmState;

/// JSON-lines store with the same semantics as the Node datastore.
pub struct ConnectionsStore {
    path: PathBuf,
}

impl ConnectionsStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn default_path(state: &DbgmState) -> PathBuf {
        state.data_dir.join("connections.jsonl")
    }

    fn load(&self) -> Result<Vec<Value>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|e| io_err(&self.path, e))?;
        let reader = BufReader::new(file);
        let mut items = Vec::new();
        for line in reader.lines() {
            let line = line.map_err(|e| io_err(&self.path, e))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(trimmed).map_err(|e| format!("DBGM-00000: Invalid JSON in {}: {e}", self.path.display()))?;
            items.push(value);
        }
        Ok(items)
    }

    fn save(&self, items: &[Value]) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("DBGM-00000: Cannot create {}: {e}", parent.display()))?;
        }
        let mut file =
            fs::File::create(&self.path).map_err(|e| format!("DBGM-00000: Cannot write {}: {e}", self.path.display()))?;
        for item in items {
            let line =
                serde_json::to_string(item).map_err(|e| format!("DBGM-00000: Cannot serialize connection: {e}"))?;
            writeln!(file, "{line}").map_err(|e| io_err(&self.path, e))?;
        }
        Ok(())
    }

    pub fn find(&self) -> Result<Vec<Value>, String> {
        self.load()
    }

    pub fn get(&self, id: &str) -> Result<Value, String> {
        Ok(self
            .load()?
            .into_iter()
            .find(|x| x.get("_id").and_then(Value::as_str) == Some(id))
            .unwrap_or(Value::Null))
    }

    fn new_id() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{nanos:x}")
    }

    /// Insert a new record, generating `_id` when absent. Mirrors JS `insert()`.
    pub fn insert(&self, mut obj: Value) -> Result<Value, String> {
        if obj.get("_id").map(Value::is_null).unwrap_or(true) {
            if let Some(id) = obj.get("_id").and_then(Value::as_str) {
                if self.get(id)?.is_object() && !id.is_empty() {
                    return Err(format!("DBGM-00000: Cannot insert duplicate ID {id} into {}", self.path.display()));
                }
            }
        }
        if !obj.get("_id").and_then(Value::as_str).is_some_and(|s| !s.is_empty()) {
            obj["_id"] = json!(Self::new_id());
        }
        let mut items = self.load()?;
        items.push(obj.clone());
        self.save(&items)?;
        Ok(obj)
    }

    /// Replace whole record by `_id`. Mirrors JS `update()`.
    pub fn update(&self, obj: &Value) -> Result<Value, String> {
        let id = obj
            .get("_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("DBGM-00000: Cannot update connection without _id in {}", self.path.display()))?
            .to_string();
        let mut items = self.load()?;
        let mut replaced = false;
        for item in items.iter_mut() {
            if item.get("_id").and_then(Value::as_str) == Some(&id) {
                *item = obj.clone();
                replaced = true;
            }
        }
        if !replaced {
            items.push(obj.clone());
        }
        self.save(&items)?;
        Ok(obj.clone())
    }

    /// Shallow-merge values into the record with `_id`. Mirrors JS `patch()`.
    pub fn patch(&self, id: &str, values: &Value) -> Result<Value, String> {
        let mut items = self.load()?;
        let mut patched: Option<Value> = None;
        for item in items.iter_mut() {
            if item.get("_id").and_then(Value::as_str) == Some(id) {
                if let (Some(target), Some(source)) = (item.as_object_mut(), values.as_object()) {
                    for (k, v) in source {
                        target.insert(k.clone(), v.clone());
                    }
                }
                patched = Some(item.clone());
            }
        }
        let patched = patched.ok_or_else(|| format!("DBGM-00000: Cannot patch unknown connection {id} in {}", self.path.display()))?;
        self.save(&items)?;
        Ok(patched)
    }

    pub fn remove(&self, id: &str) -> Result<(), String> {
        let mut items = self.load()?;
        let before = items.len();
        items.retain(|x| x.get("_id").and_then(Value::as_str) != Some(id));
        if items.len() == before {
            return Ok(());
        }
        self.save(&items)
    }
}

fn io_err(path: &Path, e: std::io::Error) -> String {
    format!("DBGM-00000: IO error on {}: {e}", path.display())
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/// `connections_list` — all saved connections.
pub fn list(state: &DbgmState, _args: Value) -> Result<Value, String> {
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    let items = store.find()?;
    Ok(Value::Array(items))
}

/// `connections_get` — single connection by `_id` (null when absent).
pub fn get(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| "DBGM-00000: connections_get missing conid".to_string())?;
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    store.get(conid)
}

/// `connections_save` — insert (new `_id`) or replace (existing `_id`).
pub fn save(state: &DbgmState, args: Value) -> Result<Value, String> {
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    if args.get("_id").and_then(Value::as_str).is_some_and(|s| !s.is_empty()) {
        store.update(&args)
    } else {
        store.insert(args)
    }
}

/// `connections_update` — shallow-merge `values` into the record with `_id`.
pub fn update(state: &DbgmState, args: Value) -> Result<Value, String> {
    let id = args
        .get("_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "DBGM-00000: connections_update missing _id".to_string())?;
    let values = args.get("values").cloned().unwrap_or_else(|| json!({}));
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    store.patch(id, &values)
}

/// `connections_update_database` — upsert a `{ name, ... }` entry into the
/// `databases` array of the connection with `conid`.
pub fn update_database(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .ok_or_else(|| "DBGM-00000: connections_update-database missing conid".to_string())?;
    let database = args
        .get("database")
        .and_then(Value::as_str)
        .ok_or_else(|| "DBGM-00000: connections_update-database missing database".to_string())?;
    let values = args.get("values").cloned().unwrap_or_else(|| json!({}));

    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    let current = store.get(conid)?;
    if current.is_null() {
        return Err(format!("DBGM-00000: Unknown connection {conid}"));
    }
    let mut databases = current
        .get("databases")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut entry = json!({ "name": database });
    if let Value::Object(map) = &mut entry {
        if let Value::Object(src) = &values {
            for (k, v) in src {
                map.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(existing) = databases.iter_mut().find(|d| d.get("name").and_then(Value::as_str) == Some(database)) {
        *existing = entry;
    } else {
        databases.push(entry);
    }
    store.patch(conid, &json!({ "databases": databases }))
}

/// `connections_delete` — remove connection by `_id`.
pub fn delete(state: &DbgmState, args: Value) -> Result<Value, String> {
    let conid = args
        .get("conid")
        .and_then(Value::as_str)
        .or_else(|| args.get("_id").and_then(Value::as_str))
        .ok_or_else(|| "DBGM-00000: connections_delete missing conid".to_string())?;
    let store = ConnectionsStore::new(ConnectionsStore::default_path(state));
    let current = store.get(conid)?;
    if current.is_null() {
        return Ok(Value::Null);
    }
    store.remove(conid)?;
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(tag: &str) -> (ConnectionsStore, PathBuf) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dbgate-connections-{tag}-{nanos}"));
        let store = ConnectionsStore::new(dir.join("connections.jsonl"));
        (store, dir)
    }

    #[test]
    fn round_trip_list_get_save_update() {
        let (store, dir) = temp_store("roundtrip");

        let conn = json!({ "engine": "sqlite@dbgate-plugin-sqlite", "name": "test", "databaseFile": "/tmp/x.sqlite" });
        let saved = store.insert(conn).unwrap();
        let id = saved["_id"].as_str().unwrap().to_string();
        assert!(!id.is_empty());

        let found: Vec<Value> = store.find().unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0]["_id"].as_str().unwrap(), id);

        let got = store.get(&id).unwrap();
        assert_eq!(got["databaseFile"].as_str().unwrap(), "/tmp/x.sqlite");

        let patched = store.patch(&id, &json!({ "port": 5432 })).unwrap();
        assert_eq!(patched["port"].as_u64().unwrap(), 5432);
        assert_eq!(patched["databaseFile"].as_str().unwrap(), "/tmp/x.sqlite");

        let got2 = store.get(&id).unwrap();
        assert_eq!(got2["port"].as_u64().unwrap(), 5432);

        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn insert_generates_unique_ids() {
        let (store, dir) = temp_store("unique");
        let a = store.insert(json!({ "name": "a" })).unwrap();
        let b = store.insert(json!({ "name": "b" })).unwrap();
        let id_a = a["_id"].as_str().unwrap().to_string();
        let id_b = b["_id"].as_str().unwrap().to_string();
        assert_ne!(id_a, id_b);
        assert_eq!(store.find().unwrap().len(), 2);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_database_route_upserts() {
        let (store, dir) = temp_store("dbupr");
        let saved = store
            .insert(json!({ "name": "c", "engine": "mysql@dbgate-plugin-mysql" }))
            .unwrap();
        let id = saved["_id"].as_str().unwrap().to_string();
        drop(store);

        let state = DbgmState::with_data_dir(dir.clone());
        let res = update_database(
            &state,
            json!({ "conid": id, "database": "orders", "values": { "readOnly": true } }),
        )
        .unwrap();
        assert_eq!(res["databases"][0]["name"].as_str().unwrap(), "orders");
        assert!(res["databases"][0]["readOnly"].as_bool().unwrap());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_route_inserts_then_updates() {
        let (store, dir) = temp_store("saver");
        let saved = store.insert(json!({ "name": "x" })).unwrap();
        let id = saved["_id"].as_str().unwrap().to_string();
        drop(store);

        let state = DbgmState::with_data_dir(dir.clone());
        let upd = save(
            &state,
            json!({ "_id": id, "name": "x2", "engine": "sqlite@dbgate-plugin-sqlite" }),
        )
        .unwrap();
        assert_eq!(upd["name"].as_str().unwrap(), "x2");
        let arr = list(&state, json!({})).unwrap();
        assert_eq!(arr.as_array().unwrap().len(), 1);
        assert_eq!(arr[0]["name"].as_str().unwrap(), "x2");
        assert_eq!(arr[0]["_id"].as_str().unwrap(), id);

        let removed = delete(&state, json!({ "conid": id })).unwrap();
        assert_eq!(removed["_id"].as_str().unwrap(), id);
        let arr2 = list(&state, json!({})).unwrap();
        assert_eq!(arr2.as_array().unwrap().len(), 0);

        let _ = fs::remove_dir_all(&dir);
    }
}