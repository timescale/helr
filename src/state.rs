//! State store contract: cursor, watermark, next_url per source.
//!
//! Implementations: in-memory (tests), SQLite (v0.1), Redis/Postgres (later).

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// State store: get/set/list keys per source. Must be Send + Sync for use across tasks.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Get value for (source_id, key). Returns None if missing.
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>>;

    /// Set value for (source_id, key). Overwrites if present.
    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()>;

    /// List all keys for a source (e.g. cursor, watermark_ts, next_url).
    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>>;

    /// List all source_ids that have state.
    async fn list_sources(&self) -> anyhow::Result<Vec<String>>;

    /// Remove all state for a source (e.g. for reset).
    async fn clear_source(&self, source_id: &str) -> anyhow::Result<()>;
}

/// In-memory state store for tests and default when no path is configured.
#[derive(Default)]
pub struct MemoryStateStore {
    inner: tokio::sync::RwLock<HashMap<String, HashMap<String, String>>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>> {
        let g = self.inner.read().await;
        Ok(g.get(source_id).and_then(|m| m.get(key).cloned()))
    }

    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()> {
        let mut g = self.inner.write().await;
        g.entry(source_id.to_string())
            .or_default()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>> {
        let g = self.inner.read().await;
        Ok(g.get(source_id)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn list_sources(&self) -> anyhow::Result<Vec<String>> {
        let g = self.inner.read().await;
        Ok(g.keys().cloned().collect())
    }

    async fn clear_source(&self, source_id: &str) -> anyhow::Result<()> {
        let mut g = self.inner.write().await;
        g.remove(source_id);
        Ok(())
    }
}

/// SQLite-backed state store. Table: (source_id, key, value, updated_at).
/// Uses spawn_blocking so rusqlite's sync API doesn't block the async runtime.
pub struct SqliteStateStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteStateStore {
    /// Open or create DB at path; creates state table if missing.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| anyhow::anyhow!("open sqlite {:?}: {}", path, e))?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS hel_state (
                source_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (source_id, key)
            );
            "#,
        )
        .map_err(|e| anyhow::anyhow!("init sqlite: {}", e))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl StateStore for SqliteStateStore {
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn.clone();
        let source_id = source_id.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt = c.prepare(
                "SELECT value FROM hel_state WHERE source_id = ?1 AND key = ?2",
            )?;
            let mut rows = stmt.query([&source_id, &key])?;
            if let Some(row) = rows.next()? {
                let value: String = row.get(0)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }

    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()> {
        let conn = self.conn.clone();
        let source_id = source_id.to_string();
        let key = key.to_string();
        let value = value.to_string();
        let updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            c.execute(
                "INSERT INTO hel_state (source_id, key, value, updated_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (source_id, key) DO UPDATE SET value = ?3, updated_at = ?4",
                rusqlite::params![&source_id, &key, &value, updated_at],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }

    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.clone();
        let source_id = source_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt = c.prepare("SELECT key FROM hel_state WHERE source_id = ?1 ORDER BY key")?;
            let rows = stmt.query_map([&source_id], |row| row.get(0))?;
            let keys: Result<Vec<String>, _> = rows.collect();
            Ok(keys?)
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }

    async fn list_sources(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt = c.prepare("SELECT DISTINCT source_id FROM hel_state ORDER BY source_id")?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            let ids: Result<Vec<String>, _> = rows.collect();
            Ok(ids?)
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }

    async fn clear_source(&self, source_id: &str) -> anyhow::Result<()> {
        let conn = self.conn.clone();
        let source_id = source_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            c.execute("DELETE FROM hel_state WHERE source_id = ?1", [&source_id])?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_state_store_get_set_list() {
        let dir = std::env::temp_dir().join("hel_state_test");
        let _ = std::fs::remove_file(&dir);
        let store = SqliteStateStore::open(&dir).unwrap();
        assert!(store.get("s1", "cursor").await.unwrap().is_none());
        store.set("s1", "cursor", "abc123").await.unwrap();
        assert_eq!(store.get("s1", "cursor").await.unwrap(), Some("abc123".into()));
        store.set("s1", "watermark", "2024-01-01T00:00:00Z").await.unwrap();
        let keys = store.list_keys("s1").await.unwrap();
        assert!(keys.contains(&"cursor".to_string()));
        assert!(keys.contains(&"watermark".to_string()));
        let sources = store.list_sources().await.unwrap();
        assert_eq!(sources, vec!["s1"]);
        store.clear_source("s1").await.unwrap();
        assert!(store.list_keys("s1").await.unwrap().is_empty());
        assert!(store.get("s1", "cursor").await.unwrap().is_none());
        let _ = std::fs::remove_file(&dir);
    }
}
