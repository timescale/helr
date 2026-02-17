//! State store contract: cursor, watermark, next_url per source.
//!
//! Implementations: in-memory (tests), SQLite, Redis, Postgres.
//!
//! **Single-writer assumption:** Only one Helr process should use a given state store (e.g. one SQLite file).
//! For multi-instance deployments use Redis or Postgres state backend.

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

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
            CREATE TABLE IF NOT EXISTS helr_state (
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
            let c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt =
                c.prepare("SELECT value FROM helr_state WHERE source_id = ?1 AND key = ?2")?;
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
            let c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            c.execute(
                "INSERT INTO helr_state (source_id, key, value, updated_at) VALUES (?1, ?2, ?3, ?4)
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
            let c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt =
                c.prepare("SELECT key FROM helr_state WHERE source_id = ?1 ORDER BY key")?;
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
            let c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            let mut stmt =
                c.prepare("SELECT DISTINCT source_id FROM helr_state ORDER BY source_id")?;
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
            let c = conn
                .lock()
                .map_err(|_| anyhow::anyhow!("state store lock poisoned"))?;
            c.execute("DELETE FROM helr_state WHERE source_id = ?1", [&source_id])?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking: {}", e))?
    }
}

/// Redis key prefix for state hashes: one hash per source, key = "helr:state:{source_id}".
const REDIS_STATE_PREFIX: &str = "helr:state:";

/// Redis-backed state store. One hash per source: HGET/HSET helr:state:{source_id} {key} {value}.
pub struct RedisStateStore {
    conn: Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>,
}

impl RedisStateStore {
    /// Connect to Redis at url (e.g. `redis://127.0.0.1/`).
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let client =
            redis::Client::open(url).map_err(|e| anyhow::anyhow!("redis client: {}", e))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| anyhow::anyhow!("redis connect: {}", e))?;
        Ok(Self {
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
        })
    }

    fn redis_key(source_id: &str) -> String {
        format!("{}{}", REDIS_STATE_PREFIX, source_id)
    }
}

#[async_trait]
#[allow(unused_imports)] // AsyncCommands used by get/set/list_keys/clear_source
impl StateStore for RedisStateStore {
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>> {
        use redis::AsyncCommands;
        let key_redis = Self::redis_key(source_id);
        let mut conn = self.conn.lock().await;
        let value: Option<String> = conn
            .hget(&key_redis, key)
            .await
            .map_err(|e| anyhow::anyhow!("redis: {}", e))?;
        Ok(value)
    }

    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;
        let key_redis = Self::redis_key(source_id);
        let mut conn = self.conn.lock().await;
        let _: usize = conn
            .hset(&key_redis, key, value)
            .await
            .map_err(|e| anyhow::anyhow!("redis: {}", e))?;
        Ok(())
    }

    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>> {
        use redis::AsyncCommands;
        let key_redis = Self::redis_key(source_id);
        let mut conn = self.conn.lock().await;
        let keys: Vec<String> = conn
            .hkeys(&key_redis)
            .await
            .map_err(|e| anyhow::anyhow!("redis: {}", e))?;
        Ok(keys)
    }

    async fn list_sources(&self) -> anyhow::Result<Vec<String>> {
        use redis::AsyncCommands;
        let mut conn = self.conn.lock().await;
        let pattern = format!("{}*", REDIS_STATE_PREFIX);
        let prefix_len = REDIS_STATE_PREFIX.len();
        let mut sources: Vec<String> = Vec::new();
        let mut cursor: isize = 0;
        loop {
            let (next, keys): (isize, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| anyhow::anyhow!("redis: {}", e))?;
            for k in keys {
                if k.len() > prefix_len {
                    sources.push(k[prefix_len..].to_string());
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        sources.sort();
        Ok(sources)
    }

    async fn clear_source(&self, source_id: &str) -> anyhow::Result<()> {
        use redis::AsyncCommands;
        let key_redis = Self::redis_key(source_id);
        let mut conn = self.conn.lock().await;
        let _: usize = conn
            .del(&key_redis)
            .await
            .map_err(|e| anyhow::anyhow!("redis: {}", e))?;
        Ok(())
    }
}

/// Postgres-backed state store. Same schema as SQLite: (source_id, key, value, updated_at).
pub struct PostgresStateStore {
    client: tokio_postgres::Client,
    _connection: tokio::task::JoinHandle<()>,
}

impl PostgresStateStore {
    /// Connect to Postgres at url (e.g. `postgres://user:pass@localhost/dbname`). Creates helr_state table if missing.
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("postgres connect: {}", e))?;
        let _connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres connection error: {}", e);
            }
        });
        client
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS helr_state (
                    source_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY (source_id, key)
                );
                "#,
            )
            .await
            .map_err(|e| anyhow::anyhow!("postgres init: {}", e))?;
        Ok(Self {
            client,
            _connection,
        })
    }
}

#[async_trait]
impl StateStore for PostgresStateStore {
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>> {
        let row: Option<tokio_postgres::Row> = self
            .client
            .query_opt(
                "SELECT value FROM helr_state WHERE source_id = $1 AND key = $2",
                &[&source_id, &key],
            )
            .await
            .map_err(|e| anyhow::anyhow!("postgres: {}", e))?;
        Ok(row.map(|r: tokio_postgres::Row| r.get::<_, String>(0)))
    }

    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()> {
        let updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        self.client
            .execute(
                "INSERT INTO helr_state (source_id, key, value, updated_at) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (source_id, key) DO UPDATE SET value = $3, updated_at = $4",
                &[&source_id, &key, &value, &updated_at],
            )
            .await
            .map_err(|e| anyhow::anyhow!("postgres: {}", e))?;
        Ok(())
    }

    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT key FROM helr_state WHERE source_id = $1 ORDER BY key",
                &[&source_id],
            )
            .await
            .map_err(|e| anyhow::anyhow!("postgres: {}", e))?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn list_sources(&self) -> anyhow::Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT DISTINCT source_id FROM helr_state ORDER BY source_id",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("postgres: {}", e))?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn clear_source(&self, source_id: &str) -> anyhow::Result<()> {
        self.client
            .execute("DELETE FROM helr_state WHERE source_id = $1", &[&source_id])
            .await
            .map_err(|e| anyhow::anyhow!("postgres: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_state_store_get_set_list() {
        let dir = std::env::temp_dir().join("helr_state_test");
        let _ = std::fs::remove_file(&dir);
        let store = SqliteStateStore::open(&dir).unwrap();
        assert!(store.get("s1", "cursor").await.unwrap().is_none());
        store.set("s1", "cursor", "abc123").await.unwrap();
        assert_eq!(
            store.get("s1", "cursor").await.unwrap(),
            Some("abc123".into())
        );
        store
            .set("s1", "watermark", "2024-01-01T00:00:00Z")
            .await
            .unwrap();
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
