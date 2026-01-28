//! State store contract: cursor, watermark, next_url per source.
//!
//! Implementations: in-memory (tests), SQLite (v0.1), Redis/Postgres (later).

#![allow(dead_code)] // used when implementing poll loop

use async_trait::async_trait;
use std::collections::HashMap;

/// State store: get/set/list keys per source. Must be Send + Sync for use across tasks.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Get value for (source_id, key). Returns None if missing.
    async fn get(&self, source_id: &str, key: &str) -> anyhow::Result<Option<String>>;

    /// Set value for (source_id, key). Overwrites if present.
    async fn set(&self, source_id: &str, key: &str, value: &str) -> anyhow::Result<()>;

    /// List all keys for a source (e.g. cursor, watermark_ts, next_url).
    async fn list_keys(&self, source_id: &str) -> anyhow::Result<Vec<String>>;
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
}
