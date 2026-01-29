//! LRU deduplication: track last N event IDs per source and skip emitting duplicates.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Per-source LRU cache of seen event IDs. When capacity is exceeded, oldest IDs are evicted.
pub struct LruDedupe {
    capacity: u64,
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl LruDedupe {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            seen: HashSet::new(),
        }
    }

    /// Returns true if `id` was already seen (duplicate); false if new. When false, the ID is recorded.
    pub fn seen_and_add(&mut self, id: String) -> bool {
        if id.is_empty() {
            return false; // treat empty ID as new so we emit
        }
        if self.seen.contains(&id) {
            return true; // duplicate
        }
        self.seen.insert(id.clone());
        self.order.push_back(id);
        while self.order.len() as u64 > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        false // new
    }
}

/// Store of per-source LRU dedupes. Shared across poll ticks.
pub type DedupeStore = Arc<RwLock<std::collections::HashMap<String, LruDedupe>>>;

/// Create an empty dedupe store.
pub fn new_dedupe_store() -> DedupeStore {
    Arc::new(RwLock::new(std::collections::HashMap::new()))
}

/// Returns true if `id` was already seen (duplicate) for this source; false if new (and records it).
/// Creates the per-source LRU on first use.
pub async fn seen_and_add(
    store: &DedupeStore,
    source_id: &str,
    id: String,
    capacity: u64,
) -> bool {
    let mut g = store.write().await;
    let dedupe = g
        .entry(source_id.to_string())
        .or_insert_with(|| LruDedupe::new(capacity));
    dedupe.seen_and_add(id)
}
