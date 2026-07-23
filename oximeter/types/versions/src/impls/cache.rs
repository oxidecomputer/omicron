use std::collections::HashMap;
use std::sync::Arc;

use crate::v1::types::FieldSet;

pub struct FieldSetCache {
    fieldsets: HashMap<Box<str>, CachedFieldSet>,
    stats: CacheStats,
}

struct CachedFieldSet {
    fieldset: Arc<FieldSet>,
    seen: bool,
}

#[derive(Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

impl FieldSetCache {
    pub fn new() -> Self {
        Self { fieldsets: HashMap::new(), stats: CacheStats::default() }
    }

    pub(crate) fn get(
        &mut self,
        key: &str,
    ) -> Result<Arc<FieldSet>, serde_json::Error> {
        if let Some(cached_fieldset) = self.fieldsets.get_mut(key) {
            self.stats.hits += 1;
            cached_fieldset.seen = true;
            Ok(Arc::clone(&cached_fieldset.fieldset))
        } else {
            self.stats.misses += 1;
            let fieldset = Arc::new(serde_json::from_str::<FieldSet>(&key)?);
            self.fieldsets.insert(
                Box::from(key),
                CachedFieldSet { fieldset: Arc::clone(&fieldset), seen: true },
            );
            Ok(fieldset)
        }
    }

    /// Remove stale entries from the cache.
    ///
    /// To limit the growth of the cache as series churn, we mark each entry as `seen` when it's fetched or created via `get()`. After each collection, the collection task calls `purge()`, removing any entries not `seen` since the previous call to `purge`. This limits the size of the cache to the unique `FieldSet` values seen during a single collection.
    ///
    /// Returns cache stats on purge.
    pub fn purge(&mut self) -> CacheStats {
        self.fieldsets.retain(|_, cached_fieldset| {
            let seen = cached_fieldset.seen;
            cached_fieldset.seen = false;
            seen
        });
        std::mem::take(&mut self.stats)
    }
}
