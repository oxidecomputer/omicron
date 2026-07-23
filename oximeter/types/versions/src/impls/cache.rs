// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::sync::Arc;

use crate::v1::types::FieldSet;

/// Cache mapping raw field JSON to parsed fieldsets.
///
/// Oximeter receives the full set of field labels and values for each
/// sample. This fieldset metadata may repeat across samples, both within
/// and between collections, for a given collection task. To avoid the work
/// of re-parsing fieldsets that we've already encountered, we cache the
/// mapping from raw JSON strings to parsed fieldsets, and only parse
/// incoming fieldset strings on cache misses. To avoid the fieldset cache
/// growing too large, we only retain cache entries that were accessed on
/// the previous run of the collection task.
///
/// Note: the hit rate of the cache depends on producers serializing
/// fieldsets in a consistent field order. Producers that use `FieldSet`
/// to represent fields get this property for free, since fields are
/// collected into a `BTreeMap`, which orders fields alphabetically.
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
    /// To limit the growth of the cache as series churn, we mark each entry
    /// as `seen` when it's fetched or created via `get()`. After each
    /// collection, the collection task calls `purge()`, removing any entries
    /// not `seen` since the previous call to `purge`. This limits the size of
    /// the cache to the unique `FieldSet` values seen during a single
    /// collection.
    ///
    /// Note: this is a simplified version of the scrape cache used in
    /// Prometheus. Unlike the Prometheus implementation, we only retain
    /// fieldsets accessed on the previous collection, rather than retaining for
    /// multiple generations. If we find that a given fieldset may occur every
    /// Nth collection, but not every collection, we'll consider adopting a
    /// model like Prometheus's, which retains cached fieldsets for multiple
    /// collections.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::v1::types::{Field, FieldValue};

    use super::*;

    #[test]
    fn get_miss() {
        let mut cache = FieldSetCache::new();

        let mut fields = BTreeMap::new();
        fields.insert(
            "field1".to_string(),
            Field {
                name: "field1".to_string(),
                value: FieldValue::String("foo".into()),
            },
        );
        fields.insert(
            "field2".to_string(),
            Field {
                name: "field2".to_string(),
                value: FieldValue::String("bar".into()),
            },
        );
        let fs = FieldSet { name: "t".to_string(), fields };
        let key = serde_json::to_string(&fs).unwrap();

        // Cache miss: stats updated.
        let value_miss = cache.get(key.as_str()).unwrap();
        assert_eq!(cache.stats.hits, 0);
        assert_eq!(cache.stats.misses, 1);

        // Cache hit: pointer matches original Arc<FieldSet>, stats updated.
        let value_hit = cache.get(key.as_str()).unwrap();
        assert!(Arc::ptr_eq(&value_miss, &value_hit));
        assert_eq!(cache.stats.hits, 1);
        assert_eq!(cache.stats.misses, 1);
    }
}
