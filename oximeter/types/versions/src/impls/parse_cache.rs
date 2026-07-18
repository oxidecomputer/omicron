// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A per-producer cache that deduplicates parsed label sets.
//!
//! Every collection interval, a producer re-emits the same set of series: only
//! the [`Measurement`] (datum + timestamp) changes, while the `target` and
//! `metric` label sub-objects are byte-for-byte identical to the previous
//! interval. (The fields of a [`FieldSet`] live in a `BTreeMap`, so serde
//! serializes them in canonical sorted order every time, which makes the raw
//! label JSON stable across intervals.)
//!
//! Deserializing straight into [`ProducerResults`] therefore rebuilds an
//! identical `FieldSet` — a `BTreeMap` plus its interned names and owned string
//! values — for every sample of every known series, once per interval, forever.
//!
//! [`SampleCache`] avoids that. It deserializes into a mirror of the wire type
//! that borrows the raw bytes of each label sub-object ([`RawValue`]), then
//! looks those bytes up in a cache keyed on the exact bytes. On a hit it clones
//! a shared `Arc<FieldSet>`; only on a miss does it parse and allocate. The
//! `Arc` is then shared by the constructed [`Sample`], so the retained field set
//! is allocated once per distinct series rather than once per sample.

use crate::v1::schema::TimeseriesName;
use crate::v1::schema::default_schema_version;
use crate::v1::types::FieldSet;
use crate::v1::types::Measurement;
use crate::v1::types::MetricsError;
use crate::v1::types::ProducerResults;
use crate::v1::types::ProducerResultsItem;
use crate::v1::types::Sample;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::num::NonZeroU8;
use std::sync::Arc;

/// Maximum number of distinct label sets retained per producer.
///
/// The cache is cleared when it reaches this size, bounding the memory retained
/// by a long-lived collection task whose producer's set of series churns.
/// Outstanding `Arc<FieldSet>`s handed out before a clear remain valid; only the
/// deduplication resets, so the next sample of a still-live series re-parses it
/// once. A single producer emits at most a few hundred to low thousands of
/// distinct series in practice, so this is a generous cap.
const MAX_CACHED_FIELD_SETS: usize = 8192;

/// Mirror of [`Sample`] used only for deserialization.
///
/// It is identical on the wire to `Sample`, except that the two label
/// sub-objects are captured as borrowed raw JSON rather than parsed into
/// `FieldSet`s. The `measurement` still parses eagerly because it changes every
/// sample and is never cached.
#[derive(Deserialize)]
struct SampleRaw<'a> {
    measurement: Measurement,
    timeseries_name: TimeseriesName,
    #[serde(default = "default_schema_version")]
    timeseries_version: NonZeroU8,
    #[serde(borrow)]
    target: &'a RawValue,
    #[serde(borrow)]
    metric: &'a RawValue,
}

/// Mirror of [`ProducerResultsItem`] whose `Ok` variant holds [`SampleRaw`]s.
///
/// The `serde(tag/content/rename_all)` attributes must match
/// `ProducerResultsItem` exactly so the two decode from identical bytes.
#[derive(Deserialize)]
#[serde(tag = "status", content = "info", rename_all = "snake_case")]
enum ProducerResultsItemRaw<'a> {
    Ok(#[serde(borrow)] Vec<SampleRaw<'a>>),
    Err(MetricsError),
}

/// A per-producer cache mapping the raw JSON bytes of a label sub-object to its
/// shared, parsed `Arc<FieldSet>`.
///
/// One `SampleCache` serves a single producer's collection task. It is not
/// internally synchronized: a collection task drives it from a single task, so
/// callers hold it as a plain owned field.
#[derive(Debug, Default)]
pub struct SampleCache {
    // Keyed on the actual bytes (not a hash) so there is no collision risk.
    entries: HashMap<Box<[u8]>, Arc<FieldSet>>,
}

impl SampleCache {
    /// Construct an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct label sets currently retained. Exposed for tests.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Parse a producer's raw HTTP response body into [`ProducerResults`],
    /// reusing cached label sets for series seen before.
    ///
    /// The returned `Sample`s are identical to those produced by deserializing
    /// the same bytes directly into `ProducerResults`; the only difference is
    /// that their `target`/`metric` field sets may be shared `Arc`s rather than
    /// freshly allocated.
    pub fn parse(
        &mut self,
        body: &[u8],
    ) -> Result<ProducerResults, serde_json::Error> {
        // The borrowed `&RawValue`s point into `body`, which outlives this
        // conversion loop, so hits never copy the label bytes.
        let raw_items: Vec<ProducerResultsItemRaw<'_>> =
            serde_json::from_slice(body)?;
        let mut results = Vec::with_capacity(raw_items.len());
        for item in raw_items {
            match item {
                ProducerResultsItemRaw::Ok(raw_samples) => {
                    let mut samples = Vec::with_capacity(raw_samples.len());
                    for raw in raw_samples {
                        // Each field set is cached independently on its own raw
                        // bytes, so a target shared across several metrics is
                        // deduplicated too.
                        let target = self.get_or_parse(raw.target)?;
                        let metric = self.get_or_parse(raw.metric)?;
                        samples.push(Sample {
                            measurement: raw.measurement,
                            timeseries_name: raw.timeseries_name,
                            timeseries_version: raw.timeseries_version,
                            target,
                            metric,
                        });
                    }
                    results.push(ProducerResultsItem::Ok(samples));
                }
                ProducerResultsItemRaw::Err(err) => {
                    results.push(ProducerResultsItem::Err(err));
                }
            }
        }
        Ok(results)
    }

    /// Look up the raw label bytes; on a miss, parse, allocate, and cache.
    fn get_or_parse(
        &mut self,
        raw: &RawValue,
    ) -> Result<Arc<FieldSet>, serde_json::Error> {
        let bytes = raw.get().as_bytes();
        if let Some(field_set) = self.entries.get(bytes) {
            return Ok(Arc::clone(field_set));
        }
        let field_set = Arc::new(serde_json::from_str::<FieldSet>(raw.get())?);
        if self.entries.len() >= MAX_CACHED_FIELD_SETS {
            self.entries.clear();
        }
        self.entries.insert(Box::from(bytes), Arc::clone(&field_set));
        Ok(field_set)
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_CACHED_FIELD_SETS;
    use super::SampleCache;
    use crate::v1::types::ProducerResultsItem;
    use crate::v1::types::Sample;
    use std::sync::Arc;

    // Build the JSON body for a single-item `ProducerResults` carrying one
    // sample, with the given target/metric label values and datum. Field names
    // are emitted in sorted order to match how the producer serializes them.
    fn body(target_id: &str, metric_id: &str, datum: i64) -> Vec<u8> {
        format!(
            r#"[{{"status":"ok","info":[{{
                "measurement":{{"timestamp":"2024-01-01T00:00:00Z",
                    "datum":{{"type":"i64","datum":{datum}}}}},
                "timeseries_name":"my_target:my_metric",
                "target":{{"name":"my_target","fields":{{
                    "id":{{"name":"id",
                        "value":{{"type":"string","value":"{target_id}"}}}}}}}},
                "metric":{{"name":"my_metric","fields":{{
                    "id":{{"name":"id",
                        "value":{{"type":"string","value":"{metric_id}"}}}}}}}}
            }}]}}]"#
        )
        .into_bytes()
    }

    // Pull the single sample out of a single-item `ProducerResults`.
    fn only_sample(results: &[ProducerResultsItem]) -> &Sample {
        match &results[0] {
            ProducerResultsItem::Ok(samples) => &samples[0],
            ProducerResultsItem::Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn hit_reuses_arc_and_yields_equal_sample() {
        let mut cache = SampleCache::new();
        let bytes = body("a", "a", 1);

        let cold = cache.parse(&bytes).unwrap();
        // Second parse of the identical label bytes must reuse the same Arcs.
        let warm = cache.parse(&bytes).unwrap();

        let cold = only_sample(&cold);
        let warm = only_sample(&warm);
        assert_eq!(cold, warm);
        assert!(Arc::ptr_eq(&cold.target, &warm.target));
        assert!(Arc::ptr_eq(&cold.metric, &warm.metric));
        // One distinct target and one distinct metric were cached.
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn same_series_across_intervals_shares_arc_but_differs_from_other_series() {
        let mut cache = SampleCache::new();

        // Same series, two "intervals": identical labels, different datum.
        let first = cache.parse(&body("a", "a", 1)).unwrap();
        let second = cache.parse(&body("a", "a", 2)).unwrap();
        let first = only_sample(&first);
        let second = only_sample(&second);
        assert!(Arc::ptr_eq(&first.target, &second.target));
        assert!(Arc::ptr_eq(&first.metric, &second.metric));

        // A different series has different label bytes -> different Arc.
        let other = cache.parse(&body("b", "b", 1)).unwrap();
        let other = only_sample(&other);
        assert!(!Arc::ptr_eq(&first.target, &other.target));
        assert!(!Arc::ptr_eq(&first.metric, &other.metric));
    }

    #[test]
    fn cache_is_bounded() {
        let mut cache = SampleCache::new();
        // Insert well past the cap with unique series; the cache must clear on
        // overflow rather than grow without bound.
        for i in 0..(MAX_CACHED_FIELD_SETS + 100) {
            let id = i.to_string();
            cache.parse(&body(&id, &id, 0)).unwrap();
        }
        assert!(cache.len() <= MAX_CACHED_FIELD_SETS);
    }
}
