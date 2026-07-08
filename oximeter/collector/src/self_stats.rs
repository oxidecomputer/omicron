// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics oximeter reports about itself

use crate::ProducerEndpoint;
use chrono::DateTime;
use chrono::Utc;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::histogram::Histogram;
use oximeter::types::Cumulative;
use oximeter::types::ProducerResultsItem;
use reqwest::StatusCode;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

oximeter::use_timeseries!("oximeter-collector.toml");
pub use self::oximeter_collector::Collections;
pub use self::oximeter_collector::DatabaseInsertsFailed;
pub use self::oximeter_collector::DatabaseQueueDepth;
pub use self::oximeter_collector::DatabaseSamplesDropped;
pub use self::oximeter_collector::FailedCollections;
pub use self::oximeter_collector::OximeterCollector;

/// The interval on which we report self statistics
pub const COLLECTION_INTERVAL: Duration = Duration::from_secs(60);

/// Small enum to help understand why oximeter failed to collect from a
/// producer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FailureReason {
    /// The producer could not be reached.
    Unreachable,
    /// Error during deserialization.
    Deserialization,
    /// The collection interval has expired while an outstanding collection is
    /// already in progress.
    ///
    /// This may indicate that the producer's collection interval is too short
    /// for the amount of data it generates, and the collector cannot keep up.
    CollectionsInProgress,
    /// Some other reason, which includes the status code.
    Other(StatusCode),
}

impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unreachable => f.write_str(Self::UNREACHABLE),
            Self::Deserialization => f.write_str(Self::DESERIALIZATION),
            Self::CollectionsInProgress => {
                f.write_str(Self::COLLECTIONS_IN_PROGRESS)
            }
            Self::Other(c) => write!(f, "{}", c.as_u16()),
        }
    }
}

impl FailureReason {
    const UNREACHABLE: &'static str = "unreachable";
    const DESERIALIZATION: &'static str = "deserialization";
    const COLLECTIONS_IN_PROGRESS: &'static str = "collections in progress";

    fn as_string(&self) -> Cow<'static, str> {
        match self {
            Self::Unreachable => Cow::Borrowed(Self::UNREACHABLE),
            Self::Deserialization => Cow::Borrowed(Self::DESERIALIZATION),
            Self::CollectionsInProgress => {
                Cow::Borrowed(Self::COLLECTIONS_IN_PROGRESS)
            }
            Self::Other(c) => Cow::Owned(c.as_u16().to_string()),
        }
    }
}

pub struct CollectorStats {
    pub single_stats: Arc<CollectorSinkStats>,
    pub cluster_stats: Arc<CollectorSinkStats>,
}

impl CollectorStats {
    pub fn new(start_time: DateTime<Utc>) -> Self {
        Self {
            single_stats: Arc::new(CollectorSinkStats::new(
                "clickhouse-single".into(),
                start_time,
            )),
            cluster_stats: Arc::new(CollectorSinkStats::new(
                "clickhouse-cluster".into(),
                start_time,
            )),
        }
    }
}

pub struct CollectorSinkStats {
    pub label: String,
    pub samples_dropped: Mutex<Cumulative<u64>>,
    pub queue_depth: Mutex<Histogram<u64>>,
    pub insert_errors: Mutex<Cumulative<u64>>,
}

impl CollectorSinkStats {
    pub fn new(label: String, start_time: DateTime<Utc>) -> Self {
        // Note: `span_decades(0, 5)` produces a log-linear histogram that spans
        // [0, 1_000_000). As of this writing, the collector's `BoundedQueue` is
        // sized at a fixed 100_000 (batch_size of 1000 * MAX_BUFFER_SIZE_MULTIPLIER
        // of 100), but we set a higher cap here so that we don't saturate it if we
        // choose a larger queue later on.
        let mut queue_depth =
            Histogram::span_decades(0, 5).expect("histogram bounds are valid");
        queue_depth.set_start_time(start_time);
        Self {
            label,
            samples_dropped: Mutex::new(Cumulative::with_start_time(
                start_time, 0,
            )),
            queue_depth: Mutex::new(queue_depth),
            insert_errors: Mutex::new(Cumulative::with_start_time(
                start_time, 0,
            )),
        }
    }

    pub fn samples(
        &self,
        collection_target: &OximeterCollector,
    ) -> Result<Vec<Sample>, MetricsError> {
        let drop_metric = DatabaseSamplesDropped {
            datum: *self.samples_dropped.lock().unwrap(),
            collector_sink: self.label.clone().into(),
        };
        let queue_metric = DatabaseQueueDepth {
            datum: self.queue_depth.lock().unwrap().clone(),
            collector_sink: self.label.clone().into(),
        };
        let insert_metric = DatabaseInsertsFailed {
            datum: *self.insert_errors.lock().unwrap(),
            collector_sink: self.label.clone().into(),
        };
        Ok(vec![
            Sample::new(collection_target, &drop_metric)?,
            Sample::new(collection_target, &queue_metric)?,
            Sample::new(collection_target, &insert_metric)?,
        ])
    }
}

/// Oximeter collection statistics maintained by each collection task.
#[derive(Clone, Debug)]
pub struct CollectionTaskStats {
    pub collector: OximeterCollector,
    pub collections: Collections,
    pub failed_collections: BTreeMap<FailureReason, FailedCollections>,
}

impl CollectionTaskStats {
    pub fn new(
        collector: OximeterCollector,
        producer: &ProducerEndpoint,
    ) -> Self {
        Self {
            collector,
            collections: Collections {
                producer_id: producer.id,
                producer_ip: producer.address.ip(),
                producer_port: producer.address.port(),
                base_route: "".into(),
                datum: Cumulative::new(0),
            },
            failed_collections: BTreeMap::new(),
        }
    }

    /// Update this information with a new producer endpoint.
    ///
    /// # Panics
    ///
    /// This panics if `new_info` refers to a different ID.
    pub fn update(&mut self, new_info: &ProducerEndpoint) {
        assert_eq!(self.collections.producer_id, new_info.id);

        // Only reset the counters if the new information is actually different.
        let new_ip = new_info.address.ip();
        let new_port = new_info.address.port();
        if self.collections.producer_ip == new_ip
            && self.collections.producer_port == new_port
        {
            return;
        }
        self.collections.producer_ip = new_ip;
        self.collections.producer_port = new_port;
        self.collections.datum = Cumulative::new(0);
        for each in self.failed_collections.values_mut() {
            each.producer_ip = new_ip;
            each.producer_port = new_port;
            each.datum = Cumulative::new(0);
        }
    }

    pub fn failures_for_reason(
        &mut self,
        reason: FailureReason,
    ) -> &mut FailedCollections {
        self.failed_collections.entry(reason).or_insert_with(|| {
            FailedCollections {
                producer_id: self.collections.producer_id,
                producer_ip: self.collections.producer_ip,
                producer_port: self.collections.producer_port,
                base_route: self.collections.base_route.clone(),
                reason: reason.as_string(),
                datum: Cumulative::new(0),
            }
        })
    }

    pub fn sample(&self) -> Vec<ProducerResultsItem> {
        fn to_item(res: Result<Sample, MetricsError>) -> ProducerResultsItem {
            match res {
                Ok(s) => ProducerResultsItem::Ok(vec![s]),
                Err(s) => ProducerResultsItem::Err(s),
            }
        }
        let mut samples = Vec::with_capacity(1 + self.failed_collections.len());
        samples.push(to_item(Sample::new(&self.collector, &self.collections)));
        samples.extend(
            self.failed_collections
                .values()
                .map(|metric| to_item(Sample::new(&self.collector, metric))),
        );
        samples
    }
}

#[cfg(test)]
mod tests {
    use super::CollectionTaskStats;
    use super::FailureReason;
    use super::OximeterCollector;
    use super::StatusCode;
    use omicron_common::api::internal::nexus::ProducerEndpoint;
    use omicron_common::api::internal::nexus::ProducerKind;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_failure_reason_serialization() {
        let data = &[
            (FailureReason::Deserialization, FailureReason::DESERIALIZATION),
            (FailureReason::Unreachable, FailureReason::UNREACHABLE),
            (
                FailureReason::CollectionsInProgress,
                FailureReason::COLLECTIONS_IN_PROGRESS,
            ),
            (FailureReason::Other(StatusCode::INTERNAL_SERVER_ERROR), "500"),
        ];
        for (variant, as_str) in data.iter() {
            assert_eq!(variant.to_string(), *as_str);
        }
    }

    #[test]
    fn only_reset_counters_if_info_is_different() {
        let info = ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: ProducerKind::Service,
            address: "[::1]:12345".parse().unwrap(),
            interval: Duration::from_secs(1),
        };
        let collector = OximeterCollector {
            collector_id: Uuid::new_v4(),
            collector_ip: "::1".parse().unwrap(),
            collector_port: 12345,
        };
        let mut stats = CollectionTaskStats::new(collector, &info);
        stats.collections.datum.increment();

        stats.update(&info);
        assert_eq!(
            stats.collections.datum.value(),
            1,
            "Should not have reset the counter when updating \
            with the same producer endpoint information"
        );
        let info = ProducerEndpoint {
            address: "[::1]:11111".parse().unwrap(),
            ..info
        };
        stats.update(&info);
        assert_eq!(
            stats.collections.datum.value(),
            0,
            "Should have reset the counter when updating \
            with different producer endpoint information"
        );
    }
}
