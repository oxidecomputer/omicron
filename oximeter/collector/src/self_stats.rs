// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics oximeter reports about itself

// Copyright 2023 Oxide Computer Company

use crate::ProducerEndpoint;
use oximeter::types::Cumulative;
use oximeter::types::ProducerResultsItem;
use oximeter::Metric;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::Target;
use reqwest::StatusCode;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;
use uuid::Uuid;

/// The interval on which we report self statistics
pub const COLLECTION_INTERVAL: Duration = Duration::from_secs(60);

/// A target representing a single oximeter collector.
#[derive(Clone, Copy, Debug, Target)]
pub struct OximeterCollector {
    /// The collector's ID.
    pub collector_id: Uuid,
    /// The collector server's IP address.
    pub collector_ip: IpAddr,
    /// The collector server's port.
    pub collector_port: u16,
}

/// The number of successful collections from a single producer.
#[derive(Clone, Debug, Metric)]
pub struct Collections {
    /// The producer's ID.
    pub producer_id: Uuid,
    /// The producer's IP address.
    pub producer_ip: IpAddr,
    /// The producer's port.
    pub producer_port: u16,
    /// The base route in the producer server used to collect metrics.
    ///
    /// The full route is `{base_route}/{producer_id}`.
    pub base_route: String,
    pub datum: Cumulative<u64>,
}

/// Small enum to help understand why oximeter failed to collect from a
/// producer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum FailureReason {
    /// The producer could not be reached.
    Unreachable,
    /// Error during deserialization.
    Deserialization,
    /// Some other reason, which includes the status code.
    Other(StatusCode),
}

impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unreachable => f.write_str(Self::UNREACHABLE),
            Self::Deserialization => f.write_str(Self::DESERIALIZATION),
            Self::Other(c) => write!(f, "{}", c.as_u16()),
        }
    }
}

impl FailureReason {
    const UNREACHABLE: &'static str = "unreachable";
    const DESERIALIZATION: &'static str = "deserialization";

    fn as_string(&self) -> Cow<'static, str> {
        match self {
            Self::Unreachable => Cow::Borrowed(Self::UNREACHABLE),
            Self::Deserialization => Cow::Borrowed(Self::DESERIALIZATION),
            Self::Other(c) => Cow::Owned(c.as_u16().to_string()),
        }
    }
}

/// The number of failed collections from a single producer.
#[derive(Clone, Debug, Metric)]
pub struct FailedCollections {
    /// The producer's ID.
    pub producer_id: Uuid,
    /// The producer's IP address.
    pub producer_ip: IpAddr,
    /// The producer's port.
    pub producer_port: u16,
    /// The base route in the producer server used to collect metrics.
    ///
    /// The full route is `{base_route}/{producer_id}`.
    pub base_route: String,
    /// The reason we could not collect.
    //
    // NOTE: This should always be generated through a `FailureReason`.
    pub reason: Cow<'static, str>,
    pub datum: Cumulative<u64>,
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
                base_route: producer.base_route.clone(),
                datum: Cumulative::new(0),
            },
            failed_collections: BTreeMap::new(),
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
    use super::Collections;
    use super::Cumulative;
    use super::FailedCollections;
    use super::FailureReason;
    use super::OximeterCollector;
    use super::StatusCode;
    use oximeter::schema::SchemaSet;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;

    #[test]
    fn test_failure_reason_serialization() {
        let data = &[
            (FailureReason::Deserialization, "deserialization"),
            (FailureReason::Unreachable, "unreachable"),
            (FailureReason::Other(StatusCode::INTERNAL_SERVER_ERROR), "500"),
        ];
        for (variant, as_str) in data.iter() {
            assert_eq!(variant.to_string(), *as_str);
        }
    }

    const fn collector() -> OximeterCollector {
        OximeterCollector {
            collector_id: uuid::uuid!("cfebaa5f-3ba9-4bb5-9145-648d287df78a"),
            collector_ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
            collector_port: 12345,
        }
    }

    fn collections() -> Collections {
        Collections {
            producer_id: uuid::uuid!("718452ab-7cca-42f6-b8b1-1aaaa1b09104"),
            producer_ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
            producer_port: 12345,
            base_route: String::from("/"),
            datum: Cumulative::new(0),
        }
    }

    fn failed_collections() -> FailedCollections {
        FailedCollections {
            producer_id: uuid::uuid!("718452ab-7cca-42f6-b8b1-1aaaa1b09104"),
            producer_ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
            producer_port: 12345,
            base_route: String::from("/"),
            reason: FailureReason::Unreachable.as_string(),
            datum: Cumulative::new(0),
        }
    }

    // Check that the self-stat timeseries schema have not changed.
    #[test]
    fn test_no_schema_changes() {
        let collector = collector();
        let collections = collections();
        let failed = failed_collections();
        let mut set = SchemaSet::default();
        assert!(set.insert_checked(&collector, &collections).is_none());
        assert!(set.insert_checked(&collector, &failed).is_none());

        const PATH: &'static str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/output/self-stat-schema.json"
        );
        set.assert_contents(PATH);
    }
}
