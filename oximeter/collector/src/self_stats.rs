// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Metrics oximeter reports about itself

// Copyright 2023 Oxide Computer Company

use crate::ProducerEndpoint;
use oximeter::types::Cumulative;
use oximeter::types::ProducerResultsItem;
use oximeter::types::StrValue;
use oximeter::MetricsError;
use oximeter::Sample;
use reqwest::StatusCode;
use std::collections::BTreeMap;
use std::time::Duration;

oximeter::use_timeseries!("oximeter-collector.toml");
pub use self::oximeter_collector::Collections;
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

    fn as_string(&self) -> StrValue {
        match self {
            Self::Unreachable => Self::UNREACHABLE.into(),
            Self::Deserialization => Self::DESERIALIZATION.into(),
            Self::Other(c) => c.as_u16().to_string().into(),
        }
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
    use super::FailureReason;
    use super::StatusCode;

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
}
