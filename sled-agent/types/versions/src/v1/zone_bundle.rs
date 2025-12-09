// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone bundle types for Sled Agent API version 1.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An identifier for a zone bundle.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct ZoneBundleId {
    /// The name of the zone this bundle is derived from.
    pub zone_name: String,
    /// The ID for this bundle itself.
    pub bundle_id: Uuid,
}

/// The reason or cause for a zone bundle, i.e., why it was created.
//
// NOTE: The ordering of the enum variants is important, and should not be
// changed without careful consideration.
//
// The ordering is used when deciding which bundles to remove automatically. In
// addition to time, the cause is used to sort bundles, so changing the variant
// order will change that priority.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ZoneBundleCause {
    /// Some other, unspecified reason.
    #[default]
    Other,
    /// A zone bundle taken when a sled agent finds a zone that it does not
    /// expect to be running.
    UnexpectedZone,
    /// An instance zone was terminated.
    TerminatedInstance,
}

/// Metadata about a zone bundle.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct ZoneBundleMetadata {
    /// Identifier for this zone bundle
    pub id: ZoneBundleId,
    /// The time at which this zone bundle was created.
    pub time_created: DateTime<Utc>,
    /// A version number for this zone bundle.
    pub version: u8,
    /// The reason or cause a bundle was created.
    pub cause: ZoneBundleCause,
}

impl ZoneBundleMetadata {
    pub const VERSION: u8 = 0;

    /// Create a new set of metadata for the provided zone.
    pub fn new(zone_name: &str, cause: ZoneBundleCause) -> Self {
        Self {
            id: ZoneBundleId {
                zone_name: zone_name.to_string(),
                bundle_id: Uuid::new_v4(),
            },
            time_created: Utc::now(),
            version: Self::VERSION,
            cause,
        }
    }
}

/// A dimension along with bundles can be sorted, to determine priority.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Serialize,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[serde(rename_all = "snake_case")]
pub enum PriorityDimension {
    /// Sorting by time, with older bundles with lower priority.
    Time,
    /// Sorting by the cause for creating the bundle.
    Cause,
}

/// The priority order for bundles during cleanup.
///
/// Bundles are sorted along the dimensions in [`PriorityDimension`], with each
/// dimension appearing exactly once. During cleanup, lesser-priority bundles
/// are pruned first, to maintain the dataset quota. Note that bundles are
/// sorted by each dimension in the order in which they appear, with each
/// dimension having higher priority than the next.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct PriorityOrder([PriorityDimension; PriorityOrder::EXPECTED_SIZE]);

impl std::ops::Deref for PriorityOrder {
    type Target = [PriorityDimension; PriorityOrder::EXPECTED_SIZE];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for PriorityOrder {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Error type for creating a priority order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PriorityOrderCreateError {
    WrongDimensionCount(usize),
    DuplicateFound(PriorityDimension),
}

impl std::fmt::Display for PriorityOrderCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriorityOrderCreateError::WrongDimensionCount(n) => {
                write!(
                    f,
                    "expected exactly {} dimensions, found {}",
                    PriorityOrder::EXPECTED_SIZE,
                    n
                )
            }
            PriorityOrderCreateError::DuplicateFound(dim) => {
                write!(
                    f,
                    "duplicate element found in priority ordering: {:?}",
                    dim
                )
            }
        }
    }
}

impl std::error::Error for PriorityOrderCreateError {}

impl PriorityOrder {
    // NOTE: Must match the number of variants in `PriorityDimension`.
    pub(crate) const EXPECTED_SIZE: usize = 2;
    const DEFAULT: Self =
        Self([PriorityDimension::Cause, PriorityDimension::Time]);

    /// Construct a new priority order.
    ///
    /// This requires that each dimension appear exactly once.
    pub fn new(
        dims: &[PriorityDimension],
    ) -> Result<Self, PriorityOrderCreateError> {
        if dims.len() != Self::EXPECTED_SIZE {
            return Err(PriorityOrderCreateError::WrongDimensionCount(
                dims.len(),
            ));
        }
        let mut seen = HashSet::new();
        for dim in dims.iter() {
            if !seen.insert(dim) {
                return Err(PriorityOrderCreateError::DuplicateFound(*dim));
            }
        }
        Ok(Self(dims.try_into().unwrap()))
    }

    /// Get the priority order as a slice.
    pub fn as_slice(&self) -> &[PriorityDimension] {
        &self.0
    }

    /// Order zone bundle metadata according to the contained priority.
    ///
    /// We sort the metadata by each dimension, in the order in which it
    /// appears. That means earlier dimensions have higher priority than later
    /// ones.
    pub fn compare_metadata(
        &self,
        lhs: &ZoneBundleMetadata,
        rhs: &ZoneBundleMetadata,
    ) -> Ordering {
        for dim in self.0.iter() {
            let ord = match dim {
                PriorityDimension::Cause => lhs.cause.cmp(&rhs.cause),
                PriorityDimension::Time => {
                    lhs.time_created.cmp(&rhs.time_created)
                }
            };
            if matches!(ord, Ordering::Equal) {
                continue;
            }
            return ord;
        }
        Ordering::Equal
    }
}

/// A period on which bundles are automatically cleaned up.
#[derive(
    Clone, Copy, Deserialize, JsonSchema, PartialEq, PartialOrd, Serialize,
)]
pub struct CleanupPeriod(Duration);

impl Default for CleanupPeriod {
    fn default() -> Self {
        Self(Duration::from_secs(600))
    }
}

/// Error type for creating a cleanup period.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupPeriodCreateError(pub Duration);

impl std::fmt::Display for CleanupPeriodCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid cleanup period ({:?}): must be between {:?} and {:?}, inclusive",
            self.0,
            CleanupPeriod::MIN.as_duration(),
            CleanupPeriod::MAX.as_duration(),
        )
    }
}

impl std::error::Error for CleanupPeriodCreateError {}

impl CleanupPeriod {
    /// The minimum supported cleanup period.
    pub const MIN: Self = Self(Duration::from_secs(60));

    /// The maximum supported cleanup period.
    pub const MAX: Self = Self(Duration::from_secs(60 * 60 * 24));

    /// Construct a new cleanup period, checking that it's valid.
    pub fn new(duration: Duration) -> Result<Self, CleanupPeriodCreateError> {
        if duration >= Self::MIN.as_duration()
            && duration <= Self::MAX.as_duration()
        {
            Ok(Self(duration))
        } else {
            Err(CleanupPeriodCreateError(duration))
        }
    }

    /// Return the period as a duration.
    pub const fn as_duration(&self) -> Duration {
        self.0
    }
}

impl TryFrom<Duration> for CleanupPeriod {
    type Error = CleanupPeriodCreateError;

    fn try_from(duration: Duration) -> Result<Self, Self::Error> {
        Self::new(duration)
    }
}

impl std::fmt::Debug for CleanupPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The limit on space allowed for zone bundles, as a percentage of the overall
/// dataset's quota.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    JsonSchema,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct StorageLimit(u8);

impl std::fmt::Display for StorageLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}%", self.as_u8())
    }
}

impl Default for StorageLimit {
    fn default() -> Self {
        StorageLimit(25)
    }
}

/// Error type for creating a storage limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageLimitCreateError(pub u8);

impl std::fmt::Display for StorageLimitCreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid storage limit ({}): must be expressed as a percentage in ({}, {}]",
            self.0,
            StorageLimit::MIN.0,
            StorageLimit::MAX.0,
        )
    }
}

impl std::error::Error for StorageLimitCreateError {}

impl StorageLimit {
    /// Minimum percentage of dataset quota supported.
    pub const MIN: Self = Self(0);

    /// Maximum percentage of dataset quota supported.
    pub const MAX: Self = Self(50);

    /// Construct a new limit allowed for zone bundles.
    ///
    /// This should be expressed as a percentage, in the range (Self::MIN,
    /// Self::MAX].
    pub const fn new(percentage: u8) -> Result<Self, StorageLimitCreateError> {
        if percentage > Self::MIN.0 && percentage <= Self::MAX.0 {
            Ok(Self(percentage))
        } else {
            Err(StorageLimitCreateError(percentage))
        }
    }

    /// Return the contained quota percentage.
    pub const fn as_u8(&self) -> u8 {
        self.0
    }

    // Compute the number of bytes available from a dataset quota, in bytes.
    pub const fn bytes_available(&self, dataset_quota: u64) -> u64 {
        (dataset_quota * self.as_u8() as u64) / 100
    }
}

/// The portion of a debug dataset used for zone bundles.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct BundleUtilization {
    /// The total dataset quota, in bytes.
    pub dataset_quota: u64,
    /// The total number of bytes available for zone bundles.
    ///
    /// This is `dataset_quota` multiplied by the context's storage limit.
    pub bytes_available: u64,
    /// Total bundle usage, in bytes.
    pub bytes_used: u64,
}

/// Context provided for the zone bundle cleanup task.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize,
)]
pub struct CleanupContext {
    /// The period on which automatic checks and cleanup is performed.
    pub period: CleanupPeriod,
    /// The limit on the dataset quota available for zone bundles.
    pub storage_limit: StorageLimit,
    /// The priority ordering for keeping old bundles.
    pub priority: PriorityOrder,
}

/// The count of bundles / bytes removed during a cleanup operation.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct CleanupCount {
    /// The number of bundles removed.
    pub bundles: u64,
    /// The number of bytes removed.
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_zone_bundle_cause() {
        use ZoneBundleCause::*;
        let mut original = [Other, TerminatedInstance, UnexpectedZone];
        let expected = [Other, UnexpectedZone, TerminatedInstance];
        original.sort();
        assert_eq!(original, expected);
    }

    #[test]
    fn test_priority_dimension() {
        assert!(PriorityOrder::new(&[]).is_err());
        assert!(PriorityOrder::new(&[PriorityDimension::Cause]).is_err());
        assert!(
            PriorityOrder::new(&[
                PriorityDimension::Cause,
                PriorityDimension::Cause
            ])
            .is_err()
        );
        assert!(
            PriorityOrder::new(&[
                PriorityDimension::Cause,
                PriorityDimension::Cause,
                PriorityDimension::Time
            ])
            .is_err()
        );

        assert!(
            PriorityOrder::new(&[
                PriorityDimension::Cause,
                PriorityDimension::Time
            ])
            .is_ok()
        );
        assert_eq!(
            PriorityOrder::new(PriorityOrder::default().as_slice()).unwrap(),
            PriorityOrder::default()
        );
    }

    #[test]
    fn test_storage_limit_bytes_available() {
        let pct = StorageLimit(1);
        assert_eq!(pct.bytes_available(100), 1);
        assert_eq!(pct.bytes_available(1000), 10);

        let pct = StorageLimit(50);
        assert_eq!(pct.bytes_available(100), 50);
        assert_eq!(pct.bytes_available(1000), 500);

        // Test non-power of 10.
        let pct = StorageLimit(25);
        assert_eq!(pct.bytes_available(32768), 8192);
    }
}
