// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zone bundle types for Sled Agent API version 1.

use std::time::Duration;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Path parameters for zone requests.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZonePathParam {
    /// The name of the zone.
    pub zone_name: String,
}

/// Query parameters for zone bundle list filtering.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ZoneBundleFilter {
    /// An optional substring used to filter zone bundles.
    pub filter: Option<String>,
}

/// Parameters used to update the zone bundle cleanup context.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CleanupContextUpdate {
    /// The new period on which automatic cleanups are run.
    pub period: Option<Duration>,
    /// The priority ordering for preserving old zone bundles.
    pub priority: Option<PriorityOrder>,
    /// The new limit on the underlying dataset quota allowed for bundles.
    pub storage_limit: Option<u8>,
}

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
///
/// TODO: The serde deserializer does not currently verify uniqueness of
/// dimensions.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct PriorityOrder(pub(crate) [PriorityDimension; 2]);

/// Error type for creating a priority order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PriorityOrderCreateError {
    WrongDimensionCount(usize),
    DuplicateFound(PriorityDimension),
}

/// A period on which bundles are automatically cleaned up.
#[derive(
    Clone, Copy, Deserialize, JsonSchema, PartialEq, PartialOrd, Serialize,
)]
pub struct CleanupPeriod(pub(crate) Duration);

/// Error type for creating a cleanup period.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupPeriodCreateError(pub Duration);

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
pub struct StorageLimit(pub(crate) u8);

/// Error type for creating a storage limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageLimitCreateError(pub u8);

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
