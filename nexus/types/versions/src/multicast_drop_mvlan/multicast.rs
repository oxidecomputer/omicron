// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group types from API version `MULTICAST_IMPLICIT_LIFECYCLE_UPDATES`
//! that changed in version `MULTICAST_DROP_MVLAN`.
//!
//! Removes the `mvlan` field from `MulticastGroup`, `MulticastGroupCreate`,
//! and `MulticastGroupUpdate`. Adds `has_any_source_member` to
//! `MulticastGroup`.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2025_11_20_00::multicast::{
    validate_multicast_ip_param, validate_source_ips_param,
};

/// View of a Multicast Group
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
pub struct MulticastGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The multicast IP address held by this resource.
    pub multicast_ip: IpAddr,
    /// Deduplicated union of source IPs specified by members.
    ///
    /// Contains only sources from members that joined with explicit `source_ips`.
    /// Members using any-source multicast (empty `source_ips`) do not contribute,
    /// so a non-empty value does not imply all members use source filtering.
    /// For SSM addresses (232/8, ff3x::/32), this is always non-empty.
    pub source_ips: Vec<IpAddr>,
    /// True if any member joined without specifying source IPs (any-source).
    ///
    /// When true, at least one member receives traffic from any source rather
    /// than filtering to specific sources.
    pub has_any_source_member: bool,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// Current state of the multicast group.
    pub state: String,
}

/// Create-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The multicast IP address to allocate. If None, one will be allocated
    /// from the default pool.
    #[serde(default, deserialize_with = "validate_multicast_ip_param")]
    pub multicast_ip: Option<IpAddr>,
    /// Source IP addresses for Source-Specific Multicast (SSM).
    ///
    /// None uses default behavior (Any-Source Multicast).
    /// Empty list explicitly allows any source (Any-Source Multicast).
    /// Non-empty list restricts to specific sources (SSM).
    #[serde(default, deserialize_with = "validate_source_ips_param")]
    pub source_ips: Option<Vec<IpAddr>>,
    /// Name or ID of the IP pool to allocate from. If None, uses the default
    /// multicast pool.
    #[serde(default)]
    pub pool: Option<NameOrId>,
}

/// Update-time parameters for a multicast group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MulticastGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    #[serde(
        default,
        deserialize_with = "validate_source_ips_param",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_ips: Option<Vec<IpAddr>>,
}

// -- Downward conversion: MULTICAST_DROP_MVLAN -> MULTICAST_IMPLICIT_LIFECYCLE_UPDATES --
// Response types convert downward through consecutive versions.

impl From<MulticastGroup> for crate::v2026_01_08_00::multicast::MulticastGroup {
    fn from(new: MulticastGroup) -> Self {
        Self {
            identity: new.identity,
            multicast_ip: new.multicast_ip,
            source_ips: new.source_ips,
            mvlan: None,
            ip_pool_id: new.ip_pool_id,
            state: new.state,
        }
    }
}

// -- Upward conversions: INITIAL -> MULTICAST_DROP_MVLAN --
// Request types convert upward, dropping the mvlan field.

impl From<crate::v2025_11_20_00::multicast::MulticastGroupCreate>
    for MulticastGroupCreate
{
    fn from(
        old: crate::v2025_11_20_00::multicast::MulticastGroupCreate,
    ) -> Self {
        Self {
            identity: old.identity,
            multicast_ip: old.multicast_ip,
            source_ips: old.source_ips,
            pool: old.pool,
        }
    }
}

impl From<crate::v2025_11_20_00::multicast::MulticastGroupUpdate>
    for MulticastGroupUpdate
{
    fn from(
        old: crate::v2025_11_20_00::multicast::MulticastGroupUpdate,
    ) -> Self {
        Self { identity: old.identity, source_ips: old.source_ips }
    }
}
