// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version EXTERNAL_SUBNET_ATTACHMENT.
//!
//! These are wire-format shims: the blessed OpenAPI spec for this version
//! range includes fields that were later removed. We accept the fields on
//! the wire, validate them, and discard them during conversion.

use omicron_common::address::IpVersion;
use omicron_common::api::external::IdentityMetadataCreateParams;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025112000::ip_pool::IpPoolType;

/// Create a subnet pool.
///
/// This version of the type accepts a `pool_type` field. The only valid
/// value is `unicast`; any other value is rejected.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The IP version for this pool (IPv4 or IPv6). All subnets in the pool
    /// must match this version.
    pub ip_version: IpVersion,
    /// Type of subnet pool (defaults to Unicast).
    #[serde(default)]
    pub pool_type: IpPoolType,
}

/// Add a member (subnet) to a subnet pool.
///
/// This version of the type accepts an `identity` field (flattened as
/// `name` / `description`). The field is silently discarded.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberAdd {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The subnet to add to the pool.
    pub subnet: IpNet,
    /// Minimum prefix length for allocations from this subnet; a smaller prefix
    /// means larger allocations are allowed (e.g. a /16 prefix yields larger
    /// subnet allocations than a /24 prefix).
    ///
    /// Valid values: 0-32 for IPv4, 0-128 for IPv6.
    /// Default if not specified is equal to the subnet's prefix length.
    pub min_prefix_length: Option<u8>,
    /// Maximum prefix length for allocations from this subnet; a larger prefix
    /// means smaller allocations are allowed (e.g. a /24 prefix yields smaller
    /// subnet allocations than a /16 prefix).
    ///
    /// Valid values: 0-32 for IPv4, 0-128 for IPv6.
    /// Default if not specified is 32 for IPv4 and 128 for IPv6.
    pub max_prefix_length: Option<u8>,
}
