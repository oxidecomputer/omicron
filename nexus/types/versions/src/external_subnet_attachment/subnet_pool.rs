// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version EXTERNAL_SUBNET_ATTACHMENT.
//!
//! This is the earliest version where subnet pool endpoints exist. Types
//! defined here are either wire-format shims (request types with fields that
//! were later removed) or the first definitions of types that are unchanged
//! in later versions.
//!
//! Wire-format shims:
//! - `SubnetPoolCreate` accepts a `pool_type` field (validated to be Unicast).
//! - `SubnetPoolMemberAdd` accepts an `identity` field (silently discarded).
//!
//! View-type shims (for EXTERNAL_SUBNET_ATTACHMENT..REMOVE_SUBNET_POOL_POOL_TYPE):
//! - `SubnetPool` includes a `pool_type` field.
//! - `SubnetPoolMember` includes identity metadata.

use api_identity::ObjectIdentity;
use omicron_common::address::IpVersion;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::ip_pool::IpPoolType;

// -- Path params --

/// Path parameters for subnet pool operations.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SubnetPoolPath {
    /// Name or ID of the subnet pool.
    pub pool: NameOrId,
}

/// Path parameters for subnet pool silo operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolSiloPath {
    /// Name or ID of the subnet pool.
    pub pool: NameOrId,
    /// Name or ID of the silo.
    pub silo: NameOrId,
}

// -- Create/update params --

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

/// Update a subnet pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
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

/// Remove a subnet from a pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberRemove {
    /// The subnet to remove from the pool. Must match an existing entry exactly.
    pub subnet: IpNet,
}

/// Link a subnet pool to a silo.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolLinkSilo {
    /// The silo to link.
    pub silo: NameOrId,
    /// Whether this is the default subnet pool for the silo.
    pub is_default: bool,
}

/// Update a subnet pool's silo link.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolSiloUpdate {
    /// Whether this is the default subnet pool for the silo.
    pub is_default: bool,
}

// -- View types --

/// A pool of subnets for external subnet allocation.
///
/// This version includes a `pool_type` field that was removed in
/// `REMOVE_SUBNET_POOL_POOL_TYPE`.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for this pool.
    pub ip_version: IpVersion,
    /// Type of subnet pool (unicast or multicast).
    pub pool_type: IpPoolType,
}

/// A member (subnet) within a subnet pool.
///
/// This version includes identity metadata that was removed in
/// `REMOVE_SUBNET_POOL_POOL_TYPE`.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMember {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// ID of the parent subnet pool.
    pub subnet_pool_id: Uuid,
    /// The subnet CIDR.
    pub subnet: IpNet,
    /// Minimum prefix length for allocations from this subnet; a smaller prefix
    /// means larger allocations are allowed (e.g. a /16 prefix yields larger
    /// subnet allocations than a /24 prefix).
    pub min_prefix_length: u8,
    /// Maximum prefix length for allocations from this subnet; a larger prefix
    /// means smaller allocations are allowed (e.g. a /24 prefix yields smaller
    /// subnet allocations than a /16 prefix).
    pub max_prefix_length: u8,
}

/// A link between a subnet pool and a silo.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SubnetPoolSiloLink {
    pub subnet_pool_id: Uuid,
    pub silo_id: Uuid,
    pub is_default: bool,
}

/// Utilization information for a subnet pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUtilization {
    /// Number of addresses allocated from this pool.
    pub allocated: f64,
    /// Total capacity of this pool in addresses.
    pub capacity: f64,
}
