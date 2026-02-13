// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version FLOATING_IP_ALLOCATOR_UPDATE.

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::address::IpVersion;
use omicron_common::api::external::{
    Error, IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// -- Path params --

/// Path parameters for subnet pool operations
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SubnetPoolPath {
    /// Name or ID of the subnet pool
    pub pool: NameOrId,
}

/// Path parameters for subnet pool silo operations
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolSiloPath {
    /// Name or ID of the subnet pool
    pub pool: NameOrId,
    /// Name or ID of the silo
    pub silo: NameOrId,
}

// -- Create/update params --

/// Create a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The IP version for this pool (IPv4 or IPv6). All subnets in the pool
    /// must match this version.
    pub ip_version: IpVersion,
}

/// Update a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Add a member (subnet) to a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberAdd {
    /// The subnet to add to the pool
    pub subnet: IpNet,
    /// Minimum prefix length for allocations from this subnet.
    pub min_prefix_length: Option<u8>,
    /// Maximum prefix length for allocations from this subnet.
    pub max_prefix_length: Option<u8>,
}

// Conversion from the prior version (EXTERNAL_SUBNET_ATTACHMENT), which
// accepted a `pool_type` field on SubnetPoolCreate (only `unicast` was
// valid) and an `identity` field on SubnetPoolMemberAdd (silently
// discarded).
impl TryFrom<crate::v2026011601::subnet_pool::SubnetPoolCreate>
    for SubnetPoolCreate
{
    type Error = Error;

    fn try_from(
        value: crate::v2026011601::subnet_pool::SubnetPoolCreate,
    ) -> Result<Self, Self::Error> {
        let crate::v2026011601::subnet_pool::SubnetPoolCreate {
            identity,
            ip_version,
            pool_type,
        } = value;
        if pool_type != crate::v2025112000::ip_pool::IpPoolType::Unicast {
            return Err(Error::invalid_request(
                "Subnet Pools must have pool type unicast",
            ));
        }
        Ok(Self { identity, ip_version })
    }
}

impl From<crate::v2026011601::subnet_pool::SubnetPoolMemberAdd>
    for SubnetPoolMemberAdd
{
    fn from(
        value: crate::v2026011601::subnet_pool::SubnetPoolMemberAdd,
    ) -> Self {
        let crate::v2026011601::subnet_pool::SubnetPoolMemberAdd {
            subnet,
            min_prefix_length,
            max_prefix_length,
            // Silently discard the identity field.
            ..
        } = value;
        Self { subnet, min_prefix_length, max_prefix_length }
    }
}

/// Remove a subnet from a pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberRemove {
    /// The subnet to remove from the pool. Must match an existing entry exactly.
    pub subnet: IpNet,
}

/// Link a subnet pool to a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolLinkSilo {
    /// The silo to link
    pub silo: NameOrId,
    /// Whether this is the default subnet pool for the silo.
    pub is_default: bool,
}

/// Update a subnet pool's silo link
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolSiloUpdate {
    /// Whether this is the default subnet pool for the silo
    pub is_default: bool,
}

// -- View types --

/// A pool of subnets for external subnet allocation
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct SubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for this pool
    pub ip_version: IpVersion,
}

/// A subnet pool in the context of a silo
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct SiloSubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// When a pool is the default for a silo, external subnet allocations will
    /// come from that pool when no other pool is specified.
    pub is_default: bool,
    /// The IP version for the pool.
    pub ip_version: IpVersion,
}

/// A member (subnet) within a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SubnetPoolMember {
    /// ID of the pool member
    pub id: Uuid,
    /// Time the pool member was created.
    pub time_created: DateTime<Utc>,
    /// ID of the parent subnet pool
    pub subnet_pool_id: Uuid,
    /// The subnet CIDR
    pub subnet: IpNet,
    /// Minimum prefix length for allocations from this subnet.
    pub min_prefix_length: u8,
    /// Maximum prefix length for allocations from this subnet.
    pub max_prefix_length: u8,
}

/// A link between a subnet pool and a silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SubnetPoolSiloLink {
    pub subnet_pool_id: Uuid,
    pub silo_id: Uuid,
    pub is_default: bool,
}

/// Utilization information for a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolUtilization {
    /// Number of addresses allocated from this pool
    pub allocated: f64,
    /// Total capacity of this pool in addresses
    pub capacity: f64,
}
