// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version FLOATING_IP_ALLOCATOR_UPDATE.
//!
//! This version removes `pool_type` from `SubnetPool` and identity metadata
//! from `SubnetPoolMember`. It also removes `pool_type` from
//! `SubnetPoolCreate` and `identity` from `SubnetPoolMemberAdd`.

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::address::IpVersion;
use omicron_common::api::external::{
    Error, IdentityMetadata, IdentityMetadataCreateParams, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::ip_pool::IpPoolType;

// -- Create/update params --

/// Create a subnet pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The IP version for this pool (IPv4 or IPv6). All subnets in the pool
    /// must match this version.
    pub ip_version: IpVersion,
}

/// Add a member (subnet) to a subnet pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberAdd {
    /// The subnet to add to the pool.
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
impl TryFrom<crate::v2026_01_16_01::subnet_pool::SubnetPoolCreate>
    for SubnetPoolCreate
{
    type Error = Error;

    fn try_from(
        value: crate::v2026_01_16_01::subnet_pool::SubnetPoolCreate,
    ) -> Result<Self, Self::Error> {
        let crate::v2026_01_16_01::subnet_pool::SubnetPoolCreate {
            identity,
            ip_version,
            pool_type,
        } = value;
        if pool_type != crate::v2025_11_20_00::ip_pool::IpPoolType::Unicast {
            return Err(Error::invalid_request(
                "Subnet Pools must have pool type unicast",
            ));
        }
        Ok(Self { identity, ip_version })
    }
}

impl From<crate::v2026_01_16_01::subnet_pool::SubnetPoolMemberAdd>
    for SubnetPoolMemberAdd
{
    fn from(
        value: crate::v2026_01_16_01::subnet_pool::SubnetPoolMemberAdd,
    ) -> Self {
        let crate::v2026_01_16_01::subnet_pool::SubnetPoolMemberAdd {
            subnet,
            min_prefix_length,
            max_prefix_length,
            // Silently discard the identity field.
            ..
        } = value;
        Self { subnet, min_prefix_length, max_prefix_length }
    }
}

// -- View types --

/// A pool of subnets for external subnet allocation.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct SubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for this pool.
    pub ip_version: IpVersion,
}

// Response conversion: add back pool_type for the older API version.
impl From<SubnetPool> for crate::v2026_01_16_01::subnet_pool::SubnetPool {
    fn from(value: SubnetPool) -> Self {
        // The older version included pool_type. The only valid value was
        // unicast, so we always emit that.
        Self {
            identity: value.identity,
            ip_version: value.ip_version,
            pool_type: IpPoolType::Unicast,
        }
    }
}

/// A subnet pool in the context of a silo.
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

/// A member (subnet) within a subnet pool.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SubnetPoolMember {
    /// ID of the pool member.
    pub id: Uuid,
    /// Time the pool member was created.
    pub time_created: DateTime<Utc>,
    /// ID of the parent subnet pool.
    pub subnet_pool_id: Uuid,
    /// The subnet CIDR.
    pub subnet: IpNet,
    /// Minimum prefix length for allocations from this subnet.
    pub min_prefix_length: u8,
    /// Maximum prefix length for allocations from this subnet.
    pub max_prefix_length: u8,
}

// Response conversion: add back identity metadata for the older API version.
impl From<SubnetPoolMember>
    for crate::v2026_01_16_01::subnet_pool::SubnetPoolMember
{
    fn from(value: SubnetPoolMember) -> Self {
        // The older version included identity metadata. The name was not
        // user-controllable, so we provide a dummy name. Since members
        // cannot be updated, the modification time equals the creation time.
        Self {
            identity: IdentityMetadata {
                id: value.id,
                name: "unused".parse().expect("parsed \"unused\" as a Name"),
                description: String::new(),
                time_created: value.time_created,
                time_modified: value.time_created,
            },
            subnet_pool_id: value.subnet_pool_id,
            subnet: value.subnet,
            min_prefix_length: value.min_prefix_length,
            max_prefix_length: value.max_prefix_length,
        }
    }
}
