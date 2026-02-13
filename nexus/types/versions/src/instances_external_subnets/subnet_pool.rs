// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain name at https://mozilla.org/MPL/2.0/.

//! Subnet pool types for version INSTANCES_EXTERNAL_SUBNETS.
//!
//! At this version, `SubnetPool` still includes a `pool_type` field and
//! `SubnetPoolMember` still includes identity metadata. These were removed
//! in `REMOVE_SUBNET_POOL_POOL_TYPE`.

use api_identity::ObjectIdentity;
use omicron_common::address::IpVersion;
use omicron_common::api::external::{IdentityMetadata, ObjectIdentity};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025112000::ip_pool::IpPoolType;

/// A pool of subnets for external subnet allocation
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for this pool
    pub ip_version: IpVersion,
    /// Type of subnet pool (unicast or multicast)
    pub pool_type: IpPoolType,
}

impl From<crate::v2026012200::subnet_pool::SubnetPool> for SubnetPool {
    fn from(value: crate::v2026012200::subnet_pool::SubnetPool) -> Self {
        // The newer version dropped pool_type. Assume unicast, which was
        // the only value this field could have at this API version.
        Self {
            identity: value.identity,
            ip_version: value.ip_version,
            pool_type: IpPoolType::Unicast,
        }
    }
}

/// A member (subnet) within a subnet pool
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMember {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// ID of the parent subnet pool
    pub subnet_pool_id: Uuid,
    /// The subnet CIDR
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

impl From<crate::v2026012200::subnet_pool::SubnetPoolMember>
    for SubnetPoolMember
{
    fn from(value: crate::v2026012200::subnet_pool::SubnetPoolMember) -> Self {
        // The identity metadata was removed in the newer version. The name
        // was not user-controllable, so we provide a dummy name. Since
        // members cannot be updated, the modification time is always the
        // same as the creation time.
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
