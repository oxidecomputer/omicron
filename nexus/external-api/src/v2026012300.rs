// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that changed from v2026012300 to v2026012800.
//!
//! # Summary of changes
//!
//! - Remove the `pool_type` from subnet pools. It's not clear what that really
//!   means, and no customers need it at this point.
//! - Removed the identity metadata from subnet pool members. The name isn't
//!   user-controllable, and the IP subnet itself is the identity. This is
//!   similar to IP Pool Ranges.
//! - Changed subnet pool member pagination to be by-IP-subnet.

use api_identity::ObjectIdentity;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use omicron_common::address::IpVersion;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::ObjectIdentity;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// A pool of subnets for external subnet allocation
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPool {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP version for this pool
    pub ip_version: IpVersion,
    /// Type of subnet pool (unicast or multicast)
    pub pool_type: shared::IpPoolType,
}

impl From<views::SubnetPool> for SubnetPool {
    fn from(value: views::SubnetPool) -> Self {
        // Assume unicast pool type, this was the only thing it could be from
        // this version of the API.
        Self {
            identity: value.identity,
            ip_version: value.ip_version,
            pool_type: shared::IpPoolType::Unicast,
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

impl From<views::SubnetPoolMember> for SubnetPoolMember {
    fn from(value: views::SubnetPoolMember) -> Self {
        // The identity metadata has gone away in the newer version. The name
        // wasn't user controllable, so we fake up a dummy name. Since the
        // members can't be updated, the modification time is always the same as
        // the creation time too.
        Self {
            identity: IdentityMetadata {
                id: value.id,
                name: "unused".parse().unwrap(),
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
