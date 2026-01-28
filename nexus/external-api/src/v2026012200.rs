// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that changed from v2026012200 to v2026012201.

use nexus_types::external_api::params;
use nexus_types::external_api::params::PoolSelector;
use nexus_types::external_api::shared;
use omicron_common::address::IpVersion;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::NameOrId;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Create a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The IP version for this pool (IPv4 or IPv6). All subnets in the pool
    /// must match this version.
    pub ip_version: IpVersion,
    /// Type of subnet pool (defaults to Unicast)
    #[serde(default)]
    pub pool_type: shared::IpPoolType,
}

impl TryFrom<SubnetPoolCreate> for params::SubnetPoolCreate {
    type Error = Error;

    fn try_from(value: SubnetPoolCreate) -> Result<Self, Self::Error> {
        let SubnetPoolCreate { identity, ip_version, pool_type } = value;
        if pool_type != shared::IpPoolType::Unicast {
            return Err(Error::invalid_request(
                "Subnet Pools must have pool type unicast",
            ));
        }
        Ok(Self { identity, ip_version })
    }
}
/// Add a member (subnet) to a subnet pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubnetPoolMemberAdd {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The subnet to add to the pool
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

impl From<SubnetPoolMemberAdd> for params::SubnetPoolMemberAdd {
    fn from(value: SubnetPoolMemberAdd) -> Self {
        let SubnetPoolMemberAdd {
            subnet,
            min_prefix_length,
            max_prefix_length,
            ..
        } = value;
        Self { subnet, min_prefix_length, max_prefix_length }
    }
}

/// Specify how to allocate an external subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalSubnetAllocator {
    /// Reserve a specific subnet.
    Explicit {
        /// The subnet CIDR to reserve. Must be available in the pool.
        subnet: IpNet,
        /// The pool containing this subnet. If not specified, the default
        /// subnet pool for the subnet's IP version is used.
        pool: Option<NameOrId>,
    },
    /// Automatically allocate a subnet with the specified prefix length.
    Auto {
        /// The prefix length for the allocated subnet (e.g., 24 for a /24).
        prefix_len: u8,
        /// Pool selection.
        ///
        /// If omitted, this field uses the silo's default pool. If the
        /// silo has default pools for both IPv4 and IPv6, the request will
        /// fail unless `ip_version` is specified in the pool selector.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

impl TryFrom<ExternalSubnetAllocator> for params::ExternalSubnetAllocator {
    type Error = Error;

    fn try_from(value: ExternalSubnetAllocator) -> Result<Self, Self::Error> {
        match value {
            ExternalSubnetAllocator::Explicit { subnet, pool } => {
                if pool.is_some() {
                    return Err(Error::invalid_request(
                        "May not specify both an IP subnet and a Subnet Pool",
                    ));
                }
                Ok(Self::Explicit { subnet })
            }
            ExternalSubnetAllocator::Auto { prefix_len, pool_selector } => {
                Ok(Self::Auto { prefix_len, pool_selector })
            }
        }
    }
}

/// Create an external subnet
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// Subnet allocation method.
    pub allocator: ExternalSubnetAllocator,
}

impl TryFrom<ExternalSubnetCreate> for params::ExternalSubnetCreate {
    type Error = Error;

    fn try_from(value: ExternalSubnetCreate) -> Result<Self, Self::Error> {
        let ExternalSubnetCreate { identity, allocator } = value;
        allocator.try_into().map(|allocator| Self { identity, allocator })
    }
}
