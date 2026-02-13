// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External subnet types for version FLOATING_IP_ALLOCATOR_UPDATE.
//!
//! This version removes the `pool` field from
//! `ExternalSubnetAllocator::Explicit`.

use omicron_common::api::external::{Error, IdentityMetadataCreateParams};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2026_01_05_00::ip_pool::PoolSelector;

/// Specify how to allocate an external subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalSubnetAllocator {
    /// Reserve a specific subnet.
    Explicit {
        /// The subnet CIDR to reserve. Must be available in the pool.
        subnet: IpNet,
    },
    /// Automatically allocate a subnet with the specified prefix length.
    Auto {
        /// The prefix length for the allocated subnet (e.g., 24 for a /24).
        prefix_len: u8,
        /// Pool selection.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

/// Create an external subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// Subnet allocation method.
    pub allocator: ExternalSubnetAllocator,
}

// Conversion from the prior version (EXTERNAL_SUBNET_ATTACHMENT), which
// accepted a `pool` field on ExternalSubnetAllocator::Explicit that was
// later removed.
impl TryFrom<crate::v2026_01_16_01::external_subnet::ExternalSubnetAllocator>
    for ExternalSubnetAllocator
{
    type Error = Error;

    fn try_from(
        value: crate::v2026_01_16_01::external_subnet::ExternalSubnetAllocator,
    ) -> Result<Self, Self::Error> {
        match value {
            crate::v2026_01_16_01::external_subnet::ExternalSubnetAllocator::Explicit {
                subnet,
                pool,
            } => {
                if pool.is_some() {
                    return Err(Error::invalid_request(
                        "May not specify both an IP subnet and a Subnet Pool",
                    ));
                }
                Ok(Self::Explicit { subnet })
            }
            crate::v2026_01_16_01::external_subnet::ExternalSubnetAllocator::Auto {
                prefix_len,
                pool_selector,
            } => Ok(Self::Auto { prefix_len, pool_selector }),
        }
    }
}

impl TryFrom<crate::v2026_01_16_01::external_subnet::ExternalSubnetCreate>
    for ExternalSubnetCreate
{
    type Error = Error;

    fn try_from(
        value: crate::v2026_01_16_01::external_subnet::ExternalSubnetCreate,
    ) -> Result<Self, Self::Error> {
        let crate::v2026_01_16_01::external_subnet::ExternalSubnetCreate {
            identity,
            allocator,
        } = value;
        allocator.try_into().map(|allocator| Self { identity, allocator })
    }
}
