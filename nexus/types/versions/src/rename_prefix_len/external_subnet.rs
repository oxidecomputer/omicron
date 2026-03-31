// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External subnet types for version RENAME_PREFIX_LEN.
//!
//! Renames `prefix_len` to `prefix_length` in
//! `ExternalSubnetAllocator::Auto`.

use crate::v2026_01_22_00;
use omicron_common::api::external::IdentityMetadataCreateParams;
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
        prefix_length: u8,
        /// Pool selection.
        ///
        /// If omitted, this field uses the silo's default pool. If the
        /// silo has default pools for both IPv4 and IPv6, the request will
        /// fail unless `ip_version` is specified in the pool selector.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

/// Create an external subnet
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// Subnet allocation method.
    pub allocator: ExternalSubnetAllocator,
}

// Conversion from the prior version (FLOATING_IP_ALLOCATOR_UPDATE).
impl From<v2026_01_22_00::external_subnet::ExternalSubnetAllocator>
    for ExternalSubnetAllocator
{
    fn from(
        value: v2026_01_22_00::external_subnet::ExternalSubnetAllocator,
    ) -> Self {
        match value {
            v2026_01_22_00::external_subnet::ExternalSubnetAllocator::Explicit {
                subnet,
            } => Self::Explicit { subnet },
            v2026_01_22_00::external_subnet::ExternalSubnetAllocator::Auto {
                prefix_len,
                pool_selector,
            } => Self::Auto { prefix_length: prefix_len, pool_selector },
        }
    }
}

impl From<v2026_01_22_00::external_subnet::ExternalSubnetCreate>
    for ExternalSubnetCreate
{
    fn from(
        value: v2026_01_22_00::external_subnet::ExternalSubnetCreate,
    ) -> Self {
        let v2026_01_22_00::external_subnet::ExternalSubnetCreate {
            identity,
            allocator,
        } = value;
        Self { identity, allocator: allocator.into() }
    }
}
