// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version FLOATING_IP_ALLOCATOR_UPDATE.

use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2026_01_05_00::ip_pool::PoolSelector;

/// Specify how to allocate a floating IP address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AddressAllocator {
    /// Reserve a specific IP address. The pool is inferred from the address
    /// since IP pools cannot have overlapping ranges.
    Explicit {
        /// The IP address to reserve.
        ip: IpAddr,
    },
    /// Automatically allocate an IP address from a pool.
    Auto {
        /// Pool selection.
        ///
        /// If omitted, the silo's default pool is used. If the silo has
        /// default pools for both IPv4 and IPv6, the request will fail
        /// unless `ip_version` is specified.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

impl Default for AddressAllocator {
    fn default() -> Self {
        AddressAllocator::Auto { pool_selector: PoolSelector::default() }
    }
}

impl From<crate::v2026_01_16_00::floating_ip::AddressAllocator>
    for AddressAllocator
{
    fn from(
        value: crate::v2026_01_16_00::floating_ip::AddressAllocator,
    ) -> Self {
        match value {
            // Pool field is dropped since the IP uniquely identifies
            // the pool.
            crate::v2026_01_16_00::floating_ip::AddressAllocator::Explicit {
                ip,
                pool: _,
            } => AddressAllocator::Explicit { ip },
            crate::v2026_01_16_00::floating_ip::AddressAllocator::Auto {
                pool_selector,
            } => AddressAllocator::Auto { pool_selector },
        }
    }
}

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// IP address allocation method.
    #[serde(default)]
    pub address_allocator: AddressAllocator,
}

impl From<crate::v2026_01_16_00::floating_ip::FloatingIpCreate>
    for FloatingIpCreate
{
    fn from(
        value: crate::v2026_01_16_00::floating_ip::FloatingIpCreate,
    ) -> Self {
        Self {
            identity: value.identity,
            address_allocator: value.address_allocator.into(),
        }
    }
}
