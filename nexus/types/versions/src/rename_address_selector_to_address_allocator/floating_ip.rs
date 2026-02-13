// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version RENAME_ADDRESS_SELECTOR_TO_ADDRESS_ALLOCATOR.
//!
//! This version renames `AddressSelector` to `AddressAllocator` and
//! `address_selector` to `address_allocator` in `FloatingIpCreate`. The
//! `Explicit` variant still has both `ip` and optional `pool` fields;
//! the `pool` field is dropped in FLOATING_IP_ALLOCATOR_UPDATE.

use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::v2026010500::ip_pool::PoolSelector;

/// Specify how to allocate a floating IP address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AddressAllocator {
    /// Reserve a specific IP address.
    Explicit {
        /// The IP address to reserve. Must be available in the pool.
        ip: IpAddr,
        /// The pool containing this address. If not specified, the default
        /// pool for the address's IP version is used.
        pool: Option<NameOrId>,
    },
    /// Automatically allocate an IP address from a specified pool.
    Auto {
        /// Pool selection.
        ///
        /// If omitted, this field uses the silo's default pool. If the
        /// silo has default pools for both IPv4 and IPv6, the request will
        /// fail unless `ip_version` is specified in the pool selector.
        #[serde(default)]
        pool_selector: PoolSelector,
    },
}

impl Default for AddressAllocator {
    fn default() -> Self {
        AddressAllocator::Auto { pool_selector: PoolSelector::default() }
    }
}

impl From<crate::v2026010500::floating_ip::AddressSelector>
    for AddressAllocator
{
    fn from(value: crate::v2026010500::floating_ip::AddressSelector) -> Self {
        match value {
            crate::v2026010500::floating_ip::AddressSelector::Explicit {
                ip,
                pool,
            } => AddressAllocator::Explicit { ip, pool },
            crate::v2026010500::floating_ip::AddressSelector::Auto {
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

impl From<crate::v2026010500::floating_ip::FloatingIpCreate>
    for FloatingIpCreate
{
    fn from(value: crate::v2026010500::floating_ip::FloatingIpCreate) -> Self {
        Self {
            identity: value.identity,
            address_allocator: value.address_selector.into(),
        }
    }
}
