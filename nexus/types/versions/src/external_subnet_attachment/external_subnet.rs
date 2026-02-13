// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External subnet types for version EXTERNAL_SUBNET_ATTACHMENT.
//!
//! This version's `ExternalSubnetAllocator::Explicit` variant accepted a
//! `pool` field that was later removed. We accept it on the wire but reject
//! requests where it is set.

use omicron_common::api::external::{IdentityMetadataCreateParams, NameOrId};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2026010500::ip_pool::PoolSelector;

/// Specify how to allocate an external subnet.
///
/// This version of the type accepts a `pool` field on the `explicit` variant.
/// Setting `pool` is rejected; it exists only for wire-format compatibility.
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

/// Create an external subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// Subnet allocation method.
    pub allocator: ExternalSubnetAllocator,
}
