// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External subnet types for version EXTERNAL_SUBNET_ATTACHMENT.
//!
//! This is the earliest version where external subnet endpoints exist. Types
//! here include both wire-format shims and first definitions of unchanged
//! types.
//!
//! Wire-format shims:
//! - `ExternalSubnetAllocator::Explicit` accepts a `pool` field that was
//!   later removed. We accept it on the wire but reject requests where it is
//!   set.
//! - `ExternalSubnetCreate` wraps the above allocator.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, NameOrId, ObjectIdentity,
};
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2026_01_05_00::ip_pool::PoolSelector;

// -- Path params / selectors --

/// Path parameters for external subnet operations.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ExternalSubnetPath {
    /// Name or ID of the external subnet.
    pub external_subnet: NameOrId,
}

/// Selector for looking up an external subnet.
#[derive(Deserialize, JsonSchema, Clone)]
pub struct ExternalSubnetSelector {
    /// Name or ID of the project (required if `external_subnet` is a Name).
    pub project: Option<NameOrId>,
    /// Name or ID of the external subnet.
    pub external_subnet: NameOrId,
}

// -- Create/update params --

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

/// Update an external subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Attach an external subnet to an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ExternalSubnetAttach {
    /// Name or ID of the instance to attach to.
    pub instance: NameOrId,
}

/// An external subnet allocated from a subnet pool.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct ExternalSubnet {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The allocated subnet CIDR.
    pub subnet: IpNet,
    /// The project this subnet belongs to.
    pub project_id: Uuid,
    /// The subnet pool this was allocated from.
    pub subnet_pool_id: Uuid,
    /// The subnet pool member this subnet corresponds to.
    pub subnet_pool_member_id: Uuid,
    /// The instance this subnet is attached to, if any.
    pub instance_id: Option<Uuid>,
}
