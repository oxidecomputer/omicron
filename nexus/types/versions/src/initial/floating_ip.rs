// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Floating IP types for version INITIAL.

use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, NameOrId, ObjectIdentity,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// A Floating IP is a well-known IP address which can be attached
/// and detached from instances.
#[derive(
    ObjectIdentity, Debug, PartialEq, Clone, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct FloatingIp {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The IP address held by this resource.
    pub ip: IpAddr,
    /// The ID of the IP pool this resource belongs to.
    pub ip_pool_id: Uuid,
    /// The project this resource exists within.
    pub project_id: Uuid,
    /// The ID of the instance that this Floating IP is attached to,
    /// if it is presently in use.
    pub instance_id: Option<Uuid>,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct FloatingIpSelector {
    /// Name or ID of the project, only required if `floating_ip` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Floating IP
    pub floating_ip: NameOrId,
}

/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// `pool`. If set, then the IP must be available in the resolved `pool`.
    pub ip: Option<IpAddr>,

    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<NameOrId>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// The type of resource that a floating IP is attached to
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FloatingIpParentKind {
    Instance,
}

/// Parameters for attaching a floating IP address to another resource
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpAttach {
    /// Name or ID of the resource that this IP address should be attached to
    pub parent: NameOrId,

    /// The type of `parent`'s resource
    pub kind: FloatingIpParentKind,
}
