// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Views are response bodies, most of which are public lenses onto DB models.

use crate::authn;
use crate::db::identity::{Asset, Resource};
use crate::db::model;
use api_identity::ObjectIdentity;
use omicron_common::api::external::{
    ByteCount, IdentityMetadata, Ipv4Net, Ipv6Net, Name, ObjectIdentity,
    RoleName,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

// SILOS

/// Client view of a ['Silo']
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Silo {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// A silo where discoverable is false can be retrieved only by its id - it
    /// will not be part of the "list all silos" output.
    pub discoverable: bool,
}

impl Into<Silo> for model::Silo {
    fn into(self) -> Silo {
        Silo { identity: self.identity(), discoverable: self.discoverable }
    }
}

// ORGANIZATIONS

/// Client view of an [`Organization`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Organization {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    // Important: Silo ID does not get presented to user
}

impl From<model::Organization> for Organization {
    fn from(org: model::Organization) -> Self {
        Self { identity: org.identity() }
    }
}

// PROJECTS

/// Client view of a [`Project`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Project {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub organization_id: Uuid,
}

impl From<model::Project> for Project {
    fn from(project: model::Project) -> Self {
        Self {
            identity: project.identity(),
            organization_id: project.organization_id,
        }
    }
}

// IMAGES

/// Client view of Images
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Image {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Option<Uuid>,
    pub url: Option<String>,
    pub size: ByteCount,
}

// SNAPSHOTS

/// Client view of a Snapshot
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Snapshot {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Uuid,
    pub disk_id: Uuid,
    pub size: ByteCount,
}

// VPCs

/// Client view of a [`Vpc`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Vpc {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// id for the project containing this VPC
    pub project_id: Uuid,

    /// id for the system router where subnet default routes are registered
    pub system_router_id: Uuid,

    /// The unique local IPv6 address range for subnets in this VPC
    pub ipv6_prefix: Ipv6Net,

    // TODO-design should this be optional?
    /// The name used for the VPC in DNS.
    pub dns_name: Name,
}

impl From<model::Vpc> for Vpc {
    fn from(vpc: model::Vpc) -> Self {
        Self {
            identity: vpc.identity(),
            project_id: vpc.project_id,
            system_router_id: vpc.system_router_id,
            ipv6_prefix: *vpc.ipv6_prefix,
            dns_name: vpc.dns_name.0,
        }
    }
}

/// A VPC subnet represents a logical grouping for instances that allows network traffic between
/// them, within a IPv4 subnetwork or optionall an IPv6 subnetwork.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnet {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The VPC to which the subnet belongs.
    pub vpc_id: Uuid,

    /// The IPv4 subnet CIDR block.
    pub ipv4_block: Ipv4Net,

    /// The IPv6 subnet CIDR block.
    pub ipv6_block: Ipv6Net,
}

impl From<model::VpcSubnet> for VpcSubnet {
    fn from(subnet: model::VpcSubnet) -> Self {
        Self {
            identity: subnet.identity(),
            vpc_id: subnet.vpc_id,
            ipv4_block: subnet.ipv4_block.0,
            ipv6_block: subnet.ipv6_block.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcRouterKind {
    System,
    Custom,
}

impl From<model::VpcRouterKind> for VpcRouterKind {
    fn from(kind: model::VpcRouterKind) -> Self {
        match kind {
            model::VpcRouterKind::Custom => Self::Custom,
            model::VpcRouterKind::System => Self::System,
        }
    }
}

/// A VPC router defines a series of rules that indicate where traffic
/// should be sent depending on its destination.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouter {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub kind: VpcRouterKind,

    /// The VPC to which the router belongs.
    pub vpc_id: Uuid,
}

impl From<model::VpcRouter> for VpcRouter {
    fn from(router: model::VpcRouter) -> Self {
        Self {
            identity: router.identity(),
            vpc_id: router.vpc_id,
            kind: router.kind.into(),
        }
    }
}

// RACKS

/// Client view of an [`Rack`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl From<model::Rack> for Rack {
    fn from(rack: model::Rack) -> Self {
        Self { identity: rack.identity() }
    }
}

// SLEDS

/// Client view of an [`Sled`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

impl From<model::Sled> for Sled {
    fn from(sled: model::Sled) -> Self {
        Self { identity: sled.identity(), service_address: sled.address() }
    }
}

// BUILT-IN USERS

/// Client view of a [`User`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct User {
    // TODO-correctness is flattening here (and in all the other types) the
    // intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl From<model::UserBuiltin> for User {
    fn from(user: model::UserBuiltin) -> Self {
        Self { identity: user.identity() }
    }
}

/// Client view of currently authed user.
// TODO: this may end up merged with User once more details about the user are
// stored in the auth context. Right now there is only the ID.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SessionUser {
    pub id: Uuid,
}

impl From<authn::Actor> for SessionUser {
    fn from(actor: authn::Actor) -> Self {
        Self { id: actor.id }
    }
}

// ROLES

/// Client view of a [`Role`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Role {
    pub name: RoleName,
    pub description: String,
}

impl From<model::RoleBuiltin> for Role {
    fn from(role: model::RoleBuiltin) -> Self {
        Self {
            name: RoleName::new(&role.resource_type, &role.role_name),
            description: role.description,
        }
    }
}
