// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Views are response bodies, most of which are public lenses onto DB models.
 */

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

/*
 * ORGANIZATIONS
 */

/**
 * Client view of an [`Organization`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Organization {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl Into<Organization> for model::Organization {
    fn into(self) -> Organization {
        Organization { identity: self.identity() }
    }
}

/*
 * PROJECTS
 */

/**
 * Client view of a [`Project`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Project {
    /*
     * TODO-correctness is flattening here (and in all the other types) the
     * intent in RFD 4?
     */
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub organization_id: Uuid,
}

impl Into<Project> for model::Project {
    fn into(self) -> Project {
        Project {
            identity: self.identity(),
            organization_id: self.organization_id,
        }
    }
}

/*
 * IMAGES
 */

/// Client view of Images
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Image {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    pub project_id: Option<Uuid>,
    pub size: ByteCount,
}

/*
 * VPCs
 */

/**
 * Client view of a [`Vpc`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Vpc {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /** id for the project containing this VPC */
    pub project_id: Uuid,

    /// id for the system router where subnet default routes are registered
    pub system_router_id: Uuid,

    /// The unique local IPv6 address range for subnets in this VPC
    pub ipv6_prefix: Ipv6Net,

    // TODO-design should this be optional?
    /** The name used for the VPC in DNS. */
    pub dns_name: Name,
}

impl Into<Vpc> for model::Vpc {
    fn into(self) -> Vpc {
        Vpc {
            identity: self.identity(),
            project_id: self.project_id,
            system_router_id: self.system_router_id,
            ipv6_prefix: *self.ipv6_prefix,
            dns_name: self.dns_name.0,
        }
    }
}

/// A VPC subnet represents a logical grouping for instances that allows network traffic between
/// them, within a IPv4 subnetwork or optionall an IPv6 subnetwork.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnet {
    /** common identifying metadata */
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /** The VPC to which the subnet belongs. */
    pub vpc_id: Uuid,

    /** The IPv4 subnet CIDR block. */
    pub ipv4_block: Ipv4Net,

    /** The IPv6 subnet CIDR block. */
    pub ipv6_block: Ipv6Net,
}

impl Into<VpcSubnet> for model::VpcSubnet {
    fn into(self) -> VpcSubnet {
        VpcSubnet {
            identity: self.identity(),
            vpc_id: self.vpc_id,
            ipv4_block: self.ipv4_block.0,
            ipv6_block: self.ipv6_block.0,
        }
    }
}

/*
 * RACKS
 */

/**
 * Client view of an [`Rack`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl Into<Rack> for model::Rack {
    fn into(self) -> Rack {
        Rack { identity: self.identity() }
    }
}

/*
 * SLEDS
 */

/**
 * Client view of an [`Sled`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Sled {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

impl Into<Sled> for model::Sled {
    fn into(self) -> Sled {
        Sled { identity: self.identity(), service_address: self.address() }
    }
}

/*
 * BUILT-IN USERS
 */

/**
 * Client view of a [`User`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct User {
    /*
     * TODO-correctness is flattening here (and in all the other types) the
     * intent in RFD 4?
     */
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl Into<User> for model::UserBuiltin {
    fn into(self) -> User {
        User { identity: self.identity() }
    }
}

/**
 * Client view of currently authed user.
 */
// TODO: this may end up merged with User once more details about the user are
// stored in the auth context. Right now there is only the ID.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SessionUser {
    pub id: Uuid,
}

impl Into<SessionUser> for authn::Actor {
    fn into(self) -> SessionUser {
        SessionUser { id: self.0 }
    }
}

/*
 * ROLES
 */

/**
 * Client view of a [`Role`]
 */
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Role {
    pub name: RoleName,
    pub description: String,
}

impl Into<Role> for model::RoleBuiltin {
    fn into(self) -> Role {
        Role {
            name: RoleName::new(&self.resource_type, &self.role_name),
            description: self.description,
        }
    }
}
