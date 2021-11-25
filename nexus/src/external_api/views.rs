// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Views are response bodies, most of which are public lenses onto DB models.
 */

use crate::db::identity::{Asset, Resource};
use crate::db::model;
use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, Name, ObjectIdentity};
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
 * VPCs
 */

/**
 * Client view of a [`Vpc`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Vpc {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /** id for the project containing this VPC */
    pub project_id: Uuid,

    /// id for the system router where subnet default routes are registered
    pub system_router_id: Uuid,

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
            dns_name: self.dns_name.0,
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
#[serde(rename_all = "camelCase")]
pub struct Rack {
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
#[serde(rename_all = "camelCase")]
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
