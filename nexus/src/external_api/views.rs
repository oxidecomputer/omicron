/*!
 * Views are response bodies, most of which are public lenses onto DB models.
 */

use crate::db::identity::Resource;
use crate::db::model;
use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, Name, ObjectIdentity};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
 * Client view of an [`Project`]
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
