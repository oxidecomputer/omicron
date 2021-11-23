/*!
 * Views are response bodies, most of which are public lenses onto DB models.
 */

use crate::db::identity::Resource;
use crate::db::model;
use api_identity::ObjectIdentity;
use omicron_common::api::external::{IdentityMetadata, ObjectIdentity};
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
 * PREDEFINED USERS
 */

/**
 * Client view of a [`User`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct User {
    /*
     * TODO-correctness is flattening here (and in all the other types) the
     * intent in RFD 4?
     */
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

impl Into<User> for model::UserPredefined {
    fn into(self) -> User {
        User { identity: self.identity() }
    }
}
