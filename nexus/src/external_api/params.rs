/*!
 * Params define the request bodies of API endpoints for creating or updating resources.
 */

use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/*
 * ORGANIZATIONS
 */

/**
 * Create-time parameters for an [`Organization`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`Organization`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrganizationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * PROJECTS
 */

/**
 * Create-time parameters for an [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * NETWORK INTERFACES
 */

/**
 * Create-time parameters for a [`NetworkInterface`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}
