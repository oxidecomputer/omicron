use crate::db::model;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/**
 * Create-time parameters for an [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

impl From<&ProjectCreate> for model::Project {
    fn from(params: &ProjectCreate) -> Self {
        let id = Uuid::new_v4();
        Self {
            identity: model::IdentityMetadata::new(id, params.identity.clone()),
        }
    }
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
