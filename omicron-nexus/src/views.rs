use crate::db;
use api_identity::ObjectIdentity;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::ObjectIdentity;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

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
}

impl From<db::model::Project> for Project {
    fn from(project: db::model::Project) -> Self {
        Self { identity: project.identity.into() }
    }
}
