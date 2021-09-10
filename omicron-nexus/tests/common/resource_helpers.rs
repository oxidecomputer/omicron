use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Project;
use omicron_common::api::external::ProjectCreateParams;
use std::convert::TryFrom;

pub async fn create_project(
    client: &ClientTestContext,
    project_name: &str,
) -> Project {
    objects_post(
        &client,
        "/projects",
        ProjectCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(project_name).unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await
}
