use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Project;
use omicron_common::api::external::ProjectCreateParams;
use std::convert::TryFrom;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_projects() {
    let cptestctx = test_setup("test_projects").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let _: Project = create_project(&client, &project_name).await;

    let project_url = format!("/projects/{}", project_name);

    let project: Project = object_get(&client, &project_url).await;
    assert_eq!(project.identity.name, project_name);
    assert_eq!(project.identity.description, "a pier");

    cptestctx.teardown().await;
}

async fn create_project(
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
