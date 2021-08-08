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
    let p1_name = "springfield-squidport";
    let p2_name = "cairo-airport";
    create_project(&client, &p1_name).await;
    create_project(&client, &p2_name).await;

    let p1_url = format!("/projects/{}", p1_name);
    let project: Project = object_get(&client, &p1_url).await;
    assert_eq!(project.identity.name, p1_name);

    let p2_url = format!("/projects/{}", p2_name);
    let project: Project = object_get(&client, &p2_url).await;
    assert_eq!(project.identity.name, p2_name);

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
