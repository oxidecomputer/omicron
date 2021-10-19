use omicron_common::api::external::Project;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;

pub mod common;
use common::resource_helpers::{create_organization, create_project};
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_projects() {
    let cptestctx = test_setup("test_projects").await;
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    /* Create a project that we'll use for testing. */
    let p1_name = "springfield-squidport";
    let p2_name = "cairo-airport";
    create_project(&client, &org_name, &p1_name).await;
    create_project(&client, &org_name, &p2_name).await;

    let p1_url = format!("/organizations/{}/projects/{}", org_name, p1_name);
    let project: Project = object_get(&client, &p1_url).await;
    assert_eq!(project.identity.name, p1_name);

    let p2_url = format!("/organizations/{}/projects/{}", org_name, p2_name);
    let project: Project = object_get(&client, &p2_url).await;
    assert_eq!(project.identity.name, p2_name);

    let projects = objects_list_page::<Project>(
        client,
        &format!("/organizations/{}/projects", org_name),
    )
    .await
    .items;
    assert_eq!(projects.len(), 2);
    // alphabetical order for now
    assert_eq!(projects[0].identity.name, p2_name);
    assert_eq!(projects[1].identity.name, p1_name);

    cptestctx.teardown().await;
}
