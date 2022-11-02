// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::resource_helpers::project_get;
use omicron_nexus::external_api::views::Project;

use nexus_test_utils::resource_helpers::{create_organization, create_project};
use nexus_test_utils_macros::nexus_test;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_projects(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let p1_name = "springfield-squidport";
    let p2_name = "cairo-airport";
    let org_p1_id =
        create_project(&client, &org_name, &p1_name).await.identity.id;

    create_project(&client, &org_name, &p2_name).await;

    let p1_url = format!("/organizations/{}/projects/{}", org_name, p1_name);
    let project: Project = project_get(&client, &p1_url).await;
    assert_eq!(project.identity.name, p1_name);

    let p2_url = format!("/organizations/{}/projects/{}", org_name, p2_name);
    let project: Project = project_get(&client, &p2_url).await;
    assert_eq!(project.identity.name, p2_name);

    // Verify the list of Projects.
    let projects_url = format!("/organizations/{}/projects", org_name);
    let projects = NexusRequest::iter_collection_authn::<Project>(
        &client,
        &projects_url,
        "",
        None,
    )
    .await
    .expect("failed to list projects")
    .all_items;
    assert_eq!(projects.len(), 2);
    // alphabetical order for now
    assert_eq!(projects[0].identity.name, p2_name);
    assert_eq!(projects[1].identity.name, p1_name);

    // Create a second organization and make sure we can have two projects with
    // the same name across organizations
    let org2_name = "test-org2";
    create_organization(&client, &org2_name).await;
    let org2_p1_id =
        create_project(&client, &org2_name, &p1_name).await.identity.id;
    assert_ne!(org_p1_id, org2_p1_id);

    // Make sure the list projects results for the new org make sense
    let projects = NexusRequest::iter_collection_authn::<Project>(
        &client,
        &format!("/organizations/{}/projects", org2_name),
        "",
        None,
    )
    .await
    .expect("failed to list projects")
    .all_items;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].identity.name, p1_name);
}
