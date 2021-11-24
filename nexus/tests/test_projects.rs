//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use omicron_nexus::external_api::views::Project;

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
    let org_p1_id =
        create_project(&client, &org_name, &p1_name).await.identity.id;

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

    /* Create a second organization and make sure we can have two projects with
     * the same name across organizations */
    let org2_name = "test-org2";
    create_organization(&client, &org2_name).await;
    let org2_p1_id =
        create_project(&client, &org2_name, &p1_name).await.identity.id;
    assert_ne!(org_p1_id, org2_p1_id);

    // Make sure the list projects results for the new org make sense
    let projects = objects_list_page::<Project>(
        client,
        &format!("/organizations/{}/projects", org2_name),
    )
    .await
    .items;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].identity.name, p1_name);

    cptestctx.teardown().await;
}
