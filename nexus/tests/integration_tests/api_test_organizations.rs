// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_nexus::external_api::views::Organization;

use dropshot::test_util::{object_delete, object_get};

use super::common;
use common::resource_helpers::{
    create_organization, create_project, objects_list_page_authz,
};
use common::test_setup;
use http::method::Method;
use http::StatusCode;

extern crate slog;

#[tokio::test]
async fn test_organizations() {
    let cptestctx = test_setup("test_organizations").await;
    let client = &cptestctx.external_client;

    /* Create organizations that we'll use for testing. */
    let o1_name = "test-org";
    let o2_name = "oxidecomputer";
    create_organization(&client, &o1_name).await;
    create_organization(&client, &o2_name).await;

    let o1_url = format!("/organizations/{}", o1_name);
    // Verify GET /organizations/{org} works
    let organization: Organization = object_get(&client, &o1_url).await;
    assert_eq!(organization.identity.name, o1_name);

    let o2_url = format!("/organizations/{}", o2_name);
    let organization: Organization = object_get(&client, &o2_url).await;
    assert_eq!(organization.identity.name, o2_name);

    // Verifying requesting a non-existent organization fails
    client
        .make_request_error(
            Method::GET,
            "/organizations/fake-org",
            StatusCode::NOT_FOUND,
        )
        .await;

    // Verify GET /organizations works
    let organizations =
        objects_list_page_authz::<Organization>(client, "/organizations")
            .await
            .items;
    assert_eq!(organizations.len(), 2);
    // alphabetical order for now
    assert_eq!(organizations[0].identity.name, o2_name);
    assert_eq!(organizations[1].identity.name, o1_name);

    // Verify DELETE /organization/{org} works
    let o1_old_id = organizations[1].identity.id;
    object_delete(&client, &o1_url).await;

    // Verify the org now returns a 404
    client
        .make_request_error(Method::GET, &o1_url, StatusCode::NOT_FOUND)
        .await;

    // Verify the org is gone from the organizations list
    let organizations =
        objects_list_page_authz::<Organization>(client, "/organizations")
            .await
            .items;
    assert_eq!(organizations.len(), 1);
    assert_eq!(organizations[0].identity.name, o2_name);

    // Verify the org's name can be reused
    create_organization(&client, &o1_name).await;
    let organization: Organization = object_get(&client, &o1_url).await;
    assert_eq!(organization.identity.name, o1_name);
    // It should have a different UUID now
    assert_ne!(organization.identity.id, o1_old_id);

    // Attempt to delete a non-empty organization
    let project_name = "p1";
    let project_url = format!("{}/projects/{}", o2_url, project_name);
    create_project(&client, &o2_name, &project_name).await;
    client
        .make_request_error(Method::DELETE, &o2_url, StatusCode::BAD_REQUEST)
        .await;

    // Delete the project, then delete the organization
    object_delete(&client, &project_url).await;
    object_delete(&client, &o2_url).await;

    cptestctx.teardown().await;
}
