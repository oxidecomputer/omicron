// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use omicron_nexus::external_api::views::Organization;

use dropshot::test_util::object_delete;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_project, objects_list_page_authz,
};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

#[nexus_test]
async fn test_organizations(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    /* Create organizations that we'll use for testing. */
    let o1_name = "test-org";
    let o2_name = "oxidecomputer";
    create_organization(&client, &o1_name).await;
    create_organization(&client, &o2_name).await;

    // Verify GET /organizations/{org} works
    let o1_url = format!("/organizations/{}", o1_name);
    let organization: Organization = NexusRequest::object_get(&client, &o1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(organization.identity.name, o1_name);

    // You should get a 404 if not authenticated.
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &o1_url,
    )
    .execute()
    .await
    .expect("failed to make request");

    // Same if you're authenticated but not authorized to see it.
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &o1_url,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    let o2_url = format!("/organizations/{}", o2_name);
    let organization: Organization = NexusRequest::object_get(&client, &o2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(organization.identity.name, o2_name);

    // Verify requesting a non-existent organization fails
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

    // You should get a 404 if you attempt to delete an organization if you are
    // unauthenticated or unauthorized.
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &o1_url,
    )
    .execute()
    .await
    .expect("failed to make request");

    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &o1_url,
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Verify DELETE /organization/{org} works
    let o1_old_id = organizations[1].identity.id;
    NexusRequest::object_delete(&client, &o1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // Verify the org now returns a 404
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &o1_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Verify the org is gone from the organizations list
    let organizations =
        objects_list_page_authz::<Organization>(client, "/organizations")
            .await
            .items;
    assert_eq!(organizations.len(), 1);
    assert_eq!(organizations[0].identity.name, o2_name);

    // Verify the org's name can be reused
    create_organization(&client, &o1_name).await;
    let organization: Organization = NexusRequest::object_get(&client, &o1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(organization.identity.name, o1_name);
    // It should have a different UUID now
    assert_ne!(organization.identity.id, o1_old_id);

    // Attempt to delete a non-empty organization
    let project_name = "p1";
    let project_url = format!("{}/projects/{}", o2_url, project_name);
    create_project(&client, &o2_name, &project_name).await;
    NexusRequest::expect_failure(
        &client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &o2_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Delete the project, then delete the organization
    object_delete(&client, &project_url).await;
    NexusRequest::object_delete(&client, &o2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
}
