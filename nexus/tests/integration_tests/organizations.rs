// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use omicron_nexus::external_api::views::Organization;

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::resource_helpers::{
    create_organization, create_project, objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_organizations(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create organizations that we'll use for testing.
    let o1_name = "test-org";
    let o2_name = "oxidecomputer";
    create_organization(&client, &o1_name).await;
    create_organization(&client, &o2_name).await;

    // Verify GET /v1/organizations/{org} works
    let o1_url = format!("/v1/organizations/{}", o1_name);
    let organization: Organization = NexusRequest::object_get(&client, &o1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(organization.identity.name, o1_name);

    let o2_url = format!("/v1/organizations/{}", o2_name);
    let organization: Organization = NexusRequest::object_get(&client, &o2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request")
        .parsed_body()
        .unwrap();
    assert_eq!(organization.identity.name, o2_name);

    // Verify requesting a non-existent organization fails
    NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &"/v1/organizations/fake-org",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");

    // Verify GET /organizations works
    let organizations =
        objects_list_page_authz::<Organization>(client, "/v1/organizations")
            .await
            .items;
    assert_eq!(organizations.len(), 2);
    // alphabetical order for now
    assert_eq!(organizations[0].identity.name, o2_name);
    assert_eq!(organizations[1].identity.name, o1_name);

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
        objects_list_page_authz::<Organization>(client, "/v1/organizations")
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

    // Delete:
    // - The default subnet within the default VPC for the project
    // - The default VPC for the project
    // - The project
    // - The organization
    let subnet_url = format!(
        "/v1/vpc-subnets/default?organization={}&project={}&vpc=default",
        o2_name, project_name
    );
    NexusRequest::object_delete(&client, &subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
    let vpc_url = format!(
        "/v1/vpcs/default?organization={}&project={}",
        o2_name, project_name
    );
    NexusRequest::object_delete(&client, &vpc_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
    NexusRequest::object_delete(
        &client,
        &format!("/v1/projects/{}?organization={}", project_name, o2_name),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
    NexusRequest::object_delete(&client, &o2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
}
