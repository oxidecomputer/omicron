use omicron_common::api::external::Organization;

use dropshot::test_util::{object_get, objects_list_page};

pub mod common;
use common::resource_helpers::create_organization;
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
        objects_list_page::<Organization>(client, "/organizations").await.items;
    assert_eq!(organizations.len(), 2);
    // alphabetical order for now
    assert_eq!(organizations[0].identity.name, o2_name);
    assert_eq!(organizations[1].identity.name, o1_name);

    cptestctx.teardown().await;
}
