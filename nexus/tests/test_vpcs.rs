use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Vpc;
use omicron_common::api::external::VpcUpdateParams;
use std::convert::TryFrom;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::identity_eq;
use common::resource_helpers::{
    create_organization, create_project, create_vpc, create_vpc_with_error,
};
use common::test_setup;

extern crate slog;

#[tokio::test]
async fn test_vpcs() {
    let cptestctx = test_setup("test_vpcs").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let org_name = "test-org";
    create_organization(&client, &org_name).await;
    let project_name = "springfield-squidport";
    let vpcs_url =
        format!("/organizations/{}/projects/{}/vpcs", org_name, project_name);
    let _ = create_project(&client, &org_name, &project_name).await;

    let project_name2 = "pokemon";
    let _ = create_project(&client, &org_name, &project_name2).await;

    /* List vpcs.  We see the default VPC, and nothing else. */
    let mut vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    assert_eq!(vpcs[0].identity.name, "default");
    assert_eq!(vpcs[0].dns_name, "default");
    let default_vpc = vpcs.remove(0);

    /* Make sure we get a 404 if we fetch one. */
    let vpc_url = format!("{}/just-rainsticks", vpcs_url);

    let error = client
        .make_request_error(Method::GET, &vpc_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc with name \"just-rainsticks\"");

    /* Ditto if we try to delete one. */
    let error = client
        .make_request_error(Method::DELETE, &vpc_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc with name \"just-rainsticks\"");

    /* Create a VPC. */
    let vpc_name = "just-rainsticks";
    let vpc = create_vpc(&client, org_name, project_name, vpc_name).await;
    assert_eq!(vpc.identity.name, "just-rainsticks");
    assert_eq!(vpc.identity.description, "vpc description");
    assert_eq!(vpc.dns_name, "abc");

    /* Attempt to create a second VPC with a conflicting name. */
    let error = create_vpc_with_error(
        &client,
        org_name,
        project_name,
        vpc_name,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.message, "already exists: vpc \"just-rainsticks\"");

    /* creating a VPC with the same name in another project works, though */
    let vpc2: Vpc =
        create_vpc(&client, org_name, project_name2, vpc_name).await;
    assert_eq!(vpc2.identity.name, "just-rainsticks");

    /* List VPCs again and expect to find the one we just created. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 2);
    vpcs_eq(&vpcs[0], &default_vpc);
    vpcs_eq(&vpcs[1], &vpc);

    /* Fetch the VPC and expect it to match. */
    let vpc = vpc_get(&client, &vpc_url).await;
    vpcs_eq(&vpcs[1], &vpc);

    /* Update the VPC */
    let update_params = VpcUpdateParams {
        identity: IdentityMetadataUpdateParams {
            name: Some(Name::try_from("new-name").unwrap()),
            description: Some(String::from("another description")),
        },
        dns_name: Some(Name::try_from("def").unwrap()),
    };
    vpc_put(&client, &vpc_url, update_params).await;

    // fetching by old name fails
    let error = client
        .make_request_error(Method::GET, &vpc_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc with name \"just-rainsticks\"");

    // new url with new name
    let vpc_url = format!("{}/new-name", vpcs_url);

    /* Fetch the VPC again. It should have the updated properties. */
    let vpc = vpc_get(&client, &vpc_url).await;
    assert_eq!(vpc.identity.name, "new-name");
    assert_eq!(vpc.identity.description, "another description");
    assert_eq!(vpc.dns_name, "def");

    /* Delete the VPC. */
    client
        .make_request_no_body(Method::DELETE, &vpc_url, StatusCode::NO_CONTENT)
        .await
        .unwrap();

    /* Now we expect a 404 on fetch */
    let error = client
        .make_request_error(Method::GET, &vpc_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc with name \"new-name\"");

    /* And the list should be empty (aside from default VPC) again */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    vpcs_eq(&vpcs[0], &default_vpc);

    cptestctx.teardown().await;
}

async fn vpcs_list(client: &ClientTestContext, vpcs_url: &str) -> Vec<Vpc> {
    objects_list_page::<Vpc>(client, vpcs_url).await.items
}

async fn vpc_get(client: &ClientTestContext, vpc_url: &str) -> Vpc {
    object_get::<Vpc>(client, vpc_url).await
}

async fn vpc_put(
    client: &ClientTestContext,
    vpc_url: &str,
    params: VpcUpdateParams,
) {
    client
        .make_request(Method::PUT, &vpc_url, Some(params), StatusCode::OK)
        .await
        .unwrap();
}

fn vpcs_eq(vpc1: &Vpc, vpc2: &Vpc) {
    identity_eq(&vpc1.identity, &vpc2.identity);
    assert_eq!(vpc1.project_id, vpc2.project_id);
}
