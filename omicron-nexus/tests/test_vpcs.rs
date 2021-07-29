use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::ProjectCreateParams;
use omicron_common::api::external::ProjectView;
use omicron_common::api::external::VPCCreateParams;
use omicron_common::api::external::VPC;
use std::convert::TryFrom;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::identity_eq;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_vpcs() {
    let cptestctx = test_setup("test_vpcs").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let vpcs_url = format!("/projects/{}/vpcs", project_name);
    let _ = create_project(&client, &project_name).await;

    let project_name2 = "pokemon";
    let vpcs_url2 = format!("/projects/{}/vpcs", project_name2);
    let _ = create_project(&client, &project_name2).await;

    /* List vpcs.  There aren't any yet. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 0);

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
    let new_vpc = VPCCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
    };
    let vpc: VPC = objects_post(&client, &vpcs_url, new_vpc.clone()).await;
    assert_eq!(vpc.identity.name, "just-rainsticks");
    assert_eq!(vpc.identity.description, "sells rainsticks");

    /* Attempt to create a second VPC with a conflicting name. */
    let error = client
        .make_request_error_body(
            Method::POST,
            &vpcs_url,
            new_vpc.clone(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "already exists: vpc \"just-rainsticks\"");

    /* creating a VPC with the same name in another project works, though */
    let vpc2: VPC = objects_post(&client, &vpcs_url2, new_vpc.clone()).await;
    assert_eq!(vpc2.identity.name, "just-rainsticks");
    assert_eq!(vpc2.identity.description, "sells rainsticks");

    /* List VPCs again and expect to find the one we just created. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    vpcs_eq(&vpcs[0], &vpc);

    /* Fetch the VPC and expect it to match. */
    let vpc = vpc_get(&client, &vpc_url).await;
    vpcs_eq(&vpcs[0], &vpc);

    /* Delete the VPC. */
    client
        .make_request_no_body(Method::DELETE, &vpc_url, StatusCode::NO_CONTENT)
        .await
        .unwrap();

    /* Now we expect a 404 on fetch */
    let error = client
        .make_request_error(Method::GET, &vpc_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc with name \"just-rainsticks\"");

    /* And the list should be empty again */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 0);

    cptestctx.teardown().await;
}

async fn vpcs_list(client: &ClientTestContext, vpcs_url: &str) -> Vec<VPC> {
    objects_list_page::<VPC>(client, vpcs_url).await.items
}

async fn vpc_get(client: &ClientTestContext, vpc_url: &str) -> VPC {
    object_get::<VPC>(client, vpc_url).await
}

fn vpcs_eq(vpc1: &VPC, vpc2: &VPC) {
    identity_eq(&vpc1.identity, &vpc2.identity);
    assert_eq!(vpc1.project_id, vpc2.project_id);
}

async fn create_project(
    client: &ClientTestContext,
    project_name: &str,
) -> ProjectView {
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
