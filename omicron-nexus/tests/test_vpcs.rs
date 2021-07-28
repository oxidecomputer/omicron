// use http::method::Method;
// use http::StatusCode;
use omicron_common::api::IdentityMetadataCreateParams;
use omicron_common::api::Name;
use omicron_common::api::ProjectCreateParams;
use omicron_common::api::ProjectView;
use omicron_common::api::VPCCreateParams;
use omicron_common::api::VPCView;
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
    // let apictx = &cptestctx.server.apictx;
    // let nexus = &apictx.nexus;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let vpcs_url = format!("/projects/{}/vpcs", project_name);
    let _: ProjectView = objects_post(
        &client,
        "/projects",
        ProjectCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(project_name).unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await;

    /* List vpcs.  There aren't any yet. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 0);

    // /* Make sure we get a 404 if we fetch one. */
    let vpc_url = format!("{}/just-rainsticks", vpcs_url);

    // let error = client
    //     .make_request_error(Method::GET, &vpc_url, StatusCode::NOT_FOUND)
    //     .await;
    // assert_eq!(
    //     error.message,
    //     "not found: vpc with name \"just-rainsticks\""
    // );

    // /* Ditto if we try to delete one. */
    // let error = client
    //     .make_request_error(
    //         Method::DELETE,
    //         &vpc_url,
    //         StatusCode::NOT_FOUND,
    //     )
    //     .await;
    // assert_eq!(
    //     error.message,
    //     "not found: vpc with name \"just-rainsticks\""
    // );

    /* Create a VPC. */
    let new_vpc = VPCCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
    };
    let vpc: VPCView = objects_post(&client, &vpcs_url, new_vpc.clone()).await;
    assert_eq!(vpc.identity.name, "just-rainsticks");
    assert_eq!(vpc.identity.description, "sells rainsticks");

    /* Attempt to create a second VPC with a conflicting name. */
    // let error = client
    //     .make_request_error_body(
    //         Method::POST,
    //         &vpcs_url,
    //         new_vpc,
    //         StatusCode::BAD_REQUEST,
    //     )
    //     .await;
    // assert_eq!(error.message, "already exists: instance \"just-rainsticks\"");

    /* List VPCs again and expect to find the one we just created. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    vpcs_eq(&vpcs[0], &vpc);

    /* Fetch the VPC and expect it to match. */
    // let vpc = vpc_get(&client, &vpc_url).await;
    // vpcs_eq(&vpcs[0], &vpc);

    cptestctx.teardown().await;
}

async fn vpcs_list(client: &ClientTestContext, vpcs_url: &str) -> Vec<VPCView> {
    objects_list_page::<VPCView>(client, vpcs_url).await.items
}

async fn vpc_get(client: &ClientTestContext, vpc_url: &str) -> VPCView {
    object_get::<VPCView>(client, vpc_url).await
}

fn vpcs_eq(vpc1: &VPCView, vpc2: &VPCView) {
    identity_eq(&vpc1.identity, &vpc2.identity);
    assert_eq!(vpc1.project_id, vpc2.project_id);
}
