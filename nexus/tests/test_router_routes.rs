pub mod common;
use common::test_setup;
use dropshot::test_util::objects_list_page;
use omicron_common::api::external::RouterRoute;

use crate::common::resource_helpers::{
    create_organization, create_project, create_router, create_vpc,
};

extern crate slog;

#[tokio::test]
async fn test_router_routes() {
    let cptestctx = test_setup("test_vpc_routers").await;
    let client = &cptestctx.external_client;

    let organization_name = "test-org";
    let project_name = "springfield-squidport";
    let vpc_name = "vpc1";
    let router_name = "router1";

    let routes_url = format!(
        "/organizations/{}/projects/{}/vpcs/{}/routers/{}/routes",
        organization_name, project_name, vpc_name, router_name
    );

    create_organization(&client, organization_name).await;
    let _ = create_project(&client, organization_name, project_name).await;

    /* Create a VPC. */
    let vpc =
        create_vpc(&client, organization_name, project_name, vpc_name).await;

    /* Create a VPC Router. */
    let router = create_router(
        &client,
        organization_name,
        project_name,
        vpc_name,
        router_name,
    )
    .await;

    // There should be no initial routes
    // TODO: Eventually this will list the system router as a default
    let routes =
        objects_list_page::<RouterRoute>(client, &routes_url).await.items;
    assert_eq!(routes.len(), 0);
}
