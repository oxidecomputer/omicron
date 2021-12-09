// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::VpcRouter;
use omicron_common::api::external::VpcRouterKind;
use omicron_nexus::external_api::params;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;

use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::{
    create_organization, create_project, create_vpc,
};
use nexus_test_utils::test_setup;

#[tokio::test]
async fn test_vpc_routers() {
    let cptestctx = test_setup("test_vpc_routers").await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    let organization_name = "test-org";
    let project_name = "springfield-squidport";
    let vpcs_url = format!(
        "/organizations/{}/projects/{}/vpcs",
        organization_name, project_name
    );
    create_organization(&client, organization_name).await;
    let _ = create_project(&client, organization_name, project_name).await;

    /* Create a VPC. */
    let vpc_name = "vpc1";
    let vpc =
        create_vpc(&client, organization_name, project_name, vpc_name).await;

    let vpc_url = format!("{}/{}", vpcs_url, vpc_name);
    let routers_url = format!("{}/routers", vpc_url);

    // get routers should have only the system router created w/ the VPC
    let routers =
        objects_list_page::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].kind, VpcRouterKind::System);

    let router_name = "router1";
    let router_url = format!("{}/{}", routers_url, router_name);

    // fetching a particular router should 404
    let error = client
        .make_request_error(Method::GET, &router_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc router with name \"router1\"");

    /* Create a VPC Router. */
    let new_router = params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: router_name.parse().unwrap(),
            description: "it's not really a router".to_string(),
        },
    };
    let router: VpcRouter =
        objects_post(&client, &routers_url, new_router.clone()).await;
    assert_eq!(router.identity.name, router_name);
    assert_eq!(router.identity.description, "it's not really a router");
    assert_eq!(router.vpc_id, vpc.identity.id);
    assert_eq!(router.kind, VpcRouterKind::Custom);

    // get router, should be the same
    let same_router = object_get::<VpcRouter>(client, &router_url).await;
    routers_eq(&router, &same_router);

    // routers list should now have the one in it
    let routers =
        objects_list_page::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router);

    // creating another router in the same VPC with the same name fails
    let error = client
        .make_request_error_body(
            Method::POST,
            &routers_url,
            new_router.clone(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "already exists: vpc router \"router1\"");

    let router2_name = "router2";
    let router2_url = format!("{}/{}", routers_url, router2_name);

    // second router 404s before it's created
    let error = client
        .make_request_error(Method::GET, &router2_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc router with name \"router2\"");

    // create second custom router
    let new_router = params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: router2_name.parse().unwrap(),
            description: "it's also not really a router".to_string(),
        },
    };
    let router2: VpcRouter =
        objects_post(&client, &routers_url, new_router.clone()).await;
    assert_eq!(router2.identity.name, router2_name);
    assert_eq!(router2.identity.description, "it's also not really a router");
    assert_eq!(router2.vpc_id, vpc.identity.id);
    assert_eq!(router2.kind, VpcRouterKind::Custom);

    // routers list should now have two custom and one system
    let routers =
        objects_list_page::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 3);
    routers_eq(&routers[0], &router);
    routers_eq(&routers[1], &router2);

    // update first router
    let update_params = params::VpcRouterUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("new-name".parse().unwrap()),
            description: Some("another description".to_string()),
        },
    };
    client
        .make_request(
            Method::PUT,
            &router_url,
            Some(update_params),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // fetching by old name 404s
    let error = client
        .make_request_error(Method::GET, &router_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc router with name \"router1\"");

    let router_url = format!("{}/{}", routers_url, "new-name");

    // fetching by new name works
    let updated_router = object_get::<VpcRouter>(client, &router_url).await;
    assert_eq!(&updated_router.identity.description, "another description");

    // fetching list should show updated one
    let routers =
        objects_list_page::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 3);
    routers_eq(&routers[0], &updated_router);

    // delete first router
    client
        .make_request_no_body(
            Method::DELETE,
            &router_url,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // routers list should now have two again, one system and one custom
    let routers =
        objects_list_page::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router2);

    // get router should 404
    let error = client
        .make_request_error(Method::GET, &router_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc router with name \"new-name\"");

    // delete router should 404
    let error = client
        .make_request_error(Method::DELETE, &router_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: vpc router with name \"new-name\"");

    // Creating a router with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 =
        create_vpc(&client, organization_name, project_name, vpc2_name).await;

    let router_same_name: VpcRouter = objects_post(
        &client,
        format!("{}/{}/routers", vpcs_url, vpc2_name).as_str(),
        new_router.clone(),
    )
    .await;
    assert_eq!(router_same_name.identity.name, router2_name);
    assert_eq!(
        router_same_name.identity.description,
        "it's also not really a router"
    );
    assert_eq!(router_same_name.vpc_id, vpc2.identity.id);

    cptestctx.teardown().await;
}

fn routers_eq(sn1: &VpcRouter, sn2: &VpcRouter) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}
