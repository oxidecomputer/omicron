// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_router;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::{create_project, create_vpc};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::VpcRouter;
use nexus_types::external_api::views::VpcRouterKind;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_vpc_routers(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let project_name = "springfield-squidport";
    let _ = create_project(&client, project_name).await;

    // Create a VPC.
    let vpc_name = "vpc1";
    let vpc = create_vpc(&client, project_name, vpc_name).await;

    let routers_url =
        format!("/v1/vpc-routers?project={}&vpc={}", project_name, vpc_name);

    // get routers should have only the system router created w/ the VPC
    let routers =
        objects_list_page_authz::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].kind, VpcRouterKind::System);

    let router_name = "router1";
    let router_url = format!(
        "/v1/vpc-routers/{}?project={}&vpc={}",
        router_name, project_name, vpc_name
    );

    // fetching a particular router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"router1\"");

    // Create a VPC Router.
    let router =
        create_router(&client, project_name, vpc_name, router_name).await;
    assert_eq!(router.identity.name, router_name);
    assert_eq!(router.identity.description, "router description");
    assert_eq!(router.vpc_id, vpc.identity.id);
    assert_eq!(router.kind, VpcRouterKind::Custom);

    // get router, should be the same
    let same_router = NexusRequest::object_get(client, &router_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    routers_eq(&router, &same_router);

    // routers list should now have the one in it
    let routers = objects_list_page_authz(client, &routers_url).await.items;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router);

    // creating another router in the same VPC with the same name fails
    let error: dropshot::HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(&client, Method::POST, &routers_url)
            .body(Some(&params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: router_name.parse().unwrap(),
                    description: String::from("this is not a router"),
                },
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "already exists: vpc-router \"router1\"");

    let router2_name = "router2";
    let router2_url = format!(
        "/v1/vpc-routers/{}?project={}&vpc={}",
        router2_name, project_name, vpc_name
    );

    // second router 404s before it's created
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router2_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"router2\"");

    // create second custom router
    let router2 =
        create_router(client, project_name, vpc_name, router2_name).await;
    assert_eq!(router2.identity.name, router2_name);
    assert_eq!(router2.vpc_id, vpc.identity.id);
    assert_eq!(router2.kind, VpcRouterKind::Custom);

    // routers list should now have two custom and one system
    let routers =
        objects_list_page_authz::<VpcRouter>(client, &routers_url).await.items;
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
    let update: VpcRouter =
        NexusRequest::object_put(&client, &router_url, Some(&update_params))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(update.identity.id, router.identity.id);
    assert_eq!(update.identity.name, update_params.identity.name.unwrap());
    assert_eq!(
        update.identity.description,
        update_params.identity.description.unwrap()
    );

    // fetching by old name 404s
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"router1\"");

    let router_url = format!(
        "/v1/vpc-routers/new-name?project={}&vpc={}",
        project_name, vpc_name
    );

    // fetching by new name works
    let updated_router: VpcRouter =
        NexusRequest::object_get(&client, &router_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    routers_eq(&update, &updated_router);
    assert_eq!(&updated_router.identity.description, "another description");

    // fetching list should show updated one
    let routers =
        objects_list_page_authz::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 3);
    routers_eq(&routers[0], &updated_router);

    // delete first router
    NexusRequest::object_delete(&client, &router_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // routers list should now have two again, one system and one custom
    let routers =
        objects_list_page_authz::<VpcRouter>(client, &routers_url).await.items;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router2);

    // get router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"new-name\"");

    // delete router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"new-name\"");

    // Creating a router with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 = create_vpc(&client, project_name, vpc2_name).await;

    let router_same_name =
        create_router(&client, project_name, vpc2_name, router2_name).await;
    assert_eq!(router_same_name.identity.name, router2_name);
    assert_eq!(router_same_name.vpc_id, vpc2.identity.id);
}

fn routers_eq(sn1: &VpcRouter, sn2: &VpcRouter) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}
