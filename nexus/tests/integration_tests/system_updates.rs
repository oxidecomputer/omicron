// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use http::{method::Method, StatusCode};
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{
    params, shared::UpdateableComponentType, views,
};
use omicron_common::api::external::SemverVersion;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// This file could be combined with ./updates.rs, but there's a lot going on in
// there that has nothing to do with testing the API endpoints. We could come up
// with more descriptive names.

/// Because there are no create endpoints for these resources, we need to call
/// the `nexus` functions directly.
async fn populate_db(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.apictx().nexus.datastore().clone(),
    );

    // system updates have to exist first
    let create_su =
        params::SystemUpdateCreate { version: SemverVersion::new(0, 2, 0) };
    nexus
        .upsert_system_update(&opctx, create_su)
        .await
        .expect("Failed to create system update");
    let create_su =
        params::SystemUpdateCreate { version: SemverVersion::new(1, 0, 1) };
    nexus
        .upsert_system_update(&opctx, create_su)
        .await
        .expect("Failed to create system update");

    nexus
        .create_updateable_component(
            &opctx,
            params::UpdateableComponentCreate {
                version: SemverVersion::new(0, 4, 1),
                system_version: SemverVersion::new(0, 2, 0),
                component_type: UpdateableComponentType::BootloaderForSp,
                device_id: "look-a-device".to_string(),
            },
        )
        .await
        .expect("failed to create updateable component");

    nexus
        .create_updateable_component(
            &opctx,
            params::UpdateableComponentCreate {
                version: SemverVersion::new(0, 4, 1),
                system_version: SemverVersion::new(1, 0, 1),
                component_type: UpdateableComponentType::HubrisForGimletSp,
                device_id: "another-device".to_string(),
            },
        )
        .await
        .expect("failed to create updateable component");
}

#[nexus_test]
async fn test_system_version(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Initially the endpoint 500s because there are no updateable components.
    // This is the desired behavior because those are populated by rack startup
    // before the external API starts, so it really is a problem if we can hit
    // this endpoint without any data backing it.
    //
    // Because this data is now populated at rack init, this doesn't work as a
    // test. If we really wanted to test it, we would have to run the tests
    // without that bit of setup.
    //
    // NexusRequest::expect_failure(
    //     &client,
    //     StatusCode::INTERNAL_SERVER_ERROR,
    //     Method::GET,
    //     "/v1/system/update/version",
    // )
    // .authn_as(AuthnMode::PrivilegedUser)
    // .execute()
    // .await
    // .expect("Failed to 500 with no system version data");

    // create two updateable components
    populate_db(&cptestctx).await;

    let version =
        NexusRequest::object_get(&client, "/v1/system/update/version")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::SystemVersion>()
            .await;

    assert_eq!(
        version,
        views::SystemVersion {
            version_range: views::VersionRange {
                low: SemverVersion::new(0, 2, 0),
                high: SemverVersion::new(2, 0, 0),
            },
            status: views::UpdateStatus::Updating,
        }
    );
}

#[nexus_test]
async fn test_list_updates(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let updates =
        NexusRequest::object_get(&client, &"/v1/system/update/updates")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::SystemUpdate>>()
            .await;

    assert_eq!(updates.items.len(), 3);
}

#[nexus_test]
async fn test_list_components(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let component_updates =
        NexusRequest::object_get(&client, &"/v1/system/update/components")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::ComponentUpdate>>()
            .await;

    assert_eq!(component_updates.items.len(), 9);
}

#[nexus_test]
async fn test_get_update(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // existing update works
    let update =
        NexusRequest::object_get(&client, &"/v1/system/update/updates/1.0.0")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::SystemUpdate>()
            .await;

    assert_eq!(update.version, SemverVersion::new(1, 0, 0));

    // non-existent update 404s
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/v1/system/update/updates/1.0.1",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to 404 on non-existent update");
}

#[nexus_test]
async fn test_list_update_components(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // listing components of an existing update works
    let components = NexusRequest::object_get(
        &client,
        &"/v1/system/update/updates/1.0.0/components",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<ResultsPage<views::ComponentUpdate>>()
    .await;

    assert_eq!(components.items.len(), 9);

    // non existent 404s
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/v1/system/update/updates/1.0.1/components",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to 404 on components of nonexistent system update");
}

#[nexus_test]
async fn test_update_deployments(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let deployments =
        NexusRequest::object_get(&client, &"/v1/system/update/deployments")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<ResultsPage<views::UpdateDeployment>>()
            .await;

    assert_eq!(deployments.items.len(), 2);

    let first_dep = deployments.items.get(0).unwrap();

    let dep_id = first_dep.identity.id.to_string();
    let dep_url = format!("/v1/system/update/deployments/{}", dep_id);
    let deployment = NexusRequest::object_get(&client, &dep_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::UpdateDeployment>()
        .await;

    assert_eq!(deployment.version, first_dep.version);
}
