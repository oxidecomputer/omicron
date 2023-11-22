// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::TEST_SUITE_PASSWORD;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::Rack;
use omicron_nexus::TestInterfaces;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_list_own_rack(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let racks_url = "/v1/system/hardware/racks";
    let racks: Vec<Rack> =
        NexusRequest::iter_collection_authn(client, racks_url, "", None)
            .await
            .expect("failed to list racks")
            .all_items;

    assert_eq!(1, racks.len());
    assert_eq!(cptestctx.server.apictx().nexus.rack_id(), racks[0].identity.id);
}

#[nexus_test]
async fn test_get_own_rack(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let expected_id = cptestctx.server.apictx().nexus.rack_id();
    let rack_url = format!("/v1/system/hardware/racks/{}", expected_id);
    let rack = NexusRequest::object_get(client, &rack_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get rack")
        .parsed_body::<Rack>()
        .unwrap();

    assert_eq!(expected_id, rack.identity.id);
}

#[nexus_test]
async fn test_rack_initialization(cptestctx: &ControlPlaneTestContext) {
    // The ControlPlaneTestContext has already done rack initialization.  Here
    // we can verify some of the higher-level consequences.
    let client = &cptestctx.external_client;

    // Verify that the initial user can log in with the expected password.
    // This password is (implicitly) determined by the password hash that we
    // provide when setting up the rack (when setting up the
    // ControlPlaneTestContext).  We use the status code to verify a successful
    // login.
    let login_url = format!("/v1/login/{}/local", cptestctx.silo_name);
    let username = cptestctx.user_name.clone();
    let password: params::Password = TEST_SUITE_PASSWORD.parse().unwrap();
    let _ = RequestBuilder::new(&client, Method::POST, &login_url)
        .body(Some(&params::UsernamePasswordCredentials { username, password }))
        .expect_status(Some(StatusCode::NO_CONTENT))
        .execute()
        .await
        .expect("failed to log in");

    // Verify the external DNS record for the initial Silo.
    crate::integration_tests::silos::verify_silo_dns_name(
        cptestctx,
        cptestctx.silo_name.as_str(),
        true,
    )
    .await;
}
