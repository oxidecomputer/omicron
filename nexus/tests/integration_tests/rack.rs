// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::TEST_SUITE_PASSWORD;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views::Rack;
use nexus_types::internal_api::params::Baseboard;
use nexus_types::internal_api::params::SledAgentStartupInfo;
use nexus_types::internal_api::params::SledRole;
use omicron_common::api::external::ByteCount;
use omicron_nexus::TestInterfaces;
use uuid::Uuid;

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

#[nexus_test]
async fn test_sled_list_uninitialized(cptestctx: &ControlPlaneTestContext) {
    let internal_client = &cptestctx.internal_client;
    let external_client = &cptestctx.external_client;
    let list_url = "/v1/system/hardware/sleds-uninitialized";
    let mut uninitialized_sleds =
        NexusRequest::object_get(external_client, &list_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to get uninitialized sleds")
            .parsed_body::<ResultsPage<UninitializedSled>>()
            .unwrap()
            .items;
    debug!(cptestctx.logctx.log, "{:#?}", uninitialized_sleds);

    // There are currently two fake sim gimlets created in the latest inventory
    // collection as part of test setup.
    assert_eq!(2, uninitialized_sleds.len());

    // Insert one of these fake sleds into the `sled` table.
    // Just pick some random fields other than `baseboard`
    let baseboard = uninitialized_sleds.pop().unwrap().baseboard;
    let sled_uuid = Uuid::new_v4();
    let sa = SledAgentStartupInfo {
        sa_address: "[fd00:1122:3344:01::1]:8080".parse().unwrap(),
        role: SledRole::Gimlet,
        baseboard: Baseboard {
            serial_number: baseboard.serial,
            part_number: baseboard.part,
            revision: baseboard.revision,
        },
        usable_hardware_threads: 32,
        usable_physical_ram: ByteCount::from_gibibytes_u32(100),
        reservoir_size: ByteCount::from_mebibytes_u32(100),
    };
    internal_client
        .make_request(
            Method::POST,
            format!("/sled-agents/{sled_uuid}").as_str(),
            Some(&sa),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Ensure there's only one unintialized sled remaining, and it's not
    // the one that was just added into the `sled` table
    let uninitialized_sleds_2 =
        NexusRequest::object_get(external_client, &list_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to get uninitialized sleds")
            .parsed_body::<ResultsPage<UninitializedSled>>()
            .unwrap()
            .items;
    debug!(cptestctx.logctx.log, "{:#?}", uninitialized_sleds);
    assert_eq!(1, uninitialized_sleds_2.len());
    assert_eq!(uninitialized_sleds, uninitialized_sleds_2);
}
