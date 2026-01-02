// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::ResultsPage;
use http::Method;
use http::StatusCode;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledCpuFamily as DbSledCpuFamily;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_lockstep_client::types::SledId;
use nexus_test_utils::TEST_SUITE_PASSWORD;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views::Rack;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::SledUuid;
use std::time::Duration;

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
    assert_eq!(
        cptestctx.server.server_context().nexus.rack_id(),
        racks[0].identity.id
    );
}

#[nexus_test]
async fn test_get_own_rack(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let expected_id = cptestctx.server.server_context().nexus.rack_id();
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
    let password = TEST_SUITE_PASSWORD.to_string();
    let _ = RequestBuilder::new(&client, Method::POST, &login_url)
        .body(Some(&test_params::UsernamePasswordCredentials {
            username,
            password,
        }))
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
    // Setup: wait until we've collected an inventory from the system set
    // up by `#[nexus_test].
    cptestctx
        .wait_for_at_least_one_inventory_collection(Duration::from_secs(60))
        .await;

    let internal_client = cptestctx.internal_client();
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
    let sled_uuid = SledUuid::new_v4();
    let sa = nexus_client::types::SledAgentInfo {
        sa_address: "[fd00:1122:3344:0100::1]:8080".parse().unwrap(),
        repo_depot_port: 8081,
        role: nexus_client::types::SledRole::Gimlet,
        baseboard: baseboard.into(),
        usable_hardware_threads: 32,
        usable_physical_ram: ByteCount::from_gibibytes_u32(100).into(),
        reservoir_size: ByteCount::from_mebibytes_u32(100).into(),
        cpu_family: nexus_client::types::SledCpuFamily::Unknown,
        generation: Generation::new(),
        decommissioned: false,
    };
    internal_client.sled_agent_put(&sled_uuid, &sa).await.unwrap();

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

#[nexus_test]
async fn test_sled_add(cptestctx: &ControlPlaneTestContext) {
    // Setup: wait until we've collected an inventory from the system set
    // up by `#[nexus_test].
    cptestctx
        .wait_for_at_least_one_inventory_collection(Duration::from_secs(60))
        .await;

    let external_client = &cptestctx.external_client;
    let list_url = "/v1/system/hardware/sleds-uninitialized";
    let mut uninitialized_sleds =
        NexusRequest::object_get(external_client, list_url)
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

    // Add one of these sleds.
    let add_url = "/v1/system/hardware/sleds/";
    let baseboard = uninitialized_sleds.pop().unwrap().baseboard;
    let sled_id = NexusRequest::objects_post(
        external_client,
        add_url,
        &params::UninitializedSledId {
            serial: baseboard.serial.clone(),
            part: baseboard.part.clone(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SledId>()
    .await
    .id;

    // Attempting to add the same sled again should succeed with the same sled
    // ID: this operation should be idempotent up until the point at which the
    // sled is inserted in the db.
    let repeat_sled_id = NexusRequest::objects_post(
        external_client,
        add_url,
        &params::UninitializedSledId {
            serial: baseboard.serial.clone(),
            part: baseboard.part.clone(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SledId>()
    .await
    .id;
    assert_eq!(sled_id, repeat_sled_id);

    // Now upsert the sled.
    let nexus = &cptestctx.server.server_context().nexus;
    nexus
        .datastore()
        .sled_upsert(SledUpdate::new(
            sled_id,
            "[::1]:0".parse().unwrap(),
            0,
            SledBaseboard {
                serial_number: baseboard.serial.clone(),
                part_number: baseboard.part.clone(),
                revision: 0,
            },
            SledSystemHardware {
                is_scrimlet: false,
                usable_hardware_threads: 8,
                usable_physical_ram: (1 << 30).try_into().unwrap(),
                reservoir_size: (1 << 20).try_into().unwrap(),
                cpu_family: DbSledCpuFamily::Unknown,
            },
            nexus.rack_id(),
            Generation::new().into(),
        ))
        .await
        .expect("inserted sled");

    // The sled has been commissioned as part of the rack, so adding it should
    // fail.
    let error: dropshot::HttpErrorResponseBody =
        NexusRequest::expect_failure_with_body(
            external_client,
            http::StatusCode::BAD_REQUEST,
            http::Method::POST,
            add_url,
            &params::UninitializedSledId {
                serial: baseboard.serial.clone(),
                part: baseboard.part.clone(),
            },
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("adding sled")
        .parsed_body()
        .expect("parsing error body");
    assert_eq!(error.error_code, Some("ObjectAlreadyExists".to_string()));
    assert!(
        error.message.contains(&baseboard.serial)
            && error.message.contains(&baseboard.part),
        "expected to find {} and {} within error message: {}",
        baseboard.serial,
        baseboard.part,
        error.message
    );
}
