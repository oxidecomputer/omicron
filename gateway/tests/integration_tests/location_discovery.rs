// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_client::SpComponent;
use gateway_client::types::SpComponentResetError;
use gateway_messages::SpPort;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use omicron_gateway::SpIdentifier;

#[tokio::test]
async fn discovery_both_locations() {
    let testctx0 =
        setup::test_setup("discovery_both_locations_0", SpPort::One).await;
    let testctx1 =
        setup::test_setup("discovery_both_locations_1", SpPort::Two).await;

    let client0 = &testctx0.client;
    let client1 = &testctx1.client;

    // the two instances should've discovered that they were switch0 and
    // switch1, respectively
    assert_eq!(
        testctx0.server.management_switch().local_switch().unwrap(),
        SpIdentifier { typ: SpType::Switch.into(), slot: 0 },
    );
    assert_eq!(
        testctx1.server.management_switch().local_switch().unwrap(),
        SpIdentifier { typ: SpType::Switch.into(), slot: 1 },
    );

    // both instances should report the same serial number for switch 0 and
    // switch 1, and it should match the expected values from the config
    for (switch, expected_serial) in [(0, "SimSidecar0"), (1, "SimSidecar1")] {
        for client in [client0, client1] {
            let state = client
                .sp_get(&SpType::Switch, switch)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(state.serial_number, expected_serial);
        }
    }

    testctx0.teardown().await;
    testctx1.teardown().await;
}

#[tokio::test]
async fn cannot_reset_local_switch() {
    let testctx0 = setup::test_setup_local_reset_disabled(
        "cannot_reset_local_switch_0",
        SpPort::One,
    )
    .await;
    let testctx1 = setup::test_setup_local_reset_disabled(
        "cannot_reset_local_switch_1",
        SpPort::Two,
    )
    .await;

    let client0 = &testctx0.client;
    let client1 = &testctx1.client;

    for (switch, client) in [(0, client0), (1, client1)] {
        // Resetting the client's local switch SP should fail...
        let err = client
            .sp_component_reset(
                &SpType::Switch,
                switch,
                SpComponent::SP_ITSELF.const_as_str(),
            )
            .await
            .unwrap_err();
        match err {
            gateway_client::Error::ErrorResponse(err) => {
                match err.into_inner() {
                    SpComponentResetError::ResetSpOfLocalSwitch => (),
                    other => panic!("unexpected inner error: {other:?}"),
                }
            }
            _ => panic!("unexpected error variant: {err:?}"),
        }

        // ... but resetting the _other_ switch should succeed.
        let other_switch = switch ^ 1;
        client
            .sp_component_reset(
                &SpType::Switch,
                other_switch,
                SpComponent::SP_ITSELF.const_as_str(),
            )
            .await
            .unwrap();
    }

    testctx0.teardown().await;
    testctx1.teardown().await;
}
